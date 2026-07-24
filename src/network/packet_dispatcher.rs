use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::AppError;
use super::{NetworkEngine, PacketContext, PacketHandler};
use crate::types::FileAttachment;
use crate::protocol::{
    parse_file_attachments, IPMsgPacket, IPMSG_ANSENTRY, IPMSG_BR_ENTRY, IPMSG_BR_EXIT,
    IPMSG_FILE_CLIPBOARD, IPMSG_INPUTING, IPMSG_INPUT_END, IPMSG_KNOCK,
    IPMSG_RECVMSG, IPMSG_SENDMSG,
};

pub struct BrEntryHandler;
pub struct SendMsgHandler;
pub struct RecvMsgHandler;
pub struct BrExitHandler;
pub struct KnockHandler;
pub struct InputHandler;

#[async_trait]
impl PacketHandler for BrEntryHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let cmd_base = packet.command_base();
        if cmd_base == IPMSG_BR_ENTRY {
            let my_username = ctx.engine.my_username.lock().unwrap().clone();
            let _ = ctx
                .engine
                .send_packet(&ctx.peer_ip(), IPMSG_ANSENTRY, &my_username)
                .await;
        }
        Ok(())
    }
}

/// Extracts Null-separated attachments and trims custom font styling from messages.
fn parse_and_format_message(
    packet: &IPMsgPacket,
) -> (String, Vec<FileAttachment>) {
    let mut text_content = packet.extra.clone();
    let mut attachments = Vec::new();

    if packet.extra.contains('\0') {
        let parts: Vec<&str> = packet.extra.splitn(2, '\0').collect();
        text_content = parts[0].to_string();
        attachments = parse_file_attachments(parts[1]);
    }

    if let Some(pos) = text_content.find('{') {
        text_content.truncate(pos);
    }

    // Sanitize the raw user message text
    text_content = super::validation::sanitize_message(&text_content);

    // Sanitize file attachment names to prevent path traversal or giant filenames
    for att in &mut attachments {
        att.name = super::validation::sanitize_filename(&att.name);
    }

    if packet.is_file_attach() && !attachments.is_empty() {
        for att in &attachments {
            let size_str = crate::types::format_file_size(att.size);
            let file_line = format!("Shared a file: {} ({})", att.name, size_str);
            if text_content.is_empty() {
                text_content = file_line;
            } else {
                text_content = format!("{}\n{}", text_content, file_line);
            }
        }
    }

    (text_content, attachments)
}

/// Automatically handles parallel disk caching and download queues for clipboard media files.
fn auto_download_clipboards(
    ctx: &PacketContext,
    packet_no: u32,
    attachments: &[FileAttachment],
) {
    for att in attachments {
        if (att.file_type & IPMSG_FILE_CLIPBOARD) == IPMSG_FILE_CLIPBOARD {
            let cache_dir = std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join("image_cache");
            if !cache_dir.exists() {
                let _ = std::fs::create_dir_all(&cache_dir);
            }
            let save_path = cache_dir.join(&att.name);

            let task_id = ctx.engine.next_transfer_task_id();
            ctx.engine.event_dispatcher.on_transfer_started(
                task_id,
                ctx.peer_ip(),
                att.name.clone(),
                att.size as i64,
                false,
            );

            let req = crate::types::FileDownloadRequest {
                peer_ip: ctx.peer_ip(),
                packet_no,
                file_id: att.id,
                file_size: att.size,
                save_path,
                is_directory: false,
                task_id,
            };

            let engine_clone = ctx.engine.clone();
            tokio::spawn(async move {
                if let Err(e) = engine_clone.download_file_direct(req).await {
                    eprintln!("Auto-download of inline clipboard image failed: {}", e);
                }
            });
        }
    }
}

#[async_trait]
impl PacketHandler for SendMsgHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let (text_content, attachments) = parse_and_format_message(packet);
        let now = chrono::Utc::now().timestamp_millis();

        ctx.engine.event_dispatcher.on_message_received(
            ctx.peer_ip(),
            text_content,
            now,
            packet.username.clone(),
        );

        if packet.is_file_attach() && !attachments.is_empty() {
            ctx.engine.event_dispatcher.on_file_attachments_received(
                ctx.peer_ip(),
                packet.packet_no,
                attachments.clone(),
            );
            auto_download_clipboards(ctx, packet.packet_no, &attachments);
        }

        if packet.is_send_check() {
            let ack_extra = format!("{}", packet.packet_no);
            let _ = ctx
                .engine
                .send_packet(&ctx.peer_ip(), IPMSG_RECVMSG, &ack_extra)
                .await;
        }

        Ok(())
    }
}

#[async_trait]
impl PacketHandler for RecvMsgHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        if let Ok(ack_no) = packet.extra.trim().parse::<u32>() {
            ctx.engine.ack_tracker.ack(ack_no).await;
        }
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for BrExitHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        ctx.engine.event_dispatcher.on_peer_status_changed(
            ctx.peer_ip(),
            packet.username.clone(),
            packet.hostname.clone(),
            None,
            false,
        );
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for KnockHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        ctx.engine
            .event_dispatcher
            .on_window_knock(ctx.peer_ip(), packet.username.clone());
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for InputHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let cmd_base = packet.command_base();
        let typing = cmd_base == IPMSG_INPUTING;
        ctx.engine
            .event_dispatcher
            .on_peer_typing(ctx.peer_ip(), typing);
        Ok(())
    }
}

pub struct PacketDispatcher {
    received_packets_cache: std::sync::Mutex<HashSet<(u32, u32)>>,
    handlers: HashMap<u32, Box<dyn PacketHandler>>,
}

impl Default for PacketDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketDispatcher {
    pub fn new() -> Self {
        let mut handlers: HashMap<u32, Box<dyn PacketHandler>> = HashMap::new();
        handlers.insert(IPMSG_BR_ENTRY, Box::new(BrEntryHandler));
        handlers.insert(IPMSG_ANSENTRY, Box::new(BrEntryHandler));
        handlers.insert(IPMSG_SENDMSG, Box::new(SendMsgHandler));
        handlers.insert(IPMSG_RECVMSG, Box::new(RecvMsgHandler));
        handlers.insert(IPMSG_BR_EXIT, Box::new(BrExitHandler));
        handlers.insert(IPMSG_KNOCK, Box::new(KnockHandler));
        handlers.insert(IPMSG_INPUTING, Box::new(InputHandler));
        handlers.insert(IPMSG_INPUT_END, Box::new(InputHandler));

        Self {
            received_packets_cache: std::sync::Mutex::new(HashSet::new()),
            handlers,
        }
    }

    pub async fn dispatch(
        &self,
        engine: Arc<NetworkEngine>,
        peer_ip_addr: std::net::IpAddr,
        ip_u32: u32,
        mut packet: IPMsgPacket,
    ) -> Result<(), AppError> {
        // Sanitize username and hostname at network boundary
        packet.username = super::validation::sanitize_username(&packet.username);
        packet.hostname = super::validation::sanitize_username(&packet.hostname);

        let cmd_base = packet.command_base();

        let is_duplicate = {
            let mut cache = self.received_packets_cache.lock().unwrap();
            let key = (ip_u32, packet.packet_no);
            if cache.contains(&key) {
                true
            } else {
                cache.insert(key);
                if cache.len() > 1000 {
                    let to_remove = cache.len() - 1000;
                    let mut removed = 0;
                    cache.retain(|_| {
                        if removed < to_remove {
                            removed += 1;
                            false // remove
                        } else {
                            true // keep
                        }
                    });
                }
                false
            }
        };

        if is_duplicate {
            if cmd_base == IPMSG_SENDMSG && packet.is_send_check()
            {
                let ack_extra = format!("{}", packet.packet_no);
                let peer_ip = peer_ip_addr.to_string();
                let _ = engine
                    .send_packet(&peer_ip, IPMSG_RECVMSG, &ack_extra)
                    .await;
            }
            return Ok(());
        }

        let peer_ip = peer_ip_addr.to_string();
        let nickname = if !packet.extra.trim().is_empty()
            && (cmd_base == IPMSG_BR_ENTRY || cmd_base == IPMSG_ANSENTRY)
        {
            Some(super::validation::sanitize_username(packet.extra.trim()))
        } else {
            None
        };

        // Fix redundant/racey status changed update: Only trigger online=true status update if NOT going offline.
        if cmd_base != IPMSG_BR_EXIT {
            engine.event_dispatcher.on_peer_status_changed(
                peer_ip.clone(),
                packet.username.clone(),
                packet.hostname.clone(),
                nickname.clone(),
                true,
            );
        }

        let ctx = PacketContext {
            peer_ip_addr,
            engine,
        };

        if let Some(handler) = self.handlers.get(&cmd_base) {
            handler.handle(&ctx, &packet).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::NetworkEvents;
    use crate::protocol::Utf8Transcoder;
    use std::sync::Mutex;

    struct TestEvents {
        messages: Mutex<Vec<(String, String, String)>>, // (sender_ip, content, username)
        peers: Mutex<Vec<(String, String, String, Option<String>, bool)>>, // (ip, username, hostname, nickname, online)
    }

    impl NetworkEvents for TestEvents {
        fn on_peer_status_changed(
            &self,
            ip: String,
            username: String,
            hostname: String,
            nickname: Option<String>,
            online: bool,
        ) {
            self.peers
                .lock()
                .unwrap()
                .push((ip, username, hostname, nickname, online));
        }
        fn on_message_received(
            &self,
            sender_ip: String,
            text_content: String,
            _timestamp: i64,
            username: String,
        ) {
            self.messages
                .lock()
                .unwrap()
                .push((sender_ip, text_content, username));
        }
        fn on_file_attachments_received(
            &self,
            _sender_ip: String,
            _packet_no: u32,
            _files: Vec<FileAttachment>,
        ) {
        }
        fn on_window_knock(&self, _sender_ip: String, _username: String) {}
        fn on_peer_typing(&self, _sender_ip: String, _typing: bool) {}
        fn on_transfer_progress(
            &self,
            _task_id: i64,
            _progress: f64,
            _status: crate::types::TransferStatus,
        ) {
        }
        fn on_transfer_started(
            &self,
            _task_id: i64,
            _peer_ip: String,
            _file_name: String,
            _file_size: i64,
            _is_sending: bool,
        ) {
        }
    }

    #[tokio::test]
    async fn test_send_msg_handler_no_ack() {
        let events = Arc::new(TestEvents {
            messages: Mutex::new(Vec::new()),
            peers: Mutex::new(Vec::new()),
        });

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let transcoder = Arc::new(Utf8Transcoder);
        let engine = Arc::new(
            NetworkEngine::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                events.clone(),
                transcoder,
                0,
            )
            .unwrap(),
        );

        let context = PacketContext {
            peer_ip_addr: "127.0.0.1".parse().unwrap(),
            engine,
        };

        let packet = IPMsgPacket {
            version: "1".to_string(),
            packet_no: 12345,
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            cmd: crate::protocol::IPMSG_SENDMSG,
            extra: "Hello from Bob!".to_string(),
        };

        let handler = SendMsgHandler;
        let result = handler.handle(&context, &packet).await;
        assert!(result.is_ok());

        let messages = events.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "127.0.0.1");
        assert_eq!(messages[0].1, "Hello from Bob!");
        assert_eq!(messages[0].2, "bob");
    }

    #[tokio::test]
    async fn test_send_msg_handler_with_ack() {
        let events = Arc::new(TestEvents {
            messages: Mutex::new(Vec::new()),
            peers: Mutex::new(Vec::new()),
        });

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let transcoder = Arc::new(Utf8Transcoder);
        let engine = Arc::new(
            NetworkEngine::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                events.clone(),
                transcoder,
                0,
            )
            .unwrap(),
        );

        let context = PacketContext {
            peer_ip_addr: "127.0.0.1".parse().unwrap(),
            engine,
        };

        let packet = IPMsgPacket {
            version: "1".to_string(),
            packet_no: 54321,
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            cmd: crate::protocol::IPMSG_SENDMSG | crate::protocol::IPMSG_SENDCHECKOPT,
            extra: "ACK message".to_string(),
        };

        let handler = SendMsgHandler;
        let result = handler.handle(&context, &packet).await;
        assert!(result.is_ok());

        let messages = events.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].1, "ACK message");
    }

    #[tokio::test]
    async fn test_dispatch_sanitizes_untrusted_data() {
        let events = Arc::new(TestEvents {
            messages: Mutex::new(Vec::new()),
            peers: Mutex::new(Vec::new()),
        });

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let transcoder = Arc::new(Utf8Transcoder);
        let engine = Arc::new(
            NetworkEngine::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                events.clone(),
                transcoder,
                0,
            )
            .unwrap(),
        );

        let dispatcher = PacketDispatcher::new();

        // Prepare extremely long and malformed user details & messages
        let long_username_with_whitespace = format!("  {}  ", "u".repeat(100));
        let long_hostname_with_whitespace = format!("  {}  ", "h".repeat(100));
        
        let packet = IPMsgPacket {
            version: "1".to_string(),
            packet_no: 9999,
            username: long_username_with_whitespace,
            hostname: long_hostname_with_whitespace,
            cmd: crate::protocol::IPMSG_SENDMSG,
            extra: "Malicious\x00control\x1Fchars\nand attachments".to_string(),
        };

        let result = dispatcher.dispatch(
            engine,
            "127.0.0.1".parse().unwrap(),
            127 * 256 * 256 * 256 + 1,
            packet,
        ).await;

        assert!(result.is_ok());

        // Verify status changed username and hostname were trimmed and truncated
        let peers = events.peers.lock().unwrap();
        assert_eq!(peers.len(), 1);
        let (_, sanitized_user, sanitized_host, _, _) = &peers[0];
        assert_eq!(sanitized_user.len(), 64);
        assert_eq!(sanitized_user, &"u".repeat(64));
        assert_eq!(sanitized_host.len(), 64);
        assert_eq!(sanitized_host, &"h".repeat(64));

        // Verify the received message text was stripped of \x1f and terminated at \x00
        let messages = events.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        let (_, text, user) = &messages[0];
        assert_eq!(text, "Malicious");
        assert_eq!(user, &"u".repeat(64));
    }
}
