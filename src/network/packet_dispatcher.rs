use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::AppError;
use super::{PacketIO, PacketContext, PacketHandler};
use crate::protocol::{
    IPMsgPacket, IPMSG_ANSENTRY, IPMSG_BR_ENTRY, IPMSG_BR_EXIT,
    IPMSG_FILEATTACHOPT, IPMSG_INPUTING, IPMSG_INPUT_END, IPMSG_KNOCK,
    IPMSG_RECVMSG, IPMSG_SENDCHECKOPT, IPMSG_SENDMSG,
};

pub struct BrEntryHandler;
pub struct SendMsgHandler;
pub struct RecvMsgHandler;
pub struct BrExitHandler;
pub struct KnockHandler;
pub struct InputHandler;

fn auto_download_clipboards(
    attachments: &[crate::protocol::FileAttachment],
    packet_no: u32,
    peer_ip: &str,
    packet_io: &Arc<PacketIO>,
) {
    let clipboards = crate::protocol::get_clipboard_downloads(attachments);
    for att in clipboards {
        let cache_dir = std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join("image_cache");
        if !cache_dir.exists() {
            let _ = std::fs::create_dir_all(&cache_dir);
        }
        let save_path = cache_dir.join(&att.name);

        let task_id = packet_io.next_transfer_task_id();
        let _ = packet_io.event_tx.send(crate::types::CoreEvent::TransferStarted {
            task_id,
            peer_ip: peer_ip.to_string(),
            file_name: att.name.clone(),
            file_size: att.size as i64,
            is_sending: false,
        });

        let req = crate::types::FileDownloadRequest {
            peer_ip: peer_ip.to_string(),
            packet_no,
            file_id: att.id,
            file_size: att.size,
            save_path,
            task_id,
        };

        let io_clone = packet_io.clone();
        tokio::spawn(async move {
            if let Err(e) = io_clone.download_file_direct(req).await {
                eprintln!("Auto-download of inline clipboard image failed: {}", e);
            }
        });
    }
}

#[async_trait]
impl PacketHandler for BrEntryHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let cmd_base = packet.cmd & 0xFF;
        if cmd_base == IPMSG_BR_ENTRY {
            let my_username = ctx.packet_io.get_username();
            let _ = ctx
                .packet_io
                .send_packet(&ctx.peer_ip(), IPMSG_ANSENTRY, &my_username)
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for SendMsgHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let (text_content, attachments) = crate::protocol::parse_incoming_message(packet);
        let now = chrono::Utc::now().timestamp_millis();

        let _ = ctx.packet_io.event_tx.send(crate::types::CoreEvent::MessageReceived {
            id: 0,
            sender_ip: ctx.peer_ip(),
            content: text_content,
            timestamp: now,
            username: packet.username.clone(),
        });

        if (packet.cmd & IPMSG_FILEATTACHOPT) == IPMSG_FILEATTACHOPT && !attachments.is_empty() {
            let _ = ctx.packet_io.event_tx.send(crate::types::CoreEvent::FileAttachmentsReceived {
                sender_ip: ctx.peer_ip(),
                packet_no: packet.packet_no,
                files: attachments.clone(),
            });
            auto_download_clipboards(&attachments, packet.packet_no, &ctx.peer_ip(), &ctx.packet_io);
        }

        if (packet.cmd & IPMSG_SENDCHECKOPT) == IPMSG_SENDCHECKOPT {
            let ack_extra = format!("{}", packet.packet_no);
            let _ = ctx
                .packet_io
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
            ctx.packet_io.ack_tracker.ack(ack_no).await;
        }
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for BrExitHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let _ = ctx.packet_io.event_tx.send(crate::types::CoreEvent::PeerStatusChanged {
            ip: ctx.peer_ip(),
            username: packet.username.clone(),
            hostname: packet.hostname.clone(),
            nickname: None,
            online: false,
        });
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for KnockHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let _ = ctx.packet_io.event_tx.send(crate::types::CoreEvent::WindowKnock {
            sender_ip: ctx.peer_ip(),
            username: packet.username.clone(),
        });
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for InputHandler {
    async fn handle(&self, ctx: &PacketContext, packet: &IPMsgPacket) -> Result<(), AppError> {
        let cmd_base = packet.cmd & 0xFF;
        let typing = cmd_base == IPMSG_INPUTING;
        let _ = ctx.packet_io.event_tx.send(crate::types::CoreEvent::PeerTyping {
            sender_ip: ctx.peer_ip(),
            typing,
        });
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
        packet_io: Arc<PacketIO>,
        peer_ip_addr: std::net::IpAddr,
        ip_u32: u32,
        mut packet: IPMsgPacket,
    ) -> Result<(), AppError> {
        // Sanitize username and hostname at network boundary
        packet.username = super::validation::sanitize_username(&packet.username);
        packet.hostname = super::validation::sanitize_username(&packet.hostname);

        let cmd_base = packet.cmd & 0xFF;

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
            if cmd_base == IPMSG_SENDMSG && (packet.cmd & IPMSG_SENDCHECKOPT) == IPMSG_SENDCHECKOPT
            {
                let ack_extra = format!("{}", packet.packet_no);
                let peer_ip = peer_ip_addr.to_string();
                let _ = packet_io
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
            let _ = packet_io.event_tx.send(crate::types::CoreEvent::PeerStatusChanged {
                ip: peer_ip.clone(),
                username: packet.username.clone(),
                hostname: packet.hostname.clone(),
                nickname: nickname.clone(),
                online: true,
            });
        }

        let ctx = PacketContext {
            peer_ip_addr,
            packet_io,
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

    #[tokio::test]
    async fn test_send_msg_handler_no_ack() {
        let (event_tx, mut event_rx) = tokio::sync::broadcast::channel(128);

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let peer_directory = crate::network::PeerDirectory::new();
        let file_registry = crate::network::FileRegistry::new();
        let ack_tracker = crate::network::AckTracker::new();
        let packet_io = Arc::new(
            PacketIO::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                event_tx,
                0,
                peer_directory,
                file_registry,
                ack_tracker,
            )
        );

        let context = PacketContext {
            peer_ip_addr: "127.0.0.1".parse().unwrap(),
            packet_io,
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

        let event = event_rx.try_recv().unwrap();
        if let crate::types::CoreEvent::MessageReceived { sender_ip, content, username, .. } = event {
            assert_eq!(sender_ip, "127.0.0.1");
            assert_eq!(content, "Hello from Bob!");
            assert_eq!(username, "bob");
        } else {
            panic!("Expected MessageReceived event, got {:?}", event);
        }
    }

    #[tokio::test]
    async fn test_send_msg_handler_with_ack() {
        let (event_tx, mut event_rx) = tokio::sync::broadcast::channel(128);

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let peer_directory = crate::network::PeerDirectory::new();
        let file_registry = crate::network::FileRegistry::new();
        let ack_tracker = crate::network::AckTracker::new();
        let packet_io = Arc::new(
            PacketIO::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                event_tx,
                0,
                peer_directory,
                file_registry,
                ack_tracker,
            )
        );

        let context = PacketContext {
            peer_ip_addr: "127.0.0.1".parse().unwrap(),
            packet_io,
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

        let event = event_rx.try_recv().unwrap();
        if let crate::types::CoreEvent::MessageReceived { content, .. } = event {
            assert_eq!(content, "ACK message");
        } else {
            panic!("Expected MessageReceived event, got {:?}", event);
        }
    }

    #[tokio::test]
    async fn test_dispatch_sanitizes_untrusted_data() {
        let (event_tx, mut event_rx) = tokio::sync::broadcast::channel(128);

        let transport = Arc::new(crate::network::FakeTransport::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
        let peer_directory = crate::network::PeerDirectory::new();
        let file_registry = crate::network::FileRegistry::new();
        let ack_tracker = crate::network::AckTracker::new();
        let packet_io = Arc::new(
            PacketIO::new(
                "alice".to_string(),
                "alice-pc".to_string(),
                transport,
                event_tx,
                0,
                peer_directory,
                file_registry,
                ack_tracker,
            )
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
            packet_io,
            "127.0.0.1".parse().unwrap(),
            127 * 256 * 256 * 256 + 1,
            packet,
        ).await;

        assert!(result.is_ok());

        // Verify status changed username and hostname were trimmed and truncated
        let event1 = event_rx.try_recv().unwrap();
        if let crate::types::CoreEvent::PeerStatusChanged { username, hostname, .. } = event1 {
            assert_eq!(username.len(), 64);
            assert_eq!(username, "u".repeat(64));
            assert_eq!(hostname.len(), 64);
            assert_eq!(hostname, "h".repeat(64));
        } else {
            panic!("Expected PeerStatusChanged event, got {:?}", event1);
        }

        // Verify the received message text was stripped of \x1f and terminated at \x00
        let event2 = event_rx.try_recv().unwrap();
        if let crate::types::CoreEvent::MessageReceived { content, username, .. } = event2 {
            assert_eq!(content, "Malicious");
            assert_eq!(username, "u".repeat(64));
        } else {
            panic!("Expected MessageReceived event, got {:?}", event2);
        }
    }
}
