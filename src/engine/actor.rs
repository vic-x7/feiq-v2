use crate::database::DbClient;
use crate::network::NetworkEngine;
use crate::types::{CoreCommand, CoreEvent, CancellationToken};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;

pub struct CoreEngineActor {
    cmd_rx: Receiver<CoreCommand>,
    network: Arc<NetworkEngine>,
    db: DbClient,
    event_tx: Sender<CoreEvent>,
    cancel: CancellationToken,
}

impl CoreEngineActor {
    pub fn new(
        cmd_rx: Receiver<CoreCommand>,
        network: Arc<NetworkEngine>,
        db: DbClient,
        event_tx: Sender<CoreEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            cmd_rx,
            network,
            db,
            event_tx,
            cancel,
        }
    }

    pub async fn run(mut self) {
        // Subscribe to event_tx to perform database persistence in the background
        let mut event_rx = self.event_tx.subscribe();
        let persister = crate::engine::EventPersister::new(self.db.clone());
        let cancel_persist = self.cancel.clone();
        let persist_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_persist.cancelled() => {
                        break;
                    }
                    res = event_rx.recv() => {
                        match res {
                            Ok(event) => persister.persist(&event).await,
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        // Spawn network engine receive loop inside actor context (this also starts run_tcp_server)
        let network_clone = self.network.clone();
        let cancel_net = self.cancel.clone();
        let net_handle = tokio::spawn(async move {
            network_clone.start_receive_loop(cancel_net).await;
        });

        // Discover local peers immediately
        if let Err(e) = self.network.broadcast_online().await {
            eprintln!("Warning: Failed to broadcast presence: {}", e);
        }

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    break;
                }
                cmd_opt = self.cmd_rx.recv() => {
                    match cmd_opt {
                        Some(cmd) => {
                            let res = match cmd {
                                CoreCommand::SendMessage { to_ip, content } => {
                                    self.handle_send_message(to_ip, content).await
                                }
                                CoreCommand::BroadcastPresence => {
                                    if let Err(e) = self.network.broadcast_online().await {
                                        Err(format!("Failed to broadcast presence: {}", e))
                                    } else {
                                        Ok(())
                                    }
                                }
                                CoreCommand::RegisterSharedFile { path } => {
                                    self.handle_register_shared_file(path)
                                }
                                CoreCommand::DownloadFile { peer_ip, packet_no, file_id, name, size } => {
                                    self.handle_download_file(peer_ip, packet_no, file_id, name, size).await
                                }
                                CoreCommand::UpdateIdentity { username, hostname } => {
                                    self.handle_update_identity(username, hostname).await
                                }
                                CoreCommand::ScanSubnet { subnet } => {
                                    self.handle_scan_subnet(subnet)
                                }
                                CoreCommand::ShareFile { peer_ip, path } => {
                                    self.handle_share_file(peer_ip, path).await
                                }
                                CoreCommand::SendKnock { peer_ip } => {
                                    self.handle_send_knock(peer_ip).await
                                }
                            };
                            if let Err(e) = res {
                                eprintln!("Error handling command: {}", e);
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        // When the loop exits (the actor shuts down):
        self.cancel.cancel();
        self.network.stop();
        let _ = tokio::join!(persist_handle, net_handle);
    }

    async fn send_and_persist_message(
        &self,
        peer_ip: String,
        cmd_flags: u32,
        payload: &str,
        db_content: String,
    ) -> Result<(), String> {
        let port = self.network.get_peer_port(&peer_ip);
        match self
            .network
            .send_packet_on_port(&peer_ip, port, cmd_flags, payload)
            .await
        {
            Ok(_packet_no) => {
                let msg = crate::database::MessageRecord {
                    id: None,
                    sender_ip: "0.0.0.0".to_string(), // Self
                    receiver_ip: peer_ip.clone(),
                    text_content: db_content,
                    timestamp: chrono::Utc::now().timestamp(),
                    is_read: true,
                };
                if let Err(e) = self.db.save_message(msg).await {
                    eprintln!("Warning: Failed to save message: {}", e);
                }
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to send packet to {}: {}", peer_ip, e);
                Err(format!("Failed to send packet: {}", e))
            }
        }
    }

    async fn handle_send_message(&self, to_ip: String, content: String) -> Result<(), String> {
        self.send_and_persist_message(to_ip, crate::protocol::IPMSG_SENDMSG, &content, content.clone()).await
    }

    async fn handle_send_knock(&self, peer_ip: String) -> Result<(), String> {
        self.send_and_persist_message(
            peer_ip,
            crate::protocol::IPMSG_KNOCK,
            "",
            crate::types::NUDGE_MESSAGE_CONTENT.to_string(),
        )
        .await
    }

    async fn handle_update_identity(&self, username: String, hostname: String) -> Result<(), String> {
        self.network.update_identity(username.clone(), hostname.clone());
        if let Err(e) = self
            .db
            .save_config("username".to_string(), username.clone())
            .await
        {
            eprintln!("Warning: Failed to save config username: {}", e);
        }
        if let Err(e) = self
            .db
            .save_config("hostname".to_string(), hostname.clone())
            .await
        {
            eprintln!("Warning: Failed to save config hostname: {}", e);
        }
        println!("Identity updated to {}@{}", username, hostname);
        Ok(())
    }

    fn handle_register_shared_file(&self, path: std::path::PathBuf) -> Result<(), String> {
        if path.exists() {
            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let size = path.metadata().map(|m| m.len()).unwrap_or(0);
            let packet_no = self.network.next_packet_no();
            let file_id = 0; // Standard single file ID
            self.network.register_shared_file(
                packet_no,
                file_id,
                path.clone(),
                name.clone(),
                size,
            );
            println!(
                "Registered shared file: {} ({} bytes) under packet_no: {}, file_id: {}",
                path.display(),
                size,
                packet_no,
                file_id
            );
        }
        Ok(())
    }

    fn handle_scan_subnet(&self, subnet: String) -> Result<(), String> {
        let network_inner = self.network.clone();
        let cancel_clone = self.cancel.clone();
        tokio::spawn(async move {
            if cancel_clone.is_cancelled() {
                return;
            }
            network_inner.scan_subnet(&subnet, cancel_clone).await;
            println!("\n[SCAN] Subnet scan sent for {}", subnet);
        });
        Ok(())
    }

    async fn handle_download_file(
        &self,
        peer_ip: String,
        packet_no: u32,
        file_id: u32,
        name: String,
        size: u64,
    ) -> Result<(), String> {
        let download_dir = self
            .db
            .get_config("download_dir".to_string())
            .await
            .unwrap_or(None)
            .unwrap_or_else(|| "downloads".to_string());

        let task_id = self.network.next_transfer_task_id();

        let task = crate::database::FileTaskRecord {
            id: Some(task_id),
            file_name: name.clone(),
            file_size: size as i64,
            peer_ip: peer_ip.clone(),
            is_sending: false, // Receiving
            status: crate::types::TransferStatus::Pending,
            progress: 0.0,
            timestamp: chrono::Utc::now().timestamp(),
        };
        if let Err(e) = self.db.create_file_task(task).await {
            let err_msg = format!("Failed to create file task in DB: {}", e);
            eprintln!("{}", err_msg);
            return Err(err_msg);
        }

        let network_clone = self.network.clone();
        let cancel_clone = self.cancel.clone();
        tokio::spawn(async move {
            if cancel_clone.is_cancelled() {
                return;
            }
            let cache_dir = std::path::PathBuf::from(download_dir);
            if !cache_dir.is_dir() {
                let _ = std::fs::create_dir_all(&cache_dir);
            }
            let save_path = cache_dir.join(&name);
            let download_req = crate::types::FileDownloadRequest {
                peer_ip,
                packet_no,
                file_id,
                save_path,
                file_size: size,
                task_id,
            };
            match network_clone.download_file_direct(download_req).await {
                Ok(_) => {
                    println!("\n[DOWNLOAD SUCCESS] Download complete.");
                }
                Err(e) => {
                    eprintln!("\n[DOWNLOAD FAILED] Download of {} failed: {}", name, e);
                }
            }
        });
        Ok(())
    }

    async fn handle_share_file(&self, peer_ip: String, path: std::path::PathBuf) -> Result<(), String> {
        if !path.exists() {
            let err_msg = format!("Error: File path does not exist: {}", path.display());
            eprintln!("{}", err_msg);
            return Err(err_msg);
        }

        let file_name = match path.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => {
                let err_msg = "Error: Invalid file name".to_string();
                eprintln!("{}", err_msg);
                return Err(err_msg);
            }
        };

        let metadata = match std::fs::metadata(&path) {
            Ok(meta) => meta,
            Err(e) => {
                let err_msg = format!("Error reading metadata: {}", e);
                eprintln!("{}", err_msg);
                return Err(err_msg);
            }
        };

        let file_size = metadata.len();
        let file_mtime = metadata
            .modified()
            .and_then(|t| {
                t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map_err(std::io::Error::other)
            })
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let packet_no = self.network.next_packet_no();
        let file_id = 0u32;

        // Register file in TCP registry
        self.network.register_shared_file(
            packet_no,
            file_id,
            path.clone(),
            file_name.clone(),
            file_size,
        );

        // Save to SQLite
        let task_id = self.network.next_transfer_task_id();
        let task_record = crate::database::FileTaskRecord {
            id: Some(task_id),
            file_name: file_name.clone(),
            file_size: file_size as i64,
            peer_ip: peer_ip.clone(),
            is_sending: true,
            status: crate::types::TransferStatus::Pending,
            progress: 0.0,
            timestamp: chrono::Utc::now().timestamp(),
        };

        if let Err(e) = self.db.create_file_task(task_record).await {
            eprintln!("Warning: Failed to save task record: {}", e);
        }

        // Format metadata using FileAttachment serialize and format_file_size helper
        let att = crate::protocol::FileAttachment {
            id: file_id,
            name: file_name.clone(),
            size: file_size,
            mtime: file_mtime,
            file_type: 1, // Regular file
            progress: 0.0,
            status: crate::types::TransferStatus::Pending,
        };
        let attachment_meta = crate::protocol::serialize_file_attachment(&att);
        let size_str = crate::protocol::format_file_size(file_size);

        let text_content = format!("Shared a file: {} ({})", file_name, size_str);
        let payload = format!("{}\0{}", text_content, attachment_meta);
        let cmd_flags = crate::protocol::IPMSG_SENDMSG | crate::protocol::IPMSG_FILEATTACHOPT;

        let port = self.network.get_peer_port(&peer_ip);
        let _ = self
            .network
            .send_packet_on_port(&peer_ip, port, cmd_flags, &payload)
            .await;
        println!("File metadata sent to {}!", peer_ip);
        Ok(())
    }
}
