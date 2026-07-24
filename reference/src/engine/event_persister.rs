use crate::database::{DbClient, FileTaskRecord, MessageRecord, PeerRecord};
use crate::types::{CoreEvent, TransferStatus, LOCAL_USER_IDENTIFIER};

pub struct EventPersister {
    db: DbClient,
}

impl EventPersister {
    pub fn new(db: DbClient) -> Self {
        Self { db }
    }

    async fn save_received_message(&self, sender_ip: String, text_content: String, timestamp: i64) {
        let msg_record = MessageRecord {
            id: None,
            sender_ip,
            receiver_ip: LOCAL_USER_IDENTIFIER.to_string(),
            text_content,
            timestamp,
            is_read: false,
        };
        if let Err(e) = self.db.save_message(msg_record).await {
            eprintln!("Warning: Failed to save message: {}", e);
        }
    }

    pub async fn persist(&self, event: &CoreEvent) {
        match event {
            CoreEvent::PeerStatusChanged {
                ip,
                username,
                hostname,
                nickname,
                online,
            } => {
                if *online {
                    let now = chrono::Utc::now().timestamp_millis();
                    let peer_record = PeerRecord {
                        ip: ip.clone(),
                        username: username.clone(),
                        hostname: hostname.clone(),
                        nickname: nickname.clone(),
                        avatar_id: None,
                        last_seen: now,
                    };
                    if let Err(e) = self.db.save_peer(peer_record).await {
                        eprintln!("Warning: Failed to save peer: {}", e);
                    }
                }
            }
            CoreEvent::MessageReceived {
                sender_ip,
                content,
                timestamp,
                ..
            } => {
                self.save_received_message(sender_ip.clone(), content.clone(), *timestamp)
                    .await;
            }
            CoreEvent::WindowKnock { sender_ip, .. } => {
                let now = chrono::Utc::now().timestamp_millis();
                self.save_received_message(
                    sender_ip.clone(),
                    "* Received a window knock! 📳 *".to_string(),
                    now,
                )
                .await;
            }
            CoreEvent::TransferProgress {
                task_id,
                progress,
                status,
            } => {
                if let Err(e) = self
                    .db
                    .update_file_task_progress(*task_id, *progress, status.clone())
                    .await
                {
                    eprintln!("Warning: Failed to update file task progress: {}", e);
                }
            }
            CoreEvent::TransferStarted {
                task_id,
                peer_ip,
                file_name,
                file_size,
                is_sending,
            } => {
                let task_record = FileTaskRecord {
                    id: Some(*task_id),
                    file_name: file_name.clone(),
                    file_size: *file_size,
                    peer_ip: peer_ip.clone(),
                    is_sending: *is_sending,
                    status: TransferStatus::Transferring,
                    progress: 0.0,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                };
                if let Err(e) = self.db.create_file_task(task_record).await {
                    eprintln!("Warning: Failed to create file task: {}", e);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{start_db_actor, DatabaseManager};
    use std::path::PathBuf;

    async fn temp_db_client() -> (DbClient, tokio::task::JoinHandle<()>) {
        let manager = DatabaseManager::new(PathBuf::from(":memory:")).unwrap();
        let (client, handle) = start_db_actor(manager);
        (client, handle)
    }

    #[tokio::test]
    async fn test_persist_peer_status_changed_online() {
        let (db, _handle) = temp_db_client().await;
        let persister = EventPersister::new(db.clone());

        let event = CoreEvent::PeerStatusChanged {
            ip: "192.168.1.5".to_string(),
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            nickname: Some("Bobby".to_string()),
            online: true,
        };

        persister.persist(&event).await;

        let peers = db.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].ip, "192.168.1.5");
        assert_eq!(peers[0].username, "bob");
        assert_eq!(peers[0].hostname, "bob-pc");
        assert_eq!(peers[0].nickname, Some("Bobby".to_string()));
    }

    #[tokio::test]
    async fn test_persist_peer_status_changed_offline() {
        let (db, _handle) = temp_db_client().await;
        let persister = EventPersister::new(db.clone());

        let event = CoreEvent::PeerStatusChanged {
            ip: "192.168.1.5".to_string(),
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            nickname: Some("Bobby".to_string()),
            online: false,
        };

        persister.persist(&event).await;

        let peers = db.get_peers().await.unwrap();
        assert_eq!(peers.len(), 0);
    }

    #[tokio::test]
    async fn test_persist_message_received() {
        let (db, _handle) = temp_db_client().await;
        let persister = EventPersister::new(db.clone());

        let event = CoreEvent::MessageReceived {
            id: 0,
            sender_ip: "192.168.1.5".to_string(),
            content: "hello world".to_string(),
            timestamp: 123456789,
            username: "bob".to_string(),
        };

        persister.persist(&event).await;

        let messages = db
            .get_chat_history("192.168.1.5".to_string(), 10, 0)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].sender_ip, "192.168.1.5");
        assert_eq!(messages[0].receiver_ip, LOCAL_USER_IDENTIFIER);
        assert_eq!(messages[0].text_content, "hello world");
        assert_eq!(messages[0].timestamp, 123456789);
        assert!(!messages[0].is_read);
    }

    #[tokio::test]
    async fn test_persist_window_knock() {
        let (db, _handle) = temp_db_client().await;
        let persister = EventPersister::new(db.clone());

        let event = CoreEvent::WindowKnock {
            sender_ip: "192.168.1.5".to_string(),
            username: "bob".to_string(),
        };

        persister.persist(&event).await;

        let messages = db
            .get_chat_history("192.168.1.5".to_string(), 10, 0)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].sender_ip, "192.168.1.5");
        assert_eq!(messages[0].text_content, "* Received a window knock! 📳 *");
        assert!(!messages[0].is_read);
    }

    #[tokio::test]
    async fn test_persist_transfer_lifecycle() {
        let (db, _handle) = temp_db_client().await;
        let persister = EventPersister::new(db.clone());

        // 1. Start transfer
        let start_event = CoreEvent::TransferStarted {
            task_id: 42,
            peer_ip: "192.168.1.5".to_string(),
            file_name: "test.txt".to_string(),
            file_size: 1024,
            is_sending: false,
        };

        persister.persist(&start_event).await;

        let count = db.get_total_file_transfers_count().await.unwrap();
        assert_eq!(count, 1);

        let status_res = db.get_file_task_status(42).await.unwrap();
        assert!(status_res.is_some());
        let (status, progress) = status_res.unwrap();
        assert_eq!(status, TransferStatus::Transferring);
        assert_eq!(progress, 0.0);

        // 2. Progress update
        let progress_event = CoreEvent::TransferProgress {
            task_id: 42,
            progress: 0.5,
            status: TransferStatus::Transferring,
        };

        persister.persist(&progress_event).await;

        let status_res = db.get_file_task_status(42).await.unwrap();
        assert!(status_res.is_some());
        let (status, progress) = status_res.unwrap();
        assert_eq!(status, TransferStatus::Transferring);
        assert_eq!(progress, 0.5);

        // 3. Complete update
        let complete_event = CoreEvent::TransferProgress {
            task_id: 42,
            progress: 1.0,
            status: TransferStatus::Completed,
        };

        persister.persist(&complete_event).await;

        let status_res = db.get_file_task_status(42).await.unwrap();
        assert!(status_res.is_some());
        let (status, progress) = status_res.unwrap();
        assert_eq!(status, TransferStatus::Completed);
        assert_eq!(progress, 1.0);
    }
}
