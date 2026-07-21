use async_trait::async_trait;
use feiq_v2::database::DatabaseManager;
use feiq_v2::engine::handlers::{
    CommandHandler, HandlerContext, SendMessageHandler, UpdateIdentityHandler, SendKnockHandler,
};
use feiq_v2::network::NetworkEngineTrait;
use feiq_v2::error::AppError;
use feiq_v2::types::{CoreCommand, FileDownloadRequest, CancellationToken};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::channel as broadcast_channel;
use tokio::sync::mpsc::channel as mpsc_channel;

struct MockNetworkEngine {
    pub peer_ports: Mutex<HashMap<String, u16>>,
    pub sent_packets: Mutex<Vec<(String, u16, u32, String)>>,
    pub packet_counter: std::sync::atomic::AtomicU32,
    pub registered_files: Mutex<Vec<(u32, u32, PathBuf, String, u64)>>,
    pub current_username: Mutex<String>,
    pub current_hostname: Mutex<String>,
    pub task_counter: std::sync::atomic::AtomicI64,
}

impl MockNetworkEngine {
    fn new() -> Self {
        Self {
            peer_ports: Mutex::new(HashMap::new()),
            sent_packets: Mutex::new(Vec::new()),
            packet_counter: std::sync::atomic::AtomicU32::new(100),
            registered_files: Mutex::new(Vec::new()),
            current_username: Mutex::new("alice".to_string()),
            current_hostname: Mutex::new("alice-pc".to_string()),
            task_counter: std::sync::atomic::AtomicI64::new(1),
        }
    }
}

#[async_trait]
impl NetworkEngineTrait for MockNetworkEngine {
    fn get_peer_port(&self, ip: &str) -> u16 {
        *self.peer_ports.lock().unwrap().get(ip).unwrap_or(&2425)
    }

    async fn send_packet_on_port(
        &self,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<u32, AppError> {
        let packet_no = self.next_packet_no();
        self.sent_packets
            .lock()
            .unwrap()
            .push((to_ip.to_string(), port, cmd, extra.to_string()));
        Ok(packet_no)
    }

    async fn broadcast_online(&self) -> Result<(), AppError> {
        let _ = self
            .send_packet_on_port("255.255.255.255", 2425, 1, "alice")
            .await;
        Ok(())
    }

    fn next_packet_no(&self) -> u32 {
        self.packet_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn register_shared_file(
        &self,
        packet_no: u32,
        file_id: u32,
        path: PathBuf,
        name: String,
        size: u64,
    ) {
        self.registered_files
            .lock()
            .unwrap()
            .push((packet_no, file_id, path, name, size));
    }

    async fn download_file_direct(&self, _req: FileDownloadRequest) -> Result<(), AppError> {
        Ok(())
    }

    fn update_identity(&self, username: String, hostname: String) {
        *self.current_username.lock().unwrap() = username;
        *self.current_hostname.lock().unwrap() = hostname;
    }

    async fn scan_subnet(self: Arc<Self>, subnet_prefix: &str, _cancel: CancellationToken) {
        let _ = self
            .send_packet_on_port(subnet_prefix, 2425, 1, "scan")
            .await;
    }

    fn next_transfer_task_id(&self) -> i64 {
        self.task_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test]
async fn test_send_message_handler() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_handler_db_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);

    // Inject our beautiful mock network engine instead of binding actual system ports!
    let mock_network = Arc::new(MockNetworkEngine::new());
    mock_network
        .peer_ports
        .lock()
        .unwrap()
        .insert("192.168.1.100".to_string(), 2425);

    let (cmd_tx, _cmd_rx) = mpsc_channel(64);

    let context = HandlerContext {
        network: mock_network.clone(),
        db: db_client.clone(),
        event_tx: event_tx.clone(),
        cmd_tx: cmd_tx.clone(),
        cancel: CancellationToken::new(),
    };

    // 1. Invoke SendMessageHandler
    let send_handler = SendMessageHandler;
    let cmd = CoreCommand::SendMessage {
        to_ip: "192.168.1.100".to_string(),
        content: "Testing SendMessageHandler with Mocks!".to_string(),
    };

    let result = send_handler.handle(cmd, &context).await;
    assert!(result.is_ok());

    // 2. Validate message was saved in database
    let history = db_client
        .get_chat_history("192.168.1.100".to_string(), 100, 0)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].text_content,
        "Testing SendMessageHandler with Mocks!"
    );
    assert_eq!(history[0].receiver_ip, "192.168.1.100");
    assert_eq!(history[0].sender_ip, "0.0.0.0");
    assert_eq!(history[0].is_read, true);

    // 3. Assert correct network packet was dispatched (mock check!)
    let packets = mock_network.sent_packets.lock().unwrap();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].0, "192.168.1.100");
    assert_eq!(packets[0].1, 2425);
    assert_eq!(packets[0].2, feiq_v2::protocol::IPMSG_SENDMSG);
    assert_eq!(packets[0].3, "Testing SendMessageHandler with Mocks!");
}

#[tokio::test]
async fn test_update_identity_handler() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_identity_db_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);

    let mock_network = Arc::new(MockNetworkEngine::new());
    let (cmd_tx, _cmd_rx) = mpsc_channel(64);

    let context = HandlerContext {
        network: mock_network.clone(),
        db: db_client.clone(),
        event_tx: event_tx.clone(),
        cmd_tx: cmd_tx.clone(),
        cancel: CancellationToken::new(),
    };

    // 1. Invoke UpdateIdentityHandler
    let identity_handler = UpdateIdentityHandler;
    let cmd = CoreCommand::UpdateIdentity {
        username: "new_alice".to_string(),
        hostname: "new_alice-pc".to_string(),
    };

    let result = identity_handler.handle(cmd, &context).await;
    assert!(result.is_ok());

    // 2. Validate configuration updates in database
    let saved_username = db_client.get_config("username".to_string()).await.unwrap();
    let saved_hostname = db_client.get_config("hostname".to_string()).await.unwrap();
    assert_eq!(saved_username, Some("new_alice".to_string()));
    assert_eq!(saved_hostname, Some("new_alice-pc".to_string()));

    // 3. Assert mock identity was updated
    assert_eq!(*mock_network.current_username.lock().unwrap(), "new_alice");
    assert_eq!(
        *mock_network.current_hostname.lock().unwrap(),
        "new_alice-pc"
    );
}

#[tokio::test]
async fn test_send_knock_handler() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_knock_db_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);

    let mock_network = Arc::new(MockNetworkEngine::new());
    mock_network
        .peer_ports
        .lock()
        .unwrap()
        .insert("192.168.1.100".to_string(), 2425);

    let (cmd_tx, _cmd_rx) = mpsc_channel(64);

    let context = HandlerContext {
        network: mock_network.clone(),
        db: db_client.clone(),
        event_tx: event_tx.clone(),
        cmd_tx: cmd_tx.clone(),
        cancel: CancellationToken::new(),
    };

    // 1. Invoke SendKnockHandler
    let knock_handler = SendKnockHandler;
    let cmd = CoreCommand::SendKnock {
        peer_ip: "192.168.1.100".to_string(),
    };

    let result = knock_handler.handle(cmd, &context).await;
    assert!(result.is_ok());

    // 2. Validate message was saved in database
    let history = db_client
        .get_chat_history("192.168.1.100".to_string(), 100, 0)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].text_content, feiq_v2::types::NUDGE_MESSAGE_CONTENT);
    assert_eq!(history[0].receiver_ip, "192.168.1.100");
    assert_eq!(history[0].sender_ip, "0.0.0.0");
    assert_eq!(history[0].is_read, true);

    // 3. Assert correct network packet was dispatched
    let packets = mock_network.sent_packets.lock().unwrap();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].0, "192.168.1.100");
    assert_eq!(packets[0].1, 2425);
    assert_eq!(packets[0].2, feiq_v2::protocol::IPMSG_KNOCK);
    assert_eq!(packets[0].3, "");
}
