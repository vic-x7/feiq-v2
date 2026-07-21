use feiq_v2::database::DatabaseManager;
use feiq_v2::engine::CoreEngineActor;
use feiq_v2::network::{FakeTransport, PacketIO, PacketDispatcher, PeerDirectory, FileRegistry, AckTracker};
use feiq_v2::types::{CoreCommand, CancellationToken};
use std::sync::Arc;
use tokio::sync::broadcast::channel as broadcast_channel;
use tokio::sync::mpsc::channel as mpsc_channel;

#[tokio::test]
async fn test_actor_send_message() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_actor_db_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);
    let (cmd_tx, cmd_rx) = mpsc_channel(64);

    let local_addr = "127.0.0.1:2425".parse().unwrap();
    let transport = Arc::new(FakeTransport::new(local_addr));
    let peer_directory = PeerDirectory::new();
    let file_registry = FileRegistry::new();
    let ack_tracker = AckTracker::new();
    let packet_dispatcher = Arc::new(PacketDispatcher::new());
    let packet_io = Arc::new(
        PacketIO::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport.clone(),
            event_tx.clone(),
            0,
            peer_directory,
            file_registry,
            ack_tracker,
        )
    );

    let cancel = CancellationToken::new();
    let actor = CoreEngineActor::new(
        cmd_rx,
        packet_io,
        packet_dispatcher,
        db_client.clone(),
        event_tx,
        cancel.clone(),
    );

    // Spawn actor run loop
    let actor_handle = tokio::spawn(async move {
        actor.run().await;
    });

    // Clear initial broadcast presence packets to isolate the send message packet
    transport.clear_sent_udp();

    // 1. Send Message Command
    cmd_tx
        .send(CoreCommand::SendMessage {
            to_ip: "192.168.1.100".to_string(),
            content: "Testing CoreEngineActor directly!".to_string(),
        })
        .await
        .unwrap();

    // Yield control to let the actor loop process
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 2. Validate message was saved in database
    let history = db_client
        .get_chat_history("192.168.1.100".to_string(), 100, 0)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].text_content, "Testing CoreEngineActor directly!");
    assert_eq!(history[0].receiver_ip, "192.168.1.100");
    assert_eq!(history[0].sender_ip, "0.0.0.0");
    assert_eq!(history[0].is_read, true);

    // 3. Assert correct network packet was dispatched (captured in FakeTransport!)
    let sent = transport.get_sent_udp();
    let msg_packets: Vec<_> = sent.iter().filter(|p| p.0 == "192.168.1.100").collect();
    assert_eq!(msg_packets.len(), 1);
    assert_eq!(msg_packets[0].1, 2425);
    let payload_str = String::from_utf8_lossy(&msg_packets[0].2);
    assert!(payload_str.contains("Testing CoreEngineActor directly!"));

    // Clean up
    cancel.cancel();
    let _ = actor_handle.await;
}

#[tokio::test]
async fn test_actor_update_identity() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_actor_identity_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);
    let (cmd_tx, cmd_rx) = mpsc_channel(64);

    let local_addr = "127.0.0.1:2425".parse().unwrap();
    let transport = Arc::new(FakeTransport::new(local_addr));
    let peer_directory = PeerDirectory::new();
    let file_registry = FileRegistry::new();
    let ack_tracker = AckTracker::new();
    let packet_dispatcher = Arc::new(PacketDispatcher::new());
    let packet_io = Arc::new(
        PacketIO::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport.clone(),
            event_tx.clone(),
            0,
            peer_directory,
            file_registry,
            ack_tracker,
        )
    );

    let cancel = CancellationToken::new();
    let actor = CoreEngineActor::new(
        cmd_rx,
        packet_io,
        packet_dispatcher,
        db_client.clone(),
        event_tx,
        cancel.clone(),
    );

    // Spawn actor run loop
    let actor_handle = tokio::spawn(async move {
        actor.run().await;
    });

    // 1. Update Identity Command
    cmd_tx
        .send(CoreCommand::UpdateIdentity {
            username: "new_alice".to_string(),
            hostname: "new_alice-pc".to_string(),
        })
        .await
        .unwrap();

    // Yield control to let the actor loop process
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 2. Validate configuration updates in database
    let saved_username = db_client.get_config("username".to_string()).await.unwrap();
    let saved_hostname = db_client.get_config("hostname".to_string()).await.unwrap();
    assert_eq!(saved_username, Some("new_alice".to_string()));
    assert_eq!(saved_hostname, Some("new_alice-pc".to_string()));

    // Clean up
    cancel.cancel();
    let _ = actor_handle.await;
}

#[tokio::test]
async fn test_actor_send_knock() {
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_path = temp_dir.join(format!("test_actor_knock_{}.db", unique_id));

    let db = DatabaseManager::new(db_path).unwrap();
    let db_client = feiq_v2::database::DbClient::new(db);

    let (event_tx, _event_rx) = broadcast_channel(128);
    let (cmd_tx, cmd_rx) = mpsc_channel(64);

    let local_addr = "127.0.0.1:2425".parse().unwrap();
    let transport = Arc::new(FakeTransport::new(local_addr));
    let peer_directory = PeerDirectory::new();
    let file_registry = FileRegistry::new();
    let ack_tracker = AckTracker::new();
    let packet_dispatcher = Arc::new(PacketDispatcher::new());
    let packet_io = Arc::new(
        PacketIO::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport.clone(),
            event_tx.clone(),
            0,
            peer_directory,
            file_registry,
            ack_tracker,
        )
    );

    let cancel = CancellationToken::new();
    let actor = CoreEngineActor::new(
        cmd_rx,
        packet_io,
        packet_dispatcher,
        db_client.clone(),
        event_tx,
        cancel.clone(),
    );

    // Spawn actor run loop
    let actor_handle = tokio::spawn(async move {
        actor.run().await;
    });

    // Clear initial broadcast presence packets to isolate the send knock packet
    transport.clear_sent_udp();

    // 1. Send Knock Command
    cmd_tx
        .send(CoreCommand::SendKnock {
            peer_ip: "192.168.1.100".to_string(),
        })
        .await
        .unwrap();

    // Yield control to let the actor loop process
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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
    let sent = transport.get_sent_udp();
    let msg_packets: Vec<_> = sent.iter().filter(|p| p.0 == "192.168.1.100").collect();
    assert_eq!(msg_packets.len(), 1);
    assert_eq!(msg_packets[0].1, 2425);
    let payload_str = String::from_utf8_lossy(&msg_packets[0].2);
    assert!(payload_str.contains(":209:")); // command is IPMSG_KNOCK (209)

    // Clean up
    cancel.cancel();
    let _ = actor_handle.await;
}
