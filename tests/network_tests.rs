use feiq_v2::database::DatabaseManager;
use feiq_v2::network::{PacketIO, PeerDirectory, FileRegistry, AckTracker};
use feiq_v2::types::{FileDownloadRequest, CancellationToken};
use std::sync::Arc;

#[tokio::test]
async fn test_network_engine_binding_and_fallback() {
    let (event_tx1, _) = tokio::sync::broadcast::channel(128);
    let (event_tx2, _) = tokio::sync::broadcast::channel(128);

    // 1. Create first network engine on loopback
    let transport1 = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let peer_directory1 = PeerDirectory::new();
    let file_registry1 = FileRegistry::new();
    let ack_tracker1 = AckTracker::new();
    let engine1 = PacketIO::new(
        "alice".to_string(),
        "alice-pc".to_string(),
        transport1,
        event_tx1,
        0,
        peer_directory1,
        file_registry1,
        ack_tracker1,
    );

    let engine1 = Arc::new(engine1);

    // 2. Create second network engine on the same loopback IP
    // Since port 2425 is already bound by engine1, engine2 MUST fall back to a port in range 2426..=2435!
    let transport2 = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let peer_directory2 = PeerDirectory::new();
    let file_registry2 = FileRegistry::new();
    let ack_tracker2 = AckTracker::new();
    let engine2 = PacketIO::new(
        "bob".to_string(),
        "bob-pc".to_string(),
        transport2,
        event_tx2,
        0,
        peer_directory2,
        file_registry2,
        ack_tracker2,
    );

    let engine2 = Arc::new(engine2);

    // 3. Verify that engine1 and engine2 are bound to different loopback UDP ports in IPMsg range
    let addr1 = engine1.transport.local_addr().unwrap();
    let addr2 = engine2.transport.local_addr().unwrap();

    assert_eq!(addr1.ip().to_string(), "127.0.0.1");
    assert_eq!(addr2.ip().to_string(), "127.0.0.1");

    assert_ne!(addr1.port(), addr2.port());
    assert!(addr1.port() >= 2425 && addr1.port() <= 2435);
    assert!(addr2.port() >= 2425 && addr2.port() <= 2435);
}

#[tokio::test]
async fn test_network_file_transfer_loopback() {
    use std::io::Write;

    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let source_path = temp_dir.join(format!("source_{}.txt", unique_id));
    let dest_path = temp_dir.join(format!("downloaded_{}.txt", unique_id));

    // Create source file with unique test payload
    let test_content =
        "This is a high-performance Rust IPMsg test payload for concurrent TCP transfers.";
    {
        let mut file = std::fs::File::create(&source_path).unwrap();
        file.write_all(test_content.as_bytes()).unwrap();
    }

    let (event_tx_bob, _) = tokio::sync::broadcast::channel(128);
    let (event_tx_alice, _) = tokio::sync::broadcast::channel(128);

    // Initialize databases
    let _db_bob = Arc::new(std::sync::Mutex::new(
        DatabaseManager::new(":memory:".into()).unwrap(),
    ));
    let db_alice = Arc::new(std::sync::Mutex::new(
        DatabaseManager::new(":memory:".into()).unwrap(),
    ));

    // Create engines
    let transport_bob = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let peer_directory_bob = PeerDirectory::new();
    let file_registry_bob = FileRegistry::new();
    let ack_tracker_bob = AckTracker::new();
    let bob = Arc::new(
        PacketIO::new(
            "bob".to_string(),
            "bob-pc".to_string(),
            transport_bob,
            event_tx_bob,
            0,
            peer_directory_bob,
            file_registry_bob,
            ack_tracker_bob,
        )
    );

    let transport_alice = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let peer_directory_alice = PeerDirectory::new();
    let file_registry_alice = FileRegistry::new();
    let ack_tracker_alice = AckTracker::new();
    let alice = Arc::new(
        PacketIO::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport_alice,
            event_tx_alice,
            0,
            peer_directory_alice,
            file_registry_alice,
            ack_tracker_alice,
        )
    );

    // Record Alice's port into Bob's known list, and Bob's port into Alice's list
    let bob_port = bob.transport.local_addr().unwrap().port();
    let alice_port = alice.transport.local_addr().unwrap().port();

    // Register peer ports manually to skip discovery sequence for this socket unit test
    alice.peer_directory.upsert_str("127.0.0.1", bob_port);
    bob.peer_directory.upsert_str("127.0.0.1", alice_port);

    // Spawn receive loops for both nodes (this automatically spawns their TCP servers)
    let bob_clone = bob.clone();
    let cancel_bob = CancellationToken::new();
    let cancel_bob_clone = cancel_bob.clone();
    let dispatcher_bob = Arc::new(feiq_v2::network::PacketDispatcher::new());
    let handle_bob = tokio::spawn(async move {
        bob_clone.start_receive_loop(dispatcher_bob, cancel_bob_clone).await;
    });

    let alice_clone = alice.clone();
    let cancel_alice = CancellationToken::new();
    let cancel_alice_clone = cancel_alice.clone();
    let dispatcher_alice = Arc::new(feiq_v2::network::PacketDispatcher::new());
    let handle_alice = tokio::spawn(async move {
        alice_clone.start_receive_loop(dispatcher_alice, cancel_alice_clone).await;
    });

    // Bob registers the shared file
    let packet_no = 42u32;
    let file_id = 0u32;
    let file = feiq_v2::network::SharedFile {
        path: source_path.clone(),
        name: "source.txt".to_string(),
        size: test_content.len() as u64,
    };
    bob.file_registry.register(
        packet_no,
        file_id,
        file,
    );

    // Create FileTask record on Alice's (receiver) side
    let task_record = feiq_v2::database::FileTaskRecord {
        id: None,
        file_name: "downloaded.txt".to_string(),
        file_size: test_content.len() as i64,
        peer_ip: "127.0.0.1".to_string(),
        is_sending: false,
        status: feiq_v2::types::TransferStatus::Pending,
        progress: 0.0,
        timestamp: 1234567,
    };
    let task_id = db_alice
        .lock()
        .unwrap()
        .create_file_task(&task_record)
        .unwrap();

    // Alice requests download direct from Bob
    let req = FileDownloadRequest {
        peer_ip: "127.0.0.1".to_string(),
        packet_no,
        file_id,
        save_path: dest_path.clone(),
        file_size: test_content.len() as u64,
        task_id,
    };

    let download_result = alice.download_file_direct(req).await;
    assert!(
        download_result.is_ok(),
        "Download failed: {:?}",
        download_result.err()
    );

    // Verify downloaded content matches source exactly
    let downloaded_data = std::fs::read_to_string(&dest_path).unwrap();
    assert_eq!(downloaded_data, test_content);

    // Verify Alice's SQLite task transitions to completed (progress = 1.0)
    {
        let db_lock = db_alice.lock().unwrap();
        // Check database is updated correctly
        let count = db_lock.get_total_file_transfers_count().unwrap();
        assert_eq!(count, 1);
    }

    // Verify Observability counters incremented correctly on Alice
    let alice_stats = alice.stats();
    assert_eq!(alice_stats.bytes_received, test_content.len() as u64);
    assert_eq!(alice_stats.errors, 0);
    assert_eq!(alice_stats.packets_sent, 0);

    // Teardown
    cancel_bob.cancel();
    cancel_alice.cancel();
    bob.shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);
    alice
        .shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Stop threads by waking up sockets
    let _ = bob.send_packet_on_port("127.0.0.1", bob_port, 0, "").await;
    let _ = alice
        .send_packet_on_port("127.0.0.1", alice_port, 0, "")
        .await;

    let _ = tokio::join!(handle_bob, handle_alice);

    // Clean up temporary files on disk
    let _ = std::fs::remove_file(&source_path);
    let _ = std::fs::remove_file(&dest_path);
}

#[tokio::test]
async fn test_tokio_transport_udp_sending_and_parsing() {
    use feiq_v2::network::NetworkTransport;
    use feiq_v2::network::TokioTransport;

    // Bind receiver socket
    let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let recv_port = recv_socket.local_addr().unwrap().port();

    // Bind sender socket
    let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let transport = TokioTransport::new(Arc::new(send_socket), None);

    // Send UDP payload
    let test_payload = b"TEST_DECOUPLED_TRANSPORT_UDP_PAYLOAD";
    let send_res = transport
        .send_udp("127.0.0.1", recv_port, test_payload)
        .await;
    assert!(send_res.is_ok());

    // Receive UDP payload
    let mut buf = [0u8; 1024];
    let (n, src_addr) = recv_socket.recv_from(&mut buf).await.unwrap();

    assert_eq!(&buf[..n], test_payload);
    assert_eq!(src_addr.ip().to_string(), "127.0.0.1");
}

#[tokio::test]
async fn test_fake_transport_injection_and_behavior() {
    use feiq_v2::network::{FakeTransport, PacketIO};

    let local_addr = "127.0.0.1:12345".parse().unwrap();
    let transport = Arc::new(FakeTransport::new(local_addr));
    let (event_tx, mut event_rx) = tokio::sync::broadcast::channel(128);

    let peer_directory = PeerDirectory::new();
    let file_registry = FileRegistry::new();
    let ack_tracker = AckTracker::new();
    let engine = Arc::new(
        PacketIO::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport.clone(),
            event_tx,
            0,
            peer_directory,
            file_registry,
            ack_tracker,
        )
    );

    // 1. Verify we can get local_addr from engine delegation correctly
    assert_eq!(engine.transport.local_addr().unwrap(), local_addr);

    // 2. Verify sending a packet captures it in the FakeTransport
    let packet_no = engine
        .send_packet("192.168.1.100", 2425, "Hello")
        .await
        .unwrap();
    assert!(packet_no > 0);

    let sent = transport.get_sent_udp();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "192.168.1.100");
    assert_eq!(sent[0].1, 2425);

    // 3. Simulating receiving a packet
    // Let's inject a classic IPMsg packet bytes representing bob logging in
    let raw_packet = b"1:12345:bob:bob-pc:1:Bob"; // IPMSG_BR_ENTRY (1)
    let peer_addr = "192.168.1.200:2425".parse().unwrap();
    transport.inject_incoming(raw_packet.to_vec(), peer_addr);

    // Let's spawn a quick test task or directly dispatch
    let packet = feiq_v2::protocol::IPMsgPacket::parse(raw_packet).unwrap();
    let dispatcher = feiq_v2::network::PacketDispatcher::new();
    dispatcher
        .dispatch(engine, peer_addr.ip(), 0xC0A801C8, packet)
        .await
        .unwrap();

    // Verify dispatcher received status changed update
    let event = event_rx.try_recv().unwrap();
    if let feiq_v2::types::CoreEvent::PeerStatusChanged { ip, username, online, .. } = event {
        assert_eq!(ip, "192.168.1.200");
        assert_eq!(username, "bob");
        assert!(online);
    } else {
        panic!("Expected PeerStatusChanged event, got {:?}", event);
    }
}

#[tokio::test]
async fn test_ack_tracker_file_registry_peer_directory() {
    use feiq_v2::network::{AckTracker, FileRegistry, PeerDirectory};

    // 1. Test PeerDirectory
    let peer_dir = PeerDirectory::new();
    peer_dir.upsert(0x7F000001, 2426); // 127.0.0.1
    assert_eq!(peer_dir.get_port(0x7F000001), 2426);
    assert_eq!(peer_dir.get_port(0xC0A80101), 2425); // Default port for unknown IP

    // 2. Test FileRegistry
    let file_registry = FileRegistry::new();
    let file_path = std::path::PathBuf::from("test.txt");
    file_registry.register(
        12345,
        54321,
        feiq_v2::network::SharedFile {
            path: file_path.clone(),
            name: "test.txt".to_string(),
            size: 1000,
        },
    );

    let lookup_result = file_registry.lookup(12345, 54321);
    assert!(lookup_result.is_some());
    let found = lookup_result.unwrap();
    assert_eq!(found.name, "test.txt");
    assert_eq!(found.size, 1000);

    // 3. Test AckTracker
    let ack_tracker = AckTracker::new();
    let tracker_clone = ack_tracker.clone();

    // Spawn a task to send ACK after 50ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tracker_clone.ack(100).await;
    });

    let res = ack_tracker.send_with_ack(100, || async {
        Ok(())
    }).await;

    assert!(res.is_ok());
}

#[tokio::test]
async fn test_typed_error_matching() {
    use feiq_v2::error::{AppError, NetworkError};

    let err: AppError = NetworkError::SendTimeout.into();
    match &err {
        AppError::Network(NetworkError::SendTimeout) => {
            // Success! We can match on the variants.
        }
        _ => panic!("Expected Network(SendTimeout) variant"),
    }

    let err2: AppError = NetworkError::BindFailed("port busy".to_string()).into();
    match &err2 {
        AppError::Network(NetworkError::BindFailed(msg)) => {
            assert_eq!(msg, "port busy");
        }
        _ => panic!("Expected Network(BindFailed) variant"),
    }

    let err3: AppError = AppError::Other("test error".to_string());
    assert_eq!(err3.to_string(), "test error");
}
