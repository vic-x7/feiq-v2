use feiq_v2::network::{NetworkEngine, NetworkEvents};
use feiq_v2::types::{FileDownloadRequest, CancellationToken};
use feiq_v2::protocol::Utf8Transcoder;
use std::sync::Arc;

struct MockEvents;

impl NetworkEvents for MockEvents {
    fn on_peer_status_changed(
        &self,
        _ip: String,
        _username: String,
        _hostname: String,
        _nickname: Option<String>,
        _online: bool,
    ) {
    }

    fn on_message_received(
        &self,
        _sender_ip: String,
        _text_content: String,
        _timestamp: i64,
        _username: String,
    ) {
    }

    fn on_file_attachments_received(
        &self,
        _sender_ip: String,
        _packet_no: u32,
        _files: Vec<feiq_v2::types::FileAttachment>,
    ) {
    }

    fn on_window_knock(&self, _sender_ip: String, _username: String) {}

    fn on_peer_typing(&self, _sender_ip: String, _typing: bool) {}

    fn on_transfer_progress(
        &self,
        _task_id: i64,
        _progress: f64,
        _status: feiq_v2::types::TransferStatus,
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
async fn test_network_engine_binding_and_fallback() {
    let dispatcher1 = Arc::new(MockEvents);
    let dispatcher2 = Arc::new(MockEvents);
    let transcoder1 = Arc::new(Utf8Transcoder);
    let transcoder2 = Arc::new(Utf8Transcoder);

    // 1. Create first network engine on loopback
    let transport1 = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let engine1 = NetworkEngine::new(
        "alice".to_string(),
        "alice-pc".to_string(),
        transport1,
        dispatcher1,
        transcoder1,
        0,
    );

    assert!(
        engine1.is_ok(),
        "Failed to bind first engine: {:?}",
        engine1.err()
    );
    let engine1 = engine1.unwrap();

    // 2. Create second network engine on the same loopback IP
    // Since port 2425 is already bound by engine1, engine2 MUST fall back to a port in range 2426..=2435!
    let transport2 = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let engine2 = NetworkEngine::new(
        "bob".to_string(),
        "bob-pc".to_string(),
        transport2,
        dispatcher2,
        transcoder2,
        0,
    );

    assert!(
        engine2.is_ok(),
        "Failed to bind second engine: {:?}",
        engine2.err()
    );
    let engine2 = engine2.unwrap();

    // 3. Verify that engine1 and engine2 are bound to different loopback UDP ports in IPMsg range
    let addr1 = engine1.socket_local_addr().unwrap();
    let addr2 = engine2.socket_local_addr().unwrap();

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

    let dispatcher_bob = Arc::new(MockEvents);
    let dispatcher_alice = Arc::new(MockEvents);
    let transcoder_bob = Arc::new(Utf8Transcoder);
    let transcoder_alice = Arc::new(Utf8Transcoder);

    // Create engines
    let transport_bob = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let bob = Arc::new(
        NetworkEngine::new(
            "bob".to_string(),
            "bob-pc".to_string(),
            transport_bob,
            dispatcher_bob,
            transcoder_bob,
            0,
        )
        .unwrap(),
    );

    let transport_alice = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let alice = Arc::new(
        NetworkEngine::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport_alice,
            dispatcher_alice,
            transcoder_alice,
            0,
        )
        .unwrap(),
    );

    // Record Alice's port into Bob's known list, and Bob's port into Alice's list
    let bob_port = bob.socket_local_addr().unwrap().port();
    let alice_port = alice.socket_local_addr().unwrap().port();

    // Register peer ports manually to skip discovery sequence for this socket unit test
    alice.register_peer_port("127.0.0.1", bob_port);
    bob.register_peer_port("127.0.0.1", alice_port);

    // Spawn receive loops for both nodes (this automatically spawns their TCP servers)
    let bob_clone = bob.clone();
    let cancel_bob = CancellationToken::new();
    let cancel_bob_clone = cancel_bob.clone();
    let handle_bob = tokio::spawn(async move {
        bob_clone.start_receive_loop(cancel_bob_clone).await;
    });

    let alice_clone = alice.clone();
    let cancel_alice = CancellationToken::new();
    let cancel_alice_clone = cancel_alice.clone();
    let handle_alice = tokio::spawn(async move {
        alice_clone.start_receive_loop(cancel_alice_clone).await;
    });

    // Bob registers the shared file
    let packet_no = 42u32;
    let file_id = 0u32;
    bob.register_shared_file(
        packet_no,
        file_id,
        source_path.clone(),
        "source.txt".to_string(),
        test_content.len() as u64,
    );

    // Alice requests download direct from Bob
    let req = FileDownloadRequest {
        peer_ip: "127.0.0.1".to_string(),
        packet_no,
        file_id,
        save_path: dest_path.clone(),
        file_size: test_content.len() as u64,
        is_directory: false,
        task_id: 1, // Static task ID for testing without database
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
async fn test_network_directory_transfer_loopback() {
    use std::io::Write;

    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    
    // Create local folder structure:
    // temp_dir/src_dir_unique/
    // ├── file1.txt ("hello")
    // └── sub_dir/
    //     └── file2.txt ("subfile content")
    let src_dir = temp_dir.join(format!("src_dir_{}", unique_id));
    let sub_dir = src_dir.join("sub_dir");
    std::fs::create_dir_all(&sub_dir).unwrap();

    let file1_path = src_dir.join("file1.txt");
    {
        let mut file = std::fs::File::create(&file1_path).unwrap();
        file.write_all(b"hello").unwrap();
    }

    let file2_path = sub_dir.join("file2.txt");
    {
        let mut file = std::fs::File::create(&file2_path).unwrap();
        file.write_all(b"subfile content").unwrap();
    }

    let dest_dir = temp_dir.join(format!("dest_dir_{}", unique_id));

    let dispatcher_bob = Arc::new(MockEvents);
    let dispatcher_alice = Arc::new(MockEvents);
    let transcoder_bob = Arc::new(Utf8Transcoder);
    let transcoder_alice = Arc::new(Utf8Transcoder);

    // Create engines
    let transport_bob = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let bob = Arc::new(
        NetworkEngine::new(
            "bob".to_string(),
            "bob-pc".to_string(),
            transport_bob,
            dispatcher_bob,
            transcoder_bob,
            0,
        )
        .unwrap(),
    );

    let transport_alice = Arc::new(
        feiq_v2::network::TokioTransport::bind_fallback("127.0.0.1", 2425)
            .await
            .unwrap(),
    );
    let alice = Arc::new(
        NetworkEngine::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport_alice,
            dispatcher_alice,
            transcoder_alice,
            0,
        )
        .unwrap(),
    );

    let bob_port = bob.socket_local_addr().unwrap().port();
    let alice_port = alice.socket_local_addr().unwrap().port();

    alice.register_peer_port("127.0.0.1", bob_port);
    bob.register_peer_port("127.0.0.1", alice_port);

    // Spawn receive loops
    let bob_clone = bob.clone();
    let cancel_bob = CancellationToken::new();
    let cancel_bob_clone = cancel_bob.clone();
    let handle_bob = tokio::spawn(async move {
        bob_clone.start_receive_loop(cancel_bob_clone).await;
    });

    let alice_clone = alice.clone();
    let cancel_alice = CancellationToken::new();
    let cancel_alice_clone = cancel_alice.clone();
    let handle_alice = tokio::spawn(async move {
        alice_clone.start_receive_loop(cancel_alice_clone).await;
    });

    // Bob registers the shared directory
    let packet_no = 99u32;
    let file_id = 1u32;
    bob.register_shared_file(
        packet_no,
        file_id,
        src_dir.clone(),
        format!("src_dir_{}", unique_id),
        0, // Size is typically 0 for directories
    );

    // Alice requests directory download direct from Bob
    let req = FileDownloadRequest {
        peer_ip: "127.0.0.1".to_string(),
        packet_no,
        file_id,
        save_path: dest_dir.clone(),
        file_size: 0,
        is_directory: true, // This is a directory download!
        task_id: 2,
    };

    let download_result = alice.download_file_direct(req).await;
    assert!(
        download_result.is_ok(),
        "Directory download failed: {:?}",
        download_result.err()
    );

    // Verify downloaded folder contents
    let folder_name = format!("src_dir_{}", unique_id);
    let downloaded_file1 = dest_dir.join(&folder_name).join("file1.txt");
    let downloaded_file2 = dest_dir.join(&folder_name).join("sub_dir").join("file2.txt");

    assert_eq!(std::fs::read_to_string(&downloaded_file1).unwrap(), "hello");
    assert_eq!(std::fs::read_to_string(&downloaded_file2).unwrap(), "subfile content");

    // Teardown
    cancel_bob.cancel();
    cancel_alice.cancel();
    bob.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    alice.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);

    let _ = bob.send_packet_on_port("127.0.0.1", bob_port, 0, "").await;
    let _ = alice.send_packet_on_port("127.0.0.1", alice_port, 0, "").await;

    let _ = tokio::join!(handle_bob, handle_alice);

    // Clean up disk
    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&dest_dir);
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
    use feiq_v2::network::{FakeTransport, NetworkEngine, NetworkEvents};
    use std::str::FromStr;
    use std::sync::Mutex;

    struct DummyEvents {
        status_updates: Mutex<Vec<(String, String, bool)>>,
    }

    impl NetworkEvents for DummyEvents {
        fn on_peer_status_changed(
            &self,
            ip: String,
            username: String,
            _hostname: String,
            _nickname: Option<String>,
            online: bool,
        ) {
            self.status_updates
                .lock()
                .unwrap()
                .push((ip, username, online));
        }
        fn on_message_received(
            &self,
            _sender_ip: String,
            _text_content: String,
            _timestamp: i64,
            _username: String,
        ) {
        }
        fn on_file_attachments_received(
            &self,
            _sender_ip: String,
            _packet_no: u32,
            _files: Vec<feiq_v2::types::FileAttachment>,
        ) {
        }
        fn on_window_knock(&self, _sender_ip: String, _username: String) {}
        fn on_peer_typing(&self, _sender_ip: String, _typing: bool) {}
        fn on_transfer_progress(
            &self,
            _task_id: i64,
            _progress: f64,
            _status: feiq_v2::types::TransferStatus,
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

    let local_addr = "127.0.0.1:12345".parse().unwrap();
    let transport = Arc::new(FakeTransport::new(local_addr));
    let dispatcher = Arc::new(DummyEvents {
        status_updates: Mutex::new(Vec::new()),
    });
    let transcoder = Arc::new(Utf8Transcoder);

    let engine = Arc::new(
        NetworkEngine::new(
            "alice".to_string(),
            "alice-pc".to_string(),
            transport.clone(),
            dispatcher.clone(),
            transcoder,
            0,
        )
        .unwrap(),
    );

    // 1. Verify we can get local_addr from engine delegation correctly
    assert_eq!(engine.socket_local_addr().unwrap(), local_addr);

    // 2. Verify sending a packet captures it in the FakeTransport
    let packet_no = engine
        .send_packet("192.168.1.100", 32, "Hello")
        .await
        .unwrap();
    assert!(packet_no > 0);

    let sent = transport.get_sent_udp();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "192.168.1.100");
    assert_eq!(sent[0].1, 2425);

    // 3. Simulating receiving a packet
    // Let's inject a classic IPMsg packet bytes representing bob logging in
    let raw_packet = "1:12345:bob:bob-pc:1:Bob"; // IPMSG_BR_ENTRY (1)
    let peer_addr = "192.168.1.200:2425".parse().unwrap();
    transport.inject_incoming(raw_packet.as_bytes().to_vec(), peer_addr);

    // Let's spawn a quick test task or directly dispatch
    let packet = feiq_v2::protocol::IPMsgPacket::from_str(raw_packet).unwrap();
    engine
        .handle_incoming_packet(peer_addr.ip(), 0xC0A801C8, packet)
        .await
        .unwrap();

    // Verify dispatcher received status changed update
    let updates = dispatcher.status_updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0, "192.168.1.200");
    assert_eq!(updates[0].1, "bob");
    assert!(updates[0].2);
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
