use feiq_v2::engine::start_engine;
use feiq_v2::protocol::IPMSG_BR_ENTRY;
use feiq_v2::types::{CoreCommand, CoreEvent};

#[tokio::test]
async fn test_full_lifecycle_multi_instance_emulation() {
    use std::io::Write;

    // 1. Set up temporary file paths for databases and testing files
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let db_alice_path = temp_dir.join(format!("alice_db_{}.db", unique_id));
    let db_bob_path = temp_dir.join(format!("bob_db_{}.db", unique_id));

    let bob_source_file = temp_dir.join(format!("bob_source_{}.txt", unique_id));
    let alice_download_file = temp_dir.join(format!("alice_dest_{}.txt", unique_id));

    // Create Bob's shared file source
    let file_payload = "This is a high-fidelity integration test file content containing complete IPMsg v1.2 emulation frames.";
    {
        let mut f = std::fs::File::create(&bob_source_file).unwrap();
        f.write_all(file_payload.as_bytes()).unwrap();
    }

    // 2. Start Alice's Core Engine
    let (alice_cmd, _alice_ev_tx, mut alice_ev_rx, alice_net, alice_db) = start_engine(
        "alice".to_string(),
        "alice-pc".to_string(),
        "127.0.0.1".to_string(),
        2425,
        db_alice_path.clone(),
    )
    .await
    .unwrap();

    // 3. Start Bob's Core Engine
    let (bob_cmd, _bob_ev_tx, mut bob_ev_rx, bob_net, bob_db) = start_engine(
        "bob".to_string(),
        "bob-pc".to_string(),
        "127.0.0.1".to_string(),
        2425,
        db_bob_path.clone(),
    )
    .await
    .unwrap();

    // Retrieve loopback ports
    let alice_port = alice_net.socket_local_addr().unwrap().port();
    let bob_port = bob_net.socket_local_addr().unwrap().port();

    println!(
        "Alice bound on port {}, Bob bound on port {}",
        alice_port, bob_port
    );

    // Manually cross-register peer ports in memory so they know how to communicate back and forth
    alice_net.register_peer_port("127.0.0.1", bob_port);
    bob_net.register_peer_port("127.0.0.1", alice_port);

    // 4. Trigger peer discovery manually
    // Alice sends entry packet directly to Bob's bound loopback port
    alice_net
        .send_packet_on_port("127.0.0.1", bob_port, IPMSG_BR_ENTRY, "alice")
        .await
        .unwrap();

    // Alice expects CoreEvent::PeerStatusChanged for Bob (online)
    let mut discovered_bob = false;
    for _ in 0..10 {
        if let Ok(Ok(CoreEvent::PeerStatusChanged {
            ip,
            username,
            online,
            ..
        })) =
            tokio::time::timeout(std::time::Duration::from_millis(150), alice_ev_rx.recv()).await
        {
            if ip == "127.0.0.1" && username == "bob" && online {
                discovered_bob = true;
                break;
            }
        }
    }
    assert!(
        discovered_bob,
        "Alice failed to discover Bob via loopback entry/ans sequence!"
    );

    // Bob expects CoreEvent::PeerStatusChanged for Alice (online)
    let mut discovered_alice = false;
    for _ in 0..10 {
        if let Ok(Ok(CoreEvent::PeerStatusChanged {
            ip,
            username,
            online,
            ..
        })) = tokio::time::timeout(std::time::Duration::from_millis(150), bob_ev_rx.recv()).await
        {
            if ip == "127.0.0.1" && username == "alice" && online {
                discovered_alice = true;
                break;
            }
        }
    }
    assert!(
        discovered_alice,
        "Bob failed to discover Alice via loopback entry/ans sequence!"
    );

    // 5. Test Chat messaging: Alice sends Bob a direct message
    let chat_text = "Hello Bob! Did you see the new Rust architecture?";
    alice_cmd
        .send(CoreCommand::SendMessage {
            to_ip: "127.0.0.1".to_string(),
            content: chat_text.to_string(),
        })
        .await
        .unwrap();

    // Bob asserts receiving message event
    let mut msg_received = false;
    for _ in 0..10 {
        if let Ok(Ok(CoreEvent::MessageReceived {
            sender_ip,
            content,
            username,
            ..
        })) = tokio::time::timeout(std::time::Duration::from_millis(150), bob_ev_rx.recv()).await
        {
            if sender_ip == "127.0.0.1" && username == "alice" && content == chat_text {
                msg_received = true;
                break;
            }
        }
    }
    assert!(
        msg_received,
        "Bob failed to receive Alice's chat message event!"
    );

    // Verify Bob's SQLite database successfully persistent-saved the message
    {
        let history = bob_db
            .get_chat_history("127.0.0.1".to_string(), 10, 0)
            .await
            .unwrap();
        println!("Bob DB messages found ({}):", history.len());
        for (i, m) in history.iter().enumerate() {
            println!(
                "  [{}] sender: {}, receiver: {}, text: {}",
                i, m.sender_ip, m.receiver_ip, m.text_content
            );
        }
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].sender_ip, "127.0.0.1");
        assert_eq!(history[0].text_content, chat_text);
    }

    // 6. Test File Sharing Signalling: Bob registers and shares a file with Alice
    bob_cmd
        .send(CoreCommand::ShareFile {
            peer_ip: "127.0.0.1".to_string(),
            path: bob_source_file.clone(),
        })
        .await
        .unwrap();

    // Alice expects CoreEvent::FileAttachmentsReceived carrying file metadata
    let mut file_advertised = false;
    let mut packet_no = 0;
    let mut file_id = 0;
    let mut file_name = String::new();
    let mut file_size = 0u64;

    for _ in 0..10 {
        if let Ok(Ok(CoreEvent::FileAttachmentsReceived {
            sender_ip,
            packet_no: pno,
            files,
        })) =
            tokio::time::timeout(std::time::Duration::from_millis(150), alice_ev_rx.recv()).await
        {
            if sender_ip == "127.0.0.1" && !files.is_empty() {
                file_advertised = true;
                packet_no = pno;
                file_id = files[0].id;
                file_name = files[0].name.clone();
                file_size = files[0].size;
                break;
            }
        }
    }
    assert!(
        file_advertised,
        "Alice failed to receive Bob's shared file metadata signaling packet!"
    );
    assert_eq!(
        file_name,
        "bob_source_".to_owned() + &unique_id.to_string() + ".txt"
    );
    assert_eq!(file_size, file_payload.len() as u64);

    // 7. Test File Chunk Downloading: Alice initiates direct TCP download from Bob's registry
    alice_cmd
        .send(CoreCommand::DownloadFile {
            peer_ip: "127.0.0.1".to_string(),
            packet_no,
            file_id,
            name: alice_download_file.to_string_lossy().to_string(),
            size: file_size,
        })
        .await
        .unwrap();

    // Poll to wait for file download completion
    let mut download_verified = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if alice_download_file.exists() {
            let data = std::fs::read_to_string(&alice_download_file).unwrap();
            if data == file_payload {
                download_verified = true;
                break;
            }
        }
    }
    assert!(
        download_verified,
        "Alice failed to download Bob's file successfully over TCP!"
    );

    // Cryptographic Checksum/MD5 Verification: Verify Bob's source and Alice's downloaded file MD5 hash match identically
    let bob_source_bytes = std::fs::read(&bob_source_file).unwrap();
    let alice_download_bytes = std::fs::read(&alice_download_file).unwrap();
    let hash_bob = format!("{:x}", md5::compute(&bob_source_bytes));
    let hash_alice = format!("{:x}", md5::compute(&alice_download_bytes));
    assert_eq!(
        hash_bob, hash_alice,
        "Cryptographic MD5 checksum mismatch between source and download!"
    );
    println!("Integration Test MD5 Match: {}", hash_alice);

    // Verify Alice's SQLite DB reports file transfer status as "completed" with progress 1.0 (Task ID = 1)
    {
        let count = alice_db.get_total_file_transfers_count().await.unwrap();
        assert_eq!(count, 1, "Alice should have exactly 1 file transfer task");

        let mut task_status = (feiq_v2::types::TransferStatus::Failed, 0.0);
        for _ in 0..20 {
            if let Some(status) = alice_db.get_file_task_status(1).await.unwrap() {
                task_status = status;
                if task_status.0 == feiq_v2::types::TransferStatus::Completed {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(
            task_status.0,
            feiq_v2::types::TransferStatus::Completed,
            "Alice's file task status was not updated to completed!"
        );
        assert_eq!(
            task_status.1, 1.0,
            "Alice's file task progress was not updated to 1.0!"
        );
    }

    // Verify Bob's SQLite DB reports file transfer status as "completed" with progress 1.0
    // Bob has two records in his DB: Task ID = 1 (pre-registered pending share) and Task ID = 2 (active served transfer updated to completed)
    {
        let mut count = 0;
        for _ in 0..20 {
            count = bob_db.get_total_file_transfers_count().await.unwrap();
            if count == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            count, 2,
            "Bob should have exactly 2 file tasks (1 registered + 1 active served)"
        );

        let mut served_task_status = (feiq_v2::types::TransferStatus::Failed, 0.0);
        for _ in 0..20 {
            if let Some(status) = bob_db.get_file_task_status(2).await.unwrap() {
                served_task_status = status;
                if served_task_status.0 == feiq_v2::types::TransferStatus::Completed {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(
            served_task_status.0,
            feiq_v2::types::TransferStatus::Completed,
            "Bob's served file task status was not updated to completed!"
        );
        assert_eq!(
            served_task_status.1, 1.0,
            "Bob's served file task progress was not updated to 1.0!"
        );
    }

    // 8. Teardown Engines Cleanly
    bob_net
        .shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);
    alice_net
        .shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Stop loops by waking up sockets
    let _ = bob_net
        .send_packet_on_port("127.0.0.1", bob_port, 0, "")
        .await;
    let _ = alice_net
        .send_packet_on_port("127.0.0.1", alice_port, 0, "")
        .await;

    // Small delay to allow socket loops to shutdown cleanly
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Clean up temporary database files and text files on disk
    let _ = std::fs::remove_file(&db_alice_path);
    let _ = std::fs::remove_file(&db_bob_path);
    let _ = std::fs::remove_file(&bob_source_file);
    let _ = std::fs::remove_file(&alice_download_file);

    // SQLite transaction log files (WAL / SHM) cleanups
    let _ = std::fs::remove_file(format!("{}-wal", db_alice_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_alice_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", db_bob_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_bob_path.display()));
}

#[tokio::test]
async fn test_classic_feiq_features_emulation() {
    use feiq_v2::protocol::{
        IPMSG_BR_EXIT, IPMSG_INPUTING, IPMSG_INPUT_END, IPMSG_KNOCK, IPMSG_SENDMSG,
    };

    // 1. Setup temp DB paths
    let temp_dir = std::env::temp_dir();
    let unique_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let db_alice_path = temp_dir.join(format!("alice_classic_db_{}.db", unique_id));
    let db_bob_path = temp_dir.join(format!("bob_classic_db_{}.db", unique_id));

    // 2. Start Alice's and Bob's Core Engines (using :memory: databases to eliminate disk I/O and prevent UDP loop delays)
    let (_alice_cmd, _alice_ev_tx, _alice_ev_rx, alice_net, _alice_db) = start_engine(
        "alice".to_string(),
        "alice-pc".to_string(),
        "127.0.0.1".to_string(),
        2425,
        std::path::PathBuf::from(":memory:"),
    )
    .await
    .unwrap();

    let (_bob_cmd, _bob_ev_tx, mut bob_ev_rx, bob_net, _bob_db) = start_engine(
        "bob".to_string(),
        "bob-pc".to_string(),
        "127.0.0.1".to_string(),
        2425,
        std::path::PathBuf::from(":memory:"),
    )
    .await
    .unwrap();

    let alice_port = alice_net.socket_local_addr().unwrap().port();
    let bob_port = bob_net.socket_local_addr().unwrap().port();

    // Register peer ports
    alice_net.register_peer_port("127.0.0.1", bob_port);
    bob_net.register_peer_port("127.0.0.1", alice_port);

    // 3. Test Typing: Alice is typing (IPMSG_INPUTING) -> Bob receives typing event
    alice_net
        .send_packet("127.0.0.1", IPMSG_INPUTING, "")
        .await
        .unwrap();

    let mut typing_started = false;
    let timeout_duration = std::time::Duration::from_secs(3);
    let mut start_time = std::time::Instant::now();
    while start_time.elapsed() < timeout_duration {
        match tokio::time::timeout(std::time::Duration::from_millis(50), bob_ev_rx.recv()).await {
            Ok(Ok(event)) => {
                println!("[DEBUG CLASSIC TEST] Bob event: {:?}", event);
                if let CoreEvent::PeerTyping { sender_ip, typing } = event {
                    if sender_ip == "127.0.0.1" && typing {
                        typing_started = true;
                        break;
                    }
                }
            }
            Ok(Err(e)) => println!("[DEBUG CLASSIC TEST] Bob recv error: {:?}", e),
            Err(_) => {} // Timeout
        }
    }
    assert!(
        typing_started,
        "Bob failed to receive Alice's typing-start event!"
    );

    // Test Typing: Alice stopped typing (IPMSG_INPUT_END) -> Bob receives typing-end event
    alice_net
        .send_packet("127.0.0.1", IPMSG_INPUT_END, "")
        .await
        .unwrap();

    let mut typing_ended = false;
    start_time = std::time::Instant::now();
    while start_time.elapsed() < timeout_duration {
        match tokio::time::timeout(std::time::Duration::from_millis(50), bob_ev_rx.recv()).await {
            Ok(Ok(event)) => {
                println!("[DEBUG CLASSIC TEST] Bob event: {:?}", event);
                if let CoreEvent::PeerTyping { sender_ip, typing } = event {
                    if sender_ip == "127.0.0.1" && !typing {
                        typing_ended = true;
                        break;
                    }
                }
            }
            Ok(Err(e)) => println!("[DEBUG CLASSIC TEST] Bob recv error: {:?}", e),
            Err(_) => {} // Timeout
        }
    }
    assert!(
        typing_ended,
        "Bob failed to receive Alice's typing-end event!"
    );

    // 4. Test Window Knock: Alice knocks Bob's window (IPMSG_KNOCK) -> Bob receives knock event
    alice_net
        .send_packet("127.0.0.1", IPMSG_KNOCK, "")
        .await
        .unwrap();

    let mut knock_received = false;
    start_time = std::time::Instant::now();
    while start_time.elapsed() < timeout_duration {
        match tokio::time::timeout(std::time::Duration::from_millis(50), bob_ev_rx.recv()).await {
            Ok(Ok(event)) => {
                println!("[DEBUG CLASSIC TEST] Bob event: {:?}", event);
                if let CoreEvent::WindowKnock {
                    sender_ip,
                    username,
                } = event
                {
                    if sender_ip == "127.0.0.1" && username == "alice" {
                        knock_received = true;
                        break;
                    }
                }
            }
            Ok(Err(e)) => println!("[DEBUG CLASSIC TEST] Bob recv error: {:?}", e),
            Err(_) => {} // Timeout
        }
    }
    assert!(
        knock_received,
        "Bob failed to receive Alice's window knock event!"
    );

    // 5. Test Packet Acknowledgment: Alice sends direct msg to Bob requiring ACK
    // Using send_packet_with_ack ensures that the ACK-wait loop completes successfully!
    let ack_result = alice_net
        .send_packet_with_ack("127.0.0.1", IPMSG_SENDMSG, "Hi Bob, confirm this!")
        .await;
    assert!(
        ack_result.is_ok(),
        "Acknowledge handshake failed: {:?}",
        ack_result.err()
    );

    // 6. Test Exit: Alice goes offline (IPMSG_BR_EXIT) -> Bob receives peer status changed event (online: false)
    alice_net
        .send_packet("127.0.0.1", IPMSG_BR_EXIT, "")
        .await
        .unwrap();

    let mut peer_offline = false;
    let start_time_exit = std::time::Instant::now();
    while start_time_exit.elapsed() < timeout_duration {
        match tokio::time::timeout(std::time::Duration::from_millis(50), bob_ev_rx.recv()).await {
            Ok(Ok(event)) => {
                println!("[DEBUG CLASSIC TEST] Bob event: {:?}", event);
                if let CoreEvent::PeerStatusChanged {
                    ip,
                    username,
                    online,
                    ..
                } = event
                {
                    if ip == "127.0.0.1" && username == "alice" && !online {
                        peer_offline = true;
                        break;
                    }
                }
            }
            Ok(Err(e)) => println!("[DEBUG CLASSIC TEST] Bob recv error: {:?}", e),
            Err(_) => {} // Timeout
        }
    }
    assert!(
        peer_offline,
        "Bob failed to receive Alice's offline exit status changed event!"
    );

    // 7. Teardown
    bob_net
        .shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);
    alice_net
        .shutdown
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Wake up loops to shutdown cleanly
    let _ = bob_net
        .send_packet_on_port("127.0.0.1", bob_port, 0, "")
        .await;
    let _ = alice_net
        .send_packet_on_port("127.0.0.1", alice_port, 0, "")
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let _ = std::fs::remove_file(&db_alice_path);
    let _ = std::fs::remove_file(&db_bob_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_alice_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_alice_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", db_bob_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_bob_path.display()));
}
