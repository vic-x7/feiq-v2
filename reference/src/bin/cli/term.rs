use feiq_v2::types::CoreEvent;
use std::io::Write;

pub fn print_prompt() {
    print!("feiq-cli> ");
    let _ = std::io::stdout().flush();
}

pub fn render_event(event: CoreEvent) {
    match event {
        CoreEvent::PeerStatusChanged {
            ip,
            username,
            hostname: _,
            nickname: _,
            online,
        } => {
            println!(
                "\n[EVENT] Peer status: {} ({}) is {}",
                username,
                ip,
                if online { "ONLINE" } else { "OFFLINE" }
            );
            print_prompt();
        }
        CoreEvent::MessageReceived {
            sender_ip,
            content,
            username,
            ..
        } => {
            println!("\n[MSG] From {} ({}): {}", username, sender_ip, content);
            print_prompt();
        }
        CoreEvent::FileAttachmentsReceived {
            sender_ip,
            packet_no,
            files,
        } => {
            println!("\n[FILE RECEIVED] From {}:", sender_ip);
            for f in files {
                println!(
                    "  - Name: {}, Size: {}, ID: {}, Packet No: {}",
                    f.name, f.size, f.id, packet_no
                );
            }
            println!(
                "  To download, type: download {} {} <file_id> <file_name> <file_size>",
                sender_ip, packet_no
            );
            print_prompt();
        }
        CoreEvent::WindowKnock {
            sender_ip,
            username,
        } => {
            println!("\n[KNOCK] Screen Knock from {} ({})!", username, sender_ip);
            print_prompt();
        }
        CoreEvent::PeerTyping { sender_ip, typing } => {
            if typing {
                println!("\n[EVENT] {} is typing...", sender_ip);
            }
            print_prompt();
        }
        CoreEvent::TransferProgress {
            task_id,
            progress,
            status,
        } => {
            println!(
                "\n[EVENT] Task {}: Progress {:.1}% [{}]",
                task_id,
                progress * 100.0,
                status
            );
            print_prompt();
        }
        CoreEvent::TransferStarted {
            task_id,
            peer_ip,
            file_name,
            file_size,
            is_sending,
        } => {
            println!(
                "\n[EVENT] Task {}: Started {} file '{}' ({} bytes) with {}",
                task_id,
                if is_sending { "sending" } else { "receiving" },
                file_name,
                file_size,
                peer_ip
            );
            print_prompt();
        }
    }
}

pub fn print_help() {
    println!("Available commands:");
    println!("  help / ?                                                                  Show this message");
    println!("  scan <subnet>                                                             Scan subnet (e.g. scan 192.168.1)");
    println!("  peers                                                                     List discovered peers from SQLite");
    println!("  send <ip> <message_body...>                                               Send message to specific IP");
    println!("  share <ip> <file_path>                                                    Share local file with peer IP");
    println!("  download <ip> <packet_no> <file_id> <file_name> <file_size>               Download shared file from peer IP");
    println!("  stats                                                                     Display engine performance and network statistics");
    println!(
        "  exit / quit                                                               Exit the CLI"
    );
}

pub fn print_stats(stats: &feiq_v2::network::EngineStats) {
    println!("=== Engine Stats Snapshot ===");
    println!("Packets Sent:     {}", stats.packets_sent);
    println!("Packets Received: {}", stats.packets_received);
    println!("Bytes Sent:       {}", stats.bytes_sent);
    println!("Bytes Received:   {}", stats.bytes_received);
    println!("Errors:           {}", stats.errors);
}
