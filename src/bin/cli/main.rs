mod args;
mod term;

use feiq_v2::engine::EngineHandle;
use feiq_v2::types::CoreCommand;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() {
    println!("=== FeiQ Successor v2.0 Pure Rust CLI ===");

    // Parse command line arguments
    let cli_args = args::parse_cli_args();

    // Configuration fallback logic for username and hostname
    let username = cli_args.username.unwrap_or_else(|| {
        std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "cli_user".to_string())
    });

    let hostname = cli_args.hostname.unwrap_or_else(|| {
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "cli_host".to_string())
    });

    let bind_ip = cli_args.ip;
    let bind_port = cli_args.port;
    let db_path = cli_args.db;

    println!(
        "Starting engine as {}@{} on IP {}:{}...",
        username, hostname, bind_ip, bind_port
    );

    let config = feiq_v2::engine::EngineConfig {
        username,
        hostname,
        bind_ip,
        start_port: bind_port,
        db_path,
    };
    let engine =
        match EngineHandle::start(config).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("CRITICAL ERROR: Failed to start engine: {}", e);
                return;
            }
        };

    if cli_args.stats {
        let stats = engine.stats();
        term::print_stats(&stats);
        engine.shutdown();
        return;
    }

    let mut event_rx = engine.subscribe();
    // Spawn event listener to print background events
    tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            term::render_event(event);
        }
    });

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    println!("\nEnter 'help' or '?' for a list of available commands.");
    term::print_prompt();

    loop {
        line.clear();
        if reader.read_line(&mut line).await.is_err() {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            term::print_prompt();
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let command = parts[0];

        match command {
            "help" | "?" => {
                term::print_help();
            }
            "stats" => {
                let stats = engine.stats();
                term::print_stats(&stats);
            }
            "scan" => {
                if parts.len() < 2 {
                    println!("Usage: scan <subnet> (e.g. scan 192.168.1)");
                } else {
                    let subnet = parts[1].to_string();
                    let _ = engine.try_send(CoreCommand::ScanSubnet { subnet });
                }
            }
            "peers" => match engine.db().get_peers().await {
                Ok(peers) => {
                    println!("Discovered Peers ({}):", peers.len());
                    for p in peers {
                        println!(
                            "  - IP: {}, User: {}, Host: {}, Nickname: {:?}, Last Seen: {}",
                            p.ip, p.username, p.hostname, p.nickname, p.last_seen
                        );
                    }
                }
                Err(e) => println!("Error getting peers: {}", e),
            },
            "send" => {
                if parts.len() < 3 {
                    println!("Usage: send <ip> <message_body...>");
                } else {
                    let ip = parts[1].to_string();
                    let message_body = parts[2..].join(" ");
                    let _ = engine.try_send(CoreCommand::SendMessage {
                        to_ip: ip,
                        content: message_body,
                    });
                }
            }
            "share" => {
                if parts.len() < 3 {
                    println!("Usage: share <ip> <file_path>");
                } else {
                    let ip = parts[1].to_string();
                    let path = PathBuf::from(parts[2..].join(" "));
                    let _ = engine.try_send(CoreCommand::ShareFile { peer_ip: ip, path });
                }
            }
            "download" => {
                if parts.len() < 6 {
                    println!("Usage: download <ip> <packet_no> <file_id> <file_name> <file_size>");
                } else {
                    let peer_ip = parts[1].to_string();
                    let packet_no = parts[2].parse::<u32>().unwrap_or(0);
                    let file_id = parts[3].parse::<u32>().unwrap_or(0);
                    let file_name = parts[4].to_string();
                    let file_size = parts[5].parse::<u64>().unwrap_or(0);

                    let _ = engine.try_send(CoreCommand::DownloadFile {
                        peer_ip,
                        packet_no,
                        file_id,
                        name: file_name,
                        size: file_size,
                    });
                }
            }
            "exit" | "quit" => {
                println!("Shutting down engine...");
                engine.shutdown(); // Stop network loop and trigger cancellation
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                println!("Goodbye!");
                break;
            }
            _ => {
                println!(
                    "Unknown command: '{}'. Type 'help' for instructions.",
                    command
                );
            }
        }
        term::print_prompt();
    }
}
