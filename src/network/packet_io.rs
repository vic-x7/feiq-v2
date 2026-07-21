use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicI64, Ordering};
use std::sync::Mutex;
use crate::error::AppError;
use crate::network::transport::NetworkTransport;
use crate::network::{AckTracker, FileRegistry, PeerDirectory, PacketDispatcher};
use crate::protocol::IPMsgPacket;
use tokio::sync::broadcast::Sender;
use crate::types::CoreEvent;

#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub errors: u64,
}

pub struct PacketIO {
    pub transport: Arc<dyn NetworkTransport>,
    pub my_username: Mutex<String>,
    pub my_hostname: Mutex<String>,
    packet_counter: AtomicU32,
    pub shutdown: AtomicBool,

    // Observability counters
    pub stats_packets_sent: AtomicU64,
    pub stats_packets_received: AtomicU64,
    pub stats_bytes_sent: AtomicU64,
    pub stats_bytes_received: AtomicU64,
    pub stats_errors: AtomicU64,

    pub event_tx: Sender<CoreEvent>,
    next_task_id: AtomicI64,

    // References to shared components
    pub peer_directory: PeerDirectory,
    pub file_registry: FileRegistry,
    pub ack_tracker: AckTracker,
}

impl PacketIO {
    pub fn new(
        username: String,
        hostname: String,
        transport: Arc<dyn NetworkTransport>,
        event_tx: Sender<CoreEvent>,
        max_task_id: i64,
        peer_directory: PeerDirectory,
        file_registry: FileRegistry,
        ack_tracker: AckTracker,
    ) -> Self {
        let final_port = transport
            .local_addr()
            .ok()
            .map(|addr| addr.port())
            .unwrap_or(0);

        let start_packet_no = (chrono::Utc::now().timestamp() as u32)
            .wrapping_add((final_port as u32).wrapping_mul(100_000));

        Self {
            transport,
            my_username: Mutex::new(username),
            my_hostname: Mutex::new(hostname),
            packet_counter: AtomicU32::new(start_packet_no),
            shutdown: AtomicBool::new(false),
            stats_packets_sent: AtomicU64::new(0),
            stats_packets_received: AtomicU64::new(0),
            stats_bytes_sent: AtomicU64::new(0),
            stats_bytes_received: AtomicU64::new(0),
            stats_errors: AtomicU64::new(0),
            event_tx,
            next_task_id: AtomicI64::new(max_task_id + 1),
            peer_directory,
            file_registry,
            ack_tracker,
        }
    }

    pub fn next_packet_no(&self) -> u32 {
        self.packet_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_transfer_task_id(&self) -> i64 {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_username(&self) -> String {
        if let Ok(guard) = self.my_username.lock() {
            guard.clone()
        } else {
            crate::types::LOCAL_USER_IDENTIFIER.to_string()
        }
    }

    pub fn get_hostname(&self) -> String {
        if let Ok(guard) = self.my_hostname.lock() {
            guard.clone()
        } else {
            crate::types::LOCAL_USER_IDENTIFIER.to_string()
        }
    }

    pub(crate) fn make_progress_callback(&self) -> Arc<dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static> {
        let event_tx = self.event_tx.clone();
        Arc::new(
            move |task_id: i64, progress: f64, status: crate::types::TransferStatus| {
                let _ = event_tx.send(crate::types::CoreEvent::TransferProgress {
                    task_id,
                    progress,
                    status,
                });
            },
        )
    }

    pub async fn send_packet_on_port(
        &self,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<u32, AppError> {
        let packet_no = self.next_packet_no();
        self.send_packet_with_no_on_port(packet_no, to_ip, port, cmd, extra).await?;
        Ok(packet_no)
    }

    pub async fn send_packet_with_no_on_port(
        &self,
        packet_no: u32,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let username = self.get_username();
        let hostname = self.get_hostname();
        let packet = IPMsgPacket {
            version: crate::protocol::IPMSG_VERSION.to_string(),
            packet_no,
            username,
            hostname,
            cmd,
            extra: extra.to_string(),
        };

        let data = packet.serialize();
        match self.transport.send_udp(to_ip, port, &data).await {
            Ok(()) => {
                self.stats_packets_sent.fetch_add(1, Ordering::Relaxed);
                self.stats_bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub async fn send_packet_with_no(
        &self,
        packet_no: u32,
        to_ip: &str,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let port = self.peer_directory.get_port_str(to_ip);
        self.send_packet_with_no_on_port(packet_no, to_ip, port, cmd, extra).await
    }

    pub async fn send_packet(&self, to_ip: &str, cmd: u32, extra: &str) -> Result<u32, AppError> {
        let port = self.peer_directory.get_port_str(to_ip);
        self.send_packet_on_port(to_ip, port, cmd, extra).await
    }

    pub async fn send_packet_with_ack(
        &self,
        to_ip: &str,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let packet_no = self.next_packet_no();
        let cmd_with_opt = cmd | crate::protocol::IPMSG_SENDCHECKOPT;
        let to_ip_str = to_ip.to_string();
        let extra_str = extra.to_string();

        self.ack_tracker.send_with_ack(packet_no, || {
            let to_ip_str = to_ip_str.clone();
            let extra_str = extra_str.clone();
            async move {
                self.send_packet_with_no(packet_no, &to_ip_str, cmd_with_opt, &extra_str).await
            }
        }).await
    }

    pub async fn download_file_direct(
        &self,
        req: crate::types::FileDownloadRequest,
    ) -> Result<(), AppError> {
        let peer_ip = req.peer_ip.clone();
        let peer_port = self.peer_directory.get_port_str(&peer_ip);

        // Format and transmit IPMSG_GETFILEDATA request
        let extra_payload = format!("{:x}:{:x}:0", req.packet_no, req.file_id);

        let username = self.get_username();
        let hostname = self.get_hostname();

        let request_packet = IPMsgPacket {
            version: crate::protocol::IPMSG_VERSION.to_string(),
            packet_no: self.next_packet_no(),
            username,
            hostname,
            cmd: crate::protocol::IPMSG_GETFILEDATA,
            extra: extra_payload,
        };

        let request_data = request_packet.serialize();
        let save_path = req.save_path;
        let file_size = req.file_size;
        let task_id = req.task_id;

        let progress_callback = self.make_progress_callback();

        match self
            .transport
            .download_file(
                &peer_ip,
                peer_port,
                request_data,
                save_path,
                file_size,
                task_id,
                progress_callback,
            )
            .await
        {
            Ok(()) => {
                self.stats_bytes_received.fetch_add(file_size, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.transport.stop();
    }

    pub fn stats(&self) -> EngineStats {
        EngineStats {
            packets_sent: self.stats_packets_sent.load(Ordering::Relaxed),
            packets_received: self.stats_packets_received.load(Ordering::Relaxed),
            bytes_sent: self.stats_bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.stats_bytes_received.load(Ordering::Relaxed),
            errors: self.stats_errors.load(Ordering::Relaxed),
        }
    }

    pub fn handle_tcp_upload_request(
        &self,
        peer_ip: &str,
        request_payload: &[u8],
    ) -> Result<crate::network::transport::UploadRequestDetails, AppError> {
        let parsed_packet = IPMsgPacket::parse(request_payload)
            .ok_or_else(|| AppError::Other("Failed to parse IPMsg TCP packet".to_string()))?;

        let cmd_base = parsed_packet.cmd & 0xFF;
        if cmd_base != crate::protocol::IPMSG_GETFILEDATA {
            return Err(AppError::Other(format!("Unsupported TCP command: {}", cmd_base)));
        }

        let req = crate::protocol::parse_getfiledata_extra(&parsed_packet.extra)
            .ok_or_else(|| AppError::Other(format!("Malformed IPMSG_GETFILEDATA extra part: {}", parsed_packet.extra)))?;

        let file_info = self.file_registry.lookup(req.packet_no, req.file_id).ok_or_else(|| {
            AppError::Other(format!(
                "Requested file not found in registry. packet_no: {}, file_id: {}",
                req.packet_no, req.file_id
            ))
        })?;

        let task_id = self.next_transfer_task_id();
        let _ = self.event_tx.send(crate::types::CoreEvent::TransferStarted {
            task_id,
            peer_ip: peer_ip.to_string(),
            file_name: file_info.name.clone(),
            file_size: file_info.size as i64,
            is_sending: true,
        });

        Ok(crate::network::transport::UploadRequestDetails {
            file_path: file_info.path,
            file_size: file_info.size,
            offset: req.offset,
            task_id,
        })
    }

    pub async fn start_receive_loop(
        self: Arc<Self>,
        packet_dispatcher: Arc<PacketDispatcher>,
        cancel: crate::types::CancellationToken,
    ) {
        let io_clone = self.clone();
        let transport = self.transport.clone();

        let on_request = Arc::new(move |peer_ip: &str, request_payload: &[u8]| {
            io_clone.handle_tcp_upload_request(peer_ip, request_payload)
        });

        let progress_callback = self.make_progress_callback();

        let cancel_tcp = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel_tcp.cancelled() => {}
                res = transport.run_tcp_server(on_request, progress_callback) => {
                    if let Err(e) = res {
                        eprintln!("TCP Server error: {}", e);
                    }
                }
            }
        });

        let mut buf = [0u8; 8192];
        loop {
            if self.shutdown.load(Ordering::SeqCst) || cancel.is_cancelled() {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                res = self.transport.recv_udp(&mut buf) => {
                    if self.shutdown.load(Ordering::SeqCst) || cancel.is_cancelled() {
                        break;
                    }
                    match res {
                        Ok((size, src_addr)) => {
                            self.stats_packets_received.fetch_add(1, Ordering::Relaxed);
                            self.stats_bytes_received.fetch_add(size as u64, Ordering::Relaxed);
                            let raw_bytes = &buf[..size];

                            // RAW PACKET DIAGNOSTICS LOGGING
                            let (decoded, _, _) = encoding_rs::GBK.decode(raw_bytes);
                            println!("[UDP RECV] Raw payload from {}: {}", src_addr, decoded);

                            let ip_u32 = match src_addr.ip() {
                                std::net::IpAddr::V4(ipv4) => u32::from(ipv4),
                                _ => 0,
                            };

                            let is_self = if let Ok(local_addr) = self.transport.local_addr() {
                                src_addr == local_addr || (src_addr.ip() == local_addr.ip() && src_addr.port() == local_addr.port())
                            } else {
                                false
                            };

                            if !is_self && ip_u32 != 0 {
                                self.peer_directory.upsert(ip_u32, src_addr.port());
                            }

                            if let Some(packet) = IPMsgPacket::parse(raw_bytes) {
                                if let Err(e) = packet_dispatcher.dispatch(self.clone(), src_addr.ip(), ip_u32, packet).await {
                                    self.stats_errors.fetch_add(1, Ordering::Relaxed);
                                    eprintln!("Error handling packet: {}", e);
                                }
                            } else {
                                self.stats_errors.fetch_add(1, Ordering::Relaxed);
                                println!("[UDP RECV WARNING] Failed to parse packet from {}.", src_addr);
                            }
                        }
                        Err(e) => {
                            if self.shutdown.load(Ordering::SeqCst) {
                                break;
                            }
                            self.stats_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("UDP receive error: {}", e);
                        }
                    }
                }
            }
        }
    }
}
