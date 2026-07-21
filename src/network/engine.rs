use async_trait::async_trait;
use std::sync::Arc;

use super::{AckTracker, FileRegistry, PeerDirectory, SharedFile};
use crate::protocol::*;
use crate::network::transport::NetworkTransport;
use crate::error::AppError;

#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub errors: u64,
}

pub struct NetworkEngine {
    transport: Arc<dyn NetworkTransport>,
    pub(crate) my_username: std::sync::Mutex<String>,
    my_hostname: std::sync::Mutex<String>,
    packet_counter: std::sync::atomic::AtomicU32,
    pub shutdown: std::sync::atomic::AtomicBool,

    // Observability counters
    pub stats_packets_sent: std::sync::atomic::AtomicU64,
    pub stats_packets_received: std::sync::atomic::AtomicU64,
    pub stats_bytes_sent: std::sync::atomic::AtomicU64,
    pub stats_bytes_received: std::sync::atomic::AtomicU64,
    pub stats_errors: std::sync::atomic::AtomicU64,

    packet_dispatcher: Arc<super::PacketDispatcher>,
    pub(crate) event_tx: tokio::sync::broadcast::Sender<crate::types::CoreEvent>,

    // Deep modules extracted for Candidate 1 refactor
    pub ack_tracker: AckTracker,
    pub file_registry: FileRegistry,
    pub peer_directory: PeerDirectory,

    // Task ID generation moved to engine (C10)
    pub(crate) next_task_id: std::sync::atomic::AtomicI64,
}

impl NetworkEngine {
    /// Creates a new `NetworkEngine` instance using an injected transport.
    pub fn new(
        username: String,
        hostname: String,
        transport: Arc<dyn NetworkTransport>,
        event_tx: tokio::sync::broadcast::Sender<crate::types::CoreEvent>,
        max_task_id: i64,
    ) -> Result<Self, AppError> {
        let final_port = transport
            .local_addr()
            .ok()
            .map(|addr| addr.port())
            .unwrap_or(0);

        // Offset starting packet number with final_port multiplied by 100,000 to guarantee completely disjoint
        // packet number spaces (separated by 100k) and prevent duplicate cache collisions when multiple
        // instances run concurrently on loopback (127.0.0.1).
        let start_packet_no = (chrono::Utc::now().timestamp() as u32)
            .wrapping_add((final_port as u32).wrapping_mul(100_000));

        println!(
            "Successfully initialized NetworkEngine with transport bound to port {}",
            final_port
        );

        let packet_dispatcher = Arc::new(super::PacketDispatcher::new());

        Ok(NetworkEngine {
            transport,
            my_username: std::sync::Mutex::new(username),
            my_hostname: std::sync::Mutex::new(hostname),
            packet_counter: std::sync::atomic::AtomicU32::new(start_packet_no),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            stats_packets_sent: std::sync::atomic::AtomicU64::new(0),
            stats_packets_received: std::sync::atomic::AtomicU64::new(0),
            stats_bytes_sent: std::sync::atomic::AtomicU64::new(0),
            stats_bytes_received: std::sync::atomic::AtomicU64::new(0),
            stats_errors: std::sync::atomic::AtomicU64::new(0),
            packet_dispatcher,
            event_tx,
            ack_tracker: AckTracker::new(),
            file_registry: FileRegistry::new(),
            peer_directory: PeerDirectory::new(),
            next_task_id: std::sync::atomic::AtomicI64::new(max_task_id + 1),
        })
    }

    /// Increments and returns the next transfer task ID (C10)
    pub fn next_transfer_task_id(&self) -> i64 {
        self.next_task_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
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

    /// Register a custom peer port manually in our peer ports list (useful for loopback or testing).
    pub fn register_peer_port(&self, ip_str: &str, port: u16) {
        self.peer_directory.upsert_str(ip_str, port);
    }

    /// Retrieves the dynamically discovered port for a given peer IP address,
    /// or returns the default port (2425) if the peer's port is unknown.
    pub fn get_peer_port(&self, ip: &str) -> u16 {
        self.peer_directory.get_port_str(ip)
    }

    /// Returns the local socket address that the underlying UDP socket is bound to.
    pub fn socket_local_addr(&self) -> Result<std::net::SocketAddr, AppError> {
        self.transport.local_addr()
    }

    pub fn update_identity(&self, username: String, hostname: String) {
        if let Ok(mut u) = self.my_username.lock() {
            *u = username;
        }
        if let Ok(mut h) = self.my_hostname.lock() {
            *h = hostname;
        }
    }

    pub fn stats(&self) -> EngineStats {
        EngineStats {
            packets_sent: self
                .stats_packets_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            packets_received: self
                .stats_packets_received
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_sent: self
                .stats_bytes_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_received: self
                .stats_bytes_received
                .load(std::sync::atomic::Ordering::Relaxed),
            errors: self.stats_errors.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub fn register_shared_file(
        &self,
        packet_no: u32,
        file_id: u32,
        path: std::path::PathBuf,
        name: String,
        size: u64,
    ) {
        let file = SharedFile { path, name, size };
        self.file_registry.register(packet_no, file_id, file);
    }

    pub fn next_packet_no(&self) -> u32 {
        self.packet_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Sends an IPMsg packet containing a command and custom payload to a specified
    /// IP address and port.
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

    /// Internal helper to send a packet with a pre-determined packet number.
    pub async fn send_packet_with_no_on_port(
        &self,
        packet_no: u32,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let username = self.my_username.lock().unwrap().clone();
        let hostname = self.my_hostname.lock().unwrap().clone();
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
                self.stats_packets_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.stats_bytes_sent
                    .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Internal helper to send a packet with a pre-determined packet number, resolving the peer port automatically.
    pub async fn send_packet_with_no(
        &self,
        packet_no: u32,
        to_ip: &str,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let port = self.get_peer_port(to_ip);
        self.send_packet_with_no_on_port(packet_no, to_ip, port, cmd, extra).await
    }

    pub async fn send_packet(&self, to_ip: &str, cmd: u32, extra: &str) -> Result<u32, AppError> {
        let port = self.get_peer_port(to_ip);
        self.send_packet_on_port(to_ip, port, cmd, extra).await
    }

    pub async fn broadcast_online(&self) -> Result<(), AppError> {
        let username = self.my_username.lock().unwrap().clone();

        // Broadcast on port range 2425..=2430 to support discovering multi-instances in LAN
        for port in 2425..=2430 {
            let _ = self
                .send_packet_on_port("255.255.255.255", port, IPMSG_BR_ENTRY, &username)
                .await;

            // Try to broadcast on our specific adapter's subnet broadcast address
            if let Ok(local_addr) = self.transport.local_addr() {
                let ip_str = local_addr.ip().to_string();
                if ip_str != "0.0.0.0" && ip_str != "127.0.0.1" {
                    if let Some(pos) = ip_str.rfind('.') {
                        let subnet_prefix = &ip_str[..pos];
                        let subnet_broadcast = format!("{}.255", subnet_prefix);
                        let _ = self
                            .send_packet_on_port(&subnet_broadcast, port, IPMSG_BR_ENTRY, &username)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn send_packet_with_ack(
        &self,
        to_ip: &str,
        cmd: u32,
        extra: &str,
    ) -> Result<(), AppError> {
        let packet_no = self.next_packet_no();
        let cmd_with_opt = cmd | IPMSG_SENDCHECKOPT;
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

    pub fn stop(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.transport.stop();
    }

    pub async fn start_receive_loop(self: Arc<Self>, cancel: crate::types::CancellationToken) {
        let engine_clone = self.clone();
        let transport = self.transport.clone();

        let on_request = Arc::new(move |peer_ip: &str, request_payload: &[u8]| {
            engine_clone.handle_tcp_upload_request(peer_ip, request_payload)
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
            if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) || cancel.is_cancelled() {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                res = self.transport.recv_udp(&mut buf) => {
                    if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) || cancel.is_cancelled() {
                        break;
                    }
                    match res {
                        Ok((size, src_addr)) => {
                            self.stats_packets_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.stats_bytes_received.fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
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
                                if let Err(e) = self.clone().handle_incoming_packet(src_addr.ip(), ip_u32, packet).await {
                                    self.stats_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    eprintln!("Error handling packet: {}", e);
                                }
                            } else {
                                self.stats_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                println!("[UDP RECV WARNING] Failed to parse packet from {}.", src_addr);
                            }
                        }
                        Err(e) => {
                            if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                                break;
                            }
                            self.stats_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            eprintln!("UDP receive error: {}", e);
                        }
                    }
                }
            }
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
        if cmd_base != IPMSG_GETFILEDATA {
            return Err(AppError::Other(format!("Unsupported TCP command: {}", cmd_base)));
        }

        let parts: Vec<&str> = parsed_packet.extra.split(':').collect();
        if parts.len() < 3 {
            return Err(AppError::Other(format!(
                "Malformed IPMSG_GETFILEDATA extra part: {}",
                parsed_packet.extra
            )));
        }

        let packet_id_hex = parts[0];
        let file_id_hex = parts[1];
        let offset_hex = parts[2];

        let packet_id = u32::from_str_radix(packet_id_hex, 16).unwrap_or(0);
        let file_id = u32::from_str_radix(file_id_hex, 16).unwrap_or(0);
        let offset = u64::from_str_radix(offset_hex, 16).unwrap_or(0);

        let file_info = self.file_registry.lookup(packet_id, file_id).ok_or_else(|| {
            AppError::Other(format!(
                "Requested file not found in registry. packet_id: {}, file_id: {}",
                packet_id, file_id
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
            offset,
            task_id,
        })
    }

    pub async fn handle_incoming_packet(
        self: Arc<Self>,
        peer_ip_addr: std::net::IpAddr,
        ip_u32: u32,
        packet: IPMsgPacket,
    ) -> Result<(), AppError> {
        let dispatcher = self.packet_dispatcher.clone();
        dispatcher
            .dispatch(self, peer_ip_addr, ip_u32, packet)
            .await
    }

    pub async fn scan_subnet(self: Arc<Self>, subnet_prefix: &str, cancel: crate::types::CancellationToken) {
        // Thread pool discovery scan across subnet IP ranges (1 to 254) with throttled batch dispatch
        let subnet = subnet_prefix.trim_end_matches('.').to_string();
        let username = self.my_username.lock().unwrap().clone();

        for i in 1..255 {
            if cancel.is_cancelled() {
                break;
            }
            let ip = format!("{}.{}", subnet, i);
            let engine = self.clone();
            let username_clone = username.clone();
            let cancel_clone = cancel.clone();
            tokio::spawn(async move {
                if cancel_clone.is_cancelled() {
                    return;
                }
                let _ = engine
                    .send_packet(&ip, IPMSG_BR_ENTRY, &username_clone)
                    .await;
            });
            // 5ms throttle delay to avoid saturating network stack
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
            }
        }
    }

    pub async fn download_file_direct(
        &self,
        req: crate::types::FileDownloadRequest,
    ) -> Result<(), AppError> {
        let peer_ip = req.peer_ip.clone();
        let peer_port = self.get_peer_port(&peer_ip);

        // Format and transmit IPMSG_GETFILEDATA request
        let extra_payload = format!("{:x}:{:x}:0", req.packet_no, req.file_id);

        let username = {
            if let Ok(guard) = self.my_username.lock() {
                guard.clone()
            } else {
                crate::types::LOCAL_USER_IDENTIFIER.to_string()
            }
        };
        let hostname = {
            if let Ok(guard) = self.my_hostname.lock() {
                guard.clone()
            } else {
                crate::types::LOCAL_USER_IDENTIFIER.to_string()
            }
        };

        let request_packet = IPMsgPacket {
            version: crate::protocol::IPMSG_VERSION.to_string(),
            packet_no: self.next_packet_no(),
            username,
            hostname,
            cmd: IPMSG_GETFILEDATA,
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
                self.stats_bytes_received
                    .fetch_add(file_size, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl super::NetworkEngineTrait for NetworkEngine {
    fn get_peer_port(&self, ip: &str) -> u16 {
        self.get_peer_port(ip)
    }

    async fn send_packet_on_port(
        &self,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<u32, AppError> {
        self.send_packet_on_port(to_ip, port, cmd, extra).await
    }

    async fn broadcast_online(&self) -> Result<(), AppError> {
        self.broadcast_online().await
    }

    fn next_packet_no(&self) -> u32 {
        self.next_packet_no()
    }

    fn register_shared_file(
        &self,
        packet_no: u32,
        file_id: u32,
        path: std::path::PathBuf,
        name: String,
        size: u64,
    ) {
        self.register_shared_file(packet_no, file_id, path, name, size)
    }

    async fn download_file_direct(
        &self,
        req: crate::types::FileDownloadRequest,
    ) -> Result<(), AppError> {
        self.download_file_direct(req).await
    }

    fn update_identity(&self, username: String, hostname: String) {
        self.update_identity(username, hostname)
    }

    async fn scan_subnet(self: Arc<Self>, subnet_prefix: &str, cancel: crate::types::CancellationToken) {
        self.scan_subnet(subnet_prefix, cancel).await
    }

    fn next_transfer_task_id(&self) -> i64 {
        self.next_transfer_task_id()
    }
}
