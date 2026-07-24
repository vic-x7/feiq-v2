use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, UdpSocket};

use crate::error::{AppError, NetworkError};
use crate::network::transport::{NetworkTransport, UploadRequestDetails};

pub struct TokioTransport {
    socket: Arc<UdpSocket>,
    tcp_listener: std::sync::Mutex<Option<TcpListener>>,
    tcp_port: Option<u16>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl TokioTransport {
    pub fn new(socket: Arc<UdpSocket>, tcp_listener: Option<TcpListener>) -> Self {
        let tcp_port = tcp_listener
            .as_ref()
            .and_then(|l| l.local_addr().ok().map(|a| a.port()));
        Self {
            socket,
            tcp_listener: std::sync::Mutex::new(tcp_listener),
            tcp_port,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Attempts to bind UDP and TCP listeners on a starting port on the given bind_ip,
    /// falling back to sequential ports if the base port is locked.
    pub async fn bind_fallback(bind_ip: &str, start_port: u16) -> Result<Self, AppError> {
        let actual_ip = if bind_ip.trim().is_empty() {
            "0.0.0.0".to_string()
        } else {
            bind_ip.to_string()
        };

        let mut bound_udp: Option<UdpSocket> = None;
        let mut bound_tcp: Option<TcpListener> = None;

        // Try the requested starting port first
        let bind_addr = format!("{}:{}", actual_ip, start_port);
        if let Ok(udp) = UdpSocket::bind(&bind_addr).await {
            if let Ok(tcp) = TcpListener::bind(&bind_addr).await {
                bound_udp = Some(udp);
                bound_tcp = Some(tcp);
            }
        }

        // If starting port is locked, sequentially probe the next 10 ports immediately without sleeps
        if bound_udp.is_none() || bound_tcp.is_none() {
            let fallback_start = start_port + 1;
            let fallback_end = start_port + 10;
            println!(
                "Port {} is busy. Trying fallback ports sequential range {}..={}...",
                start_port, fallback_start, fallback_end
            );
            for port in fallback_start..=fallback_end {
                let bind_addr = format!("{}:{}", actual_ip, port);
                if let Ok(udp) = UdpSocket::bind(&bind_addr).await {
                    if let Ok(tcp) = TcpListener::bind(&bind_addr).await {
                        bound_udp = Some(udp);
                        bound_tcp = Some(tcp);
                        break;
                    }
                }
            }
        }

        // Unpack the sockets or return Err
        let socket =
            match bound_udp {
                Some(s) => s,
                None => {
                    return Err(NetworkError::BindFailed(format!(
                        "UDP/TCP socket binding failed on {} for range {}..={}.",
                        actual_ip, start_port, start_port + 10
                    )).into());
                }
            };

        let tcp_listener =
            match bound_tcp {
                Some(t) => t,
                None => {
                    return Err(NetworkError::BindFailed(format!(
                        "TCP listener binding failed on {} for range {}..={}.",
                        actual_ip, start_port, start_port + 10
                    )).into());
                }
            };

        socket
            .set_broadcast(true)
            .map_err(|e| AppError::Other(format!("Failed to enable UDP broadcast: {}", e)))?;

        Ok(TokioTransport::new(Arc::new(socket), Some(tcp_listener)))
    }
}

#[async_trait]
impl NetworkTransport for TokioTransport {
    async fn send_udp(&self, to_ip: &str, port: u16, data: &[u8]) -> Result<(), AppError> {
        let ip: std::net::IpAddr = to_ip
            .parse()
            .map_err(|e| AppError::Other(format!("Invalid IP address '{}': {}", to_ip, e)))?;
        let addr = std::net::SocketAddr::new(ip, port);
        self.socket
            .send_to(data, &addr)
            .await
            .map_err(|e| AppError::Io(e))?;
        Ok(())
    }

    async fn recv_udp(&self, buf: &mut [u8]) -> Result<(usize, std::net::SocketAddr), AppError> {
        self.socket.recv_from(buf).await.map_err(|e| AppError::Io(e))
    }

    async fn download_file(
        &self,
        peer_ip: &str,
        peer_port: u16,
        request_data: Vec<u8>,
        save_path: PathBuf,
        file_size: u64,
        task_id: i64,
        progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
    ) -> Result<(), AppError> {
        let mut stream =
            match tokio::net::TcpStream::connect(format!("{}:{}", peer_ip, peer_port)).await {
                Ok(s) => s,
                Err(e) => {
                    progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
                    return Err(NetworkError::ConnectionRefused(format!(
                        "Failed to connect to sender's TCP port {}: {}",
                        peer_port, e
                    )).into());
                }
            };

        if let Err(e) = stream.write_all(&request_data).await {
            progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
            return Err(AppError::Other(format!("Failed to write request to TCP stream: {}", e)));
        }

        if let Err(e) = stream.shutdown().await {
            progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
            return Err(AppError::Io(e));
        }

        let file = match tokio::fs::File::create(&save_path).await {
            Ok(f) => f,
            Err(e) => {
                progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
                return Err(AppError::Other(format!("Failed to create local file: {}", e)));
            }
        };

        Self::copy_with_progress(
            stream,
            file,
            file_size,
            0,
            task_id,
            progress_callback,
            self.shutdown.clone(),
            "Shutdown initiated during file download",
        )
        .await
    }

    async fn run_tcp_server(
        &self,
        on_request: Arc<
            dyn for<'a, 'b> Fn(&'a str, &'b [u8]) -> Result<UploadRequestDetails, AppError>
                + Send
                + Sync
                + 'static,
        >,
        progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
    ) -> Result<(), AppError> {
        let listener_opt = {
            let mut guard = self
                .tcp_listener
                .lock()
                .expect("Mutex tcp_listener should not be poisoned");
            guard.take()
        };

        let listener = match listener_opt {
            Some(l) => l,
            None => return Err(AppError::Other("TCP Listener not initialized or already taken".to_string())),
        };

        loop {
            if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            tokio::select! {
                res = listener.accept() => {
                    if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    match res {
                        Ok((stream, src_addr)) => {
                            let on_request_clone = on_request.clone();
                            let progress_callback_clone = progress_callback.clone();
                            let shutdown_clone = self.shutdown.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_tcp_upload(stream, &src_addr.ip().to_string(), on_request_clone, progress_callback_clone, shutdown_clone).await {
                                    eprintln!("Error handling TCP upload to {}: {}", src_addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            if self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                                break;
                            }
                            eprintln!("TCP accept error: {}", e);
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn local_addr(&self) -> Result<std::net::SocketAddr, AppError> {
        self.socket
            .local_addr()
            .map_err(|e| AppError::Other(format!("Failed to get socket address: {}", e)))
    }

    fn stop(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let socket_clone = self.socket.clone();
        let bound_addr = self.socket.local_addr().ok();
        let tcp_port_opt = self.tcp_port;

        tokio::spawn(async move {
            // Wake up UDP socket
            if let Some(addr) = bound_addr {
                let target_ip = Self::normalize_target_ip(addr.ip().to_string());
                let target_addr = format!("{}:{}", target_ip, addr.port());
                let _ = socket_clone.send_to(b"WAKEUP", &target_addr).await;
            }

            // Wake up TCP socket
            if let Some(port) = tcp_port_opt {
                let ip_str = bound_addr
                    .map(|a| a.ip().to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let target_ip = Self::normalize_target_ip(ip_str);
                let target_addr = format!("{}:{}", target_ip, port);
                let _ = tokio::net::TcpStream::connect(&target_addr).await;
            }
        });
    }
}

impl TokioTransport {
    fn normalize_target_ip(ip_str: String) -> String {
        if ip_str == "0.0.0.0" {
            "127.0.0.1".to_string()
        } else {
            ip_str
        }
    }

    async fn copy_with_progress<R, W>(
        mut reader: R,
        mut writer: W,
        file_size: u64,
        initial_bytes: u64,
        task_id: i64,
        progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        shutdown_err_msg: &str,
    ) -> Result<(), AppError>
    where
        R: tokio::io::AsyncRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut bytes_copied = initial_bytes;
        let mut buffer = [0u8; 65536]; // Stack-allocated, Zero-Heap in hot chunk loop!
        let mut last_reported_percent = if file_size > 0 {
            (initial_bytes as f64 / file_size as f64 * 100.0) as i32
        } else {
            0
        };

        loop {
            if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                progress_callback(
                    task_id,
                    if file_size > 0 {
                        bytes_copied as f64 / file_size as f64
                    } else {
                        0.0
                    },
                    crate::types::TransferStatus::Failed,
                );
                return Err(AppError::Other(shutdown_err_msg.to_string()));
            }

            let n = reader.read(&mut buffer).await.map_err(|e| {
                progress_callback(
                    task_id,
                    if file_size > 0 {
                        bytes_copied as f64 / file_size as f64
                    } else {
                        0.0
                    },
                    crate::types::TransferStatus::Failed,
                );
                AppError::Other(format!("Read error: {}", e))
            })?;

            if n == 0 {
                break;
            }

            writer.write_all(&buffer[..n]).await.map_err(|e| {
                progress_callback(
                    task_id,
                    if file_size > 0 {
                        bytes_copied as f64 / file_size as f64
                    } else {
                        0.0
                    },
                    crate::types::TransferStatus::Failed,
                );
                AppError::Other(format!("Write error: {}", e))
            })?;

            bytes_copied += n as u64;
            let percent = if file_size > 0 {
                (bytes_copied as f64 / file_size as f64 * 100.0) as i32
            } else {
                100
            };

            if percent > last_reported_percent {
                last_reported_percent = percent;
                let progress = if file_size > 0 {
                    bytes_copied as f64 / file_size as f64
                } else {
                    1.0
                };
                progress_callback(
                    task_id,
                    progress,
                    crate::types::TransferStatus::Transferring,
                );
            }
        }

        progress_callback(task_id, 1.0, crate::types::TransferStatus::Completed);
        Ok(())
    }

    async fn handle_tcp_upload(
        mut stream: tokio::net::TcpStream,
        peer_ip: &str,
        on_request: Arc<
            dyn for<'a, 'b> Fn(&'a str, &'b [u8]) -> Result<UploadRequestDetails, AppError>
                + Send
                + Sync
                + 'static,
        >,
        progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), AppError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // Read request from peer
        let mut buf = vec![0u8; 2048];
        let mut total_read = 0;
        loop {
            let n = stream
                .read(&mut buf[total_read..])
                .await
                .map_err(|e| AppError::Other(format!("Failed to read from TCP stream: {}", e)))?;
            if n == 0 {
                break;
            }
            total_read += n;
            if buf[..total_read].contains(&0) || total_read >= buf.len() {
                break;
            }
        }

        if total_read == 0 {
            return Err(AppError::Other("Empty TCP connection".to_string()));
        }

        let details = on_request(peer_ip, &buf[..total_read])?;
        let file_path = details.file_path;
        let file_size = details.file_size;
        let offset = details.offset;
        let task_id = details.task_id;

        // Open the file and seek to requested offset
        let mut file = match tokio::fs::File::open(&file_path).await {
            Ok(f) => f,
            Err(e) => {
                let err_msg = format!("Failed to open requested file: {}", e);
                progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
                return Err(AppError::Other(err_msg));
            }
        };

        if offset > 0 {
            if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                let err_msg = format!("Failed to seek file: {}", e);
                progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
                return Err(AppError::Other(err_msg));
            }
        }

        Self::copy_with_progress(
            file,
            stream,
            file_size,
            offset,
            task_id,
            progress_callback,
            shutdown,
            "Shutdown initiated during file upload",
        )
        .await?;

        println!(
            "[TCP SEND SUCCESS] Successfully sent file '{}' to peer {}",
            file_path.display(),
            peer_ip
        );

        Ok(())
    }
}
