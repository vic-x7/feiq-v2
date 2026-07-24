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
        is_directory: bool,
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

        if is_directory {
            download_directory_stream_tokio(&mut stream, &save_path, self.shutdown.clone()).await?;
            progress_callback(task_id, 1.0, crate::types::TransferStatus::Completed);
            return Ok(());
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

        // Check if the path is a directory (C1)
        let metadata = match tokio::fs::metadata(&file_path).await {
            Ok(m) => m,
            Err(e) => {
                progress_callback(task_id, 0.0, crate::types::TransferStatus::Failed);
                return Err(AppError::Other(format!("Failed to read file/folder metadata: {}", e)));
            }
        };

        if metadata.is_dir() {
            let name = file_path.file_name().unwrap_or_default().to_string_lossy().to_string();
            stream_directory_recursive(&mut stream, &file_path, &name, shutdown).await?;
            progress_callback(task_id, 1.0, crate::types::TransferStatus::Completed);
            println!(
                "[TCP SEND DIR SUCCESS] Successfully sent folder '{}' to peer {}",
                file_path.display(),
                peer_ip
            );
            return Ok(());
        }

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

fn format_dir_header(name: &str, file_size: u64, file_attr: u32) -> Vec<u8> {
    let name_escaped = name.replace(':', "::");
    let body = format!("{}:{:x}:{:x}:", name_escaped, file_size, file_attr);
    let mut header_size = body.len() + 2;
    loop {
        let hex_size = format!("{:x}", header_size);
        if hex_size.len() + 1 + body.len() == header_size {
            return format!("{}:{}", hex_size, body).into_bytes();
        }
        header_size = hex_size.len() + 1 + body.len();
    }
}

async fn stream_directory_recursive<W>(
    writer: &mut W,
    dir_path: &std::path::Path,
    dir_name: &str,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), AppError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(AppError::Other("Shutdown initiated during directory stream".to_string()));
    }

    // 1. Write the directory start header
    let header = format_dir_header(dir_name, 0, crate::protocol::IPMSG_FILE_DIR);
    writer.write_all(&header).await?;

    // 2. Read directory contents
    let mut entries = tokio::fs::read_dir(dir_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AppError::Other("Shutdown initiated during directory stream".to_string()));
        }

        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let file_type = entry.file_type().await?;

        if file_type.is_dir() {
            // Recurse into subdirectory
            Box::pin(stream_directory_recursive(writer, &path, &name, shutdown.clone())).await?;
        } else if file_type.is_file() {
            // Write regular file entry
            let metadata = entry.metadata().await?;
            let file_size = metadata.len();
            let header = format_dir_header(&name, file_size, crate::protocol::IPMSG_FILE_REGULAR);
            writer.write_all(&header).await?;

            // Open and write file contents
            let mut file = tokio::fs::File::open(&path).await?;
            tokio::io::copy(&mut file, writer).await?;
        }
    }

    // 3. Write directory end header (RETPARENT)
    let end_header = format_dir_header(".", 0, crate::protocol::IPMSG_FILE_RETPARENT);
    writer.write_all(&end_header).await?;

    Ok(())
}

async fn download_directory_stream_tokio<R>(
    reader: &mut R,
    base_save_path: &std::path::Path,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), AppError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    
    // Stack of active directory paths during recursion
    let mut dir_stack: Vec<std::path::PathBuf> = vec![base_save_path.to_path_buf()];

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AppError::Other("Shutdown initiated during directory download".to_string()));
        }

        // 1. Read the header size string until ':'
        let mut hex_size_bytes = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = reader.read(&mut byte).await?;
            if n == 0 {
                if dir_stack.is_empty() || dir_stack.len() == 1 {
                    return Ok(());
                }
                return Err(AppError::Other("Unexpected EOF in directory download stream".to_string()));
            }
            if byte[0] == b':' {
                break;
            }
            hex_size_bytes.push(byte[0]);
        }

        let hex_size_str = String::from_utf8(hex_size_bytes)
            .map_err(|e| AppError::Other(format!("Invalid non-UTF-8 header size: {}", e)))?;
        
        let header_size = usize::from_str_radix(&hex_size_str, 16)
            .map_err(|e| AppError::Other(format!("Failed to parse header size hex '{}': {}", hex_size_str, e)))?;

        // 2. Read the remainder of the header: H - (hex_size_str.len() + 1)
        let body_len = header_size - (hex_size_str.len() + 1);
        let mut body_bytes = vec![0u8; body_len];
        reader.read_exact(&mut body_bytes).await?;

        let body_str = String::from_utf8(body_bytes)
            .map_err(|e| AppError::Other(format!("Invalid non-UTF-8 header body: {}", e)))?;

        // The body format is: "filename:file_size_hex:file_attr_hex:"
        let trimmed_body = body_str.trim_end_matches(':');
        let parts: Vec<&str> = trimmed_body.rsplitn(3, ':').collect();
        if parts.len() != 3 {
            return Err(AppError::Other(format!("Malformed directory entry header body: {}", body_str)));
        }

        let file_attr = u32::from_str_radix(parts[0], 16)
            .map_err(|e| AppError::Other(format!("Invalid file_attr hex: {}", e)))?;
        let file_size = u64::from_str_radix(parts[1], 16)
            .map_err(|e| AppError::Other(format!("Invalid file_size hex: {}", e)))?;
        let filename = parts[2].replace("::", ":");

        // 3. Process the entry based on its attribute
        match file_attr {
            crate::protocol::IPMSG_FILE_DIR => {
                let target_dir = if dir_stack.len() == 1 && dir_stack[0].ends_with(&filename) {
                    dir_stack[0].clone()
                } else {
                    dir_stack.last().unwrap().join(&filename)
                };

                tokio::fs::create_dir_all(&target_dir).await?;
                dir_stack.push(target_dir);
            }
            crate::protocol::IPMSG_FILE_REGULAR => {
                let target_file = dir_stack.last().ok_or_else(|| AppError::Other("File entry without active directory context".to_string()))?.join(&filename);
                
                if let Some(parent) = target_file.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let mut file = tokio::fs::File::create(&target_file).await?;
                
                let mut bytes_to_copy = file_size;
                let mut buf = [0u8; 65536];
                while bytes_to_copy > 0 {
                    let chunk_size = std::cmp::min(bytes_to_copy, buf.len() as u64) as usize;
                    reader.read_exact(&mut buf[..chunk_size]).await?;
                    file.write_all(&buf[..chunk_size]).await?;
                    bytes_to_copy -= chunk_size as u64;
                }
            }
            crate::protocol::IPMSG_FILE_RETPARENT => {
                if dir_stack.len() > 1 {
                    dir_stack.pop();
                } else {
                    return Ok(());
                }
            }
            _ => {
                return Err(AppError::Other(format!("Unknown directory stream file_attr: {}", file_attr)));
            }
        }
    }
}
