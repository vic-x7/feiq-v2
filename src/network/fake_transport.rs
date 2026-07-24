use async_trait::async_trait;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};

use crate::error::AppError;
use crate::network::transport::{NetworkTransport, UploadRequestDetails};

type PacketQueue = mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>;

pub struct FakeTransport {
    local_addr: SocketAddr,
    incoming_rx: Arc<tokio::sync::Mutex<PacketQueue>>,
    incoming_tx: mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
    sent_udp: Arc<Mutex<Vec<(String, u16, Vec<u8>)>>>,
    stopped: Arc<std::sync::atomic::AtomicBool>,
    notify_stop: Arc<Notify>,
}

impl FakeTransport {
    pub fn new(local_addr: SocketAddr) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        Self {
            local_addr,
            incoming_rx: Arc::new(tokio::sync::Mutex::new(incoming_rx)),
            incoming_tx,
            sent_udp: Arc::new(Mutex::new(Vec::new())),
            stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            notify_stop: Arc::new(Notify::new()),
        }
    }

    /// Simulates receiving a UDP packet by queuing it up for `recv_udp` to consume.
    pub fn inject_incoming(&self, data: Vec<u8>, from: SocketAddr) {
        let _ = self.incoming_tx.send((data, from));
    }

    /// Returns a list of captured sent UDP packets.
    pub fn get_sent_udp(&self) -> Vec<(String, u16, Vec<u8>)> {
        self.sent_udp.lock().unwrap().clone()
    }

    /// Clears the captured sent UDP packets list.
    pub fn clear_sent_udp(&self) {
        self.sent_udp.lock().unwrap().clear();
    }
}

#[async_trait]
impl NetworkTransport for FakeTransport {
    async fn send_udp(&self, to_ip: &str, port: u16, data: &[u8]) -> Result<(), AppError> {
        if self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("Transport stopped".into());
        }
        self.sent_udp
            .lock()
            .unwrap()
            .push((to_ip.to_string(), port, data.to_vec()));
        Ok(())
    }

    async fn recv_udp(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), AppError> {
        if self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("Transport stopped".into());
        }
        let mut rx = self.incoming_rx.lock().await;

        // Prevent hanging indefinitely when transport is stopped from another thread
        match tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await {
            Ok(Some((data, addr))) => {
                let len = std::cmp::min(buf.len(), data.len());
                buf[..len].copy_from_slice(&data[..len]);
                Ok((len, addr))
            }
            Ok(None) => Err("No more packets available".into()),
            Err(_) => {
                if self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
                    Err("Transport stopped".into())
                } else {
                    Err("Receive timeout".into())
                }
            }
        }
    }

    async fn download_file(
        &self,
        _peer_ip: &str,
        _peer_port: u16,
        _request_data: Vec<u8>,
        _save_path: PathBuf,
        _file_size: u64,
        _is_directory: bool,
        _task_id: i64,
        _progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
    ) -> Result<(), AppError> {
        Ok(())
    }

    async fn run_tcp_server(
        &self,
        _on_request: Arc<
            dyn for<'a, 'b> Fn(&'a str, &'b [u8]) -> Result<UploadRequestDetails, AppError>
                + Send
                + Sync
                + 'static,
        >,
        _progress_callback: Arc<
            dyn Fn(i64, f64, crate::types::TransferStatus) + Send + Sync + 'static,
        >,
    ) -> Result<(), AppError> {
        // Wait efficiently for shutdown notification instead of burning CPU cycles in a busy-sleep loop
        self.notify_stop.notified().await;
        Ok(())
    }

    fn local_addr(&self) -> Result<SocketAddr, AppError> {
        Ok(self.local_addr)
    }

    fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.notify_stop.notify_one();
    }
}
