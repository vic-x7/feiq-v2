use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct UploadRequestDetails {
    pub file_path: PathBuf,
    pub file_size: u64,
    pub offset: u64,
    pub task_id: i64,
}

#[async_trait]
pub trait NetworkTransport: Send + Sync {
    async fn send_udp(&self, to_ip: &str, port: u16, data: &[u8]) -> Result<(), AppError>;
    async fn recv_udp(&self, buf: &mut [u8]) -> Result<(usize, std::net::SocketAddr), AppError>;

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
    ) -> Result<(), AppError>;

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
    ) -> Result<(), AppError>;

    fn local_addr(&self) -> Result<std::net::SocketAddr, AppError>;
    fn stop(&self);
}
