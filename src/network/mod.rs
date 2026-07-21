pub mod engine;
pub mod fake_transport;
pub mod packet_dispatcher;
pub mod tokio_transport;
pub mod transport;
pub mod ack_tracker;
pub mod file_registry;
pub mod peer_directory;
pub mod validation;

pub use engine::{EngineStats, NetworkEngine};
pub use fake_transport::FakeTransport;
pub use packet_dispatcher::PacketDispatcher;
pub use tokio_transport::TokioTransport;
pub use transport::NetworkTransport;
pub use ack_tracker::AckTracker;
pub use file_registry::{FileRegistry, SharedFile};
pub use peer_directory::PeerDirectory;

use crate::types::FileDownloadRequest;
use crate::error::AppError;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

#[async_trait]
pub trait NetworkEngineTrait: Send + Sync + 'static {
    fn get_peer_port(&self, ip: &str) -> u16;
    async fn send_packet_on_port(
        &self,
        to_ip: &str,
        port: u16,
        cmd: u32,
        extra: &str,
    ) -> Result<u32, AppError>;
    async fn broadcast_online(&self) -> Result<(), AppError>;
    fn next_packet_no(&self) -> u32;
    fn register_shared_file(
        &self,
        packet_no: u32,
        file_id: u32,
        path: PathBuf,
        name: String,
        size: u64,
    );
    async fn download_file_direct(&self, req: FileDownloadRequest) -> Result<(), AppError>;
    fn update_identity(&self, username: String, hostname: String);
    async fn scan_subnet(self: Arc<Self>, subnet_prefix: &str, cancel: crate::types::CancellationToken);
    fn next_transfer_task_id(&self) -> i64;
}

#[async_trait]
pub trait PacketHandler: Send + Sync + 'static {
    async fn handle(
        &self,
        ctx: &PacketContext,
        packet: &crate::protocol::IPMsgPacket,
    ) -> Result<(), AppError>;
}

pub struct PacketContext {
    pub peer_ip_addr: std::net::IpAddr,
    pub engine: Arc<NetworkEngine>,
}

impl PacketContext {
    pub fn peer_ip(&self) -> String {
        self.peer_ip_addr.to_string()
    }

    pub fn ip_u32(&self) -> u32 {
        match self.peer_ip_addr {
            std::net::IpAddr::V4(ipv4) => u32::from(ipv4),
            _ => 0,
        }
    }
}
