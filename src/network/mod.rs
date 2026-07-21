pub mod packet_io;
pub mod discovery;
pub mod fake_transport;
pub mod packet_dispatcher;
pub mod tokio_transport;
pub mod transport;
pub mod ack_tracker;
pub mod file_registry;
pub mod peer_directory;
pub mod validation;

pub use packet_io::{EngineStats, PacketIO};
pub use fake_transport::FakeTransport;
pub use packet_dispatcher::PacketDispatcher;
pub use tokio_transport::TokioTransport;
pub use transport::NetworkTransport;
pub use ack_tracker::AckTracker;
pub use file_registry::{FileRegistry, SharedFile};
pub use peer_directory::PeerDirectory;

use crate::error::AppError;
use async_trait::async_trait;
use std::sync::Arc;

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
    pub packet_io: Arc<PacketIO>,
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
