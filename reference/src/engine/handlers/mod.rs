use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::mpsc::Sender as MpscSender;

use super::actor::BroadcastEventDispatcher;
use crate::database::DbClient;
use crate::network::NetworkEngineTrait;
use crate::types::{CoreCommand, CoreEvent, CancellationToken};

pub mod broadcast_presence;
pub mod download_file;
pub mod register_shared_file;
pub mod scan_subnet;
pub mod send_message;
pub mod share_file;
pub mod update_identity;
pub mod send_knock;

pub use broadcast_presence::BroadcastPresenceHandler;
pub use download_file::DownloadFileHandler;
pub use register_shared_file::RegisterSharedFileHandler;
pub use scan_subnet::ScanSubnetHandler;
pub use send_message::SendMessageHandler;
pub use share_file::ShareFileHandler;
pub use update_identity::UpdateIdentityHandler;
pub use send_knock::SendKnockHandler;

#[derive(Clone)]
pub struct HandlerContext {
    pub network: Arc<dyn NetworkEngineTrait>,
    pub db: DbClient,
    pub event_tx: BroadcastSender<CoreEvent>,
    pub cmd_tx: MpscSender<CoreCommand>,
    pub dispatcher: Arc<BroadcastEventDispatcher>,
    pub cancel: CancellationToken,
}

#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String>;
}
