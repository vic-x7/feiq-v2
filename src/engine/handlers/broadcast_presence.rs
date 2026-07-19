use super::{CommandHandler, HandlerContext};
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct BroadcastPresenceHandler;

#[async_trait]
impl CommandHandler for BroadcastPresenceHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::BroadcastPresence = cmd {
            if let Err(e) = ctx.network.broadcast_online().await {
                return Err(format!("Failed to broadcast presence: {}", e));
            }
        }
        Ok(())
    }
}
