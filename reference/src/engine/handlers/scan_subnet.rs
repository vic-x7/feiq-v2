use super::{CommandHandler, HandlerContext};
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct ScanSubnetHandler;

#[async_trait]
impl CommandHandler for ScanSubnetHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::ScanSubnet { subnet } = cmd {
            let network_inner = ctx.network.clone();
            let cancel_clone = ctx.cancel.clone();
            tokio::spawn(async move {
                if cancel_clone.is_cancelled() {
                    return;
                }
                network_inner.scan_subnet(&subnet, cancel_clone).await;
                println!("\n[SCAN] Subnet scan sent for {}", subnet);
            });
        }
        Ok(())
    }
}
