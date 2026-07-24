use super::{CommandHandler, HandlerContext};
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct UpdateIdentityHandler;

#[async_trait]
impl CommandHandler for UpdateIdentityHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::UpdateIdentity { username, hostname } = cmd {
            ctx.network
                .update_identity(username.clone(), hostname.clone());
            if let Err(e) = ctx
                .db
                .save_config("username".to_string(), username.clone())
                .await
            {
                eprintln!("Warning: Failed to save config username: {}", e);
            }
            if let Err(e) = ctx
                .db
                .save_config("hostname".to_string(), hostname.clone())
                .await
            {
                eprintln!("Warning: Failed to save config hostname: {}", e);
            }
            println!("Identity updated to {}@{}", username, hostname);
        }
        Ok(())
    }
}
