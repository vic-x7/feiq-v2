use super::{CommandHandler, HandlerContext};
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct RegisterSharedFileHandler;

#[async_trait]
impl CommandHandler for RegisterSharedFileHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::RegisterSharedFile { path } = cmd {
            if path.exists() {
                let name = path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let size = path.metadata().map(|m| m.len()).unwrap_or(0);
                let packet_no = ctx.network.next_packet_no();
                let file_id = 0; // Standard single file ID
                ctx.network.register_shared_file(
                    packet_no,
                    file_id,
                    path.clone(),
                    name.clone(),
                    size,
                );
                println!(
                    "Registered shared file: {} ({} bytes) under packet_no: {}, file_id: {}",
                    path.display(),
                    size,
                    packet_no,
                    file_id
                );
            } else {
                let err_msg = format!("File does not exist: {}", path.display());
                eprintln!("{}", err_msg);
                return Err(err_msg);
            }
        }
        Ok(())
    }
}
