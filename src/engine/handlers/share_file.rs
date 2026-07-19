use super::{CommandHandler, HandlerContext};
use crate::database::FileTaskRecord;
use crate::protocol::{IPMSG_FILEATTACHOPT, IPMSG_SENDMSG};
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct ShareFileHandler;

#[async_trait]
impl CommandHandler for ShareFileHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::ShareFile { peer_ip, path } = cmd {
            if !path.exists() {
                let err_msg = format!("Error: File path does not exist: {}", path.display());
                eprintln!("{}", err_msg);
                return Err(err_msg);
            }

            let file_name = match path.file_name() {
                Some(n) => n.to_string_lossy().to_string(),
                None => {
                    let err_msg = "Error: Invalid file name".to_string();
                    eprintln!("{}", err_msg);
                    return Err(err_msg);
                }
            };

            let metadata = match std::fs::metadata(&path) {
                Ok(meta) => meta,
                Err(e) => {
                    let err_msg = format!("Error reading metadata: {}", e);
                    eprintln!("{}", err_msg);
                    return Err(err_msg);
                }
            };

            let file_size = metadata.len();
            let file_mtime = metadata
                .modified()
                .and_then(|t| {
                    t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .map_err(std::io::Error::other)
                })
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let packet_no = ctx.network.next_packet_no();
            let file_id = 0u32;

            // Register file in TCP registry
            ctx.network.register_shared_file(
                packet_no,
                file_id,
                path.clone(),
                file_name.clone(),
                file_size,
            );

            // Save to SQLite
            let task_id = ctx.network.next_transfer_task_id();
            let task_record = FileTaskRecord {
                id: Some(task_id),
                file_name: file_name.clone(),
                file_size: file_size as i64,
                peer_ip: peer_ip.clone(),
                is_sending: true,
                status: crate::types::TransferStatus::Pending,
                progress: 0.0,
                timestamp: chrono::Utc::now().timestamp(),
            };

            if let Err(e) = ctx.db.create_file_task(task_record).await {
                eprintln!("Warning: Failed to save task record: {}", e);
            }

            // Format metadata using FileAttachment serialize and format_file_size helper
            let att = crate::protocol::FileAttachment {
                id: file_id,
                name: file_name.clone(),
                size: file_size,
                mtime: file_mtime,
                file_type: 1, // Regular file
                progress: 0.0,
                status: crate::types::TransferStatus::Pending,
            };
            let attachment_meta = crate::protocol::serialize_file_attachment(&att);
            let size_str = crate::protocol::format_file_size(file_size);

            let text_content = format!("Shared a file: {} ({})", file_name, size_str);
            let payload = format!("{}\0{}", text_content, attachment_meta);
            let cmd_flags = IPMSG_SENDMSG | IPMSG_FILEATTACHOPT;

            let port = ctx.network.get_peer_port(&peer_ip);
            let _ = ctx
                .network
                .send_packet_on_port(&peer_ip, port, cmd_flags, &payload)
                .await;
            println!("File metadata sent to {}!", peer_ip);
        }
        Ok(())
    }
}
