use super::{CommandHandler, HandlerContext};
use crate::database::FileTaskRecord;
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct DownloadFileHandler;

#[async_trait]
impl CommandHandler for DownloadFileHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::DownloadFile {
            peer_ip,
            packet_no,
            file_id,
            name,
            size,
        } = cmd
        {
            let download_dir = ctx
                .db
                .get_config("download_dir".to_string())
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| "downloads".to_string());

            let task_id = ctx.network.next_transfer_task_id();

            let task = FileTaskRecord {
                id: Some(task_id),
                file_name: name.clone(),
                file_size: size as i64,
                peer_ip: peer_ip.clone(),
                is_sending: false, // Receiving
                status: crate::types::TransferStatus::Pending,
                progress: 0.0,
                timestamp: chrono::Utc::now().timestamp(),
            };
            if let Err(e) = ctx.db.create_file_task(task).await {
                let err_msg = format!("Failed to create file task in DB: {}", e);
                eprintln!("{}", err_msg);
                return Err(err_msg);
            }

            let network_clone = ctx.network.clone();
            let cancel_clone = ctx.cancel.clone();
            tokio::spawn(async move {
                if cancel_clone.is_cancelled() {
                    return;
                }
                let cache_dir = std::path::PathBuf::from(download_dir);
                if !cache_dir.is_dir() {
                    let _ = std::fs::create_dir_all(&cache_dir);
                }
                let save_path = cache_dir.join(&name);
                let download_req = crate::types::FileDownloadRequest {
                    peer_ip,
                    packet_no,
                    file_id,
                    save_path,
                    file_size: size,
                    task_id,
                };
                match network_clone.download_file_direct(download_req).await {
                    Ok(_) => {
                        println!("\n[DOWNLOAD SUCCESS] Download complete.");
                    }
                    Err(e) => {
                        eprintln!("\n[DOWNLOAD FAILED] Download of {} failed: {}", name, e);
                    }
                }
            });
        }
        Ok(())
    }
}
