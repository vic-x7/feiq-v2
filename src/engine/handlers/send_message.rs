use super::{CommandHandler, HandlerContext};
use crate::database::MessageRecord;
use crate::protocol::IPMSG_SENDMSG;
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct SendMessageHandler;

#[async_trait]
impl CommandHandler for SendMessageHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::SendMessage { to_ip, content } = cmd {
            let port = ctx.network.get_peer_port(&to_ip);
            let cmd_flags = IPMSG_SENDMSG;
            match ctx
                .network
                .send_packet_on_port(&to_ip, port, cmd_flags, &content)
                .await
            {
                Ok(_packet_no) => {
                    let msg = MessageRecord {
                        id: None,
                        sender_ip: "0.0.0.0".to_string(), // Self
                        receiver_ip: to_ip.clone(),
                        text_content: content.clone(),
                        timestamp: chrono::Utc::now().timestamp(),
                        is_read: true,
                    };
                    if let Err(e) = ctx.db.save_message(msg).await {
                        eprintln!("Warning: Failed to save message: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to send message to {}: {}", to_ip, e);
                    return Err(format!("Failed to send packet: {}", e));
                }
            }
        }
        Ok(())
    }
}
