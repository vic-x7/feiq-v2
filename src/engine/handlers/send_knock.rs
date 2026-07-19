use super::{CommandHandler, HandlerContext};
use crate::database::MessageRecord;
use crate::protocol::IPMSG_KNOCK;
use crate::types::CoreCommand;
use async_trait::async_trait;

pub struct SendKnockHandler;

#[async_trait]
impl CommandHandler for SendKnockHandler {
    async fn handle(&self, cmd: CoreCommand, ctx: &HandlerContext) -> Result<(), String> {
        if let CoreCommand::SendKnock { peer_ip } = cmd {
            let port = ctx.network.get_peer_port(&peer_ip);
            let cmd_flags = IPMSG_KNOCK;
            match ctx
                .network
                .send_packet_on_port(&peer_ip, port, cmd_flags, "")
                .await
            {
                Ok(_packet_no) => {
                    let msg = MessageRecord {
                        id: None,
                        sender_ip: "0.0.0.0".to_string(), // Self
                        receiver_ip: peer_ip.clone(),
                        text_content: crate::types::NUDGE_MESSAGE_CONTENT.to_string(),
                        timestamp: chrono::Utc::now().timestamp(),
                        is_read: true,
                    };
                    if let Err(e) = ctx.db.save_message(msg).await {
                        eprintln!("Warning: Failed to save message: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to send window knock to {}: {}", peer_ip, e);
                    return Err(format!("Failed to send packet: {}", e));
                }
            }
        }
        Ok(())
    }
}
