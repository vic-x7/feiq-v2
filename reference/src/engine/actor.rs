use crate::database::DbClient;
use crate::engine::handlers::{
    BroadcastPresenceHandler, CommandHandler, DownloadFileHandler, HandlerContext,
    RegisterSharedFileHandler, ScanSubnetHandler, SendMessageHandler, ShareFileHandler,
    UpdateIdentityHandler, SendKnockHandler,
};
use crate::network::{NetworkEngine, NetworkEngineTrait, NetworkEvents};
use crate::protocol::FileAttachment;
use crate::types::{CoreCommand, CoreEvent, CancellationToken};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;

pub struct BroadcastEventDispatcher {
    pub event_tx: Sender<CoreEvent>,
}

impl BroadcastEventDispatcher {
    pub fn new(event_tx: Sender<CoreEvent>) -> Self {
        Self {
            event_tx,
        }
    }
}

impl NetworkEvents for BroadcastEventDispatcher {
    fn on_peer_status_changed(
        &self,
        ip: String,
        username: String,
        hostname: String,
        nickname: Option<String>,
        online: bool,
    ) {
        let _ = self.event_tx.send(CoreEvent::PeerStatusChanged {
            ip,
            username,
            hostname,
            nickname,
            online,
        });
    }

    fn on_message_received(
        &self,
        sender_ip: String,
        text_content: String,
        timestamp: i64,
        username: String,
    ) {
        let _ = self.event_tx.send(CoreEvent::MessageReceived {
            id: 0,
            sender_ip,
            content: text_content,
            timestamp,
            username,
        });
    }

    fn on_file_attachments_received(
        &self,
        sender_ip: String,
        packet_no: u32,
        files: Vec<FileAttachment>,
    ) {
        let _ = self.event_tx.send(CoreEvent::FileAttachmentsReceived {
            sender_ip,
            packet_no,
            files,
        });
    }

    fn on_window_knock(&self, sender_ip: String, username: String) {
        let _ = self.event_tx.send(CoreEvent::WindowKnock {
            sender_ip,
            username,
        });
    }

    fn on_peer_typing(&self, sender_ip: String, typing: bool) {
        let _ = self
            .event_tx
            .send(CoreEvent::PeerTyping { sender_ip, typing });
    }

    fn on_transfer_progress(
        &self,
        task_id: i64,
        progress: f64,
        status: crate::types::TransferStatus,
    ) {
        let _ = self.event_tx.send(CoreEvent::TransferProgress {
            task_id,
            progress,
            status,
        });
    }

    fn on_transfer_started(
        &self,
        task_id: i64,
        peer_ip: String,
        file_name: String,
        file_size: i64,
        is_sending: bool,
    ) {
        let _ = self.event_tx.send(CoreEvent::TransferStarted {
            task_id,
            peer_ip,
            file_name,
            file_size,
            is_sending,
        });
    }
}

pub struct CoreEngineActor {
    cmd_rx: Receiver<CoreCommand>,
    cmd_tx: tokio::sync::mpsc::Sender<CoreCommand>,
    network: Arc<NetworkEngine>,
    db: DbClient,
    event_tx: Sender<CoreEvent>,
    dispatcher: Arc<BroadcastEventDispatcher>,
    cancel: CancellationToken,
}

impl CoreEngineActor {
    pub fn new(
        cmd_rx: Receiver<CoreCommand>,
        cmd_tx: tokio::sync::mpsc::Sender<CoreCommand>,
        network: Arc<NetworkEngine>,
        db: DbClient,
        event_tx: Sender<CoreEvent>,
        dispatcher: Arc<BroadcastEventDispatcher>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            cmd_rx,
            cmd_tx,
            network,
            db,
            event_tx,
            dispatcher,
            cancel,
        }
    }

    pub async fn run(mut self) {
        // Subscribe to event_tx to perform database persistence in the background
        let mut event_rx = self.event_tx.subscribe();
        let persister = crate::engine::EventPersister::new(self.db.clone());
        let cancel_persist = self.cancel.clone();
        let persist_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_persist.cancelled() => {
                        break;
                    }
                    res = event_rx.recv() => {
                        match res {
                            Ok(event) => persister.persist(&event).await,
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        // Spawn network engine receive loop inside actor context (this also starts run_tcp_server)
        let network_clone = self.network.clone();
        let cancel_net = self.cancel.clone();
        let net_handle = tokio::spawn(async move {
            network_clone.start_receive_loop(cancel_net).await;
        });

        // Discover local peers immediately
        if let Err(e) = self.network.broadcast_online().await {
            eprintln!("Warning: Failed to broadcast presence: {}", e);
        }

        let ctx = HandlerContext {
            network: self.network.clone() as Arc<dyn NetworkEngineTrait>,
            db: self.db.clone(),
            event_tx: self.event_tx.clone(),
            cmd_tx: self.cmd_tx.clone(),
            dispatcher: self.dispatcher.clone(),
            cancel: self.cancel.clone(),
        };

        while let Some(cmd) = self.cmd_rx.recv().await {
            let res = match &cmd {
                CoreCommand::SendMessage { .. } => SendMessageHandler.handle(cmd, &ctx).await,
                CoreCommand::BroadcastPresence => BroadcastPresenceHandler.handle(cmd, &ctx).await,
                CoreCommand::RegisterSharedFile { .. } => {
                    RegisterSharedFileHandler.handle(cmd, &ctx).await
                }
                CoreCommand::DownloadFile { .. } => DownloadFileHandler.handle(cmd, &ctx).await,
                CoreCommand::UpdateIdentity { .. } => UpdateIdentityHandler.handle(cmd, &ctx).await,
                CoreCommand::ScanSubnet { .. } => ScanSubnetHandler.handle(cmd, &ctx).await,
                CoreCommand::ShareFile { .. } => ShareFileHandler.handle(cmd, &ctx).await,
                CoreCommand::SendKnock { .. } => SendKnockHandler.handle(cmd, &ctx).await,
            };
            if let Err(e) = res {
                eprintln!("Error handling command: {}", e);
            }
        }

        // When the loop exits (the actor shuts down):
        self.cancel.cancel();
        self.network.stop();
        let _ = tokio::join!(persist_handle, net_handle);
    }
}
