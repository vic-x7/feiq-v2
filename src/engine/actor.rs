use crate::database::DbClient;
use crate::engine::handlers::{
    BroadcastPresenceHandler, CommandHandler, DownloadFileHandler, HandlerContext,
    RegisterSharedFileHandler, ScanSubnetHandler, SendMessageHandler, ShareFileHandler,
    UpdateIdentityHandler, SendKnockHandler,
};
use crate::network::{NetworkEngine, NetworkEngineTrait};
use crate::types::{CoreCommand, CoreEvent, CancellationToken};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;

pub struct CoreEngineActor {
    cmd_rx: Receiver<CoreCommand>,
    cmd_tx: tokio::sync::mpsc::Sender<CoreCommand>,
    network: Arc<NetworkEngine>,
    db: DbClient,
    event_tx: Sender<CoreEvent>,
    cancel: CancellationToken,
}

impl CoreEngineActor {
    pub fn new(
        cmd_rx: Receiver<CoreCommand>,
        cmd_tx: tokio::sync::mpsc::Sender<CoreCommand>,
        network: Arc<NetworkEngine>,
        db: DbClient,
        event_tx: Sender<CoreEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            cmd_rx,
            cmd_tx,
            network,
            db,
            event_tx,
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
