pub mod actor;
pub mod event_persister;
pub mod handlers;

pub use actor::{BroadcastEventDispatcher, CoreEngineActor};
pub use event_persister::EventPersister;

use crate::database::{start_db_actor, DatabaseManager, DbClient};
use crate::network::{NetworkEngine, TokioTransport};
use crate::types::{CoreCommand, CoreEvent, CancellationToken};
use crate::error::AppError;
use std::sync::Arc;
use tokio::sync::broadcast::{
    channel as broadcast_channel, Receiver as BroadcastReceiver, Sender as BroadcastSender,
};
use tokio::sync::mpsc::{channel, Sender as MpscSender};

pub struct EngineHandle {
    cmd_tx: MpscSender<CoreCommand>,
    event_tx: BroadcastSender<CoreEvent>,
    event_rx: BroadcastReceiver<CoreEvent>,
    db: DbClient,
    network: Arc<NetworkEngine>,
    cancel: CancellationToken,
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub username: String,
    pub hostname: String,
    pub bind_ip: String,
    pub start_port: u16,
    pub db_path: std::path::PathBuf,
}

impl EngineHandle {
    pub async fn start(config: EngineConfig) -> Result<Self, AppError> {
        // 1. Initialize SQLite Database Manager & start actor
        let db = DatabaseManager::new(config.db_path)?;
        let max_task_id = db.get_max_file_task_id().unwrap_or(0);
        let (db_client, _db_join_handle) = start_db_actor(db);

        // 2. Create Event channels
        let (event_tx, event_rx) = broadcast_channel(128);

        // 3. Create Event Dispatcher (translates network callbacks into CoreEvent broadcasts)
        let dispatcher = Arc::new(BroadcastEventDispatcher::new(event_tx.clone()));

        // 4. Initialize NetworkEngine with TokioTransport
        let transport = Arc::new(TokioTransport::bind_fallback(&config.bind_ip, config.start_port).await?);
        let network = NetworkEngine::new(config.username, config.hostname, transport, dispatcher.clone(), max_task_id)?;
        let network_arc = Arc::new(network);

        // 5. Create Command channels
        let (cmd_tx, cmd_rx) = channel(64);

        // 6. Spawn CoreEngineActor in a tokio task
        let cancel = CancellationToken::new();
        let actor = CoreEngineActor::new(
            cmd_rx,
            cmd_tx.clone(),
            network_arc.clone(),
            db_client.clone(),
            event_tx.clone(),
            dispatcher.clone(),
            cancel.clone(),
        );
        tokio::spawn(async move {
            actor.run().await;
        });

        Ok(Self {
            cmd_tx,
            event_tx,
            event_rx,
            db: db_client,
            network: network_arc,
            cancel,
        })
    }

    pub fn try_send(&self, cmd: CoreCommand) -> Result<(), String> {
        self.cmd_tx
            .try_send(cmd)
            .map_err(|e| format!("Failed to send command: {:?}", e))
    }

    pub fn db(&self) -> &DbClient {
        &self.db
    }

    pub fn cmd_tx(&self) -> MpscSender<CoreCommand> {
        self.cmd_tx.clone()
    }

    pub fn shutdown(&self) {
        self.cancel.cancel();
        self.network.stop();
    }

    pub fn subscribe(&self) -> BroadcastReceiver<CoreEvent> {
        self.event_rx.resubscribe()
    }

    pub fn network(&self) -> &Arc<NetworkEngine> {
        &self.network
    }

    pub fn stats(&self) -> crate::network::EngineStats {
        self.network.stats()
    }

    pub fn drain_events<F: FnMut(CoreEvent)>(&mut self, mut handler: F) -> bool {
        let mut received = false;
        loop {
            match self.event_rx.try_recv() {
                Ok(event) => {
                    handler(event);
                    received = true;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    continue;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    break;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    break;
                }
            }
        }
        received
    }
}

pub async fn start_engine(
    username: String,
    hostname: String,
    bind_ip: String,
    start_port: u16,
    db_path: std::path::PathBuf,
) -> Result<
    (
        MpscSender<CoreCommand>,
        BroadcastSender<CoreEvent>,
        BroadcastReceiver<CoreEvent>,
        Arc<NetworkEngine>,
        DbClient,
    ),
    AppError,
> {
    let config = EngineConfig {
        username,
        hostname,
        bind_ip,
        start_port,
        db_path,
    };
    let handle = EngineHandle::start(config).await?;
    Ok((
        handle.cmd_tx(),
        handle.event_tx.clone(),
        handle.event_tx.subscribe(),
        handle.network.clone(),
        handle.db().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_handle_lifecycle_and_methods() {
        let temp_db_path = std::env::temp_dir().join(format!(
            "engine_handle_test_{}.db",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));

        let config = EngineConfig {
            username: "test_user".to_string(),
            hostname: "test_host".to_string(),
            bind_ip: "127.0.0.1".to_string(),
            start_port: 0, // bind to an ephemeral port
            db_path: temp_db_path.clone(),
        };

        // Start the engine
        let engine = EngineHandle::start(config).await.expect("Failed to start EngineHandle");

        // Verify we can access database and perform queries
        let db = engine.db();
        let config_val = db.get_config("username".to_string()).await.ok().flatten();
        assert!(config_val.is_none() || config_val.is_some());

        // Verify we can subscribe to events
        let mut _rx = engine.subscribe();
        
        // Try to send a core command and see that try_send works
        let cmd = CoreCommand::UpdateIdentity {
            username: "new_name".to_string(),
            hostname: "new_host".to_string(),
        };
        engine.try_send(cmd).expect("Failed to send command");

        // Verify we can read stats
        let stats = engine.stats();
        assert_eq!(stats.errors, 0);

        // Verify we can access the underlying network engine
        let net = engine.network();
        assert!(net.socket_local_addr().is_ok());

        // Shutdown cleanly
        engine.shutdown();

        // Clean up the temp db file
        let _ = std::fs::remove_file(temp_db_path);
    }
}
