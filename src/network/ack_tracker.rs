use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::error::AppError;

#[derive(Clone)]
pub struct AckTracker {
    pending_acks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<()>>>>,
}

impl AckTracker {
    pub fn new() -> Self {
        Self {
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a oneshot sender to wait for the specified packet acknowledgement.
    pub async fn track(&self, packet_no: u32, tx: tokio::sync::oneshot::Sender<()>) {
        let mut pending = self.pending_acks.lock().await;
        pending.insert(packet_no, tx);
    }

    /// Signals acknowledgment of a packet, completing any pending wait.
    pub async fn ack(&self, packet_no: u32) -> bool {
        let mut pending = self.pending_acks.lock().await;
        if let Some(tx) = pending.remove(&packet_no) {
            let _ = tx.send(());
            true
        } else {
            false
        }
    }

    /// Explicitly cancels or removes a pending tracker.
    pub async fn remove(&self, packet_no: u32) {
        let mut pending = self.pending_acks.lock().await;
        pending.remove(&packet_no);
    }

    /// Retries sending a packet with ACK and timeout, handling the oneshot lifecycle cleanly.
    /// Addresses C14: old sender is explicitly removed from pending_acks before re-inserting,
    /// avoiding channel overwrite or dangling sender issues.
    pub async fn send_with_ack<F, Fut>(&self, packet_no: u32, send_fn: F) -> Result<(), AppError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), AppError>> + Send,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.track(packet_no, tx).await;

        send_fn().await?;

        tokio::select! {
            _ = rx => {
                Ok(())
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                // Retry once:
                // C14: Ensure the old sender is explicitly removed from pending_acks before re-inserting,
                // avoiding any nanosecond-scale channel overwrite or dangling sender issues.
                self.remove(packet_no).await;

                let (tx_retry, rx_retry) = tokio::sync::oneshot::channel();
                self.track(packet_no, tx_retry).await;

                send_fn().await?;

                tokio::select! {
                    _ = rx_retry => {
                        Ok(())
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                        self.remove(packet_no).await;
                        Err(crate::error::NetworkError::SendTimeout.into())
                    }
                }
            }
        }
    }
}
