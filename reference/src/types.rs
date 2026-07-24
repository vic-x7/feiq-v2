use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

pub const LOCAL_USER_IDENTIFIER: &str = "me";
pub const NUDGE_MESSAGE_CONTENT: &str = "* 发送了一个窗口抖动 *";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Pending,
    Transferring,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileAttachment {
    pub id: u32,
    pub name: String,
    pub size: u64,
    pub mtime: u64,
    pub file_type: u32,
    pub progress: f64,
    pub status: TransferStatus,
}

impl fmt::Display for TransferStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferStatus::Pending => write!(f, "pending"),
            TransferStatus::Transferring => write!(f, "transferring"),
            TransferStatus::Completed => write!(f, "completed"),
            TransferStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for TransferStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(TransferStatus::Pending),
            "transferring" => Ok(TransferStatus::Transferring),
            "completed" => Ok(TransferStatus::Completed),
            "failed" => Ok(TransferStatus::Failed),
            _ => Err(format!("Unknown TransferStatus: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileDownloadRequest {
    pub peer_ip: String,
    pub packet_no: u32,
    pub file_id: u32,
    pub save_path: PathBuf,
    pub file_size: u64,
    pub task_id: i64,
}

#[derive(Debug, Clone)]
pub enum CoreCommand {
    SendMessage {
        to_ip: String,
        content: String,
    },
    BroadcastPresence,
    RegisterSharedFile {
        path: PathBuf,
    },
    DownloadFile {
        peer_ip: String,
        packet_no: u32,
        file_id: u32,
        name: String,
        size: u64,
    },
    UpdateIdentity {
        username: String,
        hostname: String,
    },
    ScanSubnet {
        subnet: String,
    },
    ShareFile {
        peer_ip: String,
        path: PathBuf,
    },
    SendKnock {
        peer_ip: String,
    },
}

#[derive(Debug, Clone)]
pub enum CoreEvent {
    PeerStatusChanged {
        ip: String,
        username: String,
        hostname: String,
        nickname: Option<String>,
        online: bool,
    },
    MessageReceived {
        id: i64,
        sender_ip: String,
        content: String,
        timestamp: i64,
        username: String,
    },
    FileAttachmentsReceived {
        sender_ip: String,
        packet_no: u32,
        files: Vec<FileAttachment>,
    },
    WindowKnock {
        sender_ip: String,
        username: String,
    },
    PeerTyping {
        sender_ip: String,
        typing: bool,
    },
    TransferProgress {
        task_id: i64,
        progress: f64,
        status: TransferStatus,
    },
    TransferStarted {
        task_id: i64,
        peer_ip: String,
        file_name: String,
        file_size: i64,
        is_sending: bool,
    },
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: std::sync::Arc<CancellationTokenInner>,
}

#[derive(Debug)]
struct CancellationTokenInner {
    tx: tokio::sync::watch::Sender<bool>,
    rx: tokio::sync::watch::Receiver<bool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self {
            inner: std::sync::Arc::new(CancellationTokenInner { tx, rx }),
        }
    }

    pub fn cancel(&self) {
        let _ = self.inner.tx.send(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.inner.rx.borrow()
    }

    pub async fn cancelled(&self) {
        let mut rx = self.inner.rx.clone();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cancellation_token() {
        let cancel = CancellationToken::new();
        assert!(!cancel.is_cancelled());

        let cancel_clone = cancel.clone();
        assert!(!cancel_clone.is_cancelled());

        cancel.cancel();
        assert!(cancel.is_cancelled());
        assert!(cancel_clone.is_cancelled());

        // Test awaiting cancelled
        cancel_clone.cancelled().await; // should return immediately
    }
}
