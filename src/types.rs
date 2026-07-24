use std::fmt;
use std::path::PathBuf;

pub const LOCAL_USER_IDENTIFIER: &str = "me";
pub const NUDGE_MESSAGE_CONTENT: &str = "* sent a window nudge *";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Transferring,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
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
    pub is_directory: bool,
    pub task_id: i64,
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

pub fn format_file_size(bytes: u64) -> String {
    if bytes > 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} KB", bytes as f64 / 1024.0)
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

    #[test]
    fn test_format_file_size() {
        assert_eq!(format_file_size(512), "0.5 KB");
        assert_eq!(format_file_size(1024), "1.0 KB");
        assert_eq!(format_file_size(1024 * 1024 + 1), "1.0 MB");
    }
}
