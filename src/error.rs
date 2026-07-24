#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("network error: {0}")]
    Network(#[from] NetworkError),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum NetworkError {
    #[error("bind failed: {0}")]
    BindFailed(String),
    #[error("send timeout")]
    SendTimeout,
    #[error("connection refused: {0}")]
    ConnectionRefused(String),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum ProtocolError {
    #[error("packet has too few fields (expected at least 5, got {0})")]
    TooFewFields(usize),
    #[error("failed to parse packet number: {0}")]
    InvalidPacketNo(String),
    #[error("failed to parse command: {0}")]
    InvalidCommand(String),
    #[error("failed to decode text content: {0}")]
    DecodeError(String),
}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        AppError::Other(s)
    }
}

impl From<&str> for AppError {
    fn from(s: &str) -> Self {
        AppError::Other(s.to_string())
    }
}

impl From<String> for NetworkError {
    fn from(s: String) -> Self {
        NetworkError::Other(s)
    }
}

impl From<&str> for NetworkError {
    fn from(s: &str) -> Self {
        NetworkError::Other(s.to_string())
    }
}
