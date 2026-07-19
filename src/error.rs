#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("network error: {0}")]
    Network(#[from] NetworkError),
    #[error("database error: {0}")]
    Database(#[from] DatabaseError),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, thiserror::Error)]
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

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("{0}")]
    Other(String),
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

impl From<rusqlite::Error> for AppError {
    fn from(e: rusqlite::Error) -> Self {
        AppError::Database(DatabaseError::Sqlite(e))
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

impl From<String> for DatabaseError {
    fn from(s: String) -> Self {
        DatabaseError::Other(s)
    }
}

impl From<&str> for DatabaseError {
    fn from(s: &str) -> Self {
        DatabaseError::Other(s.to_string())
    }
}
