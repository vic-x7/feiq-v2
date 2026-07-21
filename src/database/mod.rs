use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::error::AppError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PeerRecord {
    pub ip: String,
    pub username: String,
    pub hostname: String,
    pub nickname: Option<String>,
    pub avatar_id: Option<String>,
    pub last_seen: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageRecord {
    pub id: Option<i64>,
    pub sender_ip: String,
    pub receiver_ip: String,
    pub text_content: String,
    pub timestamp: i64,
    pub is_read: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileTaskRecord {
    pub id: Option<i64>,
    pub file_name: String,
    pub file_size: i64,
    pub peer_ip: String,
    pub is_sending: bool, // true for sending, false for receiving
    pub status: crate::types::TransferStatus,
    pub progress: f64,
    pub timestamp: i64,
}

pub struct DatabaseManager {
    conn: Connection,
}

impl DatabaseManager {
    pub fn new(db_path: PathBuf) -> Result<Self, AppError> {
        let conn = Connection::open(db_path).map_err(|e| AppError::from(e))?;

        // Enable high-concurrency Write-Ahead Logging (WAL) and set synchronous to NORMAL
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .map_err(|e| AppError::Other(format!("Failed to configure SQLite WAL: {}", e)))?;

        let manager = DatabaseManager { conn };
        manager.run_migrations()?;
        Ok(manager)
    }

    fn run_migrations(&self) -> Result<(), AppError> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_ip TEXT NOT NULL,
                receiver_ip TEXT NOT NULL,
                text_content TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                is_read INTEGER DEFAULT 0
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("Messages migration failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(sender_ip, receiver_ip)",
                [],
            )
            .map_err(|e| AppError::Other(format!("Messages index failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS peers (
                ip TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                hostname TEXT NOT NULL,
                nickname TEXT,
                avatar_id TEXT,
                last_seen INTEGER NOT NULL
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("Peers migration failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS file_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                peer_ip TEXT NOT NULL,
                is_sending INTEGER NOT NULL,
                status TEXT NOT NULL,
                progress REAL DEFAULT 0.0,
                timestamp INTEGER NOT NULL
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("File tasks migration failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS subnet_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subnet_prefix TEXT UNIQUE NOT NULL
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("Subnet config migration failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("App config migration failed: {}", e)))?;

        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS active_sessions (
                peer_ip TEXT PRIMARY KEY,
                last_updated_at INTEGER NOT NULL
            )",
                [],
            )
            .map_err(|e| AppError::Other(format!("Active sessions migration failed: {}", e)))?;

        Ok(())
    }

    // --- Peer Operations ---
    pub fn save_peer(&self, peer: &PeerRecord) -> Result<(), AppError> {
        self.conn
            .execute(
                "INSERT INTO peers (ip, username, hostname, nickname, avatar_id, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(ip) DO UPDATE SET
                username=excluded.username,
                hostname=excluded.hostname,
                nickname=COALESCE(excluded.nickname, peers.nickname),
                last_seen=excluded.last_seen",
                params![
                    peer.ip,
                    peer.username,
                    peer.hostname,
                    peer.nickname,
                    peer.avatar_id,
                    peer.last_seen
                ],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(())
    }

    pub fn get_peers(&self) -> Result<Vec<PeerRecord>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT ip, username, hostname, nickname, avatar_id, last_seen FROM peers")
            .map_err(|e| AppError::from(e))?;
        let peer_iter = stmt
            .query_map([], |row| {
                Ok(PeerRecord {
                    ip: row.get(0)?,
                    username: row.get(1)?,
                    hostname: row.get(2)?,
                    nickname: row.get(3)?,
                    avatar_id: row.get(4)?,
                    last_seen: row.get(5)?,
                })
            })
            .map_err(|e| AppError::from(e))?;

        let mut results = Vec::new();
        for p in peer_iter.flatten() {
            results.push(p);
        }
        Ok(results)
    }

    // --- Message Operations ---
    pub fn save_message(&self, msg: &MessageRecord) -> Result<i64, AppError> {
        self.conn
            .execute(
                "INSERT INTO messages (sender_ip, receiver_ip, text_content, timestamp, is_read)
             VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    msg.sender_ip,
                    msg.receiver_ip,
                    msg.text_content,
                    msg.timestamp,
                    if msg.is_read { 1 } else { 0 }
                ],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_chat_history(
        &self,
        peer_ip: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MessageRecord>, AppError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, sender_ip, receiver_ip, text_content, timestamp, is_read FROM messages
             WHERE (sender_ip = ?1 OR receiver_ip = ?1)
             ORDER BY timestamp DESC LIMIT ?2 OFFSET ?3",
            )
            .map_err(|e| AppError::from(e))?;

        let msg_iter = stmt
            .query_map(params![peer_ip, limit, offset], |row| {
                let is_read_val: i32 = row.get(5)?;
                Ok(MessageRecord {
                    id: Some(row.get(0)?),
                    sender_ip: row.get(1)?,
                    receiver_ip: row.get(2)?,
                    text_content: row.get(3)?,
                    timestamp: row.get(4)?,
                    is_read: is_read_val != 0,
                })
            })
            .map_err(|e| AppError::from(e))?;

        let mut results = Vec::new();
        for m in msg_iter.flatten() {
            results.push(m);
        }
        // Reverse so they are returned in chronological order
        results.reverse();
        Ok(results)
    }

    // --- Subnet Config Operations ---
    pub fn save_subnets(&self, subnets: &Vec<String>) -> Result<(), AppError> {
        // Clear old config and insert new config inside a transaction
        // We will just do a simple delete & batch insert on self.conn
        // Since we are inside a Mutex context we don't have to worry about race conditions.
        self.conn
            .execute("DELETE FROM subnet_config", [])
            .map_err(|e| AppError::from(e))?;
        for subnet in subnets {
            self.conn
                .execute(
                    "INSERT INTO subnet_config (subnet_prefix) VALUES (?1)",
                    params![subnet],
                )
                .map_err(|e| AppError::from(e))?;
        }
        Ok(())
    }

    pub fn get_subnets(&self) -> Result<Vec<String>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT subnet_prefix FROM subnet_config")
            .map_err(|e| AppError::from(e))?;
        let subnet_iter = stmt
            .query_map([], |row| {
                let prefix: String = row.get(0)?;
                Ok(prefix)
            })
            .map_err(|e| AppError::from(e))?;

        let mut results = Vec::new();
        for s in subnet_iter.flatten() {
            results.push(s);
        }
        Ok(results)
    }

    // --- File Task Operations ---
    pub fn create_file_task(&self, task: &FileTaskRecord) -> Result<i64, AppError> {
        if let Some(predefined_id) = task.id {
            self.conn.execute(
                "INSERT INTO file_tasks (id, file_name, file_size, peer_ip, is_sending, status, progress, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    predefined_id,
                    task.file_name,
                    task.file_size,
                    task.peer_ip,
                    if task.is_sending { 1 } else { 0 },
                    task.status.to_string(),
                    task.progress,
                    task.timestamp
                ],
            ).map_err(|e| AppError::from(e))?;
            Ok(predefined_id)
        } else {
            self.conn.execute(
                "INSERT INTO file_tasks (file_name, file_size, peer_ip, is_sending, status, progress, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    task.file_name,
                    task.file_size,
                    task.peer_ip,
                    if task.is_sending { 1 } else { 0 },
                    task.status.to_string(),
                    task.progress,
                    task.timestamp
                ],
            ).map_err(|e| AppError::from(e))?;
            Ok(self.conn.last_insert_rowid())
        }
    }

    pub fn update_file_task_progress(
        &self,
        id: i64,
        progress: f64,
        status: crate::types::TransferStatus,
    ) -> Result<(), AppError> {
        self.conn
            .execute(
                "UPDATE file_tasks SET progress = ?1, status = ?2 WHERE id = ?3",
                params![progress, status.to_string(), id],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(())
    }

    pub fn get_file_task_status(
        &self,
        id: i64,
    ) -> Result<Option<(crate::types::TransferStatus, f64)>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT status, progress FROM file_tasks WHERE id = ?1")
            .map_err(|e| AppError::from(e))?;
        let mut rows = stmt.query(params![id]).map_err(|e| AppError::from(e))?;
        if let Some(row) = rows.next().map_err(|e| AppError::from(e))? {
            let status_str: String = row.get(0).map_err(|e| AppError::from(e))?;
            let progress: f64 = row.get(1).map_err(|e| AppError::from(e))?;
            use std::str::FromStr;
            let status =
                crate::types::TransferStatus::from_str(&status_str).map_err(|e| AppError::Other(e))?;
            Ok(Some((status, progress)))
        } else {
            Ok(None)
        }
    }

    // --- App Config Operations ---
    pub fn save_config(&self, key: &str, value: &str) -> Result<(), AppError> {
        self.conn
            .execute(
                "INSERT INTO app_config (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(())
    }

    pub fn get_config(&self, key: &str) -> Result<Option<String>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM app_config WHERE key = ?1")
            .map_err(|e| AppError::from(e))?;
        let mut rows = stmt.query(params![key]).map_err(|e| AppError::from(e))?;
        if let Some(row) = rows.next().map_err(|e| AppError::from(e))? {
            let val: String = row.get(0).map_err(|e| AppError::from(e))?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn get_total_messages_count(&self) -> Result<i64, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM messages")
            .map_err(|e| AppError::from(e))?;
        let count: i64 = stmt
            .query_row([], |row| row.get(0))
            .map_err(|e| AppError::from(e))?;
        Ok(count)
    }

    pub fn get_total_file_transfers_count(&self) -> Result<i64, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM file_tasks")
            .map_err(|e| AppError::from(e))?;
        let count: i64 = stmt
            .query_row([], |row| row.get(0))
            .map_err(|e| AppError::from(e))?;
        Ok(count)
    }

    pub fn get_max_file_task_id(&self) -> Result<i64, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT IFNULL(MAX(id), 0) FROM file_tasks")
            .map_err(|e| AppError::from(e))?;
        let max_id: i64 = stmt
            .query_row([], |row| row.get(0))
            .map_err(|e| AppError::from(e))?;
        Ok(max_id)
    }

    // --- Active Sessions Operations ---
    pub fn save_active_session(&self, peer_ip: &str, last_updated_at: i64) -> Result<(), AppError> {
        self.conn
            .execute(
                "INSERT INTO active_sessions (peer_ip, last_updated_at)
                 VALUES (?1, ?2)
                 ON CONFLICT(peer_ip) DO UPDATE SET last_updated_at=excluded.last_updated_at",
                params![peer_ip, last_updated_at],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(())
    }

    pub fn get_active_sessions(&self) -> Result<Vec<String>, AppError> {
        let mut stmt = self
            .conn
            .prepare("SELECT peer_ip FROM active_sessions ORDER BY last_updated_at DESC")
            .map_err(|e| AppError::from(e))?;
        let ip_iter = stmt
            .query_map([], |row| {
                let ip: String = row.get(0)?;
                Ok(ip)
            })
            .map_err(|e| AppError::from(e))?;

        let mut results = Vec::new();
        for i in ip_iter.flatten() {
            results.push(i);
        }
        Ok(results)
    }

    pub fn delete_active_session(&self, peer_ip: &str) -> Result<(), AppError> {
        self.conn
            .execute(
                "DELETE FROM active_sessions WHERE peer_ip = ?1",
                params![peer_ip],
            )
            .map_err(|e| AppError::from(e))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct DbClient {
    db: std::sync::Arc<std::sync::Mutex<DatabaseManager>>,
}

impl std::fmt::Debug for DbClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbClient").finish_non_exhaustive()
    }
}

impl DbClient {
    pub fn new(manager: DatabaseManager) -> Self {
        Self {
            db: std::sync::Arc::new(std::sync::Mutex::new(manager)),
        }
    }

    async fn run_blocking<F, R>(&self, f: F) -> Result<R, AppError>
    where
        F: FnOnce(&DatabaseManager) -> Result<R, AppError> + Send + 'static,
        R: Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let guard = db.lock().unwrap();
            f(&*guard)
        })
        .await
        .map_err(|e| AppError::Other(e.to_string()))?
    }

    pub async fn save_peer(&self, peer: PeerRecord) -> Result<(), AppError> {
        self.run_blocking(move |db| db.save_peer(&peer)).await
    }

    pub async fn get_peers(&self) -> Result<Vec<PeerRecord>, AppError> {
        self.run_blocking(move |db| db.get_peers()).await
    }

    pub async fn save_message(&self, msg: MessageRecord) -> Result<i64, AppError> {
        self.run_blocking(move |db| db.save_message(&msg)).await
    }

    pub async fn get_chat_history(&self, peer_ip: String, limit: i64, offset: i64) -> Result<Vec<MessageRecord>, AppError> {
        self.run_blocking(move |db| db.get_chat_history(&peer_ip, limit, offset)).await
    }

    pub async fn save_subnets(&self, subnets: Vec<String>) -> Result<(), AppError> {
        self.run_blocking(move |db| db.save_subnets(&subnets)).await
    }

    pub async fn get_subnets(&self) -> Result<Vec<String>, AppError> {
        self.run_blocking(move |db| db.get_subnets()).await
    }

    pub async fn create_file_task(&self, task: FileTaskRecord) -> Result<i64, AppError> {
        self.run_blocking(move |db| db.create_file_task(&task)).await
    }

    pub async fn update_file_task_progress(&self, id: i64, progress: f64, status: crate::types::TransferStatus) -> Result<(), AppError> {
        self.run_blocking(move |db| db.update_file_task_progress(id, progress, status)).await
    }

    pub async fn get_file_task_status(&self, id: i64) -> Result<Option<(crate::types::TransferStatus, f64)>, AppError> {
        self.run_blocking(move |db| db.get_file_task_status(id)).await
    }

    pub async fn save_config(&self, key: String, value: String) -> Result<(), AppError> {
        self.run_blocking(move |db| db.save_config(&key, &value)).await
    }

    pub async fn get_config(&self, key: String) -> Result<Option<String>, AppError> {
        self.run_blocking(move |db| db.get_config(&key)).await
    }

    pub async fn get_total_messages_count(&self) -> Result<i64, AppError> {
        self.run_blocking(move |db| db.get_total_messages_count()).await
    }

    pub async fn get_total_file_transfers_count(&self) -> Result<i64, AppError> {
        self.run_blocking(move |db| db.get_total_file_transfers_count()).await
    }

    pub async fn get_max_file_task_id(&self) -> Result<i64, AppError> {
        self.run_blocking(move |db| db.get_max_file_task_id()).await
    }

    pub async fn save_active_session(&self, peer_ip: String, last_updated_at: i64) -> Result<(), AppError> {
        self.run_blocking(move |db| db.save_active_session(&peer_ip, last_updated_at)).await
    }

    pub async fn get_active_sessions(&self) -> Result<Vec<String>, AppError> {
        self.run_blocking(move |db| db.get_active_sessions()).await
    }

    pub async fn delete_active_session(&self, peer_ip: String) -> Result<(), AppError> {
        self.run_blocking(move |db| db.delete_active_session(&peer_ip)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> DatabaseManager {
        DatabaseManager::new(PathBuf::from(":memory:")).unwrap()
    }

    #[test]
    fn test_peer_operations() {
        let db = temp_db();
        let peer = PeerRecord {
            ip: "192.168.1.100".to_string(),
            username: "alice".to_string(),
            hostname: "alice-pc".to_string(),
            nickname: Some("Al".to_string()),
            avatar_id: None,
            last_seen: 123456,
        };

        db.save_peer(&peer).unwrap();

        let peers = db.get_peers().unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].ip, "192.168.1.100");
        assert_eq!(peers[0].username, "alice");
        assert_eq!(peers[0].hostname, "alice-pc");
        assert_eq!(peers[0].nickname, Some("Al".to_string()));

        // Test update on conflict
        let updated_peer = PeerRecord {
            ip: "192.168.1.100".to_string(),
            username: "alice_new".to_string(),
            hostname: "alice-new-pc".to_string(),
            nickname: Some("Alice".to_string()),
            avatar_id: None,
            last_seen: 123457,
        };
        db.save_peer(&updated_peer).unwrap();

        let peers2 = db.get_peers().unwrap();
        assert_eq!(peers2.len(), 1);
        assert_eq!(peers2[0].username, "alice_new");
        assert_eq!(peers2[0].nickname, Some("Alice".to_string()));
    }

    #[test]
    fn test_message_operations() {
        let db = temp_db();
        let msg1 = MessageRecord {
            id: None,
            sender_ip: "192.168.1.100".to_string(),
            receiver_ip: "0.0.0.0".to_string(),
            text_content: "Hello Alice!".to_string(),
            timestamp: 1000,
            is_read: false,
        };
        let msg2 = MessageRecord {
            id: None,
            sender_ip: "0.0.0.0".to_string(),
            receiver_ip: "192.168.1.100".to_string(),
            text_content: "Hey there Bob".to_string(),
            timestamp: 2000,
            is_read: true,
        };

        let id1 = db.save_message(&msg1).unwrap();
        let id2 = db.save_message(&msg2).unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        let chat = db.get_chat_history("192.168.1.100", 10, 0).unwrap();
        assert_eq!(chat.len(), 2);
        assert_eq!(chat[0].text_content, "Hello Alice!");
        assert!(!chat[0].is_read);
        assert_eq!(chat[1].text_content, "Hey there Bob");
        assert!(chat[1].is_read);

        let count = db.get_total_messages_count().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_subnet_config_operations() {
        let db = temp_db();
        let subnets = vec!["192.168.1".to_string(), "10.0.0".to_string()];
        db.save_subnets(&subnets).unwrap();

        let loaded = db.get_subnets().unwrap();
        assert_eq!(loaded, subnets);
    }

    #[test]
    fn test_file_task_operations() {
        let db = temp_db();
        let task = FileTaskRecord {
            id: None,
            file_name: "test.txt".to_string(),
            file_size: 1024,
            peer_ip: "192.168.1.5".to_string(),
            is_sending: true,
            status: crate::types::TransferStatus::Pending,
            progress: 0.0,
            timestamp: 1620000000,
        };

        let task_id = db.create_file_task(&task).unwrap();
        assert_eq!(task_id, 1);

        db.update_file_task_progress(1, 0.5, crate::types::TransferStatus::Transferring)
            .unwrap();

        let count = db.get_total_file_transfers_count().unwrap();
        assert_eq!(count, 1);

        let status_res = db.get_file_task_status(1).unwrap();
        assert!(status_res.is_some());
        let (status, progress) = status_res.unwrap();
        assert_eq!(status, crate::types::TransferStatus::Transferring);
        assert_eq!(progress, 0.5);
    }

    #[test]
    fn test_app_config_operations() {
        let db = temp_db();
        db.save_config("username", "master_rust").unwrap();
        let val = db.get_config("username").unwrap();
        assert_eq!(val, Some("master_rust".to_string()));
    }

    #[test]
    fn test_active_sessions_operations() {
        let db = temp_db();
        db.save_active_session("192.168.1.100", 12345).unwrap();
        db.save_active_session("192.168.1.101", 12346).unwrap();

        let sessions = db.get_active_sessions().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0], "192.168.1.101");

        db.delete_active_session("192.168.1.100").unwrap();
        let sessions2 = db.get_active_sessions().unwrap();
        assert_eq!(sessions2.len(), 1);
        assert_eq!(sessions2[0], "192.168.1.101");
    }
}
