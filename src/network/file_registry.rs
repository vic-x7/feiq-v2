use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SharedFile {
    pub path: PathBuf,
    pub name: String,
    pub size: u64,
}

#[derive(Clone)]
pub struct FileRegistry {
    files: Arc<Mutex<HashMap<String, SharedFile>>>,
}

impl FileRegistry {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a shared file under multiple keys to support various IPMsg lookups.
    /// Eliminates the Data Clump smell by accepting a cohesive `SharedFile` object.
    pub fn register(&self, packet_no: u32, file_id: u32, file: SharedFile) {
        let mut files = self.files.lock().unwrap();
        // Store under compound keys to support both hex/dec and with/without packet_no lookups
        files.insert(format!("{}:{}", packet_no, file_id), file.clone());
        files.insert(format!("{:x}:{:x}", packet_no, file_id), file.clone());
        files.insert(format!("{}", file_id), file.clone());
        files.insert(format!("{:x}", file_id), file);
    }

    /// Looks up a file in the registry given packet_no and file_id, supporting standard/hex formats.
    /// Eliminates the Mysterious Name smell by consistently utilizing `packet_no` instead of `packet_id`.
    pub fn lookup(&self, packet_no: u32, file_id: u32) -> Option<SharedFile> {
        let files = self.files.lock().unwrap();
        let packet_no_hex = format!("{:x}", packet_no);
        let file_id_hex = format!("{:x}", file_id);

        let key1 = format!("{}:{}", packet_no, file_id);
        let key2 = format!("{}:{}", packet_no_hex, file_id_hex);
        let key3 = file_id.to_string();
        let key4 = file_id_hex;

        files
            .get(&key1)
            .or_else(|| files.get(&key2))
            .or_else(|| files.get(&key3))
            .or_else(|| files.get(&key4))
            .cloned()
    }
}
