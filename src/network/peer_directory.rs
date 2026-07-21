use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn ip_to_u32(ip: &str) -> u32 {
    ip.parse::<std::net::Ipv4Addr>().map(u32::from).unwrap_or(0)
}

#[derive(Clone)]
pub struct PeerDirectory {
    peer_ports: Arc<Mutex<HashMap<u32, u16>>>,
}

impl PeerDirectory {
    pub fn new() -> Self {
        Self {
            peer_ports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers/updates a peer's custom port.
    pub fn upsert(&self, ip_u32: u32, port: u16) {
        if let Ok(mut ports) = self.peer_ports.lock() {
            ports.insert(ip_u32, port);
        }
    }

    /// Helper to register/update a peer's custom port using a string IP.
    pub fn upsert_str(&self, ip_str: &str, port: u16) {
        let ip_u32 = ip_to_u32(ip_str);
        self.upsert(ip_u32, port);
    }

    /// Retrieves the dynamically discovered port for a given peer IP address (as u32),
    /// or returns the default port (2425) if unknown.
    pub fn get_port(&self, ip_u32: u32) -> u16 {
        if let Ok(guard) = self.peer_ports.lock() {
            *guard.get(&ip_u32).unwrap_or(&crate::protocol::IPMSG_PORT)
        } else {
            crate::protocol::IPMSG_PORT
        }
    }

    /// Retrieves the dynamically discovered port for a given peer IP address string,
    /// or returns the default port (2425) if unknown.
    pub fn get_port_str(&self, ip_str: &str) -> u16 {
        let ip_u32 = ip_to_u32(ip_str);
        self.get_port(ip_u32)
    }
}
