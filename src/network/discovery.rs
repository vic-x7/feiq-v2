use std::net::SocketAddr;

/// Generate all host IPs for a subnet: "192.168.1.1" through "192.168.1.254"
pub fn subnet_ips(prefix: &str) -> Vec<String> {
    let subnet = prefix.trim_end_matches('.').to_string();
    let mut ips = Vec::with_capacity(254);
    for i in 1..255 {
        ips.push(format!("{}.{}", subnet, i));
    }
    ips
}

/// Derive subnet broadcast addresses from local socket
pub fn subnet_broadcast_addrs(local_addr: SocketAddr) -> Vec<String> {
    let mut addrs = vec!["255.255.255.255".to_string()];
    let ip_str = local_addr.ip().to_string();
    if ip_str != "0.0.0.0" && ip_str != "127.0.0.1" {
        if let Some(pos) = ip_str.rfind('.') {
            let subnet_prefix = &ip_str[..pos];
            addrs.push(format!("{}.255", subnet_prefix));
        }
    }
    addrs
}
