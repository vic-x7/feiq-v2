use encoding_rs::GBK;

#[derive(Debug, Clone)]
pub struct IPMsgPacket {
    pub version: String,
    pub packet_no: u32,
    pub username: String,
    pub hostname: String,
    pub cmd: u32,
    pub extra: String,
}

pub use crate::types::FileAttachment;

pub fn serialize_file_attachment(att: &FileAttachment) -> String {
    let name_escaped = att.name.replace(':', "::");
    let capacity = 64 + name_escaped.len();
    let mut s = String::with_capacity(capacity);
    use std::fmt::Write;
    let _ = write!(
        &mut s,
        "{:x}:{}:{:x}:{:x}:{:x}:\x07",
        att.id, name_escaped, att.size, att.mtime, att.file_type
    );
    s
}

pub fn serialize_file_attachments(attachments: &[FileAttachment]) -> String {
    let mut s = String::with_capacity(attachments.len() * 64);
    for att in attachments {
        s.push_str(&serialize_file_attachment(att));
    }
    s
}

pub fn parse_file_attachments(extra_part: &str) -> Vec<FileAttachment> {
    let mut attachments = Vec::new();
    for file_str in extra_part.split('\u{7}') {
        if file_str.is_empty() {
            continue;
        }
        // file_str is format: "id:name_escaped:size:mtime:file_type:"
        let trimmed = file_str.trim_end_matches(':');
        let parts: Vec<&str> = trimmed.rsplitn(4, ':').collect();
        if parts.len() == 4 {
            // rsplitn returns parts in reverse order:
            // parts[0] -> file_type (hex)
            // parts[1] -> mtime (hex)
            // parts[2] -> size (hex)
            // parts[3] -> id:name_escaped
            let file_type = u32::from_str_radix(parts[0], 16).unwrap_or(0);
            let mtime = u64::from_str_radix(parts[1], 16).unwrap_or(0);
            let size = u64::from_str_radix(parts[2], 16).unwrap_or(0);

            if let Some(pos) = parts[3].find(':') {
                let id = u32::from_str_radix(&parts[3][..pos], 16).unwrap_or(0);
                let name = parts[3][pos + 1..].replace("::", ":");
                attachments.push(FileAttachment {
                    id,
                    name,
                    size,
                    mtime,
                    file_type,
                    progress: 0.0,
                    status: crate::types::TransferStatus::Pending,
                });
            }
        }
    }
    attachments
}

impl IPMsgPacket {
    pub fn parse(raw_bytes: &[u8]) -> Option<Self> {
        // Transcode GBK bytes to UTF-8
        let (decoded, _, has_errors) = GBK.decode(raw_bytes);
        if has_errors {
            // Log or handle decode warning
        }

        let s = decoded.into_owned();
        let trimmed = s.trim_end_matches('\0');
        let parts: Vec<&str> = trimmed.splitn(6, ':').collect();
        if parts.len() < 5 {
            return None;
        }

        let version = parts[0].to_string();
        let packet_no = parts[1].parse::<u32>().unwrap_or(0);
        let username = parts[2].to_string();
        let hostname = parts[3].to_string();
        let cmd = parts[4].parse::<u32>().unwrap_or(0);
        let extra = if parts.len() > 5 {
            parts[5].to_string()
        } else {
            String::new()
        };

        Some(IPMsgPacket {
            version,
            packet_no,
            username,
            hostname,
            cmd,
            extra,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        // High-Performance Optimization: Pre-allocate String heap capacity to avoid resize allocations!
        let capacity = 64 + self.username.len() + self.hostname.len() + self.extra.len();
        let mut serialized_str = String::with_capacity(capacity);

        use std::fmt::Write;
        let _ = write!(
            &mut serialized_str,
            "{}:{}:{}:{}:{}:{}\0",
            self.version, self.packet_no, self.username, self.hostname, self.cmd, self.extra
        );

        let (encoded_bytes, _, _) = GBK.encode(&serialized_str);
        encoded_bytes.into_owned()
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

    #[test]
    fn test_ipmsg_packet_serialize_parse() {
        let pkt = IPMsgPacket {
            version: "1".to_string(),
            packet_no: 12345,
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            cmd: 32,
            extra: "hello".to_string(),
        };

        let bytes = pkt.serialize();
        let parsed = IPMsgPacket::parse(&bytes).unwrap();

        assert_eq!(parsed.version, "1");
        assert_eq!(parsed.packet_no, 12345);
        assert_eq!(parsed.username, "bob");
        assert_eq!(parsed.hostname, "bob-pc");
        assert_eq!(parsed.cmd, 32);
        assert_eq!(parsed.extra, "hello");
    }

    #[test]
    fn test_ipmsg_packet_gbk_chinese() {
        let pkt = IPMsgPacket {
            version: "1_lbt6".to_string(),
            packet_no: 999,
            username: "飞秋用户".to_string(),
            hostname: "主机".to_string(),
            cmd: 32,
            extra: "你好，飞秋！".to_string(),
        };

        let bytes = pkt.serialize();
        let parsed = IPMsgPacket::parse(&bytes).unwrap();

        assert_eq!(parsed.username, "飞秋用户");
        assert_eq!(parsed.hostname, "主机");
        assert_eq!(parsed.extra, "你好，飞秋！");
    }

    #[test]
    fn test_ipmsg_packet_malformed() {
        // Test malformed packet with fewer than 5 fields
        let malformed_short = b"1:12345:bob:bob-pc";
        let parsed_short = IPMsgPacket::parse(malformed_short);
        assert!(parsed_short.is_none());

        // Test non-numeric packet_no and command (should fall back to 0 gracefully without panicking)
        let malformed_non_numeric = b"1:invalid_no:bob:bob-pc:invalid_cmd:hello";
        let parsed_non_numeric = IPMsgPacket::parse(malformed_non_numeric).unwrap();
        assert_eq!(parsed_non_numeric.packet_no, 0);
        assert_eq!(parsed_non_numeric.cmd, 0);
        assert_eq!(parsed_non_numeric.extra, "hello");
    }

    #[test]
    fn test_file_attachment_serialize_parse() {
        let att1 = FileAttachment {
            id: 0,
            name: "test:file.txt".to_string(),
            size: 1024,
            mtime: 1600000000,
            file_type: 1,
            progress: 0.0,
            status: crate::types::TransferStatus::Pending,
        };
        let att2 = FileAttachment {
            id: 1,
            name: "hello.png".to_string(),
            size: 2048576,
            mtime: 1600000001,
            file_type: 1,
            progress: 0.0,
            status: crate::types::TransferStatus::Pending,
        };

        let list = vec![att1, att2];
        let serialized = serialize_file_attachments(&list);
        let parsed = parse_file_attachments(&serialized);

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, 0);
        assert_eq!(parsed[0].name, "test:file.txt");
        assert_eq!(parsed[0].size, 1024);
        assert_eq!(parsed[0].mtime, 1600000000);
        assert_eq!(parsed[0].file_type, 1);

        assert_eq!(parsed[1].id, 1);
        assert_eq!(parsed[1].name, "hello.png");
        assert_eq!(parsed[1].size, 2048576);
        assert_eq!(parsed[1].mtime, 1600000001);
        assert_eq!(parsed[1].file_type, 1);
    }

    #[test]
    fn test_format_file_size() {
        assert_eq!(format_file_size(512), "0.5 KB");
        assert_eq!(format_file_size(1024), "1.0 KB");
        assert_eq!(format_file_size(1024 * 1024 + 1), "1.0 MB");
    }
}
