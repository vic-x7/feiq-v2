use crate::error::ProtocolError;
use crate::types::FileAttachment;
use std::str::FromStr;
use super::command::{IPMSG_FILEATTACHOPT, IPMSG_SENDCHECKOPT};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IPMsgPacket {
    pub version: String,
    pub packet_no: u32,
    pub username: String,
    pub hostname: String,
    pub cmd: u32,
    pub extra: String,
}

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

impl FromStr for IPMsgPacket {
    type Err = ProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim_end_matches('\0');
        let parts: Vec<&str> = trimmed.splitn(6, ':').collect();
        if parts.len() < 5 {
            return Err(ProtocolError::TooFewFields(parts.len()));
        }

        let version = parts[0].to_string();
        let packet_no = parts[1]
            .parse::<u32>()
            .map_err(|_| ProtocolError::InvalidPacketNo(parts[1].to_string()))?;
        let username = parts[2].to_string();
        let hostname = parts[3].to_string();
        let cmd = parts[4]
            .parse::<u32>()
            .map_err(|_| ProtocolError::InvalidCommand(parts[4].to_string()))?;
        let extra = if parts.len() > 5 {
            parts[5].to_string()
        } else {
            String::new()
        };

        Ok(IPMsgPacket {
            version,
            packet_no,
            username,
            hostname,
            cmd,
            extra,
        })
    }
}

impl IPMsgPacket {
    pub fn serialize_to_string(&self) -> String {
        // High-Performance Optimization: Pre-allocate String heap capacity to avoid resize allocations!
        let capacity = 64 + self.username.len() + self.hostname.len() + self.extra.len();
        let mut s = String::with_capacity(capacity);

        use std::fmt::Write;
        let _ = write!(
            &mut s,
            "{}:{}:{}:{}:{}:{}\0",
            self.version, self.packet_no, self.username, self.hostname, self.cmd, self.extra
        );
        s
    }

    /// Gets the base core command (filtering out all high-bit option flags).
    pub fn command_base(&self) -> u32 {
        self.cmd & 0x000000FF
    }

    /// Checks if the command contains a specific option flag.
    pub fn has_option(&self, option_flag: u32) -> bool {
        (self.cmd & option_flag) == option_flag
    }

    /// Convenience method to check if the command has the file attachment option.
    pub fn is_file_attach(&self) -> bool {
        self.has_option(IPMSG_FILEATTACHOPT)
    }

    /// Convenience method to check if the command needs an acknowledgement reply.
    pub fn is_send_check(&self) -> bool {
        self.has_option(IPMSG_SENDCHECKOPT)
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

        let s = pkt.serialize_to_string();
        let parsed = IPMsgPacket::from_str(&s).unwrap();

        assert_eq!(parsed.version, "1");
        assert_eq!(parsed.packet_no, 12345);
        assert_eq!(parsed.username, "bob");
        assert_eq!(parsed.hostname, "bob-pc");
        assert_eq!(parsed.cmd, 32);
        assert_eq!(parsed.extra, "hello");
    }

    #[test]
    fn test_ipmsg_packet_malformed() {
        // Test malformed packet with fewer than 5 fields
        let malformed_short = "1:12345:bob:bob-pc";
        let parsed_short = IPMsgPacket::from_str(malformed_short);
        assert!(matches!(parsed_short, Err(ProtocolError::TooFewFields(_))));

        // Test non-numeric packet_no and command (should return correct ProtocolErrors)
        let malformed_non_numeric_no = "1:invalid_no:bob:bob-pc:32:hello";
        let parsed_non_numeric_no = IPMsgPacket::from_str(malformed_non_numeric_no);
        assert!(matches!(parsed_non_numeric_no, Err(ProtocolError::InvalidPacketNo(_))));

        let malformed_non_numeric_cmd = "1:12345:bob:bob-pc:invalid_cmd:hello";
        let parsed_non_numeric_cmd = IPMsgPacket::from_str(malformed_non_numeric_cmd);
        assert!(matches!(parsed_non_numeric_cmd, Err(ProtocolError::InvalidCommand(_))));
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
    fn test_packet_cmd_helpers() {
        let pkt = IPMsgPacket {
            version: "1".to_string(),
            packet_no: 12345,
            username: "bob".to_string(),
            hostname: "bob-pc".to_string(),
            cmd: 0x00200120, // IPMSG_SENDMSG (0x20) | IPMSG_FILEATTACHOPT (0x00200000) | IPMSG_SENDCHECKOPT (0x00000100)
            extra: "".to_string(),
        };

        assert_eq!(pkt.command_base(), 0x20);
        assert!(pkt.is_file_attach());
        assert!(pkt.is_send_check());
    }
}
