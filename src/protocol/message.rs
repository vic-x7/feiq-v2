use crate::protocol::{parse_file_attachments, IPMsgPacket, FileAttachment, IPMSG_FILEATTACHOPT, IPMSG_FILE_CLIPBOARD};
use crate::network::validation::{sanitize_message, sanitize_filename};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetFileDataRequest {
    pub packet_no: u32,
    pub file_id: u32,
    pub offset: u64,
}

/// Parse IPMSG_GETFILEDATA extra field: "packet_id_hex:file_id_hex:offset_hex"
/// Avoids Primitive Obsession by returning a cohesive `GetFileDataRequest` struct.
pub fn parse_getfiledata_extra(extra: &str) -> Option<GetFileDataRequest> {
    let parts: Vec<&str> = extra.split(':').collect();
    if parts.len() < 3 {
        return None;
    }
    let packet_no = u32::from_str_radix(parts[0], 16).ok()?;
    let file_id = u32::from_str_radix(parts[1], 16).ok()?;
    let offset = u64::from_str_radix(parts[2], 16).ok()?;
    Some(GetFileDataRequest {
        packet_no,
        file_id,
        offset,
    })
}

/// Parse incoming message: extract text, attachments, strip font styling
pub fn parse_incoming_message(packet: &IPMsgPacket) -> (String, Vec<FileAttachment>) {
    let mut text_content = packet.extra.clone();
    let mut attachments = Vec::new();

    if packet.extra.contains('\0') {
        let parts: Vec<&str> = packet.extra.splitn(2, '\0').collect();
        text_content = parts[0].to_string();
        attachments = parse_file_attachments(parts[1]);
    }

    if let Some(pos) = text_content.find('{') {
        text_content.truncate(pos);
    }

    // Sanitize the raw user message text
    text_content = sanitize_message(&text_content);

    // Sanitize file attachment names to prevent path traversal or giant filenames
    for att in &mut attachments {
        att.name = sanitize_filename(&att.name);
    }

    if (packet.cmd & IPMSG_FILEATTACHOPT) == IPMSG_FILEATTACHOPT && !attachments.is_empty() {
        for att in &attachments {
            let size_str = crate::protocol::format_file_size(att.size);
            let file_line = format!("Shared a file: {} ({})", att.name, size_str);
            if text_content.is_empty() {
                text_content = file_line;
            } else {
                text_content = format!("{}\n{}", text_content, file_line);
            }
        }
    }

    (text_content, attachments)
}

/// Identify which attachments are clipboard images and should be auto-downloaded.
/// This maintains perfect pure-logic module boundaries for message.rs.
pub fn get_clipboard_downloads(attachments: &[FileAttachment]) -> Vec<FileAttachment> {
    attachments
        .iter()
        .filter(|att| (att.file_type & IPMSG_FILE_CLIPBOARD) == IPMSG_FILE_CLIPBOARD)
        .cloned()
        .collect()
}
