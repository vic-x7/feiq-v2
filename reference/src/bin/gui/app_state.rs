use std::collections::HashMap;
use std::net::Ipv4Addr;
use feiq_v2::types::{CoreCommand, CoreEvent, FileAttachment};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActiveTab {
    Chats,
    Friends,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Theme {
    Dark,
    Light,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IdentitySettings {
    pub username: String,
    pub hostname: String,
    pub nickname: String,
    pub signature: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NetworkingSettings {
    pub port: u16,
    pub bind_ip: String,
    pub broadcast_subnets: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PreferenceSettings {
    pub theme: Theme,
    pub sound_notify: bool,
    pub screen_nudges_enabled: bool,
    pub enable_system_tray: bool,
    pub minimize_to_tray_on_close: bool,
    pub desktop_notifications_enabled: bool,
    pub download_dir: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Peer {
    pub ip: Ipv4Addr,
    pub username: String,
    pub hostname: String,
    pub nickname: String,
    pub signature: String,
    pub online: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub id: i64,
    pub sender_ip: Ipv4Addr,
    pub content: String,
    pub timestamp: i64,
    pub is_outgoing: bool,
    pub file: Option<FileAttachment>,
    pub packet_no: Option<u32>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AppState {
    pub active_tab: ActiveTab,
    pub settings_active: bool,
    pub peers: Vec<Peer>,
    pub messages: HashMap<Ipv4Addr, Vec<Message>>,
    pub selected_peer_ip: Option<Ipv4Addr>,
    pub nudge_shake_time: f32,

    // Configurable state
    pub identity: IdentitySettings,
    pub networking: NetworkingSettings,
    pub preferences: PreferenceSettings,
    pub minimized_to_tray: bool,

    // Temp state for editing
    #[serde(skip)]
    pub new_message_text: String,
    #[serde(skip)]
    pub new_subnet_text: String,

    #[serde(skip)]
    pub task_mappings: HashMap<i64, (Ipv4Addr, String)>,
    #[serde(skip)]
    pub file_to_share_path: String,
    #[serde(skip)]
    pub show_share_file_dialog: bool,
    #[serde(skip)]
    pub last_synced_username: String,
    #[serde(skip)]
    pub last_synced_hostname: String,
    #[serde(skip)]
    pub last_synced_download_dir: String,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            active_tab: ActiveTab::Chats,
            settings_active: false,
            peers: Vec::new(),
            messages: HashMap::new(),
            selected_peer_ip: None,
            nudge_shake_time: 0.0,
            identity: IdentitySettings {
                username: "local_user".to_string(),
                hostname: "LOCAL-PC".to_string(),
                nickname: "昵称".to_string(),
                signature: "在线".to_string(),
            },
            networking: NetworkingSettings {
                port: feiq_v2::protocol::IPMSG_PORT,
                bind_ip: "0.0.0.0".to_string(),
                broadcast_subnets: vec!["192.168.1.255".to_string(), "10.0.0.255".to_string()],
            },
            preferences: PreferenceSettings {
                theme: Theme::Dark,
                sound_notify: true,
                screen_nudges_enabled: true,
                enable_system_tray: true,
                minimize_to_tray_on_close: false,
                desktop_notifications_enabled: true,
                download_dir: "downloads".to_string(),
            },
            minimized_to_tray: false,
            new_message_text: String::new(),
            new_subnet_text: String::new(),
            task_mappings: HashMap::new(),
            file_to_share_path: String::new(),
            show_share_file_dialog: false,
            last_synced_username: "local_user".to_string(),
            last_synced_hostname: "LOCAL-PC".to_string(),
            last_synced_download_dir: "downloads".to_string(),
        }
    }
}

impl AppState {
    pub fn get_shake_offset(&self) -> Option<(f32, f32)> {
        if self.nudge_shake_time > 0.0 {
            let frequency = 60.0;
            let amplitude = 8.0;
            let dx = (self.nudge_shake_time * frequency).sin() * amplitude;
            let dy = (self.nudge_shake_time * frequency * 1.5).cos() * amplitude;
            Some((dx, dy))
        } else {
            None
        }
    }

    /// Selects an online peer from the Friend Discovery list, switching the
    /// active tab to Chats, deactivating settings, and initializing their chat thread.
    pub fn select_discovery_peer(&mut self, ip: Ipv4Addr) {
        if let Some(peer) = self.peers.iter().find(|p| p.ip == ip) {
            if peer.online {
                self.selected_peer_ip = Some(ip);
                self.active_tab = ActiveTab::Chats;
                self.settings_active = false;
                self.messages.entry(ip).or_default();
            }
        }
    }

    /// Appends the current message draft (`new_message_text`) to the selected
    /// peer's conversation thread in-memory and clears the text edit state.
    pub fn send_chat_message(&mut self, cmd_tx: Option<&tokio::sync::mpsc::Sender<CoreCommand>>) {
        let trimmed = self.new_message_text.trim().to_string();
        if trimmed.is_empty() {
            return;
        }
        if let Some(ip) = self.selected_peer_ip {
            let msg = Message {
                id: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                sender_ip: Ipv4Addr::LOCALHOST,
                content: trimmed.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                is_outgoing: true,
                file: None,
                packet_no: None,
            };
            self.messages.entry(ip).or_default().push(msg);

            if let Some(cmd_tx) = cmd_tx {
                if let Err(e) = cmd_tx.try_send(CoreCommand::SendMessage {
                    to_ip: ip.to_string(),
                    content: trimmed,
                }) {
                    eprintln!("Warning: Failed to transmit message command: {:?}", e);
                }
            }

            self.new_message_text.clear();
        }
    }

    /// Triggers a screen nudge/shake animation, adding a nudge system notification
    /// message to the target peer's conversation thread in-memory.
    pub fn trigger_screen_nudge(
        &mut self,
        peer_ip: Ipv4Addr,
        cmd_tx: Option<&tokio::sync::mpsc::Sender<CoreCommand>>,
    ) {
        if self.preferences.screen_nudges_enabled {
            self.nudge_shake_time = 0.5;
        }

        let nudge_msg = Message {
            id: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            sender_ip: Ipv4Addr::LOCALHOST,
            content: feiq_v2::types::NUDGE_MESSAGE_CONTENT.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            is_outgoing: true,
            file: None,
            packet_no: None,
        };
        self.messages.entry(peer_ip).or_default().push(nudge_msg);

        if let Some(cmd_tx) = cmd_tx {
            if let Err(e) = cmd_tx.try_send(CoreCommand::SendKnock {
                peer_ip: peer_ip.to_string(),
            }) {
                eprintln!("Warning: Failed to transmit window knock command: {:?}", e);
            }
        }
    }

    pub fn handle_event(&mut self, event: CoreEvent) {
        match event {
            CoreEvent::PeerStatusChanged {
                ip,
                username,
                hostname,
                nickname,
                online,
            } => {
                if let Ok(ipv4) = ip.parse::<Ipv4Addr>() {
                    if let Some(existing) = self.peers.iter_mut().find(|p| p.ip == ipv4) {
                        existing.nickname = nickname.unwrap_or_else(|| username.clone());
                        existing.username = username;
                        existing.hostname = hostname;
                        existing.online = online;
                    } else {
                        let display_name = nickname.unwrap_or_else(|| username.clone());
                        self.peers.push(Peer {
                            ip: ipv4,
                            username,
                            hostname,
                            nickname: display_name,
                            signature: String::new(),
                            online,
                        });
                    }
                }
            }
            CoreEvent::MessageReceived {
                id,
                sender_ip,
                content,
                timestamp,
                username: _,
            } => {
                if let Ok(ipv4) = sender_ip.parse::<Ipv4Addr>() {
                    let msg = Message {
                        id,
                        sender_ip: ipv4,
                        content,
                        timestamp,
                        is_outgoing: false,
                        file: None,
                        packet_no: None,
                    };
                    self.messages.entry(ipv4).or_default().push(msg);
                }
            }
            CoreEvent::FileAttachmentsReceived {
                sender_ip,
                packet_no,
                files,
            } => {
                if let Ok(ipv4) = sender_ip.parse::<Ipv4Addr>() {
                    for file in files {
                        let msg = Message {
                            id: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                            sender_ip: ipv4,
                            content: file.name.clone(),
                            timestamp: chrono::Utc::now().timestamp(),
                            is_outgoing: false,
                            file: Some(file),
                            packet_no: Some(packet_no),
                        };
                        self.messages.entry(ipv4).or_default().push(msg);
                    }
                }
            }
            CoreEvent::WindowKnock {
                sender_ip,
                username,
            } => {
                if let Ok(ipv4) = sender_ip.parse::<Ipv4Addr>() {
                    if self.preferences.screen_nudges_enabled {
                        self.nudge_shake_time = 0.5;
                    }
                    let nudge_msg = Message {
                        id: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                        sender_ip: ipv4,
                        content: format!("* {} 发送了一个窗口抖动 *", username),
                        timestamp: chrono::Utc::now().timestamp(),
                        is_outgoing: false,
                        file: None,
                        packet_no: None,
                    };
                    self.messages.entry(ipv4).or_default().push(nudge_msg);
                }
            }
            CoreEvent::PeerTyping {
                sender_ip: _,
                typing: _,
            } => {}
            CoreEvent::TransferStarted {
                task_id,
                peer_ip,
                file_name,
                file_size: _,
                is_sending: _,
            } => {
                if let Ok(ipv4) = peer_ip.parse::<Ipv4Addr>() {
                    self.task_mappings
                        .insert(task_id, (ipv4, file_name.clone()));
                    if let Some(msgs) = self.messages.get_mut(&ipv4) {
                        for msg in msgs {
                            if let Some(ref mut file) = msg.file {
                                if file.name == file_name {
                                    file.status = feiq_v2::types::TransferStatus::Transferring;
                                }
                            }
                        }
                    }
                }
            }
            CoreEvent::TransferProgress {
                task_id,
                progress,
                status,
            } => {
                if let Some((ipv4, file_name)) = self.task_mappings.get(&task_id).cloned() {
                    if let Some(msgs) = self.messages.get_mut(&ipv4) {
                        for msg in msgs {
                            if let Some(ref mut file) = msg.file {
                                if file.name == file_name {
                                    file.progress = progress;
                                    file.status = status.clone();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
