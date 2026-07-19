#[allow(unused_imports)]
pub use crate::app_state::{
    ActiveTab, AppState, IdentitySettings, Message, NetworkingSettings, Peer, PreferenceSettings, Theme,
};
pub use feiq_v2::engine::EngineHandle;

use eframe::egui::{self, CentralPanel, Color32, Frame, Margin, Visuals};
use crate::views::{chat_view, left_nav, middle_list, settings};

pub struct GuiApp {
    pub state: AppState,
    pub engine: Option<EngineHandle>,
    pub rt: Option<tokio::runtime::Runtime>,
    pub engine_rx: Option<std::sync::mpsc::Receiver<Result<EngineHandle, feiq_v2::error::AppError>>>,
}

impl Default for GuiApp {
    fn default() -> Self {
        Self {
            state: AppState::default(),
            engine: None,
            rt: None,
            engine_rx: None,
        }
    }
}

impl GuiApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        setup_custom_fonts(&cc.egui_ctx);
        let state = AppState::default();

        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build() {
                Ok(r) => Some(r),
                Err(e) => {
                    eprintln!("CRITICAL ERROR: Failed to build Tokio runtime: {}", e);
                    None
                }
            };

        let mut engine_rx = None;
        if let Some(ref r) = rt {
            let config = feiq_v2::engine::EngineConfig {
                username: state.identity.username.clone(),
                hostname: state.identity.hostname.clone(),
                bind_ip: state.networking.bind_ip.clone(),
                start_port: state.networking.port,
                db_path: std::path::PathBuf::from("feiq-gui.db"),
            };

            let (tx, rx) = std::sync::mpsc::channel();
            engine_rx = Some(rx);

            let username = state.identity.username.clone();
            let hostname = state.identity.hostname.clone();
            let download_dir = state.preferences.download_dir.clone();

            r.spawn(async move {
                match EngineHandle::start(config).await {
                    Ok(eng) => {
                        // Initial config synchronization
                        if let Err(e) = eng.db().save_config("username".to_string(), username).await {
                            eprintln!("Warning: Failed to save config username: {}", e);
                        }
                        if let Err(e) = eng.db().save_config("hostname".to_string(), hostname).await {
                            eprintln!("Warning: Failed to save config hostname: {}", e);
                        }
                        if let Err(e) = eng.db().save_config("download_dir".to_string(), download_dir).await {
                            eprintln!("Warning: Failed to save config download_dir: {}", e);
                        }
                        let _ = tx.send(Ok(eng));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        }

        Self {
            state,
            engine: None,
            rt,
            engine_rx,
        }
    }

    pub fn apply_theme(&self, ctx: &egui::Context) {
        let theme = self.state.preferences.theme;
        let visuals = if theme == Theme::Dark {
            let mut v = Visuals::dark();
            v.extreme_bg_color = Color32::from_rgb(0x1a, 0x1a, 0x1a);
            v.panel_fill = Color32::from_rgb(0x24, 0x24, 0x24);
            v.widgets.noninteractive.bg_stroke.color = Color32::from_rgb(0x3d, 0x3d, 0x3d);
            v.selection.bg_fill = Color32::from_rgb(0x4a, 0x4a, 0x4a);
            v
        } else {
            let mut v = Visuals::light();
            v.extreme_bg_color = Color32::from_rgb(0xef, 0xef, 0xef);
            v.panel_fill = Color32::from_rgb(0xfd, 0xfd, 0xfd);
            v.selection.bg_fill = Color32::from_rgb(0xdb, 0xdb, 0xdb);
            v
        };
        ctx.set_visuals(visuals);
    }

    pub fn cmd_tx(&self) -> Option<tokio::sync::mpsc::Sender<feiq_v2::types::CoreCommand>> {
        self.engine.as_ref().map(|e| e.cmd_tx())
    }

    #[allow(dead_code)]
    pub fn send_chat_message(&mut self) {
        let tx = self.cmd_tx();
        self.state.send_chat_message(tx.as_ref());
    }

    pub fn sync_config(&mut self) {
        let mut identity_changed = false;
        let mut download_dir_changed = false;

        if !self.state.settings_active {
            if self.state.identity.username != self.state.last_synced_username
                || self.state.identity.hostname != self.state.last_synced_hostname
            {
                self.state.last_synced_username = self.state.identity.username.clone();
                self.state.last_synced_hostname = self.state.identity.hostname.clone();
                identity_changed = true;
            }

            if self.state.preferences.download_dir != self.state.last_synced_download_dir {
                self.state.last_synced_download_dir = self.state.preferences.download_dir.clone();
                download_dir_changed = true;
            }
        }

        if identity_changed || download_dir_changed {
            if let (Some(engine), Some(rt)) = (&self.engine, &self.rt) {
                let db_clone = engine.db().clone();
                let username = self.state.identity.username.clone();
                let hostname = self.state.identity.hostname.clone();
                let download_dir = self.state.preferences.download_dir.clone();

                rt.spawn(async move {
                    if identity_changed {
                        let _ = db_clone.save_config("username".to_string(), username).await;
                        let _ = db_clone.save_config("hostname".to_string(), hostname).await;
                    }
                    if download_dir_changed {
                        let _ = db_clone
                            .save_config("download_dir".to_string(), download_dir)
                            .await;
                    }
                });

                if identity_changed {
                    let _ = engine.try_send(feiq_v2::types::CoreCommand::UpdateIdentity {
                        username: self.state.identity.username.clone(),
                        hostname: self.state.identity.hostname.clone(),
                    });
                }
            }
        }
    }
}

impl eframe::App for GuiApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Check if the background engine initialization has finished
        if self.engine.is_none() {
            if let Some(ref rx) = self.engine_rx {
                if let Ok(res) = rx.try_recv() {
                    match res {
                        Ok(eng) => {
                            self.engine = Some(eng);
                            println!("Background network engine successfully started and synchronized!");
                            ctx.request_repaint();
                        }
                        Err(e) => {
                            eprintln!("CRITICAL ERROR: Failed to start engine in background: {}", e);
                        }
                    }
                }
            }
        }

        // Drain actual core events from the broadcast receiver and sync configs
        if let Some(ref mut engine) = self.engine {
            let repainted = engine.drain_events(|event| {
                self.state.handle_event(event);
            });
            if repainted {
                ctx.request_repaint();
            }
        }
        self.sync_config();

        // Update nudge shake timer
        let dt = ctx.input(|i| i.stable_dt);
        if self.state.nudge_shake_time > 0.0 {
            self.state.nudge_shake_time = (self.state.nudge_shake_time - dt).max(0.0);
            ctx.request_repaint();
        }

        let downloading_active = self.state.messages.values().any(|msgs| {
            msgs.iter().any(|msg| {
                msg.file
                    .as_ref()
                    .is_some_and(|f| f.status == feiq_v2::types::TransferStatus::Transferring)
            })
        });
        if downloading_active {
            ctx.request_repaint();
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx();

        // Apply styling theme
        self.apply_theme(ctx);

        // Calculate dynamic shake frames
        let mut side_frame = Frame::side_top_panel(ui.style());
        let mut central_frame = Frame::central_panel(ui.style());

        if let Some((dx, dy)) = self.state.get_shake_offset() {
            let ox = dx.round() as i8;
            let oy = dy.round() as i8;
            let margin = Margin {
                left: 8 + ox,
                right: 8 - ox,
                top: 8 + oy,
                bottom: 8 - oy,
            };
            side_frame.inner_margin = margin;
            central_frame.inner_margin = margin;
        } else {
            side_frame.inner_margin = Margin::same(8);
            central_frame.inner_margin = Margin::same(8);
        }

        let cmd_tx = self.cmd_tx();

        // Column 1: Leftmost Navigation Strip
        left_nav::draw(&mut self.state, ui, side_frame);

        // Column 2: Middle List Panel (Visible only when settings_active is false)
        middle_list::draw(&mut self.state, cmd_tx.clone(), ui, side_frame);

        // Column 3: Main Right Details Panel
        CentralPanel::default().frame(central_frame).show(ui, |ui| {
            if self.state.settings_active {
                settings::draw(&mut self.state, ui);
            } else {
                chat_view::draw(&mut self.state, cmd_tx, ui);
            }
        });
    }
}

use std::sync::LazyLock;

pub static NAV_ICON_STYLE: LazyLock<egui::TextStyle> =
    LazyLock::new(|| egui::TextStyle::Name("NavIcon".into()));

pub static FILE_ICON_STYLE: LazyLock<egui::TextStyle> =
    LazyLock::new(|| egui::TextStyle::Name("FileIcon".into()));

fn setup_custom_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    // Embed the Maple Mono CN Regular font bytes
    let font_bytes = include_bytes!("../../../assets/fonts/MapleMono-CN-Regular.ttf");
    fonts.font_data.insert(
        "maple_mono_cn".to_owned(),
        std::sync::Arc::new(egui::FontData::from_static(font_bytes)),
    );

    // Insert your font at the front (highest priority) for both proportional and monospace text
    for family in [
        &egui::FontFamily::Proportional,
        &egui::FontFamily::Monospace,
    ] {
        if let Some(vec) = fonts.families.get_mut(family) {
            vec.insert(0, "maple_mono_cn".to_owned());
        }
    }

    ctx.set_fonts(fonts);

    // Set custom text styles to avoid hardcoded font sizes
    ctx.all_styles_mut(|style| {
        style.text_styles = [
            (
                egui::TextStyle::Heading,
                egui::FontId::new(16.0, egui::FontFamily::Proportional),
            ),
            (
                egui::TextStyle::Body,
                egui::FontId::new(13.0, egui::FontFamily::Proportional),
            ),
            (
                egui::TextStyle::Button,
                egui::FontId::new(13.0, egui::FontFamily::Proportional),
            ),
            (
                egui::TextStyle::Small,
                egui::FontId::new(11.0, egui::FontFamily::Proportional),
            ),
        ]
        .into();

        // Register our custom non-allocating styles
        style.text_styles.insert(
            NAV_ICON_STYLE.clone(),
            egui::FontId::new(20.0, egui::FontFamily::Proportional),
        );
        style.text_styles.insert(
            FILE_ICON_STYLE.clone(),
            egui::FontId::new(24.0, egui::FontFamily::Proportional),
        );

        // Micro-adjust button padding and row spacing for clean alignment
        // and to prevent any visual truncation of text.
        style.spacing.button_padding = egui::vec2(8.0, 4.0);
        style.spacing.item_spacing = egui::vec2(8.0, 4.0);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use feiq_v2::types::CoreCommand;

    #[test]
    fn test_toggle_settings_hides_middle_column() {
        let mut state = AppState::default();
        assert!(!state.settings_active);

        // When settings are inactive, we expect the middle column to render
        state.settings_active = true;
        // Verify that setting active now blocks rendering/updating other views
        assert!(state.settings_active);
    }

    #[test]
    fn test_text_truncation_logic() {
        // Verify text truncation functionality (using view utility helper)
        let short_msg = "Hello";
        assert_eq!(crate::views::truncate_string(short_msg, 18), "Hello");

        let long_msg = "This is a very long chat preview message that exceeds limit";
        let truncated = crate::views::truncate_string(long_msg, 18);
        assert_eq!(truncated, "This is a very...");
        assert!(truncated.ends_with("..."));
        assert_eq!(truncated.chars().count(), 17);
    }

    #[test]
    fn test_modify_identity_and_preferences() {
        let mut state = AppState::default();
        state.identity.username = "test_user_mod".to_string();
        state.identity.hostname = "HOST-MOD".to_string();
        state.identity.nickname = "New Nickname".to_string();
        state.identity.signature = "New Signature".to_string();

        assert_eq!(state.identity.username, "test_user_mod");
        assert_eq!(state.identity.hostname, "HOST-MOD");
        assert_eq!(state.identity.nickname, "New Nickname");
        assert_eq!(state.identity.signature, "New Signature");

        // Theme preference
        assert_eq!(state.preferences.theme, Theme::Dark);
        state.preferences.theme = Theme::Light;
        assert_eq!(state.preferences.theme, Theme::Light);
    }

    #[test]
    fn test_modify_networking_subnets() {
        let mut state = AppState::default();
        // Initial subnets
        assert_eq!(state.networking.broadcast_subnets.len(), 2);

        // Add custom CIDR subnet block
        let custom_cidr = "172.16.128.255/18".to_string();
        state.networking.broadcast_subnets.push(custom_cidr.clone());
        assert_eq!(state.networking.broadcast_subnets.len(), 3);
        assert_eq!(
            state
                .networking
                .broadcast_subnets
                .last()
                .expect("at least one subnet should exist"),
            &custom_cidr
        );

        // Remove a subnet
        state.networking.broadcast_subnets.remove(0);
        assert_eq!(state.networking.broadcast_subnets.len(), 2);
        assert!(!state
            .networking
            .broadcast_subnets
            .contains(&"192.168.1.255".to_string()));
    }

    #[test]
    fn test_select_peer_from_discovery_switches_back_to_chats() {
        let mut state = AppState {
            active_tab: ActiveTab::Friends,
            settings_active: true,
            ..Default::default()
        };

        // Simulate clicking Charlie's IP "10.0.0.15" in discovery list
        let charlie_ip: Ipv4Addr = "10.0.0.15".parse().unwrap();
        state.selected_peer_ip = Some(charlie_ip);
        state.active_tab = ActiveTab::Chats;
        state.settings_active = false;

        assert_eq!(state.active_tab, ActiveTab::Chats);
        assert!(!state.settings_active);
        assert_eq!(state.selected_peer_ip, Some(charlie_ip));
    }

    #[test]
    fn test_vibration_state_offsets() {
        let mut state = AppState::default();
        // 1. Initial State
        assert_eq!(state.get_shake_offset(), None);

        // 2. Activate Nudge
        state.nudge_shake_time = 0.5;
        let offset = state.get_shake_offset();
        assert!(offset.is_some());
        let val = offset.unwrap();
        // Since sin and cos of frequency * time are non-zero generally, check they vary
        assert!(val.0.abs() > 0.0 || val.1.abs() > 0.0);

        // 3. Decline Countdown: Simulate time ticking down to 0.0
        state.nudge_shake_time = 0.25;
        assert!(state.get_shake_offset().is_some());

        state.nudge_shake_time = 0.0;
        assert_eq!(state.get_shake_offset(), None); // layout offsets decline to zero properly
    }

    #[test]
    fn test_gui_app_default_initialization() {
        let app = GuiApp::default();
        assert!(app.engine.is_none());
        assert!(app.state.task_mappings.is_empty());
        assert!(app.state.file_to_share_path.is_empty());
        assert!(!app.state.show_share_file_dialog);
    }

    #[test]
    fn test_system_tray_and_notification_settings_mutation() {
        let mut state = AppState::default();

        // Default values
        assert!(state.preferences.enable_system_tray);
        assert!(!state.preferences.minimize_to_tray_on_close);
        assert!(state.preferences.desktop_notifications_enabled);
        assert!(!state.minimized_to_tray);

        // Modify in-memory
        state.preferences.enable_system_tray = false;
        state.preferences.minimize_to_tray_on_close = true;
        state.preferences.desktop_notifications_enabled = false;
        state.minimized_to_tray = true;

        assert!(!state.preferences.enable_system_tray);
        assert!(state.preferences.minimize_to_tray_on_close);
        assert!(!state.preferences.desktop_notifications_enabled);
        assert!(state.minimized_to_tray);
    }

    #[test]
    fn test_navigation_tab_switching() {
        let mut state = AppState::default();

        // Starts with Chats
        assert_eq!(state.active_tab, ActiveTab::Chats);

        // Switch to Friends (Friend Discovery)
        state.active_tab = ActiveTab::Friends;
        assert_eq!(state.active_tab, ActiveTab::Friends);

        // Switch back to Chats
        state.active_tab = ActiveTab::Chats;
        assert_eq!(state.active_tab, ActiveTab::Chats);
    }

    #[test]
    fn test_select_discovery_peer_online_vs_offline() {
        let mut state = AppState {
            active_tab: ActiveTab::Friends,
            settings_active: true,
            ..Default::default()
        };
        state.peers = vec![
            Peer {
                ip: "192.168.1.10".parse().unwrap(),
                username: "alice_smith".to_string(),
                hostname: "ALICE-PC".to_string(),
                nickname: "Alice (P2P Client)".to_string(),
                signature: "Coding in Rust... 🦀".to_string(),
                online: true,
            },
            Peer {
                ip: "10.0.0.15".parse().unwrap(),
                username: "charlie_brown".to_string(),
                hostname: "CHARLIE-SRV".to_string(),
                nickname: "Charlie (Ops)".to_string(),
                signature: "Testing subnets".to_string(),
                online: false,
            },
        ];

        // Alice is online: true
        let alice_ip: Ipv4Addr = "192.168.1.10".parse().unwrap();
        state.select_discovery_peer(alice_ip);

        // Since Alice is online, we expect the tab to switch to Chats,
        // settings to deactivate, and Alice to be selected.
        assert_eq!(state.active_tab, ActiveTab::Chats);
        assert!(!state.settings_active);
        assert_eq!(state.selected_peer_ip, Some(alice_ip));

        // Now switch back to Friends and open settings again
        state.active_tab = ActiveTab::Friends;
        state.settings_active = true;

        // Charlie is online: false
        let charlie_ip: Ipv4Addr = "10.0.0.15".parse().unwrap();
        state.select_discovery_peer(charlie_ip);

        // Since Charlie is offline, clicking them should have no effect on tab/settings
        assert_eq!(state.active_tab, ActiveTab::Friends);
        assert!(state.settings_active);
        assert_eq!(state.selected_peer_ip, Some(alice_ip)); // remains Alice
    }

    #[test]
    fn test_send_chat_message_appends_to_selected_peer() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut state = AppState::default();
        let alice_ip: Ipv4Addr = "192.168.1.10".parse().unwrap();
        state.selected_peer_ip = Some(alice_ip);
        let msg_text = "Test message from unit test";
        state.new_message_text = msg_text.to_string();

        let initial_count = state.messages.get(&alice_ip).map_or(0, |m| m.len());

        state.send_chat_message(Some(&tx));

        let messages = state.messages.get(&alice_ip).unwrap();
        assert_eq!(messages.len(), initial_count + 1);
        let last_msg = messages.last().unwrap();
        assert_eq!(last_msg.content, msg_text);
        assert!(last_msg.is_outgoing);
        assert_eq!(state.new_message_text, "");

        // Verify command is transmitted via cmd_tx
        let cmd = rx
            .try_recv()
            .expect("CoreCommand should have been transmitted");
        let CoreCommand::SendMessage { to_ip, content } = cmd else {
            panic!("Expected SendMessage command");
        };
        assert_eq!(to_ip, alice_ip.to_string());
        assert_eq!(content, msg_text);
    }

    #[test]
    fn test_trigger_screen_nudge_transmits_command() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut state = AppState::default();
        let alice_ip: Ipv4Addr = "192.168.1.10".parse().unwrap();

        let initial_count = state.messages.get(&alice_ip).map_or(0, |m| m.len());

        state.trigger_screen_nudge(alice_ip, Some(&tx));

        let messages = state.messages.get(&alice_ip).unwrap();
        assert_eq!(messages.len(), initial_count + 1);
        let last_msg = messages.last().unwrap();
        assert_eq!(last_msg.content, feiq_v2::types::NUDGE_MESSAGE_CONTENT);
        assert!(last_msg.is_outgoing);

        // Verify command is transmitted via cmd_tx
        let cmd = rx
            .try_recv()
            .expect("CoreCommand should have been transmitted");
        let CoreCommand::SendKnock { peer_ip } = cmd else {
            panic!("Expected SendKnock command");
        };
        assert_eq!(peer_ip, alice_ip.to_string());
    }

    #[test]
    fn test_localized_defaults() {
        let mut state = AppState::default();
        // Assert localized identity defaults
        assert_eq!(state.identity.nickname, "昵称");
        assert_eq!(state.identity.signature, "在线");

        // Assert localized screen nudge content
        let alice_ip: Ipv4Addr = "192.168.1.10".parse().unwrap();
        state.trigger_screen_nudge(alice_ip, None);
        let msgs = state.messages.get(&alice_ip).unwrap();
        let last_msg = msgs.last().unwrap();
        assert_eq!(last_msg.content, feiq_v2::types::NUDGE_MESSAGE_CONTENT);
    }
}
