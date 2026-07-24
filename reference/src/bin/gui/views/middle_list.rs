use crate::app::{ActiveTab, AppState};
use crate::views::truncate_string;
use eframe::egui::{self, Button, Panel, ScrollArea, Vec2};

pub fn draw(
    app: &mut AppState,
    cmd_tx: Option<tokio::sync::mpsc::Sender<feiq_v2::types::CoreCommand>>,
    ui: &mut egui::Ui,
    side_frame: egui::Frame,
) {
    if app.settings_active {
        return;
    }

    // Pre-resolve font styles once per render to avoid traversing the style tree inside loops.
    let body_font = egui::TextStyle::Body.resolve(ui.style());
    let small_font = egui::TextStyle::Small.resolve(ui.style());

    Panel::left("middle_list_panel")
        .resizable(true)
        .default_size(240.0)
        .size_range(180.0..=320.0)
        .frame(side_frame)
        .show(ui, |ui| {
            ui.add_space(4.0);

            match app.active_tab {
                ActiveTab::Chats => {
                    ui.heading("聊天");
                    ui.add_space(8.0);

                    let mut clicked_chat_ip = None;
                    ScrollArea::vertical().show(ui, |ui| {
                        for peer in &app.peers {
                            let has_messages = app.messages.contains_key(&peer.ip);
                            if !has_messages {
                                continue;
                            }

                            let is_selected = app.selected_peer_ip.as_ref() == Some(&peer.ip);

                            ui.push_id(("chat_peer", peer.ip), |ui| {
                                let last_msg = app
                                    .messages
                                    .get(&peer.ip)
                                    .and_then(|m| m.last())
                                    .map(|m| m.content.as_str())
                                    .unwrap_or(&peer.signature);

                                if draw_peer_row(
                                    ui,
                                    peer,
                                    is_selected,
                                    last_msg,
                                    &body_font,
                                    &small_font,
                                ) {
                                    clicked_chat_ip = Some(peer.ip);
                                }
                                ui.add_space(4.0);
                            });
                        }
                    });

                    if let Some(ip) = clicked_chat_ip {
                        app.selected_peer_ip = Some(ip);
                    }
                }
                ActiveTab::Friends => {
                    let online_count = app.peers.iter().filter(|p| p.online).count();
                    ui.horizontal(|ui| {
                        ui.heading(format!("好友 ({}/{})", online_count, app.peers.len()));
                    });
                    ui.add_space(8.0);

                    // Refresh Peer List button visible ONLY on discovery tab
                    let refresh_btn = ui.add_sized(
                        Vec2::new(ui.available_width(), 30.0),
                        Button::new("🔄 刷新好友列表"),
                    );
                    if refresh_btn.clicked() {
                        println!("Refreshing peer list...");

                        if let Some(ref cmd_tx) = cmd_tx {
                            let _ = cmd_tx.try_send(feiq_v2::types::CoreCommand::BroadcastPresence);
                            for subnet_str in &app.networking.broadcast_subnets {
                                if let Some(clean) = subnet_str.split('/').next() {
                                    let mut parts: Vec<&str> = clean.split('.').collect();
                                    if parts.len() >= 3 {
                                        parts.truncate(3);
                                        let prefix = parts.join(".");
                                        let _ = cmd_tx.try_send(
                                            feiq_v2::types::CoreCommand::ScanSubnet {
                                                subnet: prefix,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                    }
                    ui.add_space(10.0);

                    let mut clicked_peer_ip = None;
                    ScrollArea::vertical().show(ui, |ui| {
                        for peer in &app.peers {
                            let is_selected = app.selected_peer_ip.as_ref() == Some(&peer.ip);

                            ui.push_id(("friend_peer", peer.ip), |ui| {
                                if draw_peer_row(
                                    ui,
                                    peer,
                                    is_selected,
                                    &peer.signature,
                                    &body_font,
                                    &small_font,
                                ) {
                                    clicked_peer_ip = Some(peer.ip);
                                }
                                ui.add_space(4.0);
                            });
                        }
                    });

                    if let Some(ip) = clicked_peer_ip {
                        app.select_discovery_peer(ip);
                    }
                }
            }
        });
}

fn draw_peer_row(
    ui: &mut egui::Ui,
    peer: &crate::app::Peer,
    is_selected: bool,
    subtext: &str,
    body_font: &egui::FontId,
    small_font: &egui::FontId,
) -> bool {
    let mut clicked = false;
    ui.horizontal(|ui| {
        let btn = ui.add_sized(
            Vec2::new(ui.available_width(), 44.0),
            Button::new("").selected(is_selected),
        );

        let rect = btn.rect;

        // Status dot (Deduplicated!)
        let dot_color = crate::views::peer_status_color(peer);
        let dot_center = rect.left_center() + Vec2::new(12.0, 0.0);
        ui.painter().circle_filled(dot_center, 5.0, dot_color);

        // Nickname
        let text_pos = rect.left_center() + Vec2::new(24.0, -8.0);
        ui.painter().text(
            text_pos,
            egui::Align2::LEFT_CENTER,
            &peer.nickname,
            body_font.clone(),
            ui.visuals().widgets.active.text_color(),
        );

        let sub_pos = rect.left_center() + Vec2::new(24.0, 10.0);

        // Text Truncation (Deduplicated!)
        let sig = truncate_string(subtext, 18);
        ui.painter().text(
            sub_pos,
            egui::Align2::LEFT_CENTER,
            sig,
            small_font.clone(),
            ui.visuals().weak_text_color(),
        );

        if btn.clicked() {
            clicked = true;
        }
    });
    clicked
}
