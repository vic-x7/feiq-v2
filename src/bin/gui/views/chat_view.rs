use crate::app::{AppState, FILE_ICON_STYLE};
use eframe::egui::{
    self, Align, Button, Color32, Frame, Layout, Margin, Panel, RichText, ScrollArea, TextEdit,
    Vec2,
};
use std::net::Ipv4Addr;

pub fn draw(
    app: &mut AppState,
    cmd_tx: Option<tokio::sync::mpsc::Sender<feiq_v2::types::CoreCommand>>,
    ui: &mut egui::Ui,
) {
    if app.settings_active {
        return;
    }

    if let Some(ip) = app.selected_peer_ip {
        let peer = if let Some(p) = app.peers.iter().find(|p| p.ip == ip) {
            p.clone()
        } else {
            return;
        };

        // 1. Bottom chat action controls panel (declared first so it docks securely to the bottom)
        Panel::bottom("chat_bottom_panel")
            .frame(egui::Frame::new().inner_margin(Margin::symmetric(0, 8)))
            .show(ui, |ui| {
                ui.add_space(4.0);

                // Small Action Toolbar (Emojis, Files, Nudge) positioned between history and text box
                ui.horizontal(|ui| {
                    ui.style_mut().spacing.button_padding = Vec2::new(6.0, 3.0);
                    let toolbar_btn_size = Vec2::new(95.0, 20.0);

                    let emoji_btn = ui.add_sized(toolbar_btn_size, Button::new("😀 Emoji").small());
                    if emoji_btn.clicked() {
                        println!("Classic Emoji selector clicked!");
                    }

                    let file_btn = ui.add_sized(toolbar_btn_size, Button::new("📎 文件").small());
                    if file_btn.clicked() {
                        app.show_share_file_dialog = true;
                    }

                    // Screen Nudge/Shake button in the action toolbar
                    let nudge_btn = ui.add_sized(toolbar_btn_size, Button::new("⚡ 抖动").small());

                    if nudge_btn.clicked() {
                        app.trigger_screen_nudge(peer.ip, cmd_tx.as_ref());
                        ui.ctx().request_repaint();
                    }
                });

                ui.add_space(4.0);

                // Main Text Input and Send Row
                ui.horizontal(|ui| {
                    // Use TextEdit::multiline with 3 desired rows for a spacious default height (~60px)
                    let text_edit = TextEdit::multiline(&mut app.new_message_text)
                        .hint_text("输入消息...")
                        .desired_rows(3)
                        .lock_focus(true);

                    let response =
                        ui.add_sized(Vec2::new(ui.available_width() - 80.0, 60.0), text_edit);

                    // Send the message on Enter (and prevent newline insertion on simple Enter)
                    if response.has_focus()
                        && ui
                            .ctx()
                            .input(|i| i.key_pressed(egui::Key::Enter) && !i.modifiers.shift)
                    {
                        app.send_chat_message(cmd_tx.as_ref());
                        response.request_focus();
                    }

                    // Large, prominent Send button next to the text box
                    let send_btn = ui.add_sized(Vec2::new(70.0, 60.0), Button::new("发送"));
                    if send_btn.clicked() {
                        app.send_chat_message(cmd_tx.as_ref());
                        response.request_focus();
                    }
                });

                ui.add_space(8.0);
            });

        // Modal share file dialog
        if app.show_share_file_dialog {
            let mut open = true;
            egui::Window::new("分享文件")
                .open(&mut open)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    ui.label("输入要分享的本地文件路径:");
                    ui.text_edit_singleline(&mut app.file_to_share_path);
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("发送文件").clicked() {
                            let path = std::path::PathBuf::from(app.file_to_share_path.trim());
                            if path.exists() {
                                if let Some(ref cmd_tx) = cmd_tx {
                                    let _ =
                                        cmd_tx.try_send(feiq_v2::types::CoreCommand::ShareFile {
                                            peer_ip: peer.ip.to_string(),
                                            path: path.clone(),
                                        });
                                }
                                app.file_to_share_path.clear();
                                app.show_share_file_dialog = false;
                            } else {
                                // Red text error is fine
                            }
                        }
                        if ui.button("取消").clicked() {
                            app.show_share_file_dialog = false;
                        }
                    });
                });
            if !open {
                app.show_share_file_dialog = false;
            }
        }

        // 2. Chat Header
        ui.horizontal(|ui| {
            // Status dot (Deduplicated!)
            let dot_color = crate::views::peer_status_color(&peer);
            ui.painter()
                .circle_filled(ui.cursor().min + Vec2::new(10.0, 10.0), 6.0, dot_color);
            ui.add_space(20.0);

            ui.vertical(|ui| {
                ui.label(
                    RichText::new(&peer.nickname)
                        .strong()
                        .text_style(egui::TextStyle::Heading),
                );
                ui.label(
                    RichText::new(format!(
                        "{} | {} | {}",
                        peer.ip, peer.username, peer.hostname
                    ))
                    .weak()
                    .text_style(egui::TextStyle::Small),
                );
            });
        });

        ui.add_space(4.0);
        ui.separator();
        ui.add_space(8.0);

        let current_theme = app.preferences.theme;
        let cmd_tx = cmd_tx.clone();

        // 3. Conversation Scroll Area (occupies all remaining central space)
        ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                if let Some(msg_list) = app.messages.get_mut(&peer.ip) {
                    let max_bubble_width = ui.available_width() * 0.7;

                    for msg in msg_list {
                        ui.add_space(6.0);
                        let is_out = msg.is_outgoing;

                        ui.push_id(msg.id, |ui| {
                            ui.horizontal(|ui| {
                                if is_out {
                                    ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
                                        draw_bubble_contents(
                                            ui,
                                            current_theme,
                                            peer.ip,
                                            msg,
                                            &cmd_tx,
                                            max_bubble_width,
                                        );
                                    });
                                } else {
                                    ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                                        draw_bubble_contents(
                                            ui,
                                            current_theme,
                                            peer.ip,
                                            msg,
                                            &cmd_tx,
                                            max_bubble_width,
                                        );
                                    });
                                }
                            });
                        });
                    }
                }
            });
    } else {
        // No peer selected
        ui.with_layout(Layout::top_down(Align::Center), |ui| {
            ui.add_space(ui.available_height() / 2.0 - 20.0);
            ui.label(
                RichText::new("开始聊天")
                    .weak()
                    .text_style(egui::TextStyle::Body),
            );
        });
    }
}

fn draw_file_card(
    theme: crate::app::Theme,
    ui: &mut egui::Ui,
    peer_ip: Ipv4Addr,
    packet_no: Option<u32>,
    file: &mut feiq_v2::types::FileAttachment,
    cmd_tx: &Option<tokio::sync::mpsc::Sender<feiq_v2::types::CoreCommand>>,
) {
    ui.push_id((file.id, packet_no), |ui| {
        let card_width = 290.0;

        // Background and border colors based on monochromatic theme
        let is_dark = theme == crate::app::Theme::Dark;
        let card_bg = if is_dark {
            Color32::from_rgb(0x2d, 0x2d, 0x2d)
        } else {
            Color32::from_rgb(0xf5, 0xf5, 0xf5)
        };
        let stroke_color = if is_dark {
            Color32::from_rgb(0x3d, 0x3d, 0x3d)
        } else {
            Color32::from_rgb(0xe0, 0xe0, 0xe0)
        };

        let title_color = if is_dark {
            Color32::from_rgb(0xf5, 0xf5, 0xf5)
        } else {
            Color32::from_rgb(0x11, 0x11, 0x11)
        };
        let size_color = if is_dark {
            Color32::from_rgb(0xaa, 0xaa, 0xaa)
        } else {
            Color32::from_rgb(0x66, 0x66, 0x66)
        };

        Frame::new()
            .fill(card_bg)
            .inner_margin(Margin::same(12))
            .corner_radius(6)
            .stroke(egui::Stroke::new(1.0, stroke_color))
            .show(ui, |ui| {
                ui.set_width(card_width - 24.0); // subtract margins

                ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                    ui.with_layout(Layout::top_down(Align::Min), |ui| {
                        ui.set_width(card_width - 24.0);

                        // 1. Top Section: Horizontal layout [Text details | File Icon Box]
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.set_width(190.0);
                                // Wrap text or truncate filenames elegantly
                                let file_name_truncated =
                                    crate::views::truncate_string(&file.name, 22);
                                ui.label(
                                    RichText::new(file_name_truncated)
                                        .strong()
                                        .text_style(egui::TextStyle::Body)
                                        .color(title_color),
                                );
                                ui.add_space(4.0);

                                let size_str = feiq_v2::protocol::format_file_size(file.size);
                                ui.label(
                                    RichText::new(size_str)
                                        .weak()
                                        .text_style(egui::TextStyle::Small)
                                        .color(size_color),
                                );
                            });

                            // Align right: Icon Box
                            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                let icon_bg = if is_dark {
                                    Color32::from_rgb(0x3d, 0x3d, 0x3d)
                                } else {
                                    Color32::from_rgb(0xe5, 0xe5, 0xe5)
                                };
                                Frame::new()
                                    .fill(icon_bg)
                                    .corner_radius(4)
                                    .inner_margin(Margin::same(6))
                                    .show(ui, |ui| {
                                        ui.label(
                                            RichText::new("📄").text_style(FILE_ICON_STYLE.clone()),
                                        );
                                    });
                            });
                        });

                        // 2. Middle Section: Progress Bar (shown when downloading)
                        if file.status == feiq_v2::types::TransferStatus::Transferring {
                            ui.add_space(8.0);
                            let time = ui.input(|i| i.time);
                            let fluctuation = (time as f32 * 2.0).sin() * 1.5;
                            let speed_mbps = (5.0 + fluctuation).max(1.0);
                            let speed_text = format!("{:.1} MB/s", speed_mbps);
                            let progress_pct = file.progress * 100.0;
                            ui.add(
                                egui::ProgressBar::new(file.progress as f32)
                                    .text(format!("{:.0}% ({})", progress_pct, speed_text))
                                    .animate(true),
                            );
                        }

                        // 3. Bottom Section: Action Controls
                        ui.add_space(8.0);
                        ui.horizontal(|ui| match file.status {
                            feiq_v2::types::TransferStatus::Pending => {
                                let btn = ui.add(Button::new("📥 下载").small());
                                if btn.clicked() {
                                    if let Some(p_no) = packet_no {
                                        if let Some(sender) = cmd_tx {
                                            let _ = sender.try_send(
                                                feiq_v2::types::CoreCommand::DownloadFile {
                                                    peer_ip: peer_ip.to_string(),
                                                    packet_no: p_no,
                                                    file_id: file.id,
                                                    name: file.name.clone(),
                                                    size: file.size,
                                                },
                                            );
                                        }
                                    }
                                    file.status = feiq_v2::types::TransferStatus::Transferring;
                                }
                            }
                            feiq_v2::types::TransferStatus::Transferring => {
                                let btn = ui.add(Button::new("❌ 取消").small());
                                if btn.clicked() {
                                    file.status = feiq_v2::types::TransferStatus::Pending;
                                    file.progress = 0.0;
                                }
                            }
                            feiq_v2::types::TransferStatus::Completed => {
                                let btn = ui.add(Button::new("📂 打开").small());
                                if btn.clicked() {
                                    println!("Opened file (mock)");
                                }
                                btn.on_hover_text("模拟打开文件");
                            }
                            feiq_v2::types::TransferStatus::Failed => {
                                let btn = ui.add(Button::new("🔄 重试").small());
                                if btn.clicked() {
                                    if let Some(p_no) = packet_no {
                                        if let Some(sender) = cmd_tx {
                                            let _ = sender.try_send(
                                                feiq_v2::types::CoreCommand::DownloadFile {
                                                    peer_ip: peer_ip.to_string(),
                                                    packet_no: p_no,
                                                    file_id: file.id,
                                                    name: file.name.clone(),
                                                    size: file.size,
                                                },
                                            );
                                        }
                                    }
                                    file.status = feiq_v2::types::TransferStatus::Transferring;
                                }
                            }
                        });
                    });
                });
            });
    });
}

fn draw_bubble_contents(
    ui: &mut egui::Ui,
    current_theme: crate::app::Theme,
    peer_ip: Ipv4Addr,
    msg: &mut crate::app::Message,
    cmd_tx: &Option<tokio::sync::mpsc::Sender<feiq_v2::types::CoreCommand>>,
    max_bubble_width: f32,
) {
    ui.set_max_width(max_bubble_width);

    if let Some(ref mut file) = msg.file {
        draw_file_card(current_theme, ui, peer_ip, msg.packet_no, file, cmd_tx);
    } else {
        let is_dark = current_theme == crate::app::Theme::Dark;
        let (bg_color, text_color) = if msg.is_outgoing {
            let bg = if is_dark {
                Color32::from_rgb(0x4d, 0x4d, 0x4d)
            } else {
                Color32::from_rgb(0xdf, 0xe1, 0xe5)
            };
            let fg = if is_dark {
                Color32::WHITE
            } else {
                Color32::from_rgb(0x11, 0x11, 0x11)
            };
            (bg, Some(fg))
        } else {
            let bg = if is_dark {
                Color32::from_rgb(0x2d, 0x2d, 0x2d)
            } else {
                Color32::from_rgb(0xee, 0xee, 0xee)
            };
            (bg, None)
        };

        Frame::group(ui.style())
            .fill(bg_color)
            .inner_margin(Margin::same(8))
            .corner_radius(4)
            .show(ui, |ui| {
                let mut rich_text = RichText::new(&msg.content).text_style(egui::TextStyle::Body);
                if let Some(color) = text_color {
                    rich_text = rich_text.color(color);
                }
                ui.add(egui::Label::new(rich_text).wrap());
            });
    }
}
