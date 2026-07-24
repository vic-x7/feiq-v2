use crate::app::{AppState, Theme};
use eframe::egui::{self, Align, ComboBox, Layout, RichText, ScrollArea, TextEdit};

pub fn draw(app: &mut AppState, ui: &mut egui::Ui) {
    if !app.settings_active {
        return;
    }

    ui.heading("设置");
    ui.add_space(12.0);

    ScrollArea::vertical().show(ui, |ui| {
        // Category 1: Local Identity
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.label(
                RichText::new("🛠 个人信息")
                    .strong()
                    .text_style(egui::TextStyle::Heading),
            );
            ui.add_space(8.0);

            egui::Grid::new("profile_grid")
                .num_columns(2)
                .spacing([12.0, 8.0])
                .min_col_width(100.0)
                .show(ui, |ui| {
                    ui.label("用户名:");
                    ui.add(
                        TextEdit::singleline(&mut app.identity.username).hint_text("local_user"),
                    );
                    ui.end_row();

                    ui.label("主机名:");
                    ui.add(TextEdit::singleline(&mut app.identity.hostname).hint_text("LOCAL-PC"));
                    ui.end_row();

                    ui.label("昵称:");
                    ui.add(TextEdit::singleline(&mut app.identity.nickname).hint_text("昵称"));
                    ui.end_row();

                    ui.label("个性签名:");
                    ui.add(
                        TextEdit::singleline(&mut app.identity.signature).hint_text("编辑个性签名"),
                    );
                    ui.end_row();
                });
        });

        ui.add_space(12.0);

        // Category 2: Network Config
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.label(
                RichText::new("🌐 网络设置")
                    .strong()
                    .text_style(egui::TextStyle::Heading),
            );
            ui.add_space(8.0);

            egui::Grid::new("network_grid")
                .num_columns(2)
                .spacing([12.0, 8.0])
                .min_col_width(100.0)
                .show(ui, |ui| {
                    ui.label("IPMsg 端口:");
                    ui.add(egui::DragValue::new(&mut app.networking.port));
                    ui.end_row();

                    ui.label("绑定 IP:");
                    ui.add(TextEdit::singleline(&mut app.networking.bind_ip).hint_text("0.0.0.0"));
                    ui.end_row();
                });

            ui.add_space(8.0);
            ui.label("广播网段:");
            ui.add_space(4.0);

            let mut subnet_to_delete = None;
            for (idx, subnet) in app.networking.broadcast_subnets.iter().enumerate() {
                ui.push_id(("subnet", subnet), |ui| {
                    ui.horizontal(|ui| {
                        ui.label(format!("• {}", subnet));
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            if ui.button("❌ 移除").clicked() {
                                subnet_to_delete = Some(idx);
                            }
                        });
                    });
                });
            }
            if let Some(idx) = subnet_to_delete {
                app.networking.broadcast_subnets.remove(idx);
            }

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.add(
                    TextEdit::singleline(&mut app.new_subnet_text)
                        .hint_text("例如：172.16.128.255/18"),
                );
                if ui.button("➕ 添加网段").clicked() && !app.new_subnet_text.trim().is_empty()
                {
                    let subnet = app.new_subnet_text.trim().to_string();
                    app.networking
                        .broadcast_subnets
                        .push(subnet);
                    app.new_subnet_text.clear();
                }
            });
        });

        ui.add_space(12.0);

        // Category 3: Preferences
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.label(
                RichText::new("⚙ 通用设置")
                    .strong()
                    .text_style(egui::TextStyle::Heading),
            );
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                ui.label("外观主题:");
                ComboBox::from_id_salt("theme_select")
                    .selected_text(match app.preferences.theme {
                        Theme::Dark => "深色模式",
                        Theme::Light => "浅色模式",
                    })
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut app.preferences.theme, Theme::Dark, "深色模式");
                        ui.selectable_value(&mut app.preferences.theme, Theme::Light, "浅色模式");
                    });
            });

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.label("下载目录:");
                ui.add(
                    TextEdit::singleline(&mut app.preferences.download_dir).hint_text("downloads"),
                );
            });

            ui.add_space(8.0);
            ui.checkbox(&mut app.preferences.sound_notify, "开启声音提示");
            ui.add_space(4.0);
            ui.checkbox(
                &mut app.preferences.screen_nudges_enabled,
                "允许接收窗口抖动",
            );
            ui.add_space(4.0);
            ui.checkbox(&mut app.preferences.enable_system_tray, "启用系统托盘图标");
            ui.add_space(4.0);
            ui.checkbox(
                &mut app.preferences.minimize_to_tray_on_close,
                "关闭主窗口时最小化到系统托盘",
            );
            ui.add_space(4.0);
            ui.checkbox(
                &mut app.preferences.desktop_notifications_enabled,
                "开启新消息桌面通知",
            );

            if app.preferences.enable_system_tray {
                ui.add_space(8.0);
                let tray_btn = ui.button("📥 立即最小化到系统托盘");
                if tray_btn.clicked() {
                    app.minimized_to_tray = true;
                }
                tray_btn.on_hover_text("模拟将应用程序最小化至系统托盘");
            }
        });
    });
}
