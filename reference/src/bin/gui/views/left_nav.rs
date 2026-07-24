use crate::app::{ActiveTab, AppState, NAV_ICON_STYLE};
use eframe::egui::{self, Align, Button, Color32, Layout, Panel, RichText, Vec2};

pub fn draw(app: &mut AppState, ui: &mut egui::Ui, side_frame: egui::Frame) {
    Panel::left("left_nav_strip")
        .resizable(false)
        .default_size(64.0)
        .frame(side_frame)
        .show(ui, |ui| {
            ui.with_layout(Layout::top_down(Align::Center), |ui| {
                ui.add_space(8.0);

                // Profile Avatar circle with initials
                let avatar_radius = 18.0;
                let (rect, _response) =
                    ui.allocate_exact_size(Vec2::splat(avatar_radius * 2.0), egui::Sense::hover());
                let center = rect.center();
                let theme_color = if app.preferences.theme == crate::app::Theme::Dark {
                    Color32::from_rgb(0x3d, 0x3d, 0x3d)
                } else {
                    Color32::from_rgb(0xdd, 0xdd, 0xdd)
                };
                ui.painter()
                    .circle_filled(center, avatar_radius, theme_color);

                let initial = app
                    .identity
                    .nickname
                    .chars()
                    .next()
                    .unwrap_or('U')
                    .to_string()
                    .to_uppercase();
                ui.painter().text(
                    center,
                    egui::Align2::CENTER_CENTER,
                    initial,
                    egui::TextStyle::Heading.resolve(ui.style()),
                    ui.visuals().widgets.active.text_color(),
                );

                ui.add_space(24.0);

                // Chats Tab Button
                let chat_active = app.active_tab == ActiveTab::Chats && !app.settings_active;
                let chat_btn = ui.add_sized(
                    Vec2::splat(40.0),
                    Button::new(RichText::new("💬").text_style(NAV_ICON_STYLE.clone()))
                        .selected(chat_active),
                );
                let chat_btn = chat_btn.on_hover_text("聊天");
                if chat_btn.clicked() {
                    app.active_tab = ActiveTab::Chats;
                    app.settings_active = false;
                }

                ui.add_space(12.0);

                // Friends/Discovery Tab Button
                let friends_active = app.active_tab == ActiveTab::Friends && !app.settings_active;
                let friends_btn = ui.add_sized(
                    Vec2::splat(40.0),
                    Button::new(RichText::new("👥").text_style(NAV_ICON_STYLE.clone()))
                        .selected(friends_active),
                );
                let friends_btn = friends_btn.on_hover_text("通讯录");
                if friends_btn.clicked() {
                    app.active_tab = ActiveTab::Friends;
                    app.settings_active = false;
                }

                // Bottom elements: Settings Button
                ui.with_layout(Layout::bottom_up(Align::Center), |ui| {
                    ui.add_space(8.0);
                    let settings_btn = ui.add_sized(
                        Vec2::splat(40.0),
                        Button::new(RichText::new("⚙").text_style(NAV_ICON_STYLE.clone()))
                            .selected(app.settings_active),
                    );
                    let settings_btn = settings_btn.on_hover_text("设置");
                    if settings_btn.clicked() {
                        app.settings_active = !app.settings_active;
                    }
                });
            });
        });
}
