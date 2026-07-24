mod app;
mod app_state;
mod views;

use app::GuiApp;

/// Configures and returns standard startup options for the eframe/egui window.
/// Exposing this function makes it fully unit-testable.
pub fn build_native_options() -> eframe::NativeOptions {
    eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([900.0, 620.0])
            .with_min_inner_size([650.0, 480.0]),
        centered: true,
        ..Default::default()
    }
}

fn main() -> eframe::Result {
    let options = build_native_options();

    eframe::run_native(
        "FeiQ Successor",
        options,
        Box::new(|cc| Ok(Box::new(GuiApp::new(cc)))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_startup_configuration_spec() {
        let options = build_native_options();

        // Verify center option is enabled
        assert!(options.centered);

        // Verify initial viewport builder width and height bounds
        let viewport = &options.viewport;

        assert_eq!(viewport.inner_size, Some(eframe::egui::vec2(900.0, 620.0)));
        assert_eq!(
            viewport.min_inner_size,
            Some(eframe::egui::vec2(650.0, 480.0))
        );
    }
}
