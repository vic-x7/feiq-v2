pub mod chat_view;
pub mod left_nav;
pub mod middle_list;
pub mod settings;

use crate::app::Peer;
use eframe::egui::Color32;

pub fn peer_status_color(peer: &Peer) -> Color32 {
    if peer.online {
        Color32::from_rgb(0x4c, 0xaf, 0x50) // Green
    } else {
        Color32::from_rgb(0x9e, 0x9e, 0x9e) // Gray
    }
}

/// Utility function to safely truncate strings to a maximum length with ellipses,
/// trimming any trailing spaces before appending ellipses to avoid awkward spacing.
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.chars().count() > max_len {
        let take_len = if max_len > 3 { max_len - 3 } else { max_len };
        let taken: String = s.chars().take(take_len).collect();
        taken.trim_end().to_string() + "..."
    } else {
        s.to_string()
    }
}
