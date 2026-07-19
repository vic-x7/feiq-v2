pub const MAX_USERNAME_LEN: usize = 64;
pub const MAX_MESSAGE_LEN: usize = 65536;
pub const MAX_FILE_NAME_LEN: usize = 256;

/// Sanitizes username/hostname: trims leading/trailing whitespace and truncates to MAX_USERNAME_LEN chars.
pub fn sanitize_username(raw: &str) -> String {
    raw.trim().chars().take(MAX_USERNAME_LEN).collect()
}

/// Sanitizes message payloads: filters out control characters (except \n, \r, \t) and truncates to MAX_MESSAGE_LEN chars.
pub fn sanitize_message(raw: &str) -> String {
    raw.chars()
        .take(MAX_MESSAGE_LEN)
        .filter(|&c| !c.is_control() || c == '\n' || c == '\r' || c == '\t')
        .collect()
}

/// Sanitizes filenames: strips path separators ('/' and '\') and truncates to MAX_FILE_NAME_LEN chars.
pub fn sanitize_filename(raw: &str) -> String {
    raw.trim()
        .chars()
        .take(MAX_FILE_NAME_LEN)
        .filter(|&c| c != '/' && c != '\\')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_username_trim_and_truncate() {
        assert_eq!(sanitize_username("  alice  "), "alice");
        
        let long_username = "a".repeat(100);
        let sanitized = sanitize_username(&long_username);
        assert_eq!(sanitized.len(), MAX_USERNAME_LEN);
        assert_eq!(sanitized, "a".repeat(MAX_USERNAME_LEN));

        // UTF-8 characters should not be sliced in half
        let unicode_username = "🦀".repeat(100);
        let sanitized_unicode = sanitize_username(&unicode_username);
        assert_eq!(sanitized_unicode.chars().count(), MAX_USERNAME_LEN);
        assert_eq!(sanitized_unicode, "🦀".repeat(MAX_USERNAME_LEN));
    }

    #[test]
    fn test_sanitize_message_filter_and_truncate() {
        // Keeps basic whitespace controls
        let msg = "Hello\nWorld\r\tTest\x00\x01\x1FGoodbye";
        let sanitized = sanitize_message(msg);
        assert_eq!(sanitized, "Hello\nWorld\r\tTestGoodbye");

        // Truncation
        let long_msg = "x".repeat(70000);
        let sanitized_long = sanitize_message(&long_msg);
        assert_eq!(sanitized_long.chars().count(), MAX_MESSAGE_LEN);
        assert_eq!(sanitized_long, "x".repeat(MAX_MESSAGE_LEN));
    }

    #[test]
    fn test_sanitize_filename_strip_and_truncate() {
        assert_eq!(sanitize_filename("  foo/bar\\baz.txt  "), "foobarbaz.txt");
        assert_eq!(sanitize_filename("../../etc/passwd"), "....etcpasswd");

        let long_filename = "f".repeat(300);
        let sanitized_long = sanitize_filename(&long_filename);
        assert_eq!(sanitized_long.chars().count(), MAX_FILE_NAME_LEN);
        assert_eq!(sanitized_long, "f".repeat(MAX_FILE_NAME_LEN));
    }
}
