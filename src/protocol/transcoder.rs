use crate::error::AppError;
use std::borrow::Cow;
use encoding_rs::GBK;

pub trait TextTranscoder: Send + Sync + 'static {
    /// Decode received raw bytes into a Rust UTF-8 String.
    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Cow<'a, str>, AppError>;

    /// Encode a Rust UTF-8 string into a byte vector for sending.
    fn encode(&self, s: &str) -> Result<Vec<u8>, AppError>;
}

/// A transcoder using GBK encoding, maintaining historical Feiq compatibility.
#[derive(Debug, Clone, Copy)]
pub struct GbkTranscoder;

impl TextTranscoder for GbkTranscoder {
    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Cow<'a, str>, AppError> {
        let (decoded, _, has_errors) = GBK.decode(bytes);
        if has_errors {
            // We can return a protocol warning or just proceed with decoded Cow.
        }
        Ok(decoded)
    }

    fn encode(&self, s: &str) -> Result<Vec<u8>, AppError> {
        let (encoded_bytes, _, _) = GBK.encode(s);
        Ok(encoded_bytes.into_owned())
    }
}

/// A transcoder using standard UTF-8 encoding.
#[derive(Debug, Clone, Copy)]
pub struct Utf8Transcoder;

impl TextTranscoder for Utf8Transcoder {
    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Cow<'a, str>, AppError> {
        std::str::from_utf8(bytes)
            .map(Cow::Borrowed)
            .map_err(|e| AppError::Other(format!("UTF-8 decode error: {}", e)))
    }

    fn encode(&self, s: &str) -> Result<Vec<u8>, AppError> {
        Ok(s.as_bytes().to_vec())
    }
}
