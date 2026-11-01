//! Redaction of user bytes in diagnostics (DESIGN LC-32).
//!
//! Logs at every level, metrics, panic and assert messages, `Debug` output and error replies
//! that reach a log never contain key, field or value bytes. They carry the length and at most a
//! 32-bit non-cryptographic xxh3 tag through [`Redacted`], rendered as `<len=N tag=xxxxxxxx>`.
//! Types that hold user bytes implement `Debug` through it instead of deriving it.

use core::fmt;

use xxhash_rust::xxh3::xxh3_64;

/// Diagnostic stand-in for user bytes: `<len=N tag=xxxxxxxx>`.
pub struct Redacted<'a>(pub &'a [u8]);

impl Redacted<'_> {
    /// The 32-bit tag: the low half of the unseeded xxh3-64 of the bytes.
    pub fn tag(&self) -> u32 {
        xxh3_64(self.0) as u32
    }
}

impl fmt::Display for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<len={} tag={:08x}>", self.0.len(), self.tag())
    }
}

impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::Redacted;

    #[test]
    fn never_shows_bytes() {
        let s = format!("{} {:?}", Redacted(b"secret-key"), Redacted(b"secret-key"));
        assert!(!s.contains("secret"));
        assert!(s.starts_with("<len=10 tag="));
    }
}
