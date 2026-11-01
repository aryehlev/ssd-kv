//! Glob matching with Redis `stringmatchlen` semantics (DESIGN §8.4, §16.6, LC-17).
//!
//! Used by KEYS and SCAN MATCH: a fixed built-in key-only filter (`*`, `?`, `[...]` with ranges
//! and `^` negation, `\` escapes, optional ASCII case folding). Ported from Redis <= 7.2
//! (BSD-3-Clause, attribution in NOTICE); unit tests port Redis's own test table (§25.1).

pub fn glob_match(pattern: &[u8], s: &[u8], nocase: bool) -> bool {
    todo!("DESIGN §8.4: Redis stringmatchlen")
}
