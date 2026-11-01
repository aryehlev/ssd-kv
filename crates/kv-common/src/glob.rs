//! Glob matching with Redis `stringmatchlen` semantics (DESIGN §8.4, §16.6, LC-17).
//!
//! Used by KEYS and SCAN MATCH: a fixed built-in key-only filter (`*`, `?`, `[...]` with ranges
//! and `^` negation, `\` escapes, optional ASCII case folding). Behaviour follows Redis 7.0's
//! `stringmatchlen` (BSD-3-Clause, attribution in NOTICE) byte for byte, quirks included:
//!
//! * An empty subject matches only the empty pattern; in particular `*` does **not** match the
//!   empty key. Redis's KEYS and SCAN special-case the pattern `*` (exactly one byte) as "all
//!   keys" before calling the matcher, and so must the handlers: see [`is_match_all`].
//! * `[...]` classes: `\x` is a literal `x` (compared case-sensitively even under `nocase`),
//!   `a-z` ranges with reversed bounds swapped, `^` negation, and an unterminated class consumes
//!   the rest of the pattern. A `-` range takes the byte after `-` even when it is `]`.
//! * Range bounds and the subject byte compare as C `char`, which is signed on x86-64 (the
//!   reference platform): bytes `>= 0x80` sort below ASCII inside ranges.
//! * `\` escapes the next byte; a trailing `\` is a literal backslash.
//! * Case folding (`nocase`) is ASCII only (C locale `tolower`); under `nocase` range bounds are
//!   folded after the swap.
//!
//! The algorithm is the classic single-backtrack wildcard matcher (resume after the most recent
//! `*`), which returns exactly what Redis's recursive matcher with its "skip longer matches"
//! cut-off returns: every non-`*` token consumes exactly one byte, and tokenization does not
//! depend on the subject. It runs in O(|pattern| x |subject|) time, without recursion, so no
//! pattern can exhaust the stack.

/// Redis treats the one-byte pattern `*` as "match every key" (including the empty key)
/// without calling `stringmatchlen`; KEYS and SCAN MATCH handlers use this to do the same.
#[inline]
pub fn is_match_all(pattern: &[u8]) -> bool {
    pattern == b"*"
}

/// The pattern byte at `i`, or 0 past the end (the C matcher reads the string's NUL
/// terminator there).
#[inline(always)]
fn at(p: &[u8], i: usize) -> u8 {
    p.get(i).copied().unwrap_or(0)
}

#[inline(always)]
fn fold(b: u8, nocase: bool) -> u8 {
    if nocase { b.to_ascii_lowercase() } else { b }
}

/// Matches the single non-`*` token starting at `p` against the subject byte `c`; returns
/// whether it matched and the index of the next token.
#[inline]
fn match_token(pat: &[u8], mut p: usize, c: u8, nocase: bool) -> (bool, usize) {
    let len = pat.len();
    match pat[p] {
        b'?' => (true, p + 1),
        b'[' => {
            p += 1;
            let not = at(pat, p) == b'^';
            if not {
                p += 1;
            }
            let mut matched = false;
            loop {
                let rem = len - p;
                let b = at(pat, p);
                if b == b'\\' && rem >= 2 {
                    p += 1;
                    if pat[p] == c {
                        matched = true;
                    }
                } else if b == b']' {
                    break;
                } else if rem == 0 {
                    // unterminated class: step back so that the common advance below lands on
                    // the end of the pattern
                    p -= 1;
                    break;
                } else if rem >= 3 && pat[p + 1] == b'-' {
                    // C `char` comparisons: signed bytes on the reference platform
                    let (mut lo, mut hi) = (pat[p] as i8, pat[p + 2] as i8);
                    if lo > hi {
                        core::mem::swap(&mut lo, &mut hi);
                    }
                    let (lo, hi, x) = if nocase {
                        (fold(lo as u8, true) as i8, fold(hi as u8, true) as i8, fold(c, true) as i8)
                    } else {
                        (lo, hi, c as i8)
                    };
                    p += 2;
                    if x >= lo && x <= hi {
                        matched = true;
                    }
                } else if fold(b, nocase) == fold(c, nocase) {
                    matched = true;
                }
                p += 1;
            }
            (matched != not, p + 1)
        }
        b'\\' if len - p >= 2 => (fold(pat[p + 1], nocase) == fold(c, nocase), p + 2),
        b => (fold(b, nocase) == fold(c, nocase), p + 1),
    }
}

/// Redis `stringmatchlen(pattern, string, nocase)`.
pub fn glob_match(pattern: &[u8], s: &[u8], nocase: bool) -> bool {
    if s.is_empty() {
        return pattern.is_empty();
    }
    let plen = pattern.len();
    let (mut p, mut i) = (0usize, 0usize);
    // (pattern index after the most recent `*`, subject index that `*` resumes from)
    let mut star: Option<(usize, usize)> = None;
    loop {
        // invariant: i < s.len()
        if p < plen {
            if pattern[p] == b'*' {
                while p + 1 < plen && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == plen {
                    return true; // a trailing `*` matches the (non-empty) rest
                }
                p += 1;
                star = Some((p, i));
                continue;
            }
            let (ok, next) = match_token(pattern, p, s[i], nocase);
            if ok {
                p = next;
                i += 1;
                if i == s.len() {
                    // subject exhausted: only `*`s may remain
                    while p < plen && pattern[p] == b'*' {
                        p += 1;
                    }
                    // A shorter match for an earlier `*` leaves fewer bytes for the same
                    // remaining tokens, so a failure here is final (Redis's skip-longer-matches).
                    return p == plen;
                }
                continue;
            }
        }
        // mismatch, or pattern exhausted with subject left: let the last `*` absorb one more byte
        match star {
            Some((sp, si)) => {
                let si = si + 1;
                if si == s.len() {
                    // `*` never matches the whole remainder unless it is the last token
                    return false;
                }
                star = Some((sp, si));
                p = sp;
                i = si;
            }
            None => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Direct transcription of Redis 7.0 `stringmatchlen_impl` (recursive, with the
    /// `skipLongerMatches` cut-off), emulating the C string terminator and signed `char`.
    fn redis_ref(pattern: &[u8], s: &[u8], nocase: bool) -> bool {
        fn imp(pat: &[u8], mut p: usize, s: &[u8], mut i: usize, nocase: bool, skip: &mut bool) -> bool {
            let low = |b: u8| if nocase { b.to_ascii_lowercase() } else { b };
            let plen_total = pat.len();
            while p < plen_total && i < s.len() {
                match pat[p] {
                    b'*' => {
                        while p < plen_total && at(pat, p + 1) == b'*' {
                            p += 1;
                        }
                        if plen_total - p == 1 {
                            return true;
                        }
                        while i < s.len() {
                            if imp(pat, p + 1, s, i, nocase, skip) {
                                return true;
                            }
                            if *skip {
                                return false;
                            }
                            i += 1;
                        }
                        *skip = true;
                        return false;
                    }
                    b'?' => i += 1,
                    b'[' => {
                        p += 1;
                        let not = at(pat, p) == b'^';
                        if not {
                            p += 1;
                        }
                        let mut m = false;
                        loop {
                            let rem = plen_total - p;
                            if at(pat, p) == b'\\' && rem >= 2 {
                                p += 1;
                                if pat[p] == s[i] {
                                    m = true;
                                }
                            } else if at(pat, p) == b']' {
                                break;
                            } else if rem == 0 {
                                p -= 1;
                                break;
                            } else if rem >= 3 && pat[p + 1] == b'-' {
                                let mut start = i32::from(pat[p] as i8);
                                let mut end = i32::from(pat[p + 2] as i8);
                                let mut c = i32::from(s[i] as i8);
                                if start > end {
                                    core::mem::swap(&mut start, &mut end);
                                }
                                if nocase {
                                    let tl = |x: i32| if (65..=90).contains(&x) { x + 32 } else { x };
                                    start = tl(start);
                                    end = tl(end);
                                    c = tl(c);
                                }
                                p += 2;
                                if c >= start && c <= end {
                                    m = true;
                                }
                            } else if low(pat[p]) == low(s[i]) {
                                m = true;
                            }
                            p += 1;
                        }
                        if not {
                            m = !m;
                        }
                        if !m {
                            return false;
                        }
                        i += 1;
                    }
                    _ => {
                        if pat[p] == b'\\' && plen_total - p >= 2 {
                            p += 1;
                        }
                        if low(pat[p]) != low(s[i]) {
                            return false;
                        }
                        i += 1;
                    }
                }
                p += 1;
                if i == s.len() {
                    while at(pat, p) == b'*' {
                        p += 1;
                    }
                    break;
                }
            }
            p == plen_total && i == s.len()
        }
        let mut skip = false;
        imp(pattern, 0, s, 0, nocase, &mut skip)
    }

    /// Case-sensitive vectors captured from Redis 7.0.15 `KEYS` (pattern, key, matches).
    const KEYS_VECTORS: &[(&[u8], &[u8], bool)] = &[
        (b"h?llo", b"hello", true),
        (b"h?llo", b"hallo", true),
        (b"h?llo", b"hllo", false),
        (b"h*llo", b"hllo", true),
        (b"h*llo", b"heeeello", true),
        (b"h[ae]llo", b"hello", true),
        (b"h[ae]llo", b"hallo", true),
        (b"h[ae]llo", b"hillo", false),
        (b"h[^e]llo", b"hallo", true),
        (b"h[^e]llo", b"hello", false),
        (b"h[a-b]llo", b"hallo", true),
        (b"h[a-b]llo", b"hbllo", true),
        (b"h[a-b]llo", b"hcllo", false),
        (b"h[b-a]llo", b"hallo", true),
        (b"h\\*llo", b"h*llo", true),
        (b"h\\*llo", b"hello", false),
        (b"*", b"x", true),
        (b"**", b"", false),
        (b"a*", b"a", true),
        (b"*a", b"a", true),
        (b"*a*", b"bab", true),
        (b"a*b*c", b"abc", true),
        (b"a*b*c", b"aXbYc", true),
        (b"a*b*c", b"acb", false),
        (b"", b"", true),
        (b"", b"a", false),
        (b"?", b"", false),
        (b"[", b"[", false),
        (b"[a", b"a", true),
        (b"[a", b"b", false),
        (b"h[ae", b"he", true),
        (b"h[ae", b"hae", false),
        (b"[]", b"]", false),
        (b"[]]", b"]", false),
        (b"[\\]]", b"]", true),
        (b"[a-]", b"]", true),
        (b"[a-]", b"-", false),
        (b"\\", b"\\", true),
        (b"a\\", b"a\\", true),
        (b"[\\", b"\\", true),
        (b"[^", b"x", true),
        (b"[^]", b"x", true),
        (b"*[", b"ab[", false),
        (b"\x00", b"\x00", true),
        (b"[\x80-\xff]", b"\x90", true),
        (b"[\x80-\xff]", b"a", false),
        (b"[a-\xff]", b"a", true),
        (b"[a-\xff]", b"\x90", false), // signed bounds: [-1, 97]
        (b"[a-\xff]", b"z", false),
        (b"[\x00-\x7f]", b"\x90", false),
        (b"{user1}:*", b"{user1}:a", true),
        (b"*{*}*", b"{user1}:a", true),
        (b"a*b", b"a*b", true),
        (b"a\\*b", b"axb", false),
        (b"?*?", b"ab", true),
        (b"?*?", b"a", false),
        (b"*?", b"ab", true),
        (b"\r\n*", b"\r\nx", true),
    ];

    #[test]
    fn redis_keys_vectors() {
        for &(p, s, want) in KEYS_VECTORS {
            assert_eq!(glob_match(p, s, false), want, "pattern {p:?} key {s:?}");
            assert_eq!(redis_ref(p, s, false), want, "reference: pattern {p:?} key {s:?}");
        }
    }

    #[test]
    fn nocase_vectors() {
        // Checked against Redis 7.0.15 `CONFIG GET` with glob patterns (it matches with nocase).
        assert!(glob_match(b"MaxMem*", b"maxmemory", true));
        assert!(glob_match(b"MAXMEMORY*", b"maxmemory-policy", true));
        assert!(glob_match(b"*MEMORY-POL?CY", b"maxmemory-policy", true));
        assert!(glob_match(b"[L-N]axmemory", b"maxmemory", true));
        assert!(glob_match(b"[l-n]AXMEMORY?policy", b"maxmemory-policy", true));
        assert!(!glob_match(b"[\\M]axmemory", b"maxmemory", true), "class escapes are case-sensitive");
        assert!(glob_match(b"\\Maxmemor?", b"maxmemory", true));
        assert!(!glob_match(b"[^M]axmemory*", b"maxmemory", true));
        assert!(!glob_match(b"[^m]axmemory*", b"maxmemory", true));
        assert!(!glob_match(b"[Z-a]axmemory*", b"maxmemory", true), "bounds folded after the swap");
        assert!(glob_match(b"[Z-a]", b"_", false));
        assert!(!glob_match(b"MAXMEMORY", b"maxmemory", false));
        assert!(glob_match(b"\xc3\x89", b"\xc3\x89", true));
        assert!(!glob_match(b"\xc3\x89", b"\xc3\xa9", true), "ASCII folding only");
    }

    #[test]
    fn match_all_shortcut() {
        assert!(is_match_all(b"*"));
        assert!(!is_match_all(b"**"));
        assert!(!glob_match(b"*", b"", false), "stringmatchlen itself does not match ''");
    }

    #[test]
    fn pathological_patterns_are_fast() {
        let pat: Vec<u8> = b"a*".repeat(5000).into_iter().chain(*b"b").collect();
        let s = vec![b'a'; 10_000];
        let t = std::time::Instant::now();
        assert!(!glob_match(&pat, &s, false));
        assert!(t.elapsed() < std::time::Duration::from_secs(2));
        // deep star nesting that would overflow a recursive matcher's stack
        let pat = b"*a".repeat(200_000);
        assert!(!glob_match(&pat, b"aaaa", false));
    }

    fn token_bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            prop_oneof![
                Just(b'*'), Just(b'?'), Just(b'['), Just(b']'), Just(b'^'), Just(b'-'), Just(b'\\'),
                Just(b'a'), Just(b'b'), Just(b'A'), Just(b'B'), Just(b'z'), Just(0x80u8), Just(0xffu8), Just(0u8),
            ],
            0..10,
        )
    }

    fn subject_bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            prop_oneof![
                Just(b'a'), Just(b'b'), Just(b'A'), Just(b'B'), Just(b'z'), Just(b'-'), Just(b']'),
                Just(b'['), Just(b'*'), Just(b'\\'), Just(0x80u8), Just(0xffu8), Just(0u8),
            ],
            0..10,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4_000))]
        #[test]
        fn iterative_equals_redis_recursion(p in token_bytes(), s in subject_bytes(), nocase in any::<bool>()) {
            prop_assert_eq!(glob_match(&p, &s, nocase), redis_ref(&p, &s, nocase));
        }
    }
}
