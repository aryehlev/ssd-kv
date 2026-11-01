//! Expiry commands (DESIGN §8.4, §16.3, LC-10): EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT with
//! NX / XX / GT / LT (GT/LT treat "no TTL" as infinite; a non-positive relative time or a past
//! absolute time deletes the key), TTL / PTTL / EXPIRETIME / PEXPIRETIME (rounding
//! `(ms + 500) / 1000`), PERSIST. Every comparison of two expiry values lives in this module.
