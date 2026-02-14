//! Hash data type: field-value map stored as a single blob.
//!
//! Binary format (version 1):
//! ```text
//! [1 byte]  version (1)
//! [4 bytes] field_count (u32 LE)
//! Per field:
//!   [2 bytes]  field_name_len (u16 LE)
//!   [4 bytes]  value_len (u32 LE)
//!   [8 bytes]  expiry_ms (i64 LE, absolute Unix ms; 0 = no expiry)
//!   [N bytes]  field_name
//!   [M bytes]  value
//! ```

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const HASH_BLOB_VERSION: u8 = 1;

/// A single field inside a hash.
#[derive(Debug, Clone)]
pub struct HashField {
    pub value: Vec<u8>,
    /// Absolute expiry in milliseconds since epoch. 0 = no expiry.
    pub expiry_ms: i64,
}

/// In-memory representation of a Redis Hash, deserialized from a blob.
#[derive(Debug, Clone)]
pub struct HashValue {
    pub fields: HashMap<Vec<u8>, HashField>,
}

impl HashValue {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Deserialize from the binary blob format.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return Some(Self::new());
        }
        if data.len() < 5 {
            return None;
        }
        let version = data[0];
        if version != HASH_BLOB_VERSION {
            return None;
        }
        let field_count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let mut offset = 5;
        let mut fields = HashMap::with_capacity(field_count);

        for _ in 0..field_count {
            if offset + 14 > data.len() {
                return None;
            }
            let name_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;
            let value_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
            offset += 4;
            let expiry_ms = i64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            offset += 8;

            if offset + name_len + value_len > data.len() {
                return None;
            }
            let name = data[offset..offset + name_len].to_vec();
            offset += name_len;
            let value = data[offset..offset + value_len].to_vec();
            offset += value_len;

            fields.insert(name, HashField { value, expiry_ms });
        }

        Some(Self { fields })
    }

    /// Serialize to the binary blob format, skipping expired fields.
    pub fn to_bytes(&self) -> Vec<u8> {
        let now_ms = now_millis();
        // Count live fields
        let live_fields: Vec<_> = self.fields.iter()
            .filter(|(_, f)| f.expiry_ms == 0 || f.expiry_ms > now_ms)
            .collect();

        let mut buf = Vec::with_capacity(5 + live_fields.len() * 20);
        buf.push(HASH_BLOB_VERSION);
        buf.extend_from_slice(&(live_fields.len() as u32).to_le_bytes());

        for (name, field) in &live_fields {
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(&(field.value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&field.expiry_ms.to_le_bytes());
            buf.extend_from_slice(name);
            buf.extend_from_slice(&field.value);
        }

        buf
    }

    /// Remove expired fields in-place. Returns number removed.
    pub fn purge_expired(&mut self) -> usize {
        let now_ms = now_millis();
        let before = self.fields.len();
        self.fields.retain(|_, f| f.expiry_ms == 0 || f.expiry_ms > now_ms);
        before - self.fields.len()
    }

    /// Number of live (non-expired) fields.
    pub fn len(&self) -> usize {
        let now_ms = now_millis();
        self.fields.iter().filter(|(_, f)| f.expiry_ms == 0 || f.expiry_ms > now_ms).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a field value if it exists and is not expired.
    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        let f = self.fields.get(field)?;
        if f.expiry_ms != 0 && f.expiry_ms <= now_millis() {
            return None;
        }
        Some(&f.value)
    }

    /// Set a field. Returns true if the field is new (did not exist before).
    pub fn set(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        let is_new = !self.fields.contains_key(&field);
        self.fields.insert(field, HashField { value, expiry_ms: 0 });
        is_new
    }

    /// Set a field only if it does not exist. Returns true if set.
    pub fn set_nx(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        if let Some(existing) = self.fields.get(&field) {
            if existing.expiry_ms == 0 || existing.expiry_ms > now_millis() {
                return false;
            }
        }
        self.fields.insert(field, HashField { value, expiry_ms: 0 });
        true
    }

    /// Delete a field. Returns true if it existed.
    pub fn del(&mut self, field: &[u8]) -> bool {
        self.fields.remove(field).is_some()
    }

    /// Get the value of a field and delete it. Returns None if the field doesn't exist or is expired.
    pub fn get_and_del(&mut self, field: &[u8]) -> Option<Vec<u8>> {
        let now_ms = now_millis();
        match self.fields.remove(field) {
            Some(f) if f.expiry_ms == 0 || f.expiry_ms > now_ms => Some(f.value),
            _ => None,
        }
    }

    /// Check if a field exists and is not expired.
    pub fn exists(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    /// Get all live field names.
    pub fn keys(&self) -> Vec<&[u8]> {
        let now_ms = now_millis();
        self.fields.iter()
            .filter(|(_, f)| f.expiry_ms == 0 || f.expiry_ms > now_ms)
            .map(|(k, _)| k.as_slice())
            .collect()
    }

    /// Get all live field values.
    pub fn values(&self) -> Vec<&[u8]> {
        let now_ms = now_millis();
        self.fields.iter()
            .filter(|(_, f)| f.expiry_ms == 0 || f.expiry_ms > now_ms)
            .map(|(_, f)| f.value.as_slice())
            .collect()
    }

    /// Increment an integer field by delta. Returns new value or error string.
    pub fn incr_by(&mut self, field: &[u8], delta: i64) -> Result<i64, String> {
        let (current, existing_expiry) = match self.fields.get(field) {
            Some(f) => {
                let val = std::str::from_utf8(&f.value)
                    .map_err(|_| "ERR hash value is not an integer".to_string())?
                    .parse::<i64>()
                    .map_err(|_| "ERR hash value is not an integer".to_string())?;
                (val, f.expiry_ms)
            }
            None => (0, 0),
        };
        let new_val = current.checked_add(delta)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
        let new_bytes = new_val.to_string().into_bytes();
        self.fields.insert(field.to_vec(), HashField { value: new_bytes, expiry_ms: existing_expiry });
        Ok(new_val)
    }

    /// Increment a float field by delta. Returns new value or error string.
    pub fn incr_by_float(&mut self, field: &[u8], delta: f64) -> Result<f64, String> {
        let (current, existing_expiry) = match self.fields.get(field) {
            Some(f) => {
                let val = std::str::from_utf8(&f.value)
                    .map_err(|_| "ERR hash value is not a valid float".to_string())?
                    .parse::<f64>()
                    .map_err(|_| "ERR hash value is not a valid float".to_string())?;
                (val, f.expiry_ms)
            }
            None => (0.0, 0),
        };
        let new_val = current + delta;
        if !new_val.is_finite() {
            return Err("ERR increment would produce NaN or Infinity".to_string());
        }
        let new_bytes = format_float(new_val).into_bytes();
        self.fields.insert(field.to_vec(), HashField { value: new_bytes, expiry_ms: existing_expiry });
        Ok(new_val)
    }

    /// Set expiry on specific fields. Returns per-field result codes:
    /// 2 = field doesn't exist or expired, 1 = expiry set, 0 = condition not met
    pub fn set_field_expiry(&mut self, fields: &[&[u8]], expiry_ms: i64, nx: bool, xx: bool, gt: bool, lt: bool) -> Vec<i64> {
        let now_ms = now_millis();
        fields.iter().map(|name| {
            match self.fields.get_mut(*name) {
                None => -2,
                Some(f) => {
                    if f.expiry_ms != 0 && f.expiry_ms <= now_ms {
                        return -2; // expired
                    }
                    let current = f.expiry_ms;
                    if nx && current != 0 { return 0; }
                    if xx && current == 0 { return 0; }
                    if gt && current != 0 && expiry_ms <= current { return 0; }
                    if lt && (current == 0 || expiry_ms >= current) { return 0; }
                    f.expiry_ms = expiry_ms;
                    1
                }
            }
        }).collect()
    }

    /// Remove expiry from fields.
    pub fn persist_fields(&mut self, fields: &[&[u8]]) -> Vec<i64> {
        let now_ms = now_millis();
        fields.iter().map(|name| {
            match self.fields.get_mut(*name) {
                None => -2,
                Some(f) => {
                    if f.expiry_ms != 0 && f.expiry_ms <= now_ms {
                        return -2;
                    }
                    if f.expiry_ms == 0 {
                        return -1; // no expiry
                    }
                    f.expiry_ms = 0;
                    1
                }
            }
        }).collect()
    }

    /// Get TTL for fields in milliseconds.
    pub fn field_pttl(&self, fields: &[&[u8]]) -> Vec<i64> {
        let now_ms = now_millis();
        fields.iter().map(|name| {
            match self.fields.get(*name) {
                None => -2,
                Some(f) => {
                    if f.expiry_ms != 0 && f.expiry_ms <= now_ms {
                        return -2;
                    }
                    if f.expiry_ms == 0 {
                        -1
                    } else {
                        (f.expiry_ms - now_ms).max(0)
                    }
                }
            }
        }).collect()
    }

    /// Get expiry time for fields in milliseconds (absolute).
    pub fn field_pexpiretime(&self, fields: &[&[u8]]) -> Vec<i64> {
        let now_ms = now_millis();
        fields.iter().map(|name| {
            match self.fields.get(*name) {
                None => -2,
                Some(f) => {
                    if f.expiry_ms != 0 && f.expiry_ms <= now_ms {
                        return -2;
                    }
                    if f.expiry_ms == 0 {
                        -1
                    } else {
                        f.expiry_ms
                    }
                }
            }
        }).collect()
    }
}

// ── Direct blob manipulation (no HashMap allocation) ─────────────

const FIELD_HEADER_SIZE: usize = 2 + 4 + 8; // name_len + value_len + expiry_ms

/// Build a blob for a single field without allocating a HashMap.
#[inline]
pub fn encode_single_field(name: &[u8], value: &[u8]) -> Vec<u8> {
    let len = 5 + FIELD_HEADER_SIZE + name.len() + value.len();
    let mut buf = Vec::with_capacity(len);
    buf.push(HASH_BLOB_VERSION);
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0i64.to_le_bytes()); // no expiry
    buf.extend_from_slice(name);
    buf.extend_from_slice(value);
    buf
}

/// Result of a direct blob HSET operation.
pub struct BlobSetResult {
    pub blob: Vec<u8>,
    pub new_fields: i64,
}

/// Set multiple field-value pairs directly in a blob without deserializing to HashMap.
/// Scans the existing blob, replaces matching fields in-place, appends new ones.
/// Skips expired fields during the copy. Returns the new blob and count of newly added fields.
pub fn blob_set_fields(existing: &[u8], fields: &[(&[u8], &[u8])]) -> BlobSetResult {
    // Handle empty/invalid existing blob
    if existing.len() < 5 || existing[0] != HASH_BLOB_VERSION {
        // Build new blob from scratch
        let total_payload: usize = fields.iter()
            .map(|(n, v)| FIELD_HEADER_SIZE + n.len() + v.len())
            .sum();
        let mut buf = Vec::with_capacity(5 + total_payload);
        buf.push(HASH_BLOB_VERSION);
        buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (name, value) in fields {
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&0i64.to_le_bytes());
            buf.extend_from_slice(name);
            buf.extend_from_slice(value);
        }
        return BlobSetResult { blob: buf, new_fields: fields.len() as i64 };
    }

    let now_ms = now_millis();
    let field_count = u32::from_le_bytes([existing[1], existing[2], existing[3], existing[4]]) as usize;

    // Track which input fields have been matched (replaced) vs need appending
    let mut matched = vec![false; fields.len()];

    // First pass: calculate output size and find matches
    // We'll scan existing fields and decide keep/replace/skip
    struct ExistingField {
        offset: usize, // start of this field's header in the blob
        end: usize,    // end of this field (after value bytes)
        replace_idx: Option<usize>, // index into `fields` if being replaced
        expired: bool,
    }

    let mut existing_fields = Vec::with_capacity(field_count);
    let mut offset = 5;
    for _ in 0..field_count {
        if offset + FIELD_HEADER_SIZE > existing.len() {
            break;
        }
        let name_len = u16::from_le_bytes([existing[offset], existing[offset + 1]]) as usize;
        let value_len = u32::from_le_bytes([
            existing[offset + 2], existing[offset + 3],
            existing[offset + 4], existing[offset + 5],
        ]) as usize;
        let expiry_ms = i64::from_le_bytes([
            existing[offset + 6], existing[offset + 7], existing[offset + 8], existing[offset + 9],
            existing[offset + 10], existing[offset + 11], existing[offset + 12], existing[offset + 13],
        ]);
        let field_start = offset;
        offset += FIELD_HEADER_SIZE;

        if offset + name_len + value_len > existing.len() {
            break;
        }

        let name = &existing[offset..offset + name_len];
        let expired = expiry_ms != 0 && expiry_ms <= now_ms;

        // Check if this existing field is being replaced by one of our input fields
        let mut replace_idx = None;
        if !expired {
            for (i, (fn_name, _)) in fields.iter().enumerate() {
                if !matched[i] && name == *fn_name {
                    replace_idx = Some(i);
                    matched[i] = true;
                    break;
                }
            }
        }

        offset += name_len + value_len;
        existing_fields.push(ExistingField {
            offset: field_start,
            end: offset,
            replace_idx,
            expired,
        });
    }

    // Count output fields and estimate size
    let kept_fields = existing_fields.iter().filter(|f| !f.expired).count();
    let new_fields_count = matched.iter().filter(|m| !**m).count();
    let total_fields = kept_fields + new_fields_count;

    // Estimate output size (rough upper bound)
    let mut est_size = 5;
    for ef in &existing_fields {
        if ef.expired { continue; }
        match ef.replace_idx {
            Some(i) => {
                est_size += FIELD_HEADER_SIZE + fields[i].0.len() + fields[i].1.len();
            }
            None => {
                est_size += ef.end - ef.offset;
            }
        }
    }
    for (i, (name, value)) in fields.iter().enumerate() {
        if !matched[i] {
            est_size += FIELD_HEADER_SIZE + name.len() + value.len();
        }
    }

    // Second pass: build output
    let mut buf = Vec::with_capacity(est_size);
    buf.push(HASH_BLOB_VERSION);
    buf.extend_from_slice(&(total_fields as u32).to_le_bytes());

    for ef in &existing_fields {
        if ef.expired { continue; }
        match ef.replace_idx {
            Some(i) => {
                // Write replacement field
                let (name, value) = fields[i];
                buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(&0i64.to_le_bytes()); // reset expiry on overwrite
                buf.extend_from_slice(name);
                buf.extend_from_slice(value);
            }
            None => {
                // Copy existing field as-is
                buf.extend_from_slice(&existing[ef.offset..ef.end]);
            }
        }
    }

    // Append new fields that didn't match existing ones
    for (i, (name, value)) in fields.iter().enumerate() {
        if !matched[i] {
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&0i64.to_le_bytes());
            buf.extend_from_slice(name);
            buf.extend_from_slice(value);
        }
    }

    BlobSetResult { blob: buf, new_fields: new_fields_count as i64 }
}

#[inline]
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn format_float(v: f64) -> String {
    if v == v.floor() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        let s = format!("{:.17}", v);
        let s = s.trim_end_matches('0');
        let s = s.trim_end_matches('.');
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_value_roundtrip() {
        let mut hv = HashValue::new();
        hv.set(b"field1".to_vec(), b"value1".to_vec());
        hv.set(b"field2".to_vec(), b"value2".to_vec());

        let blob = hv.to_bytes();
        let hv2 = HashValue::from_bytes(&blob).unwrap();

        assert_eq!(hv2.get(b"field1"), Some(b"value1".as_slice()));
        assert_eq!(hv2.get(b"field2"), Some(b"value2".as_slice()));
        assert_eq!(hv2.len(), 2);
    }

    #[test]
    fn test_hash_value_empty() {
        let hv = HashValue::new();
        let blob = hv.to_bytes();
        let hv2 = HashValue::from_bytes(&blob).unwrap();
        assert_eq!(hv2.len(), 0);
    }

    #[test]
    fn test_hash_set_overwrite() {
        let mut hv = HashValue::new();
        assert!(hv.set(b"f".to_vec(), b"v1".to_vec())); // new
        assert!(!hv.set(b"f".to_vec(), b"v2".to_vec())); // overwrite
        assert_eq!(hv.get(b"f"), Some(b"v2".as_slice()));
    }

    #[test]
    fn test_hash_del() {
        let mut hv = HashValue::new();
        hv.set(b"f".to_vec(), b"v".to_vec());
        assert!(hv.del(b"f"));
        assert!(!hv.del(b"f"));
        assert!(hv.is_empty());
    }

    #[test]
    fn test_hash_setnx() {
        let mut hv = HashValue::new();
        assert!(hv.set_nx(b"f".to_vec(), b"v1".to_vec()));
        assert!(!hv.set_nx(b"f".to_vec(), b"v2".to_vec()));
        assert_eq!(hv.get(b"f"), Some(b"v1".as_slice()));
    }

    #[test]
    fn test_hash_incr_by() {
        let mut hv = HashValue::new();
        assert_eq!(hv.incr_by(b"counter", 5).unwrap(), 5);
        assert_eq!(hv.incr_by(b"counter", 3).unwrap(), 8);
        assert_eq!(hv.incr_by(b"counter", -2).unwrap(), 6);
    }

    #[test]
    fn test_hash_incr_by_float() {
        let mut hv = HashValue::new();
        let v = hv.incr_by_float(b"f", 1.5).unwrap();
        assert!((v - 1.5).abs() < 1e-10);
        let v = hv.incr_by_float(b"f", 2.5).unwrap();
        assert!((v - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_hash_keys_values() {
        let mut hv = HashValue::new();
        hv.set(b"a".to_vec(), b"1".to_vec());
        hv.set(b"b".to_vec(), b"2".to_vec());
        let mut keys: Vec<_> = hv.keys();
        keys.sort();
        assert_eq!(keys, vec![b"a".as_slice(), b"b".as_slice()]);
    }

    #[test]
    fn test_hash_field_expiry() {
        let mut hv = HashValue::new();
        hv.set(b"f1".to_vec(), b"v1".to_vec());
        hv.set(b"f2".to_vec(), b"v2".to_vec());

        // Set expiry far in the future
        let future = now_millis() + 100_000;
        let results = hv.set_field_expiry(&[b"f1"], future, false, false, false, false);
        assert_eq!(results, vec![1]);

        // Check TTL
        let pttls = hv.field_pttl(&[b"f1", b"f2"]);
        assert!(pttls[0] > 0); // f1 has TTL
        assert_eq!(pttls[1], -1); // f2 has no expiry

        // Persist
        let results = hv.persist_fields(&[b"f1"]);
        assert_eq!(results, vec![1]);
        let pttls = hv.field_pttl(&[b"f1"]);
        assert_eq!(pttls[0], -1);
    }

    #[test]
    fn test_hash_expired_field() {
        let mut hv = HashValue::new();
        hv.fields.insert(b"expired".to_vec(), HashField {
            value: b"val".to_vec(),
            expiry_ms: 1, // expired long ago
        });
        assert_eq!(hv.get(b"expired"), None);
        assert_eq!(hv.len(), 0);
    }
}
