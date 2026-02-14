//! Set data type: unordered collection of unique byte strings stored as a single blob.
//!
//! Binary format (version 1):
//! ```text
//! [1 byte]  version (1)
//! [4 bytes] member_count (u32 LE)
//! Per member:
//!   [4 bytes]  member_len (u32 LE)
//!   [N bytes]  member_data
//! ```

use std::collections::HashSet;

const SET_BLOB_VERSION: u8 = 1;

/// In-memory representation of a Redis Set, deserialized from a blob.
#[derive(Debug, Clone)]
pub struct SetValue {
    pub members: HashSet<Vec<u8>>,
}

impl SetValue {
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
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
        if version != SET_BLOB_VERSION {
            return None;
        }
        let member_count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let mut offset = 5;
        let mut members = HashSet::with_capacity(member_count);

        for _ in 0..member_count {
            if offset + 4 > data.len() {
                return None;
            }
            let member_len = u32::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + member_len > data.len() {
                return None;
            }
            let member = data[offset..offset + member_len].to_vec();
            offset += member_len;
            members.insert(member);
        }

        Some(Self { members })
    }

    /// Serialize to the binary blob format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + self.members.len() * 12);
        buf.push(SET_BLOB_VERSION);
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());

        for member in &self.members {
            buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
            buf.extend_from_slice(member);
        }

        buf
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add a member. Returns true if the member was new.
    pub fn add(&mut self, member: Vec<u8>) -> bool {
        self.members.insert(member)
    }

    /// Remove a member. Returns true if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.members.remove(member)
    }

    /// Check if a member exists.
    pub fn is_member(&self, member: &[u8]) -> bool {
        self.members.contains(member)
    }

    /// Get all members.
    pub fn members(&self) -> Vec<&[u8]> {
        self.members.iter().map(|m| m.as_slice()).collect()
    }

    /// Return random members without removing. If allow_duplicates, may return the same member
    /// multiple times (used for SRANDMEMBER with negative count).
    pub fn random_members(&self, count: usize, allow_duplicates: bool) -> Vec<&[u8]> {
        if self.members.is_empty() || count == 0 {
            return Vec::new();
        }

        let members_vec: Vec<&[u8]> = self.members.iter().map(|m| m.as_slice()).collect();

        if allow_duplicates {
            // With duplicates: pick `count` random items (may repeat)
            let mut result = Vec::with_capacity(count);
            for i in 0..count {
                let idx = simple_random(i) % members_vec.len();
                result.push(members_vec[idx]);
            }
            result
        } else {
            // Without duplicates: pick min(count, len) unique items
            let take = count.min(members_vec.len());
            // Simple shuffle of indices
            let mut indices: Vec<usize> = (0..members_vec.len()).collect();
            for i in 0..take {
                let j = i + (simple_random(i) % (indices.len() - i));
                indices.swap(i, j);
            }
            indices[..take].iter().map(|&i| members_vec[i]).collect()
        }
    }

    /// Remove and return random members.
    pub fn pop(&mut self, count: usize) -> Vec<Vec<u8>> {
        if self.members.is_empty() || count == 0 {
            return Vec::new();
        }

        let take = count.min(self.members.len());
        let members_vec: Vec<Vec<u8>> = self.members.iter().cloned().collect();

        // Simple shuffle
        let mut indices: Vec<usize> = (0..members_vec.len()).collect();
        for i in 0..take {
            let j = i + (simple_random(i) % (indices.len() - i));
            indices.swap(i, j);
        }

        let mut result = Vec::with_capacity(take);
        for &idx in &indices[..take] {
            let member = members_vec[idx].clone();
            self.members.remove(&member);
            result.push(member);
        }
        result
    }

    /// Intersection of multiple sets.
    pub fn intersection(sets: &[&SetValue]) -> SetValue {
        if sets.is_empty() {
            return SetValue::new();
        }
        // Start with the smallest set for efficiency
        let mut sorted: Vec<&&SetValue> = sets.iter().collect();
        sorted.sort_by_key(|s| s.members.len());

        let first = sorted[0];
        let mut result = HashSet::new();
        for member in &first.members {
            if sorted[1..].iter().all(|s| s.members.contains(member)) {
                result.insert(member.clone());
            }
        }
        SetValue { members: result }
    }

    /// Union of multiple sets.
    pub fn union(sets: &[&SetValue]) -> SetValue {
        let mut result = HashSet::new();
        for set in sets {
            for member in &set.members {
                result.insert(member.clone());
            }
        }
        SetValue { members: result }
    }

    /// Difference: members in self that are not in any of the others.
    pub fn difference(&self, others: &[&SetValue]) -> SetValue {
        let mut result = HashSet::new();
        for member in &self.members {
            if !others.iter().any(|s| s.members.contains(member)) {
                result.insert(member.clone());
            }
        }
        SetValue { members: result }
    }
}

// ── Direct blob manipulation (no HashSet allocation) ─────────────

/// Build a blob for a single member without allocating a HashSet.
#[inline]
pub fn encode_single_member(member: &[u8]) -> Vec<u8> {
    let len = 5 + 4 + member.len();
    let mut buf = Vec::with_capacity(len);
    buf.push(SET_BLOB_VERSION);
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
    buf.extend_from_slice(member);
    buf
}

/// Result of a direct blob SADD operation.
pub struct BlobAddResult {
    pub blob: Vec<u8>,
    pub new_members: i64,
}

/// Add members to an existing set blob directly without deserializing to HashSet.
/// Scans existing blob to check for duplicates, appends new ones.
pub fn blob_add_members(existing: &[u8], members: &[&[u8]]) -> BlobAddResult {
    // Handle empty/invalid existing blob
    if existing.len() < 5 || existing[0] != SET_BLOB_VERSION {
        // Dedup incoming members
        let mut seen = HashSet::new();
        let unique: Vec<&[u8]> = members.iter().filter(|m| seen.insert(**m)).copied().collect();
        let total_payload: usize = unique.iter().map(|m| 4 + m.len()).sum();
        let mut buf = Vec::with_capacity(5 + total_payload);
        buf.push(SET_BLOB_VERSION);
        buf.extend_from_slice(&(unique.len() as u32).to_le_bytes());
        for member in &unique {
            buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
            buf.extend_from_slice(member);
        }
        return BlobAddResult { blob: buf, new_members: unique.len() as i64 };
    }

    let existing_count = u32::from_le_bytes([existing[1], existing[2], existing[3], existing[4]]) as usize;

    // Scan existing members to find duplicates
    let mut existing_members: HashSet<&[u8]> = HashSet::with_capacity(existing_count);
    let mut offset = 5;
    for _ in 0..existing_count {
        if offset + 4 > existing.len() { break; }
        let mlen = u32::from_le_bytes([
            existing[offset], existing[offset + 1], existing[offset + 2], existing[offset + 3],
        ]) as usize;
        offset += 4;
        if offset + mlen > existing.len() { break; }
        existing_members.insert(&existing[offset..offset + mlen]);
        offset += mlen;
    }

    // Find which members are new
    let new_members: Vec<&[u8]> = members.iter()
        .filter(|m| !existing_members.contains(**m))
        .copied()
        .collect();

    // Dedup the new members themselves
    let mut seen = HashSet::new();
    let unique_new: Vec<&[u8]> = new_members.into_iter()
        .filter(|m| seen.insert(*m))
        .collect();

    let new_count = unique_new.len();
    let total_count = existing_count + new_count;

    // Build new blob: copy existing data + append new
    let extra: usize = unique_new.iter().map(|m| 4 + m.len()).sum();
    let mut buf = Vec::with_capacity(existing.len() + extra);
    buf.push(SET_BLOB_VERSION);
    buf.extend_from_slice(&(total_count as u32).to_le_bytes());
    // Copy existing member data (everything after the 5-byte header)
    buf.extend_from_slice(&existing[5..]);
    // Append new members
    for member in &unique_new {
        buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
        buf.extend_from_slice(member);
    }

    BlobAddResult { blob: buf, new_members: new_count as i64 }
}

/// Remove members from an existing set blob directly. Returns (new_blob, count_removed).
pub fn blob_remove_members(existing: &[u8], members: &[&[u8]]) -> (Vec<u8>, i64) {
    if existing.len() < 5 || existing[0] != SET_BLOB_VERSION {
        return (existing.to_vec(), 0);
    }

    let to_remove: HashSet<&[u8]> = members.iter().copied().collect();
    let existing_count = u32::from_le_bytes([existing[1], existing[2], existing[3], existing[4]]) as usize;

    let mut kept = Vec::new();
    let mut removed = 0i64;
    let mut offset = 5;

    for _ in 0..existing_count {
        if offset + 4 > existing.len() { break; }
        let mlen = u32::from_le_bytes([
            existing[offset], existing[offset + 1], existing[offset + 2], existing[offset + 3],
        ]) as usize;
        let start = offset;
        offset += 4;
        if offset + mlen > existing.len() { break; }
        let member = &existing[offset..offset + mlen];
        offset += mlen;

        if to_remove.contains(member) {
            removed += 1;
        } else {
            kept.push(&existing[start..offset]);
        }
    }

    let new_count = kept.len();
    let mut buf = Vec::with_capacity(5 + kept.iter().map(|s| s.len()).sum::<usize>());
    buf.push(SET_BLOB_VERSION);
    buf.extend_from_slice(&(new_count as u32).to_le_bytes());
    for slice in &kept {
        buf.extend_from_slice(slice);
    }

    (buf, removed)
}

/// Simple deterministic pseudo-random for member selection.
/// Not cryptographically secure, just enough for SPOP/SRANDMEMBER.
#[inline]
fn simple_random(seed: usize) -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as usize)
        .unwrap_or(0);
    // Mix time with seed using a simple hash
    let mut h = t.wrapping_add(seed.wrapping_mul(6364136223846793005));
    h ^= h >> 16;
    h = h.wrapping_mul(0x45d9f3b);
    h ^= h >> 16;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_value_roundtrip() {
        let mut sv = SetValue::new();
        sv.add(b"member1".to_vec());
        sv.add(b"member2".to_vec());
        sv.add(b"member3".to_vec());

        let blob = sv.to_bytes();
        let sv2 = SetValue::from_bytes(&blob).unwrap();

        assert_eq!(sv2.len(), 3);
        assert!(sv2.is_member(b"member1"));
        assert!(sv2.is_member(b"member2"));
        assert!(sv2.is_member(b"member3"));
    }

    #[test]
    fn test_set_value_empty() {
        let sv = SetValue::new();
        let blob = sv.to_bytes();
        let sv2 = SetValue::from_bytes(&blob).unwrap();
        assert_eq!(sv2.len(), 0);
        assert!(sv2.is_empty());
    }

    #[test]
    fn test_set_add_remove() {
        let mut sv = SetValue::new();
        assert!(sv.add(b"a".to_vec()));  // new
        assert!(!sv.add(b"a".to_vec())); // duplicate
        assert_eq!(sv.len(), 1);

        assert!(sv.remove(b"a"));
        assert!(!sv.remove(b"a"));
        assert!(sv.is_empty());
    }

    #[test]
    fn test_set_membership() {
        let mut sv = SetValue::new();
        sv.add(b"x".to_vec());
        assert!(sv.is_member(b"x"));
        assert!(!sv.is_member(b"y"));
    }

    #[test]
    fn test_set_pop() {
        let mut sv = SetValue::new();
        sv.add(b"a".to_vec());
        sv.add(b"b".to_vec());
        sv.add(b"c".to_vec());

        let popped = sv.pop(2);
        assert_eq!(popped.len(), 2);
        assert_eq!(sv.len(), 1);

        // All popped members should not be in the set anymore
        for p in &popped {
            assert!(!sv.is_member(p));
        }
    }

    #[test]
    fn test_set_random_members() {
        let mut sv = SetValue::new();
        sv.add(b"a".to_vec());
        sv.add(b"b".to_vec());
        sv.add(b"c".to_vec());

        // Without duplicates
        let result = sv.random_members(2, false);
        assert_eq!(result.len(), 2);

        // With duplicates (negative count)
        let result = sv.random_members(5, true);
        assert_eq!(result.len(), 5);

        // Set should still have all members
        assert_eq!(sv.len(), 3);
    }

    #[test]
    fn test_set_intersection() {
        let mut s1 = SetValue::new();
        s1.add(b"a".to_vec());
        s1.add(b"b".to_vec());
        s1.add(b"c".to_vec());

        let mut s2 = SetValue::new();
        s2.add(b"b".to_vec());
        s2.add(b"c".to_vec());
        s2.add(b"d".to_vec());

        let inter = SetValue::intersection(&[&s1, &s2]);
        assert_eq!(inter.len(), 2);
        assert!(inter.is_member(b"b"));
        assert!(inter.is_member(b"c"));
    }

    #[test]
    fn test_set_union() {
        let mut s1 = SetValue::new();
        s1.add(b"a".to_vec());
        s1.add(b"b".to_vec());

        let mut s2 = SetValue::new();
        s2.add(b"b".to_vec());
        s2.add(b"c".to_vec());

        let u = SetValue::union(&[&s1, &s2]);
        assert_eq!(u.len(), 3);
        assert!(u.is_member(b"a"));
        assert!(u.is_member(b"b"));
        assert!(u.is_member(b"c"));
    }

    #[test]
    fn test_set_difference() {
        let mut s1 = SetValue::new();
        s1.add(b"a".to_vec());
        s1.add(b"b".to_vec());
        s1.add(b"c".to_vec());

        let mut s2 = SetValue::new();
        s2.add(b"b".to_vec());
        s2.add(b"d".to_vec());

        let diff = s1.difference(&[&s2]);
        assert_eq!(diff.len(), 2);
        assert!(diff.is_member(b"a"));
        assert!(diff.is_member(b"c"));
    }

    #[test]
    fn test_encode_single_member() {
        let blob = encode_single_member(b"hello");
        let sv = SetValue::from_bytes(&blob).unwrap();
        assert_eq!(sv.len(), 1);
        assert!(sv.is_member(b"hello"));
    }

    #[test]
    fn test_blob_add_members() {
        let blob = encode_single_member(b"a");
        let result = blob_add_members(&blob, &[b"b", b"c", b"a"]); // a is duplicate
        assert_eq!(result.new_members, 2);
        let sv = SetValue::from_bytes(&result.blob).unwrap();
        assert_eq!(sv.len(), 3);
        assert!(sv.is_member(b"a"));
        assert!(sv.is_member(b"b"));
        assert!(sv.is_member(b"c"));
    }

    #[test]
    fn test_blob_remove_members() {
        let mut sv = SetValue::new();
        sv.add(b"a".to_vec());
        sv.add(b"b".to_vec());
        sv.add(b"c".to_vec());
        let blob = sv.to_bytes();

        let (new_blob, removed) = blob_remove_members(&blob, &[b"b", b"x"]);
        assert_eq!(removed, 1);
        let sv2 = SetValue::from_bytes(&new_blob).unwrap();
        assert_eq!(sv2.len(), 2);
        assert!(sv2.is_member(b"a"));
        assert!(sv2.is_member(b"c"));
    }

    #[test]
    fn test_set_empty_after_from_bytes() {
        let sv = SetValue::from_bytes(&[]).unwrap();
        assert!(sv.is_empty());
    }

    #[test]
    fn test_duplicate_member_handling() {
        let result = blob_add_members(&[], &[b"a", b"a", b"b"]);
        assert_eq!(result.new_members, 2); // a counted once, b once
        let sv = SetValue::from_bytes(&result.blob).unwrap();
        assert_eq!(sv.len(), 2);
    }
}
