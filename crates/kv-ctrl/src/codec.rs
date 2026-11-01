//! Control frame codec (DESIGN §23.4).
//!
//! A frame is `u32 len (little endian) || body`, `1 <= len <= 16 MiB`, where
//! `body = postcard((PROTO_VERSION: u8, msg))`: one protocol-version byte followed by the
//! postcard encoding of the message. A receiver accepts versions
//! `MIN_PROTO_VERSION..=PROTO_VERSION` and rejects anything else with
//! [`CodecError::UnsupportedVersion`] before it looks at the message bytes; a body with bytes
//! left over after the message is malformed. The length is checked before any body byte is
//! buffered, so a peer cannot make the receiver hold more than one 16 MiB frame.

/// Largest control frame body (`len`), 16 MiB.
pub const MAX_FRAME: usize = 16 << 20;

/// Bytes of the length prefix.
pub const HEADER_LEN: usize = 4;

/// Control protocol version written by this build.
pub const PROTO_VERSION: u8 = 1;

/// Oldest control protocol version this build reads.
pub const MIN_PROTO_VERSION: u8 = 1;

static_assertions::const_assert!(MAX_FRAME == 16 * 1024 * 1024);
static_assertions::const_assert!(MAX_FRAME <= u32::MAX as usize);
static_assertions::const_assert!(MIN_PROTO_VERSION <= PROTO_VERSION);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CodecError {
    /// The frame length exceeds [`MAX_FRAME`].
    TooLarge,
    /// The body is empty, does not decode as the expected message, or has trailing bytes.
    Malformed,
    /// The body carries a protocol version outside `MIN_PROTO_VERSION..=PROTO_VERSION`.
    UnsupportedVersion(u8),
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CodecError::TooLarge => f.write_str("control frame exceeds 16 MiB"),
            CodecError::Malformed => f.write_str("malformed control frame"),
            CodecError::UnsupportedVersion(v) => {
                write!(f, "unsupported control protocol version {v}")
            }
        }
    }
}

impl std::error::Error for CodecError {}

/// Encodes one frame (`u32 len || postcard((PROTO_VERSION, msg))`) at the end of `out`.
///
/// # Panics
///
/// If the encoded message exceeds [`MAX_FRAME`] or does not serialize. Neither can happen for
/// the control message types of this crate, whose sizes are bounded far below 16 MiB (the
/// largest, `Assign`, is a few KiB); use [`try_encode`] for other types.
pub fn encode<T: serde::Serialize>(m: &T, out: &mut Vec<u8>) {
    if let Err(e) = try_encode(m, out) {
        panic!("control frame encoding failed: {e}");
    }
}

/// Encodes one frame at the end of `out`, or leaves `out` unchanged and reports why it cannot be
/// framed.
pub fn try_encode<T: serde::Serialize>(m: &T, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let body = postcard::to_stdvec(&(PROTO_VERSION, m)).map_err(|_| CodecError::Malformed)?;
    if body.len() > MAX_FRAME {
        return Err(CodecError::TooLarge);
    }
    // body.len() <= MAX_FRAME <= u32::MAX (asserted above).
    out.reserve(HEADER_LEN + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(())
}

/// Decodes one frame from `buf`: the message and the bytes consumed, or `Ok(None)` when more
/// bytes are needed. Errors are final for the connection (the stream cannot be resynchronized).
pub fn decode<T: serde::de::DeserializeOwned>(
    buf: &[u8],
) -> Result<Option<(T, usize)>, CodecError> {
    let Some(header) = buf.first_chunk::<HEADER_LEN>() else {
        return Ok(None);
    };
    let len = frame_len(*header)?;
    let Some(body) = buf.get(HEADER_LEN..HEADER_LEN + len) else {
        return Ok(None);
    };
    Ok(Some((decode_body(body)?, HEADER_LEN + len)))
}

/// The body length announced by a frame header, checked against the limits: for readers that
/// read the header and then exactly `len` body bytes.
pub fn frame_len(header: [u8; HEADER_LEN]) -> Result<usize, CodecError> {
    let len = u32::from_le_bytes(header) as usize;
    if len > MAX_FRAME {
        return Err(CodecError::TooLarge);
    }
    if len == 0 {
        return Err(CodecError::Malformed);
    }
    Ok(len)
}

/// Decodes a frame body (the `len` bytes after the header).
pub fn decode_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
    if body.len() > MAX_FRAME {
        return Err(CodecError::TooLarge);
    }
    let (&version, rest) = body.split_first().ok_or(CodecError::Malformed)?;
    if !(MIN_PROTO_VERSION..=PROTO_VERSION).contains(&version) {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let (m, tail) = postcard::take_from_bytes::<T>(rest).map_err(|_| CodecError::Malformed)?;
    if !tail.is_empty() {
        return Err(CodecError::Malformed);
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{CoordToNode, Health, NodeError, NodeToCoord};
    use crate::{CoordId, NodeId, ShardId};

    #[test]
    fn round_trip_and_partial() {
        let mut out = Vec::new();
        encode(&NodeToCoord::AssignAck { version: 7 }, &mut out);
        encode(&NodeToCoord::Error(NodeError::BadAuth), &mut out);
        let (a, n): (NodeToCoord, usize) = decode(&out).unwrap().unwrap();
        assert_eq!(a, NodeToCoord::AssignAck { version: 7 });
        let (b, m): (NodeToCoord, usize) = decode(&out[n..]).unwrap().unwrap();
        assert_eq!(b, NodeToCoord::Error(NodeError::BadAuth));
        assert_eq!(n + m, out.len());
        for cut in 0..n {
            assert!(decode::<NodeToCoord>(&out[..cut]).unwrap().is_none());
        }
    }

    #[test]
    fn frame_layout() {
        let mut out = Vec::new();
        encode(&NodeToCoord::SealDone { id: 3 }, &mut out);
        let len = u32::from_le_bytes(out[..4].try_into().unwrap()) as usize;
        assert_eq!(len, out.len() - 4);
        assert_eq!(out[4], PROTO_VERSION);
        // the rest is plain postcard of the message
        assert_eq!(
            &out[5..],
            &postcard::to_stdvec(&NodeToCoord::SealDone { id: 3 }).unwrap()[..]
        );
    }

    #[test]
    fn rejects_bad_frames() {
        // oversized length is refused from the header alone
        let big = ((MAX_FRAME + 1) as u32).to_le_bytes();
        assert_eq!(
            decode::<NodeToCoord>(&big).err(),
            Some(CodecError::TooLarge)
        );
        assert_eq!(frame_len(big), Err(CodecError::TooLarge));
        // zero length
        assert_eq!(
            decode::<NodeToCoord>(&[0, 0, 0, 0]).err(),
            Some(CodecError::Malformed)
        );
        // wrong version
        let mut out = Vec::new();
        encode(&NodeToCoord::SealDone { id: 3 }, &mut out);
        out[4] = PROTO_VERSION + 1;
        assert_eq!(
            decode::<NodeToCoord>(&out).err(),
            Some(CodecError::UnsupportedVersion(PROTO_VERSION + 1))
        );
        out[4] = 0;
        assert_eq!(
            decode::<NodeToCoord>(&out).err(),
            Some(CodecError::UnsupportedVersion(0))
        );
        // trailing bytes inside the frame
        let mut out = Vec::new();
        encode(&NodeToCoord::SealDone { id: 3 }, &mut out);
        out.push(0);
        let len = (out.len() - 4) as u32;
        out[..4].copy_from_slice(&len.to_le_bytes());
        assert_eq!(
            decode::<NodeToCoord>(&out).err(),
            Some(CodecError::Malformed)
        );
        // garbage body
        let out = [3u8, 0, 0, 0, PROTO_VERSION, 0xFF, 0xFF];
        assert_eq!(
            decode::<NodeToCoord>(&out).err(),
            Some(CodecError::Malformed)
        );
    }

    #[test]
    fn coord_messages_round_trip() {
        let msgs = vec![
            CoordToNode::Hello {
                cluster_id: [7; 16],
                coord: CoordId([1; 20]),
                coord_term: 9,
                auth: b"tok".to_vec(),
            },
            CoordToNode::LeaseGrant {
                seq: 4,
                lease_ms: 1500,
            },
            CoordToNode::Fence {
                req: 1,
                shard: ShardId(2),
                new_term: 3,
            },
            CoordToNode::Handoff {
                req: 2,
                shard: ShardId(2),
                to: NodeId([5; 20]),
            },
            CoordToNode::MigrationPhase {
                id: 8,
                phase: crate::MigPhase::Cutover,
            },
            CoordToNode::Ping,
        ];
        let mut out = Vec::new();
        for m in &msgs {
            encode(m, &mut out);
        }
        let mut off = 0;
        for m in &msgs {
            let (d, n): (CoordToNode, usize) = decode(&out[off..]).unwrap().unwrap();
            assert!(d == *m, "{}", m.kind());
            off += n;
        }
        assert_eq!(off, out.len());
        let renew = NodeToCoord::Renew {
            seq: 1,
            incarnation: 2,
            version_seen: 3,
            health: Health::default(),
        };
        let mut out = Vec::new();
        encode(&renew, &mut out);
        assert_eq!(
            frame_len(out[..4].try_into().unwrap()).unwrap(),
            out.len() - 4
        );
        assert_eq!(decode_body::<NodeToCoord>(&out[4..]).unwrap(), renew);
    }

    #[test]
    fn try_encode_reports_oversize_and_leaves_output() {
        let big = vec![0u8; MAX_FRAME];
        let mut out = vec![1, 2, 3];
        assert_eq!(try_encode(&big, &mut out), Err(CodecError::TooLarge));
        assert_eq!(out, vec![1, 2, 3]);
    }
}
