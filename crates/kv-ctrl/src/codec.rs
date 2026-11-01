//! Control frame codec (DESIGN §23.4): `u32 len (little endian) || postcard(msg)`, at most
//! 16 MiB per frame.

/// Largest control frame payload.
pub const MAX_FRAME: usize = 16 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CodecError {
    TooLarge,
    Malformed,
}

/// u32 len || postcard
pub fn encode<T: serde::Serialize>(m: &T, out: &mut Vec<u8>) {
    let body = postcard::to_stdvec(m).expect("control messages always serialize");
    assert!(body.len() <= MAX_FRAME, "control frame exceeds 16 MiB");
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

/// Decodes one frame from `buf`: the message and the bytes consumed, or `Ok(None)` when more
/// bytes are needed.
pub fn decode<T: serde::de::DeserializeOwned>(buf: &[u8]) -> Result<Option<(T, usize)>, CodecError> {
    let Some(len) = buf.get(..4) else { return Ok(None) };
    let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]) as usize;
    if len > MAX_FRAME {
        return Err(CodecError::TooLarge);
    }
    let Some(body) = buf.get(4..4 + len) else { return Ok(None) };
    let m = postcard::from_bytes(body).map_err(|_| CodecError::Malformed)?;
    Ok(Some((m, 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{NodeError, NodeToCoord};

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
        assert!(decode::<NodeToCoord>(&out[..n - 1]).unwrap().is_none());
    }
}
