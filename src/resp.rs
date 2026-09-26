//! RESP (REdis Serialization Protocol) framing.
//!
//! This module owns the wire format only: the [`Frame`] value type, its
//! encoding, and an incremental [`Decoder`]. It knows nothing about commands or
//! I/O, so it can be unit-tested with plain byte slices.

use bytes::{Buf, BufMut, BytesMut};

/// A single RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SimpleString(Vec<u8>), // +PONG\r\n
    BulkString(Vec<u8>),   // $5\r\nHello\r\n
    Array(Vec<Frame>),     // *2\r\n$4\r\nECHO\r\n$5\r\nHello\r\n
    Err(String),           // -ERR msg\r\n
    Null
}

impl Frame {
    /// Append the RESP encoding of this frame to `out`.
    pub fn encode(&self, out: &mut BytesMut) {
        match self {
            Self::SimpleString(s) => {
                out.put_u8(b'+');
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            Self::BulkString(s) => {
                out.put_u8(b'$');
                out.extend_from_slice(s.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            Self::Err(msg) => {
                out.put_u8(b'-');
                out.extend_from_slice(b"ERR ");
                out.extend_from_slice(msg.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Array(items) => {
                out.put_u8(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for part in items {
                    part.encode(out);
                }
            },
            Self::Null => {
                out.extend_from_slice(b"$-1\r\n");
            }
        }
    }

    /// Convenience constructor: build a bulk string from any byte-like source
    /// (`&str`, `&[u8]`, ...), keeping keys and values binary-safe.
    pub fn bulk(data: impl AsRef<[u8]>) -> Self {
        Self::BulkString(data.as_ref().to_vec())
    }

    /// Convenience constructor: build a simple string from any byte-like source.
    pub fn simple(data: impl AsRef<[u8]>) -> Self {
        Self::SimpleString(data.as_ref().to_vec())
    }
}


/// Incremental RESP decoder: buffer bytes as they arrive and pull complete
/// frames out one at a time.
///
/// Consuming a frame is O(1) (`BytesMut::advance`) rather than the O(n) front
/// shift of draining a `Vec`.
#[derive(Default)]
pub struct Decoder {
    buf: BytesMut,
}

impl Decoder {
    pub fn new() -> Self {
        Decoder {
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Add freshly-read bytes to the internal buffer.
    pub fn extend(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Remove and return the first complete frame, if one is buffered.
    ///
    /// Returns `None` when the buffered bytes do not yet contain a whole frame.
    pub fn parse(&mut self) -> Option<Frame> {
        let (frame, len) = parse_frame(&self.buf)?;
        self.buf.advance(len);
        Some(frame)
    }
}

/// Parse one frame from the front of `buf`, returning it and the number of
/// bytes it occupied.
///
/// Only arrays of bulk strings are accepted; anything else (or an incomplete
/// frame) yields `None`.
fn parse_frame(buf: &[u8]) -> Option<(Frame, usize)> {
    let mut pos = 0;
    let argc = parse_header(buf, &mut pos, b'*')?;

    let mut items = Vec::with_capacity(argc);
    for _ in 0..argc {
        let len = parse_header(buf, &mut pos, b'$')?;
        let end = pos.checked_add(len)?;
        items.push(Frame::BulkString(buf.get(pos..end)?.to_vec()));
        pos = end;
        if buf.get(pos..pos + 2)? != b"\r\n" {
            return None;
        }
        pos += 2;
    }

    Some((Frame::Array(items), pos))
}

/// Read a `<marker><number>\r\n` header, advancing `pos` past it.
fn parse_header(buf: &[u8], pos: &mut usize, marker: u8) -> Option<usize> {
    if *buf.get(*pos)? != marker {
        return None;
    }
    let start = *pos + 1;
    let end = start + buf.get(start..)?.windows(2).position(|w| w == b"\r\n")?;
    let number = std::str::from_utf8(buf.get(start..end)?).ok()?.parse().ok()?;
    *pos = end + 2;
    Some(number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_one_command() {
        let mut dec = Decoder::new();
        dec.extend(b"*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n");
        assert_eq!(
            dec.parse(),
            Some(Frame::Array(vec![Frame::bulk("ECHO"), Frame::bulk("world")]))
        );
        assert_eq!(dec.parse(), None);
    }

    #[test]
    fn waits_for_a_split_command() {
        let mut dec = Decoder::new();
        dec.extend(b"*1\r\n$4\r\nPI");
        assert_eq!(dec.parse(), None);
        dec.extend(b"NG\r\n");
        assert_eq!(
            dec.parse(),
            Some(Frame::Array(vec![Frame::BulkString(b"PING".to_vec())]))
        );
    }

    #[test]
    fn pulls_pipelined_commands_one_at_a_time() {
        let mut dec = Decoder::new();
        dec.extend(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
        let ping = Some(Frame::Array(vec![Frame::bulk("PING")]));
        assert_eq!(dec.parse(), ping.clone());
        assert_eq!(dec.parse(), ping);
        assert_eq!(dec.parse(), None);
    }

    #[test]
    fn encodes_bulk_string_and_error() {
        let mut out = BytesMut::new();
        Frame::bulk("world").encode(&mut out);
        assert_eq!(&out[..], b"$5\r\nworld\r\n");

        out.clear();
        Frame::Err("boom".into()).encode(&mut out);
        assert_eq!(&out[..], b"-ERR boom\r\n");
    }

    #[test]
    fn encodes_null_bulk_string() {
        let mut out = BytesMut::new();
        Frame::Null.encode(&mut out);
        assert_eq!(&out[..], b"$-1\r\n");
    }

    #[test]
    fn constructors_accept_str_and_bytes() {
        assert_eq!(Frame::bulk("hi"), Frame::BulkString(b"hi".to_vec()));
        assert_eq!(
            Frame::bulk(b"hi".as_slice()),
            Frame::BulkString(b"hi".to_vec())
        );
        assert_eq!(Frame::simple("OK"), Frame::SimpleString(b"OK".to_vec()));
    }
}
