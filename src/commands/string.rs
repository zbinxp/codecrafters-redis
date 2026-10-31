use std::time::{Duration, Instant};

use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at};
use crate::db::{Entry, Store, Value};
use crate::error::RedisError;
use crate::resp::Frame;
use crate::util;

pub fn cmd_get(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("get"));
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("get"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Null]);
    };
    match &entry.value {
        Value::String(val) => Ok(vec![Frame::bulk(val)]),
        _ => Err(RedisError::wrong_type()),
    }
}

pub fn cmd_set(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let (key, value, expires_at) = parse_set(args)?;
    let value = Value::String(value.to_vec());
    store.data.insert(key.to_vec(), Entry { value, expires_at });
    store.touch(key);
    Ok(vec![Frame::SimpleString(b"OK".to_vec())])
}

/// INCR key
pub fn cmd_incr(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() > 2 {
        return Err(RedisError::wrong_args("incr"))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("incr"))?;
    let Some(entry) = store.live_entry(key) else {
        store.data.insert(key.to_vec(), Entry::new(Value::String(b"1".to_vec())));
        return Ok(vec![Frame::Int(1)])
    };
    let Value::String(v) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };
    let cur:i64 = util::parse(v).ok_or_else(RedisError::not_integer)?;
    let res = cur.checked_add(1).ok_or_else(RedisError::increment_overflow)?;
    // update entry.value
    *v = res.to_string().into_bytes();
    store.touch(key);
    Ok(vec![Frame::Int(res)])
}

/// PEXPIREAT key unix_ms
pub fn cmd_pexpireat(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 3 {
        return Err(RedisError::wrong_args("pexpireat"))
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("pexpireat"))?;
    let unix_ms = bulk_at(args, 2).and_then(|bytes| util::parse::<u64>(bytes))
        .ok_or_else(|| RedisError::invalid_expire())?;

    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    // none means outdated
    entry.expires_at = Some(util::unix_ms_to_instant(unix_ms).unwrap_or_else(Instant::now));
    store.touch(key);
    Ok(vec![Frame::Int(1)])
}

// SET <key> <value> [PX <milliseconds>]
fn parse_set(args: &[Frame]) -> Result<(&[u8], &[u8], Option<Instant>), RedisError> {
    if args.len() > 5 {
        return Err(RedisError::syntax_error());
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("set"))?;
    let value = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("set"))?;
    let expires_at = match (args.get(3), args.get(4)) {
        (None, _) => None,
        (Some(Frame::BulkString(opt)), Some(Frame::BulkString(ms))) =>
        {
            let ms = util::parse(ms).ok_or_else(RedisError::invalid_expire)?;
            if opt.eq_ignore_ascii_case(b"PX") {
                Some(Instant::now().checked_add(Duration::from_millis(ms)).unwrap_or_else(Instant::now))
            } else if opt.eq_ignore_ascii_case(b"PXAT") {
                Some(util::unix_ms_to_instant(ms).unwrap_or_else(Instant::now))
            } else if opt.eq_ignore_ascii_case(b"EXAT") {
                let ms = ms.checked_mul(1000).ok_or_else(||RedisError::syntax_error())?;
                Some(util::unix_ms_to_instant(ms).unwrap_or_else(Instant::now))
            } else {
                return Err(RedisError::syntax_error())
            }
        },
        _ => return Err(RedisError::syntax_error()),
    };

    Ok((key, value, expires_at))
}

pub fn cmd_setbit(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 4 {
        return Err(RedisError::wrong_args("setbit"))
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("setbit"))?;
    let offset_raw = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("setbit"))?;
    let offset:usize = util::parse(offset_raw).ok_or_else(|| RedisError::field_not_integer("bit offset"))?;
    if offset > 0xFFFF_FFFF {
        return Err(RedisError::field_not_integer("bit offset"))
    }

    let value_raw = bulk_at(args, 3).ok_or_else(|| RedisError::wrong_args("setbit"))?;
    let value:u8 = util::parse(value_raw).ok_or_else(|| RedisError::field_not_integer("bit"))?;
    if value > 1 {
        return Err(RedisError::field_not_integer("bit"))
    }
    let value_bool = value == 1;
    let (byte_index, mask) = byte_and_mask(offset);
    let Some(entry) = store.live_entry(key) else {
        // create new value
        let mut bytes = vec![0; byte_index+1];
        // byte_index is guarded
        let x = bytes.get_mut(byte_index).unwrap();
        util::set_bit(x, mask, value_bool);
        let entry = Entry::new(Value::String(bytes));
        store.data.insert(key.to_vec(), entry);
        store.touch(key);
        return Ok(vec![Frame::Int(0)])
    };

    let Value::String(bytes) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };
    if bytes.len() < byte_index + 1 {
        bytes.resize(byte_index+1, 0);
    }
    // byte_index is guarded
    let x = bytes.get_mut(byte_index).unwrap();
    let old_x = util::get_bit(*x, mask);
    util::set_bit(x, mask, value_bool);
    store.touch(key);

    Ok(vec![Frame::Int(old_x.into())])
}

/// GETBIT key offset
pub fn cmd_getbit(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 3 {
        return Err(RedisError::wrong_args("getbit"))
    }

    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("getbit"))?;
    let offset_raw = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("getbit"))?;
    let offset:usize = util::parse(offset_raw).ok_or_else(|| RedisError::field_not_integer("bit offset"))?;
    if offset > 0xFFFF_FFFF {
        return Err(RedisError::field_not_integer("bit offset"))
    }
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    let Value::String(bytes) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    let (byte_index, mask) = byte_and_mask(offset);
    if byte_index >= bytes.len() {
        return Ok(vec![Frame::Int(0)])
    }
    let x = bytes[byte_index];
    let bit = util::get_bit(x, mask);
    Ok(vec![Frame::Int(bit.into())])
}

pub fn cmd_strlen(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("strlen"));
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("strlen"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    let Value::String(bytes) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    Ok(vec![Frame::Int(bytes.len() as i64)])
}

/// BITCOUNT key [start end]
/// start,end are byte indexes
pub fn cmd_bitcount(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    match args.len() {
       2 | 4 => {}
       1 => return Err(RedisError::wrong_args("bitcount")),
       _ => return Err(RedisError::syntax_error()),
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("bitcount"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    let Value::String(bytes) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    if bytes.len() == 0 {
        return Ok(vec![Frame::Int(0)])
    }
    match (bulk_at(args, 2), bulk_at(args, 3)) {
        (Some(start_raw), Some(end_raw)) => {
            let start = util::parse::<usize>(start_raw).ok_or_else(||RedisError::not_integer())?;
            let end = util::parse::<usize>(end_raw).ok_or_else(||RedisError::not_integer())?;
            if start > end || start >= bytes.len() {
                return Ok(vec![Frame::Int(0)])
            }
            let end = end.min(bytes.len()-1);
            let count = bytes[start..=end].iter().map(|b|b.count_ones()).sum::<u32>() as i64;
            Ok(vec![Frame::Int(count)])
        },
        (None, None) => {
            let count = bytes.iter().map(|b| b.count_ones()).sum::<u32>() as i64;
            Ok(vec![Frame::Int(count)])
        },
        _ => return Err(RedisError::syntax_error())
    }
}

#[derive(Clone, Copy)]
enum BitOp {
    And,
    Or,
}

impl BitOp {
    /// Redis matches the operation name case-insensitively.
    fn parse(name: &[u8]) -> Option<Self> {
        if name.eq_ignore_ascii_case(b"AND") {
            Some(BitOp::And)
        } else if name.eq_ignore_ascii_case(b"OR") {
            Some(BitOp::Or)
        } else {
            None
        }
    }

    fn apply(self, acc: u8, byte: u8) -> u8 {
        match self {
            BitOp::And => acc & byte,
            BitOp::Or => acc | byte,
        }
    }

    /// Identity for the fold: all ones for AND, all zeros for OR.
    fn identity(self) -> u8 {
        match self {
            BitOp::And => 0xFF,
            BitOp::Or => 0x00,
        }
    }
}

/// BITOP <AND|OR> dest key [key ...]
///
/// The destination is as long as the longest source; shorter sources and
/// missing keys read as zero bytes. Replies with the destination length in
/// bytes, or 0 after deleting an empty destination.
pub fn cmd_bitop(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() < 4 {
        return Err(RedisError::wrong_args("bitop"))
    }
    let op = bulk_at(args, 1)
        .and_then(BitOp::parse)
        .ok_or_else(RedisError::syntax_error)?;
    let dest = bulk_at(args, 2).ok_or_else(||RedisError::wrong_args("bitop"))?;

    // Read every source before touching `dest`: it may be one of them, and a
    // wrong-typed source must leave `dest` exactly as it was.
    let mut inputs: Vec<Vec<u8>> = Vec::with_capacity(args.len() - 3);
    for frame in &args[3..] {
        let key = frame.as_bulk().ok_or_else(||RedisError::wrong_args("bitop"))?;
        // expired or missing key => zero bytes
        match store.live_entry_readonly(key) {
            Some(entry) => match &entry.value {
                Value::String(bytes) => inputs.push(bytes.clone()),
                _ => return Err(RedisError::wrong_type())
            },
            None => inputs.push(Vec::new()),
        }
    }

    let maxlen = inputs.iter().map(Vec::len).max().unwrap_or(0);
    let mut out = vec![0u8; maxlen];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = inputs
            .iter()
            .fold(op.identity(), |acc, bytes| op.apply(acc, bytes.get(i).copied().unwrap_or(0)));
    }

    if out.is_empty() {
        // Redis deletes the destination rather than storing an empty string.
        if store.data.remove(dest).is_some() {
            store.touch(dest);
        }
        Ok(vec![Frame::Int(0)])
    } else {
        let len = out.len() as i64;
        store.data.insert(dest.to_vec(), Entry::new(Value::String(out)));
        store.touch(dest);
        Ok(vec![Frame::Int(len)])
    }
}

fn byte_and_mask(offset:usize) -> (usize, u8) {
    let byte_index = offset / 8;
    let mask = 1 << (7 - offset % 8);
    (byte_index, mask)
}
