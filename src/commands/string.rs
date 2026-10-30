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

fn byte_and_mask(offset:usize) -> (usize, u8) {
    let byte_index = offset / 8;
    let mask = 1 << (7 - offset % 8);
    (byte_index, mask)
}
