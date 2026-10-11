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

// SET <key> <value> [PX <milliseconds>]
fn parse_set(args: &[Frame]) -> Result<(&[u8], &[u8], Option<Instant>), RedisError> {
    if args.len() > 5 {
        return Err(RedisError::syntax_error());
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("set"))?;
    let value = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("set"))?;
    let expires_at = match (args.get(3), args.get(4)) {
        (None, _) => None,
        (Some(Frame::BulkString(opt)), Some(Frame::BulkString(ms)))
            if opt.eq_ignore_ascii_case(b"PX") =>
        {
            let Some(ms) = util::parse(ms) else {
                return Err(RedisError::invalid_expire());
            };
            Instant::now().checked_add(Duration::from_millis(ms))
        }
        _ => return Err(RedisError::syntax_error()),
    };

    Ok((key, value, expires_at))
}
