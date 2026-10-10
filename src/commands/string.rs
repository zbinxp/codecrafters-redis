use std::time::{Duration, Instant};

use crate::commands::bulk_at;
use crate::db::{Store, Value, Entry};
use crate::error::RedisError;
use crate::resp::Frame;
use crate::util;

pub fn cmd_get(args: &[Frame], store: &mut Store) -> Result<Frame, RedisError> {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("get"));
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("get"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(Frame::Null);
    };
    match &entry.value {
        Value::String(val) => Ok(Frame::bulk(val)),
        _ => Err(RedisError::wrong_type()),
    }
}

pub fn cmd_set(args: &[Frame], store: &mut Store) -> Result<Frame, RedisError> {
    match parse_set(args) {
        Ok((key, value, expires_at)) => {
            let value = Value::String(value.to_vec());
            store.data.insert(key.to_vec(), Entry { value, expires_at });
            Ok(Frame::SimpleString(b"OK".to_vec()))
        }
        Err(e) => Err(e),
    }
}

// SET <key> <value> [PX <milliseconds>]
fn parse_set(args: &[Frame]) -> Result<(&[u8], &[u8], Option<Instant>), RedisError> {
    if args.len() > 5 {
        return Err(RedisError::syntax_error());
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("set"))?;
    let value = bulk_at(args, 2).ok_or_else(||RedisError::wrong_args("set"))?;
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