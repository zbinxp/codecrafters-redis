use crate::commands::bulk_at;
use crate::db::{Store, Value};
use crate::error::RedisError;
use crate::resp::Frame;

pub fn cmd_ping(_args: &[Frame], _store: &mut Store) -> Result<Frame,RedisError> {
    Ok(Frame::simple("PONG"))
}

pub fn cmd_echo(args: &[Frame], _store: &mut Store) -> Result<Frame,RedisError> {
    // echo some_value
    match args.get(1) {
        Some(Frame::BulkString(arg)) => Ok(Frame::bulk(arg)),
        _ => Err(RedisError::wrong_args("echo")),
    }
}

pub fn cmd_type(args: &[Frame], store: &mut Store) -> Result<Frame,RedisError> {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("type"))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("type"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(Frame::simple("none"))
    };
    let type_str = match &entry.value {
        Value::String(_) => "string",
        Value::List(_) => "list",
        Value::ZSet(_) => "zset",
        Value::Stream(_) => "stream",
    };
    Ok(Frame::simple(type_str))
}
