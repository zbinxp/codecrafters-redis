use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at};
use crate::db::{Store, Value};
use crate::error::RedisError;
use crate::resp::Frame;

pub fn cmd_ping(_args: &[Frame], _conn: &mut Conn, _store: &mut Store) -> CmdResult {
    Ok(vec![Frame::simple("PONG")])
}

pub fn cmd_echo(args: &[Frame], _conn: &mut Conn, _store: &mut Store) -> CmdResult {
    // echo some_value
    let arg = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("echo"))?;
    Ok(vec![Frame::bulk(arg)])
}

pub fn cmd_type(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("type"));
    }
    let key = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("type"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::simple("none")]);
    };
    let type_str = match &entry.value {
        Value::String(_) => "string",
        Value::List(_) => "list",
        Value::ZSet(_) => "zset",
        Value::Stream(_) => "stream",
    };
    Ok(vec![Frame::simple(type_str)])
}
