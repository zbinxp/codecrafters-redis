use crate::client::Conn;
use crate::commands::{
    CmdResult, cmd_discard, cmd_echo, cmd_exec, cmd_get, cmd_incr, cmd_info, cmd_llen, cmd_lpop,
    cmd_lpush, cmd_lrange, cmd_multi, cmd_ping, cmd_publish, cmd_rpush, cmd_set, cmd_subscribe,
    cmd_type, cmd_unsubscribe, cmd_unwatch, cmd_watch, cmd_xadd, cmd_xrange, cmd_xread, cmd_zadd,
    cmd_zcard, cmd_zrange, cmd_zrank, cmd_zrem, cmd_zscore,
};
use crate::db::Store;
use crate::error::RedisError;
use crate::resp::Frame;

/// A command handler receives the full argument list (index 0 is the command
/// name), the connection state, and the store, and produces zero or more
/// replies.
type Handler = fn(&[Frame], &mut Conn, &mut Store) -> CmdResult;

/// Map a command name to its handler. Adding a command is a one-line change.
pub fn lookup(name: &[u8]) -> Option<Handler> {
    match name {
        b"PING" | b"COMMAND" => Some(cmd_ping),
        b"TYPE" => Some(cmd_type),
        b"ECHO" => Some(cmd_echo),
        b"GET" => Some(cmd_get),
        b"SET" => Some(cmd_set),
        b"INCR" => Some(cmd_incr),
        b"RPUSH" => Some(cmd_rpush),
        b"LPUSH" => Some(cmd_lpush),
        b"LRANGE" => Some(cmd_lrange),
        b"LLEN" => Some(cmd_llen),
        b"LPOP" => Some(cmd_lpop),
        b"PUBLISH" => Some(cmd_publish),
        b"SUBSCRIBE" => Some(cmd_subscribe),
        b"UNSUBSCRIBE" => Some(cmd_unsubscribe),
        b"ZADD" => Some(cmd_zadd),
        b"ZRANK" => Some(cmd_zrank),
        b"ZRANGE" => Some(cmd_zrange),
        b"ZCARD" => Some(cmd_zcard),
        b"ZSCORE" => Some(cmd_zscore),
        b"ZREM" => Some(cmd_zrem),
        b"XADD" => Some(cmd_xadd),
        b"XRANGE" => Some(cmd_xrange),
        b"XREAD" => Some(cmd_xread),
        b"MULTI" => Some(cmd_multi),
        b"EXEC" => Some(cmd_exec),
        b"DISCARD" => Some(cmd_discard),
        b"WATCH" => Some(cmd_watch),
        b"UNWATCH" => Some(cmd_unwatch),
        b"INFO" => Some(cmd_info),
        _ => None,
    }
}

/// Commands permitted while the connection is in subscribe mode.
fn allowed_in_subscribe_mode(cmd: &[u8]) -> bool {
    const ALLOWED: &[&[u8]] = &[
        b"SUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"PING",
        b"QUIT",
        b"RESET",
    ];
    ALLOWED.iter().any(|a| cmd.eq_ignore_ascii_case(a))
}

/// Turn a client request into zero or more replies.
///
/// Returns an empty vec when the frame carries no command at all (e.g. `*0\r\n`),
/// which must be silently ignored while the connection stays open -- empty means
/// "no reply", not "unknown command".
pub fn execute(req: &Frame, conn: &mut Conn, store: &mut Store) -> CmdResult {
    let Frame::Array(args) = req else {
        return Err(RedisError::not_array_args());
    };
    let Some(name) = args.first().and_then(Frame::as_bulk) else {
        return Ok(Vec::new()); // *0 / first arg not a bulk string -> silent
    };

    // Subscribe mode restricts which commands may run and turns PING into a
    // two-element ["pong", msg] array.
    if !conn.subscriptions.is_empty() {
        if !allowed_in_subscribe_mode(name) {
            let name = String::from_utf8_lossy(name).to_lowercase();
            return Err(RedisError::not_allowed_in_subscribe_mode(&name));
        }
        if name.eq_ignore_ascii_case(b"PING") {
            let msg = args.get(1).and_then(Frame::as_bulk).unwrap_or(b"");
            return Ok(vec![Frame::Array(vec![
                Frame::bulk("pong"),
                Frame::bulk(msg),
            ])]);
        }
    }

    // multi mode: all command allowed for now
    if let Some(queue) = conn.multi.as_mut() {
        if !is_transaction_control(name) {
            queue.push(req.clone());
            return Ok(vec![Frame::simple("QUEUED")]);
        }
    }

    match lookup(name) {
        Some(handler) => handler(args, conn, store),
        None => Err(RedisError::unknown_command(&String::from_utf8_lossy(name))),
    }
}

fn is_transaction_control(name: &[u8]) -> bool {
    [
        b"MULTI".as_slice(),
        b"EXEC",
        b"DISCARD",
        b"WATCH",
        b"UNWATCH",
    ]
    .iter()
    .any(|c| name.eq_ignore_ascii_case(c))
}
