mod connection;
mod list;
mod pubsub;
mod stream;
mod string;
mod zset;
mod transaction;
mod server;

use std::cmp::min;

use crate::error::RedisError;
use crate::resp::Frame;

/// What a command handler returns: zero or more RESP frames.
///
/// A command that produces no reply (e.g. `*0`) returns an empty vec; a normal
/// reply wraps one frame. `SUBSCRIBE` to N channels yields N frames.
pub type CmdResult = Result<Vec<Frame>, RedisError>;

//re-export
pub use connection::cmd_echo;
pub use connection::cmd_ping;
pub use connection::cmd_type;

pub use string::cmd_get;
pub use string::cmd_set;
pub use string::cmd_incr;
pub use string::cmd_pexpireat;
pub use string::cmd_setbit;

pub use list::blpop;
pub use list::cmd_llen;
pub use list::cmd_lpop;
pub use list::cmd_lpush;
pub use list::cmd_lrange;
pub use list::cmd_rpush;

pub use zset::cmd_zadd;
pub use zset::cmd_zcard;
pub use zset::cmd_zrange;
pub use zset::cmd_zrank;
pub use zset::cmd_zrem;
pub use zset::cmd_zscore;

pub use pubsub::cmd_publish;
pub use pubsub::cmd_subscribe;
pub use pubsub::cmd_unsubscribe;

pub use stream::cmd_xadd;
pub use stream::cmd_xrange;
pub use stream::cmd_xread;
pub use stream::collect_xread;
pub use stream::resolve_ids;

pub use transaction::cmd_multi;
pub use transaction::cmd_exec;
pub use transaction::cmd_discard;
pub use transaction::cmd_watch;
pub use transaction::cmd_unwatch;

pub use server::cmd_info;
pub use server::cmd_replconf;
pub use server::cmd_psync;
pub use server::cmd_wait;
pub use server::cmd_config;
pub use server::cmd_keys;

pub(crate) fn bulk_at(args:&[Frame], i:usize) -> Option<&[u8]> {
    args.get(i).and_then(Frame::as_bulk)
}

pub fn resolve_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    if len == 0 {
        return None;
    }
    let (mut start, mut stop) = (start, stop);
    if start < 0 {
        start += len;
    }
    if stop < 0 {
        stop += len;
    }
    if start < 0 {
        start = 0;
    }
    if start >= len || start > stop {
        return None;
    }
    let stop = min(stop, len - 1);
    Some((start as usize, stop as usize))
}
