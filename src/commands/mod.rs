mod connection;
mod list;
mod pubsub;
mod stream;
mod string;
mod zset;

use std::cmp::min;

//re-export
pub use connection::cmd_echo;
pub use connection::cmd_ping;
pub use connection::cmd_type;

pub use string::cmd_get;
pub use string::cmd_set;

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

pub use stream::cmd_xadd;
pub use stream::cmd_xrange;
pub use stream::cmd_xread;

pub use stream::collect_xread;
pub use stream::resolve_ids;

use crate::resp::Frame;

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

pub(crate) fn bulk_at(args:&[Frame], i:usize) -> Option<&[u8]> {
    args.get(i).and_then(Frame::as_bulk)
}