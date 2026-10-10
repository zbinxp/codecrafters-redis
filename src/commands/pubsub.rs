use std::collections::HashMap;

use crate::commands::bulk_at;
use crate::db::{Store};
use crate::error::RedisError;
use crate::resp::Frame;

/// PUBLISH channel message just returns the number of clients
/// subscribed to that channel
pub fn cmd_publish(args: &[Frame], store: &mut Store) -> Result<Frame, RedisError> {
    let channel = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("publish"))?;
    let msg = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("publish"))?;

    let n = store
        .channel_subscribers
        .get(channel)
        .unwrap_or(&HashMap::new())
        .iter()
        .filter_map(|(_conn_id, sender)| {
            sender
                .send(Frame::array(vec![b"message", channel, msg]))
                .ok()
        })
        .count();
    Ok(Frame::Int(n as i64))
}