use std::collections::HashMap;

use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at};
use crate::db::Store;
use crate::error::RedisError;
use crate::resp::Frame;

/// PUBLISH channel message just returns the number of clients
/// subscribed to that channel
pub fn cmd_publish(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let channel = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("publish"))?;
    let msg = bulk_at(args, 2).ok_or_else(|| RedisError::wrong_args("publish"))?;
    let n = store
        .channel_subscribers
        .get(channel)
        .unwrap_or(&HashMap::new())
        .iter()
        .filter_map(|(_conn_id, sender)| {
            sender.send(Frame::array(vec![b"message", channel, msg])).ok()
        })
        .count();
    Ok(vec![Frame::Int(n as i64)])
}

/// SUBSCRIBE channel [channel ...] registers this connection for each channel
/// and confirms with one `["subscribe", channel, count]` frame per channel.
pub fn cmd_subscribe(args: &[Frame], conn: &mut Conn, store: &mut Store) -> CmdResult {
    let channels = &args[1..];
    if channels.is_empty() {
        return Err(RedisError::wrong_args("subscribe"));
    }
    Ok(channels
        .iter()
        .filter_map(|c| c.as_bulk())
        .map(|ch| {
            // register in the store only the first time we subscribe
            if conn.subscriptions.insert(ch.to_vec()) {
                store
                    .channel_subscribers
                    .entry(ch.to_vec())
                    .or_default()
                    .insert(conn.id, conn.out_tx.clone());
            }
            Frame::Array(vec![
                Frame::bulk("subscribe"),
                Frame::bulk(ch),
                Frame::Int(conn.subscriptions.len() as i64),
            ])
        })
        .collect())
}

/// UNSUBSCRIBE [channel ...] removes this connection from each channel (or all
/// of them when no channel is given), confirming one frame per channel.
pub fn cmd_unsubscribe(args: &[Frame], conn: &mut Conn, store: &mut Store) -> CmdResult {
    let channels = &args[1..];
    if channels.is_empty() {
        return Err(RedisError::wrong_args("unsubscribe"));
    }

    Ok(channels
        .iter()
        .filter_map(|c| c.as_bulk())
        .map(|ch| {
            if conn.subscriptions.remove(ch) {
                if let Some(subs) = store.channel_subscribers.get_mut(ch) {
                    subs.remove(&conn.id);
                    if subs.is_empty() {
                        store.channel_subscribers.remove(ch);
                    }
                }
            }

            Frame::Array(vec![
                Frame::bulk(b"unsubscribe"),
                Frame::bulk(ch),
                Frame::Int(conn.subscriptions.len() as i64),
            ])
        })
        .collect())
}
