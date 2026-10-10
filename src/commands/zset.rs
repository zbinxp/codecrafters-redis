use std::collections::BTreeSet;

use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at, resolve_range};
use crate::db::{Entry, Score, Store, Value};
use crate::error::RedisError;
use crate::resp::Frame;
use crate::util;

/// Spec: ZADD key score member creates the set if absent and adds member with score, returning
/// the number of new members (:1 here). Scores are 64-bit floats; sets are ordered by
/// ascending score (ties broken by member).
pub fn cmd_zadd(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 4 {
        return Err(RedisError::wrong_args("zadd"))
    }

    let (Some(key), Some(score), Some(member)) =
        (bulk_at(args, 1),bulk_at(args, 2),bulk_at(args, 3))
    else {
        return Err(RedisError::wrong_args("zadd"))
    };
    let Some(val) = util::parse::<f64>(score) else {
        return Err(RedisError::not_valid_float())
    };
    if val.is_nan() {
        return Err(RedisError::not_valid_float())
    }
    store.live_entry(key);

    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::ZSet(BTreeSet::new())));

    let Value::ZSet(tree) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };
    // check existance
    let existed = tree.extract_if(.., |(_, m)| m == member).next().is_some();
    tree.insert((Score(val), member.to_vec()));
    // todo: use a HM to store (member, score)
    Ok(vec![Frame::Int(!existed as i64)])
}

///  Spec: ZRANK key member → the 0-based rank of member when the set is ordered by ascending
/// score, ties broken lexicographically by member. Missing member or key → null bulk string
/// $-1\r\n.
pub fn cmd_zrank(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 3 {
        return Err(RedisError::wrong_args("zrank"))
    }
    let (Some(key), Some(member)) = (bulk_at(args, 1), bulk_at(args, 2)) else {
        return Err(RedisError::wrong_args("zrank"))
    };
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Null])
    };
    let Value::ZSet(tree) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    Ok(vec![
        match tree.iter().position(|(_, m)| m == member) {
            Some(rank) => Frame::Int(rank as i64),
            None => Frame::Null,
        }
    ])
}

///  Spec: ZRANGE key start stop → members by rank, stop inclusive.
pub fn cmd_zrange(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 4 {
        return Err(RedisError::wrong_args("zrange"))
    }
    let (Some(key), Some(start_raw), Some(stop_raw)) =
        (bulk_at(args, 1), bulk_at(args, 2), bulk_at(args, 3))
    else {
        return Err(RedisError::wrong_args("zrange"))
    };
    let (Some(start), Some(stop)) = (util::parse::<i64>(start_raw), util::parse::<i64>(stop_raw)) else {
        return Err(RedisError::not_integer())
    };
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    let Value::ZSet(tree) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    let Some((start, stop)) = resolve_range(tree.len(), start, stop) else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    Ok(vec![Frame::Array(
        tree.iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|(_, m)| Frame::bulk(m))
            .collect(),
    )])
}

/// Spec: ZCARD key → number of members (:N), :0 for a missing set. It's LLEN for sorted sets
pub fn cmd_zcard(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("zcard"))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("zcard"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    let Value::ZSet(tree) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    Ok(vec![Frame::Int(tree.len() as i64)])
}

/// Spec: ZSCORE key member → the member's score as a RESP bulk string; missing member or key →
/// $-1\r\n.
pub fn cmd_zscore(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 3 {
        return Err(RedisError::wrong_args("zscore"))
    }
    let (Some(key), Some(member)) = (bulk_at(args, 1), bulk_at(args, 2)) else {
        return Err(RedisError::wrong_args("zscore"))
    };
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Null])
    };
    let Value::ZSet(tree) = &entry.value else {
        return Err(RedisError::wrong_type())
    };

    Ok(vec![match tree.iter().find(|(_, m)| m == member) {
        Some((score, _)) => Frame::bulk(score.0.to_string()),
        None => Frame::Null,
    }])
}

/// Spec: ZREM key member removes member and returns 1 if it existed, 0 otherwise (including a
/// missing key)
pub fn cmd_zrem(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 3 {
        return Err(RedisError::wrong_args("zrem"))
    }
    let (Some(key), Some(member)) = (bulk_at(args, 1), bulk_at(args, 2)) else {
        return Err(RedisError::wrong_args("zrem"))
    };
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };
    let Value::ZSet(tree) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };
    let existed = tree.extract_if(.., |(_, m)| m == member).next().is_some();
    Ok(vec![Frame::Int(existed as i64)])
}
