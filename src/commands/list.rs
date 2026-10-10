use std::collections::VecDeque;
use std::time::{Instant};
use std::{sync::{Arc, Mutex}, time::{Duration}};
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at, resolve_range};
use crate::db::{Store, Value, Entry};
use crate::error::RedisError;
use crate::resp::Frame;
use crate::util::{self, parse};

/// Spec: RPUSH <key> <element> appends to a list (creating it if absent), and returns the list length as a RESP
/// integer
pub fn cmd_rpush(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    push_impl(args, store, "rpush", VecDeque::push_back)
}

/// Spec: LPUSH key elem [elem...] prepends elements to the list, returning the new length.
pub fn cmd_lpush(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    push_impl(args, store, "lpush", VecDeque::push_front)
}

fn push_impl(
    args: &[Frame],
    store: &mut Store,
    fn_name: &str,
    push: fn(&mut VecDeque<Vec<u8>>, Vec<u8>),
) -> CmdResult {
    if args.len() < 3 {
        return Err(RedisError::wrong_args(fn_name))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args(fn_name))?;

    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::List(VecDeque::new())));

    if entry.expires_at.is_some_and(|t| t <= Instant::now()) {
        *entry = Entry::new(Value::List(VecDeque::new()));
    }

    let Value::List(list) = &mut entry.value else {
        return Err(RedisError::wrong_type());
    };

    for elem in &args[2..] {
        let val = elem.as_bulk().ok_or_else(||RedisError::wrong_args(fn_name))?;
        push(list, val.to_vec());

        // still hold the lock, notify the waiters
        if let Some(q) = store.waiters.get_mut(key) {
            while !q.is_empty() {
                let tx = q.pop_front().unwrap();
                if tx.send(()).is_ok() {
                    break;
                }
            }
        }
    }
    Ok(vec![Frame::Int(list.len() as i64)])
}

/// Spec: LRANGE key start stop returns a RESP array of the list elements from start to stop,
/// stop inclusive
pub fn cmd_lrange(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() < 4 {
        return Err(RedisError::wrong_args("lrange"))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("lrange"))?;
    let start = bulk_at(args, 2).ok_or_else(||RedisError::wrong_args("lrange"))?;
    let stop = bulk_at(args, 3).ok_or_else(||RedisError::wrong_args("lrange"))?;
    let Some(start) = util::parse(start) else {
        return Err(RedisError::not_integer());
    };

    let Some(entry) = store.data.get(key) else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    let Some(stop) = util::parse(stop) else {
        return Err(RedisError::not_integer());
    };
    let Value::List(list) = &entry.value else {
        return Err(RedisError::wrong_type());
    };

    let Some((start, stop)) = resolve_range(list.len(), start, stop) else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    let bulk_str_list: Vec<Frame> = list
        .range(start as usize..=stop as usize)
        .map(|val| Frame::BulkString(val.to_vec()))
        .collect();
    Ok(vec![Frame::Array(bulk_str_list)])
}

/// Spec: LLEN key returns the list length as a RESP integer. Missing list → :0\r\n.
pub fn cmd_llen(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 2 {
        return Err(RedisError::wrong_args("llen"))
    }
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("llen"))?;
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Int(0)])
    };

    match &entry.value {
        Value::List(list) => Ok(vec![Frame::Int(list.len() as i64)]),
        _ => Err(RedisError::wrong_type()),
    }
}

/// Spec: LPOP key removes and returns the first element as a bulk string.
/// Missing/empty list → $-1\r\n.
/// Spec: LPOP key 2 removes and returns the first N elements as a RESP array.
/// If N > length, remove all and return them.
pub fn cmd_lpop(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(RedisError::wrong_args("lpop"))
    }

    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("lpop"))?;

    let count = match bulk_at(args, 2) {
        Some(val) => match util::parse::<i64>(val) {
            Some(index) => {
                if index < 0 {
                    return Err(RedisError::value_out_of_range());
                }
                Some(index)
            }
            None => return Err(RedisError::not_integer()),
        },
        None => None,
    };
    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![count.map_or(Frame::Null, |_| Frame::NullArray)])
    };

    match &mut entry.value {
        Value::List(list) => match count {
            None => match list.pop_front() {
                Some(v) => Ok(vec![Frame::bulk(v)]),
                None => Ok(vec![Frame::Null]),
            },
            Some(n) => {
                let n = n.min(list.len() as i64);
                let removed: Vec<Frame> = (0..n)
                    .filter_map(|_| list.pop_front())
                    .map(Frame::bulk)
                    .collect();
                Ok(vec![Frame::Array(removed)])
            }
        },
        _ => Err(RedisError::wrong_type()),
    }
}


/// BLPOP key timeout → on success, a 2-element array [key, element]:
/// xj7: Spec: BLPOP key <timeout> where the timeout can be fractional seconds
pub async fn blpop(args: &[Frame], store: Arc<Mutex<Store>>) -> Result<Frame, RedisError> {
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("blpop"))?;
    let timeout:f64 = match bulk_at(args, 2) {
        Some(val) => match parse(val) {
            Some(timeout) => timeout,
            None => return Err(RedisError::timeout_not_float()),
        },
        None => 0.0,
    };

    if !timeout.is_finite() || timeout < 0.0 {
        return Err(RedisError::value_out_of_range())
    }

    let timeout = Duration::from_secs_f64(timeout);
    loop {
        // when notified, we need a loop to check if value is arrived
        let rx = {
            let mut db = store.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = db.data.get_mut(key) {
                let Value::List(list) = &mut entry.value else {
                    return Err(RedisError::wrong_type())
                };
                if let Some(v) = list.pop_front() {
                    return Ok(Frame::Array(vec![Frame::bulk(key), Frame::bulk(v)]))
                }
            }

            // if no value got, wait for notify
            let (tx, rx) = oneshot::channel();
            db.waiters.entry(key.to_vec()).or_default().push_back(tx);
            rx
        };
        tokio::select! {
            _ = rx => continue,
            _ = sleep(timeout), if !timeout.is_zero() => return Ok(Frame::NullArray)
        }
    }
}
