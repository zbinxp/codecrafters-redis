use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use crate::client::Conn;
use crate::commands::{CmdResult, bulk_at};
use crate::error::RedisError;
use crate::resp::Frame;
use crate::db::{Entry, IdSpec, Store, StreamId, Value};
use crate::util::now_ms;
/// Spec: XADD key <id> field value [field value …] creates the stream if absent, appends an
/// entry, and returns the entry ID as a bulk string.
pub fn cmd_xadd(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() < 5 || (args.len() - 3) % 2 != 0 {
        return Err(RedisError::wrong_args("xadd"))
    }
    let (Some(key), Some(id_raw)) = (bulk_at(args, 1), bulk_at(args, 2)) else {
        return Err(RedisError::wrong_args("xadd"))
    };

    let mut fields = Vec::new();
    for pair in args[3..].chunks(2) {
        let (Some(f), Some(v)) = (pair[0].as_bulk(), pair[1].as_bulk()) else {
            return Err(RedisError::wrong_args("xadd"))
        };
        fields.push((f.to_vec(), v.to_vec()));
    }

    // parse auto-generated id
    let Some(spec) = IdSpec::parse_id_spec(id_raw) else {
        return Err(RedisError::invalid_stream_id())
    };

    // peek value to check type
    let last = match store.live_entry(key) {
        Some(entry) => match &entry.value {
            Value::Stream(s) => s.keys().next_back().copied(),
            _ => return Err(RedisError::wrong_type()),
        },
        None => None,
    };

    let id = match spec {
        IdSpec::Explicit(id) => id,
        IdSpec::AutoSeq(ms) => {
            let seq = match last {
                Some(l) if l.0 == ms => l.1 + 1,
                _ => {
                    if ms == 0 {
                        1
                    } else {
                        0
                    }
                }
            };
            StreamId(ms, seq)
        }
        IdSpec::FullyAutoSeq => {
            let now = now_ms();
            match last {
                Some(l) if now <= l.0 => StreamId(l.0, l.1 + 1),
                _ => StreamId(now, 0),
            }
        }
    };

    // 0-0 is always invalid
    if id == StreamId(0, 0) {
        return Err(RedisError::stream_id_less_than_zero())
    }

    if let Some(l) = last {
        if id <= l {
            return Err(RedisError::stream_id_less_than_target())
        }
    }
    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::Stream(BTreeMap::new())));
    let Value::Stream(stream) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };
    stream.insert(id, fields);
    store.touch(key);
    
    // notify xread that new value added
    if let Some(q) = store.waiters.get_mut(key) {
        while let Some(tx) = q.pop_front() {
            let _ = tx.send(());
        }
    }
    Ok(vec![Frame::bulk(id.render())])
}

/// Spec: XRANGE key start end returns entries with start ≤ id ≤ end (inclusive), as an array
/// of [id, [f1, v1, f2, v2, …]] 
pub fn cmd_xrange(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 4 {
        return Err(RedisError::wrong_args("xrange"))
    }
    let (Some(key), Some(start_raw), Some(end_raw)) = 
        (bulk_at(args, 1), bulk_at(args, 2), bulk_at(args, 3)) else {
        return Err(RedisError::wrong_args("xrange"))    
    };

    let (Some(start), Some(end)) = (StreamId::parse_range_id(start_raw, false),
        StreamId::parse_range_id(end_raw, true)) else {
        return Err(RedisError::invalid_stream_id())
    };

    let Some(entry) = store.live_entry(key) else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    let Value::Stream(stream) = &entry.value else {
        return Err(RedisError::wrong_type())
    };
    let entries = stream
        .range(start..=end)
        .map(|(id,fields)| {
            let field_array: Vec<Frame> = fields.iter()
                .flat_map(|(k,v)| [Frame::bulk(k), Frame::bulk(v)])
                .collect();
            Frame::Array(vec![Frame::bulk(id.render()), Frame::Array(field_array)])
        })
        .collect();
    Ok(vec![Frame::Array(entries)])
}

/// Spec: XREAD STREAMS <key> <id> — exclusive (entries with id strictly greater than the given
/// one)
/// 
/// XREAD STREAMS some_key 1526985054069-0 →
/// [
///   ["some_key", [ ["1526985054079-0", [temperature, 37, humidity, 94]] ]]
/// ]
/// 
/// Spec: with BLOCK <ms>:
/// - entries already exist (> id) → return immediately
/// - else block; a new XADD before the timeout → return it
/// - timeout with no data → *-1
pub fn cmd_xread(args:&[Frame], _conn: &mut Conn, store:&mut Store) -> CmdResult {
    let Some(pos) = args.iter().position(|a| a.as_bulk() == Some(b"STREAMS")) else {
        return Err(RedisError::xread_syntax())
    };
    let rest = &args[pos+1..];
    if rest.len() < 2 || rest.len() % 2 != 0 {
        return Err(RedisError::stream_unbalanced_arguments())
    }  
    let n = rest.len() / 2;
    let keys = &rest[..n];
    let ids_raw = &rest[n..];
    let ids = resolve_ids(store, keys, ids_raw)?;
    let out = collect_xread(store, keys, &ids)?;
    Ok(vec![
        if out.is_empty() {
            Frame::NullArray
        }else {
            Frame::Array(out)
        }
    ])
}

pub fn collect_xread(store: &mut Store, keys: &[Frame], ids:&[StreamId]) -> Result<Vec<Frame>, RedisError> {
    let mut out = Vec::new();
    for (k,id) in keys.iter().zip(ids) {
        let key = k.as_bulk().ok_or_else(||RedisError::wrong_type())?;
        let Some(entry) = store.live_entry(key) else {
            continue;
        };
        let Value::Stream(stream) = &entry.value else {
            return Err(RedisError::wrong_type())
        };
        let entries: Vec<Frame> = stream
            .range((Excluded(id),Unbounded))
            .map(|(id, fields)| {
                let flat:Vec<Frame> = fields.iter()
                    .flat_map(|(f,v)| [Frame::bulk(f), Frame::bulk(v)])
                    .collect();
                Frame::Array(vec![Frame::bulk(id.render()), Frame::Array(flat)])
            })
            .collect();

        if !entries.is_empty() {
            out.push(Frame::Array(vec![
                Frame::bulk(key),
                Frame::Array(entries)
            ]));
        }
    }
    Ok(out)
}

pub fn resolve_ids(store: &mut Store, keys: &[Frame], ids_raw: &[Frame]) -> Result<Vec<StreamId>, RedisError> {
    let mut out = Vec::new();
    for (k,i) in keys.iter().zip(ids_raw) {
        let key = k.as_bulk().ok_or_else(|| RedisError::wrong_type())?;
        let id_raw = i.as_bulk().ok_or_else(||RedisError::wrong_type())?;
        let id = if id_raw == b"$" {
            // get last id
            match store.live_entry(key) {
                Some(entry) => match &entry.value {
                    Value::Stream(s) => s.keys().next_back().copied().unwrap_or(StreamId(0,0)),
                    _ => return Err(RedisError::wrong_type()),
                },
                None => StreamId(0, 0)
            }
        } else {
            StreamId::parse_range_id(id_raw, false)
            .ok_or_else(||RedisError::invalid_stream_id())?
        };
        out.push(id);
    }
    Ok(out)
}
