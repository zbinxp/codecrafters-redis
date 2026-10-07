use std::{
    cmp::{Ordering, min},
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    io,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::{Duration, Instant},
    vec,
};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf},
    net::TcpListener,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    time::sleep,
};

mod resp;
use resp::{Decoder, Frame};

const WRONGTYPE_KEY_WRONG: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";
const ERR_NOT_VALID_INDEX: &str = "ERR value is not an integer or out of range";
const ERR_VALUE_OUT_OF_RANGE: &str = "ERR value is out of range, must be positive";

// one global counter
static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// A command handler receives the full argument list (index 0 is the command
/// name) and produces a reply.
type Handler = fn(&[Frame], &mut Store) -> Frame;
type Store = Db;
type Outbox = mpsc::UnboundedSender<Frame>;
type ZEntry = (Score, Vec<u8>);

/// the value type in our Store
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: Value) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }
}

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    ZSet(BTreeSet<ZEntry>),
    Stream(BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>),
}

struct Db {
    data: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
    channel_subscribers: HashMap<Vec<u8>, HashMap<u64, Outbox>>,
}

impl Db {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            waiters: HashMap::new(),
            channel_subscribers: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct Score(f64);

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        return self.0.total_cmp(&other.0);
    }
}
impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StreamId(u64, u64);

impl StreamId {
    // fn parse(bytes: &[u8]) -> Option<Self> {
    //     let s = str::from_utf8(bytes).ok()?;
    //     let (ms, seq) = s.split_once('-')?;
    //     Some(StreamId(ms.parse().ok()?, seq.parse().ok()?))
    // }

    fn render(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}

fn cmd_ping(_args: &[Frame], _store: &mut Store) -> Frame {
    Frame::simple("PONG")
}

fn cmd_echo(args: &[Frame], _store: &mut Store) -> Frame {
    // echo some_value
    match args.get(1) {
        Some(Frame::BulkString(arg)) => Frame::bulk(arg),
        Some(_) => Frame::Err("ERR echo argument is not a bulk string".into()),
        None => Frame::Err("ERR wrong number of arguments for 'echo' command".into()),
    }
}

fn cmd_get(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 2 {
        return Frame::Err("ERR wrong number of arguments for 'get' command".into());
    }
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong format of arguments".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Null;
    };
    match &entry.value {
        Value::String(val) => Frame::bulk(val),
        _ => Frame::Err(WRONGTYPE_KEY_WRONG.into()),
    }
}

fn cmd_set(args: &[Frame], store: &mut Store) -> Frame {
    match parse_set(args) {
        Ok((key, value, expires_at)) => {
            let value = Value::String(value.to_vec());
            store.data.insert(key.to_vec(), Entry { value, expires_at });
            Frame::SimpleString(b"OK".to_vec())
        }
        Err(frame) => frame,
    }
}

fn bulk(frame: Option<&Frame>) -> Option<&[u8]> {
    match frame {
        Some(Frame::BulkString(b)) => Some(b),
        _ => None,
    }
}

fn parse_millis(bytes: &[u8]) -> Option<u64> {
    str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_index(bytes: &[u8]) -> Option<i64> {
    str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_f64(bytes: &[u8]) -> Option<f64> {
    str::from_utf8(bytes).ok()?.parse().ok()
}

// SET <key> <value> [PX <milliseconds>]
fn parse_set(args: &[Frame]) -> Result<(&[u8], &[u8], Option<Instant>), Frame> {
    if args.len() > 5 {
        return Err(Frame::Err("ERR syntax error".into()));
    }
    let Some(key) = bulk(args.get(1)) else {
        return Err(Frame::Err("ERR wrong number of arguments for 'set'".into()));
    };
    let Some(value) = bulk(args.get(2)) else {
        return Err(Frame::Err("ERR wrong number of arguments for 'set'".into()));
    };
    let expires_at = match (args.get(3), args.get(4)) {
        (None, _) => None,
        (Some(Frame::BulkString(opt)), Some(Frame::BulkString(ms)))
            if opt.eq_ignore_ascii_case(b"PX") =>
        {
            let Some(ms) = parse_millis(ms) else {
                return Err(Frame::Err("ERR value is not an integer".into()));
            };
            Instant::now().checked_add(Duration::from_millis(ms))
        }
        _ => return Err(Frame::Err("ERR syntax error".into())),
    };

    Ok((key, value, expires_at))
}

/// Spec: RPUSH <key> <element> appends to a list (creating it if absent), and returns the list length as a RESP
/// integer
fn cmd_rpush(args: &[Frame], store: &mut Store) -> Frame {
    push_impl(args, store, "rpush", VecDeque::push_back)
}

/// Spec: LPUSH key elem [elem...] prepends elements to the list, returning the new length.
fn cmd_lpush(args: &[Frame], store: &mut Store) -> Frame {
    push_impl(args, store, "lpush", VecDeque::push_front)
}

fn push_impl(
    args: &[Frame],
    store: &mut Store,
    fn_name: &str,
    push: fn(&mut VecDeque<Vec<u8>>, Vec<u8>),
) -> Frame {
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err(format!("ERR wrong number of arguments for '{}'", fn_name));
    };
    if args.len() < 3 {
        return Frame::Err(format!("ERR wrong number of arguments for '{}'", fn_name));
    }

    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::List(VecDeque::new())));

    if entry.expires_at.is_some_and(|t| t <= Instant::now()) {
        *entry = Entry::new(Value::List(VecDeque::new()));
    }

    let Value::List(list) = &mut entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };

    for elem in &args[2..] {
        let Some(val) = bulk(Some(elem)) else {
            return Frame::Err("ERR wrong format of arguments for 'rpush'".into());
        };
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
    Frame::Int(list.len() as i64)
}

/// Spec: LRANGE key start stop returns a RESP array of the list elements from start to stop,
/// stop inclusive
fn cmd_lrange(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() < 4 {
        return Frame::Err("ERR wrong number of arguments for 'lrange'".into());
    }
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'lrange'".into());
    };
    let Some(start) = bulk(args.get(2)) else {
        return Frame::Err("ERR wrong number of arguments for 'lrange'".into());
    };
    let Some(stop) = bulk(args.get(3)) else {
        return Frame::Err("ERR wrong number of arguments for 'lrange'".into());
    };
    let Some(start) = parse_index(start) else {
        return Frame::Err(ERR_NOT_VALID_INDEX.into());
    };

    let Some(entry) = store.data.get(key) else {
        return Frame::Array(Vec::new());
    };
    let Some(stop) = parse_index(stop) else {
        return Frame::Err(ERR_NOT_VALID_INDEX.into());
    };
    let Value::List(list) = &entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };

    let Some((start, stop)) = resolve_range(list.len(), start, stop) else {
        return Frame::Array(Vec::new());
    };
    let bulk_str_list: Vec<Frame> = list
        .range(start as usize..=stop as usize)
        .map(|val| Frame::BulkString(val.to_vec()))
        .collect();
    Frame::Array(bulk_str_list)
}

/// Spec: LLEN key returns the list length as a RESP integer. Missing list → :0\r\n.
fn cmd_llen(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 2 {
        return Frame::Err("ERR wrong number of arguments for 'llen' command".into());
    }
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'llen' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Int(0);
    };

    match &entry.value {
        Value::List(list) => Frame::Int(list.len() as i64),
        _ => Frame::Err(WRONGTYPE_KEY_WRONG.into()),
    }
}

/// Spec: LPOP key removes and returns the first element as a bulk string.
/// Missing/empty list → $-1\r\n.
/// Spec: LPOP key 2 removes and returns the first N elements as a RESP array.
/// If N > length, remove all and return them.
fn cmd_lpop(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() < 2 || args.len() > 3 {
        return Frame::Err("ERR wrong number of arguments for 'lpop' command".into());
    }

    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'lpop' command".into());
    };

    let count = match bulk(args.get(2)) {
        Some(val) => match parse_index(val) {
            Some(index) => {
                if index < 0 {
                    return Frame::Err(ERR_VALUE_OUT_OF_RANGE.into());
                }
                Some(index)
            }
            None => return Frame::Err(ERR_NOT_VALID_INDEX.into()),
        },
        None => None,
    };
    let Some(entry) = live_entry(store, key) else {
        return count.map_or(Frame::Null, |_| Frame::NullArray);
    };

    match &mut entry.value {
        Value::List(list) => match count {
            None => match list.pop_front() {
                Some(v) => Frame::bulk(v),
                None => Frame::Null,
            },
            Some(n) => {
                let n = n.min(list.len() as i64);
                let removed: Vec<Frame> = (0..n)
                    .filter_map(|_| list.pop_front())
                    .map(Frame::bulk)
                    .collect();
                Frame::Array(removed)
            }
        },
        _ => Frame::Err(WRONGTYPE_KEY_WRONG.into()),
    }
}

/// PUBLISH channel message just returns the number of clients
/// subscribed to that channel
fn cmd_publish(args: &[Frame], store: &mut Store) -> Frame {
    let (Some(channel), Some(_msg)) = (bulk(args.get(1)), bulk(args.get(2))) else {
        return Frame::Err("ERR wrong number of arguments for 'publish' command".into());
    };
    let n = store
        .channel_subscribers
        .get(channel)
        .unwrap_or(&HashMap::new())
        .iter()
        .filter_map(|(_conn_id, sender)| {
            sender
                .send(Frame::array(vec![b"message", channel, _msg]))
                .ok()
        })
        .count();
    Frame::Int(n as i64)
}

/// Spec: ZADD key score member creates the set if absent and adds member with score, returning
/// the number of new members (:1 here). Scores are 64-bit floats; sets are ordered by
/// ascending score (ties broken by member).
fn cmd_zadd(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 4 {
        return Frame::Err("ERR wrong number of arguments for 'zadd' command".into());
    }

    let (Some(key), Some(score), Some(member)) =
        (bulk(args.get(1)), bulk(args.get(2)), bulk(args.get(3)))
    else {
        return Frame::Err("ERR wrong number of arguments for 'zadd' command".into());
    };
    let Some(val) = parse_f64(score) else {
        return Frame::Err("ERR value is not a valid float".into());
    };
    if val.is_nan() {
        return Frame::Err("ERR value is not a valid float".into());
    }
    live_entry(store, key);

    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::ZSet(BTreeSet::new())));

    let Value::ZSet(tree) = &mut entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };
    // check existance
    let existed = tree.extract_if(.., |(_, m)| m == member).next().is_some();
    tree.insert((Score(val), member.to_vec()));
    // todo: use a HM to store (member, score)
    Frame::Int(!existed as i64)
}

///  Spec: ZRANK key member → the 0-based rank of member when the set is ordered by ascending
/// score, ties broken lexicographically by member. Missing member or key → null bulk string
/// $-1\r\n.
fn cmd_zrank(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 3 {
        return Frame::Err("ERR wrong number of arguments for 'zrank' command".into());
    }
    let (Some(key), Some(member)) = (bulk(args.get(1)), bulk(args.get(2))) else {
        return Frame::Err("ERR wrong number of arguments for 'zrank' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Null;
    };
    let Value::ZSet(tree) = &entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };

    match tree.iter().position(|(_, m)| m == member) {
        Some(rank) => Frame::Int(rank as i64),
        None => Frame::Null,
    }
}

///  Spec: ZRANGE key start stop → members by rank, stop inclusive.
fn cmd_zrange(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 4 {
        return Frame::Err("ERR wrong number of arguments for 'zrange' command".into());
    }
    let (Some(key), Some(start_raw), Some(stop_raw)) =
        (bulk(args.get(1)), bulk(args.get(2)), bulk(args.get(3)))
    else {
        return Frame::Err("ERR wrong number of arguments for 'zrange' command".into());
    };
    let (Some(start), Some(stop)) = (parse_index(start_raw), parse_index(stop_raw)) else {
        return Frame::Err(ERR_NOT_VALID_INDEX.into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Array(Vec::new());
    };
    let Value::ZSet(tree) = &entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };
    let Some((start, stop)) = resolve_range(tree.len(), start, stop) else {
        return Frame::Array(Vec::new());
    };
    Frame::Array(
        tree.iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|(_, m)| Frame::bulk(m))
            .collect(),
    )
}

/// Spec: ZCARD key → number of members (:N), :0 for a missing set. It's LLEN for sorted sets
fn cmd_zcard(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 2 {
        return Frame::Err("ERR wrong number of arguments for 'zcard' command".into());
    }
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'zcard' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Int(0);
    };
    let Value::ZSet(tree) = &entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };
    Frame::Int(tree.len() as i64)
}

/// Spec: ZSCORE key member → the member's score as a RESP bulk string; missing member or key →
/// $-1\r\n.
fn cmd_zscore(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 3 {
        return Frame::Err("ERR wrong number of arguments for 'zscore' command".into());
    }
    let (Some(key), Some(member)) = (bulk(args.get(1)), bulk(args.get(2))) else {
        return Frame::Err("ERR wrong number of arguments for 'zscore' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Null;
    };
    let Value::ZSet(tree) = &entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };

    match tree.iter().find(|(_, m)| m == member) {
        Some((score, _)) => Frame::bulk(score.0.to_string()),
        None => Frame::Null,
    }
}

/// Spec: ZREM key member removes member and returns 1 if it existed, 0 otherwise (including a
/// missing key)
fn cmd_zrem(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 3 {
        return Frame::Err("ERR wrong number of arguments for 'zrem' command".into());
    }
    let (Some(key), Some(member)) = (bulk(args.get(1)), bulk(args.get(2))) else {
        return Frame::Err("ERR wrong number of arguments for 'zrem' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::Int(0);
    };
    let Value::ZSet(tree) = &mut entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };
    let existed = tree.extract_if(.., |(_, m)| m == member).next().is_some();
    Frame::Int(existed as i64)
}

fn cmd_type(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() != 2 {
        return Frame::Err("ERR wrong number of arguments for 'type' command".into());
    }
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'type' command".into());
    };
    let Some(entry) = live_entry(store, key) else {
        return Frame::simple("none");
    };
    let type_str = match &entry.value {
        Value::String(_) => "string",
        Value::List(_) => "list",
        Value::ZSet(_) => "zset",
        Value::Stream(_) => "stream",
    };
    Frame::simple(type_str)
}

enum IdSpec {
    Explicit(StreamId),
    AutoSeq(u64),
    FullyAutoSeq,
}

fn parse_id_spec(bytes: &[u8]) -> Option<IdSpec> {
    let s = str::from_utf8(bytes).ok()?;
    if s == "*" {
        return Some(IdSpec::FullyAutoSeq);
    }
    let (ms, seq) = s.split_once('-')?;
    let ms: u64 = ms.parse().ok()?;
    if seq == "*" {
        Some(IdSpec::AutoSeq(ms))
    } else {
        Some(IdSpec::Explicit(StreamId(ms, seq.parse().ok()?)))
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Spec: XADD key <id> field value [field value …] creates the stream if absent, appends an
/// entry, and returns the entry ID as a bulk string.
fn cmd_xadd(args: &[Frame], store: &mut Store) -> Frame {
    if args.len() < 5 || (args.len() - 3) % 2 != 0 {
        return Frame::Err("ERR wrong number of arguments for 'xadd' command".into());
    }
    let (Some(key), Some(id_raw)) = (bulk(args.get(1)), bulk(args.get(2))) else {
        return Frame::Err("ERR wrong number of arguments for 'xadd' command".into());
    };

    let mut fields = Vec::new();
    for pair in args[3..].chunks(2) {
        let (Some(f), Some(v)) = (bulk(Some(&pair[0])), bulk(Some(&pair[1]))) else {
            return Frame::Err("ERR wrong number of arguments for 'xadd' command".into());
        };
        fields.push((f.to_vec(), v.to_vec()));
    }

    // parse auto-generated id
    let Some(spec) = parse_id_spec(id_raw) else {
        return Frame::Err("ERR Invalid stream ID specified as stream command argument".into());
    };

    // peek value to check type
    let last = match live_entry(store, key) {
        Some(entry) => match &entry.value {
            Value::Stream(s) => s.keys().next_back().copied(),
            _ => return Frame::Err(WRONGTYPE_KEY_WRONG.into()),
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
        return Frame::Err("ERR The ID specified in XADD must be greater than 0-0".into());
    }

    if let Some(l) = last {
        if id <= l {
            return Frame::Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into(),
            );
        }
    }
    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::Stream(BTreeMap::new())));
    let Value::Stream(stream) = &mut entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };
    stream.insert(id, fields);
    Frame::bulk(id.render())
}

fn resolve_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
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

/// BLPOP key timeout → on success, a 2-element array [key, element]:
/// xj7: Spec: BLPOP key <timeout> where the timeout can be fractional seconds
async fn blpop(args: &[Frame], store: Arc<Mutex<Store>>) -> Frame {
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err(format!(
            "ERR wrong number of arguments for '{}' command",
            "blpop"
        ));
    };
    let timeout = match bulk(args.get(2)) {
        Some(val) => match parse_f64(val) {
            Some(timeout) => timeout,
            None => return Frame::Err("ERR timeout is not a float or out of range".into()),
        },
        None => 0.0,
    };

    if !timeout.is_finite() || timeout < 0.0 {
        return Frame::Err(ERR_VALUE_OUT_OF_RANGE.into());
    }

    let timeout = Duration::from_secs_f64(timeout);
    loop {
        // when notified, we need a loop to check if value is arrived
        let rx = {
            let mut db = store.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = db.data.get_mut(key) {
                let Value::List(list) = &mut entry.value else {
                    return Frame::Err(WRONGTYPE_KEY_WRONG.into());
                };
                if let Some(v) = list.pop_front() {
                    return Frame::Array(vec![Frame::bulk(key), Frame::bulk(v)]);
                }
            }

            // if no value got, wait for notify
            let (tx, rx) = oneshot::channel();
            db.waiters.entry(key.to_vec()).or_default().push_back(tx);
            rx
        };
        tokio::select! {
            _ = rx => continue,
            _ = sleep(timeout), if !timeout.is_zero() => return Frame::NullArray
        }
    }
}

fn live_entry<'a>(store: &'a mut Store, key: &[u8]) -> Option<&'a mut Entry> {
    if store
        .data
        .get(key)
        .and_then(|e| e.expires_at)
        .is_some_and(|t| t <= Instant::now())
    {
        store.data.remove(key);
    }
    store.data.get_mut(key)
}

/// Map a command name to its handler. Adding a command is a one-line change
fn lookup(name: &[u8]) -> Option<Handler> {
    match name {
        b"PING" | b"COMMAND" => Some(cmd_ping),
        b"ECHO" => Some(cmd_echo),
        b"GET" => Some(cmd_get),
        b"SET" => Some(cmd_set),
        b"RPUSH" => Some(cmd_rpush),
        b"LPUSH" => Some(cmd_lpush),
        b"LRANGE" => Some(cmd_lrange),
        b"LLEN" => Some(cmd_llen),
        b"LPOP" => Some(cmd_lpop),
        b"PUBLISH" => Some(cmd_publish),
        b"ZADD" => Some(cmd_zadd),
        b"ZRANK" => Some(cmd_zrank),
        b"ZRANGE" => Some(cmd_zrange),
        b"ZCARD" => Some(cmd_zcard),
        b"ZSCORE" => Some(cmd_zscore),
        b"ZREM" => Some(cmd_zrem),
        b"TYPE" => Some(cmd_type),
        b"XADD" => Some(cmd_xadd),
        _ => None,
    }
}

/// Turn a client request into a reply.
///
/// Returns `None` when the frame carries no command at all (e.g. `*0\r\n`),
/// which must be silently ignored while the connection stays open -- so `None`
/// means "no reply", not "unknown command".
fn execute(req: &Frame, store: &mut Store) -> Option<Frame> {
    let Frame::Array(args) = req else {
        return Some(Frame::Err("ERR expected an array command".into()));
    };
    let Some(Frame::BulkString(name)) = args.first() else {
        return None;
    };
    Some(match lookup(name) {
        Some(handler) => handler(args, store),
        None => Frame::Err(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(name)
        )),
    })
}

/// Owns one client connection.
///
/// Generic over the transport so it can be driven by any `AsyncRead + AsyncWrite`
/// (dependency inversion). The read side decodes via [`Decoder`]; the write side
/// reuses a single output buffer across replies.
struct StreamHandler<S> {
    reader: ReadHalf<S>,
    decoder: Decoder,
    // out: BytesMut,
    store: Arc<Mutex<Store>>,
    subscriptions: HashSet<Vec<u8>>, // per-connection state
    out_tx: Outbox,
    conn_id: u64,
}

impl<S> StreamHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(reader: ReadHalf<S>, store: Arc<Mutex<Store>>, tx: UnboundedSender<Frame>) -> Self {
        StreamHandler {
            reader,
            decoder: Decoder::new(),
            // out: BytesMut::new(),
            store,
            subscriptions: HashSet::new(),
            out_tx: tx,
            conn_id: NEXT_CONN.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Read the next complete request, or `None` when the peer disconnects.
    async fn next(&mut self) -> Option<Frame> {
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(frame) = self.decoder.parse() {
                return Some(frame);
            }
            match self.reader.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.decoder.extend(&chunk[..n]),
            }
        }
    }

    /// Execute `val` and write the reply, if any.
    ///
    /// Returns the error from the write so the caller can close the connection
    /// instead of silently swallowing it.
    async fn respond(&mut self, val: Frame) -> io::Result<()> {
        let reply = self.generate_reply(&val).await;
        for f in reply {
            self.out_tx
                .send(f)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    async fn generate_reply(&mut self, f: &Frame) -> Vec<Frame> {
        // check if command is blpop
        if let Frame::Array(args) = f {
            if let Some(cmd) = bulk(args.get(0)) {
                // subscribe mode check
                if !self.subscriptions.is_empty() && !allowed_in_subscribe_mode(cmd) {
                    let name = String::from_utf8_lossy(cmd).to_lowercase();
                    return vec![Frame::Err(format!(
                        "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                    ))];
                }

                if !self.subscriptions.is_empty() {
                    if cmd == b"PING" {
                        let msg = bulk(args.get(1)).unwrap_or(b"");
                        return vec![Frame::Array(vec![Frame::bulk("pong"), Frame::bulk(msg)])];
                    }
                }

                if cmd == b"BLPOP" {
                    return vec![blpop(args, self.store.clone()).await];
                }
                if cmd == b"SUBSCRIBE" {
                    return self.subscribe(args);
                }
                if cmd == b"UNSUBSCRIBE" {
                    return self.unsubscribe(args);
                }
            }
        }
        let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        execute(f, &mut store).into_iter().collect()
    }

    fn subscribe(&mut self, args: &[Frame]) -> Vec<Frame> {
        let channels = &args[1..];
        if channels.is_empty() {
            return vec![Frame::Err(
                "ERR wrong number of arguments for 'subscribe' command".into(),
            )];
        }
        // let mut n = 0;
        channels
            .iter()
            .filter_map(|c| bulk(Some(c)))
            .map(|ch| {
                // n += 1;
                // self.out_tx.send(Frame::Array(vec![Frame::bulk("subscribe"), Frame::bulk(ch), Frame::Int(n)]));

                // for newly created channel
                let newly = self.subscriptions.insert(ch.to_vec());
                if newly {
                    let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
                    // register tx to channel_subscribers
                    store
                        .channel_subscribers
                        .entry(ch.to_vec())
                        .or_default()
                        .insert(self.conn_id, self.out_tx.clone());
                }
                Frame::Array(vec![
                    Frame::bulk("subscribe"),
                    Frame::bulk(ch),
                    Frame::Int(self.subscriptions.len() as i64),
                ])
            })
            .collect()
    }

    fn unsubscribe(&mut self, args: &[Frame]) -> Vec<Frame> {
        let channels = &args[1..];
        if channels.is_empty() {
            return vec![Frame::Err(
                "ERR wrong number of arguments for 'unsubscribe' command".into(),
            )];
        }

        channels
            .iter()
            .filter_map(|c| bulk(Some(c)))
            .map(|ch| {
                if self.subscriptions.remove(ch) {
                    let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(subs) = store.channel_subscribers.get_mut(ch) {
                        subs.remove(&self.conn_id);
                        if subs.is_empty() {
                            store.channel_subscribers.remove(ch);
                        }
                    }
                }

                Frame::Array(vec![
                    Frame::bulk(b"unsubscribe"),
                    Frame::bulk(ch),
                    Frame::Int(self.subscriptions.len() as i64),
                ])
            })
            .collect()
    }
}

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

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(Mutex::new(Store::new()));
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };

        let (rd, mut wr) = tokio::io::split(stream);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Frame>();
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            while let Some(frame) = out_rx.recv().await {
                buf.clear();
                frame.encode(&mut buf);
                if wr.write_all(&buf).await.is_err() {
                    break;
                }
            }
        });

        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            let mut handler = StreamHandler::new(rd, store_clone, out_tx);
            while let Some(val) = handler.next().await {
                if handler.respond(val).await.is_err() {
                    break;
                }
            }
        });
    }
}
