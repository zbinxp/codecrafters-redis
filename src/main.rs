use std::{
    cmp::min, collections::{HashMap, HashSet, VecDeque}, io, sync::{Arc, Mutex}, time::{Duration, Instant},
};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::sleep
};

mod resp;
use resp::{Decoder, Frame};

const WRONGTYPE_KEY_WRONG: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";
const ERR_NOT_VALID_INDEX: &str = "ERR value is not an integer or out of range";
const ERR_VALUE_OUT_OF_RANGE: &str = "ERR value is out of range, must be positive";

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
}

struct Db {
    data: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
}

impl Db {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            waiters: HashMap::new()
        }
    }
}

/// A command handler receives the full argument list (index 0 is the command
/// name) and produces a reply.
type Handler = fn(&[Frame], &mut Store) -> Frame;
type Store = Db;

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
        Value::List(_) => Frame::Err(WRONGTYPE_KEY_WRONG.into()),
        Value::String(val) => Frame::bulk(val)
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

fn push_impl(args: &[Frame], store: &mut Store, fn_name:&str, push:fn(&mut VecDeque<Vec<u8>>,Vec<u8>)) -> Frame {
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err(format!("ERR wrong number of arguments for '{}'",fn_name));
    };
    if args.len() < 3 {
        return Frame::Err(format!("ERR wrong number of arguments for '{}'",fn_name));
    }

    let entry = store.data
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
        push(list,val.to_vec());

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

    let len = list.len() as i64;
    if len == 0 {
        return Frame::Array(Vec::new());
    }
    let mut start = start;
    let mut stop = stop;
    if start < 0 { start += len;}
    if stop < 0 { stop += len;}
    if start < 0 { start = 0;}
    if start >= len || start > stop {
        return Frame::Array(Vec::new());
    }
    let stop = min(stop, len - 1);
    let bulk_str_list: Vec<Frame> = list.range(start as usize..=stop as usize)
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
        Value::String(_) => Frame::Err(WRONGTYPE_KEY_WRONG.into())
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
            },
            None => return Frame::Err(ERR_NOT_VALID_INDEX.into())
        },
        None => None
    };
    let Some(entry) = live_entry(store, key) else {
        return count.map_or(Frame::Null, |_|Frame::NullArray);
    };

    match &mut entry.value {
        Value::List(list) => {
            match count {
                None => match list.pop_front() {
                    Some(v) => Frame::bulk(v),
                    None => Frame::Null,
                },
                Some(n) => {
                    let n = n.min(list.len() as i64);
                    let removed: Vec<Frame> = (0..n).filter_map(|_|list.pop_front())
                        .map(Frame::bulk).collect();
                    Frame::Array(removed)
                }
            }          
        },
        Value::String(_) => Frame::Err(WRONGTYPE_KEY_WRONG.into())
    }
}

/// BLPOP key timeout → on success, a 2-element array [key, element]:
/// xj7: Spec: BLPOP key <timeout> where the timeout can be fractional seconds
async fn blpop(args: &[Frame], store: Arc<Mutex<Store>>) -> Frame {
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err(format!("ERR wrong number of arguments for '{}' command", "blpop"));
    };
    let timeout = match bulk(args.get(2)) {
        Some(val) => match parse_f64(val) {
            Some(timeout) => timeout,
            None => return Frame::Err("ERR timeout is not a float or out of range".into()),
        },
        None => 0.0
    };

    if !timeout.is_finite() || timeout < 0.0 {
        return Frame::Err(ERR_VALUE_OUT_OF_RANGE.into());
    }

    let timeout = Duration::from_secs_f64(timeout);
    loop { // when notified, we need a loop to check if value is arrived
        let rx = {
            let mut db = store.lock().unwrap_or_else(|e|e.into_inner());
            if let Some(entry) = db.data.get_mut(key) {
                let Value::List(list) = &mut entry.value else{
                    return Frame::Err(WRONGTYPE_KEY_WRONG.into());     
                };
                if let Some(v) = list.pop_front() {
                    return Frame::Array(vec![Frame::bulk(key), Frame::bulk(v)])
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

fn live_entry<'a>(store:&'a mut Store, key:&[u8]) -> Option<&'a mut Entry> {
    if store.data.get(key).and_then(|e|e.expires_at).is_some_and(|t|t <= Instant::now()) {
        store.data.remove(key);
    }
    store.data.get_mut(key)
}

/// Map a command name to its handler. Adding a command is a one-line change
/// here plus a new handler -- `execute` stays untouched (Open/Closed).
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
    stream: S,
    decoder: Decoder,
    out: BytesMut,
    store: Arc<Mutex<Store>>,
    subscriptions: HashSet<Vec<u8>>, // per-connection state
}

impl<S> StreamHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(stream: S, store: Arc<Mutex<Store>>) -> Self {
        StreamHandler {
            stream,
            decoder: Decoder::new(),
            out: BytesMut::new(),
            store,
            subscriptions: HashSet::new(),
        }
    }

    /// Read the next complete request, or `None` when the peer disconnects.
    async fn next(&mut self) -> Option<Frame> {
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(frame) = self.decoder.parse() {
                return Some(frame);
            }
            match self.stream.read(&mut chunk).await {
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
            self.out.clear();
            f.encode(&mut self.out);
            self.stream.write_all(&self.out).await?;
        }
        Ok(())
    }

    async fn generate_reply(&mut self, f:&Frame) -> Vec<Frame> {
        // check if command is blpop
        if let Frame::Array(args) = f {
            if let Some(cmd) = bulk(args.get(0)) {
                if &cmd == b"BLPOP" {
                    return vec![blpop(args, self.store.clone()).await];
                }
                if &cmd == b"SUBSCRIBE" {
                    return self.subscribe(args);
                }
            }
        }
        let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        execute(f, &mut store).into_iter().collect()
    }

    fn subscribe(&mut self, args:&[Frame]) -> Vec<Frame> {
        let channels = &args[1..];
        if channels.is_empty() {
            return vec![Frame::Err("ERR wrong number of arguments for 'subscribe' command".into())];
        }
        channels.iter()
            .filter_map(|c|bulk(Some(c)))
            .map(|ch| {
                self.subscriptions.insert(ch.to_vec());
                Frame::Array(vec![
                    Frame::bulk("subscribe"),
                    Frame::bulk(ch),
                    Frame::Int(self.subscriptions.len() as i64)
                ])
            })
            .collect()
        
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(Mutex::new(Store::new()));
    loop {
        let Ok((s, _)) = listener.accept().await else {
            continue;
        };
        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            let mut handler = StreamHandler::new(s, store_clone);
            while let Some(val) = handler.next().await {
                if handler.respond(val).await.is_err() {
                    break;
                }
            }
        });
    }
}
