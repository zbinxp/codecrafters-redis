use std::{collections::HashMap, io, sync::{Arc, Mutex}, time::{Duration, Instant}};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

mod resp;
use resp::{Decoder, Frame};

const WRONGTYPE_KEY_WRONG:&str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// the value type in our Store
struct Entry{
    value: Value,
    expires_at: Option<Instant>
}

impl Entry {
    fn new(value:Value) -> Self {
        Self {
            value,
            expires_at: None
        }
    }
}

enum Value {
    String(Vec<u8>),
    List(Vec<Vec<u8>>)
}

// impl Value {
//     fn rpush(&mut self, elem: Vec<u8>) -> Option<usize> {
//         match self {
//             Value::String(_) => None,
//             Value::List(list) => {
//                 list.push(elem);
//                 Some(list.len())
//             }
//         }
//     }
// }

/// A command handler receives the full argument list (index 0 is the command
/// name) and produces a reply.
type Handler = fn(&[Frame], &mut Store) -> Frame;
type Store = HashMap<Vec<u8>, Entry>;

fn cmd_ping(_args: &[Frame], _store: &mut Store) -> Frame {
    Frame::simple("PONG")
}

fn cmd_echo(args: &[Frame], _store: &mut Store) -> Frame { // echo some_value
    match args.get(1) {
        Some(Frame::BulkString(arg)) => Frame::bulk(arg),
        Some(_) => Frame::Err("ERR echo argument is not a bulk string".into()),
        None => Frame::Err("ERR wrong number of arguments for 'echo' command".into()),
    }
}

fn cmd_get(args: &[Frame], store: &mut Store) -> Frame { // get key
    match args.get(1) {
        Some(Frame::BulkString(key)) => match store.get(key) {
            Some(entry) => {
                if let Some(expire_at) = entry.expires_at {
                    if expire_at <= Instant::now() {
                        return Frame::Null;
                    }
                };
                match &entry.value {
                    Value::String(v) => Frame::bulk(v),
                    Value::List(_) => Frame::Err("WRONGTYPE Operation".into())
                }
            },
            None => Frame::Null,
        },
        Some(_) => Frame::Err("ERR wrong format of key".into()),
        None => Frame::Err("ERR wrong number of arguments".into()),
    }
}

fn cmd_set(args: &[Frame], store: &mut Store) -> Frame {
    match parse_set(args) {
        Ok((key, value, expires_at)) => {
            let value = Value::String(value.to_vec());
            store.insert(key.to_vec(), Entry{value, expires_at});
            Frame::SimpleString(b"OK".to_vec())
        },
        Err(frame) => frame,
    }
}

fn bulk(frame: Option<&Frame>) -> Option<&[u8]> {
    match frame {
        Some(Frame::BulkString(b)) => Some(b),
        _ => None,
    }
}

fn parse_millis(bytes:&[u8]) -> Option<u64> {
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
    let expires_at = match(args.get(3), args.get(4)) {
        (None, _) => None,
        (Some(Frame::BulkString(opt)), Some(Frame::BulkString(ms))) if opt.eq_ignore_ascii_case(b"PX") => {
            let Some(ms) = parse_millis(ms) else {
                return Err(Frame::Err("ERR value is not an integer".into()));
            };
            Instant::now().checked_add(Duration::from_millis(ms))
        },
        _ => return Err(Frame::Err("ERR syntax error".into())),
    };
    
    Ok((key, value, expires_at))
}

/// Spec: RPUSH <key> <element> appends to a list (creating it if absent), and returns the list length as a RESP
/// integer
fn cmd_rpush(args: &[Frame], store: &mut Store) -> Frame {
    let Some(key) = bulk(args.get(1)) else {
        return Frame::Err("ERR wrong number of arguments for 'rpush'".into());
    };
    if args.len() < 3 {
        return Frame::Err("ERR wrong number of arguments for 'rpush'".into());
    }

    let entry = store
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::List(Vec::new())));

    if entry.expires_at.is_some_and(|t| t<=Instant::now()) {
        *entry = Entry::new(Value::List(Vec::new()));
    }

    let Value::List(list) = &mut entry.value else {
        return Frame::Err(WRONGTYPE_KEY_WRONG.into());
    };

    for elem in &args[2..] {
        let Some(val) = bulk(Some(elem)) else {
            return Frame::Err("ERR wrong format of arguments for 'rpush'".into());
        };
        list.push(val.to_vec());
    }
    Frame::Int(list.len() as i64)
    
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
    store: Arc<Mutex<Store>>
}

impl<S> StreamHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(stream: S, store:Arc<Mutex<Store>>) -> Self {
        StreamHandler {
            stream,
            decoder: Decoder::new(),
            out: BytesMut::new(),
            store
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
        let reply = {
            let mut store = self.store.lock().unwrap();
            execute(&val, &mut store)
        };
        if let Some(reply) = reply {
            self.out.clear();
            reply.encode(&mut self.out);
            self.stream.write_all(&self.out).await?;
        }
        Ok(())
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
