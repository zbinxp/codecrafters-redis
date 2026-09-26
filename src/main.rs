use std::{collections::HashMap, io, sync::{Arc, Mutex}, time::{Duration, Instant}};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

mod resp;
use resp::{Decoder, Frame};

struct Entry{
    value: Vec<u8>,
    expires_at: Option<Instant>
}

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
        Some(_) => Frame::Err("echo argument is not a bulk string".into()),
        None => Frame::Err("wrong number of arguments for 'echo' command".into()),
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
                Frame::BulkString(entry.value.clone())
            },
            None => Frame::Null,
        },
        Some(_) => Frame::Err("wrong format of key".into()),
        None => Frame::Err("wrong number of arguments".into()),
    }
}

fn cmd_set(args: &[Frame], store: &mut Store) -> Frame {
    match parse_set(args) {
        Ok((key, value, expires_at)) => {
            let value = value.to_vec();
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
        return Err(Frame::Err("syntax error".into()));
    }
    let Some(key) = bulk(args.get(1)) else {
        return Err(Frame::Err("wrong number of arguments for 'set'".into()));
    };
    let Some(value) = bulk(args.get(2)) else {
        return Err(Frame::Err("wrong number of arguments for 'set'".into()));
    };
    let expires_at = match(args.get(3), args.get(4)) {
        (None, _) => None,
        (Some(Frame::BulkString(opt)), Some(Frame::BulkString(ms))) if opt.eq_ignore_ascii_case(b"PX") => {
            let Some(ms) = parse_millis(ms) else {
                return Err(Frame::Err("value is not an integer".into()));
            };
            Instant::now().checked_add(Duration::from_millis(ms))
        },
        _ => return Err(Frame::Err("syntax error".into())),
    };
    
    Ok((key, value, expires_at))
}

/// Map a command name to its handler. Adding a command is a one-line change
/// here plus a new handler -- `execute` stays untouched (Open/Closed).
fn lookup(name: &[u8]) -> Option<Handler> {
    match name {
        b"PING" | b"COMMAND" => Some(cmd_ping),
        b"ECHO" => Some(cmd_echo),
        b"GET" => Some(cmd_get),
        b"SET" => Some(cmd_set),
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
        return Some(Frame::Err("expected an array command".into()));
    };
    let Some(Frame::BulkString(name)) = args.first() else {
        return None;
    };
    Some(match lookup(name) {
        Some(handler) => handler(args, store),
        None => Frame::Err(format!(
            "unknown command '{}'",
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
