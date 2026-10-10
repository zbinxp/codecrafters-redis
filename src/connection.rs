use crate::commands::{blpop, bulk_at, collect_xread, resolve_ids};
use crate::dispatch::execute;
use crate::resp::{Decoder, Frame};
use crate::util;
use crate::{
    db::{Outbox, Store},
    error::RedisError,
};
use std::{
    collections::HashSet,
    io,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Duration,
    vec,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadHalf},
    sync::{
        mpsc::UnboundedSender,
        oneshot,
    },
    time::sleep,
};

// one global counter
static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// Owns one client connection.
///
/// Generic over the transport so it can be driven by any `AsyncRead + AsyncWrite`
/// (dependency inversion). The read side decodes via [`Decoder`]; the write side
/// reuses a single output buffer across replies.
pub struct StreamHandler<S> {
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
    pub fn new(reader: ReadHalf<S>, store: Arc<Mutex<Store>>, tx: UnboundedSender<Frame>) -> Self {
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
    pub async fn next(&mut self) -> Option<Frame> {
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
    pub async fn respond(&mut self, val: Frame) -> io::Result<()> {
        let reply = match self.generate_reply(&val).await {
            Ok(results) => results,
            Err(e) => vec![Frame::Err(e.message)]
        };
        for f in reply {
            self.out_tx
                .send(f)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    async fn generate_reply(&mut self, f: &Frame) -> Result<Vec<Frame>, RedisError> {
        // check if command is blpop
        if let Frame::Array(args) = f {
            if let Some(cmd) = args.first().and_then(Frame::as_bulk) {
                // subscribe mode check
                if !self.subscriptions.is_empty() && !allowed_in_subscribe_mode(cmd) {
                    let name = String::from_utf8_lossy(cmd).to_lowercase();
                    return Err(RedisError::not_allowed_in_subscribe_mode(&name))
                }

                if !self.subscriptions.is_empty() {
                    if cmd == b"PING" {
                        let msg = bulk_at(args, 1).unwrap_or(b"");
                        return Ok(vec![Frame::Array(vec![Frame::bulk("pong"), Frame::bulk(msg)])])
                    }
                }

                if cmd == b"BLPOP" {
                    return Ok(vec![blpop(args, self.store.clone()).await?])
                }
                if cmd == b"XREAD" && bulk_at(args, 1) == Some(b"BLOCK") {
                    return Ok(vec![self.xread_block(args).await?])
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
        execute(f, &mut store)
    }

    pub async fn xread_block(&mut self, args: &[Frame]) -> Result<Frame,RedisError> {
        let Some(pos) = args
            .iter()
            .position(|a| a.as_bulk() == Some(b"STREAMS"))
        else {
            return Err(RedisError::wrong_args("xread"))
        };
        let rest = &args[pos + 1..];
        if rest.len() < 2 || rest.len() % 2 != 0 {
            return Err(RedisError::stream_unbalanced_arguments())
        }
        let n = rest.len() / 2;
        let keys = &rest[..n];
        let ids_raw = &rest[n..];
        let ids = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            resolve_ids(&mut store, keys, ids_raw)?
        };

        let channel = keys.first().and_then(Frame::as_bulk).ok_or_else(||RedisError::wrong_type())?;

        let timeout:u64 = bulk_at(args, 2).and_then(util::parse).ok_or_else(||RedisError::timeout_not_integer())?;
        let ms = Duration::from_millis(timeout);

        loop {
            let rx = {
                let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

                let out = collect_xread(&mut store, &keys, &ids)?;
                if !out.is_empty() {
                    return Ok(Frame::Array(out))
                }

                let (tx, rx) = oneshot::channel();
                store
                    .waiters
                    .entry(channel.to_vec())
                    .or_default()
                    .push_back(tx);
                rx
            };
            tokio::select! {
                _ = rx => continue,
                _ = sleep(ms), if !ms.is_zero() => return Ok(Frame::NullArray),
            }
        }
    }

    pub fn subscribe(&mut self, args: &[Frame]) -> Result<Vec<Frame>,RedisError> {
        let channels = &args[1..];
        if channels.is_empty() {
            return Err(RedisError::wrong_args("subscribe"))
        }
        Ok(channels
            .iter()
            .filter_map(|c| c.as_bulk())
            .map(|ch| {
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
        )
    }

    pub fn unsubscribe(&mut self, args: &[Frame]) -> Result<Vec<Frame>,RedisError> {
        let channels = &args[1..];
        if channels.is_empty() {
            return Err(RedisError::wrong_args("unsubscribe"))
        }

        Ok(channels
            .iter()
            .filter_map(|c| c.as_bulk())
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
        )
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
