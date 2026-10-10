use crate::client::Conn;
use crate::commands::{blpop, collect_xread, resolve_ids};
use crate::dispatch::execute;
use crate::resp::{Decoder, Frame};
use crate::util;
use crate::{
    db::{Outbox, Store},
    error::RedisError,
};
use std::{
    io,
    sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering},
    time::Duration,
    vec,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadHalf},
    sync::oneshot,
    time::sleep,
};

// one global counter
static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// Owns one client connection.
///
/// Generic over the transport so it can be driven by any `AsyncRead + AsyncWrite`
/// (dependency inversion). The read side decodes via [`Decoder`]; the write side
/// reuses a single output buffer across replies. Per-connection command state
/// lives in [`Conn`].
pub struct StreamHandler<S> {
    reader: ReadHalf<S>,
    decoder: Decoder,
    store: Arc<Mutex<Store>>,
    conn: Conn,
}

impl<S> StreamHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(reader: ReadHalf<S>, store: Arc<Mutex<Store>>, out_tx: Outbox) -> Self {
        StreamHandler {
            reader,
            decoder: Decoder::new(),
            store,
            conn: Conn::new(NEXT_CONN.fetch_add(1, Ordering::Relaxed), out_tx),
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

    /// Execute `val` and queue the reply, if any.
    ///
    /// Returns the error from the write so the caller can close the connection
    /// instead of silently swallowing it.
    pub async fn respond(&mut self, val: Frame) -> io::Result<()> {
        let reply = match self.generate_reply(&val).await {
            Ok(results) => results,
            Err(e) => vec![Frame::Err(e.message)],
        };
        for f in reply {
            self.conn
                .out_tx
                .send(f)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    /// Dispatch one request.
    ///
    /// The blocking commands (`BLPOP`, `XREAD ... BLOCK`) need to hold the store
    /// across `.await`, so they are handled here rather than in the sync
    /// [`execute`] table. They are only reachable outside subscribe mode; while
    /// subscribed, `execute` rejects them through its allow-list.
    async fn generate_reply(&mut self, f: &Frame) -> Result<Vec<Frame>, RedisError> {
        if self.conn.multi.is_none() && self.conn.subscriptions.is_empty() {
            if let Frame::Array(args) = f {
                if let Some(cmd) = args.first().and_then(Frame::as_bulk) {
                    // async mode command
                    if cmd == b"BLPOP" {
                        return Ok(vec![blpop(args, self.store.clone()).await?]);
                    }
                    if cmd == b"XREAD" && args.get(1).and_then(Frame::as_bulk) == Some(b"BLOCK") {
                        return Ok(vec![self.xread_block(args).await?]);
                    }
                }
            }
        }
        let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        execute(f, &mut self.conn, &mut store)
    }

    pub async fn xread_block(&mut self, args: &[Frame]) -> Result<Frame, RedisError> {
        let Some(pos) = args
            .iter()
            .position(|a| a.as_bulk() == Some(b"STREAMS"))
        else {
            return Err(RedisError::xread_syntax());
        };
        let rest = &args[pos + 1..];
        if rest.len() < 2 || rest.len() % 2 != 0 {
            return Err(RedisError::stream_unbalanced_arguments());
        }
        let n = rest.len() / 2;
        let keys = &rest[..n];
        let ids_raw = &rest[n..];
        let ids = {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            resolve_ids(&mut store, keys, ids_raw)?
        };

        // single key for blocking
        let Some(channel) = keys.first().and_then(Frame::as_bulk) else {
            return Err(RedisError::wrong_type());
        };

        let block = if args.get(1).and_then(Frame::as_bulk) == Some(b"BLOCK") {
            let Some(ms) = args.get(2).and_then(Frame::as_bulk).and_then(util::parse) else {
                return Err(RedisError::timeout_not_integer());
            };
            Some(Duration::from_millis(ms))
        } else {
            None
        };

        let Some(timeout) = block else {
            return Err(RedisError::wrong_args("xread"));
        };

        loop {
            let rx = {
                let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

                let out = collect_xread(&mut store, &keys, &ids)?;
                if !out.is_empty() {
                    return Ok(Frame::Array(out));
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
                _ = sleep(timeout), if !timeout.is_zero() => return Ok(Frame::NullArray),
            }
        }
    }
}
