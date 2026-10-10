use std::collections::HashSet;

use crate::db::Outbox;

/// State that lives for the lifetime of one client connection.
///
/// Commands that need connection context (subscriptions today, `MULTI` /
/// `WATCH` later) receive `&mut Conn`. Keeping it separate from the transport
/// lets the command layer touch it without knowing about sockets or byte
/// buffers.
pub struct Conn {
    /// Stable id used to register this connection in `Db::channel_subscribers`.
    pub id: u64,
    /// Channels this connection is currently subscribed to.
    pub subscriptions: HashSet<Vec<u8>>,
    /// Writer side of this connection's outgoing queue.
    pub out_tx: Outbox,
}

impl Conn {
    pub fn new(id: u64, out_tx: Outbox) -> Self {
        Self {
            id,
            subscriptions: HashSet::new(),
            out_tx,
        }
    }
}
