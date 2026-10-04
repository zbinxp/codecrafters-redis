# dn4 — Pub/Sub message delivery (design)

> Stage `dn4`. Follows `hf2` (PUBLISH returns the subscriber count). This is the
> first stage that must send a frame to a connection that did **not** ask for it.

## Goal

`PUBLISH channel message` must deliver a 3-element array to **every** client
subscribed to `channel`:

```
["message", channel, message]  →  *3\r\n$7\r\nmessage\r\n$<n>\r\n<ch>\r\n$<m>\r\n<msg>\r\n
```

and still reply to the publisher with the number of subscribers (`:N\r\n`).
Clients not subscribed to the channel receive nothing.

## Why the current shape can't do it

Every connection writes only a reply to its own request, and the write half
lives inside `StreamHandler`. There is no handle another task can use to write
to an idle subscriber's socket.

## Design: per-connection **outbox** + one **writer task**

```
              ┌──────────────── connection A (subscriber) ─────────────┐
  read loop ──┼─▶ out_tx_A ──▶ out_rx_A ──▶ writer task A ──▶ socket A   │
              └───────────────────────────────────────────────────────┘
  Db registry:  "foo" ─▶ [ out_tx_A, out_tx_B ]
  PUBLISH (conn B): cmd_publish ──send(["message","foo",m])──▶ out_tx_A, out_tx_B
```

- **One `mpsc::UnboundedSender<Frame>` per connection** (its *outbox*). Every
  frame destined for that client — replies **and** published messages — goes
  through it.
- **One writer task per connection** owns the write half and drains the outbox.
  It is the only writer, so frames never interleave.
- The **read loop** owns the read half, reads a command, computes the reply,
  and `out_tx.send(reply)`. It never writes directly.

Publishing is just "send into other connections' outboxes" — no `select!`, no
inbox on the read loop.

## Data

```rust
type Outbox = tokio::sync::mpsc::UnboundedSender<Frame>;

struct Db {
    data: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
    channel_subscribers: HashMap<Vec<u8>, Vec<Outbox>>,   // was HashMap<_, usize>
}

struct StreamHandler<R> {
    reader: R,                          // read half
    decoder: Decoder,
    store: Arc<Mutex<Db>>,
    subscriptions: HashSet<Vec<u8>>,    // per-connection
    out_tx: Outbox,
}
```

Connection setup (in `main`): split the socket, create the channel, spawn the
writer task, build the handler with the read half + `out_tx`.

```rust
let (rd, wr) = tokio::io::split(stream);
let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
tokio::spawn(async move {
    let mut buf = BytesMut::new();
    while let Some(frame) = out_rx.recv().await {
        buf.clear();
        frame.encode(&mut buf);
        if wr_write(&mut wr, &buf).await.is_err() { break; }
    }
});
```

## Flows

- **SUBSCRIBE** (per channel, newly subscribed):
  1. `out_tx.send(subscribe_reply)` — enqueue the confirmation **first**;
  2. then `db.channel_subscribers.entry(ch).or_default().push(out_tx.clone())`.

  Ordering matters: register only *after* the reply is queued, otherwise a
  racing `PUBLISH` could deliver a message before the subscribe confirmation.

- **PUBLISH** (sync handler, holds `&mut Db`):
  ```rust
  let frame = Frame::Array(vec![Frame::bulk("message"), Frame::bulk(ch), Frame::bulk(msg)]);
  let mut n = 0;
  if let Some(subs) = store.channel_subscribers.get_mut(ch) {
      subs.retain(|tx| { if tx.send(frame.clone()).is_ok() { n += 1; true } else { false } });
  }
  Frame::Int(n as i64)
  ```
  `UnboundedSender::send` is **synchronous**, so this is safe while holding the
  `Mutex`. `retain` also drops dead outboxes (see below).

- **Delivery**: the subscriber's writer task writes the frame to its socket.

## Invariants / hazards

1. **Single writer per connection.** All output goes through the outbox; never
   write to the socket from two places.
2. **Confirmation-before-message.** Enqueue the SUBSCRIBE reply before
   registering the outbox.
3. **Sync `send` under lock.** Use `unbounded` (no `.await`, no backpressure).
4. **Disconnect leak.** A disconnected connection's outbox clones stay in the
   registry, so its writer task never ends. The `retain` above drops them on
   the next `PUBLISH` (count becomes correct); a cleaner fix uses a
   per-connection id and removes them on disconnect. Deferred.
5. `Db` now holds `UnboundedSender`s — `Send` + `Sync`, fine behind the `Mutex`.

## Tests (≥2 connections via `t.connect()`)

- c1 `SUBSCRIBE foo`; publisher `PUBLISH foo hello` → publisher `:1`, c1 receives `["message","foo","hello"]`.
- c1 + c2 `SUBSCRIBE foo` → both receive; `PUBLISH` → `:2`.
- c3 `SUBSCRIBE bar` → `expect_silent` for `PUBLISH foo`.
- two publishes → messages arrive in order on c1.
- c1's own `SUBSCRIBE` reply arrives before any message.

## Out of scope (later stages)

`UNSUBSCRIBE` (`ze9`), pattern subscribe (`PSUBSCRIBE`), disconnect-driven
unregistration.
