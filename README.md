[![progress-banner](https://backend.codecrafters.io/progress/redis/a5d79c6a-968e-4388-bd3d-2beb7c39bd7f)](https://app.codecrafters.io/users/zbinxp?r=2qF)

# Build Your Own Redis — Rust

A Redis-compatible server written in Rust + Tokio, built for the
[CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis).

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

## Status

**99 / 124 stages done** — see [`STAGES.md`](STAGES.md) for the full list and per-stage status.

- Core #1–7, Lists #8–18, Streams #19–31
- Transactions #32–42, Optimistic Locking #43–50
- Replication #51–68, RDB Persistence #69–74, AOF Persistence #75–84
- Pub/Sub #85–91 and Sorted Sets #92–99 (implemented out of order)
- Remaining: Bitmaps #100–108, Geospatial #109–116, Authentication #117–124

## What is implemented

- **RESP**: incremental `Decoder`, `Frame` encoding (including the RDB
  replication frame and `pending()` for truncated AOF tails).
- **Keys / strings**: `PING`, `ECHO`, `GET`, `SET` (`PX`/`PXAT`/`EXAT`),
  `INCR`, `PEXPIREAT`, `TYPE`, `KEYS`, `CONFIG GET`.
- **Lists**: `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP` (blocking).
- **Sorted sets**: `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`.
- **Streams**: `XADD`, `XRANGE`, `XREAD` (including `BLOCK` and `$`).
- **Pub/Sub**: `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, subscribed mode.
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`; optimistic
  locking uses per-key versions (`Db::touch`) rather than value comparison.
- **Replication**: replica handshake (`PING` / `REPLCONF` / `PSYNC`), empty RDB
  transfer, write propagation to one or many replicas, read-only replicas,
  `REPLCONF GETACK` / `ACK`, and `WAIT`.
- **Persistence**:
  - RDB loading (strings + expiry, binary-safe).
  - Multi-part AOF: manifest, append-only incr files, startup replay, orphan
    cleanup, truncated-tail repair.
  - `BGREWRITEAOF`: base snapshot + new incr, atomic manifest switch, background
    I/O, auto-rewrite by size/growth.
  - `INFO persistence` fields (`aof_enabled`, `aof_current_size`, ...).
- **CLI**: `--port`, `--replicaof`, `--dir`, `--dbfilename`, `--appendonly`,
  `--appenddirname`, `--appendfilename`, `--appendfsync`,
  `--auto-aof-rewrite-min-size`, `--auto-aof-rewrite-percentage`.

## Layout

```text
src/
  main.rs         command-line parsing
  lib.rs          Config + run(): server setup, AOF/RDB loading
  resp.rs         Frame, encode, Decoder
  db.rs           Entry / Value / Db, role, AOF state, versions
  client.rs       per-connection Conn
  connection.rs   StreamHandler: read/write, blocking and WAIT commands
  dispatch.rs     lookup + execute (subscribe mode, read-only, MULTI queue)
  error.rs        RedisError
  util.rs         parsing and time helpers
  rdb.rs          RDB reader
  aof.rs          manifest, serialization, BGREWRITEAOF, cleanup
  replica.rs      replica handshake + replication command stream
  commands/       connection, string, list, zset, stream, pubsub,
                  transaction, server handlers
```

## Run & test

```sh
./your_program.sh                     # build release and run on 127.0.0.1:6379
./your_program.sh --port 6380         # custom port
./your_program.sh --dir /tmp/redis-data --appendonly yes

cargo test                            # unit + black-box integration tests
cargo check --all-targets             # should be warning-free
```

The integration tests in `tests/pipelining.rs` spawn the real binary and talk
raw RESP over TCP, including multi-process replication and AOF restart tests.

## Docs

- [`HANDOFF.md`](HANDOFF.md) — architecture, master/replica flow, known issues
- [`LESSON.md`](LESSON.md) — Rust/Redis lessons and pitfalls
- [`STAGES.md`](STAGES.md) — stage list and progress
- [`docs/aof-rewrite-design.md`](docs/aof-rewrite-design.md) — optional staged
  AOF-rewrite extension (`ar1`–`ar9`, outside the official 124 stages)
- [`docs/dn4-pubsub-design.md`](docs/dn4-pubsub-design.md) — Pub/Sub delivery design

## Example

```text
SET key value   →  +OK\r\n
GET key         →  $<len>\r\n<value>\r\n      (exists)
                →  $-1\r\n                     (missing)
```
