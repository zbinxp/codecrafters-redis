//! Black-box tests for the server: each test spawns the real binary and talks
//! to it over a raw `TcpStream`, so we control exactly how the bytes are
//! chunked and can assert on the raw RESP the client would receive.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread::sleep;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};

const ADDR: &str = "127.0.0.1:6379";
/// Generous per-read timeout; on failure a test shows a diff rather than hanging.
const TIMEOUT: Duration = Duration::from_secs(2);

const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
/// `[a, b, c, d, e]` as a RESP array.
const ABCDE: &[u8] = b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// The server hard-codes port 6379, so only one test may hold it at a time.
/// Tests run in parallel threads by default; this serialises them.
fn lock_server() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A running server process, killed when the guard is dropped.
struct Server(Child);

impl Server {
    fn start() -> Self {
        let addr = ADDR.parse().unwrap();
        // The tests serialise themselves with `lock_server()`, but a stale
        // server (e.g. one left over from `./your_program.sh`) can still be
        // holding the port. If it is, the child we spawn below dies with
        // `AddrInUse` while `TcpStream::connect` keeps succeeding -- so the
        // test would silently talk to the stale binary instead of the one we
        // just built. Refuse to run in that case.
        if TcpStream::connect_timeout(&addr, Duration::from_millis(150)).is_ok() {
            panic!("{ADDR} is already in use -- kill the stale server before running tests");
        }

        let mut child = Command::new(env!("CARGO_BIN_EXE_codecrafters-redis"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn the server binary");

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            // If the child already exited it will never listen; report why now
            // rather than waiting out the whole timeout.
            if let Some(status) = child.try_wait().expect("failed to poll the server process") {
                panic!("server exited before listening on {ADDR}: {status}");
            }
            if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
                return Server(child);
            }
            sleep(Duration::from_millis(20));
        }
        panic!("server never started listening on {ADDR}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Owns the server + port lock for the duration of a test, plus one default
/// client connection.
struct TestCtx {
    // Drop order matters: fields drop top-to-bottom, and the server must be
    // killed (freeing port 6379) *before* the global lock is released -- else
    // the next test acquires the lock and finds the port still in use.
    _server: Server,
    _guard: MutexGuard<'static, ()>,
    client: Client,
}

impl TestCtx {
    fn new() -> Self {
        let guard = lock_server();
        let server = Server::start();
        let client = Client::connect();
        TestCtx {
            _server: server,
            _guard: guard,
            client,
        }
    }

    /// Open an additional connection to the same server.
    fn connect(&self) -> Client {
        Client::connect()
    }
}

/// A connected client with RESP helpers, so tests read as command/expect pairs.
///
/// Incoming bytes are buffered so `expect` consumes exactly the bytes it
/// asserts and keeps any remainder for the next call. Without buffering, one
/// `read` returning two frames would break a chained `expect`.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect() -> Self {
        Client {
            stream: TcpStream::connect(ADDR).expect("failed to connect"),
            buf: Vec::new(),
        }
    }

    /// Send `args` as a RESP array of bulk strings.
    fn cmd(&mut self, args: &[&[u8]]) -> &mut Self {
        let mut out = BytesMut::new();
        generate_bulk_string(args.to_vec(), &mut out);
        self.raw(&out)
    }

    /// Write raw bytes (pipelining / split-command tests).
    fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.stream.write_all(bytes).expect("failed to write");
        self
    }

    /// Read until at least `want` bytes are buffered, or `timeout` elapses.
    fn fill_at_least(&mut self, want: usize, timeout: Duration) {
        self.stream
            .set_read_timeout(Some(timeout))
            .expect("failed to set read timeout");
        let mut chunk = [0u8; 512];
        while self.buf.len() < want {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                    break
                }
                Err(e) => panic!("read failed: {e}"),
            }
        }
    }

    /// Read until the buffer contains a `\r\n`, or `timeout` elapses.
    fn fill_line(&mut self, timeout: Duration) {
        self.stream
            .set_read_timeout(Some(timeout))
            .expect("failed to set read timeout");
        let mut chunk = [0u8; 512];
        while !self.buf.windows(2).any(|w| w == b"\r\n") {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                    break
                }
                Err(e) => panic!("read failed: {e}"),
            }
        }
    }

    /// Assert the next `expected.len()` bytes equal `expected`; consume them.
    fn expect(&mut self, expected: &[u8]) -> &mut Self {
        self.fill_at_least(expected.len(), TIMEOUT);
        assert!(
            self.buf.len() >= expected.len() && &self.buf[..expected.len()] == expected,
            "expected {expected:?}, buffered {:?}",
            String::from_utf8_lossy(&self.buf)
        );
        self.buf.drain(..expected.len());
        self
    }

    /// Assert the next line starts with `prefix`; consume the whole line.
    fn expect_prefix(&mut self, prefix: &[u8]) {
        self.fill_line(TIMEOUT);
        let end = self
            .buf
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| p + 2)
            .expect("no complete line arrived");
        let line = &self.buf[..end];
        assert!(
            line.starts_with(prefix),
            "expected a reply starting with {prefix:?}, got {:?}",
            String::from_utf8_lossy(line)
        );
        self.buf.drain(..end);
    }

    /// Assert nothing is buffered and nothing arrives within `timeout`.
    fn expect_silent(&mut self, timeout: Duration) {
        if !self.buf.is_empty() {
            panic!(
                "server sent unexpected bytes: {:?}",
                String::from_utf8_lossy(&self.buf)
            );
        }
        self.stream
            .set_read_timeout(Some(timeout))
            .expect("failed to set read timeout");
        let mut chunk = [0u8; 64];
        match self.stream.read(&mut chunk) {
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Ok(0) => panic!("server closed the connection"),
            Ok(n) => panic!(
                "server sent unexpected bytes: {:?}",
                String::from_utf8_lossy(&chunk[..n])
            ),
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

/// Append a RESP bulk string to `out`.
fn put_bulk_string(val: &[u8], out: &mut BytesMut) {
    out.put_u8(b'$');
    out.extend_from_slice(val.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

/// Encode `values` as a RESP array of bulk strings.
fn generate_bulk_string(values: Vec<&[u8]>, out: &mut BytesMut) {
    out.put_u8(b'*');
    out.extend_from_slice(values.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for val in values {
        put_bulk_string(val, out);
    }
}

/// Seed the default key `k` with the list `[a, b, c, d, e]`.
fn push_abcde(c: &mut Client) {
    c.cmd(&[b"RPUSH", b"k", b"a", b"b", b"c", b"d", b"e"])
        .expect(b":5\r\n");
}

// ---------------------------------------------------------------------------
// Framing / connection handling
// ---------------------------------------------------------------------------

#[test]
fn responds_to_two_pings_sent_in_a_single_write() {
    let mut t = TestCtx::new();
    t.client.raw(PING).raw(PING).expect(b"+PONG\r\n+PONG\r\n");
}

/// The command the user actually runs: `echo -e "PING\nPING" | redis-cli`.
///
/// This is the exact byte stream that puts on the wire (captured from
/// redis-cli 7.0.15): a `COMMAND DOCS` handshake it issues on connect, then one
/// RESP array per line of stdin, all within a millisecond of each other.
///
/// `COMMAND DOCS` is not really implemented -- replying `+PONG` to it is enough
/// for redis-cli, which only uses the reply for tab-completion metadata.
#[test]
fn responds_to_the_byte_stream_redis_cli_pipes_in() {
    let mut t = TestCtx::new();
    t.client
        .raw(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
        .raw(PING)
        .raw(PING)
        .expect(b"+PONG\r\n+PONG\r\n+PONG\r\n");
}

/// TCP is a stream: a command can arrive split across reads. The server must
/// wait for the rest rather than answer a fragment or drop it.
#[test]
fn holds_a_command_split_across_writes_until_it_is_complete() {
    let mut t = TestCtx::new();

    // Half a command -- the server must stay quiet rather than guess.
    t.client
        .raw(b"*1\r\n$4\r\nPI")
        .expect_silent(Duration::from_millis(300));

    // The rest arrives, followed by a second whole command.
    t.client
        .raw(b"NG\r\n")
        .raw(PING)
        .expect(b"+PONG\r\n+PONG\r\n");
}

#[test]
fn responds_to_multiple_concurrent_pings() {
    let mut t = TestCtx::new();
    let mut c2 = t.connect();

    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
    // c1 must still be live after c2 was served
    c2.cmd(&[b"PING"]).expect(b"+PONG\r\n");
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

#[test]
fn responds_to_echo_world() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ECHO", b"world"])
        .expect(b"$5\r\nworld\r\n");
}

#[test]
fn echo_without_args_keeps_the_connection_open() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ECHO"]).expect_prefix(b"-ERR");
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

#[test]
fn empty_command_keeps_the_connection_open() {
    let mut t = TestCtx::new();
    t.client.raw(b"*0\r\n").cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

#[test]
fn empty_command_combines_normal_returns_ok() {
    let mut t = TestCtx::new();
    t.client.raw(b"*0\r\n").raw(PING).expect(b"+PONG\r\n");
}

// ---------------------------------------------------------------------------
// Strings: SET / GET
// ---------------------------------------------------------------------------

#[test]
fn set_key_value_ok_then_get() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"hello", b"world"])
        .expect(b"+OK\r\n");
    t.client
        .cmd(&[b"GET", b"hello"])
        .expect(b"$5\r\nworld\r\n");
}

/// `GET` on a key that was never set must return the RESP null bulk string,
/// exactly `$-1\r\n` -- not an empty bulk string, and not a bulk string whose
/// content happens to be the literal text `$-1\r\n`.
#[test]
fn get_missing_key_returns_null_bulk_string() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"GET", b"no-such-key"]).expect(b"$-1\r\n");
}

/// The store is shared across connections, not per-connection: a value written
/// by one client must be readable by another.
#[test]
fn set_is_visible_from_another_connection() {
    let mut t = TestCtx::new();
    let mut reader = t.connect();

    t.client
        .cmd(&[b"SET", b"shared-key", b"shared-value"])
        .expect(b"+OK\r\n");
    reader
        .cmd(&[b"GET", b"shared-key"])
        .expect(b"$12\r\nshared-value\r\n");
}

/// A second `SET` on the same key replaces the value rather than leaving the
/// old one in place.
#[test]
fn set_overwrites_previous_value() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"k", b"first"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"SET", b"k", b"second"]).expect(b"+OK\r\n");
    t.client
        .cmd(&[b"GET", b"k"])
        .expect(b"$6\r\nsecond\r\n");
}

/// Keys and values are binary-safe: an embedded CRLF in a value must survive a
/// round trip untouched rather than being treated as a terminator.
#[test]
fn set_and_get_are_binary_safe() {
    let mut t = TestCtx::new();
    let value: &[u8] = b"a\r\nb";

    t.client
        .cmd(&[b"SET", b"binary", value])
        .expect(b"+OK\r\n");
    t.client
        .cmd(&[b"GET", b"binary"])
        .expect(b"$4\r\na\r\nb\r\n");
}

// ---------------------------------------------------------------------------
// Expiry (SET ... PX)
// ---------------------------------------------------------------------------

#[test]
fn set_with_expire_then_get_absent() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"hello", b"world", b"PX", b"100"])
        .expect(b"+OK\r\n");
    t.client
        .cmd(&[b"GET", b"hello"])
        .expect(b"$5\r\nworld\r\n");

    sleep(Duration::from_millis(200));

    t.client.cmd(&[b"GET", b"hello"]).expect(b"$-1\r\n");
}

#[test]
fn set_clear_previous_ttl() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"hello", b"world", b"PX", b"100"])
        .expect(b"+OK\r\n");
    t.client
        .cmd(&[b"SET", b"hello", b"test"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client
        .cmd(&[b"GET", b"hello"])
        .expect(b"$4\r\ntest\r\n");
}

/// A non-numeric `PX` argument is rejected with an error, and the connection
/// stays usable afterwards.
#[test]
fn set_with_non_integer_px_is_an_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"not-a-number"])
        .expect_prefix(b"-ERR");
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

/// Once a key expires, a later plain `SET` on the same key stores a fresh,
/// non-expiring value.
#[test]
fn expired_key_can_be_set_again() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"old", b"PX", b"100"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client.cmd(&[b"SET", b"k", b"new"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"GET", b"k"]).expect(b"$3\r\nnew\r\n");
}

// ---------------------------------------------------------------------------
// Lists: RPUSH
// ---------------------------------------------------------------------------

#[test]
fn appends_to_a_list_returns_length() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"fruits", b"apple"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"RPUSH", b"fruits", b"banana"])
        .expect(b":2\r\n");
}

/// An expired key must be treated as absent by RPUSH: the list is created
/// fresh (length 1), not appended to the stale entry left behind by `GET`'s
/// lazy expiration.
#[test]
fn rpush_after_expiry_creates_a_fresh_list() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client.cmd(&[b"RPUSH", b"k", b"x"]).expect(b":1\r\n");
}

#[test]
fn appends_multi_value_to_a_list_returns_length() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"fruits", b"apple", b"banana", b"carrot"])
        .expect(b":3\r\n");
}

/// Appending multiple elements to an existing list returns the *total* length,
/// not the number of elements in this command (lx4).
#[test]
fn appends_multiple_to_existing_list_returns_total_length() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"list_key", b"e1", b"e2", b"e3"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"RPUSH", b"list_key", b"e4", b"e5"])
        .expect(b":5\r\n");
}

/// RPUSH against a key holding a string must fail with WRONGTYPE rather than
/// silently reporting a bogus length.
#[test]
fn rpush_on_a_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"RPUSH", b"s", b"x"]).expect(WRONGTYPE);
}

// ---------------------------------------------------------------------------
// Lists: LRANGE
// ---------------------------------------------------------------------------

#[test]
fn lrange_returns_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"list_key", b"e1", b"e2", b"e3"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"LRANGE", b"list_key", b"0", b"1"])
        .expect(b"*2\r\n$2\r\ne1\r\n$2\r\ne2\r\n");
}

#[test]
fn lrange_missing_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LRANGE", b"list_key", b"0", b"1"])
        .expect(b"*0\r\n");
}

/// `stop` beyond the end is clamped to the last element.
#[test]
fn lrange_stop_beyond_length_clamps_to_last() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"99"])
        .expect(ABCDE);
}

/// A mid-range slice is inclusive of both `start` and `stop`.
#[test]
fn lrange_mid_range_is_inclusive() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"2", b"4"])
        .expect(b"*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n");
}

/// `start == stop` returns exactly one element.
#[test]
fn lrange_single_index_returns_one_element() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"4", b"4"])
        .expect(b"*1\r\n$1\r\ne\r\n");
}

/// `start >= len` returns an empty array (and must not panic / poison the lock).
#[test]
fn lrange_start_beyond_length_returns_empty() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"6", b"10"])
        .expect(b"*0\r\n");
}

/// `start > stop` returns an empty array.
#[test]
fn lrange_start_greater_than_stop_returns_empty() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"4", b"2"])
        .expect(b"*0\r\n");
}

/// LRANGE against a key holding a string returns WRONGTYPE, not an array.
#[test]
fn lrange_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"LRANGE", b"s", b"0", b"1"]).expect(WRONGTYPE);
}

/// Non-numeric indexes are rejected with the Redis error message.
#[test]
fn lrange_non_integer_index_returns_error() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"x", b"y"])
        .expect(b"-ERR value is not an integer or out of range\r\n");
}

/// An out-of-range LRANGE must not kill the connection or poison the store lock:
/// a following PING still gets answered.
#[test]
fn lrange_out_of_range_keeps_the_server_alive() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"10", b"20"])
        .expect(b"*0\r\n");
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

/// `-2 -1` selects the last two elements.
#[test]
fn lrange_negative_indexes_return_last_two() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"-2", b"-1"])
        .expect(b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n");
}

/// `0 -3` selects everything except the last two elements.
#[test]
fn lrange_positive_start_negative_stop() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-3"])
        .expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

/// A positive start with a negative stop.
#[test]
fn lrange_mixed_indexes() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"2", b"-1"])
        .expect(b"*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n");
}

/// `-1 -1` selects exactly the last element.
#[test]
fn lrange_single_negative_index() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"-1", b"-1"])
        .expect(b"*1\r\n$1\r\ne\r\n");
}

/// A negative start below the list wraps to 0 (start of the list).
#[test]
fn lrange_negative_start_out_of_range_clamps_to_zero() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"-6", b"2"])
        .expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

/// Indexes beyond both ends of the list clamp to the full list.
#[test]
fn lrange_negative_beyond_both_ends_returns_all() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"-100", b"100"])
        .expect(ABCDE);
}

/// A negative stop that maps below 0 is left negative (Redis semantics), so
/// `start > stop` and the result is empty -- not clamped to the first element.
#[test]
fn lrange_out_of_range_negative_stop_returns_empty() {
    let mut t = TestCtx::new();
    push_abcde(&mut t.client);
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-6"])
        .expect(b"*0\r\n");
}

// ---------------------------------------------------------------------------
// Lists: LPUSH
// ---------------------------------------------------------------------------

#[test]
fn lpush_one_returns_one() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"LPUSH", b"k", b"c"]).expect(b":1\r\n");
}

#[test]
fn lpush_three_then_lrange_returns_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPUSH", b"k", b"c", b"b", b"a"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

#[test]
fn lpush_less_args_return_wrong_number_of_arguments() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPUSH", b"k"])
        .expect(b"-ERR wrong number of arguments for 'lpush'\r\n");
}

/// The exact spec flow: two LPUSH commands onto the same key, then LRANGE.
#[test]
fn lpush_across_commands_returns_length_and_is_reversed() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"LPUSH", b"k", b"c"]).expect(b":1\r\n");
    t.client
        .cmd(&[b"LPUSH", b"k", b"b", b"a"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

/// A single multi-element LPUSH prepends each element in turn, reversing order.
#[test]
fn lpush_multiple_reverses_order() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPUSH", b"k", b"a", b"b", b"c"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");
}

/// RPUSH then LPUSH on the same key: the prepend lands before the appended element.
#[test]
fn lpush_after_rpush_prepends_before() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"x"]).expect(b":1\r\n");
    t.client.cmd(&[b"LPUSH", b"k", b"y"]).expect(b":2\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*2\r\n$1\r\ny\r\n$1\r\nx\r\n");
}

/// LPUSH against a key holding a string fails with WRONGTYPE.
#[test]
fn lpush_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"k", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"LPUSH", b"k", b"x"]).expect(WRONGTYPE);
}

#[test]
fn llen_return_zero_on_missing_key() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"LLEN", b"k"]).expect(b":0\r\n");
}

#[test]
fn llen_return_length_on_list() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a",b"b",b"c"]).expect(b":3\r\n");
    t.client.cmd(&[b"LLEN",b"k"]).expect(b":3\r\n");
}

/// LLEN against a key holding a string returns WRONGTYPE.
#[test]
fn llen_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"LLEN", b"s"]).expect(WRONGTYPE);
}

/// An expired key reads as absent: LLEN returns 0, not WRONGTYPE.
#[test]
fn llen_on_expired_key_returns_zero() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client.cmd(&[b"LLEN", b"k"]).expect(b":0\r\n");
}

/// LLEN takes exactly one argument; extra args are an error.
#[test]
fn llen_with_extra_args_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LLEN", b"k", b"extra"])
        .expect(b"-ERR wrong number of arguments for 'llen' command\r\n");
}

#[test]
fn rpush_then_pop_return_the_first_element() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"k", b"one", b"two",b"three"])
        .expect(b":3\r\n");
    t.client
        .cmd(&[b"LPOP", b"k"])
        .expect(b"$3\r\none\r\n");
}

#[test]
fn lpop_missing_key_return_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPOP", b"missing"])
        .expect(b"$-1\r\n");
}

/// The spec checks that the popped element is gone and the rest shift down.
#[test]
fn lpop_removes_from_the_front_and_shifts() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"k", b"one", b"two", b"three", b"four", b"five"])
        .expect(b":5\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$3\r\none\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*4\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n$4\r\nfive\r\n");
}

/// Popping the last element empties the list; the next pop returns null.
#[test]
fn lpop_drains_list_then_returns_null() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"x"]).expect(b":1\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$1\r\nx\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$-1\r\n");
}

/// LPOP takes from the head, so an LPUSH'd element comes out first.
#[test]
fn lpop_takes_the_head_pushed_by_lpush() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a", b"b"]).expect(b":2\r\n");
    t.client.cmd(&[b"LPUSH", b"k", b"z"]).expect(b":3\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$1\r\nz\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$1\r\na\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*1\r\n$1\r\nb\r\n");
}

/// LPOP against a key holding a string returns WRONGTYPE.
#[test]
fn lpop_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"LPOP", b"s"]).expect(WRONGTYPE);
}

/// An expired key reads as absent: LPOP returns null, not WRONGTYPE.
#[test]
fn lpop_on_expired_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$-1\r\n");
}

/// The spec: `LPOP k 2` returns the first two as an array and shifts the rest down.
#[test]
fn lpop_count_two_removes_from_front() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"RPUSH", b"k", b"one", b"two", b"three", b"four", b"five"])
        .expect(b":5\r\n");
    t.client
        .cmd(&[b"LPOP", b"k", b"2"])
        .expect(b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");
    t.client
        .cmd(&[b"LRANGE", b"k", b"0", b"-1"])
        .expect(b"*3\r\n$5\r\nthree\r\n$4\r\nfour\r\n$4\r\nfive\r\n");
}

/// A count of 1 still returns an *array* (unlike the count-less form).
#[test]
fn lpop_count_one_returns_array() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a", b"b", b"c"]).expect(b":3\r\n");
    t.client
        .cmd(&[b"LPOP", b"k", b"1"])
        .expect(b"*1\r\n$1\r\na\r\n");
}

/// A count of 0 removes nothing and returns an empty array.
#[test]
fn lpop_count_zero_returns_empty_array() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a"]).expect(b":1\r\n");
    t.client.cmd(&[b"LPOP", b"k", b"0"]).expect(b"*0\r\n");
}

/// A count greater than the length removes everything and returns all elements.
#[test]
fn lpop_count_exceeding_length_returns_all() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a", b"b", b"c"]).expect(b":3\r\n");
    t.client
        .cmd(&[b"LPOP", b"k", b"99"])
        .expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    t.client.cmd(&[b"LPOP", b"k"]).expect(b"$-1\r\n");
}

/// With a count, a missing key replies with a null *array* (`*-1`), not `$-1`.
#[test]
fn lpop_missing_with_count_returns_null_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPOP", b"missing", b"2"])
        .expect(b"*-1\r\n");
}

/// A negative count is rejected.
#[test]
fn lpop_negative_count_returns_error() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a"]).expect(b":1\r\n");
    t.client
        .cmd(&[b"LPOP", b"k", b"-1"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
}

/// A non-integer count is rejected.
#[test]
fn lpop_non_integer_count_returns_error() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a"]).expect(b":1\r\n");
    t.client
        .cmd(&[b"LPOP", b"k", b"x"])
        .expect(b"-ERR value is not an integer or out of range\r\n");
}

/// A count on a string key is still WRONGTYPE.
#[test]
fn lpop_with_count_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"LPOP", b"s", b"2"]).expect(WRONGTYPE);
}


#[test]
fn blpop_single_waiter_return_key_value_array() {
    let mut t = TestCtx::new();
    let mut pusher = t.connect();
    t.client
        .cmd(&[b"BLPOP", b"k", b"0"])
        .expect_silent(Duration::from_millis(300));
    pusher.cmd(&[b"RPUSH",b"k",b"foo"]).expect(b":1\r\n");
    t.client.expect(b"*2\r\n$1\r\nk\r\n$3\r\nfoo\r\n");
}

/// If the list already has an element, BLPOP returns it without blocking.
#[test]
fn blpop_returns_immediately_when_list_has_elements() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"k", b"a"]).expect(b":1\r\n");
    t.client
        .cmd(&[b"BLPOP", b"k", b"0"])
        .expect(b"*2\r\n$1\r\nk\r\n$1\r\na\r\n");
}

/// With two blocked clients, the one that waited longest is served first.
#[test]
fn blpop_serves_longest_waiting_client_first() {
    let mut t = TestCtx::new();
    let mut c2 = t.connect();
    let mut pusher = t.connect();

    t.client.cmd(&[b"BLPOP", b"k", b"0"]);
    t.client.expect_silent(Duration::from_millis(200)); // c1 registered first
    c2.cmd(&[b"BLPOP", b"k", b"0"]);
    c2.expect_silent(Duration::from_millis(200));

    pusher.cmd(&[b"RPUSH", b"k", b"one"]).expect(b":1\r\n");
    t.client.expect(b"*2\r\n$1\r\nk\r\n$3\r\none\r\n");
    c2.expect_silent(Duration::from_millis(200)); // c2 still waiting

    pusher.cmd(&[b"RPUSH", b"k", b"two"]).expect(b":1\r\n");
    c2.expect(b"*2\r\n$1\r\nk\r\n$3\r\ntwo\r\n");
}

/// BLPOP against a string key is WRONGTYPE, not an indefinite block.
#[test]
fn blpop_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"BLPOP", b"s", b"0"]).expect(WRONGTYPE);
}

/// A negative timeout is rejected.
#[test]
fn blpop_with_negative_timeout_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP", b"k", b"-1"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
}

/// BLPOP with no key is an arity error.
#[test]
fn blpop_without_key_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP"])
        .expect(b"-ERR wrong number of arguments for 'blpop' command\r\n");
}

/// A non-zero fractional timeout with nothing pushed returns a null array.
#[test]
fn blpop_fractional_timeout_returns_null_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP", b"k", b"0.1"])
        .expect(b"*-1\r\n");
}

/// A push before the timeout expires still wakes the waiter.
#[test]
fn blpop_fractional_timeout_wakes_on_push() {
    let mut t = TestCtx::new();
    let mut pusher = t.connect();

    t.client.cmd(&[b"BLPOP", b"k", b"0.5"]);
    t.client.expect_silent(Duration::from_millis(100));
    pusher.cmd(&[b"RPUSH", b"k", b"foo"]).expect(b":1\r\n");
    t.client.expect(b"*2\r\n$1\r\nk\r\n$3\r\nfoo\r\n");
}

/// An integer timeout still works (regression for the seconds conversion).
#[test]
fn blpop_integer_timeout_returns_null_array() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"BLPOP", b"k", b"1"]).expect(b"*-1\r\n");
}

/// `nan` must be rejected with an error, not panic the connection task.
#[test]
fn blpop_nan_timeout_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP", b"k", b"nan"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
    // the connection must stay usable after the error
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

/// `inf` is rejected.
#[test]
fn blpop_inf_timeout_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP", b"k", b"inf"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
}

/// A negative fractional timeout is rejected.
#[test]
fn blpop_negative_fractional_timeout_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"BLPOP", b"k", b"-0.5"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
}

#[test]
fn blpop_err_with_negative_timeout() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"BLPOP", b"k", b"-0.5"])
        .expect(b"-ERR value is out of range, must be positive\r\n");
}

#[test]
fn subscribe_key_returns_length() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SUBSCRIBE",b"k"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$1\r\nk\r\n:1\r\n");
}

#[test]
fn subscribe_without_key_returns_error() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SUBSCRIBE"])
        .expect(b"-ERR wrong number of arguments for 'subscribe' command\r\n");
}

#[test]
fn subscribe_multiple_channels_returns_multiple_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE",b"a", b"b"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n")
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n");
}

/// Subscribing to more channels across separate commands increments the count.
#[test]
fn subscribe_multiple_commands_increment_count() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n");
}

/// Re-subscribing to the same channel does not change the count.
#[test]
fn subscribe_same_channel_does_not_increment() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
}

/// Subscription counts are per-connection.
#[test]
fn subscriptions_are_per_client() {
    let mut t = TestCtx::new();
    let mut other = t.connect();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    other
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");
}

/// The exact single-line error Redis returns for a disallowed command in
/// subscribed mode (must contain no CR/LF except the terminator).
fn subscribe_mode_error(name: &str) -> Vec<u8> {
    format!(
        "-ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
    )
    .into_bytes()
}

#[test]
fn subscribe_then_echo_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"ECHO", b"hey"])
        .expect(&subscribe_mode_error("echo"));
}

#[test]
fn subscribe_then_ping_returns_pong() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"PING"])
        .expect(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
}

/// In subscribed mode, PING echoes the optional message as the 2nd element.
#[test]
fn subscribed_ping_echoes_message() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"PING", b"hello"])
        .expect(b"*2\r\n$4\r\npong\r\n$5\r\nhello\r\n");
}

/// A client that is not subscribed still gets the plain +PONG.
#[test]
fn unsubscribed_ping_returns_pong() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"PING"]).expect(b"+PONG\r\n");
}

/// SET is rejected in subscribed mode (and the command name is lowercased).
#[test]
fn subscribe_mode_rejects_set() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SET", b"k", b"v"])
        .expect(&subscribe_mode_error("set"));
}

/// GET is rejected in subscribed mode.
#[test]
fn subscribe_mode_rejects_get() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"GET", b"k"])
        .expect(&subscribe_mode_error("get"));
}

/// SUBSCRIBE remains allowed once in subscribed mode.
#[test]
fn subscribe_mode_allows_further_subscribe() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n");
}

/// Commands are unrestricted before any subscription (guards against a global
/// flag instead of per-connection state).
#[test]
fn commands_work_before_subscribe() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ECHO", b"hey"])
        .expect(b"$3\r\nhey\r\n");
    t.client.cmd(&[b"SET", b"k", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"GET", b"k"]).expect(b"$1\r\nv\r\n");
}

/// The restriction is per-connection: another client is unaffected.
#[test]
fn subscribed_mode_is_per_connection() {
    let mut t = TestCtx::new();
    let mut other = t.connect();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    other.cmd(&[b"SET", b"k", b"v"]).expect(b"+OK\r\n");
}

#[test]
fn publish_nope_key_with_msg_return_empty() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"PUBLISH", b"nope", b"msg"])
        .expect(b":0\r\n");
}

/// PUBLISH returns the number of subscribers of the channel.
#[test]
fn publish_counts_one_subscriber() {
    let mut t = TestCtx::new();
    let mut publisher = t.connect();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    publisher
        .cmd(&[b"PUBLISH", b"foo", b"msg"])
        .expect(b":1\r\n");
}

/// The count is per-channel across all connections.
#[test]
fn publish_counts_all_subscribers_of_the_channel() {
    let mut t = TestCtx::new();
    let mut c2 = t.connect();
    let mut c3 = t.connect();
    let mut publisher = t.connect();

    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    c2.cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");
    c3.cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");

    publisher
        .cmd(&[b"PUBLISH", b"bar", b"msg"])
        .expect(b":2\r\n");
    publisher
        .cmd(&[b"PUBLISH", b"foo", b"msg"])
        .expect(b":1\r\n");
}

/// Re-subscribing the same client must not double-count.
#[test]
fn publish_after_duplicate_subscribe_counts_once() {
    let mut t = TestCtx::new();
    let mut publisher = t.connect();
    t.client
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");
    publisher
        .cmd(&[b"PUBLISH", b"bar", b"msg"])
        .expect(b":1\r\n");
}

/// PUBLISH needs both a channel and a message.
#[test]
fn publish_without_message_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"PUBLISH", b"foo"])
        .expect(b"-ERR wrong number of arguments for 'publish' command\r\n");
}

#[test]
fn subscribe_then_publish_sub_receives_message(){
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE",b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    let mut t2 = t.connect();
    t2.cmd(&[b"PUBLISH", b"foo", b"hello"])
        .expect(b":1\r\n");
    t.client.expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nhello\r\n");
}

/// Every subscriber of a channel receives the published message.
#[test]
fn publish_delivers_to_all_subscribers() {
    let mut t = TestCtx::new();
    let mut c2 = t.connect();
    let mut publisher = t.connect();

    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    c2.cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");

    publisher.cmd(&[b"PUBLISH", b"foo", b"hi"]).expect(b":2\r\n");

    let expected: &[u8] = b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$2\r\nhi\r\n";
    t.client.expect(expected);
    c2.expect(expected);
}

/// A subscriber of a different channel receives nothing.
#[test]
fn publish_does_not_deliver_to_other_channels() {
    let mut t = TestCtx::new();
    let mut other = t.connect();
    let mut publisher = t.connect();

    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    other.cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:1\r\n");

    publisher.cmd(&[b"PUBLISH", b"foo", b"hi"]).expect(b":1\r\n");

    t.client.expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$2\r\nhi\r\n");
    other.expect_silent(Duration::from_millis(200));
}

/// Multiple messages arrive in publish order.
#[test]
fn publish_delivers_messages_in_order() {
    let mut t = TestCtx::new();
    let mut publisher = t.connect();

    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");

    publisher.cmd(&[b"PUBLISH", b"foo", b"one"]).expect(b":1\r\n");
    publisher.cmd(&[b"PUBLISH", b"foo", b"two"]).expect(b":1\r\n");

    t.client.expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\none\r\n");
    t.client.expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\ntwo\r\n");
}

#[test]
fn sub_two_channels_then_unsub_returns_length() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:0\r\n");
}

/// After unsubscribing, the channel is no longer delivered, but other
/// channels the client is still subscribed to keep working.
#[test]
fn unsubscribe_stops_delivery_but_keeps_other_channels() {
    let mut t = TestCtx::new();
    let mut publisher = t.connect();

    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"SUBSCRIBE", b"baz"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nbaz\r\n:2\r\n");

    publisher.cmd(&[b"PUBLISH", b"foo", b"pre"]).expect(b":1\r\n");
    t.client
        .expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\npre\r\n");

    t.client
        .cmd(&[b"UNSUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n");

    // foo is no longer delivered (and no longer counted)
    publisher.cmd(&[b"PUBLISH", b"foo", b"post"]).expect(b":0\r\n");
    t.client.expect_silent(Duration::from_millis(200));

    // baz still is
    publisher.cmd(&[b"PUBLISH", b"baz", b"hi"]).expect(b":1\r\n");
    t.client
        .expect(b"*3\r\n$7\r\nmessage\r\n$3\r\nbaz\r\n$2\r\nhi\r\n");
}

/// Unsubscribing from a channel you never subscribed to is a no-op that
/// still replies, without changing the count.
#[test]
fn unsubscribe_from_unsubscribed_channel_keeps_count() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"bar"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:1\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n");
}

/// Removing the last channel exits subscribed mode: normal commands work again.
#[test]
fn unsubscribe_last_channel_leaves_subscribed_mode() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"foo"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n");
    t.client.cmd(&[b"ECHO", b"hey"]).expect(b"$3\r\nhey\r\n");
}

/// One UNSUBSCRIBE can name several channels, one reply each.
#[test]
fn unsubscribe_multiple_channels_in_one_command() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SUBSCRIBE", b"a", b"b"])
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n")
        .expect(b"*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n");
    t.client
        .cmd(&[b"UNSUBSCRIBE", b"a", b"b"])
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n")
        .expect(b"*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n");
}

#[test]
fn zadd_key_score_member_returns_count() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD",b"z", b"10.0", b"m"])
        .expect(b":1\r\n");
}

/// A non-float score is rejected.
#[test]
fn zadd_non_float_score_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"notanumber", b"m"])
        .expect(b"-ERR value is not a valid float\r\n");
}

/// NaN is not a valid score (Redis rejects it; it must not be stored).
#[test]
fn zadd_nan_score_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"nan", b"m"])
        .expect(b"-ERR value is not a valid float\r\n");
}

/// ZADD needs key, score and member.
#[test]
fn zadd_missing_member_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"1.0"])
        .expect(b"-ERR wrong number of arguments for 'zadd' command\r\n");
}

/// ZADD against a string key is WRONGTYPE.
#[test]
fn zadd_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client
        .cmd(&[b"ZADD", b"s", b"1.0", b"m"])
        .expect(WRONGTYPE);
}

/// An expired key is treated as absent: ZADD creates a fresh sorted set.
#[test]
fn zadd_on_expired_key_creates_new_set() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");

    sleep(Duration::from_millis(200));

    t.client
        .cmd(&[b"ZADD", b"k", b"1.0", b"m"])
        .expect(b":1\r\n");
}

#[test]
fn zadd_update_score_returns_zero() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD",b"z", b"10.0", b"m"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"ZADD", b"z", b"8.0", b"m"])
        .expect(b":0\r\n");
}

#[test]
fn zrank_return_lexicographically() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"100.0", b"foo"]).expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"100.0", b"bar"]).expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"20.0", b"baz"]).expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"30.1", b"caz"]).expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"40.2", b"paz"]).expect(b":1\r\n")
        ;
    // baz, caz, paz, bar, foo
    t.client
        .cmd(&[b"ZRANK", b"z", b"baz"])
        .expect(b":0\r\n");

    t.client
        .cmd(&[b"ZRANK", b"z", b"paz"])
        .expect(b":2\r\n");

    t.client
        .cmd(&[b"ZRANK", b"z", b"foo"])
        .expect(b":4\r\n");
}

/// The remaining ranks in the ordering (caz=1, bar=3).
#[test]
fn zrank_returns_middle_ranks() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"100.0", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"100.0", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"20.0", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"30.1", b"caz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"40.2", b"paz"])
        .expect(b":1\r\n");
    t.client.cmd(&[b"ZRANK", b"z", b"caz"]).expect(b":1\r\n");
    t.client.cmd(&[b"ZRANK", b"z", b"bar"]).expect(b":3\r\n");
}

/// A member that is not in the set returns null.
#[test]
fn zrank_missing_member_returns_null() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZADD", b"z", b"1.0", b"m"]).expect(b":1\r\n");
    t.client.cmd(&[b"ZRANK", b"z", b"nope"]).expect(b"$-1\r\n");
}

/// A missing key returns null.
#[test]
fn zrank_missing_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZRANK", b"missing", b"m"])
        .expect(b"$-1\r\n");
}

/// ZRANK against a string key is WRONGTYPE.
#[test]
fn zrank_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"ZRANK", b"s", b"m"]).expect(WRONGTYPE);
}

/// An expired key reads as absent, so ZRANK returns null.
#[test]
fn zrank_expired_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"ZRANK", b"k", b"m"]).expect(b"$-1\r\n");
}

#[test]
fn zrange_returns_array() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"100.0", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"100.0", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"20.0", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"30.1", b"caz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"40.2", b"paz"])
        .expect(b":1\r\n");
    t.client.cmd(&[b"ZRANGE", b"z", b"0", b"2"]).expect(b"*3\r\n$3\r\nbaz\r\n$3\r\ncaz\r\n$3\r\npaz\r\n");
    t.client.cmd(&[b"ZRANGE", b"z", b"2", b"4"]).expect(b"*3\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n");
}

/// Seed key `z` with the 5-member set from the spec (rank order:
/// baz, caz, paz, bar, foo).
fn zadd_spec_set(c: &mut Client) {
    c.cmd(&[b"ZADD", b"z", b"100.0", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"100.0", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"20.0", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"30.1", b"caz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"40.2", b"paz"])
        .expect(b":1\r\n");
}

/// `stop` beyond the end clamps to the last member.
#[test]
fn zrange_stop_beyond_length_clamps() {
    let mut t = TestCtx::new();
    zadd_spec_set(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"z", b"0", b"99"])
        .expect(b"*5\r\n$3\r\nbaz\r\n$3\r\ncaz\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n");
}

/// `start >= len` returns an empty array.
#[test]
fn zrange_start_beyond_length_returns_empty() {
    let mut t = TestCtx::new();
    zadd_spec_set(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"z", b"5", b"10"])
        .expect(b"*0\r\n");
}

/// `start > stop` returns an empty array.
#[test]
fn zrange_start_greater_than_stop_returns_empty() {
    let mut t = TestCtx::new();
    zadd_spec_set(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"z", b"2", b"1"])
        .expect(b"*0\r\n");
}

/// A missing key returns an empty array.
#[test]
fn zrange_missing_key_returns_empty() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZRANGE", b"missing", b"0", b"1"])
        .expect(b"*0\r\n");
}

/// ZRANGE against a string key is WRONGTYPE.
#[test]
fn zrange_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"ZRANGE", b"s", b"0", b"1"]).expect(WRONGTYPE);
}

/// An expired key reads as absent, so ZRANGE returns an empty array.
#[test]
fn zrange_expired_key_returns_empty() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"ZRANGE", b"k", b"0", b"1"]).expect(b"*0\r\n");
}

/// Negative indexes work through the shared normalization (bj4).
#[test]
fn zrange_negative_indexes() {
    let mut t = TestCtx::new();
    zadd_spec_set(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"z", b"-2", b"-1"])
        .expect(b"*2\r\n$3\r\nbar\r\n$3\r\nfoo\r\n");
    t.client
        .cmd(&[b"ZRANGE", b"z", b"0", b"-3"])
        .expect(b"*3\r\n$3\r\nbaz\r\n$3\r\ncaz\r\n$3\r\npaz\r\n");
}

/// Seed `racer_scores` with the spec's 5 members (rank order:
/// Ford, Sam-Bodden, Ben, Royce, Prickett).
fn zadd_racer_scores(c: &mut Client) {
    c.cmd(&[b"ZADD", b"racer_scores", b"8.5", b"Sam-Bodden"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"racer_scores", b"10.2", b"Royce"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"racer_scores", b"6.1", b"Ford"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"racer_scores", b"14.9", b"Prickett"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"racer_scores", b"10.2", b"Ben"])
        .expect(b":1\r\n");
}

/// The spec's `-2 -1` example: the last two members.
#[test]
fn zrange_negative_last_two() {
    let mut t = TestCtx::new();
    zadd_racer_scores(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"racer_scores", b"-2", b"-1"])
        .expect(b"*2\r\n$5\r\nRoyce\r\n$8\r\nPrickett\r\n");
}

/// An out-of-range negative start is treated as 0 (start of the set).
#[test]
fn zrange_negative_start_out_of_range_clamps_to_zero() {
    let mut t = TestCtx::new();
    zadd_racer_scores(&mut t.client);
    t.client
        .cmd(&[b"ZRANGE", b"racer_scores", b"-6", b"2"])
        .expect(b"*3\r\n$4\r\nFord\r\n$10\r\nSam-Bodden\r\n$3\r\nBen\r\n");
}

/// A positive start with a negative stop.
#[test]
fn zrange_mixed_positive_negative() {
    let mut t = TestCtx::new();
    // rank order: foo(20), caz(25), paz(25), bar(30.1), baz(40.2)
    t.client
        .cmd(&[b"ZADD", b"zset_key", b"20.0", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"30.1", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"40.2", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"25.0", b"paz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"25.0", b"caz"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"ZRANGE", b"zset_key", b"2", b"-1"])
        .expect(b"*3\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nbaz\r\n");
}

#[test]
fn zcard_return_count() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"zset_key", b"20.0", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"30.1", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"40.2", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"zset_key", b"25.0", b"paz"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"ZCARD", b"zset_key"])
        .expect(b":4\r\n");
}

/// Updating a member's score must not change the cardinality (hf1 regression).
#[test]
fn zcard_unchanged_after_score_update() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"1.0", b"a"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"2.0", b"b"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"100.0", b"a"])
        .expect(b":0\r\n");
    t.client.cmd(&[b"ZCARD", b"z"]).expect(b":2\r\n");
}

/// A missing key has cardinality 0.
#[test]
fn zcard_missing_key_returns_zero() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZCARD", b"missing"]).expect(b":0\r\n");
}

/// ZCARD against a string key is WRONGTYPE.
#[test]
fn zcard_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"ZCARD", b"s"]).expect(WRONGTYPE);
}

/// An expired key reads as absent, so ZCARD returns 0.
#[test]
fn zcard_expired_key_returns_zero() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"ZCARD", b"k"]).expect(b":0\r\n");
}

/// ZCARD takes exactly one key.
#[test]
fn zcard_with_extra_args_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZCARD", b"z", b"extra"])
        .expect(b"-ERR wrong number of arguments for 'zcard' command\r\n");
}

#[test]
fn zscore_returns_score() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"30.1", b"m2"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"ZSCORE", b"z", b"m2"])
        .expect(b"$4\r\n30.1\r\n");
}

/// ZSCORE reflects an updated score.
#[test]
fn zscore_reflects_score_update() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"30.1", b"m"])
        .expect(b":1\r\n");
    t.client
        .cmd(&[b"ZADD", b"z", b"100.99", b"m"])
        .expect(b":0\r\n");
    t.client.cmd(&[b"ZSCORE", b"z", b"m"]).expect(b"$6\r\n100.99\r\n");
}

/// An integer-valued score is rendered without a trailing `.0`.
#[test]
fn zscore_integer_score_has_no_decimal() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZADD", b"z", b"20.0", b"m"]).expect(b":1\r\n");
    t.client.cmd(&[b"ZSCORE", b"z", b"m"]).expect(b"$2\r\n20\r\n");
}

/// A member that is not in the set returns null.
#[test]
fn zscore_missing_member_returns_null() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZADD", b"z", b"1.0", b"m"]).expect(b":1\r\n");
    t.client.cmd(&[b"ZSCORE", b"z", b"nope"]).expect(b"$-1\r\n");
}

/// A missing key returns null.
#[test]
fn zscore_missing_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZSCORE", b"missing", b"m"])
        .expect(b"$-1\r\n");
}

/// ZSCORE against a string key is WRONGTYPE.
#[test]
fn zscore_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"ZSCORE", b"s", b"m"]).expect(WRONGTYPE);
}

/// An expired key reads as absent, so ZSCORE returns null.
#[test]
fn zscore_expired_key_returns_null() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"ZSCORE", b"k", b"m"]).expect(b"$-1\r\n");
}

/// ZSCORE takes exactly key + member.
#[test]
fn zscore_wrong_arity_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZSCORE", b"z"])
        .expect(b"-ERR wrong number of arguments for 'zscore' command\r\n");
}

#[test]
fn zrem_returns_one_and_remove_element(){
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"80.5", b"foo"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"50.3", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"80.5", b"bar"])
        .expect(b":1\r\n")
        .cmd(&[b"ZREM", b"z", b"baz"])
        .expect(b":1\r\n")
        .cmd(&[b"ZRANGE", b"z", b"0", b"-1"])
        .expect(b"*2\r\n$3\r\nbar\r\n$3\r\nfoo\r\n");
}

/// Removing a member that isn't there returns 0.
#[test]
fn zrem_missing_member_returns_zero() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZADD", b"z", b"1.0", b"m"]).expect(b":1\r\n");
    t.client.cmd(&[b"ZREM", b"z", b"nope"]).expect(b":0\r\n");
}

/// Removing from a missing key returns 0.
#[test]
fn zrem_missing_key_returns_zero() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZREM", b"missing", b"m"]).expect(b":0\r\n");
}

/// ZREM against a string key is WRONGTYPE.
#[test]
fn zrem_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"s", b"v"]).expect(b"+OK\r\n");
    t.client.cmd(&[b"ZREM", b"s", b"m"]).expect(WRONGTYPE);
}

/// An expired key reads as absent, so ZREM returns 0.
#[test]
fn zrem_expired_key_returns_zero() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"ZREM", b"k", b"m"]).expect(b":0\r\n");
}

/// Removing every member leaves the set empty.
#[test]
fn zrem_all_members_leaves_empty_set() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZADD", b"z", b"1.0", b"a"])
        .expect(b":1\r\n")
        .cmd(&[b"ZADD", b"z", b"2.0", b"b"])
        .expect(b":1\r\n")
        .cmd(&[b"ZREM", b"z", b"a"])
        .expect(b":1\r\n")
        .cmd(&[b"ZREM", b"z", b"b"])
        .expect(b":1\r\n")
        .cmd(&[b"ZCARD", b"z"])
        .expect(b":0\r\n");
}

/// ZREM takes exactly key + member.
#[test]
fn zrem_wrong_arity_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"ZREM", b"z"])
        .expect(b"-ERR wrong number of arguments for 'zrem' command\r\n");
}

#[test]
fn type_of_set_returns_string() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v"])
        .expect(b"+OK\r\n")
        .cmd(&[b"TYPE", b"k"])
        .expect(b"+string\r\n");
}

#[test]
fn type_of_list_returns_list() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"RPUSH", b"l", b"a"]).expect(b":1\r\n");
    t.client.cmd(&[b"TYPE", b"l"]).expect(b"+list\r\n");
}

#[test]
fn type_of_zset_returns_zset() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"ZADD", b"z", b"1.0", b"m"]).expect(b":1\r\n");
    t.client.cmd(&[b"TYPE", b"z"]).expect(b"+zset\r\n");
}

#[test]
fn type_missing_key_returns_none() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"TYPE", b"missing"]).expect(b"+none\r\n");
}

#[test]
fn type_expired_key_returns_none() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client.cmd(&[b"TYPE", b"k"]).expect(b"+none\r\n");
}

#[test]
fn type_wrong_arity_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"TYPE"])
        .expect(b"-ERR wrong number of arguments for 'type' command\r\n");
}

#[test]
fn xadd_key_id_field_value_returns_stream_id() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-1", b"foo", b"bar"])
        .expect(b"$3\r\n0-1\r\n")
        .cmd(&[b"TYPE", b"s"])
        .expect(b"+stream\r\n");
}

/// A second XADD appends and returns its own ID.
#[test]
fn xadd_appends_second_entry() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-1", b"a", b"1"])
        .expect(b"$3\r\n0-1\r\n");
    t.client
        .cmd(&[b"XADD", b"s", b"0-2", b"b", b"2"])
        .expect(b"$3\r\n0-2\r\n");
}

/// An entry can carry several field/value pairs.
#[test]
fn xadd_multi_field_entry() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-1", b"f1", b"v1", b"f2", b"v2"])
        .expect(b"$3\r\n0-1\r\n");
}

/// A malformed ID is rejected.
#[test]
fn xadd_invalid_id_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"notanid", b"f", b"v"])
        .expect(b"-ERR Invalid stream ID specified as stream command argument\r\n");
}

/// Field/value pairs must come in twos.
#[test]
fn xadd_odd_pairs_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-1", b"f"])
        .expect(b"-ERR wrong number of arguments for 'xadd' command\r\n");
}

/// XADD against a string key is WRONGTYPE.
#[test]
fn xadd_on_string_key_returns_wrongtype() {
    let mut t = TestCtx::new();
    t.client.cmd(&[b"SET", b"k", b"v"]).expect(b"+OK\r\n");
    t.client
        .cmd(&[b"XADD", b"k", b"0-1", b"f", b"v"])
        .expect(WRONGTYPE);
}

/// An expired key is treated as absent, so XADD creates a fresh stream.
#[test]
fn xadd_on_expired_key_creates_stream() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"SET", b"k", b"v", b"PX", b"100"])
        .expect(b"+OK\r\n");
    sleep(Duration::from_millis(200));
    t.client
        .cmd(&[b"XADD", b"k", b"0-1", b"f", b"v"])
        .expect(b"$3\r\n0-1\r\n");
}

#[test]
fn xadd_error_on_equal_id() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"1-2", b"bar", b"baz"])
        .expect(b"$3\r\n1-2\r\n");

    t.client
        .cmd(&[b"XADD", b"s", b"1-2", b"baz", b"foo"])
        .expect_prefix(b"-ERR The ID specified in XADD is equal or smaller than");
}

/// An ID with a smaller milliseconds value is rejected.
#[test]
fn xadd_rejects_smaller_ms() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"1-2", b"f", b"v"])
        .expect(b"$3\r\n1-2\r\n");
    t.client
        .cmd(&[b"XADD", b"s", b"0-3", b"f", b"v"])
        .expect(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
}

/// An ID with the same ms but a smaller sequence number is rejected.
#[test]
fn xadd_rejects_smaller_seq() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"1-2", b"f", b"v"])
        .expect(b"$3\r\n1-2\r\n");
    t.client
        .cmd(&[b"XADD", b"s", b"1-1", b"f", b"v"])
        .expect(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
}

/// An ID with a larger milliseconds value is accepted.
#[test]
fn xadd_accepts_larger_ms() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"1-2", b"f", b"v"])
        .expect(b"$3\r\n1-2\r\n");
    t.client
        .cmd(&[b"XADD", b"s", b"2-0", b"f", b"v"])
        .expect(b"$3\r\n2-0\r\n");
}

/// 0-0 is always invalid.
#[test]
fn xadd_rejects_zero_zero() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-0", b"f", b"v"])
        .expect(b"-ERR The ID specified in XADD must be greater than 0-0\r\n");
}

/// On an empty stream, 0-1 is the minimum valid ID.
#[test]
fn xadd_accepts_min_valid_id_on_empty() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"XADD", b"s", b"0-1", b"f", b"v"])
        .expect(b"$3\r\n0-1\r\n");
}