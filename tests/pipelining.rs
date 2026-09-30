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
struct Client(TcpStream);

impl Client {
    fn connect() -> Self {
        Client(TcpStream::connect(ADDR).expect("failed to connect"))
    }

    /// Send `args` as a RESP array of bulk strings.
    fn cmd(&mut self, args: &[&[u8]]) -> &mut Self {
        let mut out = BytesMut::new();
        generate_bulk_string(args.to_vec(), &mut out);
        self.raw(&out)
    }

    /// Write raw bytes (pipelining / split-command tests).
    fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.write_all(bytes).expect("failed to write");
        self
    }

    /// Read at least `expected.len()` bytes and assert equality.
    fn expect(&mut self, expected: &[u8]) {
        let got = read_at_least(&mut self.0, expected.len(), TIMEOUT);
        assert_eq!(got.as_slice(), expected);
    }

    /// Read at least `prefix.len()` bytes and assert the reply starts with it.
    fn expect_prefix(&mut self, prefix: &[u8]) {
        let got = read_at_least(&mut self.0, prefix.len(), TIMEOUT);
        assert!(
            got.starts_with(prefix),
            "expected a reply starting with {prefix:?}, got {got:?}"
        );
    }

    /// Assert nothing arrives within `timeout`.
    fn expect_silent(&mut self, timeout: Duration) {
        assert_silent(&mut self.0, timeout);
    }
}

/// Read until at least `want` bytes have arrived, or `timeout` elapses.
/// A timeout is not an error here: returning what we got lets a failing test
/// show a useful diff instead of hanging forever.
fn read_at_least(stream: &mut TcpStream, want: usize, timeout: Duration) -> Vec<u8> {
    stream
        .set_read_timeout(Some(timeout))
        .expect("failed to set read timeout");

    let mut got = Vec::new();
    let mut chunk = [0u8; 512];
    while got.len() < want {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => break,
            Err(e) => panic!("read failed: {e}"),
        }
    }
    got
}

/// Assert the server sends nothing at all within `timeout`.
fn assert_silent(stream: &mut TcpStream, timeout: Duration) {
    stream
        .set_read_timeout(Some(timeout))
        .expect("failed to set read timeout");

    let mut chunk = [0u8; 64];
    match stream.read(&mut chunk) {
        Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
        Ok(0) => panic!("server closed the connection"),
        Ok(n) => panic!(
            "server answered an incomplete command with {:?}",
            String::from_utf8_lossy(&chunk[..n])
        ),
        Err(e) => panic!("read failed: {e}"),
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

/// LPOP takes exactly one argument for now; extra args are an error.
#[test]
fn lpop_with_extra_args_returns_error() {
    let mut t = TestCtx::new();
    t.client
        .cmd(&[b"LPOP", b"k", b"extra"])
        .expect(b"-ERR wrong number of arguments for 'lpop' command\r\n");
}