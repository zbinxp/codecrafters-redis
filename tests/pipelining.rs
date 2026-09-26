//! Black-box tests for the server: each test spawns the real binary and talks
//! to it over a raw `TcpStream`, so we control exactly how the bytes are
//! chunked and can assert on the raw RESP the client would receive.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};

const ADDR: &str = "127.0.0.1:6379";

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
        // The tests serialise themselves with `lock_server()`, but a stale
        // server (e.g. one left over from `./your_program.sh`) can still be
        // holding the port. If it is, the child we spawn below dies with
        // `AddrInUse` while `TcpStream::connect` keeps succeeding -- so the
        // test would silently talk to the stale binary instead of the one we
        // just built. Refuse to run in that case.
        if TcpStream::connect(ADDR).is_ok() {
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
            if TcpStream::connect(ADDR).is_ok() {
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

const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const ECHO: &[u8] = b"*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n";

#[test]
fn responds_to_two_pings_sent_in_a_single_write() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut client = TcpStream::connect(ADDR).expect("failed to connect");

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(PING);
    pipeline.extend_from_slice(PING);
    client.write_all(&pipeline).expect("failed to write");

    let got = read_at_least(&mut client, 14, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n+PONG\r\n");
}

/// The command the user actually runs: `echo -e "PING\nPING" | redis-cli`.
///
/// This is the exact byte stream that puts on the wire (captured from
/// redis-cli 7.0.15): a `COMMAND DOCS` handshake it issues on connect, then one
/// RESP array per line of stdin, all within a millisecond of each other.
///
/// `COMMAND DOCS` is not really implemented — replying `+PONG` to it is enough
/// for redis-cli, which only uses the reply for tab-completion metadata.
#[test]
fn responds_to_the_byte_stream_redis_cli_pipes_in() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut client = TcpStream::connect(ADDR).expect("failed to connect");

    client
        .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
        .expect("failed to write");
    client.write_all(PING).expect("failed to write");
    client.write_all(PING).expect("failed to write");

    let got = read_at_least(&mut client, 21, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n+PONG\r\n+PONG\r\n");
}

/// TCP is a stream: a command can arrive split across reads. The server must
/// wait for the rest rather than answer a fragment or drop it.
#[test]
fn holds_a_command_split_across_writes_until_it_is_complete() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut client = TcpStream::connect(ADDR).expect("failed to connect");

    // Half a command -- the server must stay quiet rather than guess.
    client.write_all(b"*1\r\n$4\r\nPI").expect("failed to write");
    assert_silent(&mut client, Duration::from_millis(300));

    // The rest arrives, followed by a second whole command.
    client.write_all(b"NG\r\n").expect("failed to write");
    client.write_all(PING).expect("failed to write");

    let got = read_at_least(&mut client, 14, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n+PONG\r\n");
}

#[test]
fn responds_to_multiple_concurrent_pings() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    c1.write_all(PING).expect("failed to write");
    let got1 = read_at_least(&mut c1, 7, Duration::from_secs(2));
    
    let mut c2 = TcpStream::connect(ADDR).expect("failed to connect second");
    c2.write_all(PING).expect("failed to write second");
    let got2 = read_at_least(&mut c2, 7, Duration::from_secs(2));

    // c1 must still be live after c2 was served
    c1.write_all(PING).expect("failed to write");
    let got1_again = read_at_least(&mut c1, 7, Duration::from_secs(2));
    assert_eq!(got1, b"+PONG\r\n");
    assert_eq!(got2, b"+PONG\r\n");
    assert_eq!(got1_again, b"+PONG\r\n");
}

#[test]
fn responds_to_echo_world() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    c1.write_all(ECHO).expect("failed to write");
    let got = read_at_least(&mut c1, 8, Duration::from_secs(2));
    assert_eq!(got, b"$5\r\nworld\r\n");
}

#[test]
fn echo_without_args_keeps_the_connection_open() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    const ECHO: &[u8] = b"*1\r\n$4\r\nECHO\r\n";
    c1.write_all(ECHO).expect("failed to write");
    let got = read_at_least(&mut c1, 4, Duration::from_secs(2));
    assert!(got.starts_with(b"-ERR"));
    c1.write_all(PING).expect("failed to write");
    let got = read_at_least(&mut c1, 7, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n");
}

#[test]
fn empty_command_keeps_the_connection_open() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    let empty = b"*0\r\n";
    c1.write_all(empty).expect("failed to write");
    // no response
    c1.write_all(PING).expect("failed to write");
    let got = read_at_least(&mut c1, 7, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n");
}

#[test]
fn empty_command_combines_normal_returns_ok() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    let mut command = vec![];
    command.extend_from_slice(b"*0\r\n".as_slice());
    command.extend_from_slice(PING);
    c1.write_all(&command).expect("failed to write");
    // no response
    let got = read_at_least(&mut c1, 7, Duration::from_secs(2));
    assert_eq!(got, b"+PONG\r\n");
}

#[test]
fn set_key_value_ok_then_get() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    let mut command = BytesMut::new();
    generate_set_command("hello".as_bytes(), "world".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    let got = read_at_least(&mut c1, 5, Duration::from_secs(2));
    assert_eq!(got, b"+OK\r\n");

    command.clear();
    generate_get_command("hello".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    let got = read_at_least(&mut c1, 11, Duration::from_secs(2));
    assert_eq!(got, b"$5\r\nworld\r\n");
}

/// `GET` on a key that was never set must return the RESP null bulk string,
/// exactly `$-1\r\n` -- not an empty bulk string, and not a bulk string whose
/// content happens to be the literal text `$-1\r\n`.
#[test]
fn get_missing_key_returns_null_bulk_string() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    let mut command = BytesMut::new();
    generate_get_command(b"no-such-key", &mut command);
    c1.write_all(&command).expect("failed to write");

    let got = read_at_least(&mut c1, 5, Duration::from_secs(2));
    assert_eq!(got, b"$-1\r\n");
}

/// The store is shared across connections, not per-connection: a value written
/// by one client must be readable by another.
#[test]
fn set_is_visible_from_another_connection() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut writer = TcpStream::connect(ADDR).expect("failed to connect writer");
    let mut reader = TcpStream::connect(ADDR).expect("failed to connect reader");

    let mut command = BytesMut::new();
    generate_set_command(b"shared-key", b"shared-value", &mut command);
    writer.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut writer, 5, Duration::from_secs(2)),
        b"+OK\r\n"
    );

    command.clear();
    generate_get_command(b"shared-key", &mut command);
    reader.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut reader, 19, Duration::from_secs(2)),
        b"$12\r\nshared-value\r\n"
    );
}

/// A second `SET` on the same key replaces the value rather than leaving the
/// old one in place.
#[test]
fn set_overwrites_previous_value() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    let mut command = BytesMut::new();
    generate_set_command(b"k", b"first", &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(read_at_least(&mut c1, 5, Duration::from_secs(2)), b"+OK\r\n");

    command.clear();
    generate_set_command(b"k", b"second", &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(read_at_least(&mut c1, 5, Duration::from_secs(2)), b"+OK\r\n");

    command.clear();
    generate_get_command(b"k", &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 12, Duration::from_secs(2)),
        b"$6\r\nsecond\r\n"
    );
}

/// Keys and values are binary-safe: an embedded CRLF in a value must survive a
/// round trip untouched rather than being treated as a terminator.
#[test]
fn set_and_get_are_binary_safe() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    let value: &[u8] = b"a\r\nb";

    let mut command = BytesMut::new();
    generate_set_command(b"binary", value, &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(read_at_least(&mut c1, 5, Duration::from_secs(2)), b"+OK\r\n");

    command.clear();
    generate_get_command(b"binary", &mut command);
    c1.write_all(&command).expect("failed to write");

    let mut expected = BytesMut::new();
    put_bulk_string(value, &mut expected);
    assert_eq!(
        read_at_least(&mut c1, expected.len(), Duration::from_secs(2)),
        expected.to_vec()
    );
}

fn generate_get_command(key:&[u8], out: &mut BytesMut) {
    out.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
    put_bulk_string(key, out);
}

fn generate_set_command(key:&[u8], value:&[u8], out: &mut BytesMut) {
    out.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
    put_bulk_string(key, out);
    put_bulk_string(value, out);
}

fn generate_set_with_expire(key: &[u8], value:&[u8], expire:u32, out: &mut BytesMut) {
    out.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
    put_bulk_string(key, out);
    put_bulk_string(value, out);
    out.extend_from_slice(b"$2\r\nPX\r\n");
    put_bulk_string(expire.to_string().as_bytes(), out);
}

fn put_bulk_string(val:&[u8], out: &mut BytesMut) {
    out.put_u8(b'$');
    out.extend_from_slice(val.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

#[test]
fn set_with_expire_then_get_absent() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    let mut command = BytesMut::new();
    generate_set_with_expire("hello".as_bytes(), "world".as_bytes(), 100, &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 5, Duration::from_secs(2)),
        b"+OK\r\n"
    );

    command.clear();
    generate_get_command("hello".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 11, Duration::from_secs(2)), 
        b"$5\r\nworld\r\n"
    );

    thread::sleep(Duration::from_millis(200));
    command.clear();
    generate_get_command("hello".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 5, Duration::from_secs(2)),
        b"$-1\r\n"
    );
}

#[test]
fn set_clear_previous_ttl() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");
    let mut command = BytesMut::new();
    generate_set_with_expire("hello".as_bytes(), "world".as_bytes(), 100, &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 5, Duration::from_secs(2)),
        b"+OK\r\n"
    );

    command.clear();
    generate_set_command("hello".as_bytes(), "test".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 5, Duration::from_secs(2)),
        b"+OK\r\n"
    );

    thread::sleep(Duration::from_millis(200));
    command.clear();
    generate_get_command("hello".as_bytes(), &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 10, Duration::from_secs(2)),
        b"$4\r\ntest\r\n"
    );
}

/// A non-numeric `PX` argument is rejected with an error, and the connection
/// stays usable afterwards.
#[test]
fn set_with_non_integer_px_is_an_error() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    let mut command = BytesMut::new();
    command.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
    put_bulk_string(b"k", &mut command);
    put_bulk_string(b"v", &mut command);
    command.extend_from_slice(b"$2\r\nPX\r\n");
    put_bulk_string(b"not-a-number", &mut command);
    c1.write_all(&command).expect("failed to write");

    let got = read_at_least(&mut c1, 3, Duration::from_secs(2));
    assert!(got.starts_with(b"-ERR"), "expected an error, got {got:?}");

    c1.write_all(PING).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 7, Duration::from_secs(2)),
        b"+PONG\r\n"
    );
}

/// Once a key expires, a later plain `SET` on the same key stores a fresh,
/// non-expiring value.
#[test]
fn expired_key_can_be_set_again() {
    let _guard = lock_server();
    let _server = Server::start();
    let mut c1 = TcpStream::connect(ADDR).expect("failed to connect");

    let mut command = BytesMut::new();
    generate_set_with_expire(b"k", b"old", 100, &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(read_at_least(&mut c1, 5, Duration::from_secs(2)), b"+OK\r\n");

    thread::sleep(Duration::from_millis(200));

    command.clear();
    generate_set_command(b"k", b"new", &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(read_at_least(&mut c1, 5, Duration::from_secs(2)), b"+OK\r\n");

    command.clear();
    generate_get_command(b"k", &mut command);
    c1.write_all(&command).expect("failed to write");
    assert_eq!(
        read_at_least(&mut c1, 9, Duration::from_secs(2)),
        b"$3\r\nnew\r\n"
    );
}