#![allow(unused_imports)]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (mut s, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut chunk = [0; 512];
            loop {
                let n = match s.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n
                };
       
                buf.extend_from_slice(&chunk[..n]);

                // A single read can carry several commands, and a command
                // can straddle two reads. Drain every command the buffer
                // now holds, and leave any trailing partial one for later.
                while let Some((commands, len)) = parse_command(&buf) {
                    buf.drain(..len);
                    
                    if commands.is_empty(){
                        continue;
                    }
                    let cmd = commands[0].as_slice();
                    if cmd == b"PING" { // simple string
                        if s.write_all(b"+PONG\r\n").await.is_err() {
                            return;
                        }
                    } else if cmd == b"ECHO" { // bulk string
                        if commands.len() < 2 {
                            if s.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n").await.is_err() {
                                return;
                            }
                            continue; // continue other commands
                        }
                        let echo_args = commands[1].as_slice();
                        // format *n\r\n$len\r\n{args}\r\n
                        let mut res = Vec::new();
                        res.extend_from_slice(format!("${}\r\n", echo_args.len()).as_bytes());
                        res.extend_from_slice(echo_args);
                        res.extend_from_slice("\r\n".as_bytes());
                        if s.write_all(&res).await.is_err() {
                            return;
                        }
                    } else if cmd == b"COMMAND" {
                        if s.write_all(b"+PONG\r\n").await.is_err() {
                            return;
                        }
                    } else {
                        if s.write_all(format!("-ERR wrong command {}\r\n", String::from_utf8_lossy(cmd)).as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }
}

/// Length in bytes of the complete RESP command at the front of `buf`, or
/// `None` if `buf` doesn't yet hold a whole one.
///
/// Recognises an array of bulk strings, which is what real clients send:
/// `*<argc>\r\n` followed by `argc` pairs of `$<len>\r\n<bytes>\r\n`.
fn parse_command(buf: &[u8]) -> Option<(Vec<Vec<u8>>,usize)> {
    let mut pos = 0;
    let argc = parse_header(buf, &mut pos, b'*')?;

    let mut results = Vec::new();
    for _ in 0..argc {
        let len = parse_header(buf, &mut pos, b'$')?;
        results.push(buf.get(pos..pos+len)?.to_vec());
        pos = pos.checked_add(len)?;
        if buf.get(pos..pos + 2)? != b"\r\n" {
            return None;
        }
        pos += 2;
    }

    Some((results, pos))
}

/// Read a `<marker><number>\r\n` header, advancing `pos` past it.
fn parse_header(buf: &[u8], pos: &mut usize, marker: u8) -> Option<usize> {
    if *buf.get(*pos)? != marker {
        return None;
    }
    let start = *pos + 1;
    let end = start + buf.get(start..)?.windows(2).position(|w| w == b"\r\n")?;
    let number = std::str::from_utf8(buf.get(start..end)?)
        .ok()?
        .parse()
        .ok()?;
    *pos = end + 2;
    Some(number)
}
