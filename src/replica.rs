use std::{sync::{Arc, Mutex}};

use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc};

use crate::{client::Conn, db::Store, dispatch::execute, resp::{Decoder, Frame::{self}}};

pub async fn handshake(host: String, port: u16, own_port: u16, store: Arc<Mutex<Store>>) -> Option<()> {
    let addr = format!("{host}:{port}");
    let Ok(mut stream) = TcpStream::connect(&addr).await else {
        eprintln!("replica: cannot connect to master {addr}");
        return None;
    };

    send_and_read_reply(&mut stream, Frame::array(vec!["PING"])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["REPLCONF", "listening-port", &own_port.to_string()])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["REPLCONF", "capa", "psync2"])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["PSYNC", "?", "-1"])).await?; //+FULLRESYNC

    // read rdb
    let len_raw = read_line(&mut stream).await?; //$len\r\n
    let len:usize = str::from_utf8(&len_raw[1..]).ok()?.trim().parse().ok()?;
    let mut rdb = vec![0u8; len];
    stream.read_exact(&mut rdb).await.ok()?;

    // keep connection alive
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut conn = Conn::new(0, tx);
    conn.from_master = true;
    let mut decoder = Decoder::new();
    let mut chunk = [0u8; 1024];
    let mut offset: usize = 0;
    loop {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => {
                decoder.extend(&chunk[..n]);
                while let Some(frame) = decoder.parse() {
                    let len = &frame.frame_len();
                    if is_getack(&frame) {
                        let mut buf = BytesMut::new();
                        Frame::array(vec!["REPLCONF","ACK", &offset.to_string()]).encode(&mut buf);
                        if stream.write_all(&buf).await.is_err() {
                            return None
                        }
                        offset += len;
                    } else {
                        let mut guard = store.lock().unwrap_or_else(|e|e.into_inner());
                        let _ = execute(&frame, &mut conn, &mut guard);
                        offset += len;
                    }
                }
            },
        }
    }
}

async fn read_line(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let mut byte = [0u8;1];
    loop {
        match stream.read(&mut byte).await {
            Ok(0) | Err(_) => return None,
            Ok(_) => {
                line.push(byte[0]);
                if line.ends_with(b"\r\n") {
                    return Some(line)
                }
            }
        }
    }
}

async fn send_and_read_reply(stream: &mut TcpStream, frame: Frame) -> Option<Vec<u8>> {
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    stream.write_all(&buf).await.ok()?;
    read_line(stream).await
}

fn is_getack(frame: &Frame) -> bool {
    let Frame::Array(args) = frame else {
        return false
    };
    args.len() == 3 
    && args[0].as_bulk().is_some_and(|b| b.eq_ignore_ascii_case(b"replconf"))
    && args[1].as_bulk().is_some_and(|b|b.eq_ignore_ascii_case(b"getack"))
}

