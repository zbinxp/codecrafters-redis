use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::resp::{Frame};

pub async fn handshake(host: String, port: u16, own_port: u16) -> Option<()> {
    let addr = format!("{host}:{port}");
    let Ok(mut stream) = TcpStream::connect(&addr).await else {
        eprintln!("replica: cannot connect to master {addr}");
        return None;
    };

    send_and_read_reply(&mut stream, Frame::array(vec!["PING"])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["REPLCONF", "listening-port", &own_port.to_string()])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["REPLCONF", "capa", "psync2"])).await?;
    send_and_read_reply(&mut stream, Frame::array(vec!["PSYNC", "?", "-1"])).await?;

    // keep connection alive
    let mut chunk = [0u8; 1024];
    loop {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(_n) => {
                
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
