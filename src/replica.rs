use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::resp::{Frame};

pub async fn handshake(host: String, port: u16, own_port: u16) -> Option<()> {
    let addr = format!("{host}:{port}");
    let Ok(mut stream) = TcpStream::connect(&addr).await else {
        eprintln!("replica: cannot connect to master {addr}");
        return None;
    };
    let mut buf = BytesMut::new();
    Frame::array(vec!["PING"]).encode(&mut buf);
    stream.write_all(&buf).await.ok()?;
    read_line(&mut stream).await?; //+PONG

    buf.clear();
    Frame::array(vec![
        "REPLCONF", "listening-port", &own_port.to_string()
    ]).encode(&mut buf);
    stream.write_all(&buf).await.ok()?;
    read_line(&mut stream).await?; //+OK

    buf.clear();
    Frame::array(vec!["REPLCONF", "capa", "psync2"]).encode(&mut buf);
    stream.write_all(&buf).await.ok()?;
    read_line(&mut stream).await?;

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