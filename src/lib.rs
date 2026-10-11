mod resp;
mod db;
mod client;
mod connection;
mod util;
mod commands;
mod error;
mod dispatch;

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::{
    io::AsyncWriteExt, net::TcpListener,
    sync::mpsc
};

use resp::{Frame};
use db::{Store};
use connection::StreamHandler;

pub async fn run(port: u16) {
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let store = Arc::new(Mutex::new(Store::new()));
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };

        let (rd, mut wr) = tokio::io::split(stream);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Frame>();
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            while let Some(frame) = out_rx.recv().await {
                buf.clear();
                frame.encode(&mut buf);
                if wr.write_all(&buf).await.is_err() {
                    break;
                }
            }
        });

        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            let mut handler = StreamHandler::new(rd, store_clone, out_tx);
            while let Some(val) = handler.next().await {
                if handler.respond(val).await.is_err() {
                    break;
                }
            }
        });
    }
}