mod resp;
mod db;
mod client;
mod connection;
mod util;
mod commands;
mod error;
mod dispatch;
mod replica;
mod rdb;

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::{
    io::AsyncWriteExt, net::TcpListener,
    sync::mpsc
};

use resp::{Frame};
use db::{Store, Role};
use connection::StreamHandler;

pub struct Config {
    pub port: u16,
    pub replicaof: Option<(String, u16)>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}


pub async fn run(config: Config) {
    let role = match &config.replicaof {
        Some((h, p)) => Role::Replica { host: h.clone(), port:*p },
        None => Role::Master
    };
    let dir = config.dir.clone().unwrap_or(".".to_string());
    let dbfilename = config.dbfilename.clone().unwrap_or("dump.rdb".to_string());
    let mut store = Store::new(role, config.dir, config.dbfilename);
    let path = format!("{}/{}", dir, dbfilename);
    if let Ok(bytes) = std::fs::read(&path) {
        rdb::load(&bytes, &mut store);
    }

    let store = Arc::new(Mutex::new(store));

    // if we're replica, connect to master with PING
    if let Some((host, master_port)) = config.replicaof {
        tokio::spawn(replica::handshake(host, master_port, config.port, store.clone()));
    }

    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
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