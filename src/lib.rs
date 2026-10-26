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
    pub appendonly: Option<String>,
    pub appenddirname: Option<String>,
    pub appendfilename: Option<String>,
    pub appendfsync: Option<String>,
}


pub async fn run(config: Config) {
    let role = match &config.replicaof {
        Some((h, p)) => Role::Replica { host: h.clone(), port:*p },
        None => Role::Master
    };
    let mut store = Store::new(role, config.dir, config.dbfilename);
    if let Some(v) = config.appendonly {
        store.appendonly = v;
    }
    if let Some(v) = config.appenddirname {
        store.appenddirname = v;
    }
    if let Some(v) = config.appendfilename {
        store.appendfilename = v;
    }
    if let Some(v) = config.appendfsync {
        store.appendfsync = v;
    }
    let path = format!("{}/{}", store.dir, store.dbfilename);
    if let Ok(bytes) = std::fs::read(&path) {
        rdb::load(&bytes, &mut store);
    }

    if store.appendonly.eq_ignore_ascii_case("yes") {
        let aof_dir = format!("{}/{}", store.dir, store.appenddirname);
        if let Err(e) = std::fs::create_dir_all(&aof_dir) {
            eprintln!("failed to create AOF directory {aof_dir}: {e}");
        }
        let aof_path = format!("{}/{}.1.incr.aof", aof_dir, store.appendfilename);
        if let Err(e) = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&aof_path)
        {
            eprintln!("failed to create AOF file {aof_path}: {e}");
        }
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