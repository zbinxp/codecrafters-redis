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
mod aof;

use std::{fs, sync::{Arc, Mutex}};

use bytes::BytesMut;
use tokio::{
    io::AsyncWriteExt, net::TcpListener,
    sync::mpsc
};

use resp::{Frame};
use db::{Store, Role};
use connection::StreamHandler;

use crate::{client::Conn, dispatch::execute, resp::Decoder};

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
        let manifest_path = format!("{}/{}.manifest", aof_dir, store.appendfilename);
        // read active aof name
        let names = match fs::read_to_string(&manifest_path) {
            Ok(text) => aof::parse_manifest(&text).unwrap_or_else(|| {
                vec![format!("{}.1.incr.aof", store.appendfilename)]
            }),
            Err(_) => {
                let name = format!("{}.1.incr.aof", store.appendfilename);
                let path = format!("{}/{}", aof_dir, name);
                let _ = fs::OpenOptions::new().create(true).append(true).open(&path);
                let _ = fs::write(&manifest_path, format!("file {} seq 1 type i\n", name));
                vec![name]
            }
        };

        // replay existing aof
        for incr_name in names.iter() {
            let path = format!("{}/{}", aof_dir, incr_name);
            if let Ok(bytes) = fs::read(&path) {
                if incr_name.ends_with(".rdb") {
                    rdb::load(&bytes, &mut store);
                } else {
                    replay_aof(&bytes, &mut store);
                }
            }
        }

        names.last().into_iter().for_each(|incr_name| {
            let aof_path = format!("{}/{}", aof_dir, incr_name);
            match fs::OpenOptions::new().create(true).append(true).open(&aof_path) {
                Ok(f) => store.aof = Some(f),
                Err(e) => eprintln!("failed to open AOF file {aof_path}: {e}"),
            }
        });
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

fn replay_aof(bytes: &[u8], store: &mut Store) {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut conn = Conn::new(0, tx);
    conn.from_master = true;
    conn.replaying = true;
    let mut decoder = Decoder::new();
    decoder.extend(bytes);
    while let Some(frame) = decoder.parse() {
        let _ = execute(&frame, &mut conn, store);
    }
}