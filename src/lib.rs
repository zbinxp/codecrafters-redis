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

use std::{collections::HashSet, fs, io, path::PathBuf, sync::{Arc, Mutex}};

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
        let manifest_path = format!("{}/{}.manifest", aof_dir, store.appendfilename);
        if let Err(e) = std::fs::create_dir_all(&aof_dir) {
            eprintln!("failed to create AOF directory {aof_dir}: {e}");
        }
        
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

        // cleanup temp files of AOF 
        if let Err(e) = cleanup_orphan(&aof_dir, &manifest_path) {
            eprintln!("failed to clean orphan files of AOF: {e}");
        }

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

fn cleanup_orphan(aof_dir: &str, manifest_path: &str) -> io::Result<usize> {
    // `.tmp` files；files (`*.base.*` / `*.incr.*`) not contained in manifest
    let dir = PathBuf::from(aof_dir);
    if !dir.is_dir() {
        return Ok(0)
    }
    let content = fs::read_to_string(manifest_path)?;
    let keep_files = aof::parse_manifest(&content).unwrap_or_default();
    let mut keep_set = HashSet::new();
    keep_set.extend(keep_files);
    
    if let Some(name) = PathBuf::from(manifest_path).file_name().and_then(|s|s.to_str()) {
        keep_set.insert(name.to_string());
    }

    let mut targets = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        let file_name = path.file_name().and_then(|s|s.to_str()).unwrap_or_default();
        let is_aof_artifact = file_name.ends_with(".tmp") || file_name.contains(".base.") || file_name.contains(".incr.");
        if is_aof_artifact && !keep_set.contains(file_name) {
            targets.push(path);
        }
    }

    let mut deleted = 0;
    for path in targets {
        match fs::remove_file(&path) {
            Ok(_) => deleted += 1,
            Err(e) => eprintln!("failed to delete file {} : {}", path.display(), e),
        };
    }
    Ok(deleted)
}
