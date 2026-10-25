use std::{sync::{Arc, Mutex}, time::{Duration, Instant}};

use tokio::time::sleep;

use crate::{client::Conn, commands::{CmdResult, bulk_at}, db::{Role, Store}, error::RedisError, resp::Frame, util};

pub const EMPTY_RDB: &[u8] =
 b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2";

pub fn cmd_info(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let section = args.get(1).and_then(Frame::as_bulk);
    let mut out = String::new();
    if section.is_none() || section.is_some_and(|s|s.eq_ignore_ascii_case(b"replication")) {
        // # Replication section
        out.push_str("# Replication\r\n");
        let role = match &store.role {
            Role::Master => "role:master",
            Role::Replica { .. } => "role:slave",
        };
        out.push_str(role);
        out.push_str("\r\n");
        out.push_str(&format!("master_replid:{}\r\n", store.master_replid));
        out.push_str(&format!("master_repl_offset:{}\r\n", store.master_repl_offset));

        if let Role::Replica { host, port } = &store.role {
            out.push_str(&format!("master_host:{}\r\n", host));
            out.push_str(&format!("master_port:{}\r\n", port));
        }
    }

    Ok(vec![Frame::bulk(out)])
}

pub fn cmd_replconf(args: &[Frame], conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.get(1).and_then(Frame::as_bulk).is_some_and(|b|b.eq_ignore_ascii_case(b"ACK")) {
        if let Some(offset) = args.get(2).and_then(Frame::as_bulk)
            .and_then(|b|util::parse::<u64>(b)) {
            store.replic_acks.insert(conn.id, offset);
        }
        // no need to reply
        return Ok(vec![])
    }

    Ok(vec![Frame::simple("OK")])
}

pub fn cmd_psync(_args: &[Frame], conn: &mut Conn, store: &mut Store) -> CmdResult {
    store.replicas.insert(conn.id, conn.out_tx.clone());
    let reply = format!("FULLRESYNC {} {}", store.master_replid, store.master_repl_offset);
    Ok(vec![
        Frame::simple(reply),
        Frame::Rdb(EMPTY_RDB.to_vec())
    ])
}

pub fn cmd_config(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    // CONFIG GET <param>
    let sub = bulk_at(args, 1).unwrap_or(b"");
    if !sub.eq_ignore_ascii_case(b"GET") {
        return Err(RedisError::wrong_args("config"))
    }
    let param = bulk_at(args, 2).ok_or_else(||RedisError::wrong_args("config"))?;
    let value = if param.eq_ignore_ascii_case(b"dir") {
        store.dir.clone()
    } else if param.eq_ignore_ascii_case(b"dbfilename") {
        store.dbfilename.clone()
    } else if param.eq_ignore_ascii_case(b"appendonly") {
        store.appendonly.clone()
    } else if param.eq_ignore_ascii_case(b"appenddirname") {
        store.appenddirname.clone()
    } else if param.eq_ignore_ascii_case(b"appendfilename") {
        store.appendfilename.clone()
    } else if param.eq_ignore_ascii_case(b"appendfsync") {
        store.appendfsync.clone()
    } else {
        return Ok(vec![Frame::Array(Vec::new())])
    };
    Ok(vec![Frame::Array(vec![Frame::bulk(param), Frame::bulk(value)])])
}

pub async fn cmd_wait(args: &[Frame], store: Arc<Mutex<Store>>) -> Result<Frame, RedisError> {
    let args1 = bulk_at(args, 1).ok_or(RedisError::wrong_args("wait"))?;
    let args2 = bulk_at(args, 2).ok_or(RedisError::wrong_args("wait"))?;
    let numreplicas: usize = util::parse(args1).ok_or(RedisError::not_integer())?;
    let timeout_ms: u64 = util::parse(args2).ok_or(RedisError::timeout_not_integer())?;
    // replicas should be guarded
    let (target, out_txs) = {
        let s = store.lock().unwrap_or_else(|e|e.into_inner());
        (s.master_repl_offset, s.replicas.values().cloned().collect::<Vec<_>>())
    };
    // no write command, return active replicas count
    if target == 0 || numreplicas == 0 {
        return Ok(Frame::Int(out_txs.len() as i64))
    }

    // ask replicas to ACK
    let getack = Frame::array(vec!["REPLCONF", "GETACK", "*"]);
    for out in out_txs {
        let _ = out.send(getack.clone());
    }

    // wait and get replicas' ack
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let acked = {
            let s = store.lock().unwrap_or_else(|e|e.into_inner());
            s.replic_acks.values().filter(|&&offset| offset >= target).count()
        };
        if acked >= numreplicas || Instant::now() >= deadline {
            return Ok(Frame::Int(acked as i64))
        }
        sleep(Duration::from_millis(10)).await;
    }

}

pub fn cmd_keys(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let _pattern = bulk_at(args, 1).ok_or_else(|| RedisError::wrong_args("keys"))?;
    // for now, only support "*"
    let keys = store.data.keys().map(|k| Frame::bulk(k)).collect();
    Ok(vec![Frame::Array(keys)])
}