use crate::{client::Conn, commands::CmdResult, db::{Role, Store}, resp::Frame};

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

pub fn cmd_replconf(_args: &[Frame], _conn: &mut Conn, _store: &mut Store) -> CmdResult {
    Ok(vec![Frame::simple("OK")])
}

pub fn cmd_psync(_args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let reply = format!("FULLRESYNC {} {}", store.master_replid, store.master_repl_offset);
    Ok(vec![Frame::simple(reply)])
}
