use crate::{client::Conn, commands::CmdResult, db::Store, error::RedisError, resp::Frame};


pub fn cmd_multi(args: &[Frame], conn:&mut Conn, _store:&mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("multi"))
    }
    conn.multi = Some(Vec::new());
    Ok(vec![Frame::simple("OK")])
}

pub fn cmd_exec(args: &[Frame], conn:&mut Conn, _store:&mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("exec"))
    }
    if conn.multi.is_none() {
        return Err(RedisError::exec_without_multi())
    }
    Ok(vec![Frame::Array(Vec::new())])
}