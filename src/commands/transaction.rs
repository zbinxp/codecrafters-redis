use crate::{client::Conn, commands::CmdResult, db::Store, dispatch::execute, error::RedisError, resp::Frame};

pub fn cmd_multi(args: &[Frame], conn:&mut Conn, _store:&mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("multi"))
    }
    conn.multi = Some(Vec::new());
    Ok(vec![Frame::simple("OK")])
}

pub fn cmd_exec(args: &[Frame], conn:&mut Conn, store:&mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("exec"))
    }
    let Some(queue) = conn.multi.take() else {
        return Err(RedisError::exec_without_multi())
    };

    let mut out = Vec::new();
    // execute commands in queue
    for frame in queue {
        match execute(&frame, conn, store) {
            Ok(results) => out.extend(results),
            Err(e) => out.push(Frame::Err(e.message))
        }
    }
    Ok(vec![Frame::Array(out)])
}

pub fn cmd_discard(args: &[Frame], conn:&mut Conn, _store:&mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("discard"))
    }
    if conn.multi.take().is_none() {
        return Err(RedisError::discard_without_multi())
    }
    Ok(vec![Frame::simple("OK")])
}