use crate::{client::Conn, commands::{CmdResult, bulk_at}, db::Store, error::RedisError, resp::Frame};


pub fn cmd_acl(args: &[Frame], conn: &mut Conn, store: &mut Store) -> CmdResult {
    let cmd = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("acl"))?;
    match cmd {
        c if c.eq_ignore_ascii_case(b"WHOAMI") => {
            if args.len() > 2 {
                return Err(RedisError::unknown_acl_subcommand("WHOAMI"))
            }
            let user_name = conn.user.clone().unwrap_or_else(||"default".to_string());
            Ok(vec![Frame::bulk(user_name)])
        },
        c if c.eq_ignore_ascii_case(b"GETUSER") => {
            if args.len() != 3 {
                return Err(RedisError::unknown_acl_subcommand("GETUSER"))
            }
            let name = bulk_at(args, 2).ok_or_else(||RedisError::unknown_acl_subcommand("GETUSER"))?;
            let Ok(name) = String::from_utf8(name.to_vec()) else {
                return Ok(vec![Frame::Null])
            };
            let Some(_user) = store.users.get(&name) else {
                return Ok(vec![Frame::Null])
            };
            Ok(vec![Frame::Array(vec![Frame::bulk("flags"), Frame::Array(Vec::new())])])
        },
        _ => Err(RedisError::unknown_acl_subcommand(str::from_utf8(cmd).unwrap_or_default()))
    }
}