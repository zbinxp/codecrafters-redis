use crate::{client::Conn, commands::{CmdResult, bulk_at}, db::{Store, User}, error::RedisError, resp::Frame};
use sha2::{Digest, Sha256};

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
            let Some(user) = store.users.get(&name) else {
                return Ok(vec![Frame::Null])
            };
            let properties = acl_getuser_reply(user);
            Ok(vec![properties])
        },
        c if c.eq_ignore_ascii_case(b"SETUSER") => {
            let name = bulk_at(args, 2).ok_or_else(||RedisError::unknown_acl_subcommand("SETUSER"))?;
            let Ok(name) = String::from_utf8(name.to_vec()) else {
                return Ok(vec![Frame::Null])
            };

            // Work on a copy and commit only at the end: an invalid rule must
            // leave the user exactly as it was, never half-applied (a rotation
            // that adds the new password and then hits a typo would otherwise
            // leave a credential nobody expects).
            let mut user = match store.users.get(&name) {
                Some(existing) => existing.clone(),
                // ACL SETUSER creates the user when it does not exist yet.
                None => User {
                    name: name.clone(),
                    enabled: false,
                    nopass: false,
                    passwords: Vec::new(),
                },
            };

            for rule in &args[3..] {
                let r = rule.as_bulk().ok_or_else(||RedisError::unknown_acl_subcommand("SETUSER"))?;
                match r.split_first() {
                    Some((b'>', pass)) => {
                        user.passwords.push(sha256_hex(pass));
                        user.nopass = false;
                    },
                    Some((b'<', pass)) => user.passwords.retain(|h| h != &sha256_hex(pass)),
                    Some((b'#', hex)) => {
                        user.passwords.push(to_ascii_lowercase_str(hex));
                        user.nopass = false;
                    },
                    Some((b'!', hex)) => user.passwords.retain(|h| h != &to_ascii_lowercase_str(hex)),
                    _ if r.eq_ignore_ascii_case(b"nopass") => {
                        user.nopass = true;
                        user.passwords.clear();
                    }
                    _ if r.eq_ignore_ascii_case(b"resetpass") => {
                        user.passwords.clear();
                        user.nopass = false;
                    }
                    _ if r.eq_ignore_ascii_case(b"on") => user.enabled = true,
                    _ if r.eq_ignore_ascii_case(b"off") => user.enabled = false,
                    _ => return Err(RedisError::acl_setuser_syntax(r)),
                }
            }

            store.users.insert(name, user);
            Ok(vec![Frame::simple("OK")])
        },
        _ => Err(RedisError::unknown_acl_subcommand(str::from_utf8(cmd).unwrap_or_default()))
    }
}

fn acl_getuser_reply(user: &User) -> Frame {
    let mut flags = Vec::new();
    if user.nopass {
        flags.push(Frame::bulk("nopass"));
    }
    let passwords = user.passwords.iter().map(Frame::bulk).collect::<Vec<Frame>>();
    
    Frame::Array(vec![
        Frame::bulk("flags"), Frame::Array(flags),
        Frame::bulk("passwords"), Frame::Array(passwords)
    ])
}

fn to_ascii_lowercase_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_ascii_lowercase()
}


fn sha256_hex(password: &[u8]) -> String {
    let digest = Sha256::digest(password);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

