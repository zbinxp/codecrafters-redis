use std::{collections::HashMap, fs::{self, File}, io::{self, Write}, path::PathBuf, sync::atomic::Ordering, time::Instant};

use bytes::BytesMut;

use crate::{client::Conn, commands::CmdResult, db::{Entry, Store, Value}, error::RedisError, resp::Frame, util};

/// BGREWRITEAOF
pub fn cmd_bgrewriteaof(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    if args.len() != 1 {
        return Err(RedisError::wrong_args("bgrewriteaof"));
    }

    // avoid concurrent aof rewrite
    if store.aof_rewrite_in_progress.load(Ordering::SeqCst) {
        return Ok(vec![Frame::Err("ERR Background append only file rewriting already in progress".to_string())])
    }
    store.aof_rewrite_in_progress.store(true, Ordering::SeqCst);

    let aof_dir = format!("{}/{}", store.dir, store.appenddirname);
    let manifest_path = format!("{}/{}.manifest", aof_dir, store.appendfilename);

    // Files referenced by the previous manifest, plus the next rewrite generation.
    let content = fs::read_to_string(&manifest_path).unwrap_or_default();
    let file_names: Vec<String> = parse_manifest_all(&content)
        .map(|v| v.into_iter().map(|(_, _, name)| name).collect())
        .unwrap_or_default();
    let seq = parse_manifest_max_seq(&content).unwrap_or(0) + 1;

    // The base and its incr share the same seq.
    let incr_name = format!("{}.{}.incr.aof", store.appendfilename, seq);
    let base_name = format!("{}.{}.base.aof", store.appendfilename, seq);

    // 1. Switch to the new incr *before* taking the snapshot: every write from
    //    this point on lands in the new incr.
    let incr_path = format!("{}/{}", aof_dir, incr_name);
    match fs::OpenOptions::new().create(true).append(true).open(&incr_path) {
        Ok(f) => store.aof = Some(f),
        Err(e) => eprintln!("failed to open AOF file {incr_path}: {e}"),
    }

    // 2. Owned snapshot; no borrow of `store` is held below.
    let snapshot = store.data.clone();
    let flag = store.aof_rewrite_in_progress.clone();
    let ok_flag = store.aof_last_bgrewrite_ok.clone();

    tokio::task::spawn_blocking(move || {
        let result = write_snapshot_to_disk(
            snapshot,
            aof_dir,
            &manifest_path,
            file_names,
            base_name,
            incr_name,
            seq,
        );
        if let Err(e) = &result {
            eprintln!("failed to write snapshot to disk: {e}");
        }
        ok_flag.store(result.is_ok(), Ordering::SeqCst);
        flag.store(false, Ordering::SeqCst);
    });

    Ok(vec![Frame::simple(
        "Background append only file rewriting started",
    )])
}

/// (base size, current incr size) of the active AOF files.
pub fn aof_sizes(store: &Store) -> (u64, u64) {
    let aof_dir = format!("{}/{}", store.dir, store.appenddirname);
    let manifest_path = format!("{}/{}.manifest", aof_dir, store.appendfilename);
    let content = fs::read_to_string(manifest_path).unwrap_or_default();
    let entries = parse_manifest_all(&content).unwrap_or_default();
    let mut base = 0;
    let mut current = 0;
    for (kind, _, name) in &entries {
        let size = fs::metadata(format!("{}/{}", aof_dir, name))
            .map(|m| m.len())
            .unwrap_or(0);
        if kind == "b" {
            base = size;
        } else {
            current = size;
        }
    }
    (base, current)
}

/// Whether the AOF has grown enough to trigger an automatic rewrite.
pub fn should_auto_rewrite(store: &Store) -> bool {
    if !store.appendonly.eq_ignore_ascii_case("yes") {
        return false;
    }
    if store.aof_rewrite_in_progress.load(Ordering::SeqCst) {
        return false;
    }
    let (base, current) = aof_sizes(store);
    if current == 0 || current <= store.auto_aof_rewrite_min_size {
        return false;
    }
    if base == 0 {
        return true; // first rewrite
    }
    current > base
        && current.saturating_sub(base).saturating_mul(100)
            >= base.saturating_mul(store.auto_aof_rewrite_percentage)
}

fn write_snapshot_to_disk(
    snapshot: HashMap<Vec<u8>, Entry>, 
    aof_dir:String, 
    manifest_path: &str,
    file_names: Vec<String>,
    base_name:String, 
    incr_name:String, 
    seq:u64) -> io::Result<()> {
    // 3. Write the base: tmp -> fsync -> rename.
    let bytes = serialize_data(&snapshot);
    let base_tmp = format!("{}/{}.tmp", aof_dir, base_name);
    let base_path = format!("{}/{}", aof_dir, base_name);
    match File::create(&base_tmp) {
        Ok(mut f) => {
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        Err(e) => return Err(e),
    }
    fs::rename(&base_tmp, &base_path)?;

    // 4. Atomically switch the manifest (temp -> fsync -> rename).
    let manifest_ok = match atomic_write_manifest(&aof_dir, &manifest_path, &base_name, &incr_name, seq) {
        Ok(()) => true,
        Err(e) => {
            eprintln!("failed to write manifest {manifest_path}: {e}");
            false
        }
    };

    // 5. Only delete the old files once the manifest points at the new ones;
    //    otherwise we would leave a manifest referencing missing files.
    if manifest_ok {
        for name in &file_names {
            let path = format!("{}/{}", aof_dir, name);
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn atomic_write_manifest(
    aof_dir: &str,
    manifest_path: &str,
    base_name: &str,
    incr_name: &str,
    seq: u64,
) -> io::Result<()> {
    let manifest_tmp = PathBuf::from(aof_dir).join("manifest.tmp");
    let manifest_path = PathBuf::from(manifest_path);
    {
        let mut f = File::create(&manifest_tmp)?;
        let content = format!(
            "file {} seq {} type b\nfile {} seq {} type i\n",
            base_name, seq, incr_name, seq
        );
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(manifest_tmp, manifest_path)?;
    Ok(())
}

fn parse_manifest_all(manifest: &str) -> Option<Vec<(String, u64, String)>> {
    let mut ma: Vec<(&str, u64, &str)> = Vec::new();
    for line in manifest.lines() {
        let t: Vec<&str> = line.trim().split_whitespace().collect();
        if t.len() < 6 || t[0] != "file" {
            continue;
        }
        let seq:u64 = match t[3].parse() {
            Ok(s) => s,
            Err(_) => continue,
        };
        ma.push((t[5], seq, t[1]))
    }
    if ma.is_empty() {
        None
    } else {
        ma.sort_by_key(|(kind,seq,_)| (*kind, *seq));
        if let Some(i) = ma.partition_point(|m|m.0 == "b").checked_sub(1) {
            ma.drain(..i);
        }
        // Own the strings: the caller keeps this vec after `manifest` (and the
        // `content` String it borrows) is dropped.
        Some(
            ma.into_iter()
                .map(|(kind, seq, name)| (kind.to_string(), seq, name.to_string()))
                .collect(),
        )
    }
}

/// Highest `seq` appearing anywhere in the manifest (None when empty/invalid).
fn parse_manifest_max_seq(manifest: &str) -> Option<u64> {
    manifest
        .lines()
        .filter_map(|line| {
            let t: Vec<&str> = line.split_whitespace().collect();
            if t.len() < 6 || t[0] != "file" {
                return None;
            }
            t[3].parse::<u64>().ok()
        })
        .max()
}

/// parse manifest into ordered file list
/// format:
/// file appendonly.aof.2.base.aof  seq 2 type b
/// file appendonly.aof.2.incr.aof seq 2 type i
pub fn parse_manifest(manifest: &str) -> Option<Vec<String>> {
    parse_manifest_all(manifest).map(|v| v.into_iter().map(|(_, _, name)| name).collect())
}

#[allow(dead_code)]
pub fn serialize_dataset(store: &Store) -> Vec<u8> {
    serialize_data(&store.data)
}

fn serialize_data(data: &HashMap<Vec<u8>, Entry>) -> Vec<u8> {
    let mut buf = BytesMut::new();
    for (key,_entry) in data.iter() {
        let Some(entry) = Store::readonly_entry(data, key) else {
            continue;
        };
        match &entry.value {
            Value::String(f) => {
                let mut data = vec![b"SET".to_vec(), key.to_vec(), f.to_vec()];
                data.extend(serialize_expires_field(entry.expires_at));
                let frame = Frame::array(data);
                frame.encode(&mut buf);
            },
            Value::List(deq) => {
                let mut data = vec![b"RPUSH".to_vec(), key.to_vec(),];
                for item in deq {
                    data.push(item.to_vec());
                }
                Frame::array(data).encode(&mut buf);
                if let Some(expire_at) = entry.expires_at {
                    append_expire_at(key, expire_at, &mut buf);
                }
            },
            Value::ZSet(tree) => {
                for (sc, member) in tree {
                    let mut data = vec![b"ZADD".to_vec(), key.to_vec(),];
                    data.push(sc.0.to_string().into_bytes());
                    data.push(member.to_vec());
                    Frame::array(data).encode(&mut buf);
                }
                if let Some(expire_at) = entry.expires_at {
                    append_expire_at(key, expire_at, &mut buf);
                }
            },
            Value::Stream(stream) => {
                for (sid, pairs) in stream {
                    let mut data = vec![b"XADD".to_vec(), key.to_vec(), sid.render().into_bytes()];

                    for (f,v) in pairs {
                        data.push(f.to_vec());
                        data.push(v.to_vec());
                    }
                    Frame::array(data).encode(&mut buf);
                }
                if let Some(expire_at) = entry.expires_at {
                    append_expire_at(key, expire_at, &mut buf);
                }
            },
            
        }
    }
    buf.to_vec()
}

fn serialize_expires_field(expire_at:Option<Instant>) -> Vec<Vec<u8>> {
    let mut data = vec![];
    if let Some(expire) = expire_at {
        if expire > Instant::now() {
            data.push(b"PXAT".to_vec());
            data.push(util::instant_to_unix_ms(expire).to_string().into_bytes())
        }
    }
    data
}

fn append_expire_at(key: &[u8], expire_at: Instant, buf: &mut BytesMut) {
    let data = vec![b"PEXPIREAT".to_vec(), key.to_vec(), 
        util::instant_to_unix_ms(expire_at).to_string().into_bytes()];
    Frame::array(data).encode(buf);
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Entry, Role, Score, StreamId};
    use crate::resp::Decoder;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::time::Duration;

    #[test]
    fn parse_manifest_returns_correct_order() {
        let content = "file appendonly.aof.2.base.aof  seq 2 type b
   file appendonly.aof.2.incr.aof seq 2 type i
   file appendonly.aof.1.incr.aof seq 1 type i";
        let res = parse_manifest(content);
        assert!(res.is_some());

        assert_eq!(res.unwrap(), vec!["appendonly.aof.2.base.aof","appendonly.aof.1.incr.aof","appendonly.aof.2.incr.aof"])
    }

    #[test]
    fn parse_manifest_returns_only_base_with_last_seq() {
        let content = "file appendonly.aof.2.base.aof  seq 2 type b
   file appendonly.aof.1.base.aof seq 1 type b
   file appendonly.aof.1.incr.aof seq 1 type i";
        let res = parse_manifest(content);
        assert!(res.is_some());

        assert_eq!(res.unwrap(), vec!["appendonly.aof.2.base.aof","appendonly.aof.1.incr.aof"])
    }

    // --- ar3: serialize_dataset (strings) ---------------------------------

    fn empty_store() -> Store {
        Store::new(Role::Master, Some(".".into()), Some("dump.rdb".into()))
    }

    fn decode_all(bytes: &[u8]) -> Vec<Frame> {
        let mut dec = Decoder::new();
        dec.extend(bytes);
        let mut out = Vec::new();
        while let Some(f) = dec.parse() {
            out.push(f);
        }
        out
    }

    #[test]
    fn serialize_empty_dataset_is_empty() {
        assert!(serialize_dataset(&empty_store()).is_empty());
    }

    #[test]
    fn serialize_string_emits_set() {
        let mut store = empty_store();
        store.data.insert(
            b"foo".to_vec(),
            Entry::new(Value::String(b"bar".to_vec())),
        );
        assert_eq!(
            serialize_dataset(&store),
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }

    #[test]
    fn serialize_string_is_binary_safe() {
        let mut store = empty_store();
        let value = b"a\0b\r\nc".to_vec();
        store.data.insert(
            b"k".to_vec(),
            Entry::new(Value::String(value.clone())),
        );

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 1);
        let Frame::Array(args) = &frames[0] else {
            panic!("expected an array frame")
        };
        assert_eq!(args[0].as_bulk(), Some(b"SET".as_slice()));
        assert_eq!(args[1].as_bulk(), Some(b"k".as_slice()));
        assert_eq!(args[2].as_bulk(), Some(value.as_slice()));
    }

    #[test]
    fn serialize_string_with_ttl_emits_absolute_pxat() {
        let mut store = empty_store();
        store.data.insert(
            b"k".to_vec(),
            Entry {
                value: Value::String(b"v".to_vec()),
                expires_at: Some(Instant::now() + Duration::from_secs(10)),
            },
        );

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 1);
        let Frame::Array(args) = &frames[0] else {
            panic!("expected an array frame")
        };
        assert_eq!(args.len(), 5);
        assert_eq!(args[0].as_bulk(), Some(b"SET".as_slice()));
        assert_eq!(args[3].as_bulk(), Some(b"PXAT".as_slice()));

        let pxat: u64 = util::parse(args[4].as_bulk().unwrap()).unwrap();
        let now = util::now_ms();
        assert!(pxat >= now + 9_000, "PXAT {pxat} is not an absolute future time (now {now})");
        assert!(pxat <= now + 11_000, "PXAT {pxat} is too far in the future (now {now})");
    }

    #[test]
    fn serialize_skips_expired_keys() {
        let mut store = empty_store();
        store.data.insert(
            b"k".to_vec(),
            Entry {
                value: Value::String(b"v".to_vec()),
                expires_at: Some(Instant::now() - Duration::from_secs(1)),
            },
        );
        assert!(serialize_dataset(&store).is_empty());
    }

    #[test]
    fn serialize_multiple_strings_in_any_order() {
        let mut store = empty_store();
        store.data.insert(b"a".to_vec(), Entry::new(Value::String(b"1".to_vec())));
        store.data.insert(b"b".to_vec(), Entry::new(Value::String(b"2".to_vec())));

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 2);
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                let Frame::Array(args) = f else {
                    panic!("expected an array frame")
                };
                assert_eq!(args[0].as_bulk(), Some(b"SET".as_slice()));
                (
                    args[1].as_bulk().unwrap().to_vec(),
                    args[2].as_bulk().unwrap().to_vec(),
                )
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );
    }

    // --- ar4: serialize_dataset (collections) -----------------------------

    #[test]
    fn serialize_list_emits_rpush() {
        let mut store = empty_store();
        let list = VecDeque::from(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        store.data.insert(b"mylist".to_vec(), Entry::new(Value::List(list)));

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 1, "expected a single RPUSH");
        let Frame::Array(args) = &frames[0] else {
            panic!("expected an array frame")
        };
        let parts: Vec<&[u8]> = args.iter().filter_map(|f| f.as_bulk()).collect();
        assert_eq!(
            parts,
            vec![
                b"RPUSH".as_slice(),
                b"mylist".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
            ]
        );
    }

    #[test]
    fn serialize_zset_emits_one_zadd_per_member() {
        let mut store = empty_store();
        let mut tree = BTreeSet::new();
        tree.insert((Score(1.0), b"one".to_vec()));
        tree.insert((Score(2.5), b"two".to_vec()));
        store.data.insert(b"z".to_vec(), Entry::new(Value::ZSet(tree)));

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 2, "expected one ZADD per member");
        let mut scores: Vec<(Vec<u8>, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                let Frame::Array(args) = f else {
                    panic!("expected an array frame")
                };
                assert_eq!(args[0].as_bulk(), Some(b"ZADD".as_slice()));
                assert_eq!(args[1].as_bulk(), Some(b"z".as_slice()));
                assert_eq!(args.len(), 4);
                (
                    args[2].as_bulk().unwrap().to_vec(),
                    args[3].as_bulk().unwrap().to_vec(),
                )
            })
            .collect();
        scores.sort();
        assert_eq!(
            scores,
            vec![
                (b"1".to_vec(), b"one".to_vec()),
                (b"2.5".to_vec(), b"two".to_vec()),
            ]
        );
    }

    #[test]
    fn serialize_stream_emits_xadd_with_explicit_ids() {
        let mut store = empty_store();
        let mut map = BTreeMap::new();
        map.insert(StreamId(0, 1), vec![(b"f".to_vec(), b"v".to_vec())]);
        map.insert(StreamId(0, 2), vec![(b"g".to_vec(), b"w".to_vec())]);
        store.data.insert(b"s".to_vec(), Entry::new(Value::Stream(map)));

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 2, "expected one XADD per entry");
        let mut got: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                let Frame::Array(args) = f else {
                    panic!("expected an array frame")
                };
                assert_eq!(args[0].as_bulk(), Some(b"XADD".as_slice()));
                assert_eq!(args[1].as_bulk(), Some(b"s".as_slice()));
                let id = args[2].as_bulk().unwrap().to_vec();
                assert_ne!(id, b"*".to_vec(), "stream entries need explicit ids");
                (
                    id,
                    args[3].as_bulk().unwrap().to_vec(),
                    args[4].as_bulk().unwrap().to_vec(),
                )
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                (b"0-1".to_vec(), b"f".to_vec(), b"v".to_vec()),
                (b"0-2".to_vec(), b"g".to_vec(), b"w".to_vec()),
            ]
        );
    }

    /// A TTL on a non-string value must be written as a separate PEXPIREAT
    /// command: PXAT is only a SET option and is invalid after RPUSH/ZADD/XADD.
    #[test]
    fn serialize_list_with_ttl_emits_pexpireat() {
        let mut store = empty_store();
        let list = VecDeque::from(vec![b"a".to_vec()]);
        store.data.insert(
            b"l".to_vec(),
            Entry {
                value: Value::List(list),
                expires_at: Some(Instant::now() + Duration::from_secs(10)),
            },
        );

        let frames = decode_all(&serialize_dataset(&store));
        assert_eq!(frames.len(), 2, "expected RPUSH + PEXPIREAT");

        let Frame::Array(push) = &frames[0] else {
            panic!("expected an array frame")
        };
        assert_eq!(push[0].as_bulk(), Some(b"RPUSH".as_slice()));

        let Frame::Array(exp) = &frames[1] else {
            panic!("expected an array frame")
        };
        assert_eq!(exp[0].as_bulk(), Some(b"PEXPIREAT".as_slice()));
        assert_eq!(exp[1].as_bulk(), Some(b"l".as_slice()));
        let ms: u64 = util::parse(exp[2].as_bulk().unwrap()).unwrap();
        let now = util::now_ms();
        assert!(ms >= now + 9_000 && ms <= now + 11_000, "PEXPIREAT {ms} (now {now})");
    }

    /// ar6: while a background rewrite is running, a second BGREWRITEAOF must
    /// be rejected without starting another one.
    #[test]
    fn bgrewriteaof_rejects_second_call_while_running() {
        let mut store = empty_store();
        store.aof_rewrite_in_progress.store(true, Ordering::SeqCst);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut conn = Conn::new(0, tx);
        let args = vec![Frame::bulk("BGREWRITEAOF")];

        let frames = match cmd_bgrewriteaof(&args, &mut conn, &mut store) {
            Ok(frames) => frames,
            Err(e) => panic!("expected an error frame, got Err({})", e.message),
        };
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            Frame::Err(msg) => assert!(
                msg.contains("already in progress"),
                "unexpected error message: {msg}"
            ),
            other => panic!("expected an error frame, got {other:?}"),
        }
    }
}