use std::{cmp::Ordering, collections::{BTreeMap, BTreeSet, HashMap, VecDeque}, time::{Instant}};

use tokio::{sync::{
        mpsc,
        oneshot,
    }};
use crate::{resp::Frame};

pub type Store = Db;
pub type Outbox = mpsc::UnboundedSender<Frame>;
type ZEntry = (Score, Vec<u8>);

/// the value type in our Store
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }
}

pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    ZSet(BTreeSet<ZEntry>),
    Stream(BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>),
}

pub enum Role {
    Master,
    Replica { host: String, port: u16 }
}

pub(crate) struct Db {
    pub data: HashMap<Vec<u8>, Entry>,
    pub waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
    pub channel_subscribers: HashMap<Vec<u8>, HashMap<u64, Outbox>>,
    pub role: Role,
    pub master_replid: String,
    pub master_repl_offset: u64,  // replicated offset on master
    pub replicas: HashMap<u64, Outbox>,
    pub replic_acks: HashMap<u64, u64>,
    pub dir: String, // def: .
    pub dbfilename: String, // def "dump.rdb"
    pub appendonly: String, // no
    pub appenddirname: String, // appendonlydir
    pub appendfilename: String, // appendonly.aof
    pub appendfsync: String, // everysec
    pub aof: Option<std::fs::File>,
    versions: HashMap<Vec<u8>, u64>,
    version_seq: u64,
}

impl Db {
    pub(crate) fn new(role: Role, dir: Option<String>, dbfilename: Option<String>) -> Self {
        Self {
            data: HashMap::new(),
            waiters: HashMap::new(),
            channel_subscribers: HashMap::new(),
            versions: HashMap::new(),
            version_seq: 0,
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            master_repl_offset: 0,
            replicas: HashMap::new(),
            replic_acks: HashMap::new(),
            dir: dir.unwrap_or_else(|| {
                std::env::current_dir()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| ".".to_string())  
            }),
            dbfilename: dbfilename.unwrap_or("dump.rdb".to_string()),
            appendonly: "no".to_string(),
            appenddirname: "appendonlydir".to_string(),
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: "everysec".to_string(),
            aof: None,
        }
    }
    pub fn live_entry(&mut self, key: &[u8]) -> Option<& mut Entry> {
        if self
            .data
            .get(key)
            .and_then(|e| e.expires_at)
            .is_some_and(|t| t <= Instant::now())
        {
            self.data.remove(key);
            self.touch(key);
        }
        self.data.get_mut(key)
    }

    /// mark that this operation causes version bump
    pub fn touch(&mut self, key: &[u8]) {
        self.version_seq += 1;
        self.versions.insert(key.to_vec(), self.version_seq);
    }

    pub fn version(&self, key:&[u8]) -> u64 {
        self.versions.get(key).copied().unwrap_or(0)
    }
}



#[derive(Clone)]
pub struct Score(pub(crate) f64);

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        return self.0.total_cmp(&other.0);
    }
}
impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(pub u64, pub u64);

impl StreamId {
    // fn parse(bytes: &[u8]) -> Option<Self> {
    //     let s = str::from_utf8(bytes).ok()?;
    //     let (ms, seq) = s.split_once('-')?;
    //     Some(StreamId(ms.parse().ok()?, seq.parse().ok()?))
    // }

    pub(crate) fn render(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }

    /// IDs may omit the sequence: start defaults seq to 0, end defaults seq to u64::MAX
    pub fn parse_range_id(bytes:&[u8], is_end:bool) -> Option<StreamId> {
        let s = str::from_utf8(bytes).ok()?;
        match s {
            "-" => return Some(StreamId(0, 0)),
            "+" => return Some(StreamId(u64::MAX, u64::MAX)),
            _ => {}
        }
        match s.split_once('-') {
            Some((ms,seq)) => Some(StreamId(ms.parse().ok()?, seq.parse().ok()?)),
            None => {
                let ms: u64 = s.parse().ok()?;
                Some(StreamId(ms, if is_end { u64::MAX } else { 0 }))
            }
        }
    }
}

pub enum IdSpec {
    Explicit(StreamId),
    AutoSeq(u64),
    FullyAutoSeq,
}

impl IdSpec {
    
    pub fn parse_id_spec(bytes: &[u8]) -> Option<IdSpec> {
        let s = str::from_utf8(bytes).ok()?;
        if s == "*" {
            return Some(IdSpec::FullyAutoSeq);
        }
        let (ms, seq) = s.split_once('-')?;
        let ms: u64 = ms.parse().ok()?;
        if seq == "*" {
            Some(IdSpec::AutoSeq(ms))
        } else {
            Some(IdSpec::Explicit(StreamId(ms, seq.parse().ok()?)))
        }
    }
}

