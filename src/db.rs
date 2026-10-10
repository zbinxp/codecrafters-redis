use std::{cmp::Ordering, collections::{BTreeMap, BTreeSet, HashMap, VecDeque}, time::{Instant}};

use tokio::{sync::{
        mpsc,
        oneshot,
    }};
use crate::resp::Frame;

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

pub(crate) struct Db {
    pub data: HashMap<Vec<u8>, Entry>,
    pub waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
    pub channel_subscribers: HashMap<Vec<u8>, HashMap<u64, Outbox>>,
}

impl Db {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
            waiters: HashMap::new(),
            channel_subscribers: HashMap::new(),
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
        }
        self.data.get_mut(key)
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

