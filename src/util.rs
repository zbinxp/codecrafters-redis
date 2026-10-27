use std::time::{Duration, Instant};


pub(crate) fn parse<T: std::str::FromStr>(bytes: &[u8]) -> Option<T> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn unix_ms_to_instant(target_ms: u64) -> Option<Instant> {
    let now = now_ms();
    if target_ms <= now {
        return None
    }
    Instant::now().checked_add(Duration::from_millis(target_ms - now))
}