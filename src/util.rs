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

pub fn instant_to_unix_ms(inst: Instant) -> u64 {
    let now = now_ms();
    let ms = inst.duration_since(Instant::now()).as_millis() as u64;
    now.checked_add(ms).unwrap_or(0) 
}

pub fn set_bit(x: &mut u8, mask: u8, v: bool) {
    *x = if v {
        *x | mask
    } else {
        *x & !mask
    };
}

pub fn get_bit(x: u8, mask: u8) -> bool {
    x & mask != 0
}
