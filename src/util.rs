
pub(crate) fn parse<T: std::str::FromStr>(bytes: &[u8]) -> Option<T> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}