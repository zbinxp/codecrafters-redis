
/// parse manifest into ordered file list
/// format:
/// file appendonly.aof.2.base.aof  seq 2 type b
/// file appendonly.aof.2.incr.aof seq 2 type i
pub fn parse_manifest(manifest: &str) -> Option<Vec<String>> {
    let mut ma = Vec::new();
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
        Some(ma.iter().map(|t|t.2.to_string()).collect())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

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
}