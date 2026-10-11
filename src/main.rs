#[tokio::main]
async fn main() {
    codecrafters_redis::run(parse_port(std::env::args())).await
}

fn parse_port(mut args: impl Iterator<Item = String>) -> u16 {
    while let Some(a) = args.next() {
        if a == "--port" {
            if let Some(p) = args.next().and_then(|v|v.parse().ok()) {
                return p
            }
        }
    }
    6379
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_6379() {
        assert_eq!(parse_port(std::iter::empty()), 6379);
    }

    #[test]
    fn parses_port_flag() {
        let args = vec!["--port".to_string(), "6380".to_string()];
        assert_eq!(parse_port(args.into_iter()), 6380);
    }
}