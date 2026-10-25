use codecrafters_redis::Config;

#[tokio::main]
async fn main() {
    let cfg = parse_args(std::env::args());
    codecrafters_redis::run(cfg).await
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Config {
    let mut cfg = Config {
        port: 6379, 
        replicaof: None,
        dir: None,
        dbfilename: None,
    };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--port" => {
                cfg.port = args.next().and_then(|v|v.parse().ok()).unwrap_or(6379);
            },
            "--replicaof" => {
                if let Some(v) = args.next() {
                    if let Some((h,p)) = v.split_once(" ") {
                        cfg.replicaof = Some((h.to_string(), p.trim().parse().unwrap_or(0)));
                    }else if let Some(p) = args.next() {
                        cfg.replicaof = Some((v.to_string(), p.trim().parse().unwrap_or(0)));
                    }
                }
            },
            "--dir" => {
                cfg.dir = args.next();
            },
            "--dbfilename" => {
                cfg.dbfilename = args.next();
            }
            _ => {}
        }
    }

    cfg
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_6379() {
        let cfg = parse_args(std::iter::empty());
        assert_eq!(cfg.port, 6379);
        assert_eq!(cfg.replicaof, None);
    }

    #[test]
    fn parses_port_flag() {
        let args = vec!["--port".to_string(), "6380".to_string()];
        let cfg = parse_args(args.into_iter());
        assert_eq!(cfg.port, 6380);
    }

    #[test]
    fn parse_replicaof_one_argument() {
        let args = vec!["--replicaof".into(), "localhost 7788".into()];
        let cfg = parse_args(args.into_iter());
        assert_eq!(cfg.replicaof, Some(("localhost".into(), 7788)))
    }

    #[test]
    fn parse_replicaof_two_arguments() {
        let args = vec!["--replicaof".into(), "localhost".into(),"7788".into()];
        let cfg = parse_args(args.into_iter());
        assert_eq!(cfg.replicaof, Some(("localhost".into(), 7788)))
    }
}