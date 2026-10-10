
pub(crate) struct RedisError {
    pub message: String
}

impl RedisError {
    pub(crate) fn wrong_type() -> Self {
        RedisError {
            message : "WRONGTYPE Operation against a key holding the wrong kind of value".into()
        }
    }

    pub(crate) fn not_integer() -> Self {
        RedisError { message: "ERR value is not an integer or out of range".into() }
    }

    pub(crate) fn invalid_expire() -> Self {
        RedisError { message: "ERR value is not an integer".into() }
    }

    pub(crate) fn not_valid_float() -> Self {
        RedisError { message: "ERR value is not a valid float".into() }
    }

    pub(crate) fn timeout_not_float() -> Self {
        RedisError { message: "ERR timeout is not a float or out of range".into() }
    }

    pub(crate) fn timeout_not_integer() -> Self {
        RedisError { message: "ERR timeout is not an integer or out of range".into() }
    }


    pub(crate) fn wrong_args(cmd: &str) -> Self {
        RedisError { message: format!("ERR wrong number of arguments for '{}' command", cmd) }
    }

    pub(crate) fn value_out_of_range() -> Self {
        RedisError { message: "ERR value is out of range, must be positive".into() }
    }

    pub(crate) fn not_array_args() -> Self {
        RedisError { message: "ERR expected an array command".into() }
    }

    pub(crate) fn unknown_command(cmd:&str) -> Self {
        RedisError { message: format!("ERR unknown command '{}'", cmd) }
    }

    pub(crate) fn invalid_stream_id() -> Self {
        RedisError { message: "ERR Invalid stream ID specified as stream command argument".into()}
    }

    pub(crate) fn stream_id_less_than_zero() -> Self {
        RedisError { message: "ERR The ID specified in XADD must be greater than 0-0".into() }
    }

    pub(crate) fn stream_id_less_than_target() -> Self {
        RedisError { message: "ERR The ID specified in XADD is equal or smaller than the target stream top item".into() }
    }

    pub(crate) fn stream_unbalanced_arguments() -> Self {
        RedisError { message: "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.".into() }
    }

    pub(crate) fn syntax_error() -> Self {
        RedisError { message: "ERR syntax error".into() }
    }

    pub(crate) fn xread_syntax() -> Self {
        RedisError { message: "ERR syntax error for 'xread' command".into() }
    }

    pub(crate) fn not_allowed_in_subscribe_mode(name:&str) -> Self {
        RedisError { message: format!("ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context") }
    }

    pub(crate) fn increment_overflow() -> Self {
        RedisError { message: "ERR increment or decrement would overflow".into() }
    }

    pub(crate) fn exec_without_multi() -> Self {
        RedisError { message: "ERR EXEC without MULTI".into() }
    }

    pub(crate) fn discard_without_multi() -> Self {
       RedisError { message: "ERR DISCARD without MULTI".into() }
    }
}