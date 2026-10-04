use turso_core::LimboError;

const QUERY_ONLY: &str = "Cannot execute write statement in query_only mode";

/// Renders a turso error the way sqlite3_errmsg would where turso's wording
/// differs only cosmetically, e.g. `no such table: t` instead of
/// `Parse error: no such table: t`.
pub fn message(err: &LimboError) -> String {
    match err {
        LimboError::Busy => "database is locked".to_string(),
        LimboError::Interrupt => "interrupted".to_string(),
        LimboError::ParseError(msg) if msg == QUERY_ONLY => {
            "attempt to write a readonly database".to_string()
        }
        other => {
            let msg = other.to_string();
            if let Some(rest) = msg.strip_prefix("Parse error: ") {
                return rest.to_string();
            }
            // S3 durability errors travel through turso as internal errors;
            // report them as the s3 module words them.
            match msg.strip_prefix("Internal error: ") {
                Some(rest) if rest.starts_with("s3 ") => rest.to_string(),
                _ => msg,
            }
        }
    }
}

pub fn is_busy(err: &LimboError) -> bool {
    matches!(err, LimboError::Busy)
}
