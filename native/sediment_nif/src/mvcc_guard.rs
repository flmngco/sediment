//! Refuses to switch a database with AUTOINCREMENT tables into MVCC mode,
//! or one whose MVCC log another database file would share (`log_guard`).
//!
//! turso_core 0.8.1 doesn't carry an AUTOINCREMENT table's sequence over when
//! an existing database switches to MVCC: the next insert reuses id 1 and
//! silently replaces that row. Databases created in MVCC mode (every S3
//! database) are unaffected, and so are plain `INTEGER PRIMARY KEY` tables.

use std::sync::Arc;

use turso_core::{Connection, Statement};
use turso_parser::lexer::Lexer;
use turso_parser::token::TokenType;

use crate::conn::ConnRes;
use crate::error;
use crate::stmt::{self, Step};

/// Whether `sql` contains `PRAGMA [schema.]journal_mode = mvcc` (or
/// `experimental_mvcc`, quoted or not, `=` or the call form).
pub fn requests_mvcc(sql: &str) -> bool {
    // Whitespace and comments come back as TK_NONE. The lexer returns the
    // same error forever on an unterminated literal (it doesn't advance), so
    // tokens are read up to the first error; such SQL can't run past it.
    let tokens: Vec<_> = Lexer::new(sql.as_bytes())
        .map_while(Result::ok)
        .filter(|t| t.token_type != TokenType::TK_NONE)
        .collect();
    tokens.iter().enumerate().any(|(i, token)| {
        if token.token_type != TokenType::TK_PRAGMA {
            return false;
        }
        let mut rest = tokens[i + 1..].iter();
        let mut name = rest.next();
        if rest.clone().next().map(|t| t.token_type) == Some(TokenType::TK_DOT) {
            rest.next();
            name = rest.next();
        }
        let is_journal_mode = name.is_some_and(|t| unquote(t.value) == "journal_mode");
        let assigns = rest
            .next()
            .is_some_and(|t| matches!(t.token_type, TokenType::TK_EQ | TokenType::TK_LP));
        is_journal_mode
            && assigns
            && rest
                .next()
                .is_some_and(|t| matches!(unquote(t.value).as_str(), "mvcc" | "experimental_mvcc"))
    })
}

fn unquote(value: &[u8]) -> String {
    let text = String::from_utf8_lossy(value);
    let text = text.trim_matches(|c| matches!(c, '\'' | '"' | '`' | '[' | ']'));
    text.to_ascii_lowercase()
}

/// `check_switch` if `sql` (one statement of a script, about to run) switches
/// to MVCC: the earlier statements of the script have run by then, so a table
/// they created is seen.
pub fn check_statement(res: &ConnRes, conn: &Arc<Connection>, sql: &str) -> Result<(), Step> {
    if requests_mvcc(sql) {
        check_switch(res, conn)
    } else {
        Ok(())
    }
}

/// `Err(Step::Error(..))` when `conn`'s database is not in MVCC mode yet and
/// has AUTOINCREMENT tables; the database is left unchanged.
pub fn check_switch(res: &ConnRes, conn: &Arc<Connection>) -> Result<(), Step> {
    if first_column(res, conn, "PRAGMA journal_mode")?
        .first()
        .map(String::as_str)
        == Some("mvcc")
    {
        return Ok(());
    }
    let tables = autoincrement_tables(res, conn)?;
    if tables.is_empty() {
        return claim_log(res);
    }
    Err(Step::Error(format!(
        "refusing to switch to MVCC: the database has AUTOINCREMENT tables ({}), and \
         turso_core 0.8.1 would reuse their ids after the switch, silently overwriting \
         existing rows. The database is unchanged. Keep its current journal mode, or copy \
         the data into a new database opened with journal_mode mvcc from the start",
        tables.join(", ")
    )))
}

/// Claims the log the database will use in MVCC mode, for as long as it is
/// open (see `log_guard`).
fn claim_log(res: &ConnRes) -> Result<(), Step> {
    let Some((path, db)) = &res.file else {
        return Ok(());
    };
    let claim = crate::log_guard::claim(std::path::Path::new(path)).map_err(Step::Error)?;
    if let Some(db) = db.upgrade() {
        claim.keep_while(&db);
    }
    Ok(())
}

fn autoincrement_tables(res: &ConnRes, conn: &Arc<Connection>) -> Result<Vec<String>, Step> {
    let mut statement = prepare(
        conn,
        "SELECT name, sql FROM sqlite_schema WHERE type = 'table'",
    )?;
    let mut tables = Vec::new();
    while let Step::Row = stmt::advance_blocking(res, &mut statement)? {
        let row = statement.row().expect("row after Step::Row");
        let mut values = row.get_values();
        let name = values
            .next()
            .and_then(|v| v.to_text())
            .unwrap_or_default()
            .to_owned();
        let sql = values.next().and_then(|v| v.to_text()).unwrap_or_default();
        if Lexer::new(sql.as_bytes())
            .map_while(Result::ok)
            .any(|t| t.token_type == TokenType::TK_AUTOINCR)
        {
            tables.push(name);
        }
    }
    Ok(tables)
}

fn first_column(res: &ConnRes, conn: &Arc<Connection>, sql: &str) -> Result<Vec<String>, Step> {
    let mut statement = prepare(conn, sql)?;
    let mut out = Vec::new();
    while let Step::Row = stmt::advance_blocking(res, &mut statement)? {
        let row = statement.row().expect("row after Step::Row");
        if let Some(text) = row.get_values().next().and_then(|v| v.to_text()) {
            out.push(text.to_owned());
        }
    }
    Ok(out)
}

fn prepare(conn: &Arc<Connection>, sql: &str) -> Result<Statement, Step> {
    conn.prepare(sql)
        .map_err(|e| Step::Error(error::message(&e)))
}

#[cfg(test)]
mod tests {
    use super::requests_mvcc;

    #[test]
    fn detects_the_mvcc_pragma() {
        for sql in [
            "PRAGMA journal_mode = 'mvcc'",
            "pragma journal_mode=mvcc",
            "PRAGMA main.journal_mode = \"MVCC\"",
            "PRAGMA journal_mode('experimental_mvcc')",
            "select 1; /* x */ PRAGMA journal_mode = mvcc;",
        ] {
            assert!(requests_mvcc(sql), "{sql}");
        }
        for sql in [
            "PRAGMA journal_mode",
            "PRAGMA journal_mode = wal",
            "PRAGMA mvcc_checkpoint_threshold = 1",
            "select 'PRAGMA journal_mode = mvcc'",
            "create table journal_mode (mvcc)",
        ] {
            assert!(!requests_mvcc(sql), "{sql}");
        }
    }

    #[test]
    fn stops_at_a_lexer_error() {
        // The lexer returns the same error forever on an unterminated literal:
        // reading past errors never ended.
        assert!(!requests_mvcc("' AS > COLUMN INSERT PRAGMA UNION"));
        assert!(!requests_mvcc("x'0 PRAGMA journal_mode = mvcc"));
        assert!(requests_mvcc(
            "PRAGMA journal_mode = mvcc; select 'unterminated"
        ));
    }
}
