use rustler::types::binary::NewBinary;
use rustler::{Binary, Encoder, Env, Term, TermType};
use turso_core::{Numeric, Row, Value};

use crate::atoms;

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut bin = NewBinary::new(env, bytes.len());
    bin.as_mut_slice().copy_from_slice(bytes);
    bin.into()
}

/// The BEAM has no infinite floats; they decode as `:inf` and `:"-inf"`,
/// like Postgrex does.
fn float(env: Env<'_>, f: f64) -> Term<'_> {
    if f == f64::INFINITY {
        atoms::inf().encode(env)
    } else if f == f64::NEG_INFINITY {
        atoms::neg_inf().encode(env)
    } else {
        f.encode(env)
    }
}

pub fn encode<'a>(env: Env<'a>, value: &Value) -> Term<'a> {
    match value {
        Value::Null => atoms::nil().encode(env),
        Value::Numeric(Numeric::Integer(i)) => i.encode(env),
        Value::Numeric(Numeric::Float(f)) => float(env, f64::from(*f)),
        Value::Text(t) => binary(env, t.as_str().as_bytes()),
        Value::Blob(_) => binary(env, value.to_blob().unwrap_or_default()),
    }
}

pub fn encode_row<'a>(env: Env<'a>, row: &Row) -> Term<'a> {
    let values: Vec<Term<'a>> = row.get_values().map(|v| encode(env, v)).collect();
    values.encode(env)
}

pub fn text(s: &str) -> Value {
    Value::build_text(s.to_owned())
}

pub fn blob(bytes: &[u8]) -> Result<Value, String> {
    Value::from_slice(bytes).map_err(|_| "out of memory".to_string())
}

/// Decodes a bind value already normalized by `Sediment.Engine`: integers,
/// floats, binaries (text), `{:blob, binary}` and `nil`.
pub fn decode(term: Term) -> Result<Value, String> {
    match term.get_type() {
        TermType::Integer => term
            .decode::<i64>()
            .map(Value::from_i64)
            .map_err(|_| "integer out of range".to_string()),
        TermType::Float => term
            .decode::<f64>()
            .map(Value::from_f64)
            .map_err(|_| "invalid float".to_string()),
        TermType::Binary => {
            let bin: Binary = term.decode().map_err(|_| "invalid binary".to_string())?;
            std::str::from_utf8(bin.as_slice())
                .map(text)
                .or_else(|_| blob(bin.as_slice()))
        }
        TermType::Atom if term.decode::<rustler::Atom>().ok() == Some(atoms::nil()) => {
            Ok(Value::Null)
        }
        TermType::Tuple => {
            let (tag, bin): (rustler::Atom, Binary) = term
                .decode()
                .map_err(|_| "unsupported bind value".to_string())?;
            if tag == atoms::blob() {
                blob(bin.as_slice())
            } else {
                Err("unsupported bind value".to_string())
            }
        }
        _ => Err("unsupported bind value".to_string()),
    }
}
