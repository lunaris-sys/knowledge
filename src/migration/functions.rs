/// Built-in transform functions for schema migrations.

use chrono::DateTime;
use serde_json::Value;

/// Apply a named transform function to a value.
pub fn apply_transform(func_name: &str, value: &Value) -> Result<Value, String> {
    match func_name {
        "split_comma" => split_comma(value),
        "split_newline" => split_newline(value),
        "join_comma" => join_array(value, ", "),
        "join_newline" => join_array(value, "\n"),
        "to_string" => to_string(value),
        "parse_int" => parse_int(value),
        "parse_float" => parse_float(value),
        "to_lowercase" => str_map(value, |s| s.to_lowercase()),
        "to_uppercase" => str_map(value, |s| s.to_uppercase()),
        "trim" => str_map(value, |s| s.trim().to_string()),
        "epoch_to_datetime" => epoch_to_datetime(value),
        "datetime_to_epoch" => datetime_to_epoch(value),
        "nullify" => Ok(Value::Null),
        "default_empty_string" => Ok(Value::String(String::new())),
        "default_zero" => Ok(Value::Number(0.into())),
        "default_false" => Ok(Value::Bool(false)),
        "default_empty_array" => Ok(Value::Array(vec![])),
        _ => Err(format!("unknown function: {func_name}")),
    }
}

/// List all available transform functions.
pub fn list_functions() -> Vec<&'static str> {
    vec![
        "split_comma",
        "split_newline",
        "join_comma",
        "join_newline",
        "to_string",
        "parse_int",
        "parse_float",
        "to_lowercase",
        "to_uppercase",
        "trim",
        "epoch_to_datetime",
        "datetime_to_epoch",
        "nullify",
        "default_empty_string",
        "default_zero",
        "default_false",
        "default_empty_array",
    ]
}

fn split_comma(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => Ok(Value::Array(
            s.split(',').map(|p| Value::String(p.trim().into())).collect(),
        )),
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err("split_comma: expected string".into()),
    }
}

fn split_newline(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => Ok(Value::Array(
            s.lines().map(|l| Value::String(l.into())).collect(),
        )),
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err("split_newline: expected string".into()),
    }
}

fn join_array(v: &Value, sep: &str) -> Result<Value, String> {
    match v {
        Value::Array(arr) => {
            let parts: Result<Vec<String>, _> = arr
                .iter()
                .map(|el| match el {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err("join: array elements must be strings"),
                })
                .collect();
            Ok(Value::String(parts?.join(sep)))
        }
        Value::Null => Ok(Value::String(String::new())),
        _ => Err("join: expected array".into()),
    }
}

fn to_string(v: &Value) -> Result<Value, String> {
    Ok(match v {
        Value::String(s) => Value::String(s.clone()),
        Value::Number(n) => Value::String(n.to_string()),
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Null => Value::String(String::new()),
        _ => return Err("to_string: unsupported type".into()),
    })
}

fn parse_int(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => {
            let n: i64 = s.trim().parse().map_err(|_| "parse_int: invalid")?;
            Ok(Value::Number(n.into()))
        }
        Value::Number(n) => Ok(Value::Number(n.clone())),
        Value::Null => Ok(Value::Number(0.into())),
        _ => Err("parse_int: expected string or number".into()),
    }
}

fn parse_float(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => {
            let n: f64 = s.trim().parse().map_err(|_| "parse_float: invalid")?;
            Ok(serde_json::Number::from_f64(n)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        Value::Number(n) => Ok(Value::Number(n.clone())),
        Value::Null => Ok(Value::Number(
            serde_json::Number::from_f64(0.0).unwrap(),
        )),
        _ => Err("parse_float: expected string or number".into()),
    }
}

fn str_map(v: &Value, f: impl FnOnce(&str) -> String) -> Result<Value, String> {
    match v {
        Value::String(s) => Ok(Value::String(f(s))),
        Value::Null => Ok(Value::Null),
        _ => Err("expected string".into()),
    }
}

fn epoch_to_datetime(v: &Value) -> Result<Value, String> {
    match v {
        Value::Number(n) => {
            let secs = n.as_i64().ok_or("epoch_to_datetime: not i64")?;
            let dt = DateTime::from_timestamp(secs, 0).ok_or("invalid timestamp")?;
            Ok(Value::String(dt.to_rfc3339()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err("epoch_to_datetime: expected number".into()),
    }
}

fn datetime_to_epoch(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => {
            let dt: DateTime<chrono::Utc> = s.parse().map_err(|_| "invalid datetime")?;
            Ok(Value::Number(dt.timestamp().into()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err("datetime_to_epoch: expected string".into()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_split_comma() {
        let r = apply_transform("split_comma", &json!("a, b, c")).unwrap();
        assert_eq!(r, json!(["a", "b", "c"]));
    }

    #[test]
    fn test_split_newline() {
        let r = apply_transform("split_newline", &json!("a\nb\nc")).unwrap();
        assert_eq!(r, json!(["a", "b", "c"]));
    }

    #[test]
    fn test_join_comma() {
        let r = apply_transform("join_comma", &json!(["a", "b"])).unwrap();
        assert_eq!(r, json!("a, b"));
    }

    #[test]
    fn test_join_newline() {
        let r = apply_transform("join_newline", &json!(["a", "b"])).unwrap();
        assert_eq!(r, json!("a\nb"));
    }

    #[test]
    fn test_to_string() {
        assert_eq!(apply_transform("to_string", &json!(42)).unwrap(), json!("42"));
        assert_eq!(apply_transform("to_string", &json!(true)).unwrap(), json!("true"));
        assert_eq!(apply_transform("to_string", &json!("hi")).unwrap(), json!("hi"));
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(apply_transform("parse_int", &json!("42")).unwrap(), json!(42));
        assert_eq!(apply_transform("parse_int", &json!(42)).unwrap(), json!(42));
        assert!(apply_transform("parse_int", &json!("abc")).is_err());
    }

    #[test]
    fn test_parse_float() {
        let r = apply_transform("parse_float", &json!("3.14")).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.14);
    }

    #[test]
    fn test_case_transforms() {
        assert_eq!(apply_transform("to_lowercase", &json!("HELLO")).unwrap(), json!("hello"));
        assert_eq!(apply_transform("to_uppercase", &json!("hello")).unwrap(), json!("HELLO"));
    }

    #[test]
    fn test_trim() {
        assert_eq!(apply_transform("trim", &json!("  hi  ")).unwrap(), json!("hi"));
    }

    #[test]
    fn test_epoch_to_datetime() {
        let r = apply_transform("epoch_to_datetime", &json!(0)).unwrap();
        assert!(r.as_str().unwrap().contains("1970"));
    }

    #[test]
    fn test_datetime_to_epoch() {
        let r = apply_transform("datetime_to_epoch", &json!("1970-01-01T00:00:00+00:00")).unwrap();
        assert_eq!(r, json!(0));
    }

    #[test]
    fn test_defaults() {
        assert_eq!(apply_transform("nullify", &json!("x")).unwrap(), Value::Null);
        assert_eq!(apply_transform("default_empty_string", &json!(null)).unwrap(), json!(""));
        assert_eq!(apply_transform("default_zero", &json!(null)).unwrap(), json!(0));
        assert_eq!(apply_transform("default_false", &json!(null)).unwrap(), json!(false));
        assert_eq!(apply_transform("default_empty_array", &json!(null)).unwrap(), json!([]));
    }

    #[test]
    fn test_null_handling() {
        assert_eq!(apply_transform("split_comma", &Value::Null).unwrap(), json!([]));
        assert_eq!(apply_transform("join_comma", &Value::Null).unwrap(), json!(""));
        assert_eq!(apply_transform("to_lowercase", &Value::Null).unwrap(), Value::Null);
        assert_eq!(apply_transform("epoch_to_datetime", &Value::Null).unwrap(), Value::Null);
    }

    #[test]
    fn test_unknown_function() {
        assert!(apply_transform("bogus", &json!("x")).is_err());
    }

    #[test]
    fn test_list_functions() {
        let fns = list_functions();
        assert!(fns.contains(&"split_comma"));
        assert!(fns.contains(&"epoch_to_datetime"));
        assert!(fns.len() >= 17);
    }
}
