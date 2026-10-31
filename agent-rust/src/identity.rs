//! Shared contract 2.0.0 identities. Configuration hashing remains separate.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn canonical(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Ok("null".into()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::String(v) => serde_json::to_string(v).map_err(|e| e.to_string()),
        Value::Number(v) => {
            let n = v.as_f64().ok_or("unsupported JSON number")?;
            if !n.is_finite() || ((v.is_i64() || v.is_u64()) && n.abs() > 9007199254740991.0) {
                return Err("number outside I-JSON domain; use a string".into());
            }
            if n == 0.0 {
                return Ok("0".into());
            }
            let raw = serde_json::Number::from_f64(n)
                .ok_or("invalid number")?
                .to_string();
            let neg = raw.starts_with('-');
            let unsigned = raw.trim_start_matches('-');
            let (mantissa, exp) = unsigned
                .split_once(['e', 'E'])
                .map(|(m, e)| (m, e.parse::<i32>().unwrap()))
                .unwrap_or((unsigned, 0));
            let before = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
            let mut digits = mantissa.replace('.', "");
            let mut point = before + exp;
            while digits.starts_with('0') && digits.len() > 1 {
                digits.remove(0);
                point -= 1;
            }
            while digits.ends_with('0') && digits.len() > 1 {
                digits.pop();
            }
            let sign = if neg { "-" } else { "" };
            if n.abs() >= 1e-6 && n.abs() < 1e21 {
                let body = if point <= 0 {
                    format!("0.{}{}", "0".repeat((-point) as usize), digits)
                } else if point as usize >= digits.len() {
                    format!("{}{}", digits, "0".repeat(point as usize - digits.len()))
                } else {
                    format!(
                        "{}.{}",
                        &digits[..point as usize],
                        &digits[point as usize..]
                    )
                };
                Ok(format!("{sign}{body}"))
            } else {
                let exponent = point - 1;
                let mantissa = if digits.len() == 1 {
                    digits
                } else {
                    format!("{}.{}", &digits[..1], &digits[1..])
                };
                Ok(format!(
                    "{sign}{mantissa}e{}{exponent}",
                    if exponent >= 0 { "+" } else { "" }
                ))
            }
        }
        Value::Array(v) => Ok(format!(
            "[{}]",
            v.iter()
                .map(canonical)
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        )),
        Value::Object(v) => {
            let mut keys: Vec<_> = v.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            Ok(format!(
                "{{{}}}",
                keys.iter()
                    .map(|k| Ok(format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical(&v[*k])?
                    )))
                    .collect::<Result<Vec<_>, String>>()?
                    .join(",")
            ))
        }
    }
}
pub fn digest(value: &Value) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(canonical(value)?.as_bytes())
    ))
}
pub fn action(namespace: &str, session: &str, execution: &str) -> Result<String, String> {
    digest(&json!(["agent-action-v1", namespace, session, execution]))
}
pub fn payload(tool: &str, target: &str, args: &Value) -> Result<String, String> {
    digest(&json!(["agent-payload-v1", tool, target, args]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_identity_vectors() {
        let v: Value =
            serde_json::from_str(include_str!("../../schema/idempotency-vectors.json")).unwrap();
        for row in v["actions"].as_array().unwrap() {
            assert_eq!(
                action(
                    row["namespace"].as_str().unwrap(),
                    row["session_id"].as_str().unwrap(),
                    row["execution_id"].as_str().unwrap()
                )
                .unwrap(),
                row["expected_key"]
            );
            assert_eq!(
                payload(
                    row["action_type"].as_str().unwrap(),
                    row["target"].as_str().unwrap(),
                    &row["arguments"]
                )
                .unwrap(),
                row["expected_payload_sha256"]
            );
        }
    }
}

#[cfg(test)]
mod boundaries {
    use super::*;
    #[test]
    fn jcs_number_unicode_and_strict_json_boundaries() {
        for (raw, expected) in [
            ("1.0", "1"),
            ("-0.0", "0"),
            ("1e-6", "0.000001"),
            ("1e-7", "1e-7"),
            ("1e21", "1e+21"),
            ("1e20", "100000000000000000000"),
        ] {
            let v = crate::tools::parse_strict_json(raw).unwrap();
            assert_eq!(canonical(&v).unwrap(), expected);
        }
        for raw in [
            "9007199254740992",
            "{\"x\":1,\"x\":2}",
            "\"\\ud800\"",
            "NaN",
            "1e999",
        ] {
            assert!(
                crate::tools::parse_strict_json(raw)
                    .and_then(|v| canonical(&v))
                    .is_err(),
                "accepted {raw}"
            );
        }
        assert_eq!(
            canonical(&json!({"\u{e000}":1,"\u{10000}":2})).unwrap(),
            "{\"\u{10000}\":2,\"\u{e000}\":1}"
        );
    }
    #[test]
    fn fixed_command_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("../../schema/idempotency-vectors.json")).unwrap();
        for row in vectors["commands"].as_array().unwrap() {
            assert_eq!(
                digest(&json!([
                    "agent-command-v1",
                    row["method"],
                    row["route"],
                    row["request"]
                ]))
                .unwrap(),
                row["expected_sha256"]
            );
        }
    }
}

// Python's snapshot/archive/config digests predate JCS. Preserve its floating
// point spelling here; request/action/commit identities continue to use JCS.
pub fn python_number(value: &serde_json::Number) -> String {
    if value.is_i64() || value.is_u64() {
        return value.to_string();
    }
    let n = value.as_f64().unwrap();
    if n == 0.0 {
        return if n.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    let raw = value.to_string();
    let sign = if raw.starts_with('-') { "-" } else { "" };
    let unsigned = raw.trim_start_matches('-');
    let (mantissa, exp) = unsigned
        .split_once(['e', 'E'])
        .map(|(m, e)| (m, e.parse::<i32>().unwrap()))
        .unwrap_or((unsigned, 0));
    let mut point = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exp;
    let mut digits = mantissa.replace('.', "");
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
        point -= 1;
    }
    while digits.ends_with('0') && digits.len() > 1 {
        digits.pop();
    }
    if n.abs() >= 1e-4 && n.abs() < 1e16 {
        let text = if point <= 0 {
            format!("0.{}{}", "0".repeat((-point) as usize), digits)
        } else if point as usize >= digits.len() {
            format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
        } else {
            format!(
                "{}.{}",
                &digits[..point as usize],
                &digits[point as usize..]
            )
        };
        format!("{sign}{text}")
    } else {
        let exponent = point - 1;
        let m = if digits.len() == 1 {
            digits
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!(
            "{sign}{m}e{}{:02}",
            if exponent >= 0 { "+" } else { "-" },
            exponent.abs()
        )
    }
}
pub fn python_encoded(v: &Value) -> Result<String, String> {
    match v {
        Value::Number(n) => Ok(python_number(n)),
        Value::Array(a) => Ok(format!(
            "[{}]",
            a.iter()
                .map(python_encoded)
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        )),
        Value::Object(o) => {
            let mut keys: Vec<_> = o.keys().collect();
            keys.sort();
            Ok(format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|k| Ok(format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        python_encoded(&o[k])?
                    )))
                    .collect::<Result<Vec<_>, String>>()?
                    .join(",")
            ))
        }
        _ => serde_json::to_string(v).map_err(|e| e.to_string()),
    }
}
#[cfg(test)]
mod python_numbers {
    use super::*;
    #[test]
    fn legacy_digests_preserve_python_float_spelling() {
        for (input, expected) in [
            ("1.0", "1.0"),
            ("-0.0", "-0.0"),
            ("1e-5", "1e-05"),
            ("1e-7", "1e-07"),
            ("1e16", "1e+16"),
            ("1e15", "1000000000000000.0"),
        ] {
            let v: Value = serde_json::from_str(input).unwrap();
            assert_eq!(python_encoded(&v).unwrap(), expected);
        }
    }
}
