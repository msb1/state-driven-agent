//! Redacted provider failures are operator logs only, never public responses.
use regex::Regex;
use serde_json::{Value, json};
fn message(input: &str) -> String {
    let mut text = input.split_whitespace().collect::<Vec<_>>().join(" ");
    for pattern in [
        r#""[^"\r\n]{0,2000}""#,
        r"'[^'\r\n]{0,2000}'",
        r"`[^`\r\n]{0,2000}`",
        r"(?i)\b(messages?|prompt|request|body|content)\b.*$",
        r"(?i)\b(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)\b\s*[:=]\s*[^\s,;]+",
        r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]+=*",
        r"(?i)\b(sk|pk|rk|xox[baprs])-[-A-Za-z0-9_]{8,}\b",
        r"(?i)https?://[^\s\])}>]+",
    ] {
        text = Regex::new(pattern)
            .unwrap()
            .replace_all(&text, "[REDACTED]")
            .into_owned();
    }
    text.chars().filter(|c| !c.is_control()).take(500).collect()
}
pub fn record(status: u16, body: &Value) -> Value {
    let mut fields = json!({});
    let allowed = Regex::new(r"^[A-Za-z0-9_.\[\]/:-]{1,160}$").unwrap();
    for key in ["type", "code", "param"] {
        if let Some(v) = body["error"][key].as_str().filter(|v| allowed.is_match(v)) {
            fields[key] = json!(v);
        }
    }
    if let Some(v) = body["error"]["message"]
        .as_str()
        .or_else(|| body["error"].as_str())
    {
        fields["message"] = json!(message(v));
    }
    json!({"event":"model_provider_failure","http_status":status,"provider_error":fields})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provider_diagnostics_never_echo_secrets() {
        let v = record(
            400,
            &json!({"error":{"type":"invalid_request","message":"Failed model 'private-model'. authorization=secret prompt user private text"},"request":"never emit"}),
        );
        let text = v.to_string();
        for secret in [
            "private-model",
            "authorization=secret",
            "user private text",
            "never emit",
        ] {
            assert!(!text.contains(secret));
        }
        assert!(text.contains("invalid_request"));
    }
}
