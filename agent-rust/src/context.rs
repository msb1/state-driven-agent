//! Complete-wire request budgeting and safe history compaction primitives.
use crate::engine::{Memory, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RequestBudget {
    pub estimated_input_tokens: i64,
    pub reserved_output_tokens: i64,
    pub safety_margin_tokens: i64,
    pub context_window_tokens: i64,
    pub fits: bool,
    pub estimator: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Ledger {
    pub goal: String,
    pub model_claims: Vec<String>,
    pub dead_ends: Vec<String>,
    pub hypothesis: String,
    pub source_sha256: String,
    pub verification: String,
}

#[derive(Clone)]
pub struct HistoryGroup {
    pub messages: Vec<Message>,
    pub indexes: Vec<usize>,
}

pub fn wire_message(message: &Message) -> Value {
    let mut map = Map::new();
    map.insert("role".into(), json!(message.role));
    if let Some(content) = &message.content {
        map.insert("content".into(), json!(content));
    }
    if let Some(name) = &message.name {
        map.insert("name".into(), json!(name));
    }
    if let Some(id) = &message.tool_call_id {
        map.insert("tool_call_id".into(), json!(id));
    }
    if let Some(calls) = message
        .tool_calls
        .as_ref()
        .filter(|v| !v.is_null() && !v.as_array().is_some_and(Vec::is_empty))
    {
        map.insert("tool_calls".into(), calls.clone());
    }
    Value::Object(map)
}

pub fn estimate_payload(payload: &Value, memory: &Memory) -> Result<RequestBudget, String> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|e| format!("invalid request payload: {e}"))?
        .len();
    let messages = payload
        .get("messages")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let estimate = bytes as i64 + 32 * messages as i64;
    let cap = payload
        .get("max_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let reserve = memory.reserved_output_tokens.max(cap);
    let fits = estimate + reserve + memory.safety_margin_tokens <= memory.context_window_tokens;
    Ok(RequestBudget {
        estimated_input_tokens: estimate,
        reserved_output_tokens: reserve,
        safety_margin_tokens: memory.safety_margin_tokens,
        context_window_tokens: memory.context_window_tokens,
        fits,
        estimator:
            "compact UTF-8 JSON bytes + 32 per message; conservative estimate, not provider usage"
                .into(),
    })
}

pub fn interaction_groups(messages: &[Message]) -> Result<Vec<HistoryGroup>, String> {
    let mut groups = Vec::new();
    let mut index = 0;
    while index < messages.len() {
        let message = &messages[index];
        if message.role == "tool" {
            return Err(format!(
                "context_integrity_error: orphaned tool result at history index {index}"
            ));
        }
        let calls = message
            .tool_calls
            .as_ref()
            .and_then(Value::as_array)
            .filter(|calls| message.role == "assistant" && !calls.is_empty());
        let Some(calls) = calls else {
            groups.push(HistoryGroup {
                messages: vec![message.clone()],
                indexes: vec![index],
            });
            index += 1;
            continue;
        };
        let mut ids = Vec::with_capacity(calls.len());
        let mut unique = HashSet::new();
        for call in calls {
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    format!(
                        "context_integrity_error: missing tool-call ID at history index {index}"
                    )
                })?;
            if !unique.insert(id.to_owned()) {
                return Err(format!(
                    "context_integrity_error: duplicate tool-call ID at history index {index}"
                ));
            }
            ids.push(id.to_owned());
        }
        let end = index + 1 + ids.len();
        if end > messages.len() {
            return Err(format!(
                "context_integrity_error: incomplete tool-call batch at history index {index}"
            ));
        }
        let mut results = HashSet::new();
        for result in &messages[index + 1..end] {
            if result.role != "tool" {
                return Err(format!(
                    "context_integrity_error: incomplete tool-call result group at history index {index}"
                ));
            }
            let id = result
                .tool_call_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    format!(
                        "context_integrity_error: missing tool result ID at history index {index}"
                    )
                })?;
            if !results.insert(id.to_owned()) {
                return Err(format!(
                    "context_integrity_error: duplicate tool result ID {id:?}"
                ));
            }
        }
        if results.len() != ids.len() || ids.iter().any(|id| !results.contains(id)) {
            return Err(format!(
                "context_integrity_error: tool-call results do not correlate at history index {index}"
            ));
        }
        let mut group = vec![message.clone()];
        group.extend_from_slice(&messages[index + 1..end]);
        groups.push(HistoryGroup {
            messages: group,
            indexes: (index..end).collect(),
        });
        index = end;
    }
    Ok(groups)
}

pub fn split_history(
    messages: &[Message],
    minimum_messages: usize,
) -> Result<(Vec<HistoryGroup>, Vec<HistoryGroup>), String> {
    let groups = interaction_groups(messages)?;
    let mut boundary = groups.len();
    let mut count = 0;
    while boundary > 0 && count < minimum_messages {
        boundary -= 1;
        count += groups[boundary].messages.len();
    }
    Ok((groups[..boundary].to_vec(), groups[boundary..].to_vec()))
}

pub fn flatten(groups: &[HistoryGroup]) -> Vec<Message> {
    groups
        .iter()
        .flat_map(|group| group.messages.clone())
        .collect()
}

pub fn source_text(
    goal: &str,
    history: &[Message],
    previous: Option<&str>,
) -> Result<String, String> {
    let wire: Vec<Value> = history.iter().map(wire_message).map(sort_json).collect();
    let encoded = crate::tools::python_json(&json!(wire));
    Ok(format!(
        "Goal: {goal}\nPrevious ledger: {}\nHistorical prefix:\n{encoded}",
        previous.filter(|x| !x.is_empty()).unwrap_or("none")
    ))
}

fn sort_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json).collect()),
        Value::Object(values) => {
            let mut sorted = values.into_iter().collect::<Vec<_>>();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            let mut map = Map::new();
            for (key, value) in sorted {
                map.insert(key, sort_json(value));
            }
            Value::Object(map)
        }
        other => other,
    }
}

pub fn validate_ledger(
    proposal: &str,
    goal: &str,
    source: &str,
    max_bytes: usize,
    hybrid: bool,
) -> Result<(Ledger, String), String> {
    if proposal.trim().is_empty() {
        return Err("summary is empty".into());
    }
    if proposal.len() > max_bytes {
        return Err("summary exceeds ledger byte budget".into());
    }
    let proposal = strip_fence(proposal);
    let mut sections = if hybrid {
        parse_xml_sections(&proposal)?
    } else {
        parse_heading_sections(&proposal)?
    };
    if sections.len() != 4 {
        return Err("summary requires four sections".into());
    }
    sections[0] = goal.trim().to_owned();
    if sections[1..]
        .iter()
        .any(|s| matches!(s.trim(), "..." | "…"))
    {
        return Err("summary reflection must not be an ellipsis placeholder".into());
    }
    if sections.iter().any(|section| section.trim().is_empty()) {
        return Err("all summary sections must be populated".into());
    }
    let source_numbers = number_set(source);
    for number in number_set(&sections.join("\n")) {
        if !source_numbers.contains(&number) {
            return Err(format!(
                "summary introduces numeric claim {number} absent from source"
            ));
        }
    }
    let digest = format!("{:x}", Sha256::digest(source.as_bytes()));
    let ledger = Ledger {
        goal: goal.to_owned(),
        model_claims: sections[1].lines().map(str::to_owned).collect(),
        dead_ends: sections[2].lines().map(str::to_owned).collect(),
        hypothesis: sections[3].clone(),
        source_sha256: digest,
        verification: "unverified_model_summary".into(),
    };
    let rendered = render_ledger(&ledger, hybrid);
    if rendered.len() > max_bytes {
        return Err("canonical ledger exceeds configured byte limit".into());
    }
    Ok((ledger, rendered))
}

fn strip_fence(proposal: &str) -> String {
    let text = proposal.trim();
    if let Some(rest) = text.strip_prefix("```") {
        if let Some((label, body)) = rest.split_once('\n') {
            if matches!(
                label.trim().to_ascii_lowercase().as_str(),
                "" | "xml" | "text"
            ) {
                if let Some(body) = body.trim_end().strip_suffix("```") {
                    return body.trim().to_owned();
                }
            }
        }
    }
    text.to_owned()
}

fn xml_character(c: char) -> bool {
    matches!(c,'\t'|'\n'|'\r'|'\u{20}'..='\u{d7ff}'|'\u{e000}'..='\u{fffd}'|'\u{10000}'..='\u{10ffff}')
}
fn xml_name_start(c: char) -> bool {
    matches!(c,'_'|'A'..='Z'|'a'..='z'|'\u{c0}'..='\u{d6}'|'\u{d8}'..='\u{f6}'|'\u{f8}'..='\u{2ff}'|'\u{370}'..='\u{37d}'|'\u{37f}'..='\u{1fff}'|'\u{200c}'..='\u{200d}'|'\u{2070}'..='\u{218f}'|'\u{2c00}'..='\u{2fef}'|'\u{3001}'..='\u{d7ff}'|'\u{f900}'..='\u{fdcf}'|'\u{fdf0}'..='\u{fffd}'|'\u{10000}'..='\u{effff}')
}
fn xml_name_char(c: char) -> bool {
    xml_name_start(c)
        || matches!(c,'-'|'.'|'0'..='9'|'\u{b7}'|'\u{300}'..='\u{36f}'|'\u{203f}'..='\u{2040}')
}

fn parse_xml_sections(proposal: &str) -> Result<Vec<String>, String> {
    let normalized = proposal.replace("\r\n", "\n").replace('\r', "\n");
    let proposal = normalized.trim_start_matches('\u{feff}');
    if proposal.contains("]]>") {
        return Err("invalid XML character sequence".into());
    }
    if proposal.chars().any(|c| !xml_character(c)) {
        return Err("invalid XML character".into());
    }
    if proposal.contains("<!") || proposal.contains("<?") {
        return Err("XML declarations and comments are forbidden".into());
    }
    let names = [
        "USER_GOAL",
        "GLOBAL_LESSON_LEDGER",
        "DEAD_ENDS",
        "CURRENT_LOCAL_PIVOT",
    ];
    let mut stack: Vec<String> = Vec::new();
    let mut sections = std::collections::HashMap::new();
    let mut current = String::new();
    let mut root_seen = false;
    let mut rest = proposal.trim();
    while !rest.is_empty() {
        if rest.starts_with('<') {
            let end = rest.find('>').ok_or("incomplete XML tag")?;
            let tag = &rest[1..end];
            if let Some(close) = tag.strip_prefix('/') {
                let close = close.trim_end();
                if stack.pop().as_deref() != Some(close) {
                    return Err("mismatched XML tag".into());
                }
                if stack.len() == 1 {
                    sections.insert(close.to_owned(), current.clone());
                    current.clear();
                }
            } else {
                let name = tag.strip_suffix('/').unwrap_or(tag).trim_end();
                if !name.chars().next().is_some_and(xml_name_start)
                    || !name.chars().all(xml_name_char)
                {
                    return Err("XML attributes or invalid tags".into());
                }
                if stack.is_empty() {
                    if root_seen || name != "COMPACTED_STATE" {
                        return Err("invalid summary root".into());
                    }
                    root_seen = true;
                } else if stack.len() == 1 {
                    if !names.contains(&name) || sections.contains_key(name) {
                        return Err("invalid or duplicate summary section".into());
                    }
                }
                stack.push(name.to_owned());
                if tag.ends_with('/') {
                    stack.pop();
                    if stack.len() == 1 {
                        sections.insert(name.to_owned(), String::new());
                    }
                }
            }
            if stack.len() >= 2 {
                current.push('\n');
            }
            rest = &rest[end + 1..];
        } else {
            let end = rest.find('<').unwrap_or(rest.len());
            let text = &rest[..end];
            if stack.len() >= 2 {
                current.push_str(&xml_unescape(text)?);
            } else if !text.trim().is_empty() {
                return Err("text outside summary section".into());
            }
            rest = &rest[end..];
        }
    }
    if !root_seen || !stack.is_empty() || sections.len() != 4 {
        return Err("incomplete summary XML".into());
    }
    names
        .iter()
        .map(|name| {
            sections
                .remove(*name)
                .map(|s| {
                    s.split(['\n', '\r', '\u{85}', '\u{2028}', '\u{2029}'])
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .ok_or_else(|| "missing section".into())
        })
        .collect()
}

fn xml_unescape(value: &str) -> Result<String, String> {
    let mut output = String::new();
    let mut rest = value;
    while let Some(index) = rest.find('&') {
        output.push_str(&rest[..index]);
        let tail = &rest[index + 1..];
        let end = tail.find(';').ok_or("malformed XML entity reference")?;
        let entity = &tail[..end];
        let character = match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            _ if entity.starts_with("#x") => char::from_u32(
                u32::from_str_radix(&entity[2..], 16)
                    .map_err(|_| "invalid numeric XML character reference")?,
            )
            .ok_or("invalid numeric XML character reference")?,
            _ if entity.starts_with('#') => char::from_u32(
                entity[1..]
                    .parse()
                    .map_err(|_| "invalid numeric XML character reference")?,
            )
            .ok_or("invalid numeric XML character reference")?,
            _ => return Err("custom XML entities are forbidden".into()),
        };
        if !xml_character(character) {
            return Err("invalid XML character reference".into());
        }
        output.push(character);
        rest = &tail[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

fn parse_heading_sections(proposal: &str) -> Result<Vec<String>, String> {
    let names = [
        "Core Objective",
        "Universal Truths Discovered",
        "Dead Ends",
        "Current Local Pivot",
    ];
    let mut matches = Vec::new();
    for name in names {
        let heading = format!("- {name}:");
        for (position, _) in proposal.match_indices(&heading) {
            matches.push((position, heading.len(), name));
        }
    }
    matches.sort_by_key(|item| item.0);
    if matches.len() != names.len() || !proposal[..matches[0].0].trim().is_empty() {
        return Err("summary must contain exactly the four required ordered headings".into());
    }
    if matches
        .iter()
        .zip(names)
        .any(|(found, expected)| found.2 != expected)
    {
        return Err("summary headings are missing, duplicated, or out of order".into());
    }
    let mut sections = Vec::new();
    for (index, (position, heading_len, _)) in matches.iter().enumerate() {
        let content_start = position + heading_len;
        let content_end = matches.get(index + 1).map_or(proposal.len(), |next| next.0);
        sections.push(proposal[content_start..content_end].trim().to_owned());
    }
    Ok(sections)
}

pub fn render_ledger(ledger: &Ledger, hybrid: bool) -> String {
    let claims = if ledger.model_claims.is_empty() {
        "- None.".into()
    } else {
        ledger.model_claims.join("\n")
    };
    let dead_ends = if ledger.dead_ends.is_empty() {
        "- None.".into()
    } else {
        ledger.dead_ends.join("\n")
    };
    if hybrid {
        format!(
            "<COMPACTED_STATE><USER_GOAL>\n{}\n</USER_GOAL><GLOBAL_LESSON_LEDGER>\n{}\n</GLOBAL_LESSON_LEDGER><DEAD_ENDS>\n{}\n</DEAD_ENDS><CURRENT_LOCAL_PIVOT>\n{}\n</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>",
            xml_escape(ledger.goal.trim()),
            xml_escape(&claims),
            xml_escape(&dead_ends),
            xml_escape(&ledger.hypothesis)
        )
    } else {
        format!(
            "- Core Objective:\n{}\n- Universal Truths Discovered:\n{}\n- Dead Ends:\n{}\n- Current Local Pivot:\n{}",
            ledger.goal.trim(),
            claims,
            dead_ends,
            ledger.hypothesis
        )
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn number_set(text: &str) -> HashSet<String> {
    let Ok(re) = regex::Regex::new(r"[-+]?[0-9]+(,[0-9]{3})*(\.[0-9]+)?([eE][-+]?[0-9]+)?") else {
        return HashSet::new();
    };
    let mut output = HashSet::new();
    for found in re.find_iter(text) {
        let before = text[..found.start()].chars().next_back();
        let after = text[found.end()..].chars().next();
        let word = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == ',';
        if before.is_some_and(word) || after.is_some_and(word) {
            continue;
        }
        let raw = found.as_str().replace(',', "");
        if let Some(normalized) = decimal_canonical(&raw) {
            output.insert(normalized);
        }
    }
    output
}

fn decimal_canonical(value: &str) -> Option<String> {
    let value = value.trim();
    let (mantissa, exponent) = if let Some(i) = value.find(['e', 'E']) {
        (&value[..i], value[i + 1..].parse::<i32>().ok()?)
    } else {
        (value, 0)
    };
    let (negative, mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
        (true, rest)
    } else {
        (false, mantissa.strip_prefix('+').unwrap_or(mantissa))
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if !whole.chars().all(|c| c.is_ascii_digit()) || !fraction.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut digits = format!("{whole}{fraction}")
        .trim_start_matches('0')
        .to_owned();
    if digits.is_empty() {
        return Some("0".into());
    }
    let mut power = exponent.checked_sub(fraction.len() as i32)?;
    while digits.ends_with('0') {
        digits.pop();
        power = power.checked_add(1)?;
    }
    Some(format!(
        "{}{}e{power}",
        if negative { "-" } else { "" },
        digits
    ))
}

#[cfg(test)]
mod du04_context_tests {
    use super::*;

    fn message(role: &str, content: &str) -> Message {
        Message {
            role: role.into(),
            content: Some(content.into()),
            ..Default::default()
        }
    }

    fn batch(ids: &[&str]) -> Vec<Message> {
        let calls: Vec<Value> = ids.iter().map(|id| json!({"id":id,"type":"function","function":{"name":"probe","arguments":"{\"raw\":true}"}})).collect();
        let mut result = vec![Message {
            role: "assistant".into(),
            content: Some("verbatim assistant text".into()),
            tool_calls: Some(Value::Array(calls)),
            ..Default::default()
        }];
        for id in ids.iter().rev() {
            result.push(Message {
                role: "tool".into(),
                name: Some("probe".into()),
                tool_call_id: Some((*id).into()),
                content: Some(format!("raw result {id}")),
                ..Default::default()
            });
        }
        result
    }

    #[test]
    fn du04_c01_groups_complete_correlated_batches_and_expands_raw_tail() {
        let mut history = vec![message("user", "old raw user")];
        history.extend(batch(&["a", "b"]));
        history.push(message("assistant", "between"));
        history.extend(batch(&["c"]));
        let (old, tail) = split_history(&history, 1).unwrap();
        assert_eq!(old.len(), 3);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].messages.len(), 2);
        assert_eq!(
            tail[0].messages[0].content.as_deref(),
            Some("verbatim assistant text")
        );
        assert_eq!(tail[0].messages[1].tool_call_id.as_deref(), Some("c"));
        assert_eq!(flatten(&old).len() + flatten(&tail).len(), history.len());
        let mut reordered = batch(&["a", "b"]);
        reordered.swap(1, 2);
        assert!(interaction_groups(&reordered).is_ok());
        reordered[1].tool_call_id = Some("orphan".into());
        assert!(
            interaction_groups(&reordered)
                .err()
                .unwrap()
                .contains("do not correlate")
        );
        assert!(
            interaction_groups(&[message("tool", "orphan")])
                .err()
                .unwrap()
                .contains("orphaned")
        );
        assert!(
            interaction_groups(&batch(&["same", "same"]))
                .err()
                .unwrap()
                .contains("duplicate")
        );
        assert!(
            interaction_groups(&batch(&["a", "b"])[..2])
                .err()
                .unwrap()
                .contains("incomplete")
        );
    }

    #[test]
    fn du04_c01_wire_vector_and_complete_payload_budget_are_stable() {
        let wire = wire_message(&Message {
            role: "tool".into(),
            content: Some("é raw".into()),
            tool_call_id: Some("x".into()),
            name: Some("probe".into()),
            ..Default::default()
        });
        assert_eq!(
            wire,
            json!({"role":"tool","content":"é raw","name":"probe","tool_call_id":"x"})
        );
        let payload = json!({"messages":[wire],"max_tokens":40});
        let memory = Memory {
            max_tokens: 100,
            max_steps: 1,
            raw_turns_to_keep: 1,
            hybrid_reflection: true,
            run_max_steps: None,
            compaction_interval_steps: None,
            context_window_tokens: 1000,
            reserved_output_tokens: 20,
            safety_margin_tokens: 10,
            max_summary_tokens: 100,
            summary_output_tokens: 40,
            compaction_max_attempts: 2,
            compaction_max_passes: 3,
            compaction_min_growth_tokens: 1,
            compaction_min_interval_steps: 0,
            optional_compaction_min_pressure_ratio: 0.,
            optional_compaction_min_reclaimable_tokens: 0,
            optional_compaction_min_reduction_tokens: 0,
            optional_compaction_min_reduction_ratio: 0.,
        };
        let measured = estimate_payload(&payload, &memory).unwrap();
        assert_eq!(
            measured.estimated_input_tokens,
            serde_json::to_vec(&payload).unwrap().len() as i64 + 32
        );
        assert_eq!(measured.reserved_output_tokens, 40);
        assert!(measured.fits);
    }

    #[test]
    fn du04_c02_ledger_parser_enforces_structure_goal_numbers_and_byte_cap() {
        let goal = "Preserve the verified total";
        let source = "The verified total is 12.50. A rejected attempt used 9.";
        let valid = "<COMPACTED_STATE><USER_GOAL>Preserve the verified total</USER_GOAL><GLOBAL_LESSON_LEDGER>Keep verified findings.</GLOBAL_LESSON_LEDGER><DEAD_ENDS>None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>Continue from the verified total 12.5.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>";
        let (ledger, canonical) = validate_ledger(valid, goal, source, 4096, true).unwrap();
        assert_eq!(ledger.goal, goal);
        assert_eq!(ledger.verification, "unverified_model_summary");
        assert_eq!(ledger.source_sha256.len(), 64);
        assert!(canonical.len() <= 4096);
        let invalid = [
            "",
            "<COMPACTED_STATE><USER_GOAL>Preserve the verified total",
            "<COMPACTED_STATE attr=\"x\"></COMPACTED_STATE>",
            "<COMPACTED_STATE><USER_GOAL><nested>x</nested></USER_GOAL></COMPACTED_STATE>",
            "<COMPACTED_STATE><USER_GOAL>x &custom; y</USER_GOAL></COMPACTED_STATE>",
            "<COMPACTED_STATE><USER_GOAL>Preserve the verified total</USER_GOAL><GLOBAL_LESSON_LEDGER>x</GLOBAL_LESSON_LEDGER><DEAD_ENDS>x</DEAD_ENDS><CURRENT_LOCAL_PIVOT>Invented 9001</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>",
            "<COMPACTED_STATE><USER_GOAL>Preserve the verified total</USER_GOAL><GLOBAL_LESSON_LEDGER>x</GLOBAL_LESSON_LEDGER><DEAD_ENDS>x</DEAD_ENDS><CURRENT_LOCAL_PIVOT>x</CURRENT_LOCAL_PIVOT></COMPACTED_STATE> extra",
        ];
        for proposal in invalid {
            assert!(
                validate_ledger(proposal, goal, source, 4096, true).is_err(),
                "accepted invalid ledger: {proposal}"
            );
        }
        assert!(
            validate_ledger(valid, goal, source, 16, true)
                .unwrap_err()
                .contains("byte budget")
        );
        let headings = "- Core Objective:\nPreserve the verified total\n- Universal Truths Discovered:\nKeep verified findings.\n- Dead Ends:\nNone.\n- Current Local Pivot:\nContinue from total 12.5.";
        assert!(validate_ledger(headings, goal, source, 4096, false).is_ok());
    }
}

#[cfg(test)]
mod normalization_tests {
    use super::*;
    #[test]
    fn python_compaction_normalization() {
        let goal = "Preserve goal";
        let text = "```xml\n<COMPACTED_STATE><DEAD_ENDS><item>None.</item></DEAD_ENDS><USER_GOAL>model changed this</USER_GOAL><CURRENT_LOCAL_PIVOT>Continue.</CURRENT_LOCAL_PIVOT><GLOBAL_LESSON_LEDGER><item>Keep evidence.</item></GLOBAL_LESSON_LEDGER></COMPACTED_STATE>\n```";
        let (ledger, canonical) = validate_ledger(
            text,
            goal,
            "Preserve goal Keep evidence. None. Continue.",
            4096,
            true,
        )
        .unwrap();
        assert_eq!(ledger.goal, goal);
        assert_eq!(ledger.model_claims, vec!["Keep evidence."]);
        assert!(!canonical.contains("<item>"));
        assert!(!canonical.contains("model changed"));
    }
}
