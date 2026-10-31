//! Append-only diagnostic identities, bounded read caches and commit outboxes.
use crate::engine::Session;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const RECENT: usize = 32;
pub const FAMILIES: [&str; 3] = [
    "compaction_events",
    "compaction_attempts",
    "compaction_decisions",
];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub family: String,
    pub ordinal: i64,
    pub payload: Value,
}

/// Import a previously unjournaled snapshot once, before producing new records.
pub fn stage(state: &mut Session) -> Result<(), String> {
    if !state.diagnostic_counts.is_empty() {
        return Ok(());
    }
    let events = std::mem::take(&mut state.compaction_events);
    let attempts = std::mem::take(&mut state.compaction_attempts);
    let decisions = std::mem::take(&mut state.compaction_decisions);
    state.diagnostic_rejections = 0;
    state.compaction_savings_total = 0;
    let accepted = attempts
        .iter()
        .filter(|item| item["accepted"] == true)
        .count();
    state.compaction_savings_known = accepted == events.len();
    for payload in events.into_iter().map(Value::String) {
        append(state, FAMILIES[0], payload)?;
    }
    for payload in attempts {
        append(state, FAMILIES[1], payload)?;
    }
    for payload in decisions {
        append(state, FAMILIES[2], payload)?;
    }
    Ok(())
}

pub fn record(state: &mut Session, family: &str, payload: Value) -> Result<(), String> {
    if !FAMILIES.contains(&family) {
        return Err(format!("unknown diagnostic family '{family}'"));
    }
    if family == FAMILIES[0] && !payload.is_string() {
        return Err("compaction event must contain a summary string".into());
    }
    stage(state)?;
    append(state, family, payload)
}

fn append(state: &mut Session, family: &str, payload: Value) -> Result<(), String> {
    let ordinal = state
        .diagnostic_counts
        .get(family)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or("diagnostic ordinal overflow")?;
    match family {
        "compaction_events" => {
            state
                .compaction_events
                .push(payload.as_str().ok_or("invalid compaction event")?.into());
            trim(&mut state.compaction_events);
        }
        "compaction_attempts" => {
            if payload["accepted"] != true {
                state.diagnostic_rejections += 1;
            } else {
                let reduction = payload["reduction_tokens"].as_i64().or_else(|| {
                    payload["before_estimated_tokens"]
                        .as_i64()?
                        .checked_sub(payload["after_estimated_tokens"].as_i64()?)
                });
                if let Some(reduction) = reduction {
                    state.compaction_savings_total += reduction;
                } else {
                    state.compaction_savings_known = false;
                }
            }
            state.compaction_attempts.push(payload.clone());
            trim(&mut state.compaction_attempts);
        }
        "compaction_decisions" => {
            state.compaction_decisions.push(payload.clone());
            trim(&mut state.compaction_decisions);
        }
        _ => return Err(format!("unknown diagnostic family '{family}'")),
    }
    state.diagnostic_counts.insert(family.into(), ordinal);
    state.diagnostics_pending.push(Record {
        family: family.into(),
        ordinal,
        payload,
    });
    refresh_completeness(state);
    Ok(())
}

fn trim<T>(items: &mut Vec<T>) {
    if items.len() > RECENT {
        items.drain(..items.len() - RECENT);
    }
}

fn refresh_completeness(state: &mut Session) {
    state.diagnostics_complete = [
        state.compaction_events.len(),
        state.compaction_attempts.len(),
        state.compaction_decisions.len(),
    ]
    .into_iter()
    .zip(FAMILIES)
    .all(|(len, family)| state.diagnostic_counts.get(family).copied().unwrap_or(0) == len as i64);
}

pub fn snapshot(state: &Session) -> Value {
    crate::integrity::snapshot(state)
}

/// Read archives and the hot snapshot from one database statement/snapshot.
pub fn hydrate(
    state: &mut Session,
    records: Vec<Record>,
    counts: BTreeMap<String, i64>,
) -> Result<(), String> {
    let legacy = state.diagnostic_counts.is_empty();
    if legacy {
        stage(state)?;
        // Old Rust snapshots held the complete arrays. Validate every old
        // identity before replacing their projection with bounded caches.
        // SQL orders by family rather than the production order.
        let mut expected = state.diagnostics_pending.clone();
        expected.sort_by(|a, b| (&a.family, a.ordinal).cmp(&(&b.family, b.ordinal)));
        if expected != records {
            return Err("legacy diagnostic snapshot differs from archived history".into());
        }
    }
    if state
        .diagnostic_counts
        .keys()
        .any(|family| !FAMILIES.contains(&family.as_str()))
    {
        return Err("unknown diagnostic counter family".into());
    }
    state.compaction_events.clear();
    state.compaction_attempts.clear();
    state.compaction_decisions.clear();
    for family in FAMILIES {
        let expected = state.diagnostic_counts.get(family).copied().unwrap_or(0);
        let actual = counts.get(family).copied().unwrap_or(0);
        if expected < 0 || actual != expected {
            return Err(format!(
                "diagnostic counter differs from archived history: family={family} expected={expected} stored={actual}"
            ));
        }
        let rows: Vec<_> = records.iter().filter(|row| row.family == family).collect();
        let retained = if legacy {
            expected as usize
        } else {
            (expected as usize).min(RECENT)
        };
        if rows.len() != retained
            || rows
                .iter()
                .enumerate()
                .any(|(index, row)| row.ordinal != expected - retained as i64 + index as i64 + 1)
        {
            return Err(format!(
                "diagnostic recent history has a gap: family={family}"
            ));
        }
        let start = rows.len().saturating_sub(RECENT);
        for row in rows.into_iter().skip(start) {
            match family {
                "compaction_events" => state.compaction_events.push(
                    row.payload
                        .as_str()
                        .ok_or("invalid archived compaction event")?
                        .into(),
                ),
                "compaction_attempts" => state.compaction_attempts.push(row.payload.clone()),
                "compaction_decisions" => state.compaction_decisions.push(row.payload.clone()),
                _ => unreachable!(),
            }
        }
    }
    state.diagnostics_pending.clear();
    refresh_completeness(state);
    Ok(())
}

pub fn conflict(family: &str, ordinal: i64, expected: &Value, stored: &Value) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "diagnostic ordinal reused with changed payload: family={family} ordinal={ordinal} proposed_sha256={:x} stored_sha256={:x}",
        Sha256::digest(crate::canonical_json(expected.clone()).unwrap().as_bytes()),
        Sha256::digest(crate::canonical_json(stored.clone()).unwrap().as_bytes())
    )
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
