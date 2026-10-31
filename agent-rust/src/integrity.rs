//! Bounded projection integrity, compatible with Python's version-1 anchors.
use super::*;
fn digest(v: &Value) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(canonical_json(v.clone())?.as_bytes())
    ))
}
pub fn snapshot(s: &Session) -> Value {
    let mut v = serde_json::to_value(s).expect("session JSON");
    for key in [
        "memory",
        "compacted_context",
        "ledger_state",
        "compaction_ledger",
        "compaction_provenance",
        "executions",
        "model_request_diagnostics",
        "pending_transcript",
        "transcript_pending",
        "diagnostics_complete",
        "compaction_events",
        "compaction_attempts",
        "compaction_decisions",
    ] {
        v.as_object_mut().unwrap().remove(key);
    }
    let snapshot = digest(&v).expect("snapshot digest");
    let context = digest(&json!([s.memory, s.compacted_context, s.compaction_ledger]))
        .expect("context digest");
    v["_active_integrity"] =
        json!({"version":1,"snapshot_sha256":snapshot,"context_sha256":context});
    v
}
pub fn validate_snapshot(v: &Value) -> Result<(), String> {
    let mut copy = v.clone();
    let anchor = copy
        .as_object_mut()
        .ok_or("invalid snapshot")?
        .remove("_active_integrity")
        .ok_or("missing active integrity anchor")?;
    if anchor["version"] != 1 || anchor["snapshot_sha256"] != digest(&copy)? {
        return Err("snapshot integrity mismatch".into());
    }
    Ok(())
}
pub fn validate_context(v: &Value, s: &Session) -> Result<(), String> {
    if v["_active_integrity"]["context_sha256"]
        != digest(&json!([s.memory, s.compacted_context, s.compaction_ledger]))?
    {
        return Err("active context integrity mismatch".into());
    }
    Ok(())
}
pub const PROJECTION: &str = r#"SELECT jsonb_build_object(
 'memory',(SELECT COALESCE(jsonb_agg(t.payload || jsonb_build_object('message_id',t.message_id,'sequence',t.sequence,'created_at',to_char(t.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')) ORDER BY ids.ordinal),'[]'::jsonb)
 FROM (SELECT c.goal_message_id AS id,0::bigint AS ordinal UNION ALL SELECT value::uuid,ordinality FROM jsonb_array_elements_text(c.raw_message_ids) WITH ORDINALITY) ids JOIN agent_session_transcript t ON t.session_id=c.session_id AND t.message_id=ids.id AND t.kind='message'),
 'compacted_context',(SELECT payload->'content' FROM agent_session_transcript WHERE session_id=c.session_id AND message_id=c.ledger_record_id),
 'ledger_state',c.ledger_state,
 'executions',(SELECT COALESCE(jsonb_agg((e.payload || (to_jsonb(e)-'owner'-'payload')) || jsonb_build_object('attempt_records',(SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY attempt_number),'[]'::jsonb) FROM agent_action_attempts a WHERE a.execution_id=e.execution_id)) ORDER BY execution_id),'[]'::jsonb) FROM agent_tool_executions e WHERE e.session_id=c.session_id AND e.call_id IN (SELECT value->>'id' FROM jsonb_array_elements(COALESCE((SELECT state->'pending_tool_calls' FROM agent_sessions WHERE session_id=c.session_id),'[]'::jsonb))))) FROM agent_session_context c WHERE session_id=$1"#;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn python_integrity_anchor_and_message_projection_match() {
        let fixture: Value =
            serde_json::from_str(include_str!("../testdata/python-state-integrity-v1.json"))
                .unwrap();
        let hot = &fixture["hot"];
        validate_snapshot(hot).unwrap();
        let mut active = hot.clone();
        for key in ["memory", "compacted_context", "ledger_state"] {
            active[key] = fixture[key].clone();
        }
        let mut state: Session = serde_json::from_value(active).unwrap();
        validate_context(hot, &state).unwrap();
        state.memory[0].content = Some("tampered".into());
        assert!(validate_context(hot, &state).is_err());
    }
    #[test]
    fn anchors_reject_changes_and_keep_hot_projection_bounded() {
        let s: Session =
            serde_json::from_value(json!({"id":"session","goal":"goal","status":"active"}))
                .unwrap();
        let mut hot = snapshot(&s);
        assert!(hot.get("memory").is_none());
        validate_snapshot(&hot).unwrap();
        validate_context(&hot, &s).unwrap();
        hot["goal"] = json!("changed");
        assert!(validate_snapshot(&hot).is_err());
    }
}
