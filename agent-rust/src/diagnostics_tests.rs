use super::*;
use serde_json::json;
use uuid::Uuid;

fn session() -> Session {
    serde_json::from_value(
        json!({"id":Uuid::new_v4().to_string(),"goal":"diagnostic regression","status":"active"}),
    )
    .unwrap()
}

#[test]
fn journal_preserves_lifetime_ordinals_and_independent_pending_payloads() {
    let mut state = session();
    let mut payload = json!({"accepted":false,"reason":"rejected"});
    record(&mut state, FAMILIES[1], payload.clone()).unwrap();
    payload["reason"] = json!("changed outside the outbox");
    for index in 1..45 {
        record(
            &mut state,
            FAMILIES[1],
            json!({"accepted":true,"reduction_tokens":index}),
        )
        .unwrap();
    }
    assert_eq!(state.diagnostic_counts[FAMILIES[1]], 45);
    assert_eq!(state.diagnostics_pending.len(), 45);
    assert_eq!(state.diagnostics_pending[0].payload["reason"], "rejected");
    assert_eq!(state.diagnostics_pending[44].ordinal, 45);
    assert_eq!(state.compaction_attempts.len(), RECENT);
    assert!(!state.diagnostics_complete);
    assert_eq!(state.diagnostic_rejections, 1);
    assert_eq!(state.compaction_savings_total, (1..45).sum::<i64>());
    let hot = snapshot(&state);
    assert!(FAMILIES.iter().all(|family| hot.get(*family).is_none()));
    assert!(hot.get("diagnostics_pending").is_none());
    assert!(hot.get("diagnostics_complete").is_none());
}

#[test]
fn diagnostic_ratios_survive_json_roundtrips_without_false_identity_changes() {
    // The archived Test 4 fails at 15700/31488: 0.49860264227642276
    // decodes as 0.4986026422764227 without float_roundtrip. In total, 3,868
    // of these pressures fail the former insert/read-back identity check.
    for tokens in 0..31488 {
        let proposed = json!({"assessment":{"pressure_ratio":tokens as f64/31488.0}});
        let restored: Value = serde_json::from_str(&proposed.to_string()).unwrap();
        assert_eq!(
            restored, proposed,
            "diagnostic JSON roundtrip for tokens={tokens}"
        );
    }
}

#[test]
fn hydration_restores_bounded_tails_and_resumes_lifetime_ordinals() {
    let mut original = session();
    for index in 0..45 {
        record(
            &mut original,
            FAMILIES[2],
            json!({"step":index,"reason":"spacing"}),
        )
        .unwrap();
    }
    let archived = original.diagnostics_pending.clone();
    let mut restored: Session = serde_json::from_value(snapshot(&original)).unwrap();
    hydrate(
        &mut restored,
        archived[13..].to_vec(),
        original.diagnostic_counts.clone(),
    )
    .unwrap();
    assert_eq!(restored.compaction_decisions.len(), RECENT);
    assert_eq!(restored.compaction_decisions[0]["step"], 13);
    assert!(restored.diagnostics_pending.is_empty());
    assert!(!restored.diagnostics_complete);
    record(&mut restored, FAMILIES[2], json!({"step":45})).unwrap();
    assert_eq!(restored.diagnostics_pending.len(), 1);
    assert_eq!(restored.diagnostics_pending[0].ordinal, 46);

    let mut invalid: Session = serde_json::from_value(snapshot(&original)).unwrap();
    let mut counts = original.diagnostic_counts.clone();
    counts.insert(FAMILIES[2].into(), 44);
    assert!(
        hydrate(&mut invalid, archived[13..].to_vec(), counts)
            .unwrap_err()
            .contains("counter differs")
    );
}

#[test]
fn existing_unjournaled_rust_snapshots_are_validated_and_imported_once() {
    let mut old = session();
    old.compaction_events.push("summary".into());
    old.compaction_attempts
        .push(json!({"accepted":true,"reduction_tokens":50}));
    old.compaction_decisions.push(json!({"outcome":"accepted"}));
    let mut staged = old.clone();
    stage(&mut staged).unwrap();
    let mut archived = staged.diagnostics_pending.clone();
    archived.sort_by(|a, b| (&a.family, a.ordinal).cmp(&(&b.family, b.ordinal)));
    let counts = staged.diagnostic_counts.clone();
    let mut corrupt = old.clone();
    corrupt.compaction_attempts[0]["reduction_tokens"] = json!(500);
    assert!(hydrate(&mut corrupt, archived.clone(), counts.clone()).is_err());
    hydrate(&mut old, archived, counts).unwrap();
    assert!(old.diagnostics_pending.is_empty());
    assert_eq!(old.compaction_savings_total, 50);
    record(&mut old, FAMILIES[1], json!({"accepted":false})).unwrap();
    assert_eq!(old.diagnostics_pending[0].ordinal, 2);
}
