//! Durable logical actions and dispatch attempts; no model-generated recovery.
use crate::{
    engine::{Config, Session, TranscriptEntry},
    identity, tools, transcript_timestamp,
};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;
fn audit(s: &mut Session, kind: &str, payload: Value) {
    s.pending_transcript.push(TranscriptEntry {
        sequence: s.next_transcript_sequence,
        message_id: Uuid::new_v4().to_string(),
        kind: kind.into(),
        payload,
        run_id: s.run_id.clone(),
        decision_step: s.step_count,
        created_at: transcript_timestamp(),
    });
    s.next_transcript_sequence += 1;
}
fn stage(s: &mut Session, e: Value) {
    if let Some(old) = s
        .executions
        .iter_mut()
        .find(|v| v["execution_id"] == e["execution_id"])
    {
        *old = e.clone();
    } else {
        s.executions.push(e.clone());
    }
    s.pending_execution_outbox
        .retain(|v| v["execution_id"] != e["execution_id"]);
    s.pending_execution_outbox.push(e);
}
async fn checkpoint<F>(s: &mut Session, emit: &mut F, event: Value) -> Result<(), String>
where
    F: FnMut(Value, Session) -> BoxFuture<'static, Result<(), String>>,
{
    if s.run_status == "blocked" && s.run_ended_at.is_none() {
        s.run_ended_at = Some(transcript_timestamp());
        s.run_termination_reason = Some("recovery_required".into());
    }
    emit(event, s.clone()).await?;
    s.acknowledge_checkpoint();
    if s.needs_reload {
        return Err("historical commit receipt requires reload".into());
    }
    s.pending_transcript.clear();
    s.pending_execution_outbox.clear();
    s.attempts_pending.clear();
    s.diagnostics_pending.clear();
    Ok(())
}
pub async fn dispatch<F>(
    s: &mut Session,
    config: &Config,
    db: &PgPool,
    emit: &mut F,
    call: &str,
    tool: &str,
    args: &Value,
) -> Result<String, String>
where
    F: FnMut(Value, Session) -> BoxFuture<'static, Result<(), String>>,
{
    let command = s
        .command
        .clone()
        .unwrap_or_else(|| json!({"id":s.run_command_id,"namespace":"regression"}));
    let namespace = command["namespace"].as_str().unwrap_or("regression");
    let mut execution = if let Some(e) = s.executions.iter().find(|e| e["call_id"] == call) {
        e.clone()
    } else {
        let id = Uuid::new_v4().to_string();
        let target = if tool == "fixture_idempotent_write" {
            "fixture://action-receipts".into()
        } else {
            format!("tool://{tool}")
        };
        let policy=config.tools.iter().find(|t|t.name==tool).and_then(|t|t.recovery.clone()).unwrap_or_else(||json!({"mode":if tool=="fixture_idempotent_write"{"idempotent"}else{"read_only"},"max_attempts":2}));
        let mut e = json!({"execution_id":id,"run_id":s.run_id,"command_id":s.run_command_id,"namespace":namespace,"target":target,"call_id":call,"tool":tool,"arguments":args,"policy":policy,"idempotency_key":identity::action(namespace,&s.id,&id)?,"payload_sha256":identity::payload(tool,&target,args)?,"key_algorithm":"agent-action-v1","attempts":0,"status":"pending","result":null});
        if tool == "fixture_idempotent_write" {
            e["recipient"] = json!({"target":"fixture://action-receipts","key_scope":"deployment","retention_seconds":null,"outcome_query":true});
        }
        stage(s, e.clone());
        audit(s, "tool_intent", e.clone());
        checkpoint(
            s,
            emit,
            json!({"type":"tool_intent","execution_id":id,"tool_call_id":call}),
        )
        .await?;
        e
    };
    let expected_target = if tool == "fixture_idempotent_write" {
        "fixture://action-receipts".to_owned()
    } else {
        format!("tool://{tool}")
    };
    if execution["namespace"] != namespace
        || execution["target"] != expected_target
        || execution["key_algorithm"] != "agent-action-v1"
        || execution["idempotency_key"]
            != identity::action(
                namespace,
                &s.id,
                execution["execution_id"]
                    .as_str()
                    .ok_or("missing action identity")?,
            )?
    {
        return Err("frozen action identity differs from its ledger-derived key".into());
    }
    if execution["tool"] != tool
        || execution["arguments"] != *args
        || execution["payload_sha256"]
            != identity::payload(tool, execution["target"].as_str().unwrap_or(""), args)?
    {
        return Err("frozen action payload changed".into());
    }
    if matches!(execution["status"].as_str(), Some("succeeded" | "failed")) {
        return execution["result"]
            .as_str()
            .map(str::to_owned)
            .ok_or("terminal action lacks result".into());
    }
    // A started attempt from a former owner has an unknown outcome. Preserve
    // its identity and record uncertainty before considering a new attempt.
    let records = execution["attempt_records"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut ordinal = records
        .iter()
        .filter_map(|a| a["attempt_number"].as_i64())
        .max()
        .unwrap_or(execution["attempts"].as_i64().unwrap_or(0));
    for mut previous in records.into_iter().filter(|a| a["status"] == "started") {
        previous["status"] = json!("uncertain");
        previous["finished_at"] = json!(transcript_timestamp());
        previous["evidence"] = json!({"reason":"previous_owner_did_not_record_an_outcome"});
        s.attempts_pending.push(previous.clone());
        audit(s, "tool_attempt_settled", previous);
    }
    if !s.attempts_pending.is_empty() {
        checkpoint(
            s,
            emit,
            json!({"type":"status","message":"Recorded prior attempt uncertainty."}),
        )
        .await?;
    }
    let mode = execution["policy"]["mode"].as_str().unwrap_or("reconcile");
    if execution["status"] != "pending" && mode == "reconcile" && tool == "fixture_idempotent_write"
    {
        ordinal += 1;
        let mut verification = json!({"attempt_id":Uuid::new_v4().to_string(),"execution_id":execution["execution_id"],"command_id":command["id"],"attempt_number":ordinal,"kind":"verify","status":"started","owner":s.execution_owner,"fence":s.execution_fence,"payload_sha256":execution["payload_sha256"],"started_at":transcript_timestamp(),"finished_at":null,"evidence":null});
        s.attempts_pending.push(verification.clone());
        audit(s, "tool_attempt", verification.clone());
        checkpoint(
            s,
            emit,
            json!({"type":"status","message":"Verifying the original recipient receipt."}),
        )
        .await?;
        let receipt=sqlx::query_as::<_,(String,Value)>("SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=$1 AND idempotency_key=$2").bind(namespace).bind(execution["idempotency_key"].as_str()).fetch_optional(db).await;
        verification["finished_at"] = json!(transcript_timestamp());
        if let Ok(Some((fingerprint, result))) = receipt {
            if execution["payload_sha256"] == fingerprint {
                let result = identity::canonical(&result)?;
                verification["status"] = json!("succeeded");
                verification["external_reference"] = execution["idempotency_key"].clone();
                verification["evidence"] =
                    json!({"source":"recipient-primary","receipt_present":true,"result":result});
                execution["status"] = json!("succeeded");
                execution["result"] = json!(result);
                execution["finished_at"] = verification["finished_at"].clone();
                s.attempts_pending.push(verification.clone());
                audit(s, "tool_attempt_settled", verification);
                stage(s, execution);
                checkpoint(
                    s,
                    emit,
                    json!({"type":"status","message":"Recovered the committed recipient outcome."}),
                )
                .await?;
                return Ok(result);
            }
        }
        verification["status"] = json!("uncertain");
        verification["evidence"] = json!({"source":"recipient-primary","outcome":"unknown"});
        s.attempts_pending.push(verification.clone());
        audit(s, "tool_attempt_settled", verification);
        checkpoint(
            s,
            emit,
            json!({"type":"status","message":"Recipient evidence did not establish an outcome."}),
        )
        .await?;
    }
    if execution["status"] != "pending"
        && !matches!(mode, "read_only" | "repeatable")
        && !(mode == "idempotent" && tool == "fixture_idempotent_write")
    {
        s.status = "failed".into();
        s.run_status = "blocked".into();
        s.last_error = Some(
            json!({"code":"recovery_required","message":"Safe recovery requires operator resolution","recoverable":true}),
        );
        execution["status"] = json!("blocked");
        stage(s, execution.clone());
        audit(s, "tool_recovery_required", execution);
        checkpoint(s,emit,json!({"type":"error","code":"recovery_required","message":"Authoritative reconciliation is required","recoverable":true})).await?;
        return Err("recovery_required".into());
    }
    let attempts = execution["attempts"].as_i64().unwrap_or(0);
    if attempts >= execution["policy"]["max_attempts"].as_i64().unwrap_or(2) {
        return Err("recovery_required: action dispatch bound reached".into());
    }
    if mode == "idempotent" && tool != "fixture_idempotent_write" {
        return Err("recovery_required: no trusted recipient guarantee".into());
    }
    let now = transcript_timestamp();
    let attempt = json!({"attempt_id":Uuid::new_v4().to_string(),"execution_id":execution["execution_id"],"command_id":command["id"],"attempt_number":ordinal+1,"kind":"dispatch","status":"started","owner":s.execution_owner,"fence":s.execution_fence,"payload_sha256":execution["payload_sha256"],"started_at":now,"finished_at":null,"evidence":null});
    execution["status"] = json!("started");
    execution["attempts"] = json!(attempts + 1);
    if execution.get("started_at").is_none() {
        execution["started_at"] = json!(now);
    }
    s.attempts_pending.push(attempt.clone());
    stage(s, execution.clone());
    audit(s, "tool_attempt", attempt.clone());
    checkpoint(s,emit,json!({"type":"tool_started","execution_id":execution["execution_id"],"attempt":attempts+1})).await?;
    let output = if tool == "fixture_idempotent_write" {
        recipient(
            db,
            namespace,
            execution["idempotency_key"].as_str().unwrap(),
            args,
        )
        .await
    } else {
        Ok(tools::execute(db, tool, args.as_object().cloned().unwrap_or_default()).await)
    };
    let mut settled = attempt;
    settled["finished_at"] = json!(transcript_timestamp());
    match output {
        Ok(result) => {
            let status = if result.starts_with("ERROR:") || result.starts_with("AssertionError:") {
                "failed"
            } else {
                "succeeded"
            };
            settled["status"] = json!(status);
            settled["evidence"] = json!({"result":result});
            execution["status"] = json!(status);
            execution["result"] = json!(result);
            execution["finished_at"] = settled["finished_at"].clone();
            s.attempts_pending.push(settled.clone());
            audit(s, "tool_attempt_settled", settled);
            stage(s, execution.clone());
            audit(
                s,
                "tool_outcome",
                json!({"execution_id":execution["execution_id"],"status":status,"result":result}),
            );
            checkpoint(
                s,
                emit,
                json!({"type":"tool_settled","execution_id":execution["execution_id"]}),
            )
            .await?;
            Ok(result)
        }
        Err(error) => {
            settled["status"] = json!("uncertain");
            settled["error"] = json!({"message":error});
            s.status = "failed".into();
            s.run_status = "blocked".into();
            s.last_error = Some(
                json!({"code":"recovery_required","message":"Action outcome is uncertain","recoverable":true}),
            );
            execution["status"] = json!("uncertain");
            s.attempts_pending.push(settled.clone());
            audit(s, "tool_attempt_settled", settled);
            stage(s, execution);
            checkpoint(s,emit,json!({"type":"error","code":"recovery_required","message":"Action outcome is uncertain","recoverable":true})).await?;
            Err("recovery_required".into())
        }
    }
}
async fn recipient(
    db: &PgPool,
    namespace: &str,
    key: &str,
    args: &Value,
) -> Result<String, String> {
    let fingerprint = identity::payload(
        "fixture_idempotent_write",
        "fixture://action-receipts",
        args,
    )?;
    let result = json!({"effect_id":key,"value":args["value"]});
    let mut tx = db.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO agent_fixture_action_receipts(namespace,idempotency_key,payload_sha256,request,result) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,idempotency_key) DO NOTHING").bind(namespace).bind(key).bind(&fingerprint).bind(args).bind(&result).execute(&mut *tx).await.map_err(|e|e.to_string())?;
    let (stored,result):(String,Value)=sqlx::query_as("SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=$1 AND idempotency_key=$2 FOR UPDATE").bind(namespace).bind(key).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
    if stored != fingerprint {
        return Err("recipient payload mismatch".into());
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    identity::canonical(&result)
}
