//! Operator recovery of expired lifecycles; this never invokes models or tools.
use super::*;
pub async fn cli(a: &App, args: &[String]) -> Result<Value, sqlx::Error> {
    let limit = args
        .windows(2)
        .find(|v| v[0] == "--limit")
        .map(|v| v[1].parse::<i64>())
        .transpose()
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?
        .unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(sqlx::Error::Protocol(
            "limit must be between 1 and 1000".into(),
        ));
    }
    let candidates:Vec<(Uuid,Uuid)>=sqlx::query_as("SELECT r.session_id,r.run_id FROM agent_runs r JOIN agent_sessions s USING(session_id) WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp()) ORDER BY r.started_at LIMIT $1").bind(limit).fetch_all(&a.db).await?;
    let mut completed = Vec::new();
    for (id, run) in candidates {
        let command = commands::administrative(
            a,
            id,
            "/admin/reconcile",
            json!({"session_id":id,"run_id":run}),
        )
        .await?;
        let (owner, fence) = match acquire_execution_lease(&a.db, id).await {
            Ok(v) => v,
            Err(_) => {
                let mut tx = a.db.begin().await?;
                commands::finish(
                    &mut tx,
                    &command,
                    409,
                    &json!({"detail":"Session ownership changed"}),
                )
                .await?;
                tx.commit().await?;
                continue;
            }
        };
        let result = async {
            let (mut state, config) = load(a, &id.to_string())
                .await
                .ok_or_else(|| sqlx::Error::Protocol("invalid session snapshot".into()))?;
            if state.run_id.as_deref() != Some(run.to_string().as_str())
                || state.run_status != "running"
            {
                let mut tx = a.db.begin().await?;
                commands::finish(
                    &mut tx,
                    &command,
                    200,
                    &json!({"message":"Lifecycle already settled"}),
                )
                .await?;
                tx.commit().await?;
                return Ok::<bool, sqlx::Error>(false);
            }
            state.command = Some(command);
            state.execution_owner = Some(owner.to_string());
            state.execution_fence = Some(fence);
            state.run_status = "interrupted".into();
            state.run_ended_at = Some(transcript_timestamp());
            state.run_termination_reason = Some("worker_abandoned".into());
            state.status = "failed".into();
            state.final_answer =
                Some("Abandoned run detected; use /resume to recover committed work.".into());
            state.last_error = Some(
                json!({"code":"run_abandoned","message":state.final_answer,"recoverable":true}),
            );
            save(
                a,
                &mut state,
                &config,
                json!({"type":"run_reconciled","run_id":run,"reason":"worker_abandoned"}),
            )
            .await?;
            Ok(true)
        }
        .await;
        release_execution_lease(&a.db, id, owner, fence).await;
        if result? {
            completed.push(json!({"session_id":id,"run_id":run}));
        }
    }
    Ok(json!({"reconciled":completed}))
}
