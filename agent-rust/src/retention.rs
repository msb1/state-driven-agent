//! Explicit operator administration; the server never migrates schema.
use super::*;
use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
};
pub const TABLES: &[(&str, &str)] = &[
    ("agent_sessions", "session_id"),
    ("agent_session_context", "session_id"),
    ("agent_session_transcript", "sequence"),
    ("agent_session_events", "event_id"),
    ("agent_session_diagnostics", "family,ordinal"),
    ("agent_runs", "started_at,run_id"),
    ("agent_tool_executions", "execution_id"),
    ("agent_session_calls", "call_id,message_id"),
    ("agent_archive_receipts", "archive_id"),
    ("agent_commands", "created_at,command_id"),
    ("agent_commit_receipts", "committed_revision"),
    ("agent_action_attempts", "execution_id,attempt_number"),
];
fn line(v: &Value) -> Result<Vec<u8>, String> {
    Ok((canonical_json(v.clone())? + "\n").into_bytes())
}
fn protocol(e: impl ToString) -> sqlx::Error {
    sqlx::Error::Protocol(e.to_string())
}
pub fn verify(path: &str) -> Result<Value, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut stream = BufReader::new(file).lines();
    let header: Value = tools::parse_strict_json(
        &stream
            .next()
            .ok_or("empty archive")?
            .map_err(|e| e.to_string())?,
    )?;
    if header["format"] != "sda-archive-2"
        || header.as_object().map(|v| v.len()) != Some(4)
        || header["snapshot_revision"].as_i64().unwrap_or(0) < 1
    {
        return Err("invalid archive header".into());
    }
    for key in ["archive_id", "session_id"] {
        Uuid::parse_str(header[key].as_str().ok_or("missing archive UUID")?)
            .map_err(|e| e.to_string())?;
    }
    let mut digest = Sha256::new();
    digest.update(line(&header)?);
    let mut hashes: HashMap<&str, Sha256> =
        TABLES.iter().map(|(t, _)| (*t, Sha256::new())).collect();
    let mut counts: HashMap<&str, i64> = TABLES.iter().map(|(t, _)| (*t, 0)).collect();
    let mut watermarks = json!({"events":0,"diagnostics":{}});
    let mut manifest = None;
    for raw in stream {
        let v = tools::parse_strict_json(&raw.map_err(|e| e.to_string())?)?;
        if manifest.is_some() {
            return Err("trailing archive data".into());
        }
        if let Some(m) = v.get("manifest") {
            manifest = Some(m.clone());
            continue;
        }
        let table = v["table"].as_str().ok_or("missing table")?;
        if !hashes.contains_key(table)
            || v["row"]["session_id"] != header["session_id"]
            || v.as_object().map(|o| o.len()) != Some(2)
        {
            return Err("unknown table or cross-session archive row".into());
        }
        let bytes = line(&v)?;
        digest.update(&bytes);
        hashes.get_mut(table).unwrap().update(bytes);
        *counts.get_mut(table).unwrap() += 1;
        if table == "agent_session_events" {
            watermarks["events"] = json!(
                watermarks["events"]
                    .as_i64()
                    .unwrap()
                    .max(v["row"]["event_id"].as_i64().ok_or("invalid event id")?)
            );
        }
        if table == "agent_session_diagnostics" {
            let family = v["row"]["family"].as_str().ok_or("missing family")?;
            if !diagnostics::FAMILIES.contains(&family) {
                return Err("invalid diagnostic family".into());
            }
            watermarks["diagnostics"][family] = json!(
                watermarks["diagnostics"][family]
                    .as_i64()
                    .unwrap_or(0)
                    .max(v["row"]["ordinal"].as_i64().ok_or("invalid ordinal")?)
            );
        }
    }
    let m = manifest.ok_or("archive manifest missing")?;
    let expected_hashes: HashMap<_, _> = hashes
        .into_iter()
        .map(|(t, h)| (t, format!("{:x}", h.finalize())))
        .collect();
    if m["sha256"] != format!("{:x}", digest.finalize())
        || m["counts"] != json!(counts)
        || m["table_sha256"] != json!(expected_hashes)
        || m["watermarks"] != watermarks
    {
        return Err("archive manifest mismatch".into());
    }
    let mut result = header;
    for (key, v) in m.as_object().ok_or("invalid manifest")? {
        result[key] = v.clone();
    }
    result["archive_path"] = json!(
        std::fs::canonicalize(path)
            .map_err(|e| e.to_string())?
            .to_string_lossy()
    );
    Ok(result)
}
async fn export(a: &App, id: Uuid, path: &str) -> Result<Value, sqlx::Error> {
    let mut tx = a.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL TIME ZONE 'UTC'")
        .execute(&mut *tx)
        .await?;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM agent_sessions WHERE session_id=$1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(protocol)?;
    let header = json!({"format":"sda-archive-2","archive_id":Uuid::new_v4(),"session_id":id,"snapshot_revision":revision});
    let bytes = line(&header).map_err(protocol)?;
    file.write_all(&bytes).map_err(protocol)?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    let mut counts = serde_json::Map::new();
    let mut hashes = serde_json::Map::new();
    let mut watermarks = json!({"events":0,"diagnostics":{}});
    for (table, order) in TABLES {
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "SELECT to_jsonb(t) FROM {table} t WHERE session_id=$1 ORDER BY {order}"
        ))
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        let mut hash = Sha256::new();
        counts.insert((*table).into(), json!(rows.len()));
        for row in rows {
            if *table == "agent_session_events" {
                watermarks["events"] = row["event_id"].clone();
            }
            if *table == "agent_session_diagnostics" {
                watermarks["diagnostics"][row["family"].as_str().unwrap()] = row["ordinal"].clone();
            }
            let bytes = line(&json!({"table":table,"row":row})).map_err(protocol)?;
            digest.update(&bytes);
            hash.update(&bytes);
            file.write_all(&bytes).map_err(protocol)?;
        }
        hashes.insert((*table).into(), json!(format!("{:x}", hash.finalize())));
    }
    let manifest = json!({"counts":counts,"table_sha256":hashes,"watermarks":watermarks,"sha256":format!("{:x}",digest.finalize())});
    file.write_all(&line(&json!({"manifest":manifest})).map_err(protocol)?)
        .map_err(protocol)?;
    file.sync_all().map_err(protocol)?;
    tx.commit().await?;
    verify(path).map_err(protocol)
}
pub async fn cli(a: &App, args: &[String]) -> Result<Value, sqlx::Error> {
    let action = args
        .first()
        .map(String::as_str)
        .ok_or_else(|| protocol("retention {plan|export|verify|prune-telemetry}"))?;
    let option = |name: &str| args.windows(2).find(|p| p[0] == name).map(|p| p[1].clone());
    let path = option("--archive").unwrap_or_default();
    if action == "verify" {
        return verify(&path).map_err(protocol);
    }
    let id = Uuid::parse_str(&option("--session-id").unwrap_or_default()).map_err(protocol)?;
    if action == "export" {
        return export(a, id, &path).await;
    }
    let (mut state, config) = load(a, &id.to_string())
        .await
        .ok_or_else(|| protocol("invalid session snapshot"))?;
    if action == "plan" {
        let response = session_retention_plan(State(a.clone()), Path(id.to_string()))
            .await
            .into_response();
        if response.status() != StatusCode::OK {
            return Err(protocol("retention plan unavailable"));
        }
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .map_err(protocol)?;
        return serde_json::from_slice(&bytes).map_err(protocol);
    }
    if action != "prune-telemetry" {
        return Err(protocol("unknown retention action"));
    }
    let mut manifest = verify(&path).map_err(protocol)?;
    if manifest["session_id"] != id.to_string() || manifest["snapshot_revision"] != state.revision {
        return Err(protocol("stale or wrong-session export; export again"));
    }
    let keep_diagnostics = option("--keep-diagnostics")
        .unwrap_or("32".into())
        .parse::<i64>()
        .map_err(protocol)?;
    let keep_events = option("--keep-events")
        .unwrap_or("100".into())
        .parse::<i64>()
        .map_err(protocol)?;
    if keep_diagnostics < 32 || keep_events < 1 {
        return Err(protocol("retain at least 32 diagnostics and one event"));
    }
    let previous: Vec<(String, Value)> =
        sqlx::query_as("SELECT sha256,manifest FROM agent_archive_receipts WHERE session_id=$1")
            .bind(id)
            .fetch_all(&a.db)
            .await?;
    for (hash, receipt) in previous {
        let prior = verify(
            receipt["archive_path"]
                .as_str()
                .ok_or_else(|| protocol("archive dependency path missing"))?,
        )
        .map_err(protocol)?;
        if prior["sha256"] != hash {
            return Err(protocol("prior archive dependency is corrupt"));
        }
    }
    let namespace = env::var("AGENT_DEPLOYMENT_NAMESPACE").map_err(protocol)?;
    let command = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let input = json!({"session_id":id,"archive_id":manifest["archive_id"]});
    let hash = identity::digest(&json!([
        "agent-command-v1",
        "POST",
        "/admin/retention/prune",
        input
    ]))
    .map_err(protocol)?;
    sqlx::query("INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,session_id,status,owner,fence,lease_expires_at) VALUES($1,$2,'local',$3,'POST','/admin/retention/prune',$4,$5,$6,'started',$7,1,clock_timestamp()+interval '300 seconds')").bind(command).bind(&namespace).bind(command.to_string()).bind(json!({"input":input,"admission":{}})).bind(hash).bind(id).bind(owner).execute(&a.db).await?;
    let (lease, fence) = acquire_execution_lease(&a.db, id).await?;
    state.command = Some(
        json!({"id":command,"owner":owner,"fence":1,"namespace":namespace,"route":"/admin/retention/prune"}),
    );
    state.execution_owner = Some(lease.to_string());
    state.execution_fence = Some(fence);
    manifest["keep_diagnostics"] = json!(keep_diagnostics);
    manifest["keep_events"] = json!(keep_events);
    state.retention_pending = Some(manifest.clone());
    state.last_archive_id = manifest["archive_id"].as_str().map(str::to_owned);
    let result = save(
        a,
        &mut state,
        &config,
        json!({"type":"telemetry_archived","archive_id":manifest["archive_id"]}),
    )
    .await;
    release_execution_lease(&a.db, id, lease, fence).await;
    result?;
    Ok(json!({"session_id":id,"revision":state.revision,"archive_id":state.last_archive_id}))
}
pub async fn write(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    s: &Session,
) -> Result<(), sqlx::Error> {
    let Some(item) = &s.retention_pending else {
        return Ok(());
    };
    let id = Uuid::parse_str(&s.id).map_err(protocol)?;
    if s.execution_owner.is_none()
        || s.status == "active"
        || !s.pending_tool_calls.is_empty()
        || item["snapshot_revision"].as_i64() != Some(s.revision - 1)
    {
        return Err(protocol(
            "retention requires terminal owned matching revision",
        ));
    }
    let unresolved:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM agent_runs WHERE session_id=$1 AND status='running')+(SELECT count(*) FROM agent_tool_executions WHERE session_id=$1 AND status IN ('pending','started','uncertain','blocked'))").bind(id).fetch_one(&mut **tx).await?;
    if unresolved != 0 {
        return Err(protocol("unresolved recovery evidence prevents pruning"));
    }
    sqlx::query("SET LOCAL TIME ZONE 'UTC'")
        .execute(&mut **tx)
        .await?;
    for (table, condition, parameter, order) in [
        (
            "agent_session_events",
            "event_id<=($2::jsonb->>'events')::bigint",
            &item["watermarks"],
            "event_id",
        ),
        (
            "agent_session_diagnostics",
            "ordinal<=COALESCE(($2::jsonb->>family)::bigint,0)",
            &item["watermarks"]["diagnostics"],
            "family,ordinal",
        ),
    ] {
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "SELECT to_jsonb(t) FROM {table} t WHERE session_id=$1 AND {condition} ORDER BY {order}"
        ))
        .bind(id)
        .bind(parameter)
        .fetch_all(&mut **tx)
        .await?;
        let mut hash = Sha256::new();
        for row in rows {
            hash.update(line(&json!({"table":table,"row":row})).map_err(protocol)?);
        }
        if item["table_sha256"][table] != format!("{:x}", hash.finalize()) {
            return Err(protocol("archive telemetry digest differs; nothing pruned"));
        }
    }
    sqlx::query("INSERT INTO agent_archive_receipts(archive_id,session_id,snapshot_revision,sha256,manifest) VALUES($1::uuid,$2,$3,$4,$5)").bind(item["archive_id"].as_str()).bind(id).bind(item["snapshot_revision"].as_i64()).bind(item["sha256"].as_str()).bind(item).execute(&mut **tx).await?;
    for family in diagnostics::FAMILIES {
        let cutoff = item["watermarks"]["diagnostics"][family]
            .as_i64()
            .unwrap_or(0)
            .min(
                s.diagnostic_counts.get(family).copied().unwrap_or(0)
                    - item["keep_diagnostics"].as_i64().unwrap(),
            );
        sqlx::query("DELETE FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal<=$3").bind(id).bind(family).bind(cutoff).execute(&mut **tx).await?;
    }
    sqlx::query("DELETE FROM agent_session_events WHERE session_id=$1 AND event_id<=$2 AND event_id NOT IN (SELECT event_id FROM agent_session_events WHERE session_id=$1 ORDER BY event_id DESC LIMIT $3)").bind(id).bind(item["watermarks"]["events"].as_i64()).bind(item["keep_events"].as_i64()).execute(&mut **tx).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_python_archive_and_rejects_truncation_tampering_and_trailing_data() {
        let path = std::env::temp_dir().join(format!("sda-rust-archive-{}.jsonl", Uuid::new_v4()));
        let path = path.to_str().unwrap();
        let fixture = include_str!("../testdata/python-archive-v2.jsonl.fixture");
        std::fs::write(path, fixture).unwrap();
        let result = verify(path).unwrap();
        assert_eq!(result["counts"]["agent_commands"], 1);
        assert_eq!(result["counts"]["agent_sessions"], 1);
        for invalid in [
            fixture.replace("café", "changed"),
            fixture.lines().take(2).collect::<Vec<_>>().join("\n"),
            format!("{fixture}{{}}\n"),
        ] {
            std::fs::write(path, invalid).unwrap();
            assert!(verify(path).is_err());
        }
        std::fs::remove_file(path).unwrap();
    }
}
