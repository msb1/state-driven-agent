//! Axum REST/SSE service. PostgreSQL is the authority for every session.
mod actions;
mod commands;
mod context;
mod diagnostics;
mod engine;
mod events;
mod identity;
mod integrity;
mod provider_log;
mod reconcile;
mod retention;
mod sse;
mod tools;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Sse},
    routing::{get, post},
};
use engine::{Config, Engine, Message, Session, TranscriptEntry, Workflow};
use futures_util::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{
    collections::HashMap,
    env,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicI64, Ordering},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;
#[derive(Clone)]
struct App {
    db: PgPool,
    fixtures: PgPool,
    root: PathBuf,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

async fn validate_database_schema(db: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(include_str!("../../schema/validate.sql"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

async fn session_lock(app: &App, id: &str) -> Option<OwnedMutexGuard<()>> {
    let lock = {
        let mut locks = app.locks.lock().await;
        locks
            .entry(id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    lock.try_lock_owned().ok()
}
async fn acquire_execution_lease(db: &PgPool, session: Uuid) -> Result<(Uuid, i64), sqlx::Error> {
    let owner = Uuid::new_v4();
    let fence: Option<i64> = sqlx::query_scalar("UPDATE agent_sessions SET lease_owner=$2,lease_fence=lease_fence+1,lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE session_id=$1 AND (lease_owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING lease_fence")
        .bind(session).bind(owner).fetch_optional(db).await?;
    fence.map(|fence| (owner, fence)).ok_or_else(|| {
        sqlx::Error::Protocol("session ownership conflict: lease held by another worker".into())
    })
}
async fn renew_execution_lease(
    db: &PgPool,
    session: Uuid,
    owner: Uuid,
    fence: i64,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query("UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE session_id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_expires_at>clock_timestamp()")
        .bind(session).bind(owner).bind(fence).execute(db).await?;
    if result.rows_affected() != 1 {
        return Err(sqlx::Error::Protocol(
            "lease lost: owner or fence changed".into(),
        ));
    }
    Ok(())
}
async fn release_execution_lease(db: &PgPool, session: Uuid, owner: Uuid, fence: i64) {
    let _ = sqlx::query("UPDATE agent_sessions SET lease_owner=NULL,lease_expires_at=NULL WHERE session_id=$1 AND lease_owner=$2 AND lease_fence=$3")
        .bind(session).bind(owner).bind(fence).execute(db).await;
}
fn start_execution_heartbeat(
    db: PgPool,
    session: Uuid,
    owner: Uuid,
    fence: i64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            if renew_execution_lease(&db, session, owner, fence)
                .await
                .is_err()
            {
                break;
            }
        }
    })
}
fn validation_error(errors: Vec<Value>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"detail": errors})),
    )
}
pub(crate) fn transcript_timestamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (seconds / 86400) as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let rem = seconds % 86400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}
fn field_error(
    field: &str,
    value: Option<&Value>,
    kind: &str,
    message: &str,
    ctx: Option<Value>,
) -> Value {
    let mut error = json!({"loc":["body",field],"msg":message,"type":kind});
    if let Some(value) = value {
        error["input"] = value.clone();
    }
    if let Some(ctx) = ctx {
        error["ctx"] = ctx;
    }
    error
}
fn validate_max_steps(q: &Value) -> Option<Value> {
    let value = q.get("max_steps")?;
    if value.is_null() {
        return None;
    }
    let n = coerce_integer(value);
    match n {
        None => Some(field_error(
            "max_steps",
            Some(value),
            if value.is_string() {
                "int_parsing"
            } else {
                "int_type"
            },
            if value.is_string() {
                "Input should be a valid integer, unable to parse string as an integer"
            } else {
                "Input should be a valid integer"
            },
            None,
        )),
        Some(x) if x < 1 => Some(field_error(
            "max_steps",
            Some(value),
            "greater_than_equal",
            "Input should be greater than or equal to 1",
            Some(json!({"ge":1})),
        )),
        Some(x) if x > 50 => Some(field_error(
            "max_steps",
            Some(value),
            "less_than_equal",
            "Input should be less than or equal to 50",
            Some(json!({"le":50})),
        )),
        _ => None,
    }
}
fn coerce_integer(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(f) = value.as_f64() {
        return (f.fract() == 0. && f >= i64::MIN as f64 && f <= i64::MAX as f64)
            .then_some(f as i64);
    }
    if let Some(s) = value.as_str() {
        return s.trim().parse().ok();
    }
    value.as_bool().map(|x| if x { 1 } else { 0 })
}
fn validate_body(q: &Value) -> Option<Value> {
    (!q.is_object()).then(|| json!({"loc":["body"],"msg":"Input should be a valid dictionary or object to extract fields from","type":"model_attributes_type","input":q}))
}
#[tokio::main]
async fn main() {
    load_dotenv();
    let startup_args: Vec<String> = env::args().skip(1).collect();
    if startup_args.first().map(String::as_str) == Some("retention")
        && startup_args.get(1).map(String::as_str) == Some("verify")
    {
        let path = startup_args
            .windows(2)
            .find(|v| v[0] == "--archive")
            .map(|v| v[1].as_str())
            .unwrap_or("");
        match retention::verify(path) {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
        return;
    }
    assert!(
        !env::var("AGENT_DEPLOYMENT_NAMESPACE")
            .unwrap_or_default()
            .is_empty(),
        "AGENT_DEPLOYMENT_NAMESPACE is required"
    );
    let default_db = "postgresql://user:password@192.168.1.50:5432/elite_rag";
    let session_url = env::var("AGENT_SESSION_DATABASE_URL").unwrap_or_else(|_| default_db.into());
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&session_url)
        .await
        .expect("postgres");
    let fixtures_url =
        env::var("AGENT_FIXTURE_DATABASE_URL").unwrap_or_else(|_| session_url.clone());
    let fixtures = PgPoolOptions::new()
        .max_connections(10)
        .connect_lazy(&fixtures_url)
        .expect("fixture database URL");
    validate_database_schema(&db).await.expect("Shared PostgreSQL schema is missing or incompatible; run agent-python/scripts/initialize_database.py explicitly; servers do not change the schema");
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("reconcile-runs") {
        let app = App {
            db,
            fixtures,
            root: config_root(),
            locks: Arc::new(Mutex::new(HashMap::new())),
        };
        match reconcile::cli(&app, &args[1..]).await {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
        return;
    }
    if args.first().map(String::as_str) == Some("retention") {
        let app = App {
            db,
            fixtures,
            root: config_root(),
            locks: Arc::new(Mutex::new(HashMap::new())),
        };
        match retention::cli(&app, &args[1..]).await {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
        return;
    }
    let app = Router::new()
        .route("/health", get(health))
        .route("/openapi.json", get(openapi))
        .route("/docs", get(docs))
        .route("/docs/oauth2-redirect", get(oauth2_redirect))
        .route("/redoc", get(redoc))
        .route("/configs", get(configs))
        .route("/commands/{id}", get(commands::get))
        .route("/sessions", post(create))
        .route("/sessions/{id}", get(session))
        .route("/sessions/{id}/executions", get(executions))
        .route("/sessions/{id}/transcript", get(transcript))
        .route("/sessions/{id}/events", get(session_events))
        .route("/sessions/{id}/events/stream", get(events::stream))
        .route("/sessions/{id}/runs", get(session_runs))
        .route("/sessions/{id}/archives", get(session_archives))
        .route("/sessions/{id}/retention", get(session_retention_plan))
        .route("/runs/abandoned", get(abandoned_runs))
        .route(
            "/sessions/{id}/diagnostics/{family}",
            get(compaction_diagnostics),
        )
        .route("/sessions/{id}/messages", post(message))
        .route("/sessions/{id}/run", post(run))
        .route("/sessions/{id}/run/stream", post(stream_run))
        .route("/sessions/{id}/resume", post(resume))
        .route("/sessions/{id}/cancel", post(cancel_session))
        .layer(axum::middleware::from_fn_with_state(
            App {
                db: db.clone(),
                fixtures: fixtures.clone(),
                root: config_root(),
                locks: Arc::new(Mutex::new(HashMap::new())),
            },
            commands::admission,
        ))
        .with_state(App {
            db,
            fixtures,
            root: config_root(),
            locks: Arc::new(Mutex::new(HashMap::new())),
        });
    let l = tokio::net::TcpListener::bind(format!(
        "0.0.0.0:{}",
        env::var("PORT").unwrap_or_else(|_| "8000".into())
    ))
    .await
    .unwrap();
    axum::serve(l, app).await.unwrap()
}
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}
fn config_root() -> PathBuf {
    let raw = env::var("AGENT_CONFIG_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("config"));
    let absolute = if raw.is_absolute() {
        raw
    } else {
        env::current_dir().unwrap_or_default().join(raw)
    };
    absolute.canonicalize().unwrap_or(absolute)
}
fn load_dotenv() {
    load_dotenv_from(&repo_root().join(".env"));
}
fn load_dotenv_from(path: &FsPath) {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return;
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || (key != "MAIN_MODEL" && env::var_os(key).is_some()) {
            continue;
        }
        let mut value = raw.trim().to_owned();
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value = value[1..value.len() - 1].to_owned();
        } else if let Some(index) = value.find(" #") {
            value.truncate(index);
        }
        unsafe {
            env::set_var(key, value);
        }
    }
}
/// The shared REST contract.  Keeping the document alongside the other
/// implementations makes the interactive Swagger UI interchangeable with
/// FastAPI's `/docs` endpoint.
async fn openapi() -> Json<Value> {
    Json(
        serde_json::from_str(include_str!("../openapi.json"))
            .expect("the checked-in OpenAPI document must be valid JSON"),
    )
}
async fn docs() -> impl IntoResponse {
    (
        [("content-type", "text/html; charset=utf-8")],
        include_str!("../docs/swagger.html"),
    )
}
async fn oauth2_redirect() -> impl IntoResponse {
    (
        [("content-type", "text/html; charset=utf-8")],
        include_str!("../docs/oauth2-redirect.html"),
    )
}
async fn redoc() -> impl IntoResponse {
    (
        [("content-type", "text/html; charset=utf-8")],
        include_str!("../docs/redoc.html"),
    )
}
async fn health(State(a): State<App>) -> Json<Value> {
    Json(
        json!({"status":"ok","contract_version":"2.0.0","storage_schema":6,"config_root":a.root,"available_configs":collect_configs(&a).len()}),
    )
}
fn cfg(a: &App, r: &str) -> Result<(Config, Value, String), String> {
    if r.is_empty() || FsPath::new(r).is_absolute() {
        return Err("config_ref must be a non-empty relative YAML path".into());
    }
    let root = a.root.canonicalize().map_err(|e| e.to_string())?;
    let requested = FsPath::new(r);
    if requested
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("config_ref must not contain parent traversal".into());
    }
    let p = a
        .root
        .join(requested)
        .canonicalize()
        .map_err(|e| e.to_string())?;
    if !p.starts_with(&root) {
        return Err("config_ref must remain within AGENT_CONFIG_ROOT".into());
    }
    if !p.is_file()
        || !matches!(
            p.extension()
                .and_then(|x| x.to_str())
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("yaml") | Some("yml")
        )
    {
        return Err("config_ref must identify a YAML file within AGENT_CONFIG_ROOT".into());
    }
    let b = std::fs::read(p).map_err(|e| e.to_string())?;
    let raw = expand(serde_yaml::from_slice::<Value>(&b).map_err(|e| e.to_string())?);
    let agent = raw.get("agent").cloned().ok_or("missing agent mapping")?;
    let c: Config = serde_json::from_value(agent.clone())
        .map_err(|e| format!("invalid YAML workflow config: {e}"))?;
    validate_config(&c).map_err(|e| format!("invalid YAML workflow config: {e}"))?;
    let snapshot = complete_config_value(serde_json::to_value(&c).map_err(|e| e.to_string())?);
    Ok((c, snapshot, format!("{:x}", Sha256::digest(b))))
}
fn complete_config_value(mut value: Value) -> Value {
    let obj = value.as_object_mut().expect("typed config object");
    let execution = obj.entry("execution").or_insert_with(|| json!({}));
    let e = execution.as_object_mut().expect("execution object");
    e.entry("argument_bindings").or_insert_with(|| json!([]));
    e.entry("tool_policy").or_insert_with(|| json!("auto"));
    e.entry("max_consecutive_invalid_responses")
        .or_insert_with(|| json!(3));
    e.entry("max_no_progress_steps").or_insert(Value::Null);
    if let Some(bindings) = e.get_mut("argument_bindings").and_then(Value::as_array_mut) {
        for binding in bindings {
            if let Some(b) = binding.as_object_mut() {
                b.entry("path").or_insert_with(|| json!([]));
            }
        }
    }
    let completion = obj.entry("completion").or_insert_with(|| json!({}));
    let c = completion.as_object_mut().expect("completion object");
    c.entry("instructions").or_insert(Value::Null);
    c.entry("forbidden_patterns").or_insert_with(|| json!([]));
    c.entry("result_checks").or_insert_with(|| json!([]));
    c.entry("exact").or_insert(Value::Null);
    c.entry("pattern").or_insert(Value::Null);
    c.entry("json_schema").or_insert(Value::Null);
    if let Some(checks) = c.get_mut("result_checks").and_then(Value::as_array_mut) {
        for check in checks {
            if let Some(ch) = check.as_object_mut() {
                ch.entry("arguments").or_insert_with(|| json!({}));
                ch.entry("result").or_insert_with(|| json!({}));
                ch.entry("path").or_insert_with(|| json!([]));
                ch.entry("answer_path").or_insert_with(|| json!([]));
            }
        }
    }
    value
}
fn resolved_config_hash(config: &Config) -> Result<(String, String), String> {
    let mut value = complete_config_value(serde_json::to_value(config).map_err(|e| e.to_string())?);
    let object = value
        .as_object_mut()
        .ok_or("config serialization must be an object")?;
    let llm = object
        .get_mut("llm")
        .and_then(Value::as_object_mut)
        .ok_or("missing llm")?;
    if llm.get("tool_call_mode").and_then(Value::as_str) == Some("native") {
        llm.remove("tool_call_mode");
    }
    if llm.get("max_output_tokens").is_some_and(Value::is_null) {
        llm.remove("max_output_tokens");
    }
    let execution = object
        .get_mut("execution")
        .and_then(Value::as_object_mut)
        .ok_or("missing execution")?;
    execution
        .entry("argument_bindings")
        .or_insert_with(|| json!([]));
    execution
        .entry("tool_policy")
        .or_insert_with(|| json!("auto"));
    execution
        .entry("max_consecutive_invalid_responses")
        .or_insert_with(|| json!(3));
    execution
        .entry("max_no_progress_steps")
        .or_insert(Value::Null);
    if execution.get("tool_policy").and_then(Value::as_str) == Some("auto") {
        execution.remove("tool_policy");
    }
    if execution
        .get("max_no_progress_steps")
        .is_some_and(Value::is_null)
    {
        execution.remove("max_no_progress_steps");
    }
    if execution
        .get("argument_bindings")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        execution.remove("argument_bindings");
    }
    let completion = object
        .get_mut("completion")
        .and_then(Value::as_object_mut)
        .ok_or("missing completion")?;
    completion.entry("instructions").or_insert(Value::Null);
    completion
        .entry("forbidden_patterns")
        .or_insert_with(|| json!([]));
    completion
        .entry("result_checks")
        .or_insert_with(|| json!([]));
    completion.entry("exact").or_insert(Value::Null);
    completion.entry("pattern").or_insert(Value::Null);
    completion.entry("json_schema").or_insert(Value::Null);
    for key in ["instructions"] {
        if completion.get(key).is_some_and(Value::is_null) {
            completion.remove(key);
        }
    }
    for key in ["forbidden_patterns", "result_checks"] {
        if completion
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            completion.remove(key);
        }
    }
    if let Some(memory) = object.get_mut("memory").and_then(Value::as_object_mut) {
        let defaults = [
            ("run_max_steps", Value::Null),
            ("compaction_interval_steps", Value::Null),
            ("context_window_tokens", json!(32768)),
            ("reserved_output_tokens", json!(1024)),
            ("safety_margin_tokens", json!(256)),
            ("max_summary_tokens", json!(4096)),
            ("summary_output_tokens", json!(512)),
            ("compaction_max_attempts", json!(2)),
            ("compaction_max_passes", json!(3)),
            ("compaction_min_growth_tokens", json!(512)),
            ("compaction_min_interval_steps", json!(0)),
            ("optional_compaction_min_pressure_ratio", json!(0)),
            ("optional_compaction_min_reclaimable_tokens", json!(0)),
            ("optional_compaction_min_reduction_tokens", json!(0)),
            ("optional_compaction_min_reduction_ratio", json!(0)),
        ];
        for (key, default) in defaults {
            if memory.get(key) == Some(&default) {
                memory.remove(key);
            }
        }
        for key in [
            "optional_compaction_min_pressure_ratio",
            "optional_compaction_min_reduction_ratio",
        ] {
            if memory.get(key).and_then(Value::as_f64) == Some(0.0) {
                memory.remove(key);
            }
        }
    }
    if let Some(tools) = object.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            if let Some(t) = tool.as_object_mut() {
                if t.get("recovery").is_some_and(Value::is_null) {
                    t.remove("recovery");
                }
            }
        }
    }
    let canonical = canonical_json(value)?;
    let hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    Ok((canonical, hash))
}
fn canonical_json(value: Value) -> Result<String, String> {
    fn sort(v: Value) -> Value {
        match v {
            Value::Array(a) => Value::Array(a.into_iter().map(sort).collect()),
            Value::Object(o) => {
                let sorted: std::collections::BTreeMap<_, _> =
                    o.into_iter().map(|(k, v)| (k, sort(v))).collect();
                let mut out = serde_json::Map::new();
                for (k, v) in sorted {
                    out.insert(k, v);
                }
                Value::Object(out)
            }
            x => x,
        }
    }
    identity::python_encoded(&sort(value))
}
#[cfg(test)]
mod du01_config_tests {
    use super::*;
    fn app(root: PathBuf) -> App {
        let db = PgPoolOptions::new()
            .connect_lazy("postgresql://u:p@127.0.0.1/unused")
            .unwrap();
        App {
            db: db.clone(),
            fixtures: db,
            root,
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    #[tokio::test]
    async fn du01_c01_prints_profile_identity_vectors() {
        unsafe {
            env::set_var("MAIN_MODEL", "du01-synthetic-model");
            env::set_var("OPENAI_BASE_URL", "https://synthetic.invalid/v1");
        }
        let a = app(repo_root().join("config"));
        for r in [
            "log-analysis.yaml",
            "revenue-analysis.yaml",
            "employee-audit.yaml",
            "compaction-stress.yaml",
            "completion-no-workflow.yaml",
            "tool-recovery.yaml",
        ] {
            let (c, snapshot, raw_hash) = cfg(&a, r).unwrap();
            let (canonical, resolved_hash) = resolved_config_hash(&c).unwrap();
            println!(
                "DU01_C01 {}",
                json!({"ref":r,"raw_sha256":raw_hash,"resolved_sha256":resolved_hash,"resolved_preimage":canonical,"config":snapshot})
            );
        }
    }
    #[tokio::test]
    async fn du01_c01_default_identity_and_allow_list_semantics_match_contract() {
        let source = std::fs::read(repo_root().join("config/log-analysis.yaml")).unwrap();
        let root: Value = serde_yaml::from_slice(&source).unwrap();
        let mut omitted = root["agent"].clone();
        let omitted_config: Config = serde_json::from_value(omitted.clone()).unwrap();
        let omitted_hash = resolved_config_hash(&omitted_config).unwrap().1;
        omitted["llm"]["tool_call_mode"] = json!("native");
        omitted["memory"]["context_window_tokens"] = json!(32768);
        omitted["memory"]["reserved_output_tokens"] = json!(1024);
        omitted["memory"]["safety_margin_tokens"] = json!(256);
        omitted["execution"] = json!({"argument_bindings":[],"tool_policy":"auto","max_consecutive_invalid_responses":3,"max_no_progress_steps":null});
        omitted["completion"] = json!({"instructions":null,"forbidden_patterns":[],"result_checks":[],"exact":null,"pattern":null,"json_schema":null});
        let explicit_config: Config = serde_json::from_value(omitted).unwrap();
        assert_eq!(
            omitted_hash,
            resolved_config_hash(&explicit_config).unwrap().1,
            "omitted and explicit defaults must have the same identity"
        );

        for (mode, expected) in [("omitted", true), ("null", true), ("empty", false)] {
            let mut agent = root["agent"].clone();
            match mode {
                "omitted" => {
                    agent["workflow"]["phases"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("allowed_tools");
                }
                "null" => agent["workflow"]["phases"][0]["allowed_tools"] = Value::Null,
                _ => agent["workflow"]["phases"][0]["allowed_tools"] = json!([]),
            }
            let config: Config = serde_json::from_value(agent).unwrap();
            let engine = Engine::new(config).unwrap();
            let state:Session=serde_json::from_value(json!({"id":"s","goal":"g","status":"active","workflow":{"active_phase":"identify_500_ips"}})).unwrap();
            assert_eq!(
                engine
                    .action_rejection(
                        &state,
                        Some("identify_500_ips"),
                        "validate_top_offending_ip",
                        &Default::default(),
                        None,
                    )
                    .is_none(),
                expected,
                "allowed_tools {mode} semantics differ"
            );
        }
    }
    #[tokio::test]
    async fn du01_c02_rejects_traversal_and_non_yaml() {
        let root = std::env::temp_dir().join(format!("rust-du01-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("ok.yaml"),
            std::fs::read(repo_root().join("config/log-analysis.yaml")).unwrap(),
        )
        .unwrap();
        std::fs::create_dir(root.join("folder.yaml")).unwrap();
        let a = app(root.clone());
        assert!(cfg(&a, "").is_err());
        assert!(cfg(&a, "../.env").is_err());
        assert!(cfg(&a, "ok.txt").is_err());
        assert!(cfg(&a, "folder.yaml").is_err());
        assert!(cfg(&a, "/etc/passwd").is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("/etc/passwd", root.join("escape.yaml")).unwrap();
            assert!(cfg(&a, "escape.yaml").is_err());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn du01_c02_rejects_unknown_schema_keywords_and_duplicates() {
        let base = serde_yaml::from_slice::<Value>(
            &std::fs::read(repo_root().join("config/log-analysis.yaml")).unwrap(),
        )
        .unwrap();
        let mut agent = base["agent"].clone();
        agent["tools"][0]["parameters"]["properties"]["start_line"]["magicAssertion"] = json!(true);
        let c: Config = serde_json::from_value(agent.clone()).unwrap();
        assert!(
            validate_config(&c)
                .unwrap_err()
                .contains("unsupported schema assertion")
        );
        let mut agent = base["agent"].clone();
        let first_tool_name = agent["tools"][0]["name"].clone();
        agent["tools"][1]["name"] = first_tool_name;
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(validate_config(&c).unwrap_err().contains("duplicate tool"));
        let mut agent = base["agent"].clone();
        let first_phase = agent["workflow"]["phases"][0].clone();
        agent["workflow"]["phases"]
            .as_array_mut()
            .unwrap()
            .push(first_phase);
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(
            validate_config(&c)
                .unwrap_err()
                .contains("phase IDs must be unique")
        );
        let mut agent = base["agent"].clone();
        agent["workflow"]["phases"][0]["transitions"][0]["to"] = json!("missing");
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(validate_config(&c).unwrap_err().contains("unknown phase"));
        let mut agent = base["agent"].clone();
        agent["workflow"]["phases"][0]["allowed_tools"][0] = json!("not_registered");
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(validate_config(&c).unwrap_err().contains("unknown tool"));
        let mut agent = base["agent"].clone();
        agent["workflow"]["phases"][0]["completion"][0]["tool"] = json!("not_registered");
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(
            validate_config(&c)
                .unwrap_err()
                .contains("evidence references unknown tool")
        );
        let mut agent = base["agent"].clone();
        agent["memory"]["context_window_tokens"] = json!(100);
        agent["memory"]["reserved_output_tokens"] = json!(80);
        agent["memory"]["safety_margin_tokens"] = json!(30);
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(validate_config(&c).unwrap_err().contains("memory budget"));
        let mut agent = base["agent"].clone();
        agent["completion"] = json!({"result_checks":[{"label":"x","tool":"inspect_log"},{"label":"x","tool":"inspect_log"}]});
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(
            validate_config(&c)
                .unwrap_err()
                .contains("duplicate completion result label")
        );
        let mut agent = base["agent"].clone();
        agent["completion"] = json!({"result_checks":[{"label":"x","tool":"inspect_log"}]});
        agent["execution"] = json!({"argument_bindings":[{"tool":"inspect_log","argument":"start_line","source_label":"x"},{"tool":"inspect_log","argument":"start_line","source_label":"x"}]});
        let c: Config = serde_json::from_value(agent).unwrap();
        assert!(
            validate_config(&c)
                .unwrap_err()
                .contains("targets must be unique")
        );
    }
    #[tokio::test]
    async fn du01_c03_dotenv_model_wins_and_loaded_snapshot_stays_fixed() {
        let root = std::env::temp_dir().join(format!("rust-du01-env-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join(".env");
        std::fs::write(&path, "MAIN_MODEL=dotenv-model\n").unwrap();
        unsafe {
            env::set_var("MAIN_MODEL", "inherited-model");
        }
        load_dotenv_from(&path);
        assert_eq!(env::var("MAIN_MODEL").unwrap(), "dotenv-model");
        let a = app(repo_root().join("config"));
        let (c, _, _) = cfg(&a, "log-analysis.yaml").unwrap();
        assert_eq!(c.model, "dotenv-model");
        unsafe {
            env::set_var("MAIN_MODEL", "later-model");
        }
        assert_eq!(c.model, "dotenv-model");
        unsafe {
            env::remove_var("MAIN_MODEL");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn du01_c03_parsed_environment_expansion_and_snapshot_isolation() {
        let root = std::env::temp_dir().join(format!("rust-du01-expand-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut source =
            std::fs::read_to_string(repo_root().join("config/log-analysis.yaml")).unwrap();
        source = source.replace(
            "model: ${MAIN_MODEL:-qwen2.5-7b-instruct-mlx}",
            "model: ${DU01_VALUE:-fallback}",
        );
        std::fs::write(root.join("env.yaml"), source).unwrap();
        unsafe {
            env::set_var("DU01_VALUE", "shell-value");
        }
        let a = app(root.clone());
        let (stored, _, _) = cfg(&a, "env.yaml").unwrap();
        assert_eq!(stored.model, "shell-value");
        unsafe {
            env::set_var("DU01_VALUE", "changed-model");
        }
        let (reloaded, _, _) = cfg(&a, "env.yaml").unwrap();
        assert_eq!(
            stored.model, "shell-value",
            "loaded snapshot must not mutate"
        );
        assert_eq!(reloaded.model, "changed-model");
        unsafe {
            env::set_var("DU01_VALUE", "");
        }
        let (empty, _, _) = cfg(&a, "env.yaml").unwrap();
        assert_eq!(empty.model, "", "a set empty value must not use fallback");
        unsafe {
            env::remove_var("DU01_VALUE");
        }
        let (fallback, _, _) = cfg(&a, "env.yaml").unwrap();
        assert_eq!(fallback.model, "fallback");
        std::fs::remove_dir_all(root).unwrap();
    }
}
fn validate_config(c: &Config) -> Result<(), String> {
    if c.memory.max_tokens <= 0 || c.memory.max_steps <= 0 || c.memory.raw_turns_to_keep == 0 {
        return Err(
            "memory thresholds must be positive and raw_turns_to_keep must be at least 1".into(),
        );
    }
    if !matches!(c.llm.tool_call_mode.as_str(), "native" | "single")
        || c.llm.timeout_seconds <= 0.0
        || !c.llm.timeout_seconds.is_finite()
        || c.llm.temperature.is_finite() == false
        || c.llm.max_output_tokens.is_some_and(|x| x <= 0)
    {
        return Err("invalid llm settings".into());
    }
    let m = &c.memory;
    if [
        m.context_window_tokens,
        m.reserved_output_tokens,
        m.max_summary_tokens,
        m.summary_output_tokens,
    ]
    .iter()
    .any(|x| *x <= 0)
        || m.safety_margin_tokens < 0
        || m.reserved_output_tokens + m.safety_margin_tokens >= m.context_window_tokens
        || !(1..=5).contains(&m.compaction_max_attempts)
        || !(1..=10).contains(&m.compaction_max_passes)
        || m.compaction_min_growth_tokens < 0
        || m.compaction_min_interval_steps < 0
        || [
            m.optional_compaction_min_pressure_ratio,
            m.optional_compaction_min_reduction_ratio,
        ]
        .iter()
        .any(|x| !x.is_finite() || !(0.0..=1.0).contains(x))
        || [
            m.optional_compaction_min_reclaimable_tokens,
            m.optional_compaction_min_reduction_tokens,
        ]
        .iter()
        .any(|x| *x < 0)
    {
        return Err("invalid memory budget or compaction settings".into());
    }
    let mut names = std::collections::HashSet::new();
    for tool in &c.tools {
        if !names.insert(tool.name.clone()) {
            return Err(format!("duplicate tool name '{}'", tool.name));
        }
        if let Some(recovery) = tool.recovery.as_ref() {
            let mode = recovery
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("reconcile");
            if !["read_only", "repeatable", "idempotent", "reconcile"].contains(&mode) {
                return Err(format!(
                    "invalid recovery mode '{mode}' for tool '{}'",
                    tool.name
                ));
            }
            if recovery
                .get("max_attempts")
                .and_then(Value::as_i64)
                .is_some_and(|n| !(1..=10).contains(&n))
            {
                return Err(format!(
                    "invalid recovery max_attempts for tool '{}'",
                    tool.name
                ));
            }
        }
        validate_schema(&tool.parameters, &format!("tools.{}.parameters", tool.name))?;
    }
    if let Some(policy) = c.execution.get("tool_policy").and_then(Value::as_str) {
        if !["auto", "none"].contains(&policy) {
            return Err("execution.tool_policy must be auto or none".into());
        }
    }
    if c.execution
        .get("max_consecutive_invalid_responses")
        .and_then(Value::as_i64)
        .is_some_and(|x| !(1..=50).contains(&x))
    {
        return Err("execution.max_consecutive_invalid_responses must be in 1..50".into());
    }
    if c.execution
        .get("max_no_progress_steps")
        .is_some_and(|v| !v.is_null() && v.as_i64().is_none_or(|x| x < 2))
    {
        return Err("execution.max_no_progress_steps must be null or at least 2".into());
    }
    let result_checks = c
        .completion
        .get("result_checks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut labels = std::collections::HashSet::new();
    for check in &result_checks {
        let tool = check
            .get("tool")
            .and_then(Value::as_str)
            .ok_or("completion result check requires tool")?;
        if !names.contains(tool) {
            return Err(format!("completion check references unknown tool '{tool}'"));
        }
        let label = check
            .get("label")
            .and_then(Value::as_str)
            .ok_or("completion result check requires label")?;
        if !labels.insert(label.to_owned()) {
            return Err(format!("duplicate completion result label '{label}'"));
        }
    }
    if let Some(pattern) = c.completion.get("pattern").and_then(Value::as_str) {
        regex::Regex::new(pattern).map_err(|e| format!("invalid completion.pattern: {e}"))?;
    }
    if let Some(patterns) = c
        .completion
        .get("forbidden_patterns")
        .and_then(Value::as_array)
    {
        for p in patterns {
            let p = p
                .as_str()
                .ok_or("completion.forbidden_patterns entries must be strings")?;
            regex::Regex::new(p)
                .map_err(|e| format!("invalid forbidden completion pattern: {e}"))?;
        }
    }
    if c.completion
        .get("exact")
        .and_then(Value::as_str)
        .is_some_and(|x| x.trim().is_empty())
    {
        return Err("completion.exact must not be empty".into());
    }
    if let Some(bindings) = c
        .execution
        .get("argument_bindings")
        .and_then(Value::as_array)
    {
        let mut targets = std::collections::HashSet::new();
        for binding in bindings {
            let tool = binding
                .get("tool")
                .and_then(Value::as_str)
                .ok_or("argument binding requires tool")?;
            let argument = binding
                .get("argument")
                .and_then(Value::as_str)
                .ok_or("argument binding requires argument")?;
            let label = binding
                .get("source_label")
                .and_then(Value::as_str)
                .ok_or("argument binding requires source_label")?;
            if !names.contains(tool) {
                return Err(format!("argument binding references unknown tool '{tool}'"));
            }
            if !labels.contains(label) {
                return Err(format!(
                    "argument binding references unknown result label '{label}'"
                ));
            }
            if !targets.insert((tool.to_owned(), argument.to_owned())) {
                return Err("argument binding targets must be unique".into());
            }
            let declaration = c.tools.iter().find(|t| t.name == tool).unwrap();
            if declaration
                .parameters
                .get("properties")
                .and_then(Value::as_object)
                .is_none_or(|p| !p.contains_key(argument))
            {
                return Err(format!(
                    "argument binding target '{tool}.{argument}' is not a declared parameter"
                ));
            }
        }
    }
    if let Some(w) = &c.workflow {
        let ids: Vec<_> = w.phases.iter().map(|p| p.id.as_str()).collect();
        if ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len() {
            return Err("workflow phase IDs must be unique".into());
        }
        if !ids.contains(&w.entry_phase.as_str()) {
            return Err("workflow.entry_phase must name a configured phase".into());
        }
        for p in &w.phases {
            for tool in p.allowed_tools.as_deref().unwrap_or(&[]) {
                if !names.contains(tool) {
                    return Err(format!(
                        "phase '{}' references unknown tool '{}'",
                        p.id, tool
                    ));
                }
            }
            for evidence in &p.completion {
                if !names.contains(&evidence.tool) {
                    return Err(format!(
                        "workflow evidence references unknown tool '{}'",
                        evidence.tool
                    ));
                }
            }
            for edge in &p.transitions {
                if edge.to != "complete" && !ids.contains(&edge.to.as_str()) {
                    return Err(format!(
                        "workflow transition from {} targets unknown phase {}",
                        p.id, edge.to
                    ));
                }
                if edge.when == "evidence" && edge.evidence.is_empty() {
                    return Err(format!(
                        "workflow evidence transition from {} requires evidence",
                        p.id
                    ));
                }
                if !matches!(edge.when.as_str(), "phase_complete" | "always" | "evidence") {
                    return Err(format!("invalid transition type {}", edge.when));
                }
                for e in &edge.evidence {
                    if !names.contains(&e.tool) {
                        return Err(format!(
                            "workflow evidence references unknown tool '{}'",
                            e.tool
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
fn validate_schema(v: &Value, path: &str) -> Result<(), String> {
    let Some(obj) = v.as_object() else {
        return Err(format!("{path} must be an object schema"));
    };
    const ALLOWED: &[&str] = &[
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "const",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "description",
        "title",
        "default",
        "examples",
        "$schema",
    ];
    if let Some(k) = obj.keys().find(|k| !ALLOWED.contains(&k.as_str())) {
        return Err(format!("unsupported schema assertion '{k}' at {path}"));
    }
    if let Some(props) = obj.get("properties").and_then(Value::as_object) {
        for (key, schema) in props {
            validate_schema(schema, &format!("{path}.properties.{key}"))?;
        }
    }
    if let Some(items) = obj.get("items") {
        validate_schema(items, &format!("{path}.items"))?;
    }
    if let Some(extra) = obj.get("additionalProperties") {
        if let Some(schema) = extra.as_object() {
            validate_schema(
                &Value::Object(schema.clone()),
                &format!("{path}.additionalProperties"),
            )?;
        } else if !extra.is_boolean() {
            return Err(format!(
                "{path}.additionalProperties must be boolean or schema"
            ));
        }
    }
    if let Some(required) = obj.get("required") {
        if !required
            .as_array()
            .is_some_and(|a| a.iter().all(Value::is_string))
        {
            return Err(format!("{path}.required must be an array of strings"));
        }
    }
    if let Some(enumeration) = obj.get("enum") {
        if !enumeration.as_array().is_some_and(|a| !a.is_empty()) {
            return Err(format!("{path}.enum must be a nonempty array"));
        }
    }
    for key in ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"] {
        if let Some(bound) = obj.get(key) {
            if !bound.is_number() || bound.as_f64().is_none_or(|n| !n.is_finite()) {
                return Err(format!("{path}.{key} must be a finite number"));
            }
        }
    }
    for key in ["minLength", "maxLength", "minItems", "maxItems"] {
        if let Some(bound) = obj.get(key) {
            if bound.as_u64().is_none() {
                return Err(format!("{path}.{key} must be a nonnegative integer"));
            }
        }
    }
    if let Some(pattern) = obj.get("pattern") {
        if let Some(pattern) = pattern.as_str() {
            regex::Regex::new(pattern).map_err(|e| format!("invalid pattern at {path}: {e}"))?;
        } else {
            return Err(format!("{path}.pattern must be a string"));
        }
    }
    if let Some(t) = obj.get("type").and_then(Value::as_str) {
        if ![
            "object", "array", "string", "number", "integer", "boolean", "null",
        ]
        .contains(&t)
        {
            return Err(format!("invalid schema type '{t}' at {path}"));
        }
    }
    Ok(())
}
fn expand(v: Value) -> Value {
    match v {
        Value::String(s) => Value::String(expand_string(&s)),
        Value::Array(x) => Value::Array(x.into_iter().map(expand).collect()),
        Value::Object(x) => Value::Object(x.into_iter().map(|(k, v)| (k, expand(v))).collect()),
        x => x,
    }
}
fn expand_string(input: &str) -> String {
    let mut result = String::new();
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let Some(end) = tail.find('}') else {
            result.push_str(&rest[start..]);
            return result;
        };
        let expression = &tail[..end];
        let (key, default) = expression.split_once(":-").unwrap_or((expression, ""));
        if !key.is_empty()
            && key
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            result.push_str(&env::var(key).unwrap_or_else(|_| default.to_owned()));
        } else {
            result.push_str(&rest[start..start + 3 + end]);
        }
        rest = &tail[end + 1..];
    }
    result.push_str(rest);
    result
}
async fn configs(State(a): State<App>) -> Json<Value> {
    let mut out = vec![];
    for (r, c, h, resolved_hash) in collect_configs(&a) {
        if c.tools.iter().all(|tool| tools::is_supported(&tool.name)) {
            out.push(json!({"config_ref":r,"sha256":h,"resolved_sha256":resolved_hash,"agent_name":c.name,"tools":c.tools.iter().map(|t|t.name.clone()).collect::<Vec<_>>(),"workflow_phases":c.workflow.as_ref().map(|w|w.phases.iter().map(|p|p.id.clone()).collect::<Vec<_>>()).unwrap_or_default(),"verbose_setup":c.output.verbose_setup}));
        }
    }
    Json(Value::Array(out))
}
fn collect_configs(a: &App) -> Vec<(String, Config, String, String)> {
    let mut refs = vec![];
    collect_yaml(&a.root, &a.root, &mut refs);
    refs.sort();
    refs.into_iter()
        .filter_map(|r| {
            cfg(a, &r)
                .ok()
                .and_then(|(c, _, h)| resolved_config_hash(&c).ok().map(|(_, rh)| (r, c, h, rh)))
        })
        .collect()
}
fn collect_yaml(root: &FsPath, dir: &FsPath, output: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            collect_yaml(root, &path, output);
        } else if matches!(
            path.extension().and_then(|x| x.to_str()),
            Some("yaml") | Some("yml")
        ) && let Ok(relative) = path.strip_prefix(root)
        {
            output.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }
}
async fn create(
    State(a): State<App>,
    Json(q): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    if let Some(error) = validate_body(&q) {
        return Err(validation_error(vec![error]));
    }
    let mut errors = vec![];
    let goal = match q.get("user_prompt") {
        None => {
            errors.push(field_error(
                "user_prompt",
                None,
                "missing",
                "Field required",
                None,
            ));
            None
        }
        Some(v) if !v.is_string() => {
            errors.push(field_error(
                "user_prompt",
                Some(v),
                "string_type",
                "Input should be a valid string",
                None,
            ));
            None
        }
        Some(v) if v.as_str() == Some("") => {
            errors.push(field_error(
                "user_prompt",
                Some(v),
                "string_too_short",
                "String should have at least 1 character",
                Some(json!({"min_length":1})),
            ));
            None
        }
        Some(v) => v.as_str(),
    };
    if let Some(v) = q.get("config_ref") {
        if !v.is_string() {
            errors.push(field_error(
                "config_ref",
                Some(v),
                "string_type",
                "Input should be a valid string",
                None,
            ));
        } else if v.as_str() == Some("") {
            errors.push(field_error(
                "config_ref",
                Some(v),
                "string_too_short",
                "String should have at least 1 character",
                Some(json!({"min_length":1})),
            ));
        }
    }
    if !errors.is_empty() {
        return Err(validation_error(errors));
    }
    let goal = goal.unwrap();
    let requested_ref = q["config_ref"].as_str().unwrap_or("log-analysis.yaml");
    let frozen = &q["_command"]["admission"];
    let resolved = if frozen["config"].is_object() {
        serde_json::from_value::<Config>(frozen["config"].clone())
            .map(|c| {
                (
                    c,
                    frozen["config"].clone(),
                    frozen["source_sha256"].as_str().unwrap().to_owned(),
                )
            })
            .map_err(|e| e.to_string())
    } else {
        cfg(&a, requested_ref)
    };
    let (c, raw, h) = resolved.map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":format!("Invalid config_ref: {e}")})),
        )
    })?;
    let (_, resolved_hash) = resolved_config_hash(&c)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"detail":e}))))?;
    if let Some(name) = c
        .tools
        .iter()
        .find(|tool| !tools::is_supported(&tool.name))
        .map(|tool| tool.name.as_str())
    {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                json!({"detail":format!("Invalid config_ref: config references unavailable compiled-in tools: {name}")}),
            ),
        ));
    }
    let r = a
        .root
        .join(requested_ref)
        .canonicalize()
        .ok()
        .and_then(|path| {
            path.strip_prefix(&a.root)
                .ok()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
        })
        .unwrap_or_else(|| requested_ref.to_owned());
    let id = q["_command"]["admission"]["session_id"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let workflow = c.workflow.as_ref().map(|w| Workflow {
        active_phase: Some(w.entry_phase.clone()),
        ..Default::default()
    });
    let mut s = Session {
        command: q.get("_command").cloned(),
        repository: None,
        checkpoint_ack: None,
        committed_summaries: Default::default(),
        run_command_id: None,
        attempts_pending: vec![],
        needs_reload: false,
        last_archive_id: None,
        retention_pending: None,
        id: id.clone(),
        goal: goal.into(),
        revision: 0,
        pending_commit_id: None,
        pending_commit_fingerprint: None,
        execution_owner: None,
        execution_fence: None,
        next_transcript_sequence: 1,
        history_complete: true,
        pending_transcript: vec![],
        config_ref: Some(r.clone()),
        config_sha256: Some(h.clone()),
        resolved_config_sha256: Some(resolved_hash.clone()),
        memory: vec![Message {
            role: "user".into(),
            content: Some(goal.into()),
            ..Default::default()
        }],
        compacted_context: None,
        active_ledger_id: None,
        compaction_events: vec![],
        compaction_attempts: vec![],
        compaction_decisions: vec![],
        diagnostic_counts: Default::default(),
        diagnostics_complete: true,
        diagnostic_history_complete: true,
        diagnostic_rejections: 0,
        compaction_savings_total: 0,
        compaction_savings_known: true,
        diagnostics_pending: vec![],
        compaction_provenance: vec![],
        compaction_ledger: None,
        last_compaction_step: 0,
        last_compaction_estimated_tokens: None,
        last_compaction_attempt_step: None,
        last_compaction_attempt_sha256: None,
        request_budget: None,
        workflow,
        step_count: 0,
        run_count: 0,
        run_id: None,
        run_started_at: None,
        run_ended_at: None,
        run_termination_reason: None,
        status: "active".into(),
        run_status: "idle".into(),
        final_answer: None,
        last_error: None,
        consecutive_invalid_responses: 0,
        model_request_diagnostics: vec![],
        executions: vec![],
        pending_execution_outbox: vec![],
        completion_artifacts: json!({}),
        no_progress_steps: 0,
        progress_fingerprint: None,
        progress_seen_outputs: vec![],
        pending_tool_calls: vec![],
        pending_tool_phase: None,
    };
    save(
        &a,
        &mut s,
        &raw,
        json!({"type":"session_created","config_ref":r,"config_sha256":h,"resolved_config_sha256":resolved_hash}),
    )
    .await
    .map_err(internal)?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"session":s,"message":format!("Session created with immutable config {}@{}.",r,&h[..12])}),
        ),
    ))
}
async fn load(a: &App, id: &str) -> Option<(Session, Value)> {
    let id = Uuid::parse_str(id).ok()?;
    let mut read = a.db.begin().await.ok()?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *read)
        .await
        .ok()?;
    // State, context, counters and diagnostic tails must share a read snapshot.
    let row: Option<(Value, Value, i64, i64, String, Option<String>, Value, Option<Value>, Value, Value)> = sqlx::query_as(
        "SELECT s.state,s.config,s.revision,c.revision,c.goal_message_id::text,c.ledger_record_id::text,c.raw_message_ids,c.ledger_state,
        (SELECT COALESCE(jsonb_agg(jsonb_build_object('family',f.family,'ordinal',d.ordinal,'payload',d.payload) ORDER BY f.family,d.ordinal),'[]'::jsonb)
         FROM (VALUES ('compaction_events'),('compaction_attempts'),('compaction_decisions')) f(family)
         CROSS JOIN LATERAL (SELECT ordinal,payload FROM agent_session_diagnostics
           WHERE session_id=s.session_id AND family=f.family ORDER BY ordinal DESC
           LIMIT CASE WHEN COALESCE(s.state->'diagnostic_counts','{}'::jsonb)='{}'::jsonb THEN NULL ELSE 32 END) d),
        (SELECT jsonb_object_agg(f.family,(SELECT COALESCE(MAX(ordinal),0) FROM agent_session_diagnostics
          WHERE session_id=s.session_id AND family=f.family))
         FROM (VALUES ('compaction_events'),('compaction_attempts'),('compaction_decisions')) f(family))
        FROM agent_sessions s JOIN agent_session_context c USING(session_id) WHERE s.session_id=$1 AND s.storage_version=6",
    ).bind(id).fetch_optional(&mut *read).await.ok()?;
    let (
        state,
        config,
        revision,
        context_revision,
        goal_id,
        ledger_id,
        raw_ids,
        ledger_state,
        diagnostic_rows,
        diagnostic_counts,
    ) = row?;
    integrity::validate_snapshot(&state).ok()?;
    let projection: Value = sqlx::query_scalar(integrity::PROJECTION)
        .bind(id)
        .fetch_one(&mut *read)
        .await
        .ok()?;
    let mut active = state.clone();
    for (key, value) in projection.as_object()? {
        active[key] = value.clone();
    }
    let mut session: Session = serde_json::from_value(active).ok()?;
    integrity::validate_context(&state, &session).ok()?;
    if session.revision != revision {
        return None;
    }
    diagnostics::hydrate(
        &mut session,
        serde_json::from_value(diagnostic_rows).ok()?,
        serde_json::from_value(diagnostic_counts).ok()?,
    )
    .ok()?;
    let expected_goal = session.memory.first()?.message_id.as_deref()?;
    let expected_raw: Vec<Value> = session
        .memory
        .iter()
        .skip(1)
        .map(|message| {
            message
                .message_id
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null)
        })
        .collect();
    let expected_ledger = session
        .compaction_ledger
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .ok()?
        .unwrap_or(Value::Null);
    if context_revision != revision
        || goal_id != expected_goal
        || ledger_id.as_deref() != session.active_ledger_id.as_deref()
        || raw_ids != Value::Array(expected_raw)
        || ledger_state.unwrap_or(Value::Null) != expected_ledger
    {
        return None;
    }
    Some((session, config))
}
fn stage_transcript(s: &mut Session) {
    for message in &mut s.memory {
        if message.sequence.is_some()
            && message.message_id.is_some()
            && message.created_at.is_some()
        {
            continue;
        }
        let message_id = Uuid::new_v4().to_string();
        let created_at = transcript_timestamp();
        let sequence = s.next_transcript_sequence;
        s.next_transcript_sequence += 1;
        message.message_id = Some(message_id.clone());
        message.sequence = Some(sequence);
        message.created_at = Some(created_at.clone());
        s.pending_transcript.push(TranscriptEntry {
            sequence,
            message_id,
            kind: "message".into(),
            payload: context::wire_message(message),
            run_id: None,
            decision_step: s.step_count,
            created_at,
        });
    }
}
fn transcript_event_record(event: &Value, state: &Session) -> Option<(&'static str, Value)> {
    match event["type"].as_str() {
        Some("run_finished") => Some((
            "run_finished",
            json!({"reason":state.run_termination_reason.as_deref().unwrap_or(&state.run_status),"status":state.run_status}),
        )),
        Some("run_interrupted" | "run_reconciled") => Some((
            "run_interrupted",
            json!({"run_id":state.run_id,"reason":event["reason"],"status":state.run_status}),
        )),
        _ => None,
    }
}
async fn save(a: &App, s: &mut Session, c: &Value, e: Value) -> Result<(), sqlx::Error> {
    diagnostics::stage(s).map_err(|error| sqlx::Error::Protocol(error.into()))?;
    stage_transcript(s);
    if s.pending_commit_id.is_none() {
        for provenance in s.compaction_provenance.clone() {
            if let Some(record) = provenance["transcript_record_id"]
                .as_str()
                .filter(|id| !s.committed_summaries.contains(*id))
            {
                if !s.pending_transcript.iter().any(|e| e.message_id == record) {
                    s.pending_transcript.push(TranscriptEntry {
                        sequence: s.next_transcript_sequence,
                        message_id: record.into(),
                        kind: "compaction_summary".into(),
                        payload: provenance["transcript_payload"].clone(),
                        run_id: s.run_id.clone(),
                        decision_step: s.step_count,
                        created_at: transcript_timestamp(),
                    });
                    s.next_transcript_sequence += 1;
                }
            }
        }
        if let Some((kind, payload)) = transcript_event_record(&e, s) {
            s.archive_record(kind, payload);
        }
        s.pending_commit_id = Some(Uuid::new_v4().to_string());
    }
    let expected_revision = s.revision;
    let staged_next_sequence = s.next_transcript_sequence;
    let staged_memory = s.memory.clone();
    let staged_outbox = s.pending_transcript.clone();
    let staged_execution_outbox = s.pending_execution_outbox.clone();
    if s.needs_reload {
        return Err(sqlx::Error::Protocol(
            "Historical receipt requires reload".into(),
        ));
    }
    let staged = s.clone();
    let mut proposal = staged.clone();
    let mut retry = 0;
    let result = loop {
        let result = save_inner(a, &mut proposal, c, e.clone()).await;
        if matches!(
            &result,
            Err(sqlx::Error::Io(_)) | Err(sqlx::Error::Tls(_)) | Err(sqlx::Error::PoolTimedOut)
        ) && retry < 2
        {
            retry += 1;
            proposal = staged.clone();
            continue;
        }
        break result;
    };
    if result.is_ok() {
        *s = proposal;
        for provenance in &s.compaction_provenance {
            if let Some(id) = provenance["transcript_record_id"].as_str() {
                s.committed_summaries.insert(id.to_owned());
            }
        }
        if let Some(ack) = &s.checkpoint_ack {
            *ack.lock().expect("checkpoint acknowledgement") = engine::CheckpointAck {
                needs_reload: s.needs_reload,
                revision: s.revision,
                next_sequence: s.next_transcript_sequence,
                memory: s.memory.clone(),
                summaries: s.committed_summaries.clone(),
            };
        }
    } else {
        proposal.revision = expected_revision;
        proposal.next_transcript_sequence = staged_next_sequence;
        proposal.memory = staged_memory;
        proposal.pending_transcript = staged_outbox;
        proposal.pending_execution_outbox = staged_execution_outbox;
        *s = proposal;
    }
    match result {
        Err(error @ (sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolTimedOut)) => Err(
            sqlx::Error::Protocol(format!("commit_outcome_unresolved: {error}")),
        ),
        other => other,
    }
}

async fn save_inner(a: &App, s: &mut Session, c: &Value, e: Value) -> Result<(), sqlx::Error> {
    stage_transcript(s);
    let id = Uuid::parse_str(&s.id).unwrap();
    let commit_id = Uuid::parse_str(s.pending_commit_id.as_deref().unwrap())
        .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
    let fingerprint = commit_fingerprint(s, c, &e)?;
    if s.pending_commit_fingerprint
        .as_ref()
        .is_some_and(|old| old != &fingerprint)
    {
        return Err(sqlx::Error::Protocol(
            "retained commit proposal changed".into(),
        ));
    }
    s.pending_commit_fingerprint = Some(fingerprint.clone());
    let mut tx = a.db.begin().await?;
    let command = s
        .command
        .as_ref()
        .ok_or_else(|| sqlx::Error::Protocol("durable command required".into()))?
        .clone();
    let command_id = Uuid::parse_str(command["id"].as_str().unwrap()).unwrap();
    sqlx::query("SELECT command_id FROM agent_commands WHERE command_id=$1 FOR UPDATE")
        .bind(command_id)
        .fetch_one(&mut *tx)
        .await?;
    let receipt:Option<(String,i64)>=sqlx::query_as("SELECT fingerprint,committed_revision FROM agent_commit_receipts WHERE session_id=$1 AND commit_id=$2").bind(id).bind(commit_id).fetch_optional(&mut *tx).await?;
    if let Some((saved, revision)) = receipt {
        if saved != fingerprint {
            return Err(sqlx::Error::Protocol(
                "commit identity reused with changed payload".into(),
            ));
        }
        let latest: i64 =
            sqlx::query_scalar("SELECT revision FROM agent_sessions WHERE session_id=$1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        s.needs_reload = latest != revision;
        s.revision = revision;
        s.pending_commit_id = None;
        s.pending_commit_fingerprint = None;
        s.pending_transcript.clear();
        s.pending_execution_outbox.clear();
        s.diagnostics_pending.clear();
        s.attempts_pending.clear();
        return Ok(());
    }
    commands::guard(&mut tx, &command).await?;
    // SELECT FOR UPDATE does not lock a missing row. This transaction-scoped
    // key serializes first writes for the same UUID as well as existing saves.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,734910003))")
        .bind(&s.id)
        .execute(&mut *tx)
        .await?;
    let previous: Option<(Value, Value, Option<String>, Option<String>, Option<String>, i64, Option<Uuid>, Option<String>, i32)> =
        sqlx::query_as("SELECT state,config,config_ref,config_sha256,resolved_config_sha256,revision,last_commit_id,last_commit_sha256,storage_version FROM agent_sessions WHERE session_id=$1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some(row) = &previous {
        let (
            previous,
            previous_config,
            config_ref,
            config_hash,
            resolved_hash,
            revision,
            last_commit_id,
            last_commit_hash,
            storage_version,
        ) = row;
        if *last_commit_id == Some(commit_id) {
            if last_commit_hash.as_deref() != Some(fingerprint.as_str()) {
                return Err(sqlx::Error::Protocol(
                    "commit identity reused with changed content".into(),
                ));
            }
            let committed_revision = *revision;
            tx.commit().await?;
            s.revision = committed_revision;
            s.pending_transcript.clear();
            s.pending_execution_outbox.clear();
            s.diagnostics_pending.clear();
            s.pending_commit_id = None;
            return Ok(());
        }
        if *revision != s.revision {
            return Err(sqlx::Error::Protocol(
                format!(
                    "revision conflict: expected {}, stored {}",
                    s.revision, revision
                )
                .into(),
            ));
        }
        let same = previous.get("id").and_then(Value::as_str) == Some(s.id.as_str())
            && previous.get("goal").and_then(Value::as_str) == Some(s.goal.as_str())
            && previous.get("config_ref").and_then(Value::as_str) == s.config_ref.as_deref()
            && previous.get("config_sha256").and_then(Value::as_str) == s.config_sha256.as_deref()
            && previous
                .get("resolved_config_sha256")
                .and_then(Value::as_str)
                == s.resolved_config_sha256.as_deref()
            && config_ref.as_deref() == s.config_ref.as_deref()
            && config_hash.as_deref() == s.config_sha256.as_deref()
            && resolved_hash.as_deref() == s.resolved_config_sha256.as_deref()
            && previous_config == c;
        if !same {
            return Err(sqlx::Error::Protocol(
                "immutable session goal/config anchor changed".into(),
            ));
        }
        if *storage_version != 6 {
            return Err(sqlx::Error::Protocol(
                "unsupported stored session format; automatic migration is disabled".into(),
            ));
        }
        let (owner, fence, lease_valid): (Option<Uuid>, i64, bool) = sqlx::query_as(
            "SELECT lease_owner,lease_fence,lease_expires_at IS NULL OR lease_expires_at>clock_timestamp() FROM agent_sessions WHERE session_id=$1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        let expected_owner = s
            .execution_owner
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok());
        if owner != expected_owner
            || (owner.is_some() && (s.execution_fence != Some(fence) || !lease_valid))
        {
            return Err(sqlx::Error::Protocol(
                "session is owned by a fenced worker".into(),
            ));
        }
    } else if s.revision != 0 {
        return Err(sqlx::Error::Protocol(
            format!("revision conflict: expected {}, stored 0", s.revision).into(),
        ));
    }
    let stored_revision = previous.as_ref().map(|row| row.5).unwrap_or(0);
    s.revision = stored_revision + 1;
    let (transcript_count, transcript_max): (i64, i64) = sqlx::query_as("SELECT COUNT(*),COALESCE(MAX(sequence),0) FROM agent_session_transcript WHERE session_id=$1")
        .bind(id).fetch_one(&mut *tx).await?;
    if transcript_count != transcript_max {
        return Err(sqlx::Error::Protocol(
            "transcript sequence gap detected".into(),
        ));
    }
    sqlx::query("INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,resolved_config_sha256,revision,storage_version) VALUES($1,$2,$3,$4,$5,$6,$7,6) ON CONFLICT(session_id) DO NOTHING")
        .bind(id).bind(diagnostics::snapshot(s)).bind(c).bind(&s.config_ref).bind(&s.config_sha256).bind(&s.resolved_config_sha256).bind(stored_revision)
        .execute(&mut *tx).await?;
    if let Some((old_state, _, _, _, _, _, _, _, _)) = &previous {
        validate_previous_transcript(&mut tx, id, old_state, transcript_max).await?;
    }
    let mut next_sequence = transcript_max + 1;
    let staged = s.pending_transcript.clone();
    for mut entry in staged.iter().cloned() {
        if entry.kind == "message"
            && let Some(message) = s
                .memory
                .iter()
                .find(|message| message.message_id.as_deref() == Some(entry.message_id.as_str()))
            && context::wire_message(message) != entry.payload
        {
            return Err(sqlx::Error::Protocol(
                "transcript message ID reused with changed payload".into(),
            ));
        }
        let message_id = Uuid::parse_str(&entry.message_id).unwrap();
        let existing: Option<(i64, String, Value)> = sqlx::query_as("SELECT sequence,kind,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2")
            .bind(id).bind(message_id).fetch_optional(&mut *tx).await?;
        if let Some((sequence, kind, payload)) = existing {
            if kind != entry.kind
                || payload != entry.payload
                || (sequence != entry.sequence && entry.kind != "message")
            {
                return Err(sqlx::Error::Protocol(
                    "transcript immutable identity conflict".into(),
                ));
            }
            if entry.kind == "message" {
                if let Some(message) = s.memory.iter_mut().find(|message| {
                    message.message_id.as_deref() == Some(entry.message_id.as_str())
                }) {
                    message.sequence = Some(sequence);
                }
            }
            continue;
        }
        entry.sequence = next_sequence;
        next_sequence = entry.sequence + 1;
        if entry.kind == "message" {
            if let Some(message) = s
                .memory
                .iter_mut()
                .find(|message| message.message_id.as_deref() == Some(entry.message_id.as_str()))
            {
                message.sequence = Some(entry.sequence);
            }
        }
        sqlx::query("INSERT INTO agent_session_transcript(session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8::timestamptz)")
            .bind(id).bind(entry.sequence).bind(message_id).bind(&entry.kind).bind(&entry.payload).bind(entry.run_id.as_deref().and_then(|value| Uuid::parse_str(value).ok())).bind(entry.decision_step).bind(&entry.created_at).execute(&mut *tx).await?;
    }
    // Accepted summaries are durable transcript records, not snapshot-only
    // diagnostics. Replay the stable IDs from state on every checkpoint;
    // exact existing rows are idempotent and changed rows are rejected.
    for provenance in &s.compaction_provenance {
        let Some(record_id) = provenance
            .get("transcript_record_id")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let payload = provenance
            .get("transcript_payload")
            .cloned()
            .unwrap_or(Value::Null);
        let summary_uuid =
            Uuid::parse_str(record_id).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
        validate_summary_payload(&mut tx, id, &payload, &s.goal).await?;
        let existing: Option<(i64, String, Value)> = sqlx::query_as("SELECT sequence,kind,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2")
            .bind(id).bind(summary_uuid).fetch_optional(&mut *tx).await?;
        if let Some((_sequence, kind, old_payload)) = existing {
            if kind != "compaction_summary" || old_payload != payload {
                return Err(sqlx::Error::Protocol(
                    "immutable compaction summary identity conflict".into(),
                ));
            }
        } else {
            sqlx::query("INSERT INTO agent_session_transcript(session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at) VALUES($1,$2,$3,'compaction_summary',$4,NULL,$5,$6::timestamptz)")
                .bind(id).bind(next_sequence).bind(summary_uuid).bind(&payload).bind(s.step_count).bind(transcript_timestamp())
                .execute(&mut *tx).await?;
            next_sequence += 1;
        }
    }
    s.next_transcript_sequence = next_sequence;
    write_call_map(&mut tx, id, &staged).await?;
    write_diagnostics(&mut tx, id, s).await?;
    sqlx::query("UPDATE agent_commands SET session_id=$2 WHERE command_id=$1")
        .bind(command_id)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    if matches!(
        e["type"].as_str(),
        Some("run_interrupted" | "run_reconciled")
    ) {
        if let Some(origin) = s
            .run_command_id
            .as_deref()
            .filter(|origin| *origin != command["id"].as_str().unwrap_or(""))
        {
            sqlx::query("UPDATE agent_commands SET status='uncertain',owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1::uuid AND status IN ('pending','started') AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp())").bind(origin).execute(&mut *tx).await?;
        }
    }
    write_run_state(&mut tx, id, s).await?;
    write_tool_executions(&mut tx, id, &s.pending_execution_outbox, s).await?;
    // Checkpoints receive engine clones. Reload authoritative sequence and
    // timestamp metadata for the active IDs before sealing the projection.
    let active_ids: Vec<Uuid> = s
        .memory
        .iter()
        .map(|m| Uuid::parse_str(m.message_id.as_deref().unwrap()))
        .collect::<Result<_, _>>()
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let active_rows:Vec<(Uuid,i64,Value,String)>=sqlx::query_as("SELECT message_id,sequence,payload,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM agent_session_transcript WHERE session_id=$1 AND message_id=ANY($2) AND kind='message'").bind(id).bind(active_ids).fetch_all(&mut *tx).await?;
    for message in &mut s.memory {
        let row = active_rows
            .iter()
            .find(|r| Some(r.0.to_string()).as_deref() == message.message_id.as_deref())
            .ok_or_else(|| {
                sqlx::Error::Protocol("active message missing from transcript".into())
            })?;
        if context::wire_message(message) != row.2 {
            return Err(sqlx::Error::Protocol(
                "active message payload differs from archive".into(),
            ));
        }
        message.sequence = Some(row.1);
        message.created_at = Some(row.3.clone());
    }
    write_context(&mut tx, id, s).await?;
    retention::write(&mut tx, s).await?;
    sqlx::query("UPDATE agent_commands SET session_id=$2 WHERE command_id=$1")
        .bind(command_id)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO agent_commit_receipts(session_id,commit_id,command_id,fingerprint,expected_revision,committed_revision) VALUES($1,$2,$3,$4,$5,$6)").bind(id).bind(commit_id).bind(command_id).bind(&fingerprint).bind(stored_revision).bind(s.revision).execute(&mut *tx).await?;
    let mut event = e.clone();
    event["command_id"] = command["id"].clone();
    event["run_id"] = json!(s.run_id);
    sqlx::query(
        "INSERT INTO agent_session_events(session_id,event,revision,commit_id,command_id) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(id)
    .bind(event)
    .bind(s.revision)
    .bind(commit_id)
    .bind(command_id)
    .execute(&mut *tx)
    .await?;
    if let Some(owner) = s.execution_owner.as_deref() {
        let owner = Uuid::parse_str(owner)
            .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
        let (stored_owner, stored_fence, active): (Option<Uuid>, i64, bool) = sqlx::query_as(
            "SELECT lease_owner,lease_fence,lease_expires_at>clock_timestamp() FROM agent_sessions WHERE session_id=$1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        if stored_owner != Some(owner) || Some(stored_fence) != s.execution_fence || !active {
            return Err(sqlx::Error::Protocol(
                "lease lost: final fence check rejected commit".into(),
            ));
        }
    }
    let mut persisted = s.clone();
    persisted.pending_transcript.clear();
    persisted.pending_commit_id = None;
    let stored = sqlx::query("INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,resolved_config_sha256,revision,last_commit_id,last_commit_sha256,storage_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,6) ON CONFLICT(session_id) DO UPDATE SET state=EXCLUDED.state,revision=EXCLUDED.revision,last_commit_id=EXCLUDED.last_commit_id,last_commit_sha256=EXCLUDED.last_commit_sha256,storage_version=6,updated_at=now() WHERE agent_sessions.revision=$10")
        .bind(id)
        .bind(diagnostics::snapshot(&persisted))
        .bind(c)
        .bind(&s.config_ref)
        .bind(&s.config_sha256)
        .bind(&s.resolved_config_sha256)
        .bind(s.revision)
        .bind(commit_id)
        .bind(&fingerprint)
        .bind(stored_revision)
        .execute(&mut *tx)
        .await?;
    if stored.rows_affected() != 1 {
        return Err(sqlx::Error::Protocol(
            "revision conflict during commit".into(),
        ));
    }
    commands::guard(&mut tx, &command).await?;
    let kind = e["type"].as_str().unwrap_or("");
    if kind == "run_finished" && s.run_status == "blocked" {
        sqlx::query("UPDATE agent_commands SET status='blocked',owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1").bind(command_id).execute(&mut *tx).await?;
    } else if matches!(
        kind,
        "session_created"
            | "user_message_appended"
            | "run_finished"
            | "telemetry_archived"
            | "run_reconciled"
    ) {
        let message = if kind == "session_created" {
            format!(
                "Session created with immutable config {}@{}.",
                s.config_ref.as_deref().unwrap(),
                &s.config_sha256.as_deref().unwrap()[..12]
            )
        } else if kind == "user_message_appended" {
            "User input appended to session memory.".into()
        } else {
            s.final_answer
                .clone()
                .unwrap_or_else(|| "Run stopped.".into())
        };
        let mut response = s.clone();
        response.pending_transcript.clear();
        commands::finish(
            &mut tx,
            &command,
            if kind == "session_created" { 201 } else { 200 },
            &json!({"session":response,"message":message}),
        )
        .await?;
    }
    tx.commit().await?;
    s.pending_transcript.clear();
    s.pending_execution_outbox.clear();
    s.diagnostics_pending.clear();
    s.pending_commit_id = None;
    s.pending_commit_fingerprint = None;
    s.attempts_pending.clear();
    s.retention_pending = None;
    Ok(())
}

fn commit_fingerprint(s: &Session, config: &Value, event: &Value) -> Result<String, sqlx::Error> {
    let config: Config = serde_json::from_value(config.clone())
        .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
    let (_, resolved_hash) =
        resolved_config_hash(&config).map_err(|error| sqlx::Error::Protocol(error.into()))?;
    if s.resolved_config_sha256.as_deref() != Some(resolved_hash.as_str()) {
        return Err(sqlx::Error::Protocol(
            "resolved configuration hash differs from immutable session anchor".into(),
        ));
    }
    let mut hot = diagnostics::snapshot(s);
    if let Some(object) = hot.as_object_mut() {
        for field in [
            "memory",
            "compacted_context",
            "compaction_ledger",
            "compaction_events",
            "compaction_attempts",
            "compaction_decisions",
            "compaction_provenance",
            "executions",
            "pending_transcript",
            "transcript_pending",
        ] {
            object.remove(field);
        }
    }
    let preimage = json!([
        "agent-commit-v2",s.id,s.command.as_ref().map(|c|c["id"].clone()),s.revision,
        hot,
        s.memory,
        s.compacted_context,
        s.compaction_ledger,
        s.pending_transcript,
        {"executions":s.pending_execution_outbox,"attempts":s.attempts_pending},
        s.diagnostics_pending,
        s.retention_pending,
        resolved_hash,
        event
    ]);
    identity::digest(&preimage).map_err(sqlx::Error::Protocol)
}

async fn write_call_map(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: Uuid,
    entries: &[TranscriptEntry],
) -> Result<(), sqlx::Error> {
    for entry in entries.iter().filter(|entry| entry.kind == "message") {
        if let Some(calls) = entry.payload.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let call_id = call.get("id").and_then(Value::as_str).ok_or_else(|| {
                    sqlx::Error::Protocol("tool call is missing its identity".into())
                })?;
                let message_id = Uuid::parse_str(&entry.message_id)
                    .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
                sqlx::query("INSERT INTO agent_session_calls(session_id,call_id,message_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(session).bind(call_id).bind(message_id).execute(&mut **tx).await?;
                let actual: Uuid = sqlx::query_scalar(
                    "SELECT message_id FROM agent_session_calls WHERE session_id=$1 AND call_id=$2",
                )
                .bind(session)
                .bind(call_id)
                .fetch_one(&mut **tx)
                .await?;
                if actual != message_id {
                    return Err(sqlx::Error::Protocol("tool call identity collision".into()));
                }
            }
        }
    }
    Ok(())
}

async fn write_diagnostics(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: Uuid,
    s: &Session,
) -> Result<(), sqlx::Error> {
    for record in &s.diagnostics_pending {
        if !diagnostics::FAMILIES.contains(&record.family.as_str()) || record.ordinal < 1 {
            return Err(sqlx::Error::Protocol(
                "invalid pending diagnostic identity".into(),
            ));
        }
        let inserted: Option<i64> = sqlx::query_scalar("INSERT INTO agent_session_diagnostics(session_id,family,ordinal,payload) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING ordinal")
            .bind(session).bind(&record.family).bind(record.ordinal).bind(&record.payload)
            .fetch_optional(&mut **tx).await?;
        if inserted.is_none() {
            let existing: Value = sqlx::query_scalar("SELECT payload FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal=$3")
                .bind(session).bind(&record.family).bind(record.ordinal).fetch_one(&mut **tx).await?;
            if existing != record.payload {
                return Err(sqlx::Error::Protocol(
                    diagnostics::conflict(
                        &record.family,
                        record.ordinal,
                        &record.payload,
                        &existing,
                    )
                    .into(),
                ));
            }
        }
    }
    if s.diagnostic_counts
        .keys()
        .any(|family| !diagnostics::FAMILIES.contains(&family.as_str()))
    {
        return Err(sqlx::Error::Protocol(
            "unknown diagnostic counter family".into(),
        ));
    }
    for family in diagnostics::FAMILIES {
        let expected = s.diagnostic_counts.get(family).copied().unwrap_or(0);
        let actual: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(ordinal),0) FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2")
            .bind(session).bind(family).fetch_one(&mut **tx).await?;
        if expected < 0 || actual != expected {
            return Err(sqlx::Error::Protocol(format!("diagnostic counter differs from archived history: family={family} expected={expected} stored={actual}").into()));
        }
    }
    Ok(())
}

async fn write_run_state(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: Uuid,
    s: &Session,
) -> Result<(), sqlx::Error> {
    let (Some(run), Some(started_at)) = (&s.run_id, &s.run_started_at) else {
        return Ok(());
    };
    let run =
        Uuid::parse_str(run).map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
    let ended_at = if s.run_status != "running" {
        s.run_ended_at
            .clone()
            .or_else(|| Some(transcript_timestamp()))
    } else {
        None
    };
    let status = if s.run_status.is_empty() {
        "running"
    } else {
        &s.run_status
    };
    let owner = s
        .execution_owner
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or(session);
    let fence = s.execution_fence.unwrap_or(0);
    sqlx::query("INSERT INTO agent_runs(run_id,session_id,owner,fence,status,started_at,ended_at,termination_reason,last_decision_step,metadata,command_id) VALUES($1,$2,$3,$4,$5,$6::timestamptz,$7::timestamptz,$11,$8,$9,$10::uuid) ON CONFLICT(session_id,run_id) DO UPDATE SET status=EXCLUDED.status,ended_at=COALESCE(agent_runs.ended_at,EXCLUDED.ended_at),termination_reason=COALESCE(agent_runs.termination_reason,EXCLUDED.termination_reason),last_decision_step=EXCLUDED.last_decision_step,metadata=EXCLUDED.metadata")
        .bind(run).bind(session).bind(owner).bind(fence).bind(status).bind(started_at).bind(ended_at).bind(s.step_count).bind(json!({"run_count":s.run_count,"step_count":s.step_count})).bind(&s.run_command_id).bind(&s.run_termination_reason).execute(&mut **tx).await?;
    Ok(())
}

async fn write_tool_executions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: Uuid,
    outbox: &[Value],
    s: &Session,
) -> Result<(), sqlx::Error> {
    for item in outbox {
        let attempts = item["attempts"]
            .as_i64()
            .ok_or_else(|| sqlx::Error::Protocol("invalid dispatch count".into()))?;
        let maximum = item["policy"]["max_attempts"].as_i64().unwrap_or(2);
        if attempts < 0 || attempts > maximum {
            return Err(sqlx::Error::Protocol(
                "action exceeded its frozen dispatch bound".into(),
            ));
        }
        let mut aggregate = item.clone();
        if let Some(object) = aggregate.as_object_mut() {
            object.remove("attempt_records");
            object.remove("payload");
        }
        let old:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(e) FROM agent_tool_executions e WHERE session_id=$1 AND execution_id=$2::uuid FOR UPDATE").bind(session).bind(item["execution_id"].as_str()).fetch_optional(&mut **tx).await?;
        if let Some(old) = old {
            if attempts < old["attempts"].as_i64().unwrap_or(0)
                || old["payload"].get("recipient") != item.get("recipient")
            {
                return Err(sqlx::Error::Protocol(
                    "frozen recipient metadata or dispatch count changed".into(),
                ));
            }
            for key in [
                "execution_id",
                "run_id",
                "call_id",
                "tool",
                "arguments",
                "policy",
                "idempotency_key",
                "command_id",
                "namespace",
                "target",
                "payload_sha256",
                "key_algorithm",
            ] {
                if old[key] != item[key] {
                    return Err(sqlx::Error::Protocol(format!(
                        "frozen action field changed: {key}"
                    )));
                }
            }
            if matches!(old["status"].as_str(), Some("succeeded" | "failed"))
                && (old["status"] != item["status"] || old["result"] != item["result"])
            {
                return Err(sqlx::Error::Protocol("terminal action changed".into()));
            }
        }
        sqlx::query("INSERT INTO agent_tool_executions(session_id,execution_id,run_id,call_id,tool,arguments,policy,idempotency_key,attempts,status,result,owner,fence,payload,command_id,namespace,target,payload_sha256,key_algorithm,started_at,finished_at) VALUES($1,$2::uuid,$3::uuid,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,$13,$14,$15::uuid,$16,$17,$18,'agent-action-v1',$19::timestamptz,$20::timestamptz) ON CONFLICT(session_id,execution_id) DO UPDATE SET attempts=EXCLUDED.attempts,status=EXCLUDED.status,result=EXCLUDED.result,owner=EXCLUDED.owner,fence=EXCLUDED.fence,payload=EXCLUDED.payload,started_at=COALESCE(agent_tool_executions.started_at,EXCLUDED.started_at),finished_at=COALESCE(agent_tool_executions.finished_at,EXCLUDED.finished_at),updated_at=clock_timestamp()")
  .bind(session).bind(item["execution_id"].as_str()).bind(item["run_id"].as_str()).bind(item["call_id"].as_str()).bind(item["tool"].as_str()).bind(&item["arguments"]).bind(&item["policy"]).bind(item["idempotency_key"].as_str()).bind(item["attempts"].as_i64().unwrap_or(0) as i32).bind(item["status"].as_str()).bind(item["result"].as_str()).bind(&s.execution_owner).bind(s.execution_fence).bind(aggregate).bind(item["command_id"].as_str()).bind(item["namespace"].as_str()).bind(item["target"].as_str()).bind(item["payload_sha256"].as_str()).bind(item["started_at"].as_str()).bind(item["finished_at"].as_str()).execute(&mut **tx).await?;
    }
    for item in &s.attempts_pending {
        let old: Option<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(a) FROM agent_action_attempts a WHERE attempt_id=$1::uuid FOR UPDATE",
        )
        .bind(item["attempt_id"].as_str())
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(old) = &old {
            for key in [
                "execution_id",
                "command_id",
                "attempt_number",
                "kind",
                "payload_sha256",
                "owner",
                "fence",
            ] {
                if old[key] != item[key] {
                    return Err(sqlx::Error::Protocol("attempt identity changed".into()));
                }
            }
            let normalized = |v: &Value| {
                v.as_str()
                    .map(|s| s.replace("+00:00", "Z").replace(".000000Z", "Z"))
            };
            if normalized(&old["started_at"]) != normalized(&item["started_at"]) {
                return Err(sqlx::Error::Protocol("attempt start time changed".into()));
            }
            if old["status"] != "started" {
                if old["external_reference"] != item["external_reference"]
                    || normalized(&old["finished_at"]) != normalized(&item["finished_at"])
                {
                    return Err(sqlx::Error::Protocol(
                        "settled attempt evidence metadata changed".into(),
                    ));
                }
                if old["status"] != item["status"]
                    || old["evidence"] != item["evidence"]
                    || old["error"] != item["error"]
                {
                    return Err(sqlx::Error::Protocol("settled attempt changed".into()));
                }
                continue;
            }
        }
        sqlx::query("INSERT INTO agent_action_attempts(attempt_id,session_id,execution_id,command_id,attempt_number,kind,status,payload_sha256,owner,fence,started_at,finished_at,evidence,error,external_reference) VALUES($1::uuid,$2,$3::uuid,$4::uuid,$5,$6,$7,$8,$9::uuid,$10,$11::timestamptz,$12::timestamptz,$13,$14,$15) ON CONFLICT(attempt_id) DO UPDATE SET status=EXCLUDED.status,finished_at=EXCLUDED.finished_at,evidence=EXCLUDED.evidence,error=EXCLUDED.error,external_reference=EXCLUDED.external_reference WHERE agent_action_attempts.status='started'")
  .bind(item["attempt_id"].as_str()).bind(session).bind(item["execution_id"].as_str()).bind(item["command_id"].as_str()).bind(item["attempt_number"].as_i64().unwrap() as i32).bind(item["kind"].as_str()).bind(item["status"].as_str()).bind(item["payload_sha256"].as_str()).bind(item["owner"].as_str()).bind(item["fence"].as_i64()).bind(item["started_at"].as_str()).bind(item["finished_at"].as_str()).bind(item.get("evidence").filter(|v|!v.is_null())).bind(item.get("error").filter(|v|!v.is_null())).bind(item["external_reference"].as_str()).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn write_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: Uuid,
    s: &Session,
) -> Result<(), sqlx::Error> {
    let Some(goal) = s.memory.first() else {
        return Err(sqlx::Error::Protocol(
            "active context has no goal message".into(),
        ));
    };
    if goal.role != "user" || goal.content.as_deref() != Some(s.goal.as_str()) {
        return Err(sqlx::Error::Protocol(
            "active context lost immutable goal".into(),
        ));
    }
    let goal_id = Uuid::parse_str(
        goal.message_id
            .as_deref()
            .ok_or_else(|| sqlx::Error::Protocol("goal message has no identity".into()))?,
    )
    .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
    let raw_ids = Value::Array(
        s.memory
            .iter()
            .skip(1)
            .map(|message| {
                message
                    .message_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            })
            .collect(),
    );
    let ledger_id = s
        .active_ledger_id
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?;
    let ledger = s
        .compaction_ledger
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| sqlx::Error::Protocol(error.to_string().into()))?
        .unwrap_or(Value::Null);
    sqlx::query("INSERT INTO agent_session_context(session_id,revision,goal_message_id,ledger_record_id,raw_message_ids,ledger_state) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(session_id) DO UPDATE SET revision=EXCLUDED.revision,goal_message_id=EXCLUDED.goal_message_id,ledger_record_id=EXCLUDED.ledger_record_id,raw_message_ids=EXCLUDED.raw_message_ids,ledger_state=EXCLUDED.ledger_state")
        .bind(session).bind(s.revision).bind(goal_id).bind(ledger_id).bind(raw_ids).bind(ledger).execute(&mut **tx).await?;
    Ok(())
}

async fn validate_previous_transcript(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    state: &Value,
    max_sequence: i64,
) -> Result<(), sqlx::Error> {
    integrity::validate_snapshot(state).map_err(sqlx::Error::Protocol)?;
    if state["next_transcript_sequence"].as_i64() != Some(max_sequence + 1) {
        return Err(sqlx::Error::Protocol("transcript counter mismatch".into()));
    }
    let projection: Value = sqlx::query_scalar(integrity::PROJECTION)
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    let mut active = state.clone();
    for (key, value) in projection.as_object().unwrap() {
        active[key] = value.clone();
    }
    let restored: Session =
        serde_json::from_value(active).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    integrity::validate_context(state, &restored).map_err(sqlx::Error::Protocol)?;
    let goal = restored
        .memory
        .first()
        .ok_or_else(|| sqlx::Error::Protocol("missing immutable goal message".into()))?;
    if goal.role != "user" || goal.content.as_deref() != Some(restored.goal.as_str()) {
        return Err(sqlx::Error::Protocol("goal anchor mismatch".into()));
    }
    Ok(())
}

async fn validate_summary_payload(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    payload: &Value,
    goal: &str,
) -> Result<(), sqlx::Error> {
    let ids = payload
        .get("source_message_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| sqlx::Error::Protocol("summary source identities are missing".into()))?;
    if ids.is_empty() {
        return Err(sqlx::Error::Protocol(
            "summary source identities are empty".into(),
        ));
    }
    let mut distinct = std::collections::HashSet::new();
    let mut source_messages = Vec::with_capacity(ids.len());
    let mut previous_sequence = 0;
    for source in ids {
        let sid = source
            .as_str()
            .ok_or_else(|| sqlx::Error::Protocol("invalid summary source identity".into()))?;
        if !distinct.insert(sid.to_owned()) {
            return Err(sqlx::Error::Protocol(
                "duplicate summary source identity".into(),
            ));
        }
        let source_uuid =
            Uuid::parse_str(sid).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
        let found: Option<(i64, String, Value)> = sqlx::query_as(
            "SELECT sequence,kind,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2",
        )
        .bind(id)
        .bind(source_uuid)
        .fetch_optional(&mut **tx)
        .await?;
        let Some((sequence, kind, source_payload)) = found else {
            return Err(sqlx::Error::Protocol(
                "summary references a missing or cross-session source message".into(),
            ));
        };
        if kind != "message" || sequence <= previous_sequence {
            return Err(sqlx::Error::Protocol(
                "summary source messages are not ordered transcript messages".into(),
            ));
        }
        previous_sequence = sequence;
        source_messages.push(
            serde_json::from_value::<Message>(source_payload)
                .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?,
        );
    }
    if let Some(retained) = payload
        .get("retained_message_ids")
        .and_then(Value::as_array)
    {
        let mut retained_seen = std::collections::HashSet::new();
        for identity in retained {
            let identity = identity
                .as_str()
                .ok_or_else(|| sqlx::Error::Protocol("invalid retained message identity".into()))?;
            if !retained_seen.insert(identity.to_owned()) {
                return Err(sqlx::Error::Protocol(
                    "duplicate retained message identity".into(),
                ));
            }
            let retained_uuid = Uuid::parse_str(identity)
                .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
            let kind: Option<String> = sqlx::query_scalar(
                "SELECT kind FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2",
            )
            .bind(id)
            .bind(retained_uuid)
            .fetch_optional(&mut **tx)
            .await?;
            if kind.as_deref() != Some("message") {
                return Err(sqlx::Error::Protocol(
                    "summary retained message is missing or cross-session".into(),
                ));
            }
        }
    }
    let previous_content = if let Some(prior) =
        payload.get("previous_ledger_id").and_then(Value::as_str)
    {
        let prior_uuid =
            Uuid::parse_str(prior).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
        let row: Option<(String, Value)> = sqlx::query_as("SELECT kind,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2").bind(id).bind(prior_uuid).fetch_optional(&mut **tx).await?;
        let Some((kind, prior_payload)) = row else {
            return Err(sqlx::Error::Protocol(
                "previous summary ledger is missing".into(),
            ));
        };
        if !matches!(kind.as_str(), "compaction_summary" | "legacy_summary") {
            return Err(sqlx::Error::Protocol(
                "previous summary ledger is invalid".into(),
            ));
        }
        Some(
            prior_payload
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        )
    } else {
        None
    };
    let source = crate::context::source_text(goal, &source_messages, previous_content.as_deref())
        .map_err(|e| sqlx::Error::Protocol(e.into()))?;
    let fingerprint = format!("{:x}", Sha256::digest(source.as_bytes()));
    if payload.get("source_sha256").and_then(Value::as_str) != Some(fingerprint.as_str()) {
        return Err(sqlx::Error::Protocol(
            "summary source fingerprint differs from archived inputs".into(),
        ));
    }
    Ok(())
}
async fn session(State(a): State<App>, Path(id): Path<String>) -> impl IntoResponse {
    load(&a, &id)
        .await
        .map(|x| (StatusCode::OK, Json(serde_json::to_value(x.0).unwrap())))
        .unwrap_or((
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        ))
}
async fn transcript(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let parse = |key: &str, default: i64| -> Result<i64, ()> {
        query
            .get(key)
            .map_or(Ok(default), |raw| raw.parse::<i64>().map_err(|_| ()))
    };
    let after = match parse("after_sequence", 0) {
        Ok(x) if x >= 0 => x,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"after_sequence must be non-negative"})),
            )
                .into_response();
        }
    };
    let limit = match parse("limit", 100) {
        Ok(x) if (1..=1000).contains(&x) => x,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    let requested_through = match parse("through_sequence", i64::MAX) {
        Ok(x) if x >= 0 => x,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"through_sequence must be non-negative"})),
            )
                .into_response();
        }
    };
    let Some((state, _)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let through = requested_through.min(state.next_transcript_sequence.saturating_sub(1));
    let session_id = match Uuid::parse_str(&id) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
    };
    let rows = sqlx::query("SELECT sequence,message_id,kind,payload,run_id,decision_step,created_at::text AS created_at FROM agent_session_transcript WHERE session_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence LIMIT $4")
        .bind(session_id).bind(after).bind(through).bind(limit).fetch_all(&a.db).await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => return internal(error).into_response(),
    };
    let entries: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "sequence": row.get::<i64,_>("sequence"),
                "message_id": row.get::<uuid::Uuid,_>("message_id").to_string(),
                "kind": row.get::<String,_>("kind"),
                "payload": row.get::<Value,_>("payload"),
                "run_id": row.get::<Option<uuid::Uuid>,_>("run_id").map(|id| id.to_string()),
                "decision_step": row.get::<i64,_>("decision_step"),
                "created_at": row.get::<String,_>("created_at"),
            })
        })
        .collect();
    let next_after = entries
        .last()
        .and_then(|entry| entry.get("sequence"))
        .and_then(Value::as_i64)
        .unwrap_or(after);
    (StatusCode::OK, Json(json!({"history_complete":state.history_complete,"revision":state.revision,"through_sequence":through,"next_after_sequence":next_after,"entries":entries}))).into_response()
}
async fn executions(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = match query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
    {
        Ok(Some(value @ 1..=1000)) => value,
        Ok(None) => 100,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    let after = match query.get("after") {
        Some(value) => match Uuid::parse_str(value) {
            Ok(value) => Some(value),
            Err(_) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"detail":"after must be an execution UUID"})),
                )
                    .into_response();
            }
        },
        None => None,
    };
    if load(&a, &id).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    }
    let session_id = match Uuid::parse_str(&id) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
    };
    let executions: Vec<Value> = match sqlx::query_scalar("SELECT to_jsonb(e)-'payload'-'owner' FROM agent_tool_executions e WHERE session_id=$1 AND ($2::uuid IS NULL OR execution_id>$2) ORDER BY execution_id LIMIT $3")
        .bind(session_id).bind(after).bind(limit as i64).fetch_all(&a.db).await {
        Ok(rows)=>rows,Err(e)=>return internal(e).into_response()
    };
    let next_after = executions
        .last()
        .and_then(|entry| entry["execution_id"].as_str())
        .map(str::to_owned)
        .or_else(|| query.get("after").cloned());
    (
        StatusCode::OK,
        Json(json!({"session_id":id,"executions":executions,"next_after":next_after})),
    )
        .into_response()
}

async fn session_events(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let parse_cursor = |name: &str, default: i64| -> Result<i64, String> {
        query
            .get(name)
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| format!("{name} must be an integer"))
            })
            .unwrap_or(Ok(default))
            .and_then(|value| {
                if value < 0 {
                    Err(format!("{name} must be non-negative"))
                } else {
                    Ok(value)
                }
            })
    };
    let after = match parse_cursor("after", 0) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":error})),
            )
                .into_response();
        }
    };
    let limit = match parse_cursor("limit", 100) {
        Ok(value @ 1..=1000) => value,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    let requested_through = match query.get("through") {
        Some(_) => match parse_cursor("through", 0) {
            Ok(value) => Some(value),
            Err(error) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"detail":error})),
                )
                    .into_response();
            }
        },
        None => None,
    };
    let Some((_, _)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let Ok(session_id) = Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let watermark: i64 = match sqlx::query_scalar(
        "SELECT COALESCE(max(event_id),0) FROM agent_session_events WHERE session_id=$1",
    )
    .bind(session_id)
    .fetch_one(&a.db)
    .await
    {
        Ok(value) => value,
        Err(error) => return internal(error).into_response(),
    };
    let through = requested_through.unwrap_or(watermark).min(watermark);
    let rows = match sqlx::query("SELECT event_id,event,revision,created_at::text AS created_at FROM agent_session_events WHERE session_id=$1 AND event_id>$2 AND event_id<=$3 ORDER BY event_id LIMIT $4")
        .bind(session_id).bind(after).bind(through).bind(limit).fetch_all(&a.db).await {
            Ok(rows) => rows,
            Err(error) => return internal(error).into_response(),
        };
    let entries: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "event_id":row.get::<i64,_>("event_id"),
                "event":row.get::<Value,_>("event"),
                "revision":row.get::<i64,_>("revision"),
                "created_at":row.get::<String,_>("created_at"),
            })
        })
        .collect();
    let next_after = entries
        .last()
        .and_then(|entry| entry["event_id"].as_i64())
        .unwrap_or(after);
    (StatusCode::OK, Json(json!({"session_id":id,"through":through,"entries":entries,"next_after":next_after,"archive_id":null}))).into_response()
}

async fn session_runs(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = match query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
    {
        Ok(Some(value @ 1..=1000)) => value,
        Ok(None) => 100,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    let after = match query.get("after") {
        Some(value) => match Uuid::parse_str(value) {
            Ok(value) => Some(value),
            Err(_) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"detail":"after must be a run UUID"})),
                )
                    .into_response();
            }
        },
        None => None,
    };
    if load(&a, &id).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    }
    let Ok(session_id) = Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let runs: Vec<Value> = match sqlx::query_scalar("SELECT to_jsonb(r)-'owner' FROM agent_runs r WHERE session_id=$1 AND ($2::uuid IS NULL OR (started_at,run_id)>(SELECT started_at,run_id FROM agent_runs WHERE session_id=$1 AND run_id=$2)) ORDER BY started_at,run_id LIMIT $3")
        .bind(session_id).bind(after).bind(limit as i64).fetch_all(&a.db).await {
        Ok(rows)=>rows,Err(e)=>return internal(e).into_response()
    };
    let next_after = runs
        .last()
        .and_then(|run| run["run_id"].as_str())
        .map(str::to_owned)
        .or_else(|| query.get("after").cloned());
    (
        StatusCode::OK,
        Json(json!({"session_id":id,"runs":runs,"next_after":next_after})),
    )
        .into_response()
}

async fn abandoned_runs(
    State(a): State<App>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = match query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
    {
        Ok(Some(value @ 1..=1000)) => value,
        Ok(None) => 100,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    match sqlx::query("SELECT r.session_id::text AS session_id,r.run_id::text AS run_id,r.status,r.last_decision_step FROM agent_runs r JOIN agent_sessions s USING(session_id) WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp()) ORDER BY r.started_at LIMIT $1")
        .bind(limit as i64).fetch_all(&a.db).await {
            Ok(rows) => {
                let runs: Vec<Value> = rows.into_iter().map(|row| json!({"session_id":row.get::<String,_>("session_id"),"run_id":row.get::<String,_>("run_id"),"status":row.get::<String,_>("status"),"last_decision_step":row.get::<i64,_>("last_decision_step")})).collect();
                (StatusCode::OK, Json(json!({"runs":runs}))).into_response()
            }
            Err(error) => internal(error).into_response(),
        }
}

async fn session_archives(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = match query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
    {
        Ok(Some(value @ 1..=1000)) => value,
        Ok(None) => 100,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            )
                .into_response();
        }
    };
    let after = match query.get("after") {
        Some(value) => match Uuid::parse_str(value) {
            Ok(value) => Some(value),
            Err(_) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"detail":"after must be an archive UUID"})),
                )
                    .into_response();
            }
        },
        None => None,
    };
    if load(&a, &id).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    }
    let Ok(session_id) = Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    match sqlx::query("SELECT archive_id::text AS archive_id,snapshot_revision,sha256,manifest,created_at::text AS created_at FROM agent_archive_receipts WHERE session_id=$1 AND ($2::uuid IS NULL OR archive_id>$2) ORDER BY archive_id LIMIT $3")
        .bind(session_id).bind(after).bind(limit as i64).fetch_all(&a.db).await {
            Ok(rows) => {
                let archives: Vec<Value> = rows.into_iter().map(|row| json!({
                    "archive_id":row.get::<String,_>("archive_id"),
                    "snapshot_revision":row.get::<i64,_>("snapshot_revision"),
                    "sha256":row.get::<String,_>("sha256"),
                    "manifest":row.get::<Value,_>("manifest"),
                    "created_at":row.get::<String,_>("created_at"),
                })).collect();
                let next_after = archives.last().and_then(|archive| archive["archive_id"].as_str()).map(str::to_owned).or_else(|| query.get("after").cloned());
                (StatusCode::OK, Json(json!({"session_id":id,"archives":archives,"next_after":next_after}))).into_response()
            }
            Err(error) => internal(error).into_response(),
        }
}

async fn session_retention_plan(State(a): State<App>, Path(id): Path<String>) -> impl IntoResponse {
    let Some((state, _)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let Ok(session_id) = Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    let counts: Result<(i64, i64), sqlx::Error> = async {
        let unresolved: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_tool_executions WHERE session_id=$1 AND status IN ('pending','started','uncertain','blocked')")
            .bind(session_id).fetch_one(&a.db).await?;
        let running: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_runs WHERE session_id=$1 AND status='running'")
            .bind(session_id).fetch_one(&a.db).await?;
        Ok((unresolved, running))
    }.await;
    let (unresolved, running) = match counts {
        Ok(counts) => counts,
        Err(error) => return internal(error).into_response(),
    };
    let has_pending_calls = !state.pending_tool_calls.is_empty();
    let eligible = state.status != "active"
        && state.run_status != "running"
        && !has_pending_calls
        && unresolved == 0
        && running == 0;
    (StatusCode::OK, Json(json!({
        "session_id":id,
        "revision":state.revision,
        "default_policy":"keep_all",
        "eligible":eligible,
        "unresolved_executions":unresolved,
        "running_lifecycles":running,
        "keep_diagnostics":32,
        "keep_events":100,
        "protected_tables":["agent_commands","agent_commit_receipts","agent_action_attempts","agent_sessions","agent_session_context","agent_session_transcript","agent_session_calls","agent_runs","agent_tool_executions","agent_archive_receipts"],
        "prunable_tables":["agent_session_events","agent_session_diagnostics"],
        "requires":"Verified fresh export, terminal session, ownership/fencing, matching revision and database digests"
    }))).into_response()
}
async fn compaction_diagnostics(
    State(a): State<App>,
    Path((id, family)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let parse = |key: &str, default: usize, max: usize| -> Result<usize, String> {
        match query.get(key) {
            None => Ok(default),
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|n| *n <= max)
                .ok_or_else(|| format!("{key} is outside its allowed range")),
        }
    };
    let limit = match parse("limit", 100, 1000) {
        Ok(value) if value > 0 => value,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"limit must be between 1 and 1000"})),
            );
        }
    };
    let after = match parse("after", 0, usize::MAX) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":error})),
            );
        }
    };
    let through_arg = match parse("through", usize::MAX, usize::MAX) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":error})),
            );
        }
    };
    match load(&a, &id).await {
        Some((s, _)) => {
            if !diagnostics::FAMILIES.contains(&family.as_str()) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"detail":"Unknown diagnostic family"})),
                );
            }
            let total = s.diagnostic_counts.get(&family).copied().unwrap_or(0) as usize;
            let through = through_arg.min(total);
            let rows: Vec<(i64, Value)> = match sqlx::query_as("SELECT ordinal,payload FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal>$3 AND ordinal<=$4 ORDER BY ordinal LIMIT $5")
                .bind(Uuid::parse_str(&id).unwrap()).bind(&family)
                .bind(after.min(i64::MAX as usize) as i64).bind(through as i64).bind(limit as i64)
                .fetch_all(&a.db).await {
                    Ok(rows) => rows,
                    Err(error) => return internal(error),
                };
            let entries: Vec<Value> = rows
                .into_iter()
                .map(|(ordinal, payload)| json!({"ordinal":ordinal,"payload":payload}))
                .collect();
            let next_after = entries
                .last()
                .and_then(|entry| entry.get("ordinal"))
                .and_then(Value::as_u64)
                .unwrap_or(after as u64);
            (
                StatusCode::OK,
                Json(
                    json!({"session_id":id,"family":family,"total_recorded":total,"through":through,"next_after":next_after,"entries":entries,"archive_id":s.last_archive_id,"history_complete":s.diagnostic_history_complete && s.last_archive_id.is_none()}),
                ),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        ),
    }
}
async fn message(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> impl IntoResponse {
    if let Some(error) = validate_body(&q) {
        return validation_error(vec![error]);
    }
    let Some(_guard) = session_lock(&a, &id).await else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"detail":"session ownership conflict"})),
        );
    };
    if q.get("content").is_none() {
        return validation_error(vec![field_error(
            "content",
            None,
            "missing",
            "Field required",
            None,
        )]);
    }
    if !q["content"].is_string() {
        return validation_error(vec![field_error(
            "content",
            Some(&q["content"]),
            "string_type",
            "Input should be a valid string",
            None,
        )]);
    }
    if q["content"].as_str() == Some("") {
        return validation_error(vec![field_error(
            "content",
            Some(&q["content"]),
            "string_too_short",
            "String should have at least 1 character",
            Some(json!({"min_length":1})),
        )]);
    }
    let Some((mut s, c)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        );
    };
    commands::attach(&mut s, &q);
    if s.status != "active" {
        return (
            StatusCode::CONFLICT,
            Json(
                json!({"detail":"Cannot append to a terminal session; use /resume with content instead"}),
            ),
        );
    }
    let x = q["content"].as_str().unwrap();
    s.append_message(Message {
        role: "user".into(),
        content: Some(x.into()),
        ..Default::default()
    });
    match save(&a, &mut s, &c, json!({"type":"user_message_appended"})).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({"session":s,"message":"User input appended to session memory."})),
        ),
        Err(e) => internal(e),
    }
}
async fn execute<F>(
    a: &App,
    s: &mut Session,
    c: &Value,
    q: &Value,
    emit: &mut F,
) -> Result<(), String>
where
    F: FnMut(Value, Session) -> futures_util::future::BoxFuture<'static, Result<(), String>>,
{
    let config: Config = serde_json::from_value(c.clone()).map_err(|e| e.to_string())?;
    let e = Engine::new(config)?;
    s.repository = Some(a.db.clone());
    e.run(
        s,
        q.get("max_steps").and_then(coerce_integer),
        &a.fixtures,
        emit,
    )
    .await
}
async fn run(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> axum::response::Response {
    if let Some(error) = validate_body(&q) {
        return validation_error(vec![error]).into_response();
    }
    if let Some(error) = validate_max_steps(&q) {
        return validation_error(vec![error]).into_response();
    }
    let Some(_guard) = session_lock(&a, &id).await else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"detail":"session ownership conflict"})),
        )
            .into_response();
    };
    let Some((mut s, c)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    commands::attach(&mut s, &q);
    if s.status != "active" {
        return (
            StatusCode::CONFLICT,
            Json(
                json!({"detail":"Session is terminal; use POST /sessions/{id}/resume to continue it."}),
            ),
        ).into_response();
    }
    let session_uuid = match Uuid::parse_str(&id) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
    };
    let (owner, fence) = match acquire_execution_lease(&a.db, session_uuid).await {
        Ok(value) => value,
        Err(error) if error.to_string().contains("ownership conflict") => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"detail":error.to_string()})),
            )
                .into_response();
        }
        Err(error) => return internal(error).into_response(),
    };
    s.execution_owner = Some(owner.to_string());
    s.execution_fence = Some(fence);
    if s.run_status == "running" {
        s.run_status = "interrupted".into();
        s.run_ended_at = Some(transcript_timestamp());
        s.run_termination_reason = Some("previous_worker_abandoned".into());
        if let Err(error) = save(
            &a,
            &mut s,
            &c,
            json!({"type":"run_interrupted","reason":"previous_worker_abandoned"}),
        )
        .await
        {
            release_execution_lease(&a.db, session_uuid, owner, fence).await;
            return internal(error).into_response();
        }
    }
    let heartbeat = start_execution_heartbeat(a.db.clone(), session_uuid, owner, fence);
    let save_app = a.clone();
    let save_config = c.clone();
    let revision = Arc::new(AtomicI64::new(s.revision));
    let emit_revision = revision.clone();
    let mut emit = move |event: Value, state: Session| {
        let app = save_app.clone();
        let config = save_config.clone();
        let revision = emit_revision.clone();
        async move {
            let mut state = state;
            state.revision = revision.load(Ordering::SeqCst);
            save(&app, &mut state, &config, event)
                .await
                .map_err(|e| e.to_string())?;
            revision.store(state.revision, Ordering::SeqCst);
            if let Some(run)=&state.run_id {
                let cancelled:bool=sqlx::query_scalar("SELECT cancel_requested FROM agent_runs WHERE session_id=$1::uuid AND run_id=$2::uuid").bind(&state.id).bind(run).fetch_one(&app.db).await.map_err(|e|e.to_string())?;
                if cancelled{return Err("run_cancelled".into());}
            }
            Ok(())
        }
        .boxed()
    };
    let response = match execute(&a, &mut s, &c, &q, &mut emit).await {
        Ok(()) => {
            s.revision = revision.load(Ordering::SeqCst);
            let status = s.status.clone();
            match save(
                &a,
                &mut s,
                &c,
                json!({"type":"run_finished","status":status}),
            )
            .await
            {
                Ok(_) => committed_command_response(&a.db, &q).await,
                Err(e) => internal(e).into_response(),
            }
        }
        Err(error) if error == "run_cancelled" || error.starts_with("recovery_required") => {
            s.revision = revision.load(Ordering::SeqCst);
            if error == "run_cancelled" {
                s.acknowledge_committed_outboxes();
            }
            s.status = "failed".into();
            s.run_status = if error == "run_cancelled" {
                "cancelled"
            } else {
                "blocked"
            }
            .into();
            s.run_ended_at = Some(transcript_timestamp());
            s.run_termination_reason = Some(
                if error == "run_cancelled" {
                    "run_cancelled"
                } else {
                    "recovery_required"
                }
                .into(),
            );
            let message = if error == "run_cancelled" {
                "Run cancelled; use /resume to recover committed work."
            } else {
                "Safe recovery requires resolving the original action."
            };
            s.final_answer = Some(message.into());
            s.last_error =
                Some(json!({"code":s.run_termination_reason,"message":message,"recoverable":true}));
            let status = s.run_status.clone();
            match save(
                &a,
                &mut s,
                &c,
                json!({"type":"run_finished","status":status}),
            )
            .await
            {
                Ok(()) if status != "blocked" => committed_command_response(&a.db, &q).await,
                Ok(()) => (
                    StatusCode::ACCEPTED,
                    Json(json!({"session":s,"message":message})),
                )
                    .into_response(),
                Err(e) => internal(e).into_response(),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"detail":format!("Model service response was invalid: {e}")})),
        )
            .into_response(),
    };
    heartbeat.abort();
    release_execution_lease(&a.db, session_uuid, owner, fence).await;
    response
}
// Serve the exact outcome committed with the terminal checkpoint. Rehydrating
// session state here can change diagnostic tails and other response fields,
// making the initial response differ from its durable replay.
async fn committed_command_response(db: &PgPool, request: &Value) -> axum::response::Response {
    let command = &request["_command"];
    match sqlx::query("SELECT response_status,response_body FROM agent_commands WHERE command_id=$1::uuid AND response_status IS NOT NULL")
        .bind(command["id"].as_str()).fetch_one(db).await {
        Ok(row) => {
            let status: i32 = row.get("response_status");
            let body: Value = row.get("response_body");
            (StatusCode::from_u16(status as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), Json(body)).into_response()
        }
        Err(error) => internal(error).into_response(),
    }
}
async fn stream_run(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> impl IntoResponse {
    lifecycle(a, id, q, false).await
}
async fn resume(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> impl IntoResponse {
    lifecycle(a, id, q, true).await
}
async fn cancel_session(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> impl IntoResponse {
    if load(&a, &id).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    }
    let session = match Uuid::parse_str(&id) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
    };
    let mut tx = match a.db.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal(error).into_response(),
    };
    let exists = match sqlx::query_scalar::<_, bool>(
        "SELECT TRUE FROM agent_sessions WHERE session_id=$1 FOR UPDATE",
    )
    .bind(session)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(Some(exists)) => exists,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
        Err(error) => return internal(error).into_response(),
    };
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    }
    let command = q["_command"].clone();
    if let Err(e) = commands::guard(&mut tx, &command).await {
        return internal(e).into_response();
    }
    let rows = match sqlx::query("UPDATE agent_runs SET cancel_requested=TRUE,cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()) WHERE session_id=$1 AND run_id=$2::uuid AND status='running' RETURNING run_id::text")
        .bind(session).bind(q["run_id"].as_str()).fetch_all(&mut *tx).await {
            Ok(rows) => rows,
            Err(error) => return internal(error).into_response(),
        };
    let cancel_requested_for: Vec<String> = rows
        .into_iter()
        .map(|row| row.get::<String, _>("run_id"))
        .collect();
    let response = json!({"session_id":id,"cancel_requested_for":cancel_requested_for,"message":"Cancellation is cooperative; in-flight effects are not undone."});
    if let Err(e) = commands::finish(&mut tx, &command, 200, &response).await {
        return internal(e).into_response();
    }
    if let Err(error) = tx.commit().await {
        return internal(error).into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "session_id":id,
            "cancel_requested_for":cancel_requested_for,
            "message":"Cancellation is cooperative; in-flight effects are not undone."
        })),
    )
        .into_response()
}
async fn lifecycle(a: App, id: String, q: Value, resume: bool) -> axum::response::Response {
    if let Some(error) = validate_body(&q) {
        return validation_error(vec![error]).into_response();
    }
    let mut errors = vec![];
    if let Some(error) = validate_max_steps(&q) {
        errors.push(error);
    }
    if resume && let Some(value) = q.get("content") {
        if !value.is_null() && !value.is_string() {
            errors.push(field_error(
                "content",
                Some(value),
                "string_type",
                "Input should be a valid string",
                None,
            ));
        } else if value.as_str() == Some("") {
            errors.push(field_error(
                "content",
                Some(value),
                "string_too_short",
                "String should have at least 1 character",
                Some(json!({"min_length":1})),
            ));
        }
    }
    if !errors.is_empty() {
        return validation_error(errors).into_response();
    }
    let Some(guard) = session_lock(&a, &id).await else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"detail":"session ownership conflict"})),
        )
            .into_response();
    };
    let Some((mut s, c)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    commands::attach(&mut s, &q);
    if !resume && s.status != "active" {
        return(StatusCode::CONFLICT,Json(json!({"detail":"Session is terminal; use POST /sessions/{id}/resume to continue it"}))).into_response();
    }
    let session_uuid = match Uuid::parse_str(&id) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail":"Session not found"})),
            )
                .into_response();
        }
    };
    let (owner, fence) = match acquire_execution_lease(&a.db, session_uuid).await {
        Ok(value) => value,
        Err(error) if error.to_string().contains("ownership conflict") => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"detail":error.to_string()})),
            )
                .into_response();
        }
        Err(error) => return internal(error).into_response(),
    };
    s.execution_owner = Some(owner.to_string());
    s.execution_fence = Some(fence);
    if s.run_status == "running" {
        s.run_status = "interrupted".into();
        s.run_ended_at = Some(transcript_timestamp());
        s.run_termination_reason = Some("previous_worker_abandoned".into());
        if let Err(error) = save(
            &a,
            &mut s,
            &c,
            json!({"type":"run_interrupted","reason":"previous_worker_abandoned"}),
        )
        .await
        {
            release_execution_lease(&a.db, session_uuid, owner, fence).await;
            return internal(error).into_response();
        }
    }
    let heartbeat = start_execution_heartbeat(a.db.clone(), session_uuid, owner, fence);
    if resume {
        if let Some(x) = q["content"].as_str() {
            s.append_message(Message {
                role: "user".into(),
                content: Some(x.into()),
                ..Default::default()
            })
        }
        s.status = "active".into();
        s.final_answer = None;
        s.reset_resume_counters();
        if let Err(error) = save(&a, &mut s, &c, json!({"type":"session_resumed","has_new_user_message":q["content"].as_str().is_some_and(|x| !x.is_empty())})).await {
            heartbeat.abort();
            release_execution_lease(&a.db, session_uuid, owner, fence).await;
            return internal(error).into_response();
        }
    }
    let (tx, output, disconnected) = sse::event_channel();
    // Observe response-body drop without keeping an event sender alive. Normal
    // completion drains the queue and closes the body without cancellation.
    // On disconnect, let the in-flight invocation settle and commit its result.
    let disconnect_db = a.db.clone();
    tokio::spawn(async move {
        if disconnected.await.is_ok() {
            let _ = sqlx::query(
                "UPDATE agent_runs SET cancel_requested=TRUE,cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()) WHERE session_id=$1 AND owner=$2 AND fence=$3 AND status='running'",
            )
            .bind(session_uuid)
            .bind(owner)
            .bind(fence)
            .execute(&disconnect_db)
            .await;
        }
    });
    let task_app = a.clone();
    let emit_app = task_app.clone();
    let task_config = c.clone();
    let emit_config = task_config.clone();
    let event_sender = tx.clone();
    let verbose_setup = serde_json::from_value::<Config>(c.clone())
        .map(|x| x.output.verbose_setup)
        .unwrap_or(false);
    let task_resume = resume;
    tokio::spawn(async move {
        let _guard = guard;
        let mut state = s;
        if task_resume {
            let restored = json!({"type":"status","message":"Session restored from PostgreSQL; resuming agent work."});
            if let Err(error) = save(&task_app, &mut state, &task_config, restored.clone()).await {
                let _=tx.send(json!({"type":"error","code":"persistence_error","message":error.to_string(),"recoverable":true})).await;
                heartbeat.abort();
                release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
                return;
            }
            let restored = events::decorate(&task_app, &state, restored).await;
            let _ = tx.send(restored).await;
        }
        if verbose_setup {
            let (message, config_ref) = if task_resume {
                (
                    "Restored immutable config, workflow evidence, memory, and compaction ledger.",
                    state.config_ref.clone(),
                )
            } else {
                (
                    "Agent initialized from persisted session state.",
                    state.config_ref.clone(),
                )
            };
            let setup = json!({"type":"status","message":message,"config_ref":config_ref});
            if let Err(error) = save(&task_app, &mut state, &task_config, setup.clone()).await {
                let _ = tx
                    .send(json!({"type":"error","message":error.to_string(),"recoverable":true}))
                    .await;
                heartbeat.abort();
                release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
                return;
            }
            let setup = events::decorate(&task_app, &state, setup).await;
            let _ = tx.send(setup).await;
        }
        let revision = Arc::new(AtomicI64::new(state.revision));
        let emit_revision = revision.clone();
        let mut emit = move |event: Value, snapshot: Session| {
            let app = emit_app.clone();
            let config = emit_config.clone();
            let sender = event_sender.clone();
            let revision = emit_revision.clone();
            async move {
                let mut snapshot = snapshot;
                snapshot.revision = revision.load(Ordering::SeqCst);
                save(&app, &mut snapshot, &config, event.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                revision.store(snapshot.revision, Ordering::SeqCst);
                let cancellation_requested = if let Some(run_id) = snapshot.run_id.as_deref() {
                    sqlx::query_scalar::<_, bool>(
                        "SELECT cancel_requested FROM agent_runs WHERE run_id=$1::uuid",
                    )
                    .bind(run_id)
                    .fetch_optional(&app.db)
                    .await
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false)
                } else {
                    false
                };
                if cancellation_requested {
                    return Err("run_cancelled".into());
                }
                let event=events::decorate(&app,&snapshot,event).await;
                if sender.send(event).await.is_err() {
                    if let Some(run_id) = snapshot.run_id.as_deref() {
                        let _ = sqlx::query(
                            "UPDATE agent_runs SET cancel_requested=TRUE,cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()) WHERE run_id=$1::uuid AND status='running'",
                        )
                        .bind(run_id)
                        .execute(&app.db)
                        .await;
                    }
                    return Err("run_cancelled".into());
                }
                Ok(())
            }
            .boxed()
        };
        let outcome = execute(&task_app, &mut state, &task_config, &q, &mut emit).await;
        state.revision = revision.load(Ordering::SeqCst);
        if let Err(error) = outcome {
            if error.contains("commit_outcome_unresolved") {
                if let Some(command) = &state.command {
                    let _=sqlx::query("UPDATE agent_commands SET status='uncertain',owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1::uuid AND owner=$2::uuid AND fence=$3 AND status='started'").bind(command["id"].as_str()).bind(command["owner"].as_str()).bind(command["fence"].as_i64()).execute(&task_app.db).await;
                }
                let _=tx.send(json!({"type":"error","code":"persistence_error","message":"Commit outcome is unresolved; inspect the original command before recovery.","recoverable":true})).await;
                heartbeat.abort();
                release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
                return;
            }
            let cancelled = error == "run_cancelled";
            if cancelled {
                state.acknowledge_committed_outboxes();
            }
            state.status = "failed".into();
            state.run_status = if cancelled {
                "cancelled"
            } else if error.starts_with("recovery_required") {
                "blocked"
            } else {
                "failed"
            }
            .into();
            state.final_answer = if cancelled {
                Some("Run cancelled; use /resume to recover committed work.".into())
            } else {
                Some(format!("Agent run failed: {error}"))
            };
            state.run_ended_at = Some(transcript_timestamp());
            state.run_termination_reason = Some(
                if cancelled {
                    "run_cancelled"
                } else if state.run_status == "blocked" {
                    "recovery_required"
                } else {
                    "run_failed"
                }
                .into(),
            );
            state.last_error = Some(json!({
                "code": if cancelled { "run_cancelled" } else { "run_failed" },
                "message": if cancelled { "Run cancelled; use /resume to recover committed work.".to_string() } else { error.clone() },
                "recoverable": true
            }));
            let failure = json!({
                "type":"error",
                "message":state.last_error.as_ref().and_then(|e| e.get("message")).cloned(),
                "code":state.last_error.as_ref().and_then(|e| e.get("code")).cloned(),
                "recoverable":true
            });
            match save(&task_app, &mut state, &task_config, failure.clone()).await {
                Ok(_) => revision.store(state.revision, Ordering::SeqCst),
                Err(error) => {
                    let _ = tx.send(json!({"type":"error","code":"persistence_error","message":format!("Unable to persist run failure: {error}"),"recoverable":true})).await;
                    heartbeat.abort();
                    release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
                    return;
                }
            }
            let _ = tx.send(failure).await;
        }
        state.revision = revision.load(Ordering::SeqCst);
        let finished = json!({"type":"run_finished","status":state.status});
        if let Err(error) = save(&task_app, &mut state, &task_config, finished.clone()).await {
            let _ = tx.send(json!({"type":"error","code":"persistence_error","message":format!("Unable to persist run completion: {error}"),"recoverable":true})).await;
            heartbeat.abort();
            release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
            return;
        }
        let finished = events::decorate(&task_app, &state, finished).await;
        let _ = tx.send(finished).await;
        heartbeat.abort();
        release_execution_lease(&task_app.db, session_uuid, owner, fence).await;
    });
    let mut response = Sse::new(output).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-cache".parse().unwrap());
    response
        .headers_mut()
        .insert("connection", "keep-alive".parse().unwrap());
    response
}
fn internal(e: sqlx::Error) -> (StatusCode, Json<Value>) {
    let message = e.to_string();
    let status = if message.contains("revision conflict")
        || message.contains("ownership conflict")
        || message.contains("lease lost")
        || message.contains("fenced worker")
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, Json(json!({"detail":message})))
}

#[cfg(test)]
mod du05_state_tests {
    use super::*;

    #[test]
    fn du05_c01_message_identity_sequence_copy_and_wire_projection() {
        let mut state: Session = serde_json::from_value(json!({
            "id": Uuid::new_v4().to_string(), "goal": "goal", "status": "active"
        }))
        .unwrap();
        state.append_message(Message {
            role: "user".into(),
            content: Some("goal".into()),
            ..Default::default()
        });
        state.append_message(Message {
            role: "user".into(),
            content: Some("follow-up".into()),
            ..Default::default()
        });
        assert_eq!(state.memory[0].sequence, Some(1));
        assert_eq!(state.memory[1].sequence, Some(2));
        assert_ne!(state.memory[0].message_id, state.memory[1].message_id);
        assert!(state.memory.iter().all(|message| {
            message
                .created_at
                .as_deref()
                .is_some_and(|value| value.ends_with('Z'))
        }));
        let original_payload = state.pending_transcript[1].payload.clone();
        state.memory[1].content = Some("changed after staging".into());
        assert_eq!(state.pending_transcript[1].payload, original_payload);
        let public = serde_json::to_value(&state.memory[0]).unwrap();
        assert!(public.get("message_id").and_then(Value::as_str).is_some());
        let wire = context::wire_message(&state.memory[0]);
        assert!(wire.get("message_id").is_none());
        assert!(wire.get("sequence").is_none());
        assert_eq!(wire, json!({"role":"user","content":"goal"}));
    }
}

#[cfg(test)]
mod contract_v2_regressions {
    use super::*;
    #[tokio::test]
    async fn local_ownership_rejects_competitors_without_waiting() {
        let db = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let app = App {
            db: db.clone(),
            fixtures: db,
            root: repo_root().join("config"),
            locks: Arc::new(Mutex::new(HashMap::new())),
        };
        let held = session_lock(&app, "session").await.unwrap();
        assert!(session_lock(&app, "session").await.is_none());
        drop(held);
        assert!(session_lock(&app, "session").await.is_some());
    }
    #[test]
    fn cancellation_acknowledges_committed_outboxes_but_keeps_pending_action() {
        let mut state:Session=serde_json::from_value(json!({"id":"session","goal":"goal","status":"active","pending_tool_calls":[{"id":"call","name":"inspect_log","arguments":{}}]})).unwrap();
        state.append_message(Message {
            role: "user".into(),
            content: Some("goal".into()),
            ..Default::default()
        });
        state
            .pending_execution_outbox
            .push(json!({"execution_id":"action","status":"succeeded"}));
        state
            .attempts_pending
            .push(json!({"attempt_id":"attempt","status":"succeeded"}));
        state.acknowledge_committed_outboxes();
        assert!(state.pending_transcript.is_empty());
        assert!(state.pending_execution_outbox.is_empty());
        assert!(state.attempts_pending.is_empty());
        assert_eq!(state.pending_tool_calls[0]["id"], "call");
    }
    #[test]
    fn committed_checkpoint_metadata_replaces_provisional_sequences() {
        let mut state: Session =
            serde_json::from_value(json!({"id":"session","goal":"goal","status":"active"}))
                .unwrap();
        state.append_message(Message {
            role: "user".into(),
            content: Some("goal".into()),
            ..Default::default()
        });
        let mut committed = state.memory.clone();
        committed[0].sequence = Some(7);
        state.checkpoint_ack = Some(Arc::new(std::sync::Mutex::new(engine::CheckpointAck {
            needs_reload: false,
            revision: 12,
            next_sequence: 19,
            memory: committed,
            summaries: Default::default(),
        })));
        state.acknowledge_checkpoint();
        assert_eq!(state.revision, 12);
        assert_eq!(state.next_transcript_sequence, 19);
        assert_eq!(state.memory[0].sequence, Some(7));
        state.append_message(Message {
            role: "user".into(),
            content: Some("continue".into()),
            ..Default::default()
        });
        assert_eq!(state.memory[1].sequence, Some(19));
    }
}
