//! Axum REST/SSE service. PostgreSQL is the authority for every session.
mod engine;
mod tools;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Sse},
    routing::{get, post},
};
use engine::{Config, Engine, Message, Session, Workflow};
use futures_util::{FutureExt, stream};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    collections::HashMap,
    env,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
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
async fn session_lock(app: &App, id: &str) -> OwnedMutexGuard<()> {
    let lock = {
        let mut locks = app.locks.lock().await;
        locks
            .entry(id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    lock.lock_owned().await
}
fn validation_error(errors: Vec<Value>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"detail": errors})),
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
    for statement in [
        "CREATE TABLE IF NOT EXISTS agent_sessions (session_id UUID PRIMARY KEY,state JSONB NOT NULL,config JSONB NOT NULL,config_ref TEXT,config_sha256 TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        "CREATE TABLE IF NOT EXISTS agent_session_events(event_id BIGSERIAL PRIMARY KEY,session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,event JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        "CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx ON agent_session_events(session_id,event_id)",
    ] {
        sqlx::query(statement).execute(&db).await.expect("schema");
    }
    let app = Router::new()
        .route("/health", get(health))
        .route("/openapi.json", get(openapi))
        .route("/docs", get(docs))
        .route("/docs/oauth2-redirect", get(oauth2_redirect))
        .route("/redoc", get(redoc))
        .route("/configs", get(configs))
        .route("/sessions", post(create))
        .route("/sessions/{id}", get(session))
        .route("/sessions/{id}/messages", post(message))
        .route("/sessions/{id}/run", post(run))
        .route("/sessions/{id}/run/stream", post(stream_run))
        .route("/sessions/{id}/resume", post(resume))
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
    let path = repo_root().join(".env");
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
        if key.is_empty() || env::var_os(key).is_some() {
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
    Json(json!({"status":"ok","config_root":a.root,"available_configs":collect_configs(&a).len()}))
}
fn cfg(a: &App, r: &str) -> Result<(Config, Value, String), String> {
    if r.is_empty() || FsPath::new(r).is_absolute() {
        return Err("config_ref must be a non-empty relative YAML path".into());
    }
    let root = a.root.canonicalize().map_err(|e| e.to_string())?;
    let p = a.root.join(r).canonicalize().map_err(|e| e.to_string())?;
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
    let snapshot = serde_json::to_value(&c).map_err(|e| e.to_string())?;
    Ok((c, snapshot, format!("{:x}", Sha256::digest(b))))
}
fn validate_config(c: &Config) -> Result<(), String> {
    if c.memory.max_tokens <= 0 || c.memory.max_steps <= 0 || c.memory.raw_turns_to_keep == 0 {
        return Err(
            "memory thresholds must be positive and raw_turns_to_keep must be at least 1".into(),
        );
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
            }
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
    for (r, c, h) in collect_configs(&a) {
        if c.tools.iter().all(|tool| tools::is_supported(&tool.name)) {
            out.push(json!({"config_ref":r,"sha256":h,"agent_name":c.name,"tools":c.tools.iter().map(|t|t.name.clone()).collect::<Vec<_>>(),"workflow_phases":c.workflow.as_ref().map(|w|w.phases.iter().map(|p|p.id.clone()).collect::<Vec<_>>()).unwrap_or_default(),"verbose_setup":c.output.verbose_setup}));
        }
    }
    Json(Value::Array(out))
}
fn collect_configs(a: &App) -> Vec<(String, Config, String)> {
    let mut refs = vec![];
    collect_yaml(&a.root, &a.root, &mut refs);
    refs.sort();
    refs.into_iter()
        .filter_map(|r| cfg(a, &r).ok().map(|(c, _, h)| (r, c, h)))
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
    let requested_ref = q["config_ref"].as_str().unwrap_or("test-case-1.yaml");
    let (c, raw, h) = cfg(&a, requested_ref).map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":format!("Invalid config_ref: {e}")})),
        )
    })?;
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
    let id = Uuid::new_v4().to_string();
    let workflow = c.workflow.as_ref().map(|w| Workflow {
        active_phase: Some(w.entry_phase.clone()),
        ..Default::default()
    });
    let s = Session {
        id: id.clone(),
        goal: goal.into(),
        config_ref: Some(r.clone()),
        config_sha256: Some(h.clone()),
        memory: vec![Message {
            role: "user".into(),
            content: Some(goal.into()),
            ..Default::default()
        }],
        compacted_context: None,
        compaction_events: vec![],
        workflow,
        step_count: 0,
        run_count: 0,
        status: "active".into(),
        final_answer: None,
    };
    save(
        &a,
        &s,
        &raw,
        json!({"type":"session_created","config_ref":r,"config_sha256":h}),
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
    let x: Option<(Value, Value)> =
        sqlx::query_as("SELECT state,config FROM agent_sessions WHERE session_id=$1")
            .bind(id)
            .fetch_optional(&a.db)
            .await
            .ok()?;
    let (state, config) = x?;
    Some((serde_json::from_value(state).ok()?, config))
}
async fn save(a: &App, s: &Session, c: &Value, e: Value) -> Result<(), sqlx::Error> {
    let id = Uuid::parse_str(&s.id).unwrap();
    sqlx::query("INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT(session_id) DO UPDATE SET state=EXCLUDED.state,config=EXCLUDED.config,config_ref=EXCLUDED.config_ref,config_sha256=EXCLUDED.config_sha256,updated_at=now()").bind(id).bind(serde_json::to_value(s).unwrap()).bind(c).bind(&s.config_ref).bind(&s.config_sha256).execute(&a.db).await?;
    sqlx::query("INSERT INTO agent_session_events(session_id,event) VALUES($1,$2)")
        .bind(id)
        .bind(e)
        .execute(&a.db)
        .await?;
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
async fn message(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> impl IntoResponse {
    if let Some(error) = validate_body(&q) {
        return validation_error(vec![error]);
    }
    let _guard = session_lock(&a, &id).await;
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
    if s.status != "active" {
        return (
            StatusCode::CONFLICT,
            Json(
                json!({"detail":"Cannot append to a terminal session; use /resume with content instead"}),
            ),
        );
    }
    let x = q["content"].as_str().unwrap();
    s.memory.push(Message {
        role: "user".into(),
        content: Some(x.into()),
        ..Default::default()
    });
    match save(&a, &s, &c, json!({"type":"user_message_appended"})).await {
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
    let _guard = session_lock(&a, &id).await;
    let Some((mut s, c)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    if s.status != "active" {
        return (
            StatusCode::OK,
            Json(
                json!({"session":s,"message":"Session is terminal; use POST /sessions/{id}/resume to continue it."}),
            ),
        ).into_response();
    }
    let save_app = a.clone();
    let save_config = c.clone();
    let mut emit = move |event: Value, state: Session| {
        let app = save_app.clone();
        let config = save_config.clone();
        async move {
            save(&app, &state, &config, event)
                .await
                .map_err(|e| e.to_string())
        }
        .boxed()
    };
    match execute(&a, &mut s, &c, &q, &mut emit).await {
        Ok(()) => match save(&a, &s, &c, json!({"type":"run_finished","status":s.status})).await {
            Ok(_) => {
                let message = s
                    .final_answer
                    .clone()
                    .filter(|answer| !answer.is_empty())
                    .unwrap_or_else(|| "Run stopped.".into());
                (StatusCode::OK, Json(json!({"session":s,"message":message}))).into_response()
            }
            Err(e) => internal(e).into_response(),
        },
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"detail":format!("Model service response was invalid: {e}")})),
        )
            .into_response(),
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
    let guard = session_lock(&a, &id).await;
    let Some((mut s, c)) = load(&a, &id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Session not found"})),
        )
            .into_response();
    };
    if !resume && s.status != "active" {
        return(StatusCode::CONFLICT,Json(json!({"detail":"Session is terminal; use POST /sessions/{id}/resume to continue it"}))).into_response();
    }
    if resume {
        if let Some(x) = q["content"].as_str() {
            s.memory.push(Message {
                role: "user".into(),
                content: Some(x.into()),
                ..Default::default()
            })
        }
        s.status = "active".into();
        s.final_answer = None;
        if let Err(error) = save(&a, &s, &c, json!({"type":"session_resumed","has_new_user_message":q["content"].as_str().is_some_and(|x| !x.is_empty())})).await {
            return internal(error).into_response();
        }
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<Value>(32);
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
            let _ = tx.send(json!({"type":"status","message":"Session restored from PostgreSQL; resuming agent work."})).await;
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
            let _ = save(&task_app, &state, &task_config, setup.clone()).await;
            let _ = tx.send(setup).await;
        }
        let mut emit = move |event: Value, snapshot: Session| {
            let app = emit_app.clone();
            let config = emit_config.clone();
            let sender = event_sender.clone();
            async move {
                save(&app, &snapshot, &config, event.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                sender.send(event).await.map_err(|e| e.to_string())
            }
            .boxed()
        };
        let outcome = execute(&task_app, &mut state, &task_config, &q, &mut emit).await;
        if let Err(error) = outcome {
            state.status = "failed".into();
            state.final_answer = Some(format!("Agent run failed: {error}"));
            let failure = json!({"type":"error","message":state.final_answer,"recoverable":true});
            let _ = save(&task_app, &state, &task_config, failure.clone()).await;
            let _ = tx.send(failure).await;
        }
        let finished = json!({"type":"run_finished","status":state.status});
        let _ = save(&task_app, &state, &task_config, finished.clone()).await;
        let _ = tx.send(finished).await;
        let done = json!({"type":"status","message":"Agent run finished.","status":state.status});
        let _ = tx.send(done).await;
    });
    let output = stream::unfold(rx, |mut receiver| async move {
        receiver.recv().await.map(|event| {
            (
                Ok::<_, std::convert::Infallible>(
                    axum::response::sse::Event::default().data(tools::python_json(&event)),
                ),
                receiver,
            )
        })
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
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"detail":e.to_string()})),
    )
}
