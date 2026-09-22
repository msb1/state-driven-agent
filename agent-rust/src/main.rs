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
    env,
    path::{Path as FsPath, PathBuf},
};
use uuid::Uuid;
#[derive(Clone)]
struct App {
    db: PgPool,
    root: PathBuf,
}
#[tokio::main]
async fn main() {
    let db =
        PgPoolOptions::new()
            .max_connections(10)
            .connect(&env::var("AGENT_SESSION_DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://user:password@192.168.1.50:5432/elite_rag".into()
            }))
            .await
            .expect("postgres");
    sqlx::query("CREATE TABLE IF NOT EXISTS agent_sessions (session_id UUID PRIMARY KEY,state JSONB NOT NULL,config JSONB NOT NULL,config_ref TEXT,config_sha256 TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE TABLE IF NOT EXISTS agent_session_events(event_id BIGSERIAL PRIMARY KEY,session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,event JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx ON agent_session_events(session_id,event_id)").execute(&db).await.expect("schema");
    let app = Router::new()
        .route("/health", get(health))
        .route("/openapi.json", get(openapi))
        .route("/docs", get(docs))
        .route("/configs", get(configs))
        .route("/sessions", post(create))
        .route("/sessions/{id}", get(session))
        .route("/sessions/{id}/messages", post(message))
        .route("/sessions/{id}/run", post(run))
        .route("/sessions/{id}/run/stream", post(stream_run))
        .route("/sessions/{id}/resume", post(resume))
        .with_state(App {
            db,
            root: PathBuf::from(
                env::var("AGENT_CONFIG_ROOT").unwrap_or_else(|_| "../config".into()),
            ),
        });
    let l = tokio::net::TcpListener::bind(format!(
        "0.0.0.0:{}",
        env::var("PORT").unwrap_or_else(|_| "8000".into())
    ))
    .await
    .unwrap();
    axum::serve(l, app).await.unwrap()
}
/// The shared REST contract.  Keeping the document alongside the other
/// implementations makes the interactive Swagger UI interchangeable with
/// FastAPI's `/docs` endpoint.
async fn openapi() -> Json<Value> {
    Json(
        serde_yaml::from_str(include_str!("../../agent-go/openapi.yaml"))
            .expect("the checked-in OpenAPI document must be valid YAML"),
    )
}
async fn docs() -> impl IntoResponse {
    (
        [("content-type", "text/html; charset=utf-8")],
        r#"<!doctype html><html><head><title>State-Driven AI Agent API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui'});</script>
</body></html>"#,
    )
}
async fn health(State(a): State<App>) -> Json<Value> {
    Json(
        json!({"status":"ok","config_root":a.root,"available_configs":std::fs::read_dir(&a.root).map(|d|d.flatten().filter(|e|matches!(e.path().extension().and_then(|x|x.to_str()),Some("yaml")|Some("yml"))).count()).unwrap_or(0)}),
    )
}
fn cfg(a: &App, r: &str) -> Result<(Config, Value, String), String> {
    if r.is_empty() || FsPath::new(r).is_absolute() || r.split('/').any(|x| x == "..") {
        return Err("config_ref must be a non-empty relative YAML path".into());
    }
    let p = a.root.join(r);
    if !p.starts_with(&a.root) {
        return Err("config_ref must remain within AGENT_CONFIG_ROOT".into());
    }
    let b = std::fs::read(p).map_err(|e| e.to_string())?;
    let raw = expand(serde_yaml::from_slice::<Value>(&b).map_err(|e| e.to_string())?);
    let agent = raw.get("agent").cloned().ok_or("missing agent mapping")?;
    let c: Config = serde_json::from_value(agent.clone())
        .map_err(|e| format!("invalid YAML workflow config: {e}"))?;
    Ok((c, agent, format!("{:x}", Sha256::digest(b))))
}
fn expand(v: Value) -> Value {
    match v {
        Value::String(s) => {
            let mut x = s;
            while let Some(i) = x.find("${") {
                let Some(j) = x[i..].find('}').map(|n| i + n) else {
                    break;
                };
                let q = &x[i + 2..j];
                let (mut k, mut d) = (q, "");
                if let Some(n) = q.find(":-") {
                    k = &q[..n];
                    d = &q[n + 2..]
                }
                let z = env::var(k).unwrap_or_else(|_| d.into());
                x.replace_range(i..=j, &z)
            }
            Value::String(x)
        }
        Value::Array(x) => Value::Array(x.into_iter().map(expand).collect()),
        Value::Object(x) => Value::Object(x.into_iter().map(|(k, v)| (k, expand(v))).collect()),
        x => x,
    }
}
async fn configs(State(a): State<App>) -> Json<Value> {
    let mut out = vec![];
    if let Ok(d) = std::fs::read_dir(&a.root) {
        for e in d.flatten() {
            let r = e.file_name().to_string_lossy().into_owned();
            if let Ok((c, _, h)) = cfg(&a, &r) {
                out.push(json!({"config_ref":r,"sha256":h,"agent_name":c.name,"tools":c.tools.iter().map(|t|t.name.clone()).collect::<Vec<_>>(),"workflow_phases":c.workflow.as_ref().map(|w|w.phases.iter().map(|p|p.id.clone()).collect::<Vec<_>>()).unwrap_or_default(),"verbose_setup":c.output.verbose_setup}))
            }
        }
    }
    Json(Value::Array(out))
}
async fn create(
    State(a): State<App>,
    Json(q): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let goal = q["user_prompt"].as_str().filter(|x| !x.is_empty()).ok_or((
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"detail":"user_prompt is required"})),
    ))?;
    let r = q["config_ref"].as_str().unwrap_or("test-case-1.yaml");
    let (c, raw, h) = cfg(&a, r).map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":format!("Invalid config_ref: {e}")})),
        )
    })?;
    let id = Uuid::new_v4().to_string();
    let workflow = c.workflow.as_ref().map(|w| Workflow {
        active_phase: Some(w.entry_phase.clone()),
        ..Default::default()
    });
    let s = Session {
        id: id.clone(),
        goal: goal.into(),
        config_ref: Some(r.into()),
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
    let Some(x) = q["content"].as_str().filter(|x| !x.is_empty()) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":"content is required"})),
        );
    };
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
async fn execute(a: &App, s: &mut Session, c: &Value, q: &Value) -> Result<Vec<Value>, String> {
    let config: Config = serde_json::from_value(c.clone()).map_err(|e| e.to_string())?;
    let e = Engine::new(config)?;
    let mut events = vec![];
    let mut emit = |v: Value| {
        events.push(v);
        async { Ok(()) }.boxed()
    };
    e.run(s, q["max_steps"].as_i64(), &a.db, &mut emit).await?;
    Ok(events)
}
async fn run(
    State(a): State<App>,
    Path(id): Path<String>,
    Json(q): Json<Value>,
) -> axum::response::Response {
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
    match execute(&a, &mut s, &c, &q).await {
        Ok(_) => match save(&a, &s, &c, json!({"type":"run_finished","status":s.status})).await {
            Ok(_) => (
                StatusCode::OK,
                Json(json!({"session":s,"message":s.final_answer})),
            )
                .into_response(),
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
    let mut ev = vec![];
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
        ev.push(json!({"type":"status","message":"Session restored from PostgreSQL; resuming agent work."}))
    } else {
        ev.push(
            json!({"type":"status","message":"Agent initialized from persisted session state."}),
        )
    }
    match execute(&a, &mut s, &c, &q).await {
        Ok(x) => ev.extend(x),
        Err(e) => {
            s.status = "failed".into();
            s.final_answer = Some(format!("Agent run failed: {e}"));
            ev.push(json!({"type":"error","message":s.final_answer,"recoverable":true}))
        }
    }
    ev.push(json!({"type":"run_finished","status":s.status}));
    ev.push(json!({"type":"status","message":"Agent run finished.","status":s.status}));
    for x in &ev {
        if let Err(error) = save(&a, &s, &c, x.clone()).await {
            return internal(error).into_response();
        }
    }
    Sse::new(stream::iter(ev.into_iter().map(|x| {
        Ok::<_, std::convert::Infallible>(axum::response::sse::Event::default().data(x.to_string()))
    })))
    .into_response()
}
fn internal(e: sqlx::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"detail":e.to_string()})),
    )
}
