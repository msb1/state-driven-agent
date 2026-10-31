//! Fenced durable HTTP admission. PostgreSQL is the authority across workers.
use super::*;
use axum::{
    body::{Body, to_bytes},
    extract::Request,
    middleware::Next,
    response::Response,
};

fn valid_key(key: &str, count: usize) -> bool {
    count == 1
        && !key.is_empty()
        && key.len() <= 255
        && key.bytes().all(|v| (33..=126).contains(&v))
}

fn reject(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"code":code,"detail":message}))).into_response()
}
pub fn normalize(route: &str, mut body: Value) -> Result<Value, String> {
    let fields: &[&str] = if route == "/sessions" {
        &["user_prompt", "config_ref"]
    } else if route.ends_with("/messages") {
        &["content"]
    } else if route.ends_with("/cancel") {
        &["run_id"]
    } else if route.ends_with("/resume") {
        &["content", "max_steps"]
    } else {
        &["max_steps"]
    };
    let obj = body.as_object_mut().ok_or("JSON body must be an object")?;
    if obj.keys().any(|k| !fields.contains(&k.as_str())) {
        return Err("Unknown request field".into());
    }
    if route == "/sessions" {
        if !obj
            .get("user_prompt")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty())
        {
            return Err("user_prompt is required".into());
        }
        obj.entry("config_ref")
            .or_insert(json!("log-analysis.yaml"));
        if !obj["config_ref"].as_str().is_some_and(|v| !v.is_empty()) {
            return Err("config_ref must be nonempty".into());
        }
    } else if route.ends_with("/messages") {
        if !obj
            .get("content")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty())
        {
            return Err("content must be nonempty".into());
        }
    } else if route.ends_with("/cancel") {
        let id = Uuid::parse_str(
            obj.get("run_id")
                .and_then(Value::as_str)
                .ok_or("run_id required")?,
        )
        .map_err(|_| "run_id must be UUID")?;
        obj.insert("run_id".into(), json!(id.to_string()));
    } else {
        obj.entry("max_steps").or_insert(Value::Null);
        if !obj["max_steps"].is_null()
            && !obj["max_steps"].as_u64().is_some_and(|v| v > 0 && v <= 50)
        {
            return Err("max_steps must be a positive integer".into());
        }
        if route.ends_with("/resume") {
            obj.entry("content").or_insert(Value::Null);
            if !obj["content"].is_null() && !obj["content"].as_str().is_some_and(|v| !v.is_empty())
            {
                return Err("content must be nonempty or null".into());
            }
        }
    }
    identity::canonical(&body)?;
    Ok(body)
}

pub async fn admission(State(a): State<App>, request: Request, next: Next) -> Response {
    if request.method() != axum::http::Method::POST {
        return next.run(request).await;
    }
    match admit(a.clone(), request, next).await {
        Ok(r) => r,
        Err(e) => reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "persistence_error",
            &e.to_string(),
        ),
    }
}
async fn admit(a: App, request: Request, next: Next) -> Result<Response, sqlx::Error> {
    let headers = request.headers();
    let key = match headers.get("Idempotency-Key") {
        None => {
            return Ok(reject(
                StatusCode::UNPROCESSABLE_ENTITY,
                "idempotency_key_required",
                "Idempotency-Key is required",
            ));
        }
        Some(v) => v.to_str().unwrap_or("").to_owned(),
    };
    if !valid_key(&key, headers.get_all("Idempotency-Key").iter().count()) {
        return Ok(reject(
            StatusCode::UNPROCESSABLE_ENTITY,
            "idempotency_key_invalid",
            "Invalid Idempotency-Key",
        ));
    }
    let mut route = request.uri().path().to_owned();
    let mut parts = route.split('/').map(str::to_owned).collect::<Vec<_>>();
    let sid = if parts.len() > 2 && parts[1] == "sessions" {
        match Uuid::parse_str(&parts[2]) {
            Ok(id) => {
                parts[2] = id.to_string();
                Some(id)
            }
            Err(_) => {
                return Ok(reject(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_session_id",
                    "Session ID must be UUID",
                ));
            }
        }
    } else {
        None
    };
    route = parts.join("/");
    let (mut parts, body) = request.into_parts();
    let bytes = match to_bytes(body, 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return Ok(reject(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Request body too large",
            ));
        }
    };
    let body = match std::str::from_utf8(&bytes)
        .map_err(|e| e.to_string())
        .and_then(tools::parse_strict_json)
        .and_then(|v| normalize(&route, v))
    {
        Ok(v) => v,
        Err(e) => {
            return Ok(reject(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_request",
                &e,
            ));
        }
    };
    let hash = identity::digest(&json!(["agent-command-v1", "POST", route, body]))
        .map_err(sqlx::Error::Protocol)?;
    let namespace = env::var("AGENT_DEPLOYMENT_NAMESPACE")
        .map_err(|_| sqlx::Error::Protocol("AGENT_DEPLOYMENT_NAMESPACE is required".into()))?;
    let existing:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(c) FROM agent_commands c WHERE namespace=$1 AND client_scope='local' AND idempotency_key=$2").bind(&namespace).bind(&key).fetch_optional(&a.db).await?;
    if existing.is_none() {
        if let Some(session) = sid {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_sessions WHERE session_id=$1)",
            )
            .bind(session)
            .fetch_one(&a.db)
            .await?;
            if !exists {
                return Ok(reject(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "Session not found",
                ));
            }
        }
    }
    let admission = if existing.is_none() && route == "/sessions" {
        let (config, raw, source) = match cfg(&a, body["config_ref"].as_str().unwrap()) {
            Ok(v) => v,
            Err(e) => {
                return Ok(reject(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_config",
                    &e,
                ));
            }
        };
        if config.tools.iter().any(|t| !tools::is_supported(&t.name)) {
            return Ok(reject(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_config",
                "Unavailable tool",
            ));
        }
        json!({"session_id":Uuid::new_v4().to_string(),"config":raw,"source_sha256":source})
    } else {
        json!({"session_id":sid.map(|v|v.to_string())})
    };
    sqlx::query("INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,session_id,status) VALUES($1,$2,'local',$3,'POST',$4,$5,$6,$7,'pending') ON CONFLICT(namespace,client_scope,idempotency_key) DO NOTHING")
 .bind(Uuid::new_v4()).bind(&namespace).bind(&key).bind(&route).bind(json!({"input":body,"admission":admission})).bind(&hash).bind(sid).execute(&a.db).await?;
    let row:Value=sqlx::query_scalar("SELECT to_jsonb(c) FROM agent_commands c WHERE namespace=$1 AND client_scope='local' AND idempotency_key=$2").bind(&namespace).bind(&key).fetch_one(&a.db).await?;
    let command = Uuid::parse_str(row["command_id"].as_str().unwrap()).unwrap();
    if row["request_sha256"] != hash || row["route"] != route {
        return Ok(reject(
            StatusCode::CONFLICT,
            "idempotency_key_reused",
            "Key already identifies different content or route",
        ));
    }
    if let Some(status) = row["response_status"].as_u64() {
        let mut r = (
            StatusCode::from_u16(status as u16).unwrap(),
            Json(row["response_body"].clone()),
        )
            .into_response();
        r.headers_mut()
            .insert("Idempotency-Replayed", "true".parse().unwrap());
        r.headers_mut().insert(
            "Idempotency-Command-Id",
            command.to_string().parse().unwrap(),
        );
        return Ok(r);
    }
    let owner = Uuid::new_v4();
    let fence:Option<i64>=sqlx::query_scalar("UPDATE agent_commands SET owner=$2,fence=fence+1,lease_expires_at=clock_timestamp()+interval '30 seconds',status='started',updated_at=clock_timestamp() WHERE command_id=$1 AND status IN ('pending','started') AND (owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING fence").bind(command).bind(owner).fetch_optional(&a.db).await?;
    let Some(fence) = fence else {
        let mut r=(StatusCode::ACCEPTED,Json(json!({"command_id":command,"session_id":row["session_id"],"status":row["status"],"status_uri":format!("/commands/{command}")}))).into_response();
        r.headers_mut().insert("Retry-After", "1".parse().unwrap());
        r.headers_mut().insert(
            "Idempotency-Command-Id",
            command.to_string().parse().unwrap(),
        );
        return Ok(r);
    };
    let db = a.db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            match sqlx::query("UPDATE agent_commands SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE command_id=$1 AND owner=$2 AND fence=$3 AND status='started' AND lease_expires_at>clock_timestamp()").bind(command).bind(owner).bind(fence).execute(&db).await {Ok(r) if r.rows_affected()==1=>{},_=>break}
        }
    });
    let mut injected = body;
    injected["_command"] = json!({"id":command,"owner":owner,"fence":fence,"namespace":namespace,"route":route,"admission":row["request"]["admission"]});
    parts.uri = route.parse().unwrap();
    let response = next
        .run(Request::from_parts(
            parts,
            Body::from(serde_json::to_vec(&injected).unwrap()),
        ))
        .await;
    let mut response = response;
    if !response
        .headers()
        .get("Content-Type")
        .is_some_and(|v| v.to_str().unwrap_or("").starts_with("text/event-stream"))
    {
        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, 16 * 1024 * 1024)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
            let status = parts.status.as_u16() as i32;
            if status >= 500 || value.to_string().contains("lease lost") {
                sqlx::query("UPDATE agent_commands SET status='uncertain',owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1 AND owner=$2 AND fence=$3 AND status='started' AND lease_expires_at>clock_timestamp()")
                    .bind(command).bind(owner).bind(fence).execute(&a.db).await?;
            } else {
                sqlx::query("UPDATE agent_commands SET status=$4,response_status=$5,response_body=$6,completed_at=clock_timestamp(),updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL WHERE command_id=$1 AND owner=$2 AND fence=$3 AND status='started' AND lease_expires_at>clock_timestamp()").bind(command).bind(owner).bind(fence).bind(if status>=400{"failed"}else{"succeeded"}).bind(status).bind(value).execute(&a.db).await?;
            }
        }
        response = Response::from_parts(parts, Body::from(bytes));
    }
    response.headers_mut().insert(
        "Idempotency-Command-Id",
        command.to_string().parse().unwrap(),
    );
    Ok(response)
}
pub fn attach(s: &mut Session, q: &Value) {
    s.command = q.get("_command").cloned();
    s.checkpoint_ack = Some(Arc::new(std::sync::Mutex::new(
        engine::CheckpointAck::default(),
    )));
}
pub async fn guard(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    c: &Value,
) -> Result<(), sqlx::Error> {
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_commands WHERE command_id=$1::uuid AND owner=$2::uuid AND fence=$3 AND lease_expires_at>clock_timestamp() AND status='started' FOR UPDATE)").bind(c["id"].as_str()).bind(c["owner"].as_str()).bind(c["fence"].as_i64()).fetch_one(&mut **tx).await?;
    if !valid {
        return Err(sqlx::Error::Protocol("command lease lost".into()));
    }
    Ok(())
}
pub async fn finish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    c: &Value,
    status: i32,
    body: &Value,
) -> Result<(), sqlx::Error> {
    guard(tx, c).await?;
    sqlx::query("UPDATE agent_commands SET status=$4,response_status=$5,response_body=$6,completed_at=clock_timestamp(),updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL WHERE command_id=$1::uuid AND owner=$2::uuid AND fence=$3").bind(c["id"].as_str()).bind(c["owner"].as_str()).bind(c["fence"].as_i64()).bind(if status>=400{"failed"}else{"succeeded"}).bind(status).bind(body).execute(&mut **tx).await?;
    Ok(())
}
pub async fn get(State(a): State<App>, Path(id): Path<String>) -> Response {
    let Ok(id) = Uuid::parse_str(&id) else {
        return reject(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_command_id",
            "Command ID must be UUID",
        );
    };
    let namespace = env::var("AGENT_DEPLOYMENT_NAMESPACE").unwrap_or_default();
    match sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('command_id',command_id,'session_id',session_id,'status',status,'response_status',response_status,'response_body',response_body,'run_id',(SELECT run_id FROM agent_runs WHERE agent_runs.command_id=agent_commands.command_id),'status_uri','/commands/' || command_id::text) FROM agent_commands WHERE command_id=$1 AND namespace=$2 AND client_scope='local'").bind(id).bind(namespace).fetch_optional(&a.db).await {Ok(Some(v))=>Json(v).into_response(),Ok(None)=>reject(StatusCode::NOT_FOUND,"not_found","Command not found"),Err(e)=>super::internal(e).into_response()}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn idempotency_header_domain() {
        assert!(valid_key("Key-1", 1));
        for key in ["", " ", "a b", "café", "line\n"] {
            assert!(!valid_key(key, 1));
        }
        assert!(!valid_key(&"k".repeat(256), 1));
        assert!(!valid_key("key", 2));
    }
    #[test]
    fn shared_request_defaults_and_rejections() {
        assert_eq!(
            normalize("/sessions", json!({"user_prompt":"goal"})).unwrap(),
            json!({"user_prompt":"goal","config_ref":"log-analysis.yaml"})
        );
        assert_eq!(
            normalize("/sessions/id/resume", json!({})).unwrap(),
            json!({"max_steps":null,"content":null})
        );
        for (route, value) in [
            ("/sessions", json!({"user_prompt":"g","extra":1})),
            ("/sessions", json!({"user_prompt":"g","config_ref":null})),
            ("/sessions/id/run", json!({"max_steps":0})),
            ("/sessions/id/run", json!({"max_steps":51})),
            ("/sessions/id/cancel", json!({})),
        ] {
            assert!(normalize(route, value).is_err());
        }
    }
}

pub async fn administrative(
    a: &App,
    session: Uuid,
    route: &str,
    input: Value,
) -> Result<Value, sqlx::Error> {
    let namespace =
        env::var("AGENT_DEPLOYMENT_NAMESPACE").map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let id = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let hash = identity::digest(&json!(["agent-command-v1", "POST", route, input]))
        .map_err(sqlx::Error::Protocol)?;
    sqlx::query("INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,session_id,status,owner,fence,lease_expires_at) VALUES($1,$2,'local',$3,'POST',$4,$5,$6,$7,'started',$8,1,clock_timestamp()+interval '300 seconds')").bind(id).bind(&namespace).bind(id.to_string()).bind(route).bind(json!({"input":input,"admission":{"session_id":session}})).bind(hash).bind(session).bind(owner).execute(&a.db).await?;
    Ok(json!({"id":id,"owner":owner,"fence":1,"namespace":namespace,"route":route}))
}
