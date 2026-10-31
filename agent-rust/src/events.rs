use super::*;
use axum::response::Response;
pub async fn decorate(a: &App, s: &Session, mut event: Value) -> Value {
    if let Some(c) = &s.command {
        event["command_id"] = c["id"].clone();
    }
    event["run_id"] = json!(s.run_id);
    if let Ok(Some(id))=sqlx::query_scalar::<_,i64>("SELECT event_id FROM agent_session_events WHERE session_id=$1::uuid AND revision=$2 ORDER BY event_id DESC LIMIT 1").bind(&s.id).bind(s.revision).fetch_optional(&a.db).await {event["event_id"]=json!(id);}
    event
}
pub async fn stream(
    State(a): State<App>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Ok(sid) = Uuid::parse_str(&id) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":"Session ID must be UUID"})),
        )
            .into_response();
    };
    let Some(run) = q.get("run_id").and_then(|r| Uuid::parse_str(r).ok()) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail":"run_id must be UUID"})),
        )
            .into_response();
    };
    let mut after = match q
        .get("after_event_id")
        .map(|v| v.parse::<i64>())
        .transpose()
    {
        Ok(v) if v.unwrap_or(0) >= 0 => v.unwrap_or(0),
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail":"Invalid event cursor"})),
            )
                .into_response();
        }
    };
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM agent_runs WHERE session_id=$1 AND run_id=$2)",
    )
    .bind(sid)
    .bind(run)
    .fetch_one(&a.db)
    .await
    .unwrap_or(false);
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":"Run not found"})),
        )
            .into_response();
    }
    let (sender, output, _) = sse::event_channel();
    tokio::spawn(async move {
        loop {
            let rows=match sqlx::query_as::<_,(i64,Value)>("SELECT event_id,event FROM agent_session_events WHERE session_id=$1 AND event->>'run_id'=$2 AND event_id>$3 ORDER BY event_id LIMIT 1000").bind(sid).bind(run.to_string()).bind(after).fetch_all(&a.db).await{Ok(r)=>r,Err(_)=>break};
            let full = rows.len() == 1000;
            for (id, mut event) in rows {
                after = id;
                event["event_id"] = json!(id);
                if sender.send(event).await.is_err() {
                    return;
                }
            }
            if full {
                continue;
            }
            if sqlx::query_scalar::<_, String>(
                "SELECT status FROM agent_runs WHERE session_id=$1 AND run_id=$2",
            )
            .bind(sid)
            .bind(run)
            .fetch_optional(&a.db)
            .await
            .ok()
            .flatten()
            .as_deref()
                != Some("running")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });
    Sse::new(output).into_response()
}
