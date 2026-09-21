//! Durable workflow state machine shared by JSON and SSE execution paths.
use crate::tools;
use futures_util::future::BoxFuture;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::PgPool;
use std::env;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Message {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Value>,
}
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Transition {
    pub from_phase: String,
    pub to_phase: String,
    pub reason: String,
}
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Workflow {
    pub active_phase: Option<String>,
    #[serde(default)]
    pub completed_phases: Vec<String>,
    #[serde(default)]
    pub observed_evidence: Vec<String>,
    #[serde(default)]
    pub transitions: Vec<Transition>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub goal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_sha256: Option<String>,
    #[serde(default)]
    pub memory: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compacted_context: Option<String>,
    #[serde(default)]
    pub compaction_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<Workflow>,
    #[serde(default)]
    pub step_count: i64,
    #[serde(default)]
    pub run_count: i64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_answer: Option<String>,
}
fn yes() -> bool {
    true
}
fn temp() -> f64 {
    0.1
}
fn timeout() -> f64 {
    90.
}
fn api_key() -> String {
    "OPENAI_API_KEY".into()
}
fn phase_complete() -> String {
    "phase_complete".into()
}
#[derive(Clone, Deserialize)]
pub struct Memory {
    pub max_tokens: i64,
    pub max_steps: i64,
    pub raw_turns_to_keep: usize,
    #[serde(default = "yes")]
    pub hybrid_reflection: bool,
}
#[derive(Clone, Deserialize, Default)]
pub struct Output {
    #[serde(default)]
    pub verbose_setup: bool,
}
#[derive(Clone, Deserialize)]
pub struct Llm {
    pub base_url: String,
    #[serde(default = "api_key")]
    pub api_key_env: String,
    #[serde(default = "temp")]
    pub temperature: f64,
    #[serde(default = "timeout")]
    pub timeout_seconds: f64,
}
#[derive(Clone, Deserialize)]
pub struct Evidence {
    pub description: String,
    pub tool: String,
    #[serde(default = "yes")]
    pub required: bool,
    #[serde(default)]
    pub arguments: Value,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub content_contains: Vec<String>,
}
#[derive(Clone, Deserialize)]
pub struct Edge {
    pub to: String,
    #[serde(default = "phase_complete")]
    pub when: String,
    #[serde(default)]
    pub evidence: Vec<Evidence>,
}
#[derive(Clone, Deserialize)]
pub struct Phase {
    pub id: String,
    pub name: String,
    pub instruction: String,
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    pub completion: Vec<Evidence>,
    #[serde(default)]
    pub transitions: Vec<Edge>,
}
#[derive(Clone, Deserialize)]
pub struct WorkflowConfig {
    pub entry_phase: String,
    pub phases: Vec<Phase>,
}
#[derive(Clone, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Clone, Deserialize)]
pub struct Config {
    pub name: String,
    pub system_prompt: String,
    pub model: String,
    pub llm: Llm,
    pub memory: Memory,
    #[serde(default)]
    pub output: Output,
    pub tools: Vec<Tool>,
    #[serde(default)]
    pub workflow: Option<WorkflowConfig>,
}
pub struct Engine {
    pub config: Config,
    http: reqwest::Client,
}
impl Engine {
    pub fn new(config: Config) -> Result<Self, String> {
        let h = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs_f64(
                config.llm.timeout_seconds,
            ))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { config, http: h })
    }
    pub async fn run<F>(
        &self,
        s: &mut Session,
        max: Option<i64>,
        db: &PgPool,
        emit: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(Value) -> BoxFuture<'static, Result<(), String>>,
    {
        let cap = max.unwrap_or(self.config.memory.max_steps);
        let limit = s.step_count + cap;
        s.run_count += 1;
        emit(json!({"type":"status","message":"Agent run started.","run_count":s.run_count}))
            .await?;
        while s.status == "active" && s.step_count < limit {
            if let Some(t) = self.advance(s) {
                emit(json!({"type":"interim_result","step":"phase_completed","phase_id":t.from_phase,"next_phase":t.to_phase,"reason":t.reason})).await?
            }
            if self.compact(s).await? {
                emit(json!({"type":"interim_result","step":"compaction","data":{"compaction_count":s.compaction_events.len()}})).await?
            }
            emit(json!({"type":"status","message":"Agent deciding next action.","step_count":s.step_count})).await?;
            let d = self.decide(self.payload(s)).await?;
            s.step_count += 1;
            if d["type"] == "final" {
                let x = d["content"]
                    .as_str()
                    .unwrap_or("No final answer was returned.")
                    .to_owned();
                s.memory.push(msg("assistant", Some(x.clone())));
                if self.complete(s) {
                    s.status = "complete".into();
                    s.final_answer = Some(x.clone());
                    emit(json!({"type":"final_result","answer":x})).await?
                }
                continue;
            }
            let id = d["id"].as_str().unwrap_or("call_1").to_owned();
            let name = d["name"].as_str().unwrap_or("").to_owned();
            let args = d["arguments"].as_object().cloned().unwrap_or_default();
            s.memory.push(Message{role:"assistant".into(),content:None,name:None,tool_call_id:None,tool_calls:Some(json!([{"id":id.clone(),"type":"function","function":{"name":name.clone(),"arguments":Value::Object(args.clone()).to_string()}}]))});
            let out = if self.allowed(s, &name) {
                tools::execute(db, &name, args.clone()).await
            } else {
                format!(
                    "ERROR: Workflow phase '{}' cannot execute tool '{}'. Complete the active phase first. Allowed tools: {}.",
                    s.workflow
                        .as_ref()
                        .and_then(|w| w.active_phase.clone())
                        .unwrap_or_default(),
                    name,
                    self.allowed_names(s).join(", ")
                )
            };
            s.memory.push(Message {
                role: "tool".into(),
                content: Some(out.clone()),
                name: Some(name.clone()),
                tool_call_id: Some(id),
                tool_calls: None,
            });
            self.record(s, &name, args, &out);
            emit(json!({"type":"interim_result","step":"tool_completed","data":{"tool":name,"output":out}})).await?
        }
        if s.status == "active" && s.step_count >= limit {
            let x = format!(
                "Step limit ({cap}) reached without a final answer. Resume with another run request."
            );
            s.status = "failed".into();
            s.final_answer = Some(x.clone());
            emit(json!({"type":"error","message":x,"recoverable":true})).await?
        }
        Ok(())
    }
    fn payload(&self, s: &Session) -> Vec<Message> {
        let mut p = vec![
            msg("system", Some(self.config.system_prompt.clone())),
            msg(
                "user",
                Some(format!("Original user goal (immutable): {}", s.goal)),
            ),
        ];
        if let Some(x) = &s.compacted_context {
            p.push(msg(
                "system",
                Some(format!("[COMPACTED CONTEXT STATE]\n{x}")),
            ))
        }
        if let Some(x) = self.instruction(s) {
            p.push(msg("system", Some(x)))
        }
        if s.memory.len() > 1 {
            p.push(msg(
                "system",
                Some("[RAW WORKING BUFFER — PRESERVE VERBATIM]".into()),
            ))
        }
        p.extend(s.memory.iter().skip(1).cloned());
        p
    }
    fn phase(&self, id: &str) -> Option<&Phase> {
        self.config
            .workflow
            .as_ref()?
            .phases
            .iter()
            .find(|p| p.id == id)
    }
    fn complete(&self, s: &Session) -> bool {
        self.config.workflow.is_none()
            || s.workflow
                .as_ref()
                .is_some_and(|w| w.active_phase.is_none())
    }
    fn instruction(&self, s: &Session) -> Option<String> {
        let id = s.workflow.as_ref()?.active_phase.as_ref()?;
        let p = self.phase(id)?;
        Some(format!(
            "[WORKFLOW STATE]\nActive phase: {} ({}).\nPhase instruction: {}\nDo not provide prose progress updates or a final answer while this phase is incomplete; make a tool call instead.\nOutstanding required evidence:\n{}",
            p.name,
            p.id,
            p.instruction,
            self.missing(s, p)
                .iter()
                .map(|x| format!("- {x}"))
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
    fn allowed_names(&self, s: &Session) -> Vec<String> {
        let Some(id) = s.workflow.as_ref().and_then(|w| w.active_phase.as_ref()) else {
            return vec![];
        };
        let Some(p) = self.phase(id) else {
            return vec![];
        };
        p.allowed_tools
            .clone()
            .unwrap_or_else(|| p.completion.iter().map(|e| e.tool.clone()).collect())
    }
    fn allowed(&self, s: &Session, name: &str) -> bool {
        let a = self.allowed_names(s);
        a.is_empty() || a.iter().any(|x| x == name)
    }
    fn missing(&self, s: &Session, p: &Phase) -> Vec<String> {
        p.completion
            .iter()
            .enumerate()
            .filter(|(i, e)| {
                e.required
                    && !s.workflow.as_ref().is_some_and(|w| {
                        w.observed_evidence.contains(&key(&p.id, "completion", *i))
                    })
            })
            .map(|(_, e)| e.description.clone())
            .collect()
    }
    fn advance(&self, s: &mut Session) -> Option<Transition> {
        let id = s.workflow.as_ref()?.active_phase.clone()?;
        let p = self.phase(&id)?;
        let e = p
            .transitions
            .iter()
            .enumerate()
            .find(|(n, e)| match e.when.as_str() {
                "always" => true,
                "phase_complete" => self.missing(s, p).is_empty(),
                _ => e.evidence.iter().enumerate().all(|(i, _)| {
                    s.workflow
                        .as_ref()
                        .unwrap()
                        .observed_evidence
                        .contains(&key(&p.id, &format!("transition-{n}"), i))
                }),
            })?
            .1;
        let t = Transition {
            from_phase: p.id.clone(),
            to_phase: e.to.clone(),
            reason: e.when.clone(),
        };
        let w = s.workflow.as_mut()?;
        if !w.completed_phases.contains(&p.id) {
            w.completed_phases.push(p.id.clone())
        }
        w.active_phase = (e.to != "complete").then(|| e.to.clone());
        w.transitions.push(t.clone());
        Some(t)
    }
    fn record(&self, s: &mut Session, name: &str, args: Map<String, Value>, out: &str) {
        let Some(id) = s.workflow.as_ref().and_then(|w| w.active_phase.clone()) else {
            return;
        };
        let Some(p) = self.phase(&id) else { return };
        let mut v: Vec<(String, usize, &Evidence)> = p
            .completion
            .iter()
            .enumerate()
            .map(|(i, e)| ("completion".into(), i, e))
            .collect();
        for (n, t) in p.transitions.iter().enumerate() {
            v.extend(
                t.evidence
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (format!("transition-{n}"), i, e)),
            )
        }
        let w = s.workflow.as_mut().unwrap();
        for (c, i, e) in v {
            let k = key(&p.id, &c, i);
            if !w.observed_evidence.contains(&k) && matches(e, name, &args, out) {
                w.observed_evidence.push(k)
            }
        }
    }
    async fn decide(&self, m: Vec<Message>) -> Result<Value, String> {
        let base = self.config.llm.base_url.trim_end_matches('/');
        let mut urls = vec![if base.ends_with("/chat/completions") {
            base.into()
        } else {
            format!("{base}/chat/completions")
        }];
        if base.ends_with("/v1") {
            urls.push(format!("{}/chat/completions", &base[..base.len() - 3]))
        }
        let b = json!({"model":self.config.model,"messages":m,"tools":self.config.tools.iter().map(|t|json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect::<Vec<_>>(),"tool_choice":"auto","temperature":self.config.llm.temperature});
        for (i, u) in urls.iter().enumerate() {
            let r = self
                .http
                .post(u)
                .bearer_auth(
                    env::var(&self.config.llm.api_key_env)
                        .unwrap_or_else(|_| "local-not-required".into()),
                )
                .json(&b)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if r.status() == StatusCode::NOT_FOUND && i + 1 < urls.len() {
                continue;
            }
            let v = r
                .error_for_status()
                .map_err(|e| e.to_string())?
                .json::<Value>()
                .await
                .map_err(|e| e.to_string())?;
            let x = &v["choices"][0]["message"];
            if let Some(c) = x["tool_calls"].as_array().and_then(|a| a.first()) {
                return Ok(
                    match serde_json::from_str::<Value>(
                        c["function"]["arguments"].as_str().unwrap_or("{}"),
                    ) {
                        Ok(a) => {
                            json!({"type":"tool","id":c["id"].as_str().unwrap_or("call_1"),"name":c["function"]["name"],"arguments":a})
                        }
                        Err(e) => {
                            json!({"type":"final","content":format!("LLM emitted invalid tool arguments: {e}")})
                        }
                    },
                );
            }
            return Ok(
                json!({"type":"final","content":x["content"].as_str().unwrap_or("No final answer was returned.")}),
            );
        }
        Err("model completion route unavailable".into())
    }
    async fn compact(&self, s: &mut Session) -> Result<bool, String> {
        let n: i64 = s
            .memory
            .iter()
            .map(|m| (m.content.as_deref().unwrap_or("").len() / 4 + 1) as i64)
            .sum();
        let k = self.config.memory.raw_turns_to_keep;
        if n < self.config.memory.max_tokens || s.memory.len() <= k + 1 {
            return Ok(false);
        }
        let cut = s.memory.len() - k;
        let old = s.compacted_context.clone();
        let h = s.memory[1..cut].to_vec();
        let x = self
            .compact_client(
                &s.goal,
                &h,
                old.as_deref(),
                self.config.memory.hybrid_reflection,
            )
            .await;
        s.compacted_context = Some(x.clone());
        s.compaction_events.push(x);
        let goal = s.goal.clone();
        s.memory = s.memory[cut..].to_vec();
        s.memory.insert(0, msg("user", Some(goal)));
        Ok(true)
    }
    async fn compact_client(
        &self,
        goal: &str,
        h: &[Message],
        _old: Option<&str>,
        hybrid: bool,
    ) -> String {
        let fallback = format!(
            "<COMPACTED_STATE>\n<USER_GOAL>\n{goal}\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- No verified global lessons extracted.\n</GLOBAL_LESSON_LEDGER>\n<DEAD_ENDS>\n- No verified dead ends extracted; inspect the retained raw working buffer.\n</DEAD_ENDS>\n<CURRENT_LOCAL_PIVOT>\nReview the retained raw working buffer and continue from the latest verified state.\n</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>"
        );
        let source = h
            .iter()
            .map(|m| format!("{}: {}", m.role, m.content.clone().unwrap_or_default()))
            .collect::<Vec<_>>()
            .join("\n");
        let url = format!(
            "{}/chat/completions",
            self.config.llm.base_url.trim_end_matches('/')
        );
        let instruction = if hybrid {
            "Return a <COMPACTED_STATE> with verified facts only."
        } else {
            "Create a compacted context state with verified facts only."
        };
        match self.http.post(url).json(&json!({"model":self.config.model,"temperature":0,"messages":[{"role":"system","content":instruction},{"role":"user","content":format!("Goal: {goal}\nHistorical prefix:\n{source}")}]})).send().await{Ok(r)=>r.json::<Value>().await.ok().and_then(|v|v["choices"][0]["message"]["content"].as_str().map(str::to_owned)).unwrap_or(fallback),Err(_)=>fallback}
    }
}
fn msg(role: &str, content: Option<String>) -> Message {
    Message {
        role: role.into(),
        content,
        name: None,
        tool_call_id: None,
        tool_calls: None,
    }
}
fn key(p: &str, c: &str, i: usize) -> String {
    format!("{p}:{c}:{i}")
}
fn matches(e: &Evidence, n: &str, a: &Map<String, Value>, o: &str) -> bool {
    e.tool == n
        && contains(&Value::Object(a.clone()), &e.arguments)
        && e.content_contains
            .iter()
            .all(|x| o.to_lowercase().contains(&x.to_lowercase()))
        && (e.result.is_null()
            || e.result.as_object().is_some_and(|x| x.is_empty())
            || serde_json::from_str::<Value>(o).is_ok_and(|x| contains(&x, &e.result)))
}
fn contains(a: &Value, b: &Value) -> bool {
    match b {
        Value::Object(m) => a.as_object().is_some_and(|x| {
            m.iter()
                .all(|(k, v)| x.get(k).is_some_and(|q| contains(q, v)))
        }),
        Value::Array(v) => a
            .as_array()
            .is_some_and(|x| v.iter().all(|q| x.iter().any(|z| contains(z, q)))),
        _ => a == b,
    }
}
