//! Deterministic PostgreSQL fixture tools. Errors deliberately become model context.
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
pub async fn execute(db: &PgPool, name: &str, a: Map<String, Value>) -> String {
    match inner(db, name, &a).await {
        Ok(x) => x,
        Err(e) => format!("ERROR: {e}"),
    }
}
async fn inner(db: &PgPool, n: &str, a: &Map<String, Value>) -> Result<String, String> {
    match n{
"inspect_log"=>{let start=num(a,"start_line")as i64;let count=num(a,"line_count")as i64;let rows=sqlx::query("SELECT line_number,line FROM agent_fixture_access_logs WHERE line_number >= $1 ORDER BY line_number LIMIT $2").bind(start).bind(count).fetch_all(db).await.map_err(err)?;Ok(rows.iter().map(|r|format!("{}: {}",r.get::<i32,_>(0),r.get::<String,_>(1))).collect::<Vec<_>>().join("\n"))}
"analyze_500_ips"=>{let robust=boolean(a,"robust_parser");let rows=sqlx::query("SELECT line FROM agent_fixture_access_logs ORDER BY line_number").fetch_all(db).await.map_err(err)?;let mut c:HashMap<String,i64>=HashMap::new();for(i,r)in rows.iter().enumerate(){let line=r.get::<String,_>(0);if robust{let x:Vec<_>=line.split_whitespace().collect();if x.len()>=2&&valid_ip(x[0])&&x.last()==Some(&"500"){*c.entry(x[0].into()).or_default()+=1}}else{let x:Vec<_>=line.split(' ').collect();if x.len()<=7{return Ok(format!("ERROR: naive parser failed at line {}: IndexError: list index out of range. Try robust_parser=true.",i+1))}if x[7]=="500"{*c.entry(x[0].into()).or_default()+=1}}}let mut top:Vec<_>=c.into_iter().collect();top.sort_by(|a,b|b.1.cmp(&a.1));Ok(json!({"top":top.into_iter().take(5).map(|(x,y)|json!([x,y])).collect::<Vec<_>>(),"malformed_records_ignored":if robust{10}else{0}}).to_string())}
"validate_top_offending_ip"=>Ok(json!({"valid":a.get("ip").and_then(Value::as_str)==Some("10.0.0.5")&&num(a,"count")==83.,"message":if a.get("ip").and_then(Value::as_str)==Some("10.0.0.5")&&num(a,"count")==83.{"Validated."}else{"Incorrect; inspect parsing and retry."}}).to_string()),
"inspect_sql_schema"=>Ok(json!({"schema":{"customers":"id/name/city","orders":"order_id/cust_id/amount/ordered_at"},"rules":"normalize city case and ID_ foreign keys"}).to_string()),
"query_chicago_q1_revenue"=>{let keys=boolean(a,"normalize_keys");let city=boolean(a,"normalize_city");if !keys{return Ok(json!({"total":0.0,"normalize_keys":false,"normalize_city":city}).to_string())}let cond=if city{"lower(c.city) = 'chicago'"}else{"c.city = 'Chicago'"};let q=format!("SELECT COALESCE(SUM(o.amount),0)::float8 FROM agent_fixture_orders o JOIN agent_fixture_customers c ON c.id=CAST(REPLACE(o.cust_id,'ID_','') AS INTEGER) WHERE {cond} AND o.ordered_at >= DATE '2026-01-01' AND o.ordered_at < DATE '2026-04-01'");let total: f64=sqlx::query(&q).fetch_one(db).await.map_err(err)?.get(0);Ok(json!({"total":total,"normalize_keys":keys,"normalize_city":city}).to_string())}
"validate_chicago_q1_revenue"=>{let valid=(num(a,"total")-271017.77).abs()<0.005;Ok(json!({"valid":valid,"expected_hint":if valid{"Validated."}else{"Normalize city case and ID_ foreign keys."}}).to_string())}
"inspect_employee_audit_schema"=>{let n:i64=sqlx::query("SELECT count(*) FROM agent_fixture_employees").fetch_one(db).await.map_err(err)?.get(0);Ok(json!({"schema":{"table":"employees","columns":["emp_id INTEGER","name TEXT","age TEXT","salary TEXT","city TEXT"]},"record_count":n,"rules":"age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive."}).to_string())}
"audit_employee_phase_1"=>{let p=a.get("parser").and_then(Value::as_str).unwrap_or("");if p!="robust"{let x=match p{"naive"=>"$80,000","currency_cleaned"=>"NULL","null_cleaned"=>"41; extra_field_corrupted",_=>"unknown parser"};return Ok(format!("ERROR: Phase 1 parser crashed at employee; {x}. Revise only the parser assumption exposed by this failure, then retry incrementally."))}let rows=sqlx::query("SELECT emp_id,age,salary FROM agent_fixture_employees ORDER BY emp_id").fetch_all(db).await.map_err(err)?;let mut age=vec![];let mut salary=vec![];for r in rows{let id:i32=r.get(0);let x:String=r.get(1);let y:String=r.get(2);if x=="NULL"||x.contains(';'){age.push(id)}if y=="UNKNOWN"||y.starts_with('$'){salary.push(id)}}Ok(json!({"phase":"Phase 1","corrupted_age_ids":age,"corrupted_salary_ids":salary,"corrupted_row_ids":[],"status":"logged"}).to_string())}
"calculate_new_york_median_salary"=>{if !boolean(a,"exclude_invalid_salary"){return Ok(verbose("AssertionError: salary array contains UNKNOWN; filter invalid salary fields before median()."))}if !boolean(a,"normalize_city"){return Ok(verbose("AssertionError: Expected median 92500, but your script calculated 71000. Match city case-insensitively."))}let r=sqlx::query("WITH x AS (SELECT salary::numeric v FROM agent_fixture_employees WHERE lower(city)='new york' AND salary <> 'UNKNOWN' AND salary !~ '^\\$') SELECT COUNT(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY v)::float8 FROM x").fetch_one(db).await.map_err(err)?;Ok(json!({"phase":"Phase 2","median_salary":r.get::<f64,_>(1),"valid_new_york_employees":r.get::<i64,_>(0),"status":"calculated"}).to_string())}
"generate_employee_phase_3_report"=>{if !boolean(a,"normalize_city"){return Ok("ERROR: Phase 3 would split New York and new york. Set normalize_city=true.".into())}if !boolean(a,"drop_invalid_age"){return Ok("ERROR: Phase 3 cannot average age while NULL and semicolon-corrupted ages remain. Set drop_invalid_age=true.".into())}let rows=sqlx::query("SELECT initcap(lower(city)),count(*),round(avg(age::integer),2)::float8 FROM agent_fixture_employees WHERE age <> 'NULL' AND age !~ ';' GROUP BY 1 ORDER BY 1").fetch_all(db).await.map_err(err)?;let mut x="Phase 3 — clean city audit\n| City | Valid headcount | Average age |\n| --- | ---: | ---: |\n".to_owned();for r in rows{x.push_str(&format!("| {} | {} | {:.2} |\n",r.get::<String,_>(0),r.get::<i64,_>(1),r.get::<f64,_>(2)))}Ok(x)}
"validate_employee_audit"=>{let valid=boolean(a,"phase_1_logged")&&boolean(a,"normalize_city")&&boolean(a,"drop_invalid_age")&&(num(a,"median_salary")-92500.).abs()<0.005;Ok(json!({"valid":valid,"expected_median_salary":92500.0,"hint":if valid{"Validated all three phases."}else{"Log Phase 1, normalize city casing, and drop invalid ages."}}).to_string())}
_=>Ok(format!("ERROR: unknown tool '{n}'."))}
}
fn num(a: &Map<String, Value>, k: &str) -> f64 {
    a.get(k).and_then(Value::as_f64).unwrap_or(0.)
}
fn valid_ip(x: &str) -> bool {
    x.split('.').count() == 4 && x.split('.').all(|v| v.parse::<u8>().is_ok())
}
fn boolean(a: &Map<String, Value>, k: &str) -> bool {
    a.get(k).and_then(Value::as_bool).unwrap_or(false)
}
fn err(e: sqlx::Error) -> String {
    e.to_string()
}
fn verbose(s: &str) -> String {
    format!(
        "{s}\nMetric test runner context:\n{}Revise the stated normalization rule and retry the metric.",
        "  assertion-frame: candidate=[raw salary values]\n".repeat(32)
    )
}
