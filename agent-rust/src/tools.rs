//! Deterministic PostgreSQL fixture tools. Tool failures become model context.
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::fmt;

pub fn is_supported(name: &str) -> bool {
    matches!(
        name,
        "fixture_idempotent_write"
            | "inspect_log"
            | "analyze_500_ips"
            | "validate_top_offending_ip"
            | "inspect_sql_schema"
            | "query_chicago_q1_revenue"
            | "validate_chicago_q1_revenue"
            | "inspect_employee_audit_schema"
            | "audit_employee_phase_1"
            | "calculate_new_york_median_salary"
            | "generate_employee_phase_3_report"
            | "validate_employee_audit"
    )
}

pub fn validate_arguments(schema: &Value, args: &Value) -> Result<(), String> {
    validate_arguments_at(schema, args, "$")
}

pub fn validate_arguments_at(schema: &Value, args: &Value, path: &str) -> Result<(), String> {
    validate_value(schema, args, path)
}

pub fn parse_strict_json(text: &str) -> Result<Value, String> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = StrictValue::deserialize(&mut deserializer)
        .map_err(|e| e.to_string())?
        .0;
    deserializer.end().map_err(|e| e.to_string())?;
    Ok(value)
}
struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(StrictVisitor)
    }
}
struct StrictVisitor;
impl<'de> Visitor<'de> for StrictVisitor {
    type Value = StrictValue;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("strict JSON value")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(v)))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(StrictValue(json!(v)))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        if v > i64::MAX as u64 {
            return Err(E::custom("JSON integer exceeds signed 64-bit range"));
        }
        Ok(StrictValue(json!(v)))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        if !v.is_finite() {
            return Err(E::custom("nonfinite JSON number"));
        }
        Ok(StrictValue(json!(v)))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(v.into())))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(v)))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.visit_unit()
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element::<StrictValue>()? {
            out.push(v.0);
        }
        Ok(StrictValue(Value::Array(out)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut out = Map::new();
        while let Some(k) = map.next_key::<String>()? {
            if out.contains_key(&k) {
                return Err(de::Error::custom(format!("duplicate JSON property: {k}")));
            }
            let v = map.next_value::<StrictValue>()?;
            out.insert(k, v.0);
        }
        Ok(StrictValue(Value::Object(out)))
    }
}
fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let kind = schema.get("type").and_then(Value::as_str).unwrap_or("");
    let valid = match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        "number" => value.as_f64().is_some_and(f64::is_finite),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value
                    .as_f64()
                    .is_some_and(|n| n.is_finite() && n.fract() == 0.0)
        }
        _ => true,
    };
    if !valid {
        return Err(format!("{path} must be {kind}"));
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            return Err(format!("{path} is not an allowed value"));
        }
    }
    if schema.get("const").is_some_and(|x| x != value) {
        return Err(format!("{path} must equal the configured constant"));
    }
    if let Some(obj) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !obj.contains_key(key) {
                    return Err(format!("{path}.{key} is required"));
                }
            }
        }
        let props = schema.get("properties").and_then(Value::as_object);
        for (key, item) in obj {
            if let Some(sub) = props.and_then(|p| p.get(key)) {
                validate_value(sub, item, &format!("{path}.{key}"))?;
            } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!("{path}.{key} is not allowed"));
            } else if let Some(additional_schema) =
                schema.get("additionalProperties").filter(|v| v.is_object())
            {
                validate_value(additional_schema, item, &format!("{path}.{key}"))?;
            }
        }
    }
    if let Some(arr) = value.as_array() {
        if let Some(item_schema) = schema.get("items") {
            for (i, item) in arr.iter().enumerate() {
                validate_value(item_schema, item, &format!("{path}[{i}]"))?;
            }
        }
    }
    if let Some(s) = value.as_str() {
        if schema
            .get("minLength")
            .and_then(Value::as_u64)
            .is_some_and(|n| s.chars().count() < n as usize)
        {
            return Err(format!("{path} is shorter than minLength"));
        }
        if schema
            .get("maxLength")
            .and_then(Value::as_u64)
            .is_some_and(|n| s.chars().count() > n as usize)
        {
            return Err(format!("{path} exceeds maxLength"));
        }
        if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
            if !regex::Regex::new(pattern)
                .map_err(|e| format!("invalid configured pattern: {e}"))?
                .is_match(s)
            {
                return Err(format!("{path} does not match the configured pattern"));
            }
        }
    }
    if let Some(n) = value.as_f64() {
        for key in ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"] {
            if let Some(bound) = schema.get(key).and_then(Value::as_f64) {
                let failed = match key {
                    "minimum" => n < bound,
                    "maximum" => n > bound,
                    "exclusiveMinimum" => n <= bound,
                    _ => n >= bound,
                };
                if failed {
                    return Err(format!("{path} violates {key}"));
                }
            }
        }
    }
    if let Some(arr) = value.as_array() {
        if schema
            .get("minItems")
            .and_then(Value::as_u64)
            .is_some_and(|n| arr.len() < (n as usize))
        {
            return Err(format!("{path} has fewer than minItems"));
        }
        if schema
            .get("maxItems")
            .and_then(Value::as_u64)
            .is_some_and(|n| arr.len() > (n as usize))
        {
            return Err(format!("{path} exceeds maxItems"));
        }
    }
    Ok(())
}

pub async fn execute(db: &PgPool, name: &str, args: Map<String, Value>) -> String {
    match inner(db, name, &args).await {
        Ok(value) => value,
        Err(error) => format!("ERROR: {error}"),
    }
}

async fn inner(db: &PgPool, name: &str, a: &Map<String, Value>) -> Result<String, String> {
    match name {
        "inspect_log" => {
            let start_line = number(a, "start_line") as i64;
            let start = start_line - 1;
            let count = number(a, "line_count") as i64;
            let rows = sqlx::query(
                "SELECT line_number,line FROM agent_fixture_access_logs ORDER BY line_number",
            )
            .fetch_all(db)
            .await
            .map_err(error)?;
            let len = rows.len() as i64;
            let begin = normalize_slice(start, len);
            let end = normalize_slice(start.saturating_add(count), len);
            Ok(rows
                .iter()
                .skip(begin)
                .take(end.saturating_sub(begin))
                .enumerate()
                .map(|(i, row)| format!("{}: {}", start_line + i as i64, row.get::<String, _>(1)))
                .collect::<Vec<_>>()
                .join("\n"))
        }
        "analyze_500_ips" => analyze_ips(db, boolean(a, "robust_parser")).await,
        "validate_top_offending_ip" => {
            let expected = top_ip(db).await?;
            let valid = a.get("ip").and_then(Value::as_str) == Some(expected.0.as_str())
                && number(a, "count") as i64 == expected.1;
            Ok(python_json(
                &json!({"valid":valid,"message":if valid {"Validated."} else {"Incorrect; inspect parsing and retry."}}),
            ))
        }
        "inspect_sql_schema" => inspect_sql_schema(db).await,
        "query_chicago_q1_revenue" => {
            query_revenue(
                db,
                boolean(a, "normalize_keys"),
                boolean(a, "normalize_city"),
            )
            .await
        }
        "validate_chicago_q1_revenue" => {
            let expected: Value = serde_json::from_str(&query_revenue(db, true, true).await?)
                .map_err(|e| e.to_string())?;
            let valid =
                (number(a, "total") - expected["total"].as_f64().unwrap_or(0.)).abs() < 0.005;
            Ok(python_json(
                &json!({"valid":valid,"expected_hint":if valid {"Validated."} else {"Normalize city case and ID_ foreign keys."}}),
            ))
        }
        "inspect_employee_audit_schema" => inspect_employee_schema(db).await,
        "audit_employee_phase_1" => audit_employee(db, a).await,
        "calculate_new_york_median_salary" => employee_median(db, a).await,
        "generate_employee_phase_3_report" => employee_report(db, a).await,
        "validate_employee_audit" => validate_employee_audit(db, a).await,
        _ => Ok(format!("ERROR: unknown tool '{name}'.")),
    }
}

async fn analyze_ips(db: &PgPool, robust: bool) -> Result<String, String> {
    let rows = sqlx::query("SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
        .fetch_all(db)
        .await
        .map_err(error)?;
    let mut counts: HashMap<String, (i64, usize)> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        let line: String = row.get(0);
        if robust {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() >= 2 && ip_regex_match(fields[0]) && fields.last() == Some(&"500") {
                let entry = counts.entry(fields[0].to_owned()).or_insert((0, index));
                entry.0 += 1;
            }
        } else {
            let fields: Vec<_> = line.split(' ').collect();
            if fields.len() <= 7 {
                return Ok(format!(
                    "ERROR: naive parser failed at line {}: IndexError: list index out of range. Try robust_parser=true.",
                    index + 1
                ));
            }
            if fields[7] == "500" {
                let entry = counts.entry(fields[0].to_owned()).or_insert((0, index));
                entry.0 += 1;
            }
        }
    }
    let mut top: Vec<_> = counts.into_iter().collect();
    // Python Counter preserves first-seen order for ties; query in line order
    // and retain that ordinal as the stable tie-breaker.
    top.sort_by(|a, b| b.1.0.cmp(&a.1.0).then_with(|| a.1.1.cmp(&b.1.1)));
    Ok(python_json(
        &json!({"top":top.into_iter().take(5).map(|(ip,(count,_))| json!([ip,count])).collect::<Vec<_>>(),"malformed_records_ignored":if robust {10} else {0}}),
    ))
}

async fn top_ip(db: &PgPool) -> Result<(String, i64), String> {
    let rows = sqlx::query("SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
        .fetch_all(db)
        .await
        .map_err(error)?;
    let mut counts: HashMap<String, (i64, usize)> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let line: String = row.get(0);
        let f: Vec<_> = line.split_whitespace().collect();
        if f.len() >= 2 && ip_regex_match(f[0]) && f.last() == Some(&"500") {
            let entry = counts.entry(f[0].to_owned()).or_insert((0, i));
            entry.0 += 1;
        }
    }
    counts
        .into_iter()
        .max_by(|a, b| a.1.0.cmp(&b.1.0).then_with(|| b.1.1.cmp(&a.1.1)))
        .map(|(ip, (count, _))| (ip, count))
        .ok_or_else(|| "no matching access log records".into())
}

// Mirrors Python's anchored IP expression, including its 1-to-3 digit octets.
fn ip_regex_match(s: &str) -> bool {
    let mut p = s.split('.');
    let parts: Vec<_> = p.by_ref().collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|x| !x.is_empty() && x.len() <= 3 && x.bytes().all(|b| b.is_ascii_digit()))
}

async fn inspect_sql_schema(db: &PgPool) -> Result<String, String> {
    let mut schemas = serde_json::Map::new();
    for short in ["customers", "orders"] {
        let table = format!("agent_fixture_{short}");
        let rows = sqlx::query("SELECT column_name,data_type FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position")
            .bind(&table).fetch_all(db).await.map_err(error)?;
        schemas.insert(
            short.to_owned(),
            Value::Array(
                rows.iter()
                    .map(|r| json!([r.get::<String, _>(0), r.get::<String, _>(1)]))
                    .collect(),
            ),
        );
    }
    let chicago = sqlx::query(
        "SELECT id,name,city FROM agent_fixture_customers WHERE lower(city)='chicago' LIMIT 6",
    )
    .fetch_all(db)
    .await
    .map_err(error)?;
    let bad_keys = sqlx::query("SELECT order_id,cust_id,amount::text,ordered_at::text FROM agent_fixture_orders WHERE cust_id LIKE 'ID_%%' LIMIT 6").fetch_all(db).await.map_err(error)?;
    let chicago: Vec<Value> = chicago
        .iter()
        .map(|r| {
            json!([
                r.get::<i32, _>(0),
                r.get::<String, _>(1),
                r.get::<String, _>(2)
            ])
        })
        .collect();
    let bad_keys: Vec<Value> = bad_keys
        .iter()
        .map(|r| {
            json!([
                r.get::<i32, _>(0),
                r.get::<String, _>(1),
                r.get::<String, _>(2),
                r.get::<String, _>(3)
            ])
        })
        .collect();
    Ok(python_json(
        &json!({"schema":schemas,"chicago_examples":chicago,"string_key_examples":bad_keys}),
    ))
}

async fn query_revenue(db: &PgPool, keys: bool, city: bool) -> Result<String, String> {
    let join = if keys {
        "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)"
    } else {
        "o.cust_id::INTEGER"
    };
    let city_filter = if city {
        "lower(c.city) = 'chicago'"
    } else {
        "c.city = 'Chicago'"
    };
    let sql = format!(
        "SELECT ROUND(COALESCE(SUM(o.amount),0),2)::float8 FROM agent_fixture_orders o JOIN agent_fixture_customers c ON c.id={join} WHERE {city_filter} AND o.ordered_at >= DATE '2026-01-01' AND o.ordered_at < DATE '2026-04-01'"
    );
    let total: f64 = sqlx::query(&sql).fetch_one(db).await.map_err(error)?.get(0);
    Ok(python_json(
        &json!({"total":total,"normalize_keys":keys,"normalize_city":city}),
    ))
}

async fn inspect_employee_schema(db: &PgPool) -> Result<String, String> {
    let rows = employee_rows(db).await?;
    let examples = [8usize, 9, 14, 21, 27]
        .into_iter()
        .filter_map(|i| rows.get(i).cloned())
        .collect::<Vec<_>>();
    Ok(python_json(
        &json!({"schema":{"table":"employees","columns":["emp_id INTEGER","name TEXT","age TEXT","salary TEXT","city TEXT"]},"record_count":rows.len(),"examples":examples,"rules":"age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive."}),
    ))
}

type Employee = (i32, String, String, String, String);
async fn employee_rows(db: &PgPool) -> Result<Vec<Employee>, String> {
    let rows = sqlx::query(
        "SELECT emp_id,name,age,salary,city FROM agent_fixture_employees ORDER BY emp_id",
    )
    .fetch_all(db)
    .await
    .map_err(error)?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect())
}
fn employee_failure(row: usize, field: &str, value: &str) -> String {
    let window = (row.saturating_sub(8).max(1)..row + 8)
        .map(|n| format!("  {n:>4} | parsed_{field}[{n}] = {field}_parser(record[{n:?}])"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "ERROR: Phase 1 parser crashed at employee {row}; {field}='{value}'.\nTraceback (most recent call last):\n  File \"/simulated/audit.py\", line 84, in parse_employee_batch\n    normalized_{field} = {field}_parser(raw_{field})\n  File \"/simulated/audit.py\", line 39, in {field}_parser\n    raise ValueError('unsupported {field} representation')\nValueError: unsupported {field} representation '{value}'\nParser window (not a successful audit):\n{window}\nRevise only the parser assumption exposed by this failure, then retry incrementally."
    )
}
async fn audit_employee(db: &PgPool, a: &Map<String, Value>) -> Result<String, String> {
    let Some(parser) = a.get("parser") else {
        return Ok("ERROR: KeyError: 'parser'".into());
    };
    let Some(parser) = parser.as_str() else {
        return Ok(
            "ERROR: parser must be naive, currency_cleaned, null_cleaned, or robust.".into(),
        );
    };
    let failure = match parser {
        "naive" => Some((10, "salary", "$80,000")),
        "currency_cleaned" => Some((15, "age", "NULL")),
        "null_cleaned" => Some((22, "age", "41; extra_field_corrupted")),
        _ => None,
    };
    if let Some((row, field, value)) = failure {
        return Ok(employee_failure(row, field, value));
    }
    if parser != "robust" {
        return Ok(
            "ERROR: parser must be naive, currency_cleaned, null_cleaned, or robust.".into(),
        );
    }
    let rows = employee_rows(db).await?;
    let ages: Vec<i32> = rows
        .iter()
        .filter(|r| r.2 == "NULL" || r.2.contains(';'))
        .map(|r| r.0)
        .collect();
    let salaries: Vec<i32> = rows
        .iter()
        .filter(|r| r.3 == "UNKNOWN" || r.3.starts_with('$'))
        .map(|r| r.0)
        .collect();
    let mut all = ages.clone();
    all.extend(salaries.iter().copied());
    all.sort_unstable();
    all.dedup();
    Ok(python_json(
        &json!({"phase":"Phase 1","corrupted_age_ids":ages,"corrupted_salary_ids":salaries,"corrupted_row_ids":all,"status":"logged"}),
    ))
}
fn metric_failure(message: &str) -> String {
    let frames=(1..=32).map(|i|format!("  assertion-frame {i:02}: candidate=[raw salary values]; city predicate and invalid-value filter under test")).collect::<Vec<_>>().join("\n");
    format!(
        "{message}\nMetric test runner context:\n{frames}\nRevise the stated normalization rule and retry the metric."
    )
}
async fn employee_median(db: &PgPool, a: &Map<String, Value>) -> Result<String, String> {
    if !boolean(a, "exclude_invalid_salary") {
        return Ok(metric_failure(
            "AssertionError: salary array contains UNKNOWN; filter invalid salary fields before median().",
        ));
    }
    if !boolean(a, "normalize_city") {
        return Ok(metric_failure(
            "AssertionError: Expected median 92500, but your script calculated 71000. Match city case-insensitively.",
        ));
    }
    let rows = employee_rows(db).await?;
    let mut values: Vec<f64> = rows
        .iter()
        .filter(|r| r.4.to_lowercase() == "new york" && r.3 != "UNKNOWN" && !r.3.starts_with('$'))
        .filter_map(|r| r.3.parse().ok())
        .collect();
    values.sort_by(f64::total_cmp);
    let median = if values.len().is_multiple_of(2) {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.
    } else {
        values[values.len() / 2]
    };
    Ok(python_json(
        &json!({"phase":"Phase 2","median_salary":median,"valid_new_york_employees":values.len(),"status":"calculated"}),
    ))
}
async fn validate_employee_audit(db: &PgPool, a: &Map<String, Value>) -> Result<String, String> {
    let mut args = Map::new();
    args.insert("normalize_city".into(), Value::Bool(true));
    args.insert("exclude_invalid_salary".into(), Value::Bool(true));
    let expected: Value =
        serde_json::from_str(&employee_median(db, &args).await?).map_err(|e| e.to_string())?;
    let median = expected["median_salary"].as_f64().unwrap_or(0.);
    let valid = boolean(a, "phase_1_logged")
        && boolean(a, "normalize_city")
        && boolean(a, "drop_invalid_age")
        && (number(a, "median_salary") - median).abs() < 0.005;
    Ok(python_json(
        &json!({"valid":valid,"expected_median_salary":median,"hint":if valid {"Validated all three phases."} else {"Log Phase 1, normalize city casing, and drop invalid ages."}}),
    ))
}
async fn employee_report(db: &PgPool, a: &Map<String, Value>) -> Result<String, String> {
    if !boolean(a, "normalize_city") {
        return Ok(
            "ERROR: Phase 3 would split New York and new york. Set normalize_city=true.".into(),
        );
    }
    if !boolean(a, "drop_invalid_age") {
        return Ok("ERROR: Phase 3 cannot average age while NULL and semicolon-corrupted ages remain. Set drop_invalid_age=true.".into());
    }
    let rows = employee_rows(db).await?;
    let mut groups: HashMap<String, Vec<i32>> = HashMap::new();
    for r in rows {
        if r.2 == "NULL" || r.2.contains(';') {
            continue;
        }
        if let Ok(age) = r.2.parse() {
            groups.entry(title_case(&r.4)).or_default().push(age)
        }
    }
    let mut cities: Vec<_> = groups.into_iter().collect();
    cities.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out="Phase 3 — clean city audit\n| City | Valid headcount | Average age |\n| --- | ---: | ---: |\n".to_owned();
    for (city, ages) in cities {
        let mean = ages.iter().sum::<i32>() as f64 / ages.len() as f64;
        out.push_str(&format!("| {city} | {} | {mean:.2} |\n", ages.len()));
    }
    out.push_str("\nPhase 3 normalization trace (diagnostic evidence):\n");
    for i in 1..=48 {
        if i > 1 {
            out.push('\n');
        }
        out.push_str(&format!("normalization-check {i:02}: city=lowercase, age=NULL-or-semicolon excluded, salary=UNKNOWN-or-currency excluded, aggregate=verified"));
    }
    Ok(out)
}
fn title_case(input: &str) -> String {
    let mut word_start = true;
    let mut output = String::new();
    for ch in input.chars() {
        if ch.is_alphanumeric() {
            let converted = if word_start {
                ch.to_uppercase().to_string()
            } else {
                ch.to_lowercase().to_string()
            };
            output.push_str(&converted);
            word_start = false;
        } else {
            output.push(ch);
            word_start = true;
        }
    }
    output
}
pub fn python_json(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(x) => x.to_string(),
        Value::Number(x) => crate::identity::python_number(x),
        Value::String(x) => python_json_string(x),
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(python_json).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(|(key, value)| format!("{}: {}", python_json_string(key), python_json(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn python_json_string(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if c as u32 > 0xffff => {
                let code = c as u32 - 0x10000;
                out.push_str(&format!(
                    "\\u{:04x}\\u{:04x}",
                    0xd800 + (code >> 10),
                    0xdc00 + (code & 0x3ff)
                ));
            }
            c if c as u32 > 0x7f => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
fn number(a: &Map<String, Value>, key: &str) -> f64 {
    a.get(key)
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0.)
}
fn boolean(a: &Map<String, Value>, key: &str) -> bool {
    match a.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::Bool(x)) => *x,
        Some(Value::Number(x)) => x.as_f64().is_some_and(|n| n != 0.),
        Some(Value::String(x)) => !x.is_empty(),
        Some(Value::Array(x)) => !x.is_empty(),
        Some(Value::Object(x)) => !x.is_empty(),
    }
}
fn normalize_slice(index: i64, len: i64) -> usize {
    if index < 0 {
        (len + index).max(0) as usize
    } else {
        index.min(len).max(0) as usize
    }
}
fn error(e: sqlx::Error) -> String {
    if let Some(db) = e.as_database_error() {
        let code = db.code().unwrap_or_default();
        let kind = match code.as_ref() {
            "22P02" => "InvalidTextRepresentation",
            "42P01" => "UndefinedTable",
            "42703" => "UndefinedColumn",
            "23505" => "UniqueViolation",
            "23503" => "ForeignKeyViolation",
            "23502" => "NotNullViolation",
            "22003" => "NumericValueOutOfRange",
            "22012" => "DivisionByZero",
            "42601" => "SyntaxError",
            _ if code.starts_with("08") => "OperationalError",
            _ if code.starts_with("23") => "IntegrityError",
            _ if code.starts_with("22") => "DataError",
            _ => "DatabaseError",
        };
        format!("{kind}: {}", db.message())
    } else if matches!(
        e,
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_)
    ) {
        format!("OperationalError: {e}")
    } else {
        e.to_string()
    }
}

#[cfg(test)]
mod du01_validation_tests {
    use super::*;
    #[test]
    fn du01_c04_strict_json_rejects_duplicate_and_nonfinite_values() {
        for raw in [
            r#"{"x":1,"x":2}"#,
            r#"{"x":NaN}"#,
            r#"{"x":1e999}"#,
            r#"{"x":9223372036854775808}"#,
        ] {
            assert!(parse_strict_json(raw).is_err(), "accepted {raw}");
        }
        let value = parse_strict_json(r#"{"x":1.0}"#).unwrap();
        let schema = json!({"type":"object","properties":{"x":{"type":"integer","minimum":1}},"required":["x"],"additionalProperties":false});
        assert!(validate_arguments(&schema, &value).is_ok());
    }
    #[test]
    fn du01_c04_type_and_boundary_matrix() {
        let schema = json!({"type":"object","properties":{"enabled":{"type":"boolean"},"count":{"type":"integer","minimum":1,"maximum":3},"name":{"type":"string","minLength":2,"maxLength":4,"pattern":"^[A-Z]+$"}},"required":["enabled","count","name"],"additionalProperties":false});
        for bad in [
            json!({"enabled":"true","count":2,"name":"AB"}),
            json!({"enabled":true,"count":true,"name":"AB"}),
            json!({"enabled":true,"count":0,"name":"AB"}),
            json!({"enabled":true,"count":2,"name":"aB"}),
            json!({"enabled":true,"count":2,"name":"ABCDE"}),
            json!({"enabled":true,"count":2,"name":"AB","extra":1}),
        ] {
            assert!(validate_arguments(&schema, &bad).is_err(), "accepted {bad}");
        }
        for good in [
            json!({"enabled":false,"count":1,"name":"AB"}),
            json!({"enabled":true,"count":2.0,"name":"XYZ"}),
            json!({"enabled":true,"count":3,"name":"A"}),
        ] {
            let got = validate_arguments(&schema, &good);
            if good["name"] == "A" {
                assert!(got.is_err());
            } else {
                assert!(got.is_ok(), "rejected {good}: {got:?}");
            }
        }
    }
    #[test]
    fn du01_c04_nested_array_object_enum_and_bounds_matrix() {
        let schema = json!({"type":"object","properties":{"rows":{"type":"array","minItems":1,"items":{"type":"object","required":["n"],"properties":{"n":{"type":"integer","minimum":1,"maximum":5},"mode":{"type":"string","enum":["robust"]}}}}},"required":["rows"]});
        assert!(validate_arguments(&schema, &json!({"rows":[{"n":2,"mode":"robust"}]})).is_ok());
        for bad in [
            json!({"rows":[]}),
            json!({"rows":[{"n":true}]}),
            json!({"rows":[{"n":6}]}),
            json!({"rows":[{"n":2,"mode":"naive"}]}),
        ] {
            assert!(validate_arguments(&schema, &bad).is_err(), "accepted {bad}");
        }
    }
    #[test]
    fn du01_c04_shared_boundary_case_matches_go_matrix() {
        let schema = json!({"type":"object","properties":{"mode":{"type":"string","enum":["safe","strict"]},"text":{"type":"string","minLength":2,"maxLength":4,"pattern":"^ok"},"items":{"type":"array","minItems":1,"maxItems":2,"items":{"type":"integer"}}},"required":["mode","text","items"],"additionalProperties":false});
        let valid = parse_strict_json(r#"{"mode":"safe","text":"okay","items":[1,2.0]}"#).unwrap();
        assert!(validate_arguments(&schema, &valid).is_ok());
        let invalid =
            parse_strict_json(r#"{"mode":"unsafe","text":"x","items":[true,2,3]}"#).unwrap();
        assert!(validate_arguments(&schema, &invalid).is_err());
        assert!(
            validate_arguments(&schema, &json!([])).is_err(),
            "non-object tool arguments must be rejected"
        );
    }
    #[test]
    fn du01_c04_config_schema_rejects_invalid_regex_and_accepts_annotation_keys() {
        let invalid = json!({"type":"string","pattern":"["});
        assert!(crate::validate_schema(&invalid, "test").is_err());
        let valid = json!({"type":"string","title":"label","default":"x","examples":["x"]});
        assert!(crate::validate_schema(&valid, "test").is_ok());
    }
}
