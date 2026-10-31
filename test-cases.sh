#!/usr/bin/env bash
# Shared Tests 1–6 from test-cases.md. The API target is read from .env.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/scripts/agent_http.sh"
set -a
source "$ROOT/.env"
set +a
PORT="${PORT:-8081}"
AGENT_URL="${AGENT_URL:-http://127.0.0.1:${PORT}}"

mkdir -p "$PWD/archive/curl-validation"
OUT=$(mktemp -d "$PWD/archive/curl-validation/shared.XXXXXX")
printf '%s\n' "$OUT" > "$PWD/archive/curl-validation/current-run.txt"
printf 'Saving responses under %s\n' "$OUT"
printf 'Target: %s\n' "$AGENT_URL"

CASE_NUM=''
CASE_ID=''
save_failure_evidence() {
  local original_status=$?
  trap - ERR
  set +e
  printf '\nFAIL: Test %s (HTTP command/assertion exit %s)\n' "${CASE_NUM:-preflight}" "$original_status" >&2
  if [[ -z "${CASE_ID:-}" && -n "${CASE_NUM:-}" ]]; then
    local created_file
    printf -v created_file '%02d-created.json' "$CASE_NUM"
    CASE_ID=$(jq -r '.session.id // empty' "$OUT/$created_file" 2>/dev/null)
  fi
  if [[ "${CASE_NUM:-}" == '4' && -n "${CASE_ID:-}" ]]; then
    C4=${C4:-$CASE_ID}
  fi
  if [[ -n "${CASE_ID:-}" ]]; then
    agent_curl -sS -w '\nHTTP %{http_code}\n' "$AGENT_URL/sessions/$CASE_ID" \
      -o "$OUT/failure-state.json" || true
    jq '{id,status,run_status,run_id,run_count,step_count,revision,config_ref,config_sha256,
         active_phase:.workflow.active_phase,completed_phases:.workflow.completed_phases,
         last_error,final_answer}' "$OUT/failure-state.json" 2>/dev/null || true
    agent_curl -sS -w '\nHTTP %{http_code}\n' \
      "$AGENT_URL/sessions/$CASE_ID/transcript?after_sequence=0&limit=1000" \
      -o "$OUT/failure-transcript.json" || true
    jq '{history_complete,through_sequence,entries}' "$OUT/failure-transcript.json" 2>/dev/null || true
    agent_curl -sS -w '\nHTTP %{http_code}\n' \
      "$AGENT_URL/sessions/$CASE_ID/executions?limit=100" \
      -o "$OUT/failure-executions.json" || true
    jq '{executions,next_after}' "$OUT/failure-executions.json" 2>/dev/null || true
  fi
  if [[ "${CASE_NUM:-}" == '4' && -n "${C4:-}" ]]; then
    for FAMILY in compaction_attempts compaction_events compaction_decisions; do
      agent_curl -sS -w '\nHTTP %{http_code}\n' \
        "$AGENT_URL/sessions/$C4/diagnostics/$FAMILY?after=0&limit=1000" \
        -o "$OUT/failure-$FAMILY.json" || true
    done
  fi
  printf 'Failure evidence retained under %s\n' "$OUT" >&2
  exit "$original_status"
}
trap save_failure_evidence ERR

# Item 3 — prepare the curl terminal.
agent_curl -fsS "$AGENT_URL/health" -o "$OUT/health.json"
jq -e '.status == "ok"' "$OUT/health.json"
agent_curl -fsS "$AGENT_URL/configs" -o "$OUT/configs.json"
jq -e '[.[].config_ref] | contains([
  "log-analysis.yaml", "revenue-analysis.yaml", "employee-audit.yaml",
  "compaction-stress.yaml", "completion-no-workflow.yaml", "tool-recovery.yaml"
])' "$OUT/configs.json"

# Test 1 — log analysis.
CASE_NUM='1'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"log-analysis.yaml","user_prompt":"Inspect and robustly analyze the HTTP access log. Validate the most frequent HTTP 500 IP and its count. In your final answer, state the IP and count."}' \
  "$AGENT_URL/sessions" -o "$OUT/01-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "log-analysis.yaml"' "$OUT/01-created.json"
C1=$(jq -er '.session.id' "$OUT/01-created.json")
CASE_ID="$C1"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C1/run/stream" | tee "$OUT/01-run.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C1" -o "$OUT/01-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.final_answer | type == "string" and contains("10.0.0.5") and contains("83")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[]; .function.name == "inspect_log")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "analyze_500_ips" and (.function.arguments | fromjson | .robust_parser == true))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_top_offending_ip" and (.function.arguments | fromjson | .ip == "10.0.0.5" and .count == 83))) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and
    (.content | contains("10.0.0.5") and contains("83") and contains("malformed_records_ignored"))) and
  any(.memory[]; .role == "tool" and .name == "validate_top_offending_ip" and (.content | contains("\"valid\": true")))
' "$OUT/01-state.json"

# Test 2 — normalized Chicago revenue.
CASE_NUM='2'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"revenue-analysis.yaml","user_prompt":"Inspect the schema. Calculate Chicago revenue for Q1 2026, normalizing city casing and foreign keys. Validate the result, then report the validated total."}' \
  "$AGENT_URL/sessions" -o "$OUT/02-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "revenue-analysis.yaml"' "$OUT/02-created.json"
C2=$(jq -er '.session.id' "$OUT/02-created.json")
CASE_ID="$C2"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C2/run/stream" | tee "$OUT/02-run.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C2" -o "$OUT/02-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.final_answer | type == "string" and (contains("271017.77") or contains("271,017.77"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[]; .function.name == "inspect_sql_schema")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "query_chicago_q1_revenue" and
    (.function.arguments | fromjson | .normalize_keys == true and .normalize_city == true))) and
  any(.memory[]; .role == "tool" and .name == "query_chicago_q1_revenue" and (.content | contains("271017.77"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_chicago_q1_revenue" and (.function.arguments | fromjson | .total == 271017.77))) and
  any(.memory[]; .role == "tool" and .name == "validate_chicago_q1_revenue" and (.content | contains("\"valid\": true")))
' "$OUT/02-state.json"

# Test 3 — employee audit.
CASE_NUM='3'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"employee-audit.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/03-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "employee-audit.yaml"' "$OUT/03-created.json"
C3=$(jq -er '.session.id' "$OUT/03-created.json")
CASE_ID="$C3"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C3/run/stream" | tee "$OUT/03-run.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/03-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.workflow.completed_phases | sort) == (["phase_1_corruption_audit","phase_2_salary_median","phase_3_city_report"] | sort) and
  (.final_answer | startswith("Phase 3\n")) and
  ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) ==
    (["|Austin|199|42.62|","|Chicago|181|42.01|","|NewYork|329|43.02|","|Seattle|183|43.46|"] | sort) and
  (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)"))
' "$OUT/03-state.json"

# Test 4 — compaction stress.
CASE_NUM='4'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/04-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "compaction-stress.yaml"' "$OUT/04-created.json"
C4=$(jq -er '.session.id' "$OUT/04-created.json")
CASE_ID="$C4"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C4/run/stream" | tee "$OUT/04-run.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C4" -o "$OUT/04-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.workflow.observed_evidence | length) == 11 and
  (.compaction_events | length) > 0 and
  (.final_answer | startswith("Phase 3\n")) and
  ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) ==
    (["|Austin|199|42.62|","|Chicago|181|42.01|","|NewYork|329|43.02|","|Seattle|183|43.46|"] | sort) and
  (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)"))
' "$OUT/04-state.json"

# Test 5 — workflow-free completion and continuation.
CASE_NUM='5'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"completion-no-workflow.yaml","user_prompt":"Reply with exactly CURL_OK. Do not call tools."}' \
  "$AGENT_URL/sessions" -o "$OUT/05-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "completion-no-workflow.yaml"' "$OUT/05-created.json"
C5=$(jq -er '.session.id' "$OUT/05-created.json")
CASE_ID="$C5"
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"content":"The required response is still exactly CURL_OK."}' \
  "$AGENT_URL/sessions/$C5/messages" -o "$OUT/05-message.json"
jq -e '.session.status == "active" and .session.goal == "Reply with exactly CURL_OK. Do not call tools." and
  any(.session.memory[]; .role == "user" and .content == "The required response is still exactly CURL_OK.")' \
  "$OUT/05-message.json"
agent_curl -fsS -H 'Content-Type: application/json' -d '{"max_steps":5}' \
  "$AGENT_URL/sessions/$C5/run" -o "$OUT/05-run.json"
jq -e '.session.status == "complete" and .session.run_status == "complete" and .session.final_answer == "CURL_OK"' "$OUT/05-run.json"
agent_curl -fsS "$AGENT_URL/sessions/$C5" -o "$OUT/05-before-resume.json"
agent_curl -N -fsS -H 'Content-Type: application/json' \
  -d '{"content":"Now reply with exactly CURL_NEXT. Do not call tools.","max_steps":5}' \
  "$AGENT_URL/sessions/$C5/resume" | tee "$OUT/05-resume.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C5" -o "$OUT/05-after-resume.json"
jq -e --slurpfile before "$OUT/05-before-resume.json" '
  .status == "complete" and .run_status == "complete" and .final_answer == "CURL_NEXT" and
  .id == $before[0].id and .goal == $before[0].goal and
  .config_sha256 == $before[0].config_sha256 and
  .run_count == ($before[0].run_count + 1)
' "$OUT/05-after-resume.json"

# Test 6 — bounded tool error and resume.
CASE_NUM='6'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"tool-recovery.yaml","user_prompt":"Follow the failure, robust retry, and validation workflow. Return the exact recovery answer required by the profile."}' \
  "$AGENT_URL/sessions" -o "$OUT/06-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "tool-recovery.yaml"' "$OUT/06-created.json"
C6=$(jq -er '.session.id' "$OUT/06-created.json")
CASE_ID="$C6"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":1}' \
  "$AGENT_URL/sessions/$C6/run/stream" | tee "$OUT/06-first-run.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C6" -o "$OUT/06-after-stop.json"
jq -e '.status == "failed" and .run_status == "failed" and .step_count <= 1 and .final_answer != "RECOVERY_OK 10.0.0.5 83"' "$OUT/06-after-stop.json"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C6/resume" | tee "$OUT/06-resume.sse"
agent_curl -fsS "$AGENT_URL/sessions/$C6" -o "$OUT/06-final-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and .final_answer == "RECOVERY_OK 10.0.0.5 83" and
  (.workflow.completed_phases | sort) == (["diagnose_failure","robust_retry","validate_result"] | sort) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and (.content | contains("ERROR: naive parser failed"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "analyze_500_ips" and (.function.arguments | fromjson | .robust_parser == false))) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and
    (.content | contains("10.0.0.5") and contains("83"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_top_offending_ip" and (.function.arguments | fromjson | .ip == "10.0.0.5" and .count == 83))) and
  any(.memory[]; .role == "tool" and .name == "validate_top_offending_ip" and (.content | contains("\"valid\": true")))
' "$OUT/06-final-state.json"
agent_curl -fsS "$AGENT_URL/sessions/$C6/executions?limit=100" -o "$OUT/06-executions.json"
jq -e '
  (.executions | length) == 3 and
  ([.executions[] | select(.tool == "analyze_500_ips")] | length) == 2 and
  ([.executions[] | select(.tool == "validate_top_offending_ip")] | length) == 1 and
  any(.executions[]; .tool == "analyze_500_ips" and .status == "failed" and
    (.result | contains("ERROR: naive parser failed"))) and
  any(.executions[]; .tool == "analyze_500_ips" and .status == "succeeded" and
    (.result | contains("10.0.0.5") and contains("83"))) and
  any(.executions[]; .tool == "validate_top_offending_ip" and .status == "succeeded" and
    (.result | contains("\"valid\": true")))
' "$OUT/06-executions.json"

trap - ERR
printf '\nPASS: shared Tests 1–6 completed. Evidence: %s\n' "$OUT"
