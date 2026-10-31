#!/usr/bin/env bash
# Shared auxiliary checks, items 11–16 from test-cases.md.
set -Eeuo pipefail

START_ITEM=11
if [[ "${1:-}" == '--help' ]]; then
  printf 'Usage: %s [--from 11|15|16]\nResume at 15 to rerun socket tests and cross-worker checks.\n' "$0"
  exit 0
fi
if [[ "${1:-}" == '--from' && $# == 2 ]]; then
  START_ITEM="$2"
elif [[ $# != 0 ]]; then
  printf 'Usage: %s [--from 11|15|16]\n' "$0" >&2
  exit 2
fi
[[ "$START_ITEM" == 11 || "$START_ITEM" == 15 || "$START_ITEM" == 16 ]] || {
  printf 'Unsupported start item: %s (expected 11, 15, or 16)\n' "$START_ITEM" >&2
  exit 2
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
source "$ROOT/scripts/agent_http.sh"
set -a
source "$ROOT/.env"
set +a
PORT="${PORT:-8081}"
SECOND_PORT="${SECOND_PORT:-8082}"
AGENT_URL="${AGENT_URL:-http://127.0.0.1:${PORT}}"
SECOND_AGENT_URL="${SECOND_AGENT_URL:-http://127.0.0.1:${SECOND_PORT}}"
RUN_FILE="$PWD/archive/curl-validation/current-run.txt"
[[ -f "$RUN_FILE" ]] || { printf 'Run test-cases.sh first; %s is missing.\n' "$RUN_FILE" >&2; exit 2; }
OUT=$(cat "$RUN_FILE")
[[ -d "$OUT" ]] || { printf 'Evidence directory does not exist: %s\n' "$OUT" >&2; exit 2; }
if [[ "$START_ITEM" == 11 ]]; then
[[ -f "$OUT/03-created.json" && -f "$OUT/04-created.json" && -f "$OUT/05-created.json" ]] || {
  printf 'Required Test 3, 4, and 5 response files are missing under %s.\n' "$OUT" >&2; exit 2;
}
C3=$(jq -er '.session.id' "$OUT/03-created.json")
C4=$(jq -er '.session.id' "$OUT/04-created.json")
C5=$(jq -er '.session.id' "$OUT/05-created.json")
fi
C15=''

failure_notice() {
  local status=$?
  trap - ERR
  printf '\nFAIL: auxiliary test command exited %s. Evidence retained under %s\n' "$status" "$OUT" >&2
  if [[ -n "${C15:-}" ]]; then
    agent_curl -sS "$SECOND_AGENT_URL/sessions/$C15" -o "$OUT/aux-failure-state.json" || true
    agent_curl -sS "$SECOND_AGENT_URL/sessions/$C15/transcript?after_sequence=0&limit=1000" -o "$OUT/aux-failure-transcript.json" || true
  fi
  exit "$status"
}
trap failure_notice ERR

printf 'Target: %s\nSecond worker: %s\nEvidence: %s\n' "$AGENT_URL" "$SECOND_AGENT_URL" "$OUT"

# Item 11 — invalid config and restart hydration.
if [[ "$START_ITEM" == 11 ]]; then
HTTP_CODE=$(agent_curl -sS -o "$OUT/10-invalid-config.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  -d '{"config_ref":"../.env","user_prompt":"This request must be rejected."}' \
  "$AGENT_URL/sessions")
printf 'HTTP %s\n' "$HTTP_CODE"
[[ "$HTTP_CODE" == 422 ]]
printf 'PASS invalid config path rejected\n'

agent_curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/10-before-restart.json"
jq '{id,goal,config_ref,config_sha256,status,final_answer}' "$OUT/10-before-restart.json" \
  > "$OUT/10-before-restart-summary.json"
printf '\nRestart the same agent server at %s in its server terminal, using the same configuration and database on port %s.\n' "$AGENT_URL" "$PORT"
read -r -p 'After the same server has restarted and is ready, press Enter to continue: ' _
agent_curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
agent_curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/10-after-restart.json"
jq '{id,goal,config_ref,config_sha256,status,final_answer}' "$OUT/10-after-restart.json" \
  > "$OUT/10-after-restart-summary.json"
diff -u "$OUT/10-before-restart-summary.json" "$OUT/10-after-restart-summary.json"

# Item 12 — transcript pagination.
agent_curl -fsS "$AGENT_URL/sessions/$C5/transcript?after_sequence=0&limit=1" \
  -o "$OUT/11-transcript-page-1.json"
jq -e '.history_complete == true and (.entries | length) == 1 and
  .entries[0].sequence == 1 and .entries[0].message_id and
  .through_sequence >= 2 and .next_after_sequence == 1' "$OUT/11-transcript-page-1.json"
TRANSCRIPT_THROUGH=$(jq -er '.through_sequence' "$OUT/11-transcript-page-1.json")
agent_curl -fsS "$AGENT_URL/sessions/$C5/transcript?after_sequence=1&limit=1&through_sequence=$TRANSCRIPT_THROUGH" \
  -o "$OUT/11-transcript-page-2.json"
jq -e '.entries | length == 1 and .[0].sequence == 2 and .[0].message_id' \
  "$OUT/11-transcript-page-2.json"
jq -e --slurpfile first "$OUT/11-transcript-page-1.json" \
  '.entries[0].message_id != $first[0].entries[0].message_id and
   .entries[0].sequence > $first[0].entries[0].sequence' \
  "$OUT/11-transcript-page-2.json"

# Item 13 — event and run pagination.
agent_curl -fsS "$AGENT_URL/sessions/$C5/events?after=0&limit=1" -o "$OUT/12-events-page-1.json"
jq -e '.through >= 1 and (.entries | length) == 1 and
  .entries[0].event_id > 0 and .next_after == .entries[0].event_id' "$OUT/12-events-page-1.json"
EVENT_THROUGH=$(jq -er '.through' "$OUT/12-events-page-1.json")
EVENT_AFTER=$(jq -er '.next_after' "$OUT/12-events-page-1.json")
agent_curl -fsS "$AGENT_URL/sessions/$C5/events?after=$EVENT_AFTER&limit=1&through=$EVENT_THROUGH" \
  -o "$OUT/12-events-page-2.json"
jq -e --argjson after "$EVENT_AFTER" '.entries | length == 1 and .[0].event_id > $after' \
  "$OUT/12-events-page-2.json"
agent_curl -fsS "$AGENT_URL/sessions/$C5/runs?limit=1" -o "$OUT/12-runs-page-1.json"
jq -e '.runs | length == 1 and .[0].run_id and .[0].fence >= 1 and .[0].status == "complete"' \
  "$OUT/12-runs-page-1.json"
RUN_AFTER=$(jq -er '.next_after' "$OUT/12-runs-page-1.json")
agent_curl -fsS "$AGENT_URL/sessions/$C5/runs?limit=1&after=$RUN_AFTER" -o "$OUT/12-runs-page-2.json"
jq -e --arg previous "$RUN_AFTER" '.runs | length == 1 and .[0].run_id != $previous and .[0].status == "complete"' \
  "$OUT/12-runs-page-2.json"

# Item 14 — executions, compaction diagnostics, archives and retention.
agent_curl -fsS "$AGENT_URL/sessions/$C4/executions?limit=1" -o "$OUT/13-executions-page-1.json"
jq -e '(.executions | length) == 1 and .executions[0].execution_id and
  .executions[0].run_id and .executions[0].call_id and
  (.executions[0].status == "succeeded" or .executions[0].status == "failed") and
  .next_after == .executions[0].execution_id' \
  "$OUT/13-executions-page-1.json"
EXECUTION_AFTER=$(jq -er '.next_after' "$OUT/13-executions-page-1.json")
agent_curl -fsS "$AGENT_URL/sessions/$C4/executions?limit=1&after=$EXECUTION_AFTER" \
  -o "$OUT/13-executions-page-2.json"
jq -e --arg previous "$EXECUTION_AFTER" '.executions | length == 1 and
  .[0].execution_id != $previous and
  (.[0].status == "succeeded" or .[0].status == "failed")' \
  "$OUT/13-executions-page-2.json"
agent_curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_attempts?limit=1&after=0" \
  -o "$OUT/13-attempts-page-1.json"
jq -e '.total_recorded > 0 and .through >= 1 and (.entries | length) == 1 and
  .entries[0].ordinal == 1 and .next_after == 1' "$OUT/13-attempts-page-1.json"
DIAGNOSTICS_THROUGH=$(jq -er '.through' "$OUT/13-attempts-page-1.json")
agent_curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_attempts?limit=1&after=1&through=$DIAGNOSTICS_THROUGH" \
  -o "$OUT/13-attempts-page-2.json"
jq -e '.entries | length == 1 and .[0].ordinal == 2' "$OUT/13-attempts-page-2.json"
agent_curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_events?limit=10&after=0" \
  -o "$OUT/13-compaction-events.json"
agent_curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_decisions?limit=10&after=0" \
  -o "$OUT/13-compaction-decisions.json"
jq -e '.total_recorded > 0 and (.entries | length) > 0' "$OUT/13-compaction-events.json"
jq -e '.total_recorded > 0 and (.entries | length) > 0' "$OUT/13-compaction-decisions.json"
agent_curl -fsS "$AGENT_URL/sessions/$C4/archives?limit=10" -o "$OUT/13-archives.json"
jq -e '.archives | type == "array"' "$OUT/13-archives.json"
agent_curl -fsS "$AGENT_URL/sessions/$C4/retention" -o "$OUT/13-retention.json"
jq -e '.default_policy == "keep_all" and .eligible == true and
  .unresolved_executions == 0 and .running_lifecycles == 0 and
  (.protected_tables | length) > 0 and (.prunable_tables | length) > 0' \
  "$OUT/13-retention.json"
agent_curl -fsS "$AGENT_URL/runs/abandoned?limit=10" -o "$OUT/13-abandoned-runs.json"
jq -e '.runs | type == "array"' "$OUT/13-abandoned-runs.json"
fi

# Item 15 — reusable Python tests synchronize on SSE/model/cancellation signals.
if [[ "$START_ITEM" != 16 ]]; then
  DISCONNECT_EVIDENCE=$(mktemp -d "$OUT/14-socket.XXXXXX")
  printf '\nSTART real socket integration suite. Evidence: %s/suite\n' "$DISCONNECT_EVIDENCE"
  python3 "$ROOT/scripts/disconnect_integration_test.py" \
    --agent-url "$AGENT_URL" --evidence-dir "$DISCONNECT_EVIDENCE/suite"
fi

# Item 16 — run two workers; the second server must use the same database/config.
printf '\nStart a second instance of the same agent at %s on port %s in another terminal, using the same configuration and database.\n' "$SECOND_AGENT_URL" "$SECOND_PORT"
read -r -p 'After the second server is ready, press Enter to run cross-worker checks: ' _
agent_curl -fsS "$SECOND_AGENT_URL/health" | jq -e '.status == "ok"'
agent_curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/15-created.json"
C15=$(jq -er '.session.id' "$OUT/15-created.json")
printf 'Session ID: %s\nEvidence directory: %s\n' "$C15" "$OUT"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C15/run/stream" | tee "$OUT/15-owner.sse" &
OWNER_PID=$!
POLL=0
while [ "$POLL" -lt 120 ]; do
  if agent_curl -fsS "$SECOND_AGENT_URL/sessions/$C15" -o "$OUT/15-running-state.json"; then
    if jq -e '.run_status == "running" and .run_id' "$OUT/15-running-state.json" >/dev/null; then break; fi
  fi
  sleep 1
  POLL=$((POLL + 1))
done
jq -e '.run_status == "running" and .run_id' "$OUT/15-running-state.json"
HTTP_CODE=$(agent_curl -sS -o "$OUT/15-competing-run.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"max_steps":1}' "$SECOND_AGENT_URL/sessions/$C15/run")
[[ "$HTTP_CODE" == 409 ]]
printf 'PASS competing run rejected\n'
HTTP_CODE=$(agent_curl -sS -o "$OUT/15-competing-message.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"content":"Concurrent input must not be appended."}' \
  "$SECOND_AGENT_URL/sessions/$C15/messages")
[[ "$HTTP_CODE" == 409 ]]
printf 'PASS competing message rejected\n'
CANCEL_RUN_ID=$(jq -er '.run_id' "$OUT/15-running-state.json")
agent_curl -fsS -H 'Content-Type: application/json' -d "{\"run_id\":\"$CANCEL_RUN_ID\"}" \
  "$SECOND_AGENT_URL/sessions/$C15/cancel" -o "$OUT/15-cancel-response.json"
jq -e --slurpfile running "$OUT/15-running-state.json" \
  '.cancel_requested_for | index($running[0].run_id) != null' "$OUT/15-cancel-response.json"
wait "$OWNER_PID" || true
agent_curl -fsS "$SECOND_AGENT_URL/sessions/$C15" -o "$OUT/15-cancelled-state.json"
jq -e '.run_status == "cancelled" and .last_error.code == "run_cancelled"' "$OUT/15-cancelled-state.json"
agent_curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$SECOND_AGENT_URL/sessions/$C15/resume" | tee "$OUT/15-resume.sse"
agent_curl -fsS "$SECOND_AGENT_URL/sessions/$C15" -o "$OUT/15-final-state.json"
jq -e --slurpfile old "$OUT/15-cancelled-state.json" \
  '.status == "complete" and .run_status == "complete" and
   .run_id != $old[0].run_id and .goal == $old[0].goal and
   .config_sha256 == $old[0].config_sha256 and
   .workflow.active_phase == null and (.workflow.observed_evidence | length) == 11' \
  "$OUT/15-final-state.json"

trap - ERR
printf '\nPASS: auxiliary items %s–16 completed. Evidence: %s\n' "$START_ITEM" "$OUT"
