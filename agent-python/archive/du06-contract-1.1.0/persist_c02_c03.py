"""Shared black-box DU-05 C02/C03 PostgreSQL checks for every agent server.

The runner addresses only AGENT_URL and the common PostgreSQL schema. It does
not select or detect a language. Run test-cases.sh first to create the sessions
whose append/compaction/stop/resume evidence is inspected here. The runner
pauses for a user-managed server restart so it can verify restart hydration
without knowing which implementation is listening at AGENT_URL.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import psycopg
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
RUN_FILE = ROOT / "archive" / "curl-validation" / "current-run.txt"
EVIDENCE_ROOT = ROOT / "archive" / "persistence"


def load_env_file() -> dict[str, str]:
    """Read simple KEY=value dotenv entries without printing values."""
    values: dict[str, str] = {}
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


class PersistenceChecks:
    def __init__(self, base_url: str, database_url: str, evidence: Path):
        self.base_url = base_url.rstrip("/")
        self.database_url = database_url
        self.evidence = evidence
        self.http = httpx.Client(base_url=self.base_url, timeout=30.0)
        self.owned_sessions: list[str] = []

    def close(self) -> None:
        self.http.close()
        if self.owned_sessions:
            try:
                self.sql("DELETE FROM agent_sessions WHERE session_id = ANY(%s::uuid[])", (self.owned_sessions,))
                print(f"CLEANUP removed {len(self.owned_sessions)} test-owned sessions", flush=True)
            except Exception as error:
                print(f"CLEANUP WARNING: {type(error).__name__}: {error}", file=sys.stderr, flush=True)

    def get(self, path: str) -> dict[str, Any]:
        response = self.http.get(path)
        require(response.status_code == 200, f"GET {path} returned HTTP {response.status_code}: {response.text[:500]}")
        return response.json()

    def full_transcript(self, session_id: str) -> list[dict[str, Any]]:
        first = self.get(f"/sessions/{session_id}/transcript?after_sequence=0&limit=1000")
        require(first.get("history_complete") is True, f"{session_id}: transcript history is incomplete")
        through = first.get("through_sequence")
        require(isinstance(through, int), f"{session_id}: transcript has no integer watermark")
        rows = list(first.get("entries", []))
        after = first.get("next_after_sequence", 0)
        while after < through:
            page = self.get(f"/sessions/{session_id}/transcript?after_sequence={after}&limit=1000&through_sequence={through}")
            page_rows = page.get("entries", [])
            require(page_rows, f"{session_id}: transcript pagination stopped before watermark {through}")
            rows.extend(page_rows)
            after = page.get("next_after_sequence", after)
        sequences = [row.get("sequence") for row in rows]
        require(sequences == list(range(1, through + 1)), f"{session_id}: transcript sequence gaps/duplicates: {sequences[:20]}… through={through}")
        ids = [row.get("message_id") for row in rows]
        require(all(isinstance(value, str) for value in ids) and len(set(ids)) == len(ids), f"{session_id}: transcript IDs are missing or duplicated")
        return rows

    @staticmethod
    def message_payload(message: dict[str, Any]) -> dict[str, Any]:
        excluded = {"message_id", "sequence", "created_at"}
        return {key: value for key, value in message.items() if key not in excluded and value is not None}

    def assert_state_messages_in_transcript(self, state: dict[str, Any], rows: list[dict[str, Any]], label: str) -> None:
        by_id = {row["message_id"]: row for row in rows if row.get("kind") == "message"}
        messages = state.get("memory", [])
        for message in messages:
            message_id = message.get("message_id")
            if not message_id:
                continue
            record = by_id.get(message_id)
            require(record is not None, f"{label}: active message {message_id} is absent from transcript")
            require(record["sequence"] == message.get("sequence"), f"{label}: message sequence changed for {message_id}")
            require(record["payload"] == self.message_payload(message), f"{label}: persisted payload changed for {message_id}")

    def c02(self) -> None:
        print("START DU05-C02 append/compaction/stop/restart/resume", flush=True)
        # These are produced by the unchanged shared HTTP acceptance suite.
        required = [
            "04-state.json", "05-before-resume.json", "05-after-resume.json",
            "06-after-stop.json", "06-final-state.json",
        ]
        missing = [name for name in required if not (self.evidence / name).is_file()]
        require(not missing, "Run ./test-cases.sh first; missing: " + ", ".join(missing))

        stress = read_json(self.evidence / "04-state.json")
        stress_id = stress["id"]
        stress_live = self.get(f"/sessions/{stress_id}")
        require(stress_live.get("goal") == stress.get("goal"), "stress session goal changed after reload")
        require(stress_live.get("config_sha256") == stress.get("config_sha256"), "stress session config identity changed")
        require(len(stress.get("workflow", {}).get("observed_evidence", [])) == 11, "stress workflow evidence was not preserved")
        require(stress.get("compaction_events"), "stress session has no accepted compaction evidence")
        stress_rows = self.full_transcript(stress_id)
        by_id = {row["message_id"]: row for row in stress_rows}
        summaries = [row for row in stress_rows if row.get("kind") == "compaction_summary"]
        require(summaries, "accepted compaction has no immutable compaction_summary transcript record")
        for summary in summaries:
            payload = summary.get("payload", {})
            for source_id in payload.get("source_message_ids", []):
                require(source_id in by_id and by_id[source_id]["kind"] == "message", f"summary source {source_id} is unresolved")
            prior = payload.get("previous_ledger_id")
            if prior:
                require(prior in by_id and by_id[prior]["kind"] == "compaction_summary", f"prior ledger {prior} is unresolved")
        require(stress_live.get("next_transcript_sequence") == stress_rows[-1]["sequence"] + 1, "stress next sequence does not follow transcript watermark")

        before_resume = read_json(self.evidence / "05-before-resume.json")
        after_resume = read_json(self.evidence / "05-after-resume.json")
        require(before_resume["id"] == after_resume["id"], "continuation changed session identity")
        require(before_resume["goal"] == after_resume["goal"], "continuation changed original goal")
        require(before_resume["config_sha256"] == after_resume["config_sha256"], "continuation changed config identity")
        require(after_resume["run_count"] == before_resume["run_count"] + 1, "continuation did not advance run count once")
        continuation_rows = self.full_transcript(before_resume["id"])
        self.assert_state_messages_in_transcript(before_resume, continuation_rows, "pre-resume prefix")
        self.assert_state_messages_in_transcript(after_resume, continuation_rows, "post-resume state")

        stopped = read_json(self.evidence / "06-after-stop.json")
        resumed = read_json(self.evidence / "06-final-state.json")
        require(stopped["id"] == resumed["id"], "resume changed session identity")
        require(stopped["goal"] == resumed["goal"], "resume changed original goal")
        require(stopped["config_sha256"] == resumed["config_sha256"], "resume changed config identity")
        require(stopped["run_id"] != resumed["run_id"], "resume reused the stopped run identity")
        require(resumed["run_count"] == stopped["run_count"] + 1, "resume did not advance run count once")
        recovery_rows = self.full_transcript(stopped["id"])
        self.assert_state_messages_in_transcript(stopped, recovery_rows, "bounded-stop prefix")
        self.assert_state_messages_in_transcript(resumed, recovery_rows, "resumed state")

        restart_id = after_resume["id"]
        before_restart = self.get(f"/sessions/{restart_id}")
        before_restart_rows = self.full_transcript(restart_id)
        (self.evidence / "persist-before-restart.json").write_text(
            json.dumps(before_restart, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(
            f"RESTART REQUIRED: stop and restart the same server at {self.base_url} in its server terminal. "
            "Keep the same .env and database. When /health is ready, press Enter here.",
            flush=True,
        )
        input()
        health = self.get("/health")
        require(health.get("status") == "ok", "server health did not recover after restart")
        after_restart = self.get(f"/sessions/{restart_id}")
        require(compact_json({key: before_restart.get(key) for key in ("id", "goal", "config_ref", "config_sha256", "status", "final_answer")}) ==
                compact_json({key: after_restart.get(key) for key in ("id", "goal", "config_ref", "config_sha256", "status", "final_answer")}),
                "server restart changed the persisted session projection")
        after_restart_rows = self.full_transcript(restart_id)
        require(compact_json(before_restart_rows) == compact_json(after_restart_rows), "transcript changed across server restart")
        (self.evidence / "persist-after-restart.json").write_text(
            json.dumps(after_restart, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(f"PASS DU05-C02 stress_records={len(stress_rows)} continuation_records={len(continuation_rows)} recovery_records={len(recovery_rows)}", flush=True)

    def create_session(self, prompt: str) -> str:
        response = self.http.post("/sessions", json={
            "config_ref": "completion-no-workflow.yaml",
            "user_prompt": prompt,
        })
        require(response.status_code == 201, f"POST /sessions returned HTTP {response.status_code}: {response.text[:500]}")
        session_id = response.json()["session"]["id"]
        self.owned_sessions.append(session_id)
        return session_id

    def sql(self, query: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall() if cursor.description else []

    def durable_image(self, session_id: str) -> tuple[Any, ...]:
        state = self.sql("SELECT state FROM agent_sessions WHERE session_id=%s", (session_id,))
        require(bool(state), f"session {session_id} missing from shared agent_sessions table")
        transcript = self.sql("""SELECT sequence,message_id::text,kind,payload,run_id::text,decision_step,created_at::text
            FROM agent_session_transcript WHERE session_id=%s ORDER BY sequence""", (session_id,))
        events = self.sql("SELECT event_id,event,created_at::text FROM agent_session_events WHERE session_id=%s ORDER BY event_id", (session_id,))
        context = self.sql("SELECT to_jsonb(c) FROM agent_session_context c WHERE session_id=%s", (session_id,))
        diagnostics = self.sql("SELECT to_jsonb(d) FROM agent_session_diagnostics d WHERE session_id=%s ORDER BY family,ordinal", (session_id,))
        return state[0][0], tuple(transcript), tuple(events), tuple(context), tuple(diagnostics)

    def assert_rejected_atomically(self, session_id: str, before: tuple[Any, ...], label: str) -> None:
        response = self.http.post(f"/sessions/{session_id}/messages", json={"content": f"persistence probe {label}"})
        require(response.status_code >= 400, f"{label}: corrupted immutable history was accepted (HTTP {response.status_code})")
        after = self.durable_image(session_id)
        require(compact_json(before) == compact_json(after), f"{label}: rejected write changed durable rows")
        print(f"PASS DU05-C03 {label} rejected atomically HTTP={response.status_code}", flush=True)

    def c03(self) -> None:
        print("START DU05-C03 immutable identity/sequence/provenance/replay", flush=True)
        session_id = self.create_session(f"DU05 C03 integrity fixture {uuid4()}")
        other_id = self.create_session(f"DU05 C03 second integrity fixture {uuid4()}")
        initial = self.sql("""SELECT sequence,message_id::text,kind,payload,run_id::text,decision_step,created_at
            FROM agent_session_transcript WHERE session_id=%s ORDER BY sequence""", (session_id,))
        require(len(initial) >= 1 and initial[0][2] == "message", "new session does not have its initial goal transcript message")
        row = initial[0]

        # An exact duplicate identity must not create a second durable record.
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO agent_session_transcript
                    (session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (session_id,message_id) DO NOTHING RETURNING message_id""",
                    (UUID(session_id), row[0], UUID(row[1]), row[2], Jsonb(row[3]), UUID(row[4]) if row[4] else None, row[5], row[6]))
                returned = cursor.fetchone()
        require(returned is None, "exact replay inserted a duplicate transcript identity")
        require(len(self.sql("SELECT sequence FROM agent_session_transcript WHERE session_id=%s", (session_id,))) == len(initial), "exact replay changed transcript count")
        print("PASS DU05-C03 exact replay created no duplicate", flush=True)

        # Corrupt one existing message payload; a normal API save must detect
        # the immutable-history conflict and roll back all writes.
        before = self.durable_image(session_id)
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE agent_session_transcript SET payload=jsonb_set(payload,'{content}',to_jsonb('tampered'::text)) WHERE session_id=%s AND message_id=%s", (session_id, row[1]))
        corrupted = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, corrupted, "changed-payload-id-reuse")
        finally:
            self.sql("UPDATE agent_session_transcript SET payload=%s WHERE session_id=%s AND message_id=%s", (Jsonb(row[3]), session_id, row[1]))
        require(compact_json(before) == compact_json(self.durable_image(session_id)), "payload fixture was not restored")

        before_kind = self.durable_image(session_id)
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE agent_session_transcript SET kind='tool' WHERE session_id=%s AND message_id=%s", (session_id, row[1]))
        kind_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, kind_image, "changed-kind-id-reuse")
        finally:
            self.sql("UPDATE agent_session_transcript SET kind=%s WHERE session_id=%s AND message_id=%s", (row[2], session_id, row[1]))
        require(compact_json(before_kind) == compact_json(self.durable_image(session_id)), "kind fixture was not restored")

        before_timestamp = self.durable_image(session_id)
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE agent_session_transcript SET created_at=created_at+interval '1 second' WHERE session_id=%s AND message_id=%s", (session_id, row[1]))
        timestamp_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, timestamp_image, "changed-timestamp-id-reuse")
        finally:
            self.sql("UPDATE agent_session_transcript SET created_at=%s WHERE session_id=%s AND message_id=%s", (row[6], session_id, row[1]))
        require(compact_json(before_timestamp) == compact_json(self.durable_image(session_id)), "timestamp fixture was not restored")

        # A sequence gap must be rejected before appending any new record.
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE agent_session_transcript SET sequence=sequence+100 WHERE session_id=%s AND message_id=%s", (session_id, row[1]))
        gap_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, gap_image, "sequence-gap")
        finally:
            self.sql("UPDATE agent_session_transcript SET sequence=%s WHERE session_id=%s AND message_id=%s", (row[0], session_id, row[1]))

        state_row = self.sql("SELECT state FROM agent_sessions WHERE session_id=%s", (session_id,))[0][0]
        expected_next = state_row.get("next_transcript_sequence")
        require(isinstance(expected_next, int), "session snapshot has no next transcript sequence")
        self.sql("UPDATE agent_sessions SET state=jsonb_set(state,'{next_transcript_sequence}',to_jsonb(%s::bigint),true) WHERE session_id=%s", (expected_next + 1, session_id))
        counter_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, counter_image, "next-sequence-mismatch")
        finally:
            self.sql("UPDATE agent_sessions SET state=%s WHERE session_id=%s", (Jsonb(state_row), session_id))

        # Primary-key enforcement rejects a different identity at an occupied
        # sequence. The failed statement must leave the existing row intact.
        collision_before = self.durable_image(session_id)
        collision_rejected = False
        try:
            self.sql("""INSERT INTO agent_session_transcript
                (session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at)
                VALUES (%s,%s,%s,'message','{}'::jsonb,NULL,0,%s)""",
                (session_id, row[0], str(uuid4()), datetime.now(timezone.utc)))
        except psycopg.errors.UniqueViolation:
            collision_rejected = True
        require(collision_rejected, "occupied transcript sequence accepted a second record")
        require(compact_json(collision_before) == compact_json(self.durable_image(session_id)), "sequence collision changed durable rows")
        print("PASS DU05-C03 sequence-collision rejected atomically", flush=True)

        # Cross-session source reference in a summary record must be detected
        # by the next target write. The fixture is removed in all outcomes.
        other_goal = self.sql("SELECT message_id::text FROM agent_session_transcript WHERE session_id=%s AND kind='message' ORDER BY sequence LIMIT 1", (other_id,))[0][0]
        next_seq = self.sql("SELECT COALESCE(MAX(sequence),0)+1 FROM agent_session_transcript WHERE session_id=%s", (session_id,))[0][0]
        summary_id = str(uuid4())
        summary_payload = {"source_message_ids": [other_goal], "previous_ledger_id": None, "summary": "integrity fixture"}
        session_state = self.sql("SELECT state FROM agent_sessions WHERE session_id=%s", (session_id,))[0][0]
        state_before_insert = session_state
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO agent_session_transcript
                    (session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at)
                    VALUES (%s,%s,%s,'compaction_summary',%s,NULL,0,%s)""",
                    (session_id, next_seq, summary_id, Jsonb(summary_payload), datetime.now(timezone.utc)))
                cursor.execute("UPDATE agent_sessions SET state=jsonb_set(state,'{next_transcript_sequence}',to_jsonb(%s::bigint),true) WHERE session_id=%s", (next_seq + 1, session_id))
        cross_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, cross_image, "cross-session-summary-source")
        finally:
            with psycopg.connect(self.database_url) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM agent_session_transcript WHERE session_id=%s AND message_id=%s", (session_id, summary_id))
                    cursor.execute("UPDATE agent_sessions SET state=%s WHERE session_id=%s", (Jsonb(state_before_insert), session_id))
        print("PASS DU05-C03 cross-session provenance fixture restored", flush=True)

        # An absent prior-ledger reference is invalid; use a source from this
        # session so the case isolates the ledger-chain invariant.
        next_seq = self.sql("SELECT COALESCE(MAX(sequence),0)+1 FROM agent_session_transcript WHERE session_id=%s", (session_id,))[0][0]
        bad_prior_id = str(uuid4())
        bad_prior_state = self.sql("SELECT state FROM agent_sessions WHERE session_id=%s", (session_id,))[0][0]
        bad_prior_payload = {"source_message_ids": [row[1]], "previous_ledger_id": str(uuid4()), "summary": "integrity fixture"}
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO agent_session_transcript
                    (session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at)
                    VALUES (%s,%s,%s,'compaction_summary',%s,NULL,0,%s)""",
                    (session_id, next_seq, bad_prior_id, Jsonb(bad_prior_payload), datetime.now(timezone.utc)))
                cursor.execute("UPDATE agent_sessions SET state=jsonb_set(state,'{next_transcript_sequence}',to_jsonb(%s::bigint),true) WHERE session_id=%s", (next_seq + 1, session_id))
        prior_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, prior_image, "inconsistent-previous-ledger")
        finally:
            with psycopg.connect(self.database_url) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM agent_session_transcript WHERE session_id=%s AND message_id=%s", (session_id, bad_prior_id))
                    cursor.execute("UPDATE agent_sessions SET state=%s WHERE session_id=%s", (Jsonb(bad_prior_state), session_id))

        # Immutable goal and config anchors are stored separately from active
        # transcript records and must not be silently accepted on the next save.
        original_state = self.sql("SELECT state FROM agent_sessions WHERE session_id=%s", (session_id,))[0][0]
        self.sql("UPDATE agent_sessions SET state=jsonb_set(state,'{goal}',to_jsonb('tampered goal'::text),true) WHERE session_id=%s", (session_id,))
        goal_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, goal_image, "changed-goal-anchor")
        finally:
            self.sql("UPDATE agent_sessions SET state=%s WHERE session_id=%s", (Jsonb(original_state), session_id))

        original_hash = self.sql("SELECT config_sha256 FROM agent_sessions WHERE session_id=%s", (session_id,))[0][0]
        self.sql("UPDATE agent_sessions SET config_sha256='tampered-config-hash' WHERE session_id=%s", (session_id,))
        config_image = self.durable_image(session_id)
        try:
            self.assert_rejected_atomically(session_id, config_image, "changed-config-anchor")
        finally:
            self.sql("UPDATE agent_sessions SET config_sha256=%s WHERE session_id=%s", (original_hash, session_id))

        print("PASS DU05-C03 corruption fixtures restored", flush=True)


def main() -> int:
    env = load_env_file()
    base_url = env.get("AGENT_URL", "").strip()
    database_url = env.get("AGENT_SESSION_DATABASE_URL", "").strip()
    if not base_url or not database_url:
        print("FAIL: set AGENT_URL and AGENT_SESSION_DATABASE_URL in repository .env", file=sys.stderr)
        return 2
    if not RUN_FILE.is_file():
        print("FAIL: run ./test-cases.sh first; archive/curl-validation/current-run.txt is missing", file=sys.stderr)
        return 2
    evidence = Path(RUN_FILE.read_text(encoding="utf-8").strip())
    if not evidence.is_absolute():
        evidence = ROOT / evidence
    if not evidence.is_dir():
        print(f"FAIL: shared curl evidence directory is missing: {evidence}", file=sys.stderr)
        return 2
    EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
    run_dir = EVIDENCE_ROOT / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=False)
    log_file = (run_dir / "persist.log").open("w", encoding="utf-8")
    original_stdout, original_stderr = sys.stdout, sys.stderr

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, value: str) -> None:
            for stream in self.streams:
                stream.write(value)
                stream.flush()

        def flush(self) -> None:
            for stream in self.streams:
                stream.flush()

    sys.stdout = Tee(original_stdout, log_file)
    sys.stderr = Tee(original_stderr, log_file)
    print(f"Target URL: {base_url}\nShared curl evidence: {evidence}\nPersistence evidence: {run_dir}", flush=True)
    checks = PersistenceChecks(base_url, database_url, evidence)
    failures = 0
    try:
        for label, phase in (("C02", checks.c02), ("C03", checks.c03)):
            try:
                phase()
            except Exception as error:
                failures += 1
                print(f"FAIL DU05-{label}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
    finally:
        checks.close()
        sys.stdout, sys.stderr = original_stdout, original_stderr
        log_file.close()
        print(f"Persistence evidence: {run_dir}", flush=True)
    if failures:
        print(f"FAIL: {failures} canonical persistence phase(s) failed; see {run_dir / 'persist.log'}", file=sys.stderr, flush=True)
        return 1
    print("PASS: shared Python DU05-C02/C03 persistence checks completed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
