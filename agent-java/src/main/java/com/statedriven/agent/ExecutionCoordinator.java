package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static com.statedriven.agent.SqlStore.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

final class ExecutionCoordinator implements AutoCloseable {
  final SessionRepository repo;
  final CommandStore.Command command;
  final ObjectNode config;
  final String owner = uuid(), sid;
  final long fence;
  ObjectNode state;
  volatile Throwable lost;
  final ScheduledFuture<?> heartbeat;
  Consumer<ObjectNode> emit;
  java.util.function.BooleanSupplier stopRequested = () -> false;

  ExecutionCoordinator(SessionRepository repo, String sid, CommandStore.Command cmd) {
    this.repo = repo;
    this.sid = sid;
    this.command = cmd;
    fence = repo.acquire(sid, owner);
    var loaded = repo.load(sid);
    state = loaded.state();
    config = loaded.config();
    state.put("execution_owner", owner);
    state.put("execution_fence", fence);
    heartbeat =
        CommandStore.HEARTBEATS.scheduleAtFixedRate(
            () -> {
              try {
                repo.renew(sid, owner, fence);
              } catch (Throwable e) {
                lost = e;
              }
            },
            10,
            10,
            TimeUnit.SECONDS);
  }

  public void close() {
    heartbeat.cancel(false);
    try {
      repo.release(sid, owner, fence);
    } finally {
      state.remove("execution_owner");
      state.remove("execution_fence");
    }
  }

  void checkpoint(boolean cancel) {
    if (lost != null || command.lost != null)
      throw new ApiFailure(409, "lease_lost", "Heartbeat failed");
    if (state.path("needs_reload").asBoolean())
      throw new ApiFailure(409, "revision_conflict", "Reload required");
    ObjectNode r =
        repo.queryOne(
            "SELECT lease_owner=?::uuid AND lease_fence=? AND lease_expires_at>clock_timestamp() AS valid,EXISTS(SELECT 1 FROM agent_runs WHERE run_id=?::uuid AND session_id=?::uuid AND cancel_requested) AS cancelled FROM agent_sessions WHERE session_id=?::uuid",
            owner,
            fence,
            state.hasNonNull("run_id") ? state.path("run_id").asText() : null,
            sid,
            sid);
    if (r == null || !r.path("valid").asBoolean())
      throw new ApiFailure(409, "lease_lost", "Session lease lost");
    if (cancel && (r.path("cancelled").asBoolean() || stopRequested.getAsBoolean()))
      throw new Cancelled();
  }

  void persist(ObjectNode event) {
    checkpoint(false);
    if (!state.path("status").asText().equals("active")
        && state.path("run_status").asText().equals("running")) {
      lifecycle(
          state.path("status").asText().equals("complete") ? "complete" : "failed",
          state.path("last_error").path("code").asText(state.path("status").asText()));
    }
    repo.save(state, config, event, command);
    event.put("command_id", command.id());
    event.set("run_id", state.path("run_id"));
    if (emit != null) emit.accept(event);
  }

  void lifecycle(String status, String reason) {
    state.put("run_status", status);
    state.put("run_ended_at", DurableState.now());
    state.put("run_termination_reason", reason);
    DurableState.record(state, "run_finished", obj("status", status, "reason", reason));
  }

  void start() {
    if (state.hasNonNull("run_id") && state.path("run_status").asText().equals("running")) {
      lifecycle("interrupted", "lease_expired_or_worker_disconnected");
      repo.save(state, config, obj("type", "run_interrupted"), command);
    }
    state.put("run_id", uuid());
    state.put("run_command_id", command.id());
    state.put("run_status", "running");
    state.put("run_started_at", DurableState.now());
    state.putNull("run_ended_at");
    state.putNull("run_termination_reason");
    state.put("run_count", state.path("run_count").asInt() + 1);
    DurableState.record(
        state, "run_started", obj("fence", fence, "disconnect_policy", "cancel_at_boundary"));
    persist(obj("type", "run_started", "run_id", state.path("run_id")));
  }

  void finish(String status, String reason) {
    if (state.path("run_status").asText().equals(status) && state.hasNonNull("run_ended_at"))
      return;
    lifecycle(status, reason);
    repo.save(
        state, config, obj("type", "run_finished", "status", status, "reason", reason), command);
  }

  static final class Cancelled extends RuntimeException {}

  static final class Blocked extends RuntimeException {
    Blocked(String reason) {
      super(reason);
    }
  }

  static final class TransientToolError extends RuntimeException {
    TransientToolError(String reason) {
      super(reason);
    }
  }

  static final class RecipientConflict extends RuntimeException {
    RecipientConflict(String reason) {
      super(reason);
    }
  }

  interface Adapter {
    String target();

    String scope();

    Long retentionSeconds();

    String invoke(JsonNode args, String key);

    Verification verify(JsonNode args, String key);
  }

  record Verification(String outcome, String result, JsonNode evidence, String reference) {}

  static final Map<String, Adapter> ADAPTERS = new ConcurrentHashMap<>();

  static ObjectNode policy(JsonNode tool) {
    return tool.path("recovery").isObject()
        ? (ObjectNode) tool.path("recovery").deepCopy()
        : obj(
            "mode",
            FixtureTools.NAMES.contains(tool.path("name").asText())
                    && !tool.path("name").asText().equals("fixture_idempotent_write")
                ? "read_only"
                : "reconcile",
            "max_attempts",
            2);
  }

  String invoke(ObjectNode call, FixtureTools tools) {
    checkpoint(true);
    JsonNode declaration = null;
    for (JsonNode t : config.path("tools"))
      if (t.path("name").asText().equals(call.path("name").asText())) declaration = t;
    if (declaration == null) throw new IllegalArgumentException("Unknown tool");
    String name = call.path("name").asText(), callId = call.path("id").asText();
    ObjectNode policy = policy(declaration);
    Adapter adapter = ADAPTERS.get(name);
    String target = adapter == null ? "tool://" + name : adapter.target();
    if (adapter != null && policy.path("mode").asText().equals("idempotent"))
      policy.set(
          "recipient",
          obj(
              "target",
              adapter.target(),
              "key_scope",
              adapter.scope(),
              "retention_seconds",
              adapter.retentionSeconds()));
    ObjectNode row =
        repo.queryOne(
            "SELECT payload,created_at FROM agent_tool_executions WHERE session_id=?::uuid AND call_id=?",
            sid,
            callId);
    if (row == null) {
      String eid = uuid();
      row =
          obj(
              "execution_id",
              eid,
              "run_id",
              state.path("run_id"),
              "command_id",
              command.id(),
              "namespace",
              repo.commandStore().namespace(),
              "target",
              target,
              "key_algorithm",
              "agent-action-v1",
              "call_id",
              callId,
              "tool",
              name,
              "arguments",
              call.path("arguments").deepCopy(),
              "policy",
              policy,
              "attempts",
              0,
              "status",
              "pending",
              "result",
              null,
              "created_at",
              DurableState.now(),
              "idempotency_key",
              actionKey(repo.commandStore().namespace(), sid, eid),
              "payload_sha256",
              payloadHash(name, target, call.path("arguments")));
      stage(row);
      DurableState.record(state, "tool_intent", row.deepCopy());
      persist(obj("type", "tool_intent", "execution_id", eid, "tool_call_id", callId));
    } else row = (ObjectNode) row.path("payload").deepCopy();
    if (!row.path("tool").asText().equals(name)
        || !equal(row.path("arguments"), call.path("arguments")))
      throw new Blocked("Frozen action differs");
    if (Set.of("succeeded", "failed").contains(row.path("status").asText()))
      return row.path("result").asText();
    if (!equal(row.path("policy"), policy)
        || !row.path("target").asText().equals(target)
        || !row.path("namespace").asText().equals(repo.commandStore().namespace()))
      throw new Blocked("Frozen policy or recipient capability differs");
    String mode = policy.path("mode").asText();
    if (Set.of("started", "uncertain", "blocked").contains(row.path("status").asText())
        && adapter != null) {
      ObjectNode attempt = startAttempt(row, "verify");
      try {
        Verification v =
            adapter.verify(row.path("arguments").deepCopy(), row.path("idempotency_key").asText());
        if (v == null
            || !Set.of("succeeded", "failed", "not_executed", "unknown").contains(v.outcome()))
          throw new IllegalArgumentException("Invalid authoritative verification");
        endAttempt(attempt, "succeeded", v.evidence(), v.reference(), null);
        if (Set.of("succeeded", "failed").contains(v.outcome())) {
          settle(row, v.result(), v.outcome());
          return v.result();
        }
        persist(
            obj(
                "type",
                "tool_verified",
                "execution_id",
                row.path("execution_id"),
                "outcome",
                v.outcome()));
        if (mode.equals("reconcile") && !v.outcome().equals("not_executed"))
          block(row, "External outcome unknown");
      } catch (Blocked e) {
        throw e;
      } catch (Exception e) {
        endAttempt(attempt, "uncertain", null, null, obj("type", e.getClass().getSimpleName()));
        block(row, "Verification could not establish outcome");
      }
    } else if (Set.of("started", "uncertain", "blocked").contains(row.path("status").asText())
        && mode.equals("reconcile")) block(row, "Authoritative reconciliation required");
    if (mode.equals("idempotent") && adapter == null)
      block(row, "No registered recipient enforces this key");
    while (row.path("attempts").asInt() < policy.path("max_attempts").asInt()) {
      checkpoint(true);
      if (mode.equals("idempotent")
          && adapter.retentionSeconds() != null
          && java.time.Duration.between(
                      java.time.OffsetDateTime.parse(row.path("created_at").asText()),
                      java.time.OffsetDateTime.now())
                  .getSeconds()
              >= adapter.retentionSeconds())
        block(row, "Recipient deduplication guarantee expired");
      row.put("attempts", row.path("attempts").asInt() + 1);
      row.put("status", "started");
      stage(row);
      ObjectNode attempt = startAttempt(row, "dispatch");
      String result;
      try {
        result =
            mode.equals("idempotent")
                ? adapter.invoke(
                    row.path("arguments").deepCopy(), row.path("idempotency_key").asText())
                : tools.execute(name, row.path("arguments").deepCopy()).block();
        if (result == null) throw new IllegalStateException("Non-string result");
        endAttempt(
            attempt,
            result.startsWith("ERROR:") || result.startsWith("AssertionError:")
                ? "failed"
                : "succeeded",
            obj("result", result),
            null,
            null);
      } catch (Exception error) {
        if (error instanceof RecipientConflict) {
          endAttempt(attempt, "failed", null, null, obj("type", "payload_conflict"));
          block(row, "Recipient key identifies a different payload");
        }
        endAttempt(attempt, "uncertain", null, null, obj("type", error.getClass().getSimpleName()));
        row.put("status", "uncertain");
        stage(row);
        persist(
            obj(
                "type",
                "tool_attempt_uncertain",
                "execution_id",
                row.path("execution_id"),
                "attempt",
                row.path("attempts")));
        if ((mode.equals("idempotent") || error instanceof TransientToolError)
            && !mode.equals("reconcile")
            && row.path("attempts").asInt() < policy.path("max_attempts").asInt()) continue;
        if (Set.of("reconcile", "idempotent").contains(mode))
          block(row, "No definitive external outcome; retry limit reached");
        result = "ERROR: tool execution failed.";
      }
      checkpoint(false);
      settle(row, result, null);
      return result;
    }
    block(row, "Attempt limit exhausted");
    return null;
  }

  void stage(ObjectNode row) {
    state.withArray("execution_pending").add(row.deepCopy());
  }

  void settle(ObjectNode row, String result, String status) {
    if (result == null) throw new Blocked("Verification result missing");
    row.put(
        "status",
        status != null
            ? status
            : result.startsWith("ERROR:") || result.startsWith("AssertionError:")
                ? "failed"
                : "succeeded");
    row.put("result", result);
    stage(row);
    DurableState.record(
        state,
        "tool_outcome",
        obj(
            "execution_id",
            row.path("execution_id"),
            "status",
            row.path("status"),
            "result",
            result));
  }

  void block(ObjectNode row, String reason) {
    row.put("status", "blocked");
    stage(row);
    DurableState.record(
        state, "tool_uncertain", obj("execution_id", row.path("execution_id"), "reason", reason));
    persist(
        obj("type", "tool_uncertain", "execution_id", row.path("execution_id"), "message", reason));
    throw new Blocked(reason);
  }

  ObjectNode startAttempt(ObjectNode row, String kind) {
    long number =
        repo.queryOne(
                "SELECT coalesce(max(attempt_number),0)+1 AS n FROM agent_action_attempts WHERE execution_id=?::uuid",
                row.path("execution_id").asText())
            .path("n")
            .asLong();
    ObjectNode a =
        obj(
            "attempt_id",
            uuid(),
            "execution_id",
            row.path("execution_id"),
            "command_id",
            command.id(),
            "attempt_number",
            number,
            "kind",
            kind,
            "status",
            "started",
            "payload_sha256",
            row.path("payload_sha256"),
            "owner",
            owner,
            "fence",
            fence,
            "started_at",
            DurableState.now());
    state.withArray("attempts_pending").add(a.deepCopy());
    DurableState.record(state, "tool_attempt", a.deepCopy());
    persist(
        obj(
            "type",
            kind.equals("dispatch") ? "tool_started" : "tool_verifying",
            "execution_id",
            row.path("execution_id"),
            "attempt",
            row.path("attempts"),
            "attempt_number",
            number));
    checkpoint(true);
    return a;
  }

  void endAttempt(
      ObjectNode a, String status, JsonNode evidence, String reference, JsonNode error) {
    a.put("status", status);
    a.put("finished_at", DurableState.now());
    a.set("evidence", tree(evidence));
    a.set("external_reference", tree(reference));
    a.set("error", tree(error));
    state.withArray("attempts_pending").add(a.deepCopy());
    DurableState.record(state, "tool_attempt_settled", a.deepCopy());
  }

  static void write(SessionRepository repo, Connection c, ObjectNode s, CommandStore.Command cmd)
      throws SQLException {
    String sid = s.path("id").asText();
    if (s.hasNonNull("execution_owner")
        && !s.path("execution_maintenance").asBoolean()
        && s.hasNonNull("run_id")
        && s.hasNonNull("run_started_at")) {
      if (update(
              c,
              "INSERT INTO agent_runs(run_id,session_id,command_id,owner,fence,status,started_at,ended_at,termination_reason,last_decision_step) VALUES (?::uuid,?::uuid,?::uuid,?::uuid,?,?,?::timestamptz,?::timestamptz,?,?) ON CONFLICT(run_id) DO UPDATE SET status=excluded.status,ended_at=excluded.ended_at,termination_reason=excluded.termination_reason,last_decision_step=excluded.last_decision_step WHERE agent_runs.session_id=excluded.session_id AND agent_runs.command_id=excluded.command_id",
              s.path("run_id").asText(),
              sid,
              s.path("run_command_id").asText(),
              s.path("execution_owner").asText(),
              s.path("execution_fence").asLong(),
              s.path("run_status").asText(),
              s.path("run_started_at").asText(),
              s.hasNonNull("run_ended_at") ? s.path("run_ended_at").asText() : null,
              s.hasNonNull("run_termination_reason")
                  ? s.path("run_termination_reason").asText()
                  : null,
              s.path("step_count").asLong())
          != 1) throw new IllegalStateException("Run origin command or session changed");
    }
    for (JsonNode x : s.path("execution_pending")) {
      if (!s.hasNonNull("execution_owner")
          || x.path("attempts").asInt() < 0
          || !x.path("namespace").asText().equals(repo.commandStore().namespace())
          || !x.path("idempotency_key")
              .asText()
              .equals(actionKey(x.path("namespace").asText(), sid, x.path("execution_id").asText()))
          || !x.path("payload_sha256")
              .asText()
              .equals(
                  payloadHash(
                      x.path("tool").asText(), x.path("target").asText(), x.path("arguments")))
          || x.path("attempts").asInt() > x.at("/policy/max_attempts").asInt())
        throw new IllegalStateException("Invalid frozen execution");
      ObjectNode old =
          one(
              c,
              "SELECT payload FROM agent_tool_executions WHERE session_id=?::uuid AND call_id=? FOR UPDATE",
              sid,
              x.path("call_id").asText());
      if (old != null) {
        JsonNode prior = old.path("payload");
        for (String k :
            List.of(
                "execution_id",
                "run_id",
                "command_id",
                "call_id",
                "tool",
                "arguments",
                "policy",
                "namespace",
                "target",
                "idempotency_key",
                "payload_sha256",
                "key_algorithm"))
          if (!equal(prior.path(k), x.path(k)))
            throw new IllegalStateException("Action identity changed");
        if (x.path("attempts").asInt() < prior.path("attempts").asInt()
            || x.path("attempts").asInt() > prior.path("attempts").asInt() + 1
            || Set.of("succeeded", "failed").contains(prior.path("status").asText())
                && (!prior.path("result").equals(x.path("result"))
                    || !prior.path("status").equals(x.path("status"))
                    || !prior.path("attempts").equals(x.path("attempts"))))
          throw new IllegalStateException("Action outcome or attempts changed");
      }
      if (Set.of("succeeded", "failed").contains(x.path("status").asText())) {
        int found = 0;
        for (JsonNode m : s.path("memory"))
          if (m.path("role").asText().equals("tool")
              && m.path("tool_call_id").equals(x.path("call_id"))
              && m.path("content").equals(x.path("result"))
              && m.path("name").equals(x.path("tool"))) found++;
        for (JsonNode p : s.path("pending_tool_calls"))
          if (p.path("id").equals(x.path("call_id")))
            throw new IllegalStateException("Settled call remains pending");
        if (found != 1) throw new IllegalStateException("Uncorrelated execution result");
      }
      update(
          c,
          "INSERT INTO agent_tool_executions(execution_id,session_id,run_id,call_id,tool,arguments,policy,command_id,namespace,target,idempotency_key,payload_sha256,key_algorithm,attempts,status,result,owner,fence,payload) VALUES (?::uuid,?::uuid,?::uuid,?,?,?::jsonb,?::jsonb,?::uuid,?,?,?,?,?,?,?, ?,?::uuid,?,?::jsonb) ON CONFLICT(session_id,call_id) DO UPDATE SET attempts=excluded.attempts,status=excluded.status,result=excluded.result,owner=excluded.owner,fence=excluded.fence,payload=excluded.payload,updated_at=clock_timestamp()",
          x.path("execution_id").asText(),
          sid,
          x.path("run_id").asText(),
          x.path("call_id").asText(),
          x.path("tool").asText(),
          x.path("arguments"),
          x.path("policy"),
          x.path("command_id").asText(),
          x.path("namespace").asText(),
          x.path("target").asText(),
          x.path("idempotency_key").asText(),
          x.path("payload_sha256").asText(),
          x.path("key_algorithm").asText(),
          x.path("attempts").asInt(),
          x.path("status").asText(),
          x.hasNonNull("result") ? x.path("result").asText() : null,
          s.path("execution_owner").asText(),
          s.path("execution_fence").asLong(),
          x);
      update(
          c,
          "UPDATE agent_tool_executions SET started_at=CASE WHEN status='started' THEN coalesce(started_at,clock_timestamp()) ELSE started_at END,finished_at=CASE WHEN status IN ('succeeded','failed') THEN coalesce(finished_at,clock_timestamp()) ELSE NULL END WHERE execution_id=?::uuid",
          x.path("execution_id").asText());
    }
    for (JsonNode a : s.path("attempts_pending")) {
      if (!a.path("command_id").asText().equals(cmd.id())
          || !a.path("owner").equals(s.path("execution_owner"))
          || !equal(a.path("fence"), s.path("execution_fence")))
        throw new IllegalStateException("Attempt lacks current authority");
      ObjectNode action =
          one(
              c,
              "SELECT payload_sha256 FROM agent_tool_executions WHERE session_id=?::uuid AND execution_id=?::uuid",
              sid,
              a.path("execution_id").asText());
      if (action == null || !action.path("payload_sha256").equals(a.path("payload_sha256")))
        throw new IllegalStateException("Attempt differs from frozen action payload");
      ObjectNode prior =
          one(
              c,
              "SELECT * FROM agent_action_attempts WHERE execution_id=?::uuid AND attempt_number=? FOR UPDATE",
              a.path("execution_id").asText(),
              a.path("attempt_number").asInt());
      if (prior != null) {
        for (String k :
            List.of("attempt_id", "payload_sha256", "command_id", "owner", "fence", "kind"))
          if (!equal(prior.path(k), a.path(k)))
            throw new IllegalStateException("Attempt identity changed");
        if (!java.time.OffsetDateTime.parse(prior.path("started_at").asText())
            .isEqual(java.time.OffsetDateTime.parse(a.path("started_at").asText())))
          throw new IllegalStateException("Attempt start time changed");
        if (!prior.path("status").asText().equals("started")) {
          for (String k : List.of("status", "external_reference", "evidence", "error"))
            if (!equal(prior.path(k), a.path(k)))
              throw new IllegalStateException("Attempt outcome changed");
          if (!java.time.OffsetDateTime.parse(prior.path("finished_at").asText())
              .isEqual(java.time.OffsetDateTime.parse(a.path("finished_at").asText())))
            throw new IllegalStateException("Attempt finish time changed");
        }
      } else if (one(
                  c,
                  "SELECT coalesce(max(attempt_number),0)+1 AS n FROM agent_action_attempts WHERE execution_id=?::uuid",
                  a.path("execution_id").asText())
              .path("n")
              .asInt()
          != a.path("attempt_number").asInt()) throw new IllegalStateException("Attempt gap");
      update(
          c,
          "INSERT INTO agent_action_attempts(attempt_id,session_id,execution_id,command_id,attempt_number,kind,status,payload_sha256,owner,fence,external_reference,evidence,error,started_at,finished_at) VALUES (?::uuid,?::uuid,?::uuid,?::uuid,?,?,?,?,?::uuid,?,?,?::jsonb,?::jsonb,?::timestamptz,?::timestamptz) ON CONFLICT(execution_id,attempt_number) DO UPDATE SET status=excluded.status,external_reference=excluded.external_reference,evidence=excluded.evidence,error=excluded.error,finished_at=excluded.finished_at",
          a.path("attempt_id").asText(),
          sid,
          a.path("execution_id").asText(),
          a.path("command_id").asText(),
          a.path("attempt_number").asInt(),
          a.path("kind").asText(),
          a.path("status").asText(),
          a.path("payload_sha256").asText(),
          a.path("owner").asText(),
          a.path("fence").asLong(),
          a.hasNonNull("external_reference") ? a.path("external_reference").asText() : null,
          a.path("evidence").isMissingNode() ? tree(null) : a.path("evidence"),
          a.path("error").isMissingNode() ? tree(null) : a.path("error"),
          a.path("started_at").asText(),
          a.hasNonNull("finished_at") ? a.path("finished_at").asText() : null);
    }
  }
}
