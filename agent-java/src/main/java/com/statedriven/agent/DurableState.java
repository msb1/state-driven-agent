package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.time.OffsetDateTime;
import java.util.*;

final class DurableState {
  static final List<String> FAMILIES =
      List.of("compaction_events", "compaction_attempts", "compaction_decisions");

  static String now() {
    return SqlStore.timestamp(
        OffsetDateTime.now(java.time.ZoneOffset.UTC)
            .truncatedTo(java.time.temporal.ChronoUnit.MICROS));
  }

  static ObjectNode create(String goal, String ref, String sha, ObjectNode config) {
    ObjectNode s =
        obj(
            "id",
            uuid(),
            "goal",
            goal,
            "config_ref",
            ref,
            "config_sha256",
            sha,
            "resolved_config_sha256",
            ConfigRepository.resolvedHash(config),
            "revision",
            0,
            "next_transcript_sequence",
            1,
            "history_complete",
            true,
            "run_status",
            "idle",
            "run_id",
            null,
            "run_command_id",
            null,
            "run_started_at",
            null,
            "run_ended_at",
            null,
            "run_termination_reason",
            null,
            "active_ledger_id",
            null,
            "ledger_state",
            null,
            "compacted_context",
            null,
            "final_answer",
            null,
            "pending_tool_calls",
            arr(),
            "memory",
            arr(),
            "transcript_pending",
            arr(),
            "execution_pending",
            arr(),
            "attempts_pending",
            arr(),
            "diagnostics_pending",
            arr(),
            "diagnostic_counts",
            obj(),
            "diagnostics_complete",
            true,
            "diagnostic_history_complete",
            true,
            "diagnostic_rejections",
            0,
            "compaction_savings_total",
            0,
            "compaction_savings_known",
            true,
            "completion_artifacts",
            obj(),
            "consecutive_invalid_responses",
            0,
            "no_progress_steps",
            0,
            "progress_fingerprint",
            null,
            "progress_seen_outputs",
            arr(),
            "last_error",
            null,
            "last_archive_id",
            null,
            "last_compaction_step",
            0,
            "last_compaction_attempt_step",
            null,
            "last_compaction_estimated_tokens",
            null,
            "last_compaction_attempt_sha256",
            null,
            "compaction_skip_counts",
            obj(),
            "compaction_trigger_counts",
            obj(),
            "request_budget",
            null,
            "step_count",
            0,
            "run_count",
            0,
            "status",
            "active",
            "workflow",
            null);
    for (String f : FAMILIES) s.putArray(f);
    if (config.path("workflow").isObject())
      s.set(
          "workflow",
          obj(
              "active_phase",
              config.at("/workflow/entry_phase"),
              "completed_phases",
              arr(),
              "observed_evidence",
              arr(),
              "transitions",
              arr()));
    message(s, obj("role", "user", "content", goal));
    return s;
  }

  static ObjectNode wire(JsonNode m) {
    ObjectNode n = obj();
    for (String key : List.of("role", "content", "name", "tool_call_id", "tool_calls"))
      if (m.hasNonNull(key)) n.set(key, m.get(key));
    return n;
  }

  static ObjectNode record(ObjectNode s, String kind, JsonNode payload) {
    ObjectNode e =
        obj(
            "message_id",
            uuid(),
            "sequence",
            s.path("next_transcript_sequence").asLong(1),
            "kind",
            kind,
            "payload",
            payload,
            "run_id",
            s.path("run_id"),
            "decision_step",
            s.path("step_count").asLong(),
            "created_at",
            now());
    s.put("next_transcript_sequence", e.path("sequence").asLong() + 1);
    s.withArray("transcript_pending").add(e);
    return e;
  }

  static void message(ObjectNode s, ObjectNode payload) {
    ObjectNode e = record(s, "message", wire(payload));
    ObjectNode m =
        obj(
            "role",
            payload.path("role"),
            "content",
            null,
            "name",
            null,
            "tool_call_id",
            null,
            "tool_calls",
            null);
    m.setAll(payload);
    m.set("message_id", e.get("message_id"));
    m.set("sequence", e.get("sequence"));
    m.set("created_at", e.get("created_at"));
    s.withArray("memory").add(m);
  }

  static void diagnostic(ObjectNode s, String family, JsonNode payload) {
    long ordinal = s.with("diagnostic_counts").path(family).asLong() + 1;
    s.with("diagnostic_counts").put(family, ordinal);
    s.withArray("diagnostics_pending")
        .add(obj("family", family, "ordinal", ordinal, "payload", payload));
    ArrayNode values = s.withArray(family);
    values.add(payload);
    while (values.size() > 32) values.remove(0);
    s.put(
        "diagnostics_complete",
        FAMILIES.stream()
            .allMatch(f -> s.path(f).size() == s.path("diagnostic_counts").path(f).asInt()));
    if (family.equals("compaction_attempts")) {
      if (!payload.path("accepted").asBoolean())
        s.put("diagnostic_rejections", s.path("diagnostic_rejections").asInt() + 1);
      else if (payload.has("reduction_tokens"))
        s.put(
            "compaction_savings_total",
            s.path("compaction_savings_total").asLong()
                + payload.path("reduction_tokens").asLong());
    }
  }

  static ObjectNode publicState(ObjectNode s) {
    ObjectNode n = s.deepCopy();
    for (String k :
        List.of(
            "execution_owner",
            "execution_fence",
            "execution_maintenance",
            "execution_pending",
            "attempts_pending",
            "diagnostics_pending",
            "retention_pending",
            "needs_reload",
            "_active_integrity")) n.remove(k);
    return n;
  }

  static ObjectNode hot(ObjectNode s) {
    ObjectNode n = publicState(s);
    for (String k :
        List.of(
            "memory",
            "compacted_context",
            "ledger_state",
            "transcript_pending",
            "pending_commit_id",
            "diagnostics_complete")) n.remove(k);
    for (String f : FAMILIES) n.remove(f);
    ObjectNode anchor =
        obj(
            "version",
            1,
            "snapshot_sha256",
            sha(encoded(n)),
            "context_sha256",
            sha(
                encoded(
                    arr(s.path("memory"), s.path("compacted_context"), s.path("ledger_state")))));
    n.set("_active_integrity", anchor);
    return n;
  }

  static void validateHot(ObjectNode s) {
    ObjectNode n = s.deepCopy();
    JsonNode anchor = n.remove("_active_integrity");
    if (anchor == null
        || anchor.path("version").asInt() != 1
        || !anchor.path("snapshot_sha256").asText().equals(sha(encoded(n))))
      throw new IllegalStateException(
          "Snapshot integrity mismatch; automatic backfill is disabled");
  }

  static void acknowledge(ObjectNode s) {
    for (String f :
        List.of(
            "transcript_pending", "execution_pending", "attempts_pending", "diagnostics_pending"))
      s.putArray(f);
    s.remove("pending_commit_id");
    s.remove("retention_pending");
  }
}
