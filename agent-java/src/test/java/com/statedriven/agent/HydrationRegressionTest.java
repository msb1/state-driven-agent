package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.junit.jupiter.api.Test;

class HydrationRegressionTest {
  static ObjectNode row() {
    ObjectNode c = RuntimeRegressionTest.config(),
        s = DurableState.create("goal", "profile.yaml", "sha", c);
    DurableState.message(s, obj("role", "assistant", "content", "working context"));
    s.put("revision", 1);
    return obj(
        "session_id",
        s.path("id"),
        "storage_version",
        6,
        "state",
        DurableState.hot(s),
        "config",
        c,
        "config_ref",
        s.path("config_ref"),
        "config_sha256",
        s.path("config_sha256"),
        "resolved_config_sha256",
        s.path("resolved_config_sha256"),
        "revision",
        1,
        "context_revision",
        1,
        "goal_message_id",
        s.path("memory").get(0).path("message_id"),
        "raw_message_ids",
        arr(s.path("memory").get(1).path("message_id")),
        "ledger_record_id",
        null,
        "ledger_state",
        null,
        "ledger_payload",
        null,
        "ledger_kind",
        null,
        "next_sequence",
        3,
        "messages",
        s.path("memory"),
        "diagnostics",
        arr());
  }

  @Test
  void oneSnapshotHydratesWithProtectedGoalAndConfig() {
    ObjectNode r = row();
    var persisted = SessionRepository.decode(r, r.path("session_id").asText());
    assertEquals("goal", persisted.state().path("goal").asText());
    assertEquals(2, persisted.state().path("memory").size());
    assertEquals(1, persisted.state().path("revision").asInt());
  }

  @Test
  void rejectsChangedSnapshotMessageContextConfigAndLedgerReferences() {
    for (String kind :
        java.util.List.of(
            "snapshot",
            "message",
            "context_revision",
            "goal_id",
            "raw_ids",
            "config",
            "ledger_id",
            "sequence",
            "schema")) {
      ObjectNode r = row();
      switch (kind) {
        case "snapshot" -> ((ObjectNode) r.path("state")).put("step_count", 99);
        case "message" -> ((ObjectNode) r.path("messages").get(1)).put("content", "tampered");
        case "context_revision" -> r.put("context_revision", 2);
        case "goal_id" -> r.put("goal_message_id", uuid());
        case "raw_ids" -> r.set("raw_message_ids", arr(uuid()));
        case "config" -> ((ObjectNode) r.path("config")).put("system_prompt", "changed");
        case "ledger_id" -> r.put("ledger_record_id", uuid());
        case "sequence" -> r.put("next_sequence", 99);
        default -> r.put("storage_version", 5);
      }
      assertThrows(
          IllegalStateException.class,
          () -> SessionRepository.decode(r, r.path("session_id").asText()),
          kind);
    }
  }

  @Test
  void timestampNormalizationMatchesPythonMicrosecondFormatting() {
    assertEquals(
        "2026-11-03T12:00:00.123000Z",
        SqlStore.timestamp(java.time.OffsetDateTime.parse("2026-11-03T12:00:00.123Z")));
    assertEquals(
        "2026-11-03T12:00:00Z",
        SqlStore.timestamp(java.time.OffsetDateTime.parse("2026-11-03T07:00:00-05:00")));
  }
}
