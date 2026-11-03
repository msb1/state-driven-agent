package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

class RetentionRegressionTest {
  static Path archive() throws Exception {
    Path path = Files.createTempFile("java-archive-", ".jsonl");
    ObjectNode
        header =
            obj(
                "format",
                "sda-archive-2",
                "archive_id",
                "22222222-2222-4222-8222-222222222222",
                "session_id",
                "11111111-1111-4111-8111-111111111111",
                "snapshot_revision",
                1),
        record =
            obj(
                "table",
                "agent_sessions",
                "row",
                obj("session_id", header.path("session_id"), "revision", 1));
    MessageDigest digest = RetentionAdmin.digest();
    digest.update(RetentionAdmin.line(header));
    digest.update(RetentionAdmin.line(record));
    ObjectNode counts = obj(), hashes = obj();
    for (String table : RetentionAdmin.TABLES.keySet()) {
      counts.put(table, table.equals("agent_sessions") ? 1 : 0);
      MessageDigest tableHash = RetentionAdmin.digest();
      if (table.equals("agent_sessions")) tableHash.update(RetentionAdmin.line(record));
      hashes.put(table, RetentionAdmin.hex(tableHash));
    }
    ObjectNode manifest =
        obj(
            "sha256",
            RetentionAdmin.hex(digest),
            "counts",
            counts,
            "table_sha256",
            hashes,
            "watermarks",
            obj("events", 0, "diagnostics", obj()));
    Files.writeString(
        path,
        new String(RetentionAdmin.line(header), StandardCharsets.UTF_8)
            + new String(RetentionAdmin.line(record), StandardCharsets.UTF_8)
            + new String(RetentionAdmin.line(obj("manifest", manifest)), StandardCharsets.UTF_8));
    return path;
  }

  @Test
  void verifiesCanonicalArchiveManifestAndProtectedFamilies() throws Exception {
    Path path = archive();
    ObjectNode verified = RetentionAdmin.verify(path);
    assertEquals(1, verified.at("/counts/agent_sessions").asInt());
    for (String table :
        java.util.List.of(
            "agent_commands",
            "agent_commit_receipts",
            "agent_action_attempts",
            "agent_session_transcript",
            "agent_tool_executions")) assertTrue(verified.path("table_sha256").has(table));
  }

  @Test
  void tamperTruncationTrailingDataAndCrossSessionRowsAreRejected() throws Exception {
    for (String change : java.util.List.of("tamper", "truncate", "trailing", "cross-session")) {
      Path path = archive();
      String text = Files.readString(path);
      text =
          switch (change) {
            case "tamper" -> text.replace("\"revision\":1", "\"revision\":2");
            case "truncate" -> text.substring(0, text.lastIndexOf("{\"manifest\""));
            case "trailing" -> text + "{}\n";
            default ->
                text.replace(
                    "\"row\":{\"revision\":1,\"session_id\":\"11111111-1111-4111-8111-111111111111\"}",
                    "\"row\":{\"revision\":1,\"session_id\":\"33333333-3333-4333-8333-333333333333\"}");
          };
      Files.writeString(path, text);
      assertThrows(IllegalArgumentException.class, () -> RetentionAdmin.verify(path), change);
    }
  }

  @Test
  void activeSessionsCannotAuthorizePruning() throws Exception {
    try (var h = new RuntimeRegressionTest.Harness(RuntimeRegressionTest.config())) {
      org.mockito.Mockito.doReturn(obj("n", 0))
          .when(h.repo)
          .queryOne(
              org.mockito.ArgumentMatchers.anyString(),
              org.mockito.ArgumentMatchers.any(Object[].class));
      assertFalse(
          RetentionAdmin.plan(h.repo, h.s.path("id").asText(), 32, 100)
              .path("eligible")
              .asBoolean());
      assertThrows(
          IllegalArgumentException.class,
          () -> RetentionAdmin.plan(h.repo, h.s.path("id").asText(), 31, 100));
    }
  }
}
