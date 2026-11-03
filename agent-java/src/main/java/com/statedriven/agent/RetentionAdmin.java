package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static com.statedriven.agent.SqlStore.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

final class RetentionAdmin {
  static final Map<String, String> TABLES = new LinkedHashMap<>();

  static {
    TABLES.put("agent_sessions", "session_id");
    TABLES.put("agent_session_context", "session_id");
    TABLES.put("agent_session_transcript", "sequence");
    TABLES.put("agent_session_events", "event_id");
    TABLES.put("agent_session_diagnostics", "family,ordinal");
    TABLES.put("agent_runs", "started_at,run_id");
    TABLES.put("agent_tool_executions", "execution_id");
    TABLES.put("agent_session_calls", "call_id,message_id");
    TABLES.put("agent_archive_receipts", "archive_id");
    TABLES.put("agent_commands", "created_at,command_id");
    TABLES.put("agent_commit_receipts", "committed_revision");
    TABLES.put("agent_action_attempts", "execution_id,attempt_number");
  }

  static byte[] line(JsonNode value) {
    return (encoded(value) + "\n").getBytes(StandardCharsets.UTF_8);
  }

  static MessageDigest digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static String hex(MessageDigest d) {
    return HexFormat.of().formatHex(d.digest());
  }

  static ObjectNode plan(SessionRepository repo, String sid, int keepDiagnostics, int keepEvents) {
    if (keepDiagnostics < 32 || keepEvents < 1)
      throw new IllegalArgumentException("Retain at least 32 diagnostics and one event");
    var p = repo.load(sid);
    if (p == null) throw new ApiFailure(404, "session_not_found", "Session not found");
    ObjectNode s = p.state();
    long
        unresolved =
            repo.queryOne(
                    "SELECT count(*) AS n FROM agent_tool_executions WHERE session_id=?::uuid AND status IN ('pending','started','uncertain','blocked')",
                    sid)
                .path("n")
                .asLong(),
        running =
            repo.queryOne(
                    "SELECT count(*) AS n FROM agent_runs WHERE session_id=?::uuid AND status='running'",
                    sid)
                .path("n")
                .asLong();
    return obj(
        "session_id",
        sid,
        "revision",
        s.path("revision"),
        "default_policy",
        "keep_all",
        "eligible",
        !s.path("status").asText().equals("active")
            && !s.path("run_status").asText().equals("running")
            && s.path("pending_tool_calls").isEmpty()
            && unresolved == 0
            && running == 0,
        "unresolved_executions",
        unresolved,
        "running_lifecycles",
        running,
        "keep_diagnostics",
        keepDiagnostics,
        "keep_events",
        keepEvents,
        "protected_tables",
        TABLES.keySet().stream()
            .filter(t -> !Set.of("agent_session_events", "agent_session_diagnostics").contains(t))
            .toList(),
        "prunable_tables",
        List.of("agent_session_events", "agent_session_diagnostics"),
        "requires",
        "Verified fresh export, terminal session, ownership/fencing, matching revision and database digests");
  }

  static ObjectNode export(SessionRepository repo, String sid, Path path) {
    repo.read(
        c -> {
          c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          try (var statement = c.createStatement()) {
            statement.execute("SET LOCAL TIME ZONE 'UTC'");
          }
          ObjectNode session =
              one(c, "SELECT revision FROM agent_sessions WHERE session_id=?::uuid", sid);
          if (session == null) throw new IllegalArgumentException("Unknown session");
          ObjectNode header =
              obj(
                  "format",
                  "sda-archive-2",
                  "archive_id",
                  uuid(),
                  "session_id",
                  sid,
                  "snapshot_revision",
                  session.path("revision"));
          var digest = digest();
          Map<String, MessageDigest> hashes = new LinkedHashMap<>();
          ObjectNode counts = obj(), watermarks = obj("events", 0, "diagnostics", obj());
          for (String table : TABLES.keySet()) {
            hashes.put(table, digest());
            counts.put(table, 0);
          }
          try (OutputStream out =
              Files.newOutputStream(
                  path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            try {
              Files.setPosixFilePermissions(
                  path, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
            }
            byte[] bytes = line(header);
            out.write(bytes);
            digest.update(bytes);
            for (var table : TABLES.entrySet()) {
              try (var statement =
                  statement(
                      c,
                      "SELECT to_jsonb(t) AS row FROM "
                          + table.getKey()
                          + " t WHERE session_id=?::uuid ORDER BY "
                          + table.getValue(),
                      sid)) {
                statement.setFetchSize(100);
                try (var rs = statement.executeQuery()) {
                  while (rs.next()) {
                    JsonNode row = parse(rs.getString(1));
                    ObjectNode value = obj("table", table.getKey(), "row", row);
                    bytes = line(value);
                    out.write(bytes);
                    digest.update(bytes);
                    hashes.get(table.getKey()).update(bytes);
                    counts.put(table.getKey(), counts.path(table.getKey()).asLong() + 1);
                    watermark(watermarks, table.getKey(), row);
                  }
                }
              }
            }
            ObjectNode tableHashes = obj();
            hashes.forEach((k, v) -> tableHashes.put(k, hex(v)));
            out.write(
                line(
                    obj(
                        "manifest",
                        obj(
                            "sha256",
                            hex(digest),
                            "counts",
                            counts,
                            "table_sha256",
                            tableHashes,
                            "watermarks",
                            watermarks))));
            out.flush();
          }
          try (var file = java.nio.channels.FileChannel.open(path, StandardOpenOption.WRITE)) {
            file.force(true);
          }
          return null;
        });
    return verify(path);
  }

  static void watermark(ObjectNode watermarks, String table, JsonNode row) {
    if (table.equals("agent_session_events"))
      watermarks.put(
          "events", Math.max(watermarks.path("events").asLong(), row.path("event_id").asLong()));
    if (table.equals("agent_session_diagnostics")) {
      String family = row.path("family").asText();
      if (!DurableState.FAMILIES.contains(family))
        throw new IllegalArgumentException("Invalid diagnostic family");
      ObjectNode d = (ObjectNode) watermarks.path("diagnostics");
      d.put(family, Math.max(d.path(family).asLong(), row.path("ordinal").asLong()));
    }
  }

  static ObjectNode verify(Path path) {
    try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String raw = reader.readLine();
      ObjectNode header = (ObjectNode) parse(raw);
      if (header.size() != 4
          || !header.path("format").asText().equals("sda-archive-2")
          || !header.path("snapshot_revision").isIntegralNumber()
          || header.path("snapshot_revision").asLong() < 1)
        throw new IllegalArgumentException("Invalid archive header");
      uuid(header.path("session_id").asText());
      uuid(header.path("archive_id").asText());
      MessageDigest digest = digest();
      digest.update(line(header));
      Map<String, MessageDigest> hashes = new LinkedHashMap<>();
      ObjectNode counts = obj(), watermarks = obj("events", 0, "diagnostics", obj());
      for (String table : TABLES.keySet()) {
        hashes.put(table, digest());
        counts.put(table, 0);
      }
      ObjectNode manifest = null;
      while ((raw = reader.readLine()) != null) {
        if (manifest != null) throw new IllegalArgumentException("Trailing archive data");
        JsonNode value = parse(raw);
        if (value.has("manifest")) {
          manifest = (ObjectNode) value.path("manifest");
          continue;
        }
        String table = value.path("table").asText();
        if (!TABLES.containsKey(table)
            || !value.path("row").path("session_id").equals(header.path("session_id")))
          throw new IllegalArgumentException("Unknown or cross-session archive record");
        byte[] bytes = line(value);
        digest.update(bytes);
        hashes.get(table).update(bytes);
        counts.put(table, counts.path(table).asLong() + 1);
        watermark(watermarks, table, value.path("row"));
      }
      ObjectNode tableHashes = obj();
      hashes.forEach((k, v) -> tableHashes.put(k, hex(v)));
      if (manifest == null
          || manifest.size() != 4
          || !manifest.path("sha256").asText().equals(hex(digest))
          || !equal(manifest.path("counts"), counts)
          || !equal(manifest.path("table_sha256"), tableHashes)
          || !equal(manifest.path("watermarks"), watermarks))
        throw new IllegalArgumentException("Archive checksum/count mismatch or truncation");
      header.setAll(manifest);
      header.put("archive_path", path.toAbsolutePath().toString());
      return header;
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot verify archive", e);
    }
  }

  static ObjectNode prune(
      SessionRepository repo, String sid, Path path, int keepDiagnostics, int keepEvents) {
    ObjectNode manifest = verify(path);
    if (!manifest.path("session_id").asText().equals(sid)
        || !plan(repo, sid, keepDiagnostics, keepEvents).path("eligible").asBoolean())
      throw new IllegalArgumentException("Archive/session not eligible for retention");
    for (ObjectNode prior :
        repo.query(
            "SELECT sha256,manifest FROM agent_archive_receipts WHERE session_id=?::uuid", sid))
      if (!verify(Path.of(prior.at("/manifest/archive_path").asText()))
          .path("sha256")
          .equals(prior.path("sha256")))
        throw new IllegalArgumentException("Prior archive dependency unavailable");
    ObjectNode row =
        repo.commandStore().register(
            uuid(),
            "/admin/retention/prune",
            obj("session_id", sid, "archive_id", manifest.path("archive_id")),
            obj("session_id", sid));
    var cmd = repo.commandStore().claim(row);
    try (cmd;
        var execution = new ExecutionCoordinator(repo, sid, cmd)) {
      ObjectNode s = execution.state;
      if (s.path("revision").asLong() != manifest.path("snapshot_revision").asLong())
        throw new IllegalArgumentException("Stale archive; export current revision again");
      cmd.terminal = true;
      cmd.responseMessage = "Telemetry archived.";
      s.put("execution_maintenance", true);
      s.set("last_archive_id", manifest.path("archive_id"));
      manifest.put("keep_diagnostics", keepDiagnostics);
      manifest.put("keep_events", keepEvents);
      s.set("retention_pending", manifest);
      repo.save(
          s,
          execution.config,
          obj("type", "telemetry_archived", "archive_id", manifest.path("archive_id")),
          cmd);
      return obj(
          "session_id",
          sid,
          "revision",
          s.path("revision"),
          "archive_id",
          manifest.path("archive_id"));
    }
  }

  static void write(SessionRepository repo, Connection c, ObjectNode s) throws SQLException {
    JsonNode item = s.path("retention_pending");
    String sid = s.path("id").asText();
    if (!s.hasNonNull("execution_owner")
        || s.path("status").asText().equals("active")
        || !s.path("pending_tool_calls").isEmpty()
        || item.path("snapshot_revision").asLong() != s.path("revision").asLong() - 1
        || item.path("keep_diagnostics").asInt() < 32
        || item.path("keep_events").asInt() < 1)
      throw new IllegalArgumentException("Unsafe retention request");
    if (one(
                    c,
                    "SELECT count(*) AS n FROM agent_tool_executions WHERE session_id=?::uuid AND status IN ('pending','started','uncertain','blocked')",
                    sid)
                .path("n")
                .asLong()
            != 0
        || one(
                    c,
                    "SELECT count(*) AS n FROM agent_runs WHERE session_id=?::uuid AND status='running'",
                    sid)
                .path("n")
                .asLong()
            != 0) throw new IllegalArgumentException("Unresolved lifecycle prevents pruning");
    try (var sql = c.createStatement()) {
      sql.execute("SET LOCAL TIME ZONE 'UTC'");
    }
    for (String table : List.of("agent_session_events", "agent_session_diagnostics")) {
      String condition =
          table.equals("agent_session_events")
              ? "event_id<=?"
              : "ordinal<=coalesce((?::jsonb->>family)::bigint,0)";
      Object bound =
          table.equals("agent_session_events")
              ? item.at("/watermarks/events").asLong()
              : item.at("/watermarks/diagnostics");
      MessageDigest hash = digest();
      for (ObjectNode row :
          rows(
              c,
              "SELECT to_jsonb(t) AS row FROM "
                  + table
                  + " t WHERE session_id=?::uuid AND "
                  + condition
                  + " ORDER BY "
                  + TABLES.get(table),
              sid,
              bound)) hash.update(line(obj("table", table, "row", row.path("row"))));
      if (!hex(hash).equals(item.path("table_sha256").path(table).asText()))
        throw new IllegalArgumentException("Export differs from current telemetry; nothing pruned");
    }
    update(
        c,
        "INSERT INTO agent_archive_receipts(archive_id,session_id,snapshot_revision,sha256,manifest) VALUES (?::uuid,?::uuid,?,?,?::jsonb)",
        item.path("archive_id").asText(),
        sid,
        item.path("snapshot_revision").asLong(),
        item.path("sha256").asText(),
        item);
    for (String family : DurableState.FAMILIES) {
      long cutoff =
          Math.min(
              item.path("watermarks").path("diagnostics").path(family).asLong(),
              s.path("diagnostic_counts").path(family).asLong()
                  - item.path("keep_diagnostics").asLong());
      update(
          c,
          "DELETE FROM agent_session_diagnostics WHERE session_id=?::uuid AND family=? AND ordinal<=?",
          sid,
          family,
          cutoff);
    }
    update(
        c,
        "DELETE FROM agent_session_events WHERE session_id=?::uuid AND event_id<=? AND event_id NOT IN(SELECT event_id FROM agent_session_events WHERE session_id=?::uuid ORDER BY event_id DESC LIMIT ?)",
        sid,
        item.at("/watermarks/events").asLong(),
        sid,
        item.path("keep_events").asInt());
  }
}
