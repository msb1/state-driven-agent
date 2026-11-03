package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.sql.*;
import java.util.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Repository
class SessionRepository extends SqlStore {
  record Persisted(ObjectNode state, ObjectNode config) {}

  volatile boolean ready;
  final CommandStore commands;

  SessionRepository(Environment env, CommandStore commands) {
    super(env);
    this.commands = commands;
  }

  CommandStore commandStore() { return commands; }

  boolean isReady() { return ready; }

  void markReady() { ready = true; }

  Mono<Void> initialize() {
    return Mono.fromRunnable(
            () -> {
              commands.namespace();
              read(
                  c -> {
                    try {
                      SchemaValidator.validate(c);
                    } catch (Exception error) {
                      throw new IllegalStateException(
                          "Shared schema 6 is missing or incompatible; run initialize_database.py explicitly. The Java runtime does not create or change schema objects.",
                          error);
                    }
                    return null;
                  });
            })
        .subscribeOn(Schedulers.boundedElastic())
        .then();
  }

  static final String HYDRATE =
      """
 SELECT s.*,c.revision AS context_revision,c.goal_message_id,c.raw_message_ids,c.ledger_record_id,c.ledger_state,
 l.payload AS ledger_payload,l.kind AS ledger_kind,
 (SELECT coalesce(max(sequence),0)+1 FROM agent_session_transcript WHERE session_id=s.session_id) AS next_sequence,
 (SELECT jsonb_agg(t.payload||jsonb_build_object('message_id',t.message_id::text,'sequence',t.sequence,'created_at',t.created_at) ORDER BY ids.ord)
 FROM jsonb_array_elements_text(jsonb_build_array(c.goal_message_id::text)||c.raw_message_ids) WITH ORDINALITY ids(id,ord)
 LEFT JOIN agent_session_transcript t ON t.session_id=s.session_id AND t.message_id=ids.id::uuid AND t.kind='message') AS messages,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('family',d.family,'ordinal',d.ordinal,'payload',d.payload) ORDER BY d.family,d.ordinal),'[]'::jsonb)
 FROM (VALUES ('compaction_events'),('compaction_attempts'),('compaction_decisions')) f(family)
 CROSS JOIN LATERAL (SELECT family,ordinal,payload FROM agent_session_diagnostics WHERE session_id=s.session_id AND family=f.family ORDER BY ordinal DESC LIMIT 32) d) AS diagnostics
 FROM agent_sessions s JOIN agent_session_context c USING(session_id)
 LEFT JOIN agent_session_transcript l ON l.session_id=s.session_id AND l.message_id=c.ledger_record_id WHERE s.session_id=?::uuid
 """;

  Persisted load(String sid) {
    return read(c -> load(c, sid));
  }

  Persisted load(Connection c, String sid) throws SQLException {
    ObjectNode r = one(c, HYDRATE, sid);
    if (r == null) return null;
    return decode(r, sid);
  }

  static Persisted decode(ObjectNode r, String sid) {
    if (r.path("storage_version").asInt() != 6)
      throw new IllegalStateException("Schema 6 required; migration disabled");
    ObjectNode s = (ObjectNode) r.path("state").deepCopy(), config = (ObjectNode) r.path("config");
    DurableState.validateHot(s);
    ConfigRepository.validate(config);
    s.set("memory", r.path("messages"));
    for (JsonNode message : s.path("memory")) {
      if (message.isObject()) {
        for (String key : List.of("content", "name", "tool_call_id", "tool_calls"))
          if (!message.has(key)) ((ObjectNode) message).putNull(key);
      }
      if (message.isObject() && message.hasNonNull("created_at"))
        ((ObjectNode) message)
            .put(
                "created_at",
                SqlStore.timestamp(
                    java.time.OffsetDateTime.parse(message.path("created_at").asText())));
    }
    s.set("ledger_state", r.path("ledger_state"));
    s.set(
        "compacted_context",
        r.path("ledger_payload").path("content").isMissingNode()
            ? tree(null)
            : r.path("ledger_payload").path("content"));
    if (!s.at("/_active_integrity/context_sha256")
        .asText()
        .equals(
            sha(
                encoded(
                    arr(s.path("memory"), s.path("compacted_context"), s.path("ledger_state"))))))
      throw new IllegalStateException("Active context integrity mismatch");
    s.remove("_active_integrity");
    for (String family : DurableState.FAMILIES) {
      ArrayNode values = s.putArray(family);
      for (JsonNode d : r.path("diagnostics"))
        if (d.path("family").asText().equals(family)) values.add(d.get("payload"));
    }
    s.put(
        "diagnostics_complete",
        DurableState.FAMILIES.stream()
            .allMatch(f -> s.path(f).size() == s.path("diagnostic_counts").path(f).asLong()));
    for (String key :
        List.of(
            "transcript_pending", "execution_pending", "attempts_pending", "diagnostics_pending"))
      s.putArray(key);
    if (!s.path("id").asText().equals(sid)
        || s.path("revision").asLong() != r.path("revision").asLong()
        || s.path("revision").asLong() != r.path("context_revision").asLong()
        || s.path("next_transcript_sequence").asLong() != r.path("next_sequence").asLong()
        || !s.path("config_ref").equals(r.path("config_ref"))
        || !s.path("config_sha256").equals(r.path("config_sha256"))
        || !s.path("resolved_config_sha256").equals(r.path("resolved_config_sha256"))
        || !s.path("active_ledger_id").equals(r.path("ledger_record_id"))
        || !s.path("resolved_config_sha256").asText().equals(ConfigRepository.resolvedHash(config)))
      throw new IllegalStateException("Snapshot/config/context mismatch");
    validateMemory(s);
    if (!s.path("memory").get(0).path("message_id").equals(r.path("goal_message_id")))
      throw new IllegalStateException("Goal message identity differs");
    ArrayNode ids = arr();
    for (int i = 1; i < s.path("memory").size(); i++)
      ids.add(s.path("memory").get(i).path("message_id"));
    if (!ids.equals(r.path("raw_message_ids")))
      throw new IllegalStateException("Raw context references differ");
    if (s.hasNonNull("active_ledger_id")
        && !Set.of("compaction_summary", "legacy_summary").contains(r.path("ledger_kind").asText()))
      throw new IllegalStateException("Invalid active ledger kind");
    if (r.path("ledger_kind").asText().equals("compaction_summary")
        && !equal(r.path("ledger_payload").path("ledger"), s.path("ledger_state")))
      throw new IllegalStateException("Active ledger state differs from archived ledger");
    return new Persisted(s, config);
  }

  static void validateMemory(ObjectNode s) {
    JsonNode memory = s.path("memory");
    if (!memory.isArray()
        || memory.isEmpty()
        || !memory.get(0).path("role").asText().equals("user")
        || !memory.get(0).path("content").asText().equals(s.path("goal").asText()))
      throw new IllegalStateException("Immutable goal missing");
    Set<String> ids = new HashSet<>();
    long sequence = 0;
    for (JsonNode m : memory) {
      if (!ids.add(m.path("message_id").asText()) || m.path("sequence").asLong() <= sequence)
        throw new IllegalStateException("Invalid active message identities");
      sequence = m.path("sequence").asLong();
    }
  }

  ObjectNode receipt(Connection c, ObjectNode s, CommandStore.Command cmd, String fingerprint)
      throws SQLException {
    ObjectNode r =
        one(
            c,
            "SELECT * FROM agent_commit_receipts WHERE session_id=?::uuid AND commit_id=?::uuid",
            s.path("id").asText(),
            s.path("pending_commit_id").asText());
    if (r == null) return null;
    if (!r.path("command_id").asText().equals(cmd.id())
        || !r.path("fingerprint").asText().equals(fingerprint))
      throw new IllegalStateException("Commit identity reused with changed content");
    ObjectNode latest =
        one(
            c,
            "SELECT revision FROM agent_sessions WHERE session_id=?::uuid",
            s.path("id").asText());
    ObjectNode event =
        one(
            c,
            "SELECT event_id FROM agent_session_events WHERE session_id=?::uuid AND commit_id=?::uuid",
            s.path("id").asText(),
            s.path("pending_commit_id").asText());
    return obj(
        "revision",
        r.path("committed_revision"),
        "needs_reload",
        !latest.path("revision").equals(r.path("committed_revision")),
        "event_id",
        event == null ? null : event.get("event_id"));
  }

  ObjectNode save(ObjectNode s, ObjectNode config, ObjectNode event, CommandStore.Command cmd) {
    if (s.path("needs_reload").asBoolean())
      throw new ApiFailure(409, "revision_conflict", "Reload after historical receipt");
    validateMemory(s);
    if (!s.path("resolved_config_sha256").asText().equals(ConfigRepository.resolvedHash(config)))
      throw new IllegalStateException("Resolved configuration changed");
    if (!s.hasNonNull("pending_commit_id")) s.put("pending_commit_id", uuid());
    ObjectNode e = event == null ? null : event.deepCopy();
    if (e != null) {
      e.put("command_id", cmd.id());
      e.set("run_id", s.path("run_id"));
    }
    String fingerprint =
        hash(
            arr(
                "agent-commit-v2",
                s.path("id"),
                cmd.id(),
                s.path("revision"),
                DurableState.hot(s),
                s.path("memory"),
                s.path("compacted_context"),
                s.path("ledger_state"),
                s.path("transcript_pending"),
                obj(
                    "executions",
                    s.path("execution_pending"),
                    "attempts",
                    s.path("attempts_pending")),
                s.path("diagnostics_pending"),
                s.path("retention_pending").isMissingNode() ? null : s.path("retention_pending"),
                s.path("resolved_config_sha256"),
                e));
    ObjectNode outcome = null;
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        outcome = transaction(c -> saveOnce(c, s, config, e, cmd, fingerprint));
        break;
      } catch (StoreFailure error) {
        if (attempt == 2) throw error;
      }
    }
    s.set("revision", outcome.get("revision"));
    s.put("needs_reload", outcome.path("needs_reload").asBoolean());
    DurableState.acknowledge(s);
    if (event != null && outcome.hasNonNull("event_id"))
      event.set("event_id", outcome.get("event_id"));
    return outcome;
  }

  ObjectNode saveOnce(
      Connection c,
      ObjectNode s,
      ObjectNode config,
      ObjectNode event,
      CommandStore.Command cmd,
      String fingerprint)
      throws Exception {
    ObjectNode old = receipt(c, s, cmd, fingerprint);
    if (old != null) return old;
    one(c, "SELECT pg_advisory_xact_lock(hashtextextended(?,734910003))", s.path("id").asText());
    old = receipt(c, s, cmd, fingerprint);
    if (old != null) return old;
    commands.guard(c, cmd);
    String sid = s.path("id").asText(), commit = s.path("pending_commit_id").asText();
    ObjectNode existing =
        one(c, "SELECT * FROM agent_sessions WHERE session_id=?::uuid FOR UPDATE", sid);
    guard(c, s);
    long expected = s.path("revision").asLong();
    if (existing != null) {
      load(c, sid);
      if (existing.path("revision").asLong() != expected)
        throw new ApiFailure(
            409, "revision_conflict", "Session revision changed; reload before retry");
      if (!existing.path("state").path("goal").equals(s.path("goal"))
          || !existing.path("config_ref").equals(s.path("config_ref"))
          || !existing.path("config_sha256").equals(s.path("config_sha256"))
          || !equal(existing.path("config"), config))
        throw new IllegalStateException("Immutable session configuration or goal changed");
    } else if (expected != 0) throw new ApiFailure(409, "revision_conflict", "Session missing");
    ObjectNode proposed = s.deepCopy();
    proposed.put("revision", expected + 1);
    if (existing == null)
      update(
          c,
          "INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,resolved_config_sha256,revision,last_commit_id,last_commit_sha256,storage_version) VALUES (?::uuid,?::jsonb,?::jsonb,?,?,?, ?,?::uuid,?,6)",
          sid,
          DurableState.hot(proposed),
          config,
          s.path("config_ref").asText(),
          s.path("config_sha256").asText(),
          s.path("resolved_config_sha256").asText(),
          expected + 1,
          commit,
          fingerprint);
    else if (update(
            c,
            "UPDATE agent_sessions SET state=?::jsonb,revision=?,last_commit_id=?::uuid,last_commit_sha256=?,updated_at=clock_timestamp() WHERE session_id=?::uuid AND revision=?",
            DurableState.hot(proposed),
            expected + 1,
            commit,
            fingerprint,
            sid,
            expected)
        != 1) throw new ApiFailure(409, "revision_conflict", "Stale session save");
    if (update(
            c,
            "UPDATE agent_commands SET session_id=?::uuid,status='started',updated_at=clock_timestamp() WHERE command_id=?::uuid AND status IN ('pending','started') AND (session_id IS NULL OR session_id=?::uuid)",
            sid,
            cmd.id(),
            sid)
        != 1) throw new IllegalStateException("Command terminal or wrong session");
    writeTranscript(c, proposed);
    writeDiagnostics(c, proposed);
    writeExecution(c, proposed, cmd);
    ObjectNode eventRow =
        event == null
            ? null
            : one(
                c,
                "INSERT INTO agent_session_events(session_id,event,revision,commit_id,command_id) VALUES (?::uuid,?::jsonb,?,?::uuid,?::uuid) RETURNING event_id",
                sid,
                event,
                expected + 1,
                commit,
                cmd.id());
    writeContext(c, proposed);
    if (proposed.hasNonNull("retention_pending")) RetentionAdmin.write(this, c, proposed);
    update(
        c,
        "INSERT INTO agent_commit_receipts(session_id,commit_id,command_id,fingerprint,expected_revision,committed_revision) VALUES (?::uuid,?::uuid,?::uuid,?,?,?)",
        sid,
        commit,
        cmd.id(),
        fingerprint,
        expected,
        expected + 1);
    guard(c, s);
    commands.guard(c, cmd);
    boolean terminal =
        cmd.terminal
            || (proposed.path("run_command_id").asText().equals(cmd.id())
                && !Set.of("idle", "running").contains(proposed.path("run_status").asText()));
    if (terminal) {
      if (proposed.path("run_status").asText().equals("blocked")
          || (proposed.path("run_status").asText().equals("interrupted")
              && !proposed.path("pending_tool_calls").isEmpty()))
        update(
            c,
            "UPDATE agent_commands SET status=?,owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=?::uuid",
            proposed.path("run_status").asText().equals("blocked") ? "blocked" : "uncertain",
            cmd.id());
      else {
        DurableState.acknowledge(proposed);
        commands.complete(
            c,
            cmd,
            cmd.responseStatus,
            obj(
                "session",
                DurableState.publicState(proposed),
                "message",
                cmd.responseMessage != null
                    ? cmd.responseMessage
                    : proposed.path("final_answer").asText("Run stopped.")),
            proposed.path("status").asText().equals("failed") ? "failed" : "succeeded");
      }
    }
    return obj(
        "revision", expected + 1, "event_id", eventRow == null ? null : eventRow.get("event_id"));
  }

  void writeTranscript(Connection c, ObjectNode s) throws Exception {
    String sid = s.path("id").asText();
    long next =
        one(
                c,
                "SELECT coalesce(max(sequence),0)+1 AS n FROM agent_session_transcript WHERE session_id=?::uuid",
                sid)
            .path("n")
            .asLong();
    for (JsonNode e : s.path("transcript_pending")) {
      if (e.path("sequence").asLong() != next++)
        throw new IllegalStateException("Transcript sequence gap");
      if (Set.of("compaction_summary", "compaction_rejected").contains(e.path("kind").asText()))
        validateSources(c, s, e);
      update(
          c,
          "INSERT INTO agent_session_transcript(session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at) VALUES (?::uuid,?,?::uuid,?,?::jsonb,?::uuid,?,?::timestamptz)",
          sid,
          e.path("sequence").asLong(),
          e.path("message_id").asText(),
          e.path("kind").asText(),
          e.path("payload"),
          e.path("run_id").isNull() ? null : e.path("run_id").asText(),
          e.path("decision_step").asLong(),
          e.path("created_at").asText());
      if (e.path("kind").asText().equals("message"))
        for (JsonNode call : e.at("/payload/tool_calls"))
          update(
              c,
              "INSERT INTO agent_session_calls(session_id,call_id,message_id) VALUES (?::uuid,?,?::uuid)",
              sid,
              call.path("id").asText(),
              e.path("message_id").asText());
    }
    if (next != s.path("next_transcript_sequence").asLong())
      throw new IllegalStateException("Transcript counter mismatch");
  }

  void validateSources(Connection c, ObjectNode s, JsonNode entry) throws SQLException {
    JsonNode p = entry.path("payload");
    ArrayNode messages = arr();
    Set<String> ids = new HashSet<>();
    long sequence = 0;
    for (JsonNode id : p.path("source_message_ids")) {
      if (!ids.add(id.asText())) throw new IllegalStateException("Duplicate summary source");
      ObjectNode row =
          one(
              c,
              "SELECT kind,payload,sequence FROM agent_session_transcript WHERE session_id=?::uuid AND message_id=?::uuid",
              s.path("id").asText(),
              id.asText());
      if (row == null
          || !row.path("kind").asText().equals("message")
          || row.path("sequence").asLong() <= sequence)
        throw new IllegalStateException("Invalid summary source");
      messages.add(row.get("payload"));
      sequence = row.path("sequence").asLong();
    }
    if (ids.isEmpty()) throw new IllegalStateException("Empty summary source");
    String previous = null;
    if (p.hasNonNull("previous_ledger_id")) {
      ObjectNode ledger =
          one(
              c,
              "SELECT kind,payload FROM agent_session_transcript WHERE session_id=?::uuid AND message_id=?::uuid",
              s.path("id").asText(),
              p.path("previous_ledger_id").asText());
      if (ledger == null
          || !Set.of("compaction_summary", "legacy_summary").contains(ledger.path("kind").asText()))
        throw new IllegalStateException("Invalid prior ledger");
      previous = ledger.path("payload").path("content").asText();
    }
    String source = source(s.path("goal").asText(), messages, previous);
    if (!sha(source).equals(p.path("source_sha256").asText()))
      throw new IllegalStateException("Summary source hash mismatch");
  }

  static String source(String goal, JsonNode historical, String previous) {
    return "Goal: "
        + goal
        + "\nPrevious ledger: "
        + (previous == null ? "none" : previous)
        + "\nHistorical prefix:\n"
        + spaced(historical);
  }

  void writeContext(Connection c, ObjectNode s) throws SQLException {
    validateMemory(s);
    String sid = s.path("id").asText();
    ArrayNode raw = arr();
    for (int i = 0; i < s.path("memory").size(); i++) {
      JsonNode m = s.path("memory").get(i);
      ObjectNode row =
          one(
              c,
              "SELECT payload,sequence,created_at FROM agent_session_transcript WHERE session_id=?::uuid AND message_id=?::uuid AND kind='message'",
              sid,
              m.path("message_id").asText());
      if (row == null
          || !equal(row.path("payload"), DurableState.wire(m))
          || !equal(row.path("sequence"), m.path("sequence"))
          || !java.time.OffsetDateTime.parse(row.path("created_at").asText())
              .isEqual(java.time.OffsetDateTime.parse(m.path("created_at").asText())))
        throw new IllegalStateException("Active message changed or missing");
      if (i > 0) raw.add(m.path("message_id"));
    }
    if (s.hasNonNull("active_ledger_id")) {
      ObjectNode ledger =
          one(
              c,
              "SELECT kind,payload FROM agent_session_transcript WHERE session_id=?::uuid AND message_id=?::uuid",
              sid,
              s.path("active_ledger_id").asText());
      if (ledger == null
          || !Set.of("compaction_summary", "legacy_summary").contains(ledger.path("kind").asText())
          || !ledger.path("payload").path("content").equals(s.path("compacted_context"))
          || !equal(ledger.path("payload").path("ledger"), s.path("ledger_state")))
        throw new IllegalStateException("Active ledger changed");
    } else if (s.hasNonNull("compacted_context"))
      throw new IllegalStateException("Unarchived ledger");
    update(
        c,
        "INSERT INTO agent_session_context(session_id,revision,goal_message_id,ledger_record_id,raw_message_ids,ledger_state) VALUES (?::uuid,?,?::uuid,?::uuid,?::jsonb,?::jsonb) ON CONFLICT(session_id) DO UPDATE SET revision=excluded.revision,goal_message_id=excluded.goal_message_id,ledger_record_id=excluded.ledger_record_id,raw_message_ids=excluded.raw_message_ids,ledger_state=excluded.ledger_state",
        sid,
        s.path("revision").asLong(),
        s.path("memory").get(0).path("message_id").asText(),
        s.hasNonNull("active_ledger_id") ? s.path("active_ledger_id").asText() : null,
        raw,
        s.path("ledger_state"));
  }

  void writeDiagnostics(Connection c, ObjectNode s) throws SQLException {
    for (JsonNode d : s.path("diagnostics_pending")) {
      if (!DurableState.FAMILIES.contains(d.path("family").asText()))
        throw new IllegalStateException("Invalid diagnostic family");
      update(
          c,
          "INSERT INTO agent_session_diagnostics(session_id,family,ordinal,payload) VALUES (?::uuid,?,?,?::jsonb)",
          s.path("id").asText(),
          d.path("family").asText(),
          d.path("ordinal").asLong(),
          d.path("payload"));
    }
    var counts = s.path("diagnostic_counts").fields();
    while (counts.hasNext()) {
      var count = counts.next();
      if (!DurableState.FAMILIES.contains(count.getKey())
          || one(
                      c,
                      "SELECT coalesce(max(ordinal),0) AS n FROM agent_session_diagnostics WHERE session_id=?::uuid AND family=?",
                      s.path("id").asText(),
                      count.getKey())
                  .path("n")
                  .asLong()
              != count.getValue().asLong())
        throw new IllegalStateException("Diagnostic counter differs from archive");
    }
  }

  long acquire(String sid, String owner) {
    ObjectNode row =
        transaction(
            c ->
                one(
                    c,
                    "UPDATE agent_sessions SET lease_owner=?::uuid,lease_fence=lease_fence+1,lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE session_id=?::uuid AND (lease_owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING lease_fence",
                    owner,
                    sid));
    if (row == null)
      throw new ApiFailure(409, "ownership_conflict", "Session owned by another worker");
    return row.path("lease_fence").asLong();
  }

  void renew(String sid, String owner, long fence) {
    transaction(
        c -> {
          if (update(
                  c,
                  "UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE session_id=?::uuid AND lease_owner=?::uuid AND lease_fence=? AND lease_expires_at>clock_timestamp()",
                  sid,
                  owner,
                  fence)
              != 1) throw new ApiFailure(409, "lease_lost", "Session lease lost");
          return null;
        });
  }

  void release(String sid, String owner, long fence) {
    transaction(
        c -> {
          update(
              c,
              "UPDATE agent_sessions SET lease_owner=NULL,lease_expires_at=NULL WHERE session_id=?::uuid AND lease_owner=?::uuid AND lease_fence=?",
              sid,
              owner,
              fence);
          return null;
        });
  }

  void guard(Connection c, ObjectNode s) throws SQLException {
    ObjectNode row =
        one(
            c,
            "SELECT lease_owner,lease_fence,lease_expires_at>clock_timestamp() AS valid FROM agent_sessions WHERE session_id=?::uuid",
            s.path("id").asText());
    if (s.hasNonNull("execution_owner")) {
      if (row == null
          || !row.path("lease_owner").equals(s.path("execution_owner"))
          || !row.path("lease_fence").equals(s.path("execution_fence"))
          || !row.path("valid").asBoolean())
        throw new ApiFailure(409, "lease_lost", "Fenced session commit rejected");
    } else if (row != null && row.hasNonNull("lease_owner") && row.path("valid").asBoolean())
      throw new ApiFailure(409, "ownership_conflict", "Session owned by another worker");
  }

  void writeExecution(Connection c, ObjectNode s, CommandStore.Command cmd) throws SQLException {
    ExecutionCoordinator.write(this, c, s, cmd);
  }

  List<ObjectNode> query(String sql, Object... params) {
    return read(c -> rows(c, sql, params));
  }

  ObjectNode queryOne(String sql, Object... params) {
    return read(c -> one(c, sql, params));
  }
}
