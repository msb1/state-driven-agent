package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

/**
 * Scripted JDBC failure boundaries exercise the actual aggregate SQL writer, without a database.
 */
class LedgerTransactionTest {
  static final class Database {
    final Connection connection = mock(Connection.class);
    final ObjectNode state;
    final List<String> writes = new ArrayList<>();
    final List<Map<Integer, Object>> receiptWrites = new ArrayList<>();
    String failAt;
    boolean loseAck, committed, visibleAfterLock;
    long latestRevision = 1;
    final AtomicInteger commits = new AtomicInteger();

    Database(ObjectNode state) throws Exception {
      this.state = state;
      when(connection.prepareStatement(anyString()))
          .thenAnswer(
              inv -> {
                String sql = inv.getArgument(0);
                PreparedStatement statement = mock(PreparedStatement.class);
                Map<Integer, Object> params = new HashMap<>();
                doAnswer(
                        set -> {
                          params.put(set.getArgument(0), set.getArgument(1));
                          return null;
                        })
                    .when(statement)
                    .setObject(anyInt(), any());
                doAnswer(
                        set -> {
                          params.put(set.getArgument(0), set.getArgument(1));
                          return null;
                        })
                    .when(statement)
                    .setString(anyInt(), any());
                when(statement.executeUpdate())
                    .thenAnswer(
                        call -> {
                          mutation(sql, params);
                          return 1;
                        });
                when(statement.executeQuery())
                    .thenAnswer(
                        call -> {
                          if (sql.startsWith("INSERT")) mutation(sql, params);
                          return result(query(sql));
                        });
                return statement;
              });
      doAnswer(
              inv -> {
                commits.incrementAndGet();
                committed = true;
                if (loseAck) {
                  loseAck = false;
                  throw new SQLException("lost commit acknowledgement", "08006");
                }
                return null;
              })
          .when(connection)
          .commit();
    }

    void mutation(String sql, Map<Integer, Object> params) throws SQLException {
      if (failAt != null && sql.contains(failAt))
        throw new SQLException("injected failure", "08006");
      writes.add(sql);
      if (sql.contains("INSERT INTO agent_commit_receipts"))
        receiptWrites.add(new HashMap<>(params));
    }

    List<Map<String, Object>> query(String sql) {
      if (sql.contains("pg_advisory_xact_lock") && visibleAfterLock) committed = true;
      if (sql.startsWith("SELECT * FROM agent_commit_receipts")) {
        if (!committed || receiptWrites.isEmpty()) return List.of();
        var p = receiptWrites.getLast();
        return List.of(
            map("command_id", p.get(3), "fingerprint", p.get(4), "committed_revision", p.get(6)));
      }
      if (sql.startsWith("SELECT revision FROM agent_sessions"))
        return List.of(map("revision", latestRevision));
      if (sql.startsWith("SELECT event_id FROM agent_session_events")
          || sql.startsWith("INSERT INTO agent_session_events"))
        return List.of(map("event_id", 1L));
      if (sql.contains("coalesce(max(sequence)")) return List.of(map("n", 1L));
      if (sql.startsWith("SELECT payload,sequence,created_at")) {
        JsonNode m = state.path("memory").get(0);
        return List.of(
            map(
                "payload",
                DurableState.wire(m),
                "sequence",
                m.path("sequence").asLong(),
                "created_at",
                OffsetDateTime.parse(m.path("created_at").asText())));
      }
      return List.of();
    }

    static Map<String, Object> map(Object... pairs) {
      Map<String, Object> result = new LinkedHashMap<>();
      for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
      return result;
    }

    static ResultSet result(List<Map<String, Object>> values) throws Exception {
      ResultSet result = mock(ResultSet.class);
      ResultSetMetaData meta = mock(ResultSetMetaData.class);
      List<String> keys =
          values.isEmpty() ? List.of() : new ArrayList<>(values.getFirst().keySet());
      AtomicInteger index = new AtomicInteger(-1);
      when(result.next()).thenAnswer(inv -> index.incrementAndGet() < values.size());
      when(result.getMetaData()).thenReturn(meta);
      when(meta.getColumnCount()).thenReturn(keys.size());
      when(meta.getColumnLabel(anyInt())).thenAnswer(inv -> keys.get((int) inv.getArgument(0) - 1));
      when(meta.getColumnTypeName(anyInt()))
          .thenAnswer(
              inv -> {
                Object v =
                    values
                        .get(Math.max(0, index.get()))
                        .get(keys.get((int) inv.getArgument(0) - 1));
                return v instanceof JsonNode
                    ? "jsonb"
                    : v instanceof OffsetDateTime
                        ? "timestamptz"
                        : v instanceof Number ? "int8" : "text";
              });
      when(result.getObject(anyInt()))
          .thenAnswer(inv -> values.get(index.get()).get(keys.get((int) inv.getArgument(0) - 1)));
      when(result.getObject(anyInt(), eq(OffsetDateTime.class)))
          .thenAnswer(inv -> values.get(index.get()).get(keys.get((int) inv.getArgument(0) - 1)));
      return result;
    }
  }

  static final class Harness {
    final ObjectNode config = RuntimeRegressionTest.config(),
        state = DurableState.create("goal", "test.yaml", "sha", config);
    final CommandStore commands = mock(CommandStore.class);
    final CommandStore.Command command = mock(CommandStore.Command.class);
    final Database db;
    final SessionRepository repository;

    Harness() throws Exception {
      state.put("pending_commit_id", uuid());
      when(command.id()).thenReturn("11111111-1111-4111-8111-111111111111");
      command.terminal = true;
      command.responseStatus = 200;
      when(commands.namespace()).thenReturn("test");
      db = new Database(state);
      repository =
          new SessionRepository(new MockEnvironment(), commands) {
            @Override
            Connection connect() {
              return db.connection;
            }
          };
    }

    void save() {
      repository.save(state, config, obj("type", "session_created"), command);
    }
  }

  @Test
  void completeAggregateCommitsExactlyOneReceiptAndAcknowledgesOutboxes() throws Exception {
    Harness h = new Harness();
    h.save();
    assertEquals(1, h.state.path("revision").asInt());
    assertTrue(h.state.path("transcript_pending").isEmpty());
    assertEquals(1, h.db.receiptWrites.size());
    assertTrue(h.db.writes.stream().anyMatch(s -> s.contains("agent_session_transcript")));
    assertTrue(h.db.writes.stream().anyMatch(s -> s.contains("agent_session_context")));
    assertTrue(h.db.writes.stream().anyMatch(s -> s.contains("agent_session_events")));
    assertEquals(1, h.db.commits.get());
    verify(h.db.connection, never()).rollback();
    verify(h.commands).complete(any(), eq(h.command), eq(200), any(), eq("succeeded"));
  }

  @Test
  void everyAggregateWriteFailureRollsBackWithoutAcknowledging() throws Exception {
    for (String stage :
        List.of(
            "INSERT INTO agent_sessions",
            "UPDATE agent_commands SET session_id",
            "INSERT INTO agent_session_transcript",
            "INSERT INTO agent_session_context",
            "INSERT INTO agent_session_events",
            "INSERT INTO agent_commit_receipts")) {
      Harness h = new Harness();
      h.db.failAt = stage;
      assertThrows(SqlStore.StoreFailure.class, h::save, stage);
      assertEquals(0, h.state.path("revision").asInt());
      assertFalse(h.state.path("transcript_pending").isEmpty());
      assertEquals(0, h.db.commits.get());
      verify(h.db.connection, times(3)).rollback();
    }
  }

  @Test
  void lostCommitAckUsesReceiptInsteadOfRepeatingWrites() throws Exception {
    Harness h = new Harness();
    h.db.loseAck = true;
    h.save();
    assertEquals(1, h.db.receiptWrites.size());
    assertEquals(1, h.state.path("revision").asInt());
    assertTrue(h.state.path("transcript_pending").isEmpty());
    verify(h.commands, times(1)).complete(any(), eq(h.command), anyInt(), any(), anyString());
  }

  @Test
  void oldReceiptAcknowledgesOriginalRevisionAndRequiresReload() throws Exception {
    Harness h = new Harness();
    ObjectNode old = h.state.deepCopy();
    h.save();
    h.db.latestRevision = 2;
    h.repository.save(old, h.config, obj("type", "session_created"), h.command);
    assertEquals(1, old.path("revision").asInt());
    assertTrue(old.path("needs_reload").asBoolean());
    assertEquals(1, h.db.receiptWrites.size());
    assertThrows(
        ApiFailure.class, () -> h.repository.save(old, h.config, obj("type", "other"), h.command));
  }

  @Test
  void sameCommitIdChangedProposalIsRejectedBeforeRevisionHandling() throws Exception {
    Harness h = new Harness();
    ObjectNode changed = h.state.deepCopy();
    h.save();
    changed.put("step_count", 99);
    assertThrows(
        IllegalStateException.class,
        () -> h.repository.save(changed, h.config, obj("type", "session_created"), h.command));
    assertEquals(1, h.db.receiptWrites.size());
  }

  @Test
  void receiptBecomingVisibleWhileWaitingForLockPrecedesAuthorityConflict() throws Exception {
    Harness h = new Harness();
    ObjectNode retry = h.state.deepCopy();
    h.save();
    h.db.committed = false;
    h.db.visibleAfterLock = true;
    clearInvocations(h.commands);
    h.repository.save(retry, h.config, obj("type", "session_created"), h.command);
    assertEquals(1, retry.path("revision").asInt());
    assertEquals(1, h.db.receiptWrites.size());
    verify(h.commands, never()).guard(any(), any());
  }
}
