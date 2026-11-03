package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static com.statedriven.agent.SqlStore.*;

import com.fasterxml.jackson.databind.node.*;
import java.sql.*;
import java.util.concurrent.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
final class CommandStore extends SqlStore {
  static final ScheduledExecutorService HEARTBEATS =
      Executors.newScheduledThreadPool(
          2,
          r -> {
            Thread t = new Thread(r, "agent-lease-renewal");
            t.setDaemon(true);
            return t;
          });

  CommandStore(Environment env) {
    super(env);
  }

  String namespace() {
    String n = env.getProperty("AGENT_DEPLOYMENT_NAMESPACE", "").strip();
    if (n.isEmpty())
      throw new IllegalStateException("Set a stable AGENT_DEPLOYMENT_NAMESPACE in repository .env");
    return n;
  }

  ObjectNode get(String commandId, String key) {
    return read(
        c ->
            one(
                c,
                "SELECT c.*,r.run_id,r.termination_reason AS recovery_reason FROM agent_commands c LEFT JOIN agent_runs r ON r.command_id=c.command_id WHERE c.namespace=? AND c.client_scope='local' AND ((?::uuid IS NOT NULL AND c.command_id=?::uuid) OR (?::text IS NOT NULL AND c.idempotency_key=?))",
                namespace(),
                commandId,
                commandId,
                key,
                key));
  }

  static void key(String key) {
    if (key == null)
      throw new ApiFailure(422, "idempotency_key_required", "Idempotency-Key is required");
    if (!key.matches("[!-~]{1,255}"))
      throw new ApiFailure(
          422,
          "idempotency_key_invalid",
          "Idempotency-Key must be 1..255 ASCII characters without spaces");
  }

  static void matches(ObjectNode row, String route, ObjectNode body) {
    if (!row.path("request_sha256")
        .asText()
        .equals(hash(arr("agent-command-v1", "POST", route, body))))
      throw new ApiFailure(
          409,
          "idempotency_key_reused",
          "The key identifies different content or a different route");
  }

  ObjectNode register(String key, String route, ObjectNode body, ObjectNode admission) {
    String identity = uuid();
    try {
      transaction(
          c -> {
            update(
                c,
                "INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,status,session_id) VALUES (?::uuid,?,'local',?,'POST',?,?::jsonb,?,'pending',?::uuid) ON CONFLICT(namespace,client_scope,idempotency_key) DO NOTHING",
                identity,
                namespace(),
                key,
                route,
                obj("input", body, "admission", admission),
                hash(arr("agent-command-v1", "POST", route, body)),
                admission.path("session_id").isMissingNode()
                    ? null
                    : admission.path("session_id").asText());
            return null;
          });
    } catch (StoreFailure error) {
      ObjectNode existing = get(null, key);
      if (existing == null) throw error;
    }
    ObjectNode row = get(null, key);
    matches(row, route, body);
    return row;
  }

  Command claim(ObjectNode row) {
    return claim(row, false);
  }

  Command claim(ObjectNode row, boolean recover) {
    String owner = uuid();
    ObjectNode claimed =
        transaction(
            c ->
                one(
                    c,
                    "UPDATE agent_commands SET owner=?::uuid,fence=fence+1,status='started',lease_expires_at=clock_timestamp()+interval '30 seconds',updated_at=clock_timestamp() WHERE command_id=?::uuid AND (status IN ('pending','started') OR (? AND status IN ('uncertain','blocked'))) AND (owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING fence",
                    owner,
                    row.path("command_id").asText(),
                    recover));
    return claimed == null ? null : new Command(row, owner, claimed.path("fence").asLong());
  }

  final class Command implements AutoCloseable {
    final ObjectNode row;
    final String owner;
    final long fence;
    volatile Throwable lost;
    ScheduledFuture<?> heartbeat;
    boolean terminal;
    int responseStatus = 200;
    String responseMessage;

    Command(ObjectNode row, String owner, long fence) {
      this.row = row;
      this.owner = owner;
      this.fence = fence;
      heartbeat =
          HEARTBEATS.scheduleAtFixedRate(
              () -> {
                try {
                  transaction(
                      c -> {
                        if (update(
                                c,
                                "UPDATE agent_commands SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE command_id=?::uuid AND owner=?::uuid AND fence=? AND lease_expires_at>clock_timestamp()",
                                id(),
                                owner,
                                fence)
                            != 1) throw new ApiFailure(409, "lease_lost", "Command lease lost");
                        return null;
                      });
                } catch (Throwable error) {
                  lost = error;
                }
              },
              10,
              10,
              TimeUnit.SECONDS);
    }

    String id() {
      return row.path("command_id").asText();
    }

    public void close() {
      heartbeat.cancel(false);
      transaction(
          c -> {
            update(
                c,
                "UPDATE agent_commands SET owner=NULL,lease_expires_at=NULL WHERE command_id=?::uuid AND owner=?::uuid AND fence=?",
                id(),
                owner,
                fence);
            return null;
          });
    }
  }

  void guard(Connection c, Command command) throws SQLException {
    if (command.lost != null) throw new ApiFailure(409, "lease_lost", "Command heartbeat failed");
    ObjectNode r =
        one(
            c,
            "SELECT owner,fence,lease_expires_at>clock_timestamp() AS valid FROM agent_commands WHERE command_id=?::uuid FOR UPDATE",
            command.id());
    if (r == null
        || !r.path("owner").asText().equals(command.owner)
        || r.path("fence").asLong() != command.fence
        || !r.path("valid").asBoolean())
      throw new ApiFailure(409, "lease_lost", "Fenced command commit rejected");
  }

  void complete(Connection c, Command command, int status, ObjectNode body, String outcome)
      throws SQLException {
    guard(c, command);
    update(
        c,
        "UPDATE agent_commands SET status=?,response_status=?,response_body=?::jsonb,completed_at=clock_timestamp(),updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL WHERE command_id=?::uuid",
        outcome,
        status,
        body,
        command.id());
  }

  void finish(Command command, int status, ObjectNode body, String outcome) {
    transaction(
        c -> {
          complete(c, command, status, body, outcome);
          return null;
        });
  }

  void uncertain(Command cmd) {
    transaction(
        c -> {
          update(
              c,
              "UPDATE agent_commands SET status='uncertain',updated_at=clock_timestamp() WHERE command_id=?::uuid AND owner=?::uuid AND fence=? AND status IN ('pending','started')",
              cmd.id(),
              cmd.owner,
              cmd.fence);
          return null;
        });
  }

  static ObjectNode envelope(ObjectNode row) {
    return obj(
        "command_id",
        row.path("command_id"),
        "session_id",
        row.path("session_id"),
        "run_id",
        row.path("run_id"),
        "status",
        row.path("status"),
        "status_uri",
        "/commands/" + row.path("command_id").asText(),
        "response_status",
        row.path("response_status"),
        "response_body",
        row.path("response_body"),
        "recovery_reason",
        row.path("recovery_reason"));
  }
}
