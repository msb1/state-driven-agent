package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static com.statedriven.agent.SqlStore.*;

import com.fasterxml.jackson.databind.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
final class FixtureRecipient implements ExecutionCoordinator.Adapter {
  final SqlStore store;
  final CommandStore commands;

  FixtureRecipient(Environment env, CommandStore commands) {
    this.store = new SqlStore(env, "AGENT_FIXTURE_DATABASE_URL");
    this.commands = commands;
    ExecutionCoordinator.ADAPTERS.put("fixture_idempotent_write", this);
  }

  public String target() {
    return "fixture://action-receipts";
  }

  public String scope() {
    return "deployment";
  }

  public Long retentionSeconds() {
    return null;
  }

  public String invoke(JsonNode args, String key) {
    return store.transaction(
        c -> {
          String fingerprint = payloadHash("fixture_idempotent_write", target(), args);
          JsonNode result = obj("effect_id", key, "value", args.path("value"));
          update(
              c,
              "INSERT INTO agent_fixture_action_receipts(namespace,idempotency_key,payload_sha256,request,result) VALUES (?,?,?,?::jsonb,?::jsonb) ON CONFLICT(namespace,idempotency_key) DO NOTHING",
              commands.namespace(),
              key,
              fingerprint,
              args,
              result);
          var row =
              one(
                  c,
                  "SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=? AND idempotency_key=? FOR UPDATE",
                  commands.namespace(),
                  key);
          if (!row.path("payload_sha256").asText().equals(fingerprint))
            throw new ExecutionCoordinator.RecipientConflict(
                "Recipient key identifies different payload");
          return new String(canonical(row.path("result")), java.nio.charset.StandardCharsets.UTF_8);
        });
  }

  public ExecutionCoordinator.Verification verify(JsonNode args, String key) {
    var row =
        store.read(
            c ->
                one(
                    c,
                    "SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=? AND idempotency_key=?",
                    commands.namespace(),
                    key));
    if (row == null)
      return new ExecutionCoordinator.Verification(
          "unknown",
          null,
          obj("source", "recipient-primary", "key", key, "receipt_present", false),
          null);
    if (!row.path("payload_sha256")
        .asText()
        .equals(payloadHash("fixture_idempotent_write", target(), args)))
      return new ExecutionCoordinator.Verification(
          "unknown",
          null,
          obj("source", "recipient-primary", "key", key, "payload_mismatch", true),
          null);
    return new ExecutionCoordinator.Verification(
        "succeeded",
        new String(canonical(row.path("result")), java.nio.charset.StandardCharsets.UTF_8),
        obj("source", "recipient-primary", "key", key, "receipt_present", true),
        key);
  }
}
