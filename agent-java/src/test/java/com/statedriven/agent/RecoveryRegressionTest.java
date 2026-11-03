package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Mono;

class RecoveryRegressionTest {
  static class Recipient implements ExecutionCoordinator.Adapter {
    int effects, invocations, verifications;
    boolean loseResponse;
    Long retention;
    String verification = "unknown";
    final Map<String, String> results = new HashMap<>();

    public String target() {
      return "fixture://native-regression";
    }

    public String scope() {
      return "deployment";
    }

    public Long retentionSeconds() {
      return retention;
    }

    public String invoke(JsonNode args, String key) {
      invocations++;
      String result = results.get(key);
      if (result == null) {
        result = "result " + args.path("start_line").asInt();
        results.put(key, result);
        effects++;
        if (loseResponse) {
          loseResponse = false;
          throw new IllegalStateException("lost recipient response");
        }
      }
      return result;
    }

    public ExecutionCoordinator.Verification verify(JsonNode args, String key) {
      verifications++;
      return new ExecutionCoordinator.Verification(
          verification,
          verification.equals("succeeded") ? "verified result" : null,
          obj("authoritative", true, "key", key),
          key);
    }
  }

  @AfterEach
  void cleanup() {
    ExecutionCoordinator.ADAPTERS.remove("inspect_log");
  }

  static ObjectNode config(String mode) {
    ObjectNode c = RuntimeRegressionTest.config();
    ((ObjectNode) c.path("tools").get(0)).set("recovery", obj("mode", mode, "max_attempts", 3));
    return c;
  }

  static ObjectNode prior(RuntimeRegressionTest.Harness h, Recipient adapter) {
    String eid = uuid(), run = uuid(), origin = uuid();
    ObjectNode policy = ExecutionCoordinator.policy(h.c.path("tools").get(0));
    if (policy.path("mode").asText().equals("idempotent"))
      policy.set(
          "recipient",
          obj(
              "target",
              adapter.target(),
              "key_scope",
              adapter.scope(),
              "retention_seconds",
              adapter.retentionSeconds()));
    return obj(
        "execution_id",
        eid,
        "run_id",
        run,
        "command_id",
        origin,
        "namespace",
        "test-deployment",
        "target",
        adapter.target(),
        "key_algorithm",
        "agent-action-v1",
        "call_id",
        "prior",
        "tool",
        "inspect_log",
        "arguments",
        RuntimeRegressionTest.call("prior", 1).path("arguments"),
        "policy",
        policy,
        "attempts",
        1,
        "status",
        "started",
        "result",
        null,
        "created_at",
        "2020-01-01T00:00:00+00:00",
        "idempotency_key",
        actionKey("test-deployment", h.s.path("id").asText(), eid),
        "payload_sha256",
        payloadHash(
            "inspect_log",
            adapter.target(),
            RuntimeRegressionTest.call("prior", 1).path("arguments")));
  }

  static void persisted(RuntimeRegressionTest.Harness h, ObjectNode prior) {
    doReturn(obj("payload", prior, "created_at", prior.path("created_at")))
        .when(h.repo)
        .queryOne(startsWith("SELECT payload,created_at"), any(Object[].class));
    doReturn(obj("n", 2))
        .when(h.repo)
        .queryOne(startsWith("SELECT coalesce(max(attempt_number)"), any(Object[].class));
  }

  @Test
  void lostRecipientResponseRetriesSameKeyWithOneEffect() {
    Recipient adapter = new Recipient();
    adapter.loseResponse = true;
    ExecutionCoordinator.ADAPTERS.put("inspect_log", adapter);
    try (var h = new RuntimeRegressionTest.Harness(config("idempotent"))) {
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(obj("type", "tool").setAll(RuntimeRegressionTest.call("a", 1))),
              Mono.just(obj("type", "final", "content", "done")));
      h.run(3);
      assertEquals(1, adapter.effects);
      assertEquals(2, adapter.invocations);
      assertEquals(1, adapter.results.size());
      assertEquals(
          2, h.attempts.stream().filter(a -> a.path("status").asText().equals("started")).count());
      assertEquals("complete", h.s.path("status").asText());
    }
  }

  @Test
  void authoritativeRecoveryRetainsOriginalActionRunAndOriginCommand() {
    Recipient adapter = new Recipient();
    adapter.verification = "succeeded";
    ExecutionCoordinator.ADAPTERS.put("inspect_log", adapter);
    try (var h = new RuntimeRegressionTest.Harness(config("idempotent"))) {
      ObjectNode row = prior(h, adapter);
      persisted(h, row);
      h.s.withArray("pending_tool_calls").add(RuntimeRegressionTest.call("prior", 1));
      DurableState.message(
          h.s,
          obj(
              "role",
              "assistant",
              "tool_calls",
              arr(
                  obj(
                      "id",
                      "prior",
                      "type",
                      "function",
                      "function",
                      obj("name", "inspect_log", "arguments", row.path("arguments").toString())))));
      when(h.llm.decide(any(), any()))
          .thenReturn(Mono.just(obj("type", "final", "content", "done")));
      h.run(2);
      assertEquals(0, adapter.invocations);
      assertEquals(1, adapter.verifications);
      var settled =
          h.executions.stream()
              .filter(r -> r.path("status").asText().equals("succeeded"))
              .findFirst()
              .orElseThrow();
      for (String field :
          List.of("execution_id", "run_id", "command_id", "idempotency_key", "payload_sha256"))
        assertEquals(row.get(field), settled.get(field));
      assertNotEquals(h.s.path("run_id"), settled.path("run_id"));
      assertEquals(h.x.command.id(), h.attempts.getFirst().path("command_id").asText());
    }
  }

  @Test
  void unknownVerificationAndExpiredGuaranteeBlockDispatch() {
    Recipient adapter = new Recipient();
    adapter.retention = 1L;
    ExecutionCoordinator.ADAPTERS.put("inspect_log", adapter);
    try (var h = new RuntimeRegressionTest.Harness(config("idempotent"))) {
      persisted(h, prior(h, adapter));
      h.x.start();
      assertThrows(
          ExecutionCoordinator.Blocked.class,
          () -> h.x.invoke(RuntimeRegressionTest.call("prior", 1), h.tools));
      assertEquals(1, adapter.verifications);
      assertEquals(0, adapter.invocations);
      verifyNoInteractions(h.tools);
      assertTrue(h.executions.stream().anyMatch(e -> e.path("status").asText().equals("blocked")));
    }
  }

  @Test
  void reconcileModeNeverTreatsWeakNegativeAsProofOfNonexecution() {
    Recipient adapter = new Recipient();
    ExecutionCoordinator.ADAPTERS.put("inspect_log", adapter);
    try (var h = new RuntimeRegressionTest.Harness(config("reconcile"))) {
      persisted(h, prior(h, adapter));
      h.x.start();
      assertThrows(
          ExecutionCoordinator.Blocked.class,
          () -> h.x.invoke(RuntimeRegressionTest.call("prior", 1), h.tools));
      verifyNoInteractions(h.tools);
      assertEquals(0, adapter.invocations);
    }
  }

  @Test
  void unsupportedIdempotentAdapterBlocksBeforeEffect() {
    try (var h = new RuntimeRegressionTest.Harness(config("idempotent"))) {
      h.x.start();
      assertThrows(
          ExecutionCoordinator.Blocked.class,
          () -> h.x.invoke(RuntimeRegressionTest.call("a", 1), h.tools));
      verifyNoInteractions(h.tools);
    }
  }

  @Test
  void changedFrozenPayloadBlocksBeforeVerification() {
    Recipient adapter = new Recipient();
    ExecutionCoordinator.ADAPTERS.put("inspect_log", adapter);
    try (var h = new RuntimeRegressionTest.Harness(config("idempotent"))) {
      persisted(h, prior(h, adapter));
      h.x.start();
      assertThrows(
          ExecutionCoordinator.Blocked.class,
          () -> h.x.invoke(RuntimeRegressionTest.call("prior", 2), h.tools));
      assertEquals(0, adapter.verifications);
      assertEquals(0, adapter.invocations);
    }
  }

  @Test
  void onlyExplicitTransientSignalAuthorizesReadOnlyRetry() {
    try (var h = new RuntimeRegressionTest.Harness(config("read_only"))) {
      h.x.start();
      when(h.tools.execute(anyString(), any()))
          .thenReturn(
              Mono.error(new ExecutionCoordinator.TransientToolError("temporary")),
              Mono.just("known result"));
      assertEquals("known result", h.x.invoke(RuntimeRegressionTest.call("a", 1), h.tools));
      verify(h.tools, times(2)).execute(anyString(), any());
    }
    try (var h = new RuntimeRegressionTest.Harness(config("read_only"))) {
      h.x.start();
      when(h.tools.execute(anyString(), any()))
          .thenReturn(Mono.error(new IllegalStateException("unclassified")));
      assertTrue(h.x.invoke(RuntimeRegressionTest.call("b", 1), h.tools).startsWith("ERROR:"));
      verify(h.tools, times(1)).execute(anyString(), any());
    }
  }

  @Test
  void lostOwnershipCannotSettleKnownResultUnderStaleFence() {
    try (var h = new RuntimeRegressionTest.Harness(config("read_only"))) {
      AtomicBoolean valid = new AtomicBoolean(true);
      doAnswer(inv -> obj("valid", valid.get(), "cancelled", false))
          .when(h.repo)
          .queryOne(startsWith("SELECT lease_owner="), any(Object[].class));
      h.x.start();
      when(h.tools.execute(anyString(), any()))
          .thenReturn(
              Mono.fromSupplier(
                  () -> {
                    valid.set(false);
                    return "known result";
                  }));
      ApiFailure failure =
          assertThrows(
              ApiFailure.class, () -> h.x.invoke(RuntimeRegressionTest.call("a", 1), h.tools));
      assertEquals("lease_lost", failure.code);
      assertFalse(
          h.executions.stream().anyMatch(e -> e.path("status").asText().equals("succeeded")));
    }
  }
}
