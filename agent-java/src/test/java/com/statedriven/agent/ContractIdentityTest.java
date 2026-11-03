package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import org.junit.jupiter.api.Test;

class ContractIdentityTest {
  @Test
  void allSharedJcsVectorsMatchExactBytesAndHashes() throws Exception {
    var vectors = parse(Files.readString(Path.of("../schema/idempotency-vectors.json")));
    for (var v : vectors.path("actions")) {
      var key =
          arr("agent-action-v1", v.path("namespace"), v.path("session_id"), v.path("execution_id"));
      var payload =
          arr("agent-payload-v1", v.path("action_type"), v.path("target"), v.path("arguments"));
      assertEquals(
          v.path("canonical_key").asText(), new String(canonical(key), StandardCharsets.UTF_8));
      assertEquals(v.path("expected_key").asText(), hash(key));
      assertEquals(
          v.path("canonical_payload").asText(),
          new String(canonical(payload), StandardCharsets.UTF_8));
      assertEquals(v.path("expected_payload_sha256").asText(), hash(payload));
    }
    for (var v : vectors.path("commands")) {
      var request = arr("agent-command-v1", v.path("method"), v.path("route"), v.path("request"));
      assertEquals(
          v.path("canonical_request").asText(),
          new String(canonical(request), StandardCharsets.UTF_8));
      assertEquals(v.path("expected_sha256").asText(), hash(request));
    }
  }

  @Test
  void strictInputsRejectDuplicateNonfiniteUnicodeAndUnsafeIntegers() {
    for (String raw : new String[] {"{\"a\":1,\"a\":2}", "NaN", "1e999", "\"\\ud800\""})
      assertThrows(IllegalArgumentException.class, () -> parse(raw));
    assertThrows(IllegalArgumentException.class, () -> canonical(parse("9007199254740992")));
  }

  @Test
  void keyAndPayloadHaveIndependentIdentity() {
    String key =
        actionKey(
            "deployment",
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222");
    assertEquals(64, key.length());
    assertNotEquals(
        payloadHash("tool", "target", obj("n", 1)), payloadHash("tool", "target", obj("n", 2)));
    assertNotEquals(
        key,
        actionKey(
            "other",
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222"));
  }

  @Test
  void malformedUuidRejectedInsteadOfJdkShortForm() {
    assertThrows(ApiFailure.class, () -> uuid("1-1-1-1-1"));
  }

  @Test
  void requestKeyBoundToRouteAndNormalizedBody() {
    var body = obj("max_steps", null);
    var row =
        obj("request_sha256", hash(arr("agent-command-v1", "POST", "/sessions/id/run", body)));
    CommandStore.matches(row, "/sessions/id/run", body);
    assertThrows(ApiFailure.class, () -> CommandStore.matches(row, "/sessions/id/resume", body));
    assertThrows(
        ApiFailure.class, () -> CommandStore.matches(row, "/sessions/id/run", obj("max_steps", 2)));
    for (String key : new String[] {"", "bad key", "雪", "x".repeat(256)})
      assertThrows(ApiFailure.class, () -> CommandStore.key(key));
  }
}
