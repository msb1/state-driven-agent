package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import org.junit.jupiter.api.Test;

class CrossLanguageProjectionTest {
  @Test
  void exportsOfflineProjectionForReferenceValidation() throws Exception {
    var config = RuntimeRegressionTest.config();
    var state = DurableState.create("goal & <scope> 雪", "test.yaml", "sha", config);
    state.put("id", "11111111-1111-4111-8111-111111111111");
    var message = (com.fasterxml.jackson.databind.node.ObjectNode) state.path("memory").get(0);
    message.put("message_id", "22222222-2222-4222-8222-222222222222");
    message.put("created_at", "2026-11-03T12:00:00.123000Z");
    state
        .with("completion_artifacts")
        .set(
            "0",
            obj(
                "tool",
                "inspect_log",
                "arguments",
                obj("start_line", 1, "line_count", 2),
                "tool_call_id",
                "test-call",
                "step",
                0,
                "value",
                1e-7));
    var snapshot = DurableState.hot(state);
    DurableState.validateHot(snapshot);
    var value =
        obj(
            "config",
            config,
            "state",
            DurableState.publicState(state),
            "snapshot",
            snapshot,
            "progress_fingerprint",
            AgentEngine.progressFingerprint(state, config),
            "source_sha256",
            sha(
                SessionRepository.source(
                    state.path("goal").asText(), AgentEngine.wires(state.path("memory")), null)));
    Files.writeString(Path.of("target/cross-language-projection.json"), encoded(value));
    assertEquals(64, value.path("progress_fingerprint").asText().length());
  }
}
