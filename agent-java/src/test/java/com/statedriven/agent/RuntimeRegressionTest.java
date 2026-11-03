package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.Mono;

class RuntimeRegressionTest {
  static ObjectNode config() {
    ObjectNode c =
        obj(
            "name",
            "regression",
            "model",
            "configured-model",
            "system_prompt",
            "Follow the goal",
            "llm",
            obj("base_url", "http://invalid.local/v1"),
            "memory",
            obj("max_tokens", 20000, "max_steps", 10, "raw_turns_to_keep", 2),
            "tools",
            arr(
                obj(
                    "name",
                    "inspect_log",
                    "description",
                    "Inspect",
                    "parameters",
                    obj(
                        "type",
                        "object",
                        "properties",
                        obj(
                            "start_line",
                            obj("type", "integer"),
                            "line_count",
                            obj("type", "integer")),
                        "required",
                        arr("start_line", "line_count"),
                        "additionalProperties",
                        false))));
    ConfigRepository.normalize(c);
    ConfigRepository.validate(c);
    return c;
  }

  static ObjectNode call(String id, int start) {
    return obj(
        "id", id, "name", "inspect_log", "arguments", obj("start_line", start, "line_count", 2));
  }

  static ObjectNode workflow() {
    return obj(
        "entry_phase",
        "inspect",
        "phases",
        arr(
            obj(
                "id",
                "inspect",
                "name",
                "Inspect",
                "instruction",
                "Inspect once",
                "allowed_tools",
                arr("inspect_log"),
                "completion",
                arr(
                    obj(
                        "description",
                        "Required inspection",
                        "tool",
                        "inspect_log",
                        "arguments",
                        obj("start_line", 1))),
                "transitions",
                arr(obj("to", "complete")))));
  }

  static final class Harness implements AutoCloseable {
    final ObjectNode c, s;
    final CommandStore commands = mock(CommandStore.class);
    final SessionRepository repo;
    final ModelClient llm = mock(ModelClient.class);
    final FixtureTools tools = mock(FixtureTools.class);
    final AgentEngine engine;
    final ExecutionCoordinator x;
    final List<ObjectNode> events = new ArrayList<>(),
        transcript = new ArrayList<>(),
        executions = new ArrayList<>(),
        attempts = new ArrayList<>();

    Harness(ObjectNode c) {
      this.c = c;
      s = DurableState.create("Inspect the log", "test.yaml", "abc", c);
      CommandStore.Command cmd = mock(CommandStore.Command.class);
      when(cmd.id()).thenReturn("11111111-1111-4111-8111-111111111111");
      when(commands.namespace()).thenReturn("test-deployment");
      repo = spy(new SessionRepository(new MockEnvironment(), commands));
      doReturn(1L).when(repo).acquire(anyString(), anyString());
      doNothing().when(repo).release(anyString(), anyString(), anyLong());
      doReturn(new SessionRepository.Persisted(s, c)).when(repo).load(anyString());
      doAnswer(
              inv -> {
                String sql = inv.getArgument(0);
                if (sql.startsWith("SELECT 1") || sql.contains("SELECT payload,created_at"))
                  return null;
                return obj("valid", true, "cancelled", false, "n", 1);
              })
          .when(repo)
          .queryOne(anyString(), any(Object[].class));
      doAnswer(
              inv -> {
                ObjectNode state = inv.getArgument(0), event = inv.getArgument(2);
                for (JsonNode r : state.path("transcript_pending"))
                  transcript.add((ObjectNode) r.deepCopy());
                for (JsonNode r : state.path("execution_pending"))
                  executions.add((ObjectNode) r.deepCopy());
                for (JsonNode r : state.path("attempts_pending"))
                  attempts.add((ObjectNode) r.deepCopy());
                state.put("revision", state.path("revision").asLong() + 1);
                DurableState.acknowledge(state);
                if (event != null) event.put("event_id", events.size() + 1);
                return obj("revision", state.path("revision"), "event_id", events.size() + 1);
              })
          .when(repo)
          .save(any(), any(), any(), any());
      when(llm.decisionRequest(any(), any()))
          .thenAnswer(inv -> obj("messages", inv.getArgument(1)));
      when(llm.estimate(any(), any()))
          .thenAnswer(inv -> ModelClient.estimate(obj("messages", inv.getArgument(1))));
      when(tools.execute(anyString(), any())).thenReturn(Mono.just("inspection evidence"));
      x = new ExecutionCoordinator(repo, s.path("id").asText(), cmd);
      x.emit = events::add;
      engine = new AgentEngine(JSON, llm, tools);
    }

    void run(int max) {
      x.start();
      engine.run(x, max);
    }

    public void close() {
      x.close();
    }
  }

  @Test
  void allCallsInNativeBatchExecuteInOrderAndSettleBeforeFinal() {
    try (var h = new Harness(config())) {
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(obj("type", "tools", "calls", arr(call("a", 1), call("b", 2)))),
              Mono.just(obj("type", "final", "content", "done")));
      h.run(4);
      verify(h.tools, times(2)).execute(eq("inspect_log"), any());
      assertEquals("complete", h.s.path("status").asText());
      assertEquals("complete", h.s.path("run_status").asText());
      assertTrue(h.s.path("pending_tool_calls").isEmpty());
      assertEquals(
          2, h.attempts.stream().filter(a -> a.path("status").asText().equals("started")).count());
      assertEquals(
          2,
          h.executions.stream().filter(a -> a.path("status").asText().equals("succeeded")).count());
      assertTrue(h.events.getLast().path("type").asText().equals("final_result"));
    }
  }

  @Test
  void finalOnlyWorkflowBlocksLaterToolBeforeInvocation() {
    ObjectNode c = config();
    c.set("workflow", workflow());
    ConfigRepository.normalize(c);
    try (var h = new Harness(c)) {
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(obj("type", "tool").setAll(call("a", 1))),
              Mono.just(obj("type", "tool").setAll(call("b", 1))),
              Mono.just(obj("type", "final", "content", "done")));
      h.run(5);
      verify(h.tools, times(1)).execute(anyString(), any());
      assertTrue(h.s.at("/workflow/active_phase").isNull());
      assertEquals("complete", h.s.path("status").asText());
      assertTrue(
          h.transcript.stream()
              .anyMatch(
                  e -> e.path("payload").path("content").asText().contains("No further tools")));
    }
  }

  @Test
  void strictArgumentsNeverCoerceBeforeToolDispatch() {
    try (var h = new Harness(config())) {
      ObjectNode invalid = call("bad", 1);
      invalid.with("arguments").put("start_line", "1");
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(obj("type", "tool").setAll(invalid)),
              Mono.just(obj("type", "final", "content", "done")));
      h.run(3);
      verifyNoInteractions(h.tools);
      assertEquals("complete", h.s.path("status").asText());
      assertTrue(h.s.path("pending_tool_calls").isEmpty());
    }
  }

  @Test
  void repeatedEmptyResponsesHaveBoundedFailure() {
    try (var h = new Harness(config())) {
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(
                  obj(
                      "type",
                      "recoverable_error",
                      "code",
                      "empty_response",
                      "content",
                      "Return an answer")));
      h.run(10);
      assertEquals("failed", h.s.path("status").asText());
      assertEquals(3, h.s.path("step_count").asInt());
      verify(h.llm, times(3)).decide(any(), any());
    }
  }

  @Test
  void hardBudgetStopsBeforeModelCall() {
    ObjectNode c = config();
    c.with("memory")
        .put("context_window_tokens", 1200)
        .put("reserved_output_tokens", 100)
        .put("safety_margin_tokens", 0);
    try (var h = new Harness(c)) {
      DurableState.message(h.s, obj("role", "user", "content", "x".repeat(5000)));
      h.run(2);
      verify(h.llm, never()).decide(any(), any());
      assertEquals("context_budget_exceeded", h.s.at("/last_error/code").asText());
    }
  }

  @Test
  void prematureExactFinalIsRejectedThenCorrected() {
    ObjectNode c = config();
    c.with("completion").put("exact", "RECOVERY_OK 10.0.0.5 83");
    try (var h = new Harness(c)) {
      when(h.llm.decide(any(), any()))
          .thenReturn(
              Mono.just(obj("type", "final", "content", "almost")),
              Mono.just(obj("type", "final", "content", "RECOVERY_OK 10.0.0.5 83")));
      h.run(3);
      assertEquals("complete", h.s.path("status").asText());
      assertTrue(
          h.transcript.stream().anyMatch(e -> e.path("kind").asText().equals("rejected_final")));
    }
  }

  @Test
  void workflowEvidenceUsesTypedContainment() {
    assertFalse(
        AgentEngine.evidence(
            obj("tool", "inspect_log", "arguments", obj("enabled", true), "result", obj()),
            "inspect_log",
            obj("enabled", 1),
            "ok"));
    assertTrue(
        AgentEngine.evidence(
            obj("tool", "inspect_log", "arguments", obj("n", 1.0), "result", obj()),
            "inspect_log",
            obj("n", 1),
            "ok"));
  }

  @Test
  void alwaysEdgesDoNotAcquireEvidenceGates() {
    ObjectNode c = config();
    c.set("workflow", workflow());
    ((ObjectNode) c.at("/workflow/phases/0/transitions/0")).put("when", "always");
    ConfigRepository.normalize(c);
    ObjectNode s = DurableState.create("goal", "test.yaml", "sha", c);
    assertNotNull(AgentEngine.advance(s, c));
    assertTrue(s.at("/workflow/active_phase").isNull());
  }

  @Test
  void modelPolicyAndSingleWirePreserveEveryCorrelation() {
    ModelClient llm = new ModelClient(JSON, new MockEnvironment());
    ObjectNode c = config();
    c.with("llm").put("tool_call_mode", "single");
    ArrayNode history =
        arr(
            obj("role", "assistant", "tool_calls", arr(obj("id", "a"), obj("id", "b"))),
            obj("role", "tool", "tool_call_id", "b", "content", "B"),
            obj("role", "tool", "tool_call_id", "a", "content", "A"));
    ObjectNode request = llm.decisionRequest(c, history);
    assertFalse(request.path("parallel_tool_calls").asBoolean());
    assertEquals("a", request.at("/messages/2/tool_call_id").asText());
    assertEquals("b", request.at("/messages/4/tool_call_id").asText());
    assertEquals(3, history.size());
    assertFalse(request.at("/tools/0/function").has("recovery"));
    c.with("execution").put("tool_policy", "none");
    assertFalse(llm.decisionRequest(c, history).has("tools"));
  }

  @Test
  void modelNormalizerPreservesAllCallsAndInvalidArguments() {
    ModelClient llm = new ModelClient(JSON, new MockEnvironment());
    ObjectNode response =
        obj(
            "choices",
            arr(
                obj(
                    "message",
                    obj(
                        "tool_calls",
                        arr(
                            obj(
                                "id",
                                "a",
                                "function",
                                obj("name", "inspect_log", "arguments", "{\"n\":1}")),
                            obj(
                                "id",
                                "b",
                                "function",
                                obj("name", "inspect_log", "arguments", "{\"n\":1,\"n\":2}")))))));
    ObjectNode normalized = llm.normalize(response, false);
    assertEquals(2, normalized.path("calls").size());
    assertTrue(normalized.at("/calls/1").has("error"));
  }

  @Test
  void snapshotAndContextAnchorsDetectTampering() {
    ObjectNode c = config(),
        s = DurableState.create("goal", "test.yaml", "sha", c),
        hot = DurableState.hot(s);
    DurableState.validateHot(hot);
    hot.put("goal", "changed");
    assertThrows(IllegalStateException.class, () -> DurableState.validateHot(hot));
  }

  @Test
  void completionRejectsDuplicatesExtrasAndPlaceholders() {
    JsonNode check = obj("kind", "markdown_table");
    JsonNode wanted = CompletionChecks.table("| City | Count |\n| --- | --- |\n| A | 2 |");
    assertTrue(
        CompletionChecks.matches(check, "| City | Count |\n| --- | --- |\n| A | 2 |", wanted));
    assertFalse(
        CompletionChecks.matches(
            check, "| City | Count |\n| --- | --- |\n| A | 2 |\n| A | 2 |", wanted));
    assertFalse(
        CompletionChecks.matches(
            check, "| City | Count |\n| --- | --- |\n| A | 2 |\n| B | 3 |", wanted));
    assertFalse(
        CompletionChecks.matches(check, "| City | Count |\n| --- | --- |\n| A | ... |", wanted));
  }

  @Test
  void alternatingSuccessfulAndRejectedCallsCannotHideNoProgress() {
    ObjectNode c = config();
    c.set("workflow", workflow());
    ConfigRepository.normalize(c);
    ((ObjectNode) c.at("/workflow/phases/0/completion/0/arguments")).put("start_line", 2);
    c.with("execution")
        .put("max_no_progress_steps", 4)
        .put("max_consecutive_invalid_responses", 50);
    try (var h = new Harness(c)) {
      java.util.concurrent.atomic.AtomicInteger index =
          new java.util.concurrent.atomic.AtomicInteger();
      when(h.llm.decide(any(), any()))
          .thenAnswer(
              inv -> {
                int n = index.incrementAndGet();
                ObjectNode call = call("action_" + n, 1);
                if (n % 2 == 0) call.put("name", "unknown_tool");
                return Mono.just(obj("type", "tool").setAll(call));
              });
      h.run(10);
      assertEquals("no_progress_limit", h.s.at("/last_error/code").asText());
      assertEquals(4, h.s.path("step_count").asInt());
      verify(h.tools, times(2)).execute(anyString(), any());
    }
  }

  @Test
  void cooperativeStopRetainsKnownInFlightResultBeforeStoppingScheduling() {
    try (var h = new Harness(config())) {
      java.util.concurrent.atomic.AtomicBoolean stopped =
          new java.util.concurrent.atomic.AtomicBoolean();
      h.x.stopRequested = stopped::get;
      when(h.llm.decide(any(), any()))
          .thenReturn(Mono.just(obj("type", "tool").setAll(call("known", 1))));
      when(h.tools.execute(anyString(), any()))
          .thenReturn(
              Mono.fromSupplier(
                  () -> {
                    stopped.set(true);
                    return "known in-flight result";
                  }));
      assertThrows(ExecutionCoordinator.Cancelled.class, () -> h.run(5));
      assertTrue(h.s.path("pending_tool_calls").isEmpty());
      assertTrue(
          h.executions.stream()
              .anyMatch(
                  e ->
                      e.path("status").asText().equals("succeeded")
                          && e.path("result").asText().equals("known in-flight result")));
      verify(h.llm, times(1)).decide(any(), any());
    }
  }
}
