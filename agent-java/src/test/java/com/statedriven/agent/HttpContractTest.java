package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.web.reactive.server.WebTestClient;

class HttpContractTest {
  final String sid = "11111111-1111-4111-8111-111111111111",
      cid = "22222222-2222-4222-8222-222222222222";
  final CommandStore commands = mock(CommandStore.class);
  final SessionRepository repo = spy(new SessionRepository(new MockEnvironment(), commands));
  final AgentEngine engine = mock(AgentEngine.class);
  final ConfigRepository configs =
      new ConfigRepository(
          JSON, new MockEnvironment().withProperty("AGENT_CONFIG_ROOT", "../config"));

  WebTestClient client() {
    return WebTestClient.bindToController(new AgentController(JSON, configs, repo, engine, commands)).build();
  }

  @Test
  void missingAndDuplicatedKeysFailBeforeAdmission() {
    client()
        .post()
        .uri("/sessions")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"user_prompt\":\"goal\"}")
        .exchange()
        .expectStatus()
        .isEqualTo(422);
    client()
        .post()
        .uri("/sessions")
        .header("Idempotency-Key", "one", "two")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"user_prompt\":\"goal\"}")
        .exchange()
        .expectStatus()
        .isEqualTo(422);
    verifyNoInteractions(commands, engine);
  }

  @Test
  void malformedUuidAndStrictBodiesFailBeforeSql() {
    client().get().uri("/sessions/1-1-1-1-1/transcript").exchange().expectStatus().isEqualTo(422);
    for (String body :
        List.of("{\"max_steps\":true}", "{\"max_steps\":1,\"max_steps\":2}", "{\"extra\":1}"))
      client()
          .post()
          .uri("/sessions/" + sid + "/run")
          .header("Idempotency-Key", "one")
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(body)
          .exchange()
          .expectStatus()
          .isEqualTo(422);
    verify(repo, never()).load(anyString());
    verifyNoInteractions(commands, engine);
  }

  @Test
  void duplicateTerminalStreamReplaysJsonWithoutRun() {
    String route = "/sessions/" + sid + "/run/stream";
    ObjectNode body = obj("max_steps", null),
        out = obj("session", obj("id", sid, "status", "complete"), "message", "saved");
    ObjectNode row =
        obj(
            "command_id",
            cid,
            "status",
            "succeeded",
            "run_id",
            sid,
            "request_sha256",
            hash(arr("agent-command-v1", "POST", route, body)),
            "response_status",
            200,
            "response_body",
            out);
    when(commands.get(null, "same-key")).thenReturn(row);
    client()
        .post()
        .uri(route)
        .header("Idempotency-Key", "same-key")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{}")
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .valueEquals("Idempotency-Replayed", "true")
        .expectHeader()
        .contentType(MediaType.APPLICATION_JSON)
        .expectBody()
        .json(out.toString());
    verify(commands, never()).claim(any());
    verify(repo, never()).load(anyString());
    verifyNoInteractions(engine);
  }

  @Test
  void runningDuplicateReturnsExistingHandleWithoutStartingWorker() {
    String route = "/sessions/" + sid + "/resume";
    ObjectNode body = obj("max_steps", null, "content", null);
    ObjectNode row =
        obj(
            "command_id",
            cid,
            "session_id",
            sid,
            "run_id",
            sid,
            "status",
            "started",
            "request_sha256",
            hash(arr("agent-command-v1", "POST", route, body)));
    when(commands.get(null, "same-key")).thenReturn(row);
    client()
        .post()
        .uri(route)
        .header("Idempotency-Key", "same-key")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{}")
        .exchange()
        .expectStatus()
        .isEqualTo(202)
        .expectBody()
        .jsonPath("$.command_id")
        .isEqualTo(cid)
        .jsonPath("$.run_id")
        .isEqualTo(sid);
    verify(commands, never()).claim(any());
    verifyNoInteractions(engine);
  }

  @Test
  void changedRouteOnExistingKeyConflicts() {
    ObjectNode row =
        obj(
            "command_id",
            cid,
            "status",
            "succeeded",
            "request_sha256",
            hash(
                arr(
                    "agent-command-v1",
                    "POST",
                    "/sessions/" + sid + "/run",
                    obj("max_steps", null))));
    when(commands.get(null, "same-key")).thenReturn(row);
    client()
        .post()
        .uri("/sessions/" + sid + "/resume")
        .header("Idempotency-Key", "same-key")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{}")
        .exchange()
        .expectStatus()
        .isEqualTo(409)
        .expectBody()
        .jsonPath("$.code")
        .isEqualTo("idempotency_key_reused");
    verifyNoInteractions(engine);
  }

  @Test
  void readonlyReconnectionNeverStartsModelOrCommand() {
    ObjectNode state =
        DurableState.create("goal", "test.yaml", "sha", RuntimeRegressionTest.config());
    state.put("id", sid);
    doReturn(new SessionRepository.Persisted(state, RuntimeRegressionTest.config()))
        .when(repo)
        .load(sid);
    doReturn(obj("status", "complete")).when(repo).queryOne(anyString(), any(Object[].class));
    doReturn(
            List.of(obj("event_id", 7L, "event", obj("type", "final_result", "answer", "saved"))),
            List.of())
        .when(repo)
        .query(anyString(), any(Object[].class));
    String wire =
        client()
            .get()
            .uri("/sessions/" + sid + "/events/stream?run_id=" + sid)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(String.class)
            .returnResult()
            .getResponseBody();
    assertTrue(wire.contains("id:7"));
    assertTrue(wire.contains("saved"));
    verifyNoInteractions(commands, engine);
  }
}
