package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import org.springframework.http.*;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;

@RestController
final class AgentController {
  static final java.util.concurrent.ExecutorService RUN_WORKERS =
      java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
  final ConfigRepository configs;
  final SessionRepository repo;
  final AgentEngine engine;
  final CommandStore commands;

  AgentController(
      ObjectMapper ignored, ConfigRepository configs, SessionRepository repo, AgentEngine engine,
      CommandStore commands) {
    this.configs = configs;
    this.repo = repo;
    this.engine = engine;
    this.commands = commands;
  }

  static <T> Mono<T> blocking(CallableWork<T> work) {
    return Mono.fromCallable(work::run).subscribeOn(Schedulers.boundedElastic());
  }

  interface CallableWork<T> {
    T run() throws Exception;
  }

  @ExceptionHandler(ApiFailure.class)
  ResponseEntity<?> failure(ApiFailure e) {
    return ResponseEntity.status(e.status).body(e.body());
  }

  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<?> invalid(IllegalArgumentException e) {
    return ResponseEntity.unprocessableEntity().body(obj("detail", e.getMessage()));
  }

  @GetMapping("/health")
  Mono<?> health() {
    return blocking(
        () ->
            obj(
                "status",
                "ok",
                "contract_version",
                "2.0.0",
                "storage_schema",
                6,
                "config_root",
                configs.root().toString(),
                "available_configs",
                configs.list().size()));
  }

  @GetMapping("/configs")
  Mono<?> configs() {
    return blocking(
        () ->
            configs.list().stream()
                .map(
                    s -> {
                      ArrayNode tools = arr(), phases = arr();
                      s.config().path("tools").forEach(t -> tools.add(t.path("name")));
                      s.config().at("/workflow/phases").forEach(p -> phases.add(p.path("id")));
                      return obj(
                          "config_ref",
                          s.ref(),
                          "sha256",
                          s.sha256(),
                          "agent_name",
                          s.config().path("name"),
                          "tools",
                          tools,
                          "workflow_phases",
                          phases,
                          "verbose_setup",
                          s.config().at("/output/verbose_setup"));
                    })
                .toList());
  }

  SessionRepository.Persisted record(String sid) {
    var p = repo.load(uuid(sid));
    if (p == null) throw new ApiFailure(404, "session_not_found", "Session not found");
    return p;
  }

  @GetMapping("/sessions/{sid}")
  Mono<?> get(@PathVariable String sid) {
    return blocking(() -> DurableState.publicState(record(sid).state()));
  }

  @GetMapping("/commands/{cid}")
  Mono<?> command(@PathVariable String cid) {
    return blocking(
        () -> {
          ObjectNode row = commands.get(uuid(cid), null);
          if (row == null) throw new ApiFailure(404, "command_not_found", "Command not found");
          return CommandStore.envelope(row);
        });
  }

  static ObjectNode request(String raw, String kind) {
    JsonNode parsed = parse(raw);
    if (!parsed.isObject())
      throw new ApiFailure(422, "invalid_request", "Request must be an object");
    ObjectNode body = (ObjectNode) parsed;
    Set<String> keys =
        switch (kind) {
          case "create" -> Set.of("user_prompt", "config_ref");
          case "messages" -> Set.of("content");
          case "resume" -> Set.of("content", "max_steps");
          case "cancel" -> Set.of("run_id");
          default -> Set.of("max_steps");
        };
    body.fieldNames()
        .forEachRemaining(
            k -> {
              if (!keys.contains(k))
                throw new ApiFailure(422, "invalid_request", "Unknown field: " + k);
            });
    if (kind.equals("create")) {
      nonempty(body, "user_prompt");
      if (!body.has("config_ref")) body.put("config_ref", "log-analysis.yaml");
      nonempty(body, "config_ref");
    }
    if (kind.equals("messages")) nonempty(body, "content");
    if (Set.of("run", "stream", "resume").contains(kind)) {
      if (!body.has("max_steps")) body.putNull("max_steps");
      if (!body.path("max_steps").isNull()
          && (!body.path("max_steps").isIntegralNumber()
              || body.path("max_steps").asLong() < 1
              || body.path("max_steps").asLong() > 50))
        throw new ApiFailure(422, "invalid_request", "max_steps must be an integer in 1..50");
    }
    if (kind.equals("resume")) {
      if (!body.has("content")) body.putNull("content");
      if (!body.path("content").isNull()) nonempty(body, "content");
    }
    if (kind.equals("cancel")) body.put("run_id", uuid(body.path("run_id").asText()));
    return body;
  }

  static void nonempty(ObjectNode body, String field) {
    if (!body.path(field).isTextual() || body.path(field).asText().isEmpty())
      throw new ApiFailure(422, "invalid_request", field + " must be a nonempty string");
  }

  record Admission(CommandStore.Command command, ObjectNode row) {}

  Admission admit(HttpHeaders headers, String route, ObjectNode body, String sid) {
    List<String> keys = headers.get("Idempotency-Key");
    if (keys != null && keys.size() != 1)
      throw new ApiFailure(
          422, "idempotency_key_invalid", "Exactly one Idempotency-Key is required");
    String key = keys == null ? null : keys.get(0);
    CommandStore.key(key);
    ObjectNode row = commands.get(null, key);
    if (row != null) CommandStore.matches(row, route, body);
    else {
      ObjectNode admission;
      if (sid == null) {
        var snapshot = configs.resolve(body.path("config_ref").asText());
        admission =
            obj(
                "state",
                DurableState.create(
                    body.path("user_prompt").asText(),
                    snapshot.ref(),
                    snapshot.sha256(),
                    snapshot.config()),
                "config",
                snapshot.config());
      } else admission = obj("session_id", sid, "config", record(sid).config());
      row = commands.register(key, route, body, admission);
    }
    if (!Set.of("pending", "started").contains(row.path("status").asText())
        || row.hasNonNull("run_id")) return new Admission(null, row);
    var cmd = commands.claim(row);
    return new Admission(cmd, cmd == null ? commands.get(null, key) : row);
  }

  ResponseEntity<?> replay(ObjectNode row) {
    var builder =
        ResponseEntity.status(
                Set.of("succeeded", "failed").contains(row.path("status").asText())
                    ? row.path("response_status").asInt()
                    : 202)
            .header("Idempotency-Command-Id", row.path("command_id").asText());
    if (Set.of("succeeded", "failed").contains(row.path("status").asText()))
      return builder
          .header("Idempotency-Replayed", "true")
          .contentType(MediaType.APPLICATION_JSON)
          .body(row.path("response_body"));
    return builder
        .header("Retry-After", "1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(CommandStore.envelope(row));
  }

  ResponseEntity<?> mutate(String raw, HttpHeaders headers, String sid, String kind) {
    if (sid != null) sid = uuid(sid);
    String route = sid == null ? "/sessions" : "/sessions/" + sid + "/" + kind;
    ObjectNode body = request(raw, kind.equals("create") ? "create" : kind);
    Admission admission = admit(headers, route, body, sid);
    if (admission.command() == null) return replay(admission.row());
    var cmd = admission.command();
    try {
      ObjectNode result;
      int status = 200;
      if (kind.equals("create")) {
        ObjectNode saved = (ObjectNode) cmd.row.at("/request/admission/state").deepCopy(),
            config = (ObjectNode) cmd.row.at("/request/admission/config");
        cmd.terminal = true;
        cmd.responseStatus = status = 201;
        cmd.responseMessage =
            "Session created with immutable config "
                + saved.path("config_ref").asText()
                + "@"
                + saved.path("config_sha256").asText().substring(0, 12)
                + ".";
        repo.save(
            saved,
            config,
            obj(
                "type",
                "session_created",
                "config_ref",
                saved.path("config_ref"),
                "config_sha256",
                saved.path("config_sha256")),
            cmd);
        result = obj("session", DurableState.publicState(saved), "message", cmd.responseMessage);
      } else if (kind.equals("cancel")) {
        final String session = sid;
        result =
            repo.transaction(
                c -> {
                  commands.guard(c, cmd);
                  SqlStore.one(
                      c,
                      "SELECT session_id FROM agent_sessions WHERE session_id=?::uuid FOR UPDATE",
                      session);
                  var rows =
                      SqlStore.rows(
                          c,
                          "UPDATE agent_runs SET cancel_requested=true,cancel_requested_at=coalesce(cancel_requested_at,clock_timestamp()) WHERE session_id=?::uuid AND run_id=?::uuid AND status='running' RETURNING run_id",
                          session,
                          body.path("run_id").asText());
                  ArrayNode ids = arr();
                  rows.forEach(r -> ids.add(r.path("run_id")));
                  ObjectNode out =
                      obj(
                          "session_id",
                          session,
                          "cancel_requested_for",
                          ids,
                          "message",
                          "Cancellation is cooperative; in-flight effects are not undone.");
                  commands.complete(c, cmd, 200, out, "succeeded");
                  return out;
                });
      } else {
        try (var x = new ExecutionCoordinator(repo, sid, cmd)) {
          if (!x.state.path("status").asText().equals("active")
              || !x.state.path("pending_tool_calls").isEmpty())
            throw new ApiFailure(
                409,
                "execution_conflict",
                "Use /resume to resolve terminal or pending execution before adding input");
          if (x.state.path("run_status").asText().equals("running"))
            x.finish("interrupted", "abandoned_before_new_user_message");
          x.state.putNull("run_id");
          x.state.put("run_status", "idle");
          x.state.putNull("run_started_at");
          x.state.putNull("run_ended_at");
          DurableState.message(x.state, obj("role", "user", "content", body.path("content")));
          x.state.put("no_progress_steps", 0);
          x.state.putNull("progress_fingerprint");
          cmd.terminal = true;
          cmd.responseMessage = "User input appended to session memory.";
          x.persist(obj("type", "user_message_appended"));
          result =
              obj("session", DurableState.publicState(x.state), "message", cmd.responseMessage);
        }
      }
      return ResponseEntity.status(status).header("Idempotency-Command-Id", cmd.id()).body(result);
    } catch (ApiFailure e) {
      if (!e.code.equals("lease_lost")) commands.finish(cmd, e.status, e.body(), "failed");
      else commands.uncertain(cmd);
      throw e;
    } catch (Exception e) {
      commands.uncertain(cmd);
      throw e;
    } finally {
      cmd.close();
    }
  }

  @PostMapping("/sessions")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = CreateSessionRequest.class))))
  Mono<ResponseEntity<?>> create(@RequestBody String body, @RequestHeader HttpHeaders headers) {
    return blocking(() -> mutate(body, headers, null, "create"));
  }

  @PostMapping("/sessions/{sid}/messages")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = AppendUserMessageRequest.class))))
  Mono<ResponseEntity<?>> message(
      @PathVariable String sid, @RequestBody String body, @RequestHeader HttpHeaders headers) {
    return blocking(() -> mutate(body, headers, sid, "messages"));
  }

  @PostMapping("/sessions/{sid}/cancel")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = CancelSessionRequest.class))))
  Mono<ResponseEntity<?>> cancel(
      @PathVariable String sid, @RequestBody String body, @RequestHeader HttpHeaders headers) {
    return blocking(() -> mutate(body, headers, sid, "cancel"));
  }

  @PostMapping("/sessions/{sid}/run")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = RunRequest.class))))
  Mono<ResponseEntity<?>> run(
      @PathVariable String sid, @RequestBody String body, @RequestHeader HttpHeaders headers) {
    return runRoute(sid, body, headers, "run", false);
  }

  @PostMapping("/sessions/{sid}/run/stream")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = RunRequest.class))))
  Mono<ResponseEntity<?>> stream(
      @PathVariable String sid, @RequestBody String body, @RequestHeader HttpHeaders headers) {
    return runRoute(sid, body, headers, "stream", true);
  }

  @PostMapping("/sessions/{sid}/resume")
  @io.swagger.v3.oas.annotations.Operation(
      requestBody =
          @io.swagger.v3.oas.annotations.parameters.RequestBody(
              required = true,
              content =
                  @io.swagger.v3.oas.annotations.media.Content(
                      schema =
                          @io.swagger.v3.oas.annotations.media.Schema(
                              implementation = ResumeSessionRequest.class))))
  Mono<ResponseEntity<?>> resume(
      @PathVariable String sid, @RequestBody String body, @RequestHeader HttpHeaders headers) {
    return runRoute(sid, body, headers, "resume", true);
  }

  Mono<ResponseEntity<?>> runRoute(
      String supplied, String raw, HttpHeaders headers, String kind, boolean streaming) {
    AtomicBoolean disconnected = new AtomicBoolean();
    AtomicReference<String> runId = new AtomicReference<>();
    String sid = uuid(supplied);
    Mono<ResponseEntity<?>> result =
        blocking(
            () -> {
              ObjectNode body = request(raw, kind);
              String route =
                  "/sessions/" + sid + "/" + (kind.equals("stream") ? "run/stream" : kind);
              Admission admitted = admit(headers, route, body, sid);
              if (admitted.command() == null) return replay(admitted.row());
              CommandStore.Command cmd = admitted.command();
              try {
                var p = record(sid);
                if (kind.equals("stream") && !p.state().path("status").asText().equals("active"))
                  throw new ApiFailure(
                      409, "execution_conflict", "Session is terminal; use /resume");
                if (kind.equals("resume")
                    && body.hasNonNull("content")
                    && !p.state().path("pending_tool_calls").isEmpty())
                  throw new ApiFailure(
                      409, "execution_conflict", "Recover pending calls without content first");
              } catch (ApiFailure e) {
                commands.finish(cmd, e.status, e.body(), "failed");
                cmd.close();
                throw e;
              }
              if (!streaming) {
                ObjectNode done;
                try {
                  done =
                      java.util.concurrent.CompletableFuture.supplyAsync(
                              () -> {
                                try (cmd) {
                                  return execute(sid, body, kind, cmd, null, disconnected, runId);
                                }
                              },
                              RUN_WORKERS)
                          .join();
                } catch (java.util.concurrent.CompletionException error) {
                  if (error.getCause() instanceof RuntimeException failure) throw failure;
                  throw error;
                }
                return ResponseEntity.ok()
                    .header("Idempotency-Command-Id", cmd.id())
                    .body(
                        obj(
                            "session",
                            DurableState.publicState(done),
                            "message",
                            done.path("final_answer").asText("Run stopped.")));
              }
              Sinks.Many<ServerSentEvent<ObjectNode>> sink =
                  Sinks.many().unicast().onBackpressureBuffer();
              Flux<ServerSentEvent<ObjectNode>> flux =
                  sink.asFlux()
                      .doOnCancel(
                          () -> {
                            disconnected.set(true);
                            requestCancel(sid, runId.get());
                          });
              RUN_WORKERS.execute(
                  () -> {
                    try (cmd) {
                      execute(
                          sid,
                          body,
                          kind,
                          cmd,
                          e ->
                              sink.tryEmitNext(
                                  ServerSentEvent.<ObjectNode>builder(e)
                                      .id(
                                          e.hasNonNull("event_id")
                                              ? e.path("event_id").asText()
                                              : "")
                                      .build()),
                          disconnected,
                          runId);
                      sink.tryEmitComplete();
                    } catch (Exception error) {
                      ObjectNode e =
                          error instanceof ApiFailure a
                              ? a.body()
                              : obj(
                                  "type",
                                  "error",
                                  "code",
                                  "controller_error",
                                  "message",
                                  error.getMessage(),
                                  "recoverable",
                                  true);
                      sink.tryEmitNext(ServerSentEvent.builder(e).build());
                      sink.tryEmitComplete();
                    }
                  });
              return ResponseEntity.ok()
                  .header("Idempotency-Command-Id", cmd.id())
                  .header("Cache-Control", "no-cache")
                  .contentType(MediaType.TEXT_EVENT_STREAM)
                  .body(flux);
            });
    return result.doOnCancel(
        () -> {
          disconnected.set(true);
          requestCancel(sid, runId.get());
        });
  }

  void requestCancel(String sid, String rid) {
    if (rid == null) return;
    Schedulers.boundedElastic()
        .schedule(
            () ->
                repo.transaction(
                    c -> {
                      SqlStore.update(
                          c,
                          "UPDATE agent_runs SET cancel_requested=true,cancel_requested_at=coalesce(cancel_requested_at,clock_timestamp()) WHERE session_id=?::uuid AND run_id=?::uuid AND status='running'",
                          sid,
                          rid);
                      return null;
                    }));
  }

  ObjectNode execute(
      String sid,
      ObjectNode body,
      String kind,
      CommandStore.Command cmd,
      Consumer<ObjectNode> emit,
      AtomicBoolean disconnected,
      AtomicReference<String> runId) {
    try (var x = new ExecutionCoordinator(repo, sid, cmd)) {
      x.emit = emit;
      x.stopRequested = disconnected::get;
      if (!kind.equals("resume") && !x.state.path("status").asText().equals("active")) {
        commands.finish(
            cmd,
            200,
            obj(
                "session",
                DurableState.publicState(x.state),
                "message",
                x.state.path("final_answer").asText("Run stopped.")),
            "succeeded");
        return x.state;
      }
      if (kind.equals("resume")) {
        x.state.put("status", "active");
        x.state.putNull("final_answer");
        x.state.putNull("last_error");
        x.state.put("consecutive_invalid_responses", 0);
        x.state.put("no_progress_steps", 0);
        x.state.putNull("progress_fingerprint");
        x.state.putNull("last_compaction_attempt_sha256");
        if (body.hasNonNull("content") && !x.state.path("pending_tool_calls").isEmpty())
          throw new ApiFailure(409, "execution_conflict", "Recover pending calls first");
      }
      x.start();
      runId.set(x.state.path("run_id").asText());
      if (disconnected.get()) requestCancel(sid, runId.get());
      if (kind.equals("resume") && body.hasNonNull("content")) {
        DurableState.message(x.state, obj("role", "user", "content", body.path("content")));
        x.persist(obj("type", "session_resumed", "has_new_user_message", true));
      }
      try {
        engine.run(x, body.path("max_steps").isNumber() ? body.path("max_steps").asInt() : null);
      } catch (ExecutionCoordinator.Cancelled e) {
        repo.transaction(
            connection -> {
              SqlStore.update(
                  connection,
                  "UPDATE agent_runs SET cancel_requested=true,cancel_requested_at=coalesce(cancel_requested_at,clock_timestamp()) WHERE session_id=?::uuid AND run_id=?::uuid AND status='running'",
                  sid,
                  x.state.path("run_id").asText());
              return null;
            });
        x.state.put("status", "failed");
        x.state.put("final_answer", "Run cancelled; use /resume to recover committed work.");
        x.state.set(
            "last_error",
            obj(
                "code",
                "run_cancelled",
                "message",
                x.state.path("final_answer"),
                "recoverable",
                true));
        x.finish("cancelled", "cancel_requested");
        if (emit != null)
          emit.accept(obj("type", "error").setAll((ObjectNode) x.state.path("last_error")));
        return x.state;
      } catch (ExecutionCoordinator.Blocked e) {
        x.state.put("status", "failed");
        x.state.put("final_answer", e.getMessage());
        x.state.set(
            "last_error",
            obj("code", "recovery_required", "message", e.getMessage(), "recoverable", true));
        x.finish("blocked", "uncertain_tool_outcome");
        if (emit != null)
          emit.accept(obj("type", "error").setAll((ObjectNode) x.state.path("last_error")));
        return x.state;
      }
      x.finish(
          x.state.path("status").asText().equals("complete") ? "complete" : "failed",
          x.state.path("last_error").path("code").asText(x.state.path("status").asText()));
      return x.state;
    } catch (ApiFailure e) {
      if (e.code.equals("lease_lost")) commands.uncertain(cmd);
      else commands.finish(cmd, e.status, e.body(), "failed");
      throw e;
    } catch (Exception e) {
      commands.uncertain(cmd);
      throw e;
    }
  }

  static int limit(int n) {
    if (n < 1 || n > 1000) throw new ApiFailure(422, "invalid_cursor", "limit must be 1..1000");
    return n;
  }

  static long cursor(long n) {
    if (n < 0) throw new ApiFailure(422, "invalid_cursor", "Cursor must be nonnegative");
    return n;
  }

  @GetMapping("/sessions/{sid}/transcript")
  Mono<?> transcript(
      @PathVariable String sid,
      @RequestParam(defaultValue = "0") long after_sequence,
      @RequestParam(defaultValue = "100") int limit,
      @RequestParam(required = false) Long through_sequence) {
    return blocking(
        () -> {
          ObjectNode s = record(sid).state();
          long through =
              Math.min(
                  s.path("next_transcript_sequence").asLong() - 1,
                  through_sequence == null ? Long.MAX_VALUE : cursor(through_sequence));
          var rows =
              repo.query(
                  "SELECT sequence,message_id,kind,payload,run_id,decision_step,created_at,recorded_at FROM agent_session_transcript WHERE session_id=?::uuid AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?",
                  uuid(sid),
                  cursor(after_sequence),
                  through,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "snapshot_revision",
              s.path("revision"),
              "history_complete",
              s.path("history_complete"),
              "through_sequence",
              through,
              "entries",
              rows,
              "next_after_sequence",
              rows.isEmpty() ? after_sequence : rows.getLast().path("sequence").asLong());
        });
  }

  @GetMapping("/sessions/{sid}/events")
  Mono<?> events(
      @PathVariable String sid,
      @RequestParam(defaultValue = "0") long after,
      @RequestParam(defaultValue = "100") int limit,
      @RequestParam(required = false) Long through) {
    return blocking(
        () -> {
          ObjectNode s = record(sid).state();
          long watermark =
              repo.queryOne(
                      "SELECT coalesce(max(event_id),0) AS n FROM agent_session_events WHERE session_id=?::uuid",
                      uuid(sid))
                  .path("n")
                  .asLong();
          long end = Math.min(watermark, through == null ? Long.MAX_VALUE : cursor(through));
          var rows =
              repo.query(
                  "SELECT event_id,event,revision,created_at FROM agent_session_events WHERE session_id=?::uuid AND event_id>? AND event_id<=? ORDER BY event_id LIMIT ?",
                  uuid(sid),
                  cursor(after),
                  end,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "through",
              end,
              "entries",
              rows,
              "next_after",
              rows.isEmpty() ? after : rows.getLast().path("event_id").asLong(),
              "archive_id",
              s.path("last_archive_id"));
        });
  }

  @GetMapping("/sessions/{sid}/diagnostics/{family}")
  Mono<?> diagnostics(
      @PathVariable String sid,
      @PathVariable String family,
      @RequestParam(defaultValue = "0") long after,
      @RequestParam(defaultValue = "100") int limit,
      @RequestParam(required = false) Long through) {
    return blocking(
        () -> {
          if (!DurableState.FAMILIES.contains(family))
            throw new ApiFailure(422, "invalid_family", "Unknown diagnostic family");
          ObjectNode s = record(sid).state();
          long watermark = s.path("diagnostic_counts").path(family).asLong(),
              end = Math.min(watermark, through == null ? Long.MAX_VALUE : cursor(through));
          var rows =
              repo.query(
                  "SELECT ordinal,payload FROM agent_session_diagnostics WHERE session_id=?::uuid AND family=? AND ordinal>? AND ordinal<=? ORDER BY ordinal LIMIT ?",
                  uuid(sid),
                  family,
                  cursor(after),
                  end,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "family",
              family,
              "through",
              end,
              "total_recorded",
              watermark,
              "entries",
              rows,
              "next_after",
              rows.isEmpty() ? after : rows.getLast().path("ordinal").asLong(),
              "archive_id",
              s.path("last_archive_id"),
              "history_complete",
              s.path("diagnostic_history_complete").asBoolean(true)
                  && !s.hasNonNull("last_archive_id"));
        });
  }

  @GetMapping("/sessions/{sid}/runs")
  Mono<?> runs(
      @PathVariable String sid,
      @RequestParam(required = false) String after,
      @RequestParam(defaultValue = "100") int limit) {
    return blocking(
        () -> {
          record(sid);
          String cursor = after == null ? null : uuid(after);
          var rows =
              repo.query(
                  "SELECT run_id,fence,status,started_at,ended_at,termination_reason,last_decision_step,cancel_requested,cancel_requested_at FROM agent_runs WHERE session_id=?::uuid AND (?::uuid IS NULL OR (started_at,run_id)>(SELECT started_at,run_id FROM agent_runs WHERE session_id=?::uuid AND run_id=?::uuid)) ORDER BY started_at,run_id LIMIT ?",
                  uuid(sid),
                  cursor,
                  uuid(sid),
                  cursor,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "runs",
              rows,
              "next_after",
              rows.isEmpty() ? cursor : rows.getLast().path("run_id"));
        });
  }

  @GetMapping("/sessions/{sid}/executions")
  Mono<?> executions(
      @PathVariable String sid,
      @RequestParam(required = false) String after,
      @RequestParam(defaultValue = "100") int limit) {
    return blocking(
        () -> {
          record(sid);
          String cursor = after == null ? null : uuid(after);
          var rows =
              repo.query(
                  "SELECT execution_id,run_id,call_id,tool,arguments,policy,idempotency_key,attempts,status,result,fence,created_at,updated_at,started_at,finished_at,command_id,namespace,target,payload_sha256,key_algorithm FROM agent_tool_executions WHERE session_id=?::uuid AND (?::uuid IS NULL OR execution_id>?::uuid) ORDER BY execution_id LIMIT ?",
                  uuid(sid),
                  cursor,
                  cursor,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "executions",
              rows,
              "next_after",
              rows.isEmpty() ? cursor : rows.getLast().path("execution_id"));
        });
  }

  @GetMapping("/sessions/{sid}/archives")
  Mono<?> archives(
      @PathVariable String sid,
      @RequestParam(required = false) String after,
      @RequestParam(defaultValue = "100") int limit) {
    return blocking(
        () -> {
          record(sid);
          String cursor = after == null ? null : uuid(after);
          var rows =
              repo.query(
                  "SELECT archive_id,snapshot_revision,sha256,manifest,created_at FROM agent_archive_receipts WHERE session_id=?::uuid AND (?::uuid IS NULL OR archive_id>?::uuid) ORDER BY archive_id LIMIT ?",
                  uuid(sid),
                  cursor,
                  cursor,
                  limit(limit));
          return obj(
              "session_id",
              uuid(sid),
              "archives",
              rows,
              "next_after",
              rows.isEmpty() ? cursor : rows.getLast().path("archive_id"));
        });
  }

  @GetMapping("/sessions/{sid}/retention")
  Mono<?> retention(@PathVariable String sid) {
    return blocking(() -> RetentionAdmin.plan(repo, uuid(sid), 32, 100));
  }

  @GetMapping("/runs/abandoned")
  Mono<?> abandoned(@RequestParam(defaultValue = "100") int limit) {
    return blocking(
        () ->
            obj(
                "runs",
                repo.query(
                    "SELECT r.session_id,r.run_id,r.status,r.last_decision_step FROM agent_runs r JOIN agent_sessions s USING(session_id) WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp()) ORDER BY r.started_at LIMIT ?",
                    limit(limit))));
  }

  @GetMapping("/sessions/{sid}/events/stream")
  Mono<ResponseEntity<?>> reconnect(
      @PathVariable String sid,
      @RequestParam String run_id,
      @RequestParam(defaultValue = "0") long after_event_id) {
    return blocking(
        () -> {
          record(sid);
          String run = uuid(run_id), session = uuid(sid);
          cursor(after_event_id);
          if (repo.queryOne(
                  "SELECT status FROM agent_runs WHERE session_id=?::uuid AND run_id=?::uuid",
                  session,
                  run)
              == null) throw new ApiFailure(404, "run_not_found", "Run not found");
          Flux<ServerSentEvent<ObjectNode>> stream =
              Flux.create(
                  sink ->
                      Schedulers.boundedElastic()
                          .schedule(
                              () -> {
                                long after = after_event_id;
                                try {
                                  while (!sink.isCancelled()) {
                                    var rows =
                                        repo.query(
                                            "SELECT event_id,event FROM agent_session_events WHERE session_id=?::uuid AND event_id>? AND event->>'run_id'=? ORDER BY event_id LIMIT 100",
                                            session,
                                            after,
                                            run);
                                    for (ObjectNode row : rows) {
                                      after = row.path("event_id").asLong();
                                      ObjectNode e = (ObjectNode) row.path("event").deepCopy();
                                      e.put("event_id", after);
                                      sink.next(
                                          ServerSentEvent.<ObjectNode>builder(e)
                                              .id(Long.toString(after))
                                              .build());
                                    }
                                    ObjectNode r =
                                        repo.queryOne(
                                            "SELECT status FROM agent_runs WHERE session_id=?::uuid AND run_id=?::uuid",
                                            session,
                                            run);
                                    if (rows.isEmpty()
                                        && (r == null
                                            || !r.path("status").asText().equals("running"))) break;
                                    if (rows.isEmpty()) Thread.sleep(250);
                                  }
                                  sink.complete();
                                } catch (Exception e) {
                                  sink.error(e);
                                }
                              }));
          return ResponseEntity.ok()
              .contentType(MediaType.TEXT_EVENT_STREAM)
              .header("Cache-Control", "no-cache")
              .body(stream);
        });
  }
}
