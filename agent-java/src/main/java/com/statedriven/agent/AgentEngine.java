package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import java.util.regex.*;
import org.springframework.stereotype.Component;

@Component
final class AgentEngine {
  final ModelClient llm;
  final FixtureTools tools;

  AgentEngine(ObjectMapper ignored, ModelClient llm, FixtureTools tools) {
    this.llm = llm;
    this.tools = tools;
  }

  void run(ExecutionCoordinator x, Integer maxSteps) {
    ObjectNode s = x.state, c = x.config;
    int allowance =
        maxSteps != null
            ? maxSteps
            : c.at("/memory/run_max_steps").isNumber()
                ? c.at("/memory/run_max_steps").asInt()
                : c.at("/memory/max_steps").asInt();
    int limit = s.path("step_count").asInt() + allowance;
    if (!s.hasNonNull("progress_fingerprint"))
      s.put("progress_fingerprint", progressFingerprint(s, c));
    x.persist(
        obj("type", "status", "message", "Agent run started.", "run_count", s.path("run_count")));
    if (!s.path("pending_tool_calls").isEmpty()) executeCalls(x);
    while (s.path("status").asText().equals("active") && s.path("step_count").asInt() < limit) {
      x.checkpoint(true);
      ObjectNode transition = advance(s, c);
      if (transition != null) x.persist(transition);
      ObjectNode decision;
      try {
        compact(x);
        ObjectNode measured = ModelClient.budget(llm.decisionRequest(c, payload(s, c)), c);
        s.set("request_budget", measured);
        if (!measured.path("fits").asBoolean())
          throw new ModelClient.ModelFailure(
              "context_budget_exceeded", "Complete decision request exceeds context budget");
        x.persist(
            obj(
                "type",
                "status",
                "message",
                "Agent deciding next action.",
                "step_count",
                s.path("step_count")));
        x.checkpoint(true);
        decision = llm.decide(c, payload(s, c)).block();
      } catch (ModelClient.ModelFailure error) {
        s.put("status", "failed");
        s.put("final_answer", error.getMessage());
        s.set("last_error", error.diagnostic);
        ObjectNode event = error.diagnostic.deepCopy();
        event.put("type", "error");
        x.persist(event);
        break;
      } catch (IllegalArgumentException error) {
        fail(x, "context_integrity_error", error.getMessage());
        break;
      }
      s.put("step_count", s.path("step_count").asInt() + 1);
      DurableState.record(
          s,
          "model_decision",
          obj(
              "parsed_decision_json",
              decision.toString(),
              "type",
              decision.path("type"),
              "code",
              decision.path("code"),
              "content",
              decision.path("content"),
              "calls",
              decision.path("calls").isMissingNode() ? arr() : decision.path("calls")));
      x.checkpoint(true);
      if (decision.path("type").asText().equals("recoverable_error")) {
        if (decision.has("candidate_content"))
          DurableState.record(
              s,
              "rejected_final",
              obj(
                  "content",
                  decision.path("candidate_content"),
                  "errors",
                  arr(decision.path("code"))));
        reject(
            x, decision.path("code").asText("invalid_response"), decision.path("content").asText());
        continue;
      }
      if (decision.path("type").asText().equals("final")) {
        String answer = decision.path("content").asText();
        List<String> errors = completionErrors(s, c, answer);
        if (!errors.isEmpty()) {
          DurableState.record(s, "rejected_final", obj("content", answer, "errors", errors));
          if (!answer.isBlank())
            DurableState.message(s, obj("role", "assistant", "content", answer));
          reject(x, "completion_rejected", String.join(" ", errors));
          continue;
        }
        DurableState.message(s, obj("role", "assistant", "content", answer));
        s.put("status", "complete");
        s.put("final_answer", answer);
        s.put("consecutive_invalid_responses", 0);
        s.put("no_progress_steps", 0);
        s.putNull("last_error");
        x.persist(obj("type", "final_result", "answer", answer));
        break;
      }
      JsonNode calls =
          decision.path("type").asText().equals("tool") ? arr(decision) : decision.path("calls");
      if (!calls.isArray() || calls.isEmpty()) {
        reject(x, "invalid_decision", "Return a nonempty tool-call array or final answer.");
        continue;
      }
      ArrayNode normalized = arr(), wireCalls = arr();
      Set<String> ids = new HashSet<>();
      for (JsonNode call : calls) {
        ObjectNode item =
            call.isObject()
                ? (ObjectNode) call.deepCopy()
                : obj("error", "Tool call must be an object");
        String id = item.path("id").isTextual() ? item.path("id").asText() : "";
        if (id.isBlank()
            || !ids.add(id)
            || x.repo.queryOne(
                    "SELECT 1 FROM agent_session_calls WHERE session_id=?::uuid AND call_id=? LIMIT 1",
                    s.path("id").asText(),
                    id)
                != null) {
          if (!id.isBlank()) item.put("error", "Duplicate tool-call ID; use a unique ID");
          id = "call_" + uuid();
          item.put("id", id);
          ids.add(id);
        }
        if (!item.path("name").isTextual() || item.path("name").asText().isBlank()) {
          item.put("name", "invalid_tool_call");
          item.put("error", "Tool name must be configured");
        }
        String raw =
            item.path("raw_arguments").isTextual()
                ? item.path("raw_arguments").asText()
                : item.path("arguments").toString();
        item.put("raw_arguments", raw);
        normalized.add(item);
        wireCalls.add(
            obj(
                "id",
                id,
                "type",
                "function",
                "function",
                obj("name", item.path("name"), "arguments", raw)));
      }
      DurableState.message(s, obj("role", "assistant", "tool_calls", wireCalls));
      s.set("pending_tool_calls", normalized);
      ArrayNode callIds = arr();
      normalized.forEach(call -> callIds.add(call.path("id")));
      x.persist(obj("type", "tool_batch_scheduled", "call_ids", callIds));
      executeCalls(x);
    }
    if (s.path("status").asText().equals("active"))
      fail(
          x,
          "step_limit",
          "Step limit ("
              + allowance
              + ") reached without a final answer. Resume with another run request.");
  }

  void executeCalls(ExecutionCoordinator x) {
    ObjectNode s = x.state, c = x.config;
    Set<String> allowed = allowed(s, c);
    List<String> rejected = new ArrayList<>();
    ArrayNode calls = ((ArrayNode) s.path("pending_tool_calls")).deepCopy();
    for (JsonNode value : calls) {
      x.checkpoint(true);
      ObjectNode call = (ObjectNode) value;
      String name = call.path("name").asText();
      JsonNode declaration = tool(c, name);
      List<String> errors = new ArrayList<>();
      if (call.has("error")) errors.add(call.path("error").asText());
      if (declaration == null) errors.add("Tool is not configured: " + name);
      else {
        errors.addAll(
            SchemaChecks.validate(call.path("arguments"), declaration.path("parameters")));
        try {
          canonical(call.path("arguments"));
        } catch (IllegalArgumentException invalid) {
          errors.add(
              "Arguments must use finite interoperable JSON numbers; encode exact large values as strings.");
        }
        if (!allowed.contains(name))
          errors.add(
              c.path("workflow").isObject() && s.at("/workflow/active_phase").isNull()
                  ? "All workflow phases are complete. No further tools may execute; provide the final answer."
                  : "Tool is not allowed in the active phase or tool policy.");
        for (JsonNode binding : c.at("/execution/argument_bindings"))
          if (binding.path("tool").asText().equals(name)) {
            JsonNode artifact = artifact(s, c, binding.path("source_label").asText());
            try {
              JsonNode expected = path(artifact.path("value"), binding.path("path"));
              if (artifact.isMissingNode()
                  || !equal(
                      call.path("arguments").path(binding.path("argument").asText()), expected))
                errors.add(
                    "Argument "
                        + binding.path("argument").asText()
                        + " must use executed result "
                        + binding.path("source_label").asText());
            } catch (Exception error) {
              errors.add("Required argument source is unavailable");
            }
          }
      }
      String output;
      if (errors.isEmpty()) {
        output = x.invoke(call, tools);
      } else {
        output = "ERROR: " + String.join(" ", errors);
        rejected.addAll(errors);
      }
      DurableState.message(
          s, obj("role", "tool", "content", output, "name", name, "tool_call_id", call.path("id")));
      ArrayNode pending = s.withArray("pending_tool_calls");
      for (int i = pending.size() - 1; i >= 0; i--)
        if (pending.get(i).path("id").equals(call.path("id"))) pending.remove(i);
      if (errors.isEmpty()) {
        recordEvidence(s, c, name, call.path("arguments"), output);
        capture(s, c, name, call.path("arguments"), output, call.path("id").asText());
        if (!c.path("workflow").isObject()
            && !output.startsWith("ERROR:")
            && !output.startsWith("AssertionError:")) {
          String observation = sha(defaultJson(arr(name, call.path("arguments"), output)));
          ArrayNode seen = s.withArray("progress_seen_outputs");
          boolean exists = false;
          for (JsonNode n : seen) if (n.asText().equals(observation)) exists = true;
          if (!exists) seen.add(observation);
          while (seen.size() > 64) seen.remove(0);
        }
      }
      x.persist(
          obj(
              "type",
              "interim_result",
              "step",
              "tool_completed",
              "data",
              obj(
                  "tool",
                  name,
                  "tool_call_id",
                  call.path("id"),
                  "arguments",
                  call.path("arguments"),
                  "output",
                  output,
                  "executed",
                  errors.isEmpty())));
    }
    if (!rejected.isEmpty() && s.path("status").asText().equals("active"))
      reject(x, "tool_call_rejected", String.join(" ", rejected));
    else if (s.path("status").asText().equals("active")) {
      s.put("consecutive_invalid_responses", 0);
      s.putNull("last_error");
      observeProgress(x);
    }
  }

  static JsonNode tool(ObjectNode c, String name) {
    for (JsonNode t : c.path("tools")) if (t.path("name").asText().equals(name)) return t;
    return null;
  }

  static Set<String> allowed(ObjectNode s, ObjectNode c) {
    Set<String> names = new LinkedHashSet<>();
    if (c.at("/execution/tool_policy").asText().equals("none")) return names;
    for (JsonNode t : c.path("tools")) names.add(t.path("name").asText());
    if (c.path("workflow").isObject()) {
      if (s.at("/workflow/active_phase").isNull()) return Set.of();
      JsonNode p = phase(c, s.at("/workflow/active_phase").asText());
      if (p.path("allowed_tools").isArray()) {
        Set<String> permitted = new HashSet<>();
        p.path("allowed_tools").forEach(n -> permitted.add(n.asText()));
        names.retainAll(permitted);
      }
    }
    return names;
  }

  static JsonNode phase(ObjectNode c, String id) {
    for (JsonNode p : c.at("/workflow/phases")) if (p.path("id").asText().equals(id)) return p;
    throw new IllegalArgumentException("Unknown workflow phase");
  }

  static boolean seen(ObjectNode s, String key) {
    for (JsonNode value : s.at("/workflow/observed_evidence"))
      if (value.asText().equals(key)) return true;
    return false;
  }

  static List<String> missing(ObjectNode s, JsonNode p) {
    List<String> out = new ArrayList<>();
    int i = 0;
    for (JsonNode evidence : p.path("completion")) {
      if (evidence.path("required").asBoolean(true)
          && !seen(s, p.path("id").asText() + ":completion:" + i))
        out.add(evidence.path("description").asText() + "; exact constraints: " + evidence);
      i++;
    }
    return out;
  }

  static boolean evidence(JsonNode e, String name, JsonNode arguments, String output) {
    if (!e.path("tool").asText().equals(name) || !contains(arguments, e.path("arguments")))
      return false;
    for (JsonNode term : e.path("content_contains"))
      if (!output.toLowerCase(Locale.ROOT).contains(term.asText().toLowerCase(Locale.ROOT)))
        return false;
    if (e.path("result").isEmpty()) return true;
    try {
      return contains(parse(output), e.path("result"));
    } catch (Exception error) {
      return false;
    }
  }

  static void recordEvidence(
      ObjectNode s, ObjectNode c, String name, JsonNode arguments, String output) {
    if (!c.path("workflow").isObject() || s.at("/workflow/active_phase").isNull()) return;
    JsonNode p = phase(c, s.at("/workflow/active_phase").asText());
    int i = 0;
    for (JsonNode e : p.path("completion")) {
      String key = p.path("id").asText() + ":completion:" + i++;
      if (!seen(s, key) && evidence(e, name, arguments, output))
        s.with("workflow").withArray("observed_evidence").add(key);
    }
    int ti = 0;
    for (JsonNode t : p.path("transitions")) {
      i = 0;
      for (JsonNode e : t.path("evidence")) {
        String key = p.path("id").asText() + ":transition-" + ti + ":" + i++;
        if (!seen(s, key) && evidence(e, name, arguments, output))
          s.with("workflow").withArray("observed_evidence").add(key);
      }
      ti++;
    }
  }

  static ObjectNode advance(ObjectNode s, ObjectNode c) {
    if (!c.path("workflow").isObject() || s.at("/workflow/active_phase").isNull()) return null;
    JsonNode p = phase(c, s.at("/workflow/active_phase").asText());
    int i = 0;
    for (JsonNode t : p.path("transitions")) {
      String when = t.path("when").asText("phase_complete");
      boolean applies =
          when.equals("always") || when.equals("phase_complete") && missing(s, p).isEmpty();
      if (when.equals("evidence")) {
        applies = true;
        int j = 0;
        for (JsonNode e : t.path("evidence"))
          if (!seen(s, p.path("id").asText() + ":transition-" + i + ":" + j++)) applies = false;
      }
      if (applies) {
        String to = t.path("to").asText();
        ArrayNode done = s.with("workflow").withArray("completed_phases");
        boolean exists = false;
        for (JsonNode n : done) if (n.equals(p.path("id"))) exists = true;
        if (!exists) done.add(p.path("id"));
        if (to.equals("complete")) s.with("workflow").putNull("active_phase");
        else s.with("workflow").put("active_phase", to);
        ObjectNode tr = obj("from_phase", p.path("id"), "to_phase", to, "reason", when);
        s.with("workflow").withArray("transitions").add(tr);
        return obj(
            "type",
            "interim_result",
            "step",
            "phase_completed",
            "phase_id",
            p.path("id"),
            "next_phase",
            to,
            "reason",
            when);
      }
      i++;
    }
    return null;
  }

  static String artifactKey(ObjectNode c, String label) {
    int i = 0;
    for (JsonNode check : c.at("/completion/result_checks")) {
      if (check.path("label").asText().equals(label)) return String.valueOf(i);
      i++;
    }
    throw new IllegalArgumentException("Unknown result label");
  }

  static JsonNode artifact(ObjectNode s, ObjectNode c, String label) {
    return s.path("completion_artifacts").path(artifactKey(c, label));
  }

  static void capture(
      ObjectNode s, ObjectNode c, String name, JsonNode arguments, String output, String callId) {
    if (output.startsWith("ERROR:") || output.startsWith("AssertionError:")) return;
    for (JsonNode check : c.at("/completion/result_checks"))
      if (check.path("tool").asText().equals(name)
          && contains(arguments, check.path("arguments"))) {
        try {
          if (!check.path("result").isEmpty() && !contains(parse(output), check.path("result")))
            continue;
          JsonNode value = CompletionChecks.extract(check, output);
          s.with("completion_artifacts")
              .set(
                  artifactKey(c, check.path("label").asText()),
                  obj(
                      "value",
                      value,
                      "tool",
                      name,
                      "arguments",
                      arguments,
                      "tool_call_id",
                      callId,
                      "step",
                      s.path("step_count")));
        } catch (Exception ignored) {
        }
      }
  }

  static List<String> completionErrors(ObjectNode s, ObjectNode c, String answer) {
    List<String> errors = new ArrayList<>();
    if (answer == null || answer.isBlank()) errors.add("Final answer must be nonempty.");
    if (c.path("workflow").isObject() && !s.at("/workflow/active_phase").isNull())
      errors.add(
          "Complete the active phase first: "
              + String.join("; ", missing(s, phase(c, s.at("/workflow/active_phase").asText()))));
    JsonNode f = c.path("completion");
    if (f.hasNonNull("exact") && !answer.equals(f.path("exact").asText()))
      errors.add("Final answer must equal: " + f.path("exact").asText());
    if (f.hasNonNull("pattern")
        && !Pattern.compile(f.path("pattern").asText()).matcher(answer).matches())
      errors.add("Final answer does not match required format.");
    for (JsonNode p : f.path("forbidden_patterns"))
      if (Pattern.compile(p.asText(), Pattern.CASE_INSENSITIVE).matcher(answer).find())
        errors.add("Final answer contains a forbidden placeholder or phrase.");
    if (f.hasNonNull("json_schema"))
      try {
        errors.addAll(SchemaChecks.validate(parse(answer), f.path("json_schema")));
      } catch (Exception error) {
        errors.add("Final answer must be valid JSON.");
      }
    for (JsonNode check : f.path("result_checks")) {
      JsonNode artifact = artifact(s, c, check.path("label").asText());
      if (artifact.isMissingNode()
          || !CompletionChecks.matches(check, answer, artifact.path("value")))
        errors.add("Final answer must reproduce executed result: " + check.path("label").asText());
    }
    return errors;
  }

  static ArrayNode payload(ObjectNode s, ObjectNode c) {
    ArrayNode messages =
        arr(
            obj("role", "system", "content", c.path("system_prompt")),
            obj(
                "role",
                "user",
                "content",
                "Original user goal (immutable): " + s.path("goal").asText()));
    JsonNode f = c.path("completion");
    List<String> requirements = new ArrayList<>();
    if (f.hasNonNull("instructions")) requirements.add(f.path("instructions").asText());
    if (f.hasNonNull("exact")) requirements.add("Return exactly: " + f.path("exact").asText());
    if (f.hasNonNull("json_schema"))
      requirements.add("Required JSON schema: " + f.path("json_schema"));
    if (!requirements.isEmpty())
      messages.add(
          obj(
              "role",
              "system",
              "content",
              "[FINAL ANSWER REQUIREMENTS]\n" + String.join("\n", requirements)));
    if (s.hasNonNull("compacted_context"))
      messages.add(
          obj(
              "role",
              "user",
              "content",
              "[COMPACTED CONTEXT STATE — UNVERIFIED MODEL CLAIMS]\n"
                  + s.path("compacted_context").asText()));
    for (JsonNode check : f.path("result_checks")) {
      JsonNode artifact = artifact(s, c, check.path("label").asText());
      if (!artifact.isMissingNode())
        messages.add(
            obj(
                "role",
                "system",
                "content",
                "[EXECUTED RESULT — "
                    + check.path("label").asText()
                    + "]\n"
                    + CompletionChecks.describe(check, artifact.path("value"))));
    }
    if (s.path("memory").size() > 1)
      messages.add(obj("role", "system", "content", "[RAW WORKING BUFFER — PRESERVE VERBATIM]"));
    for (int i = 1; i < s.path("memory").size(); i++)
      messages.add(DurableState.wire(s.path("memory").get(i)));
    for (JsonNode binding : c.at("/execution/argument_bindings")) {
      JsonNode artifact = artifact(s, c, binding.path("source_label").asText());
      if (!artifact.isMissingNode())
        try {
          messages.add(
              obj(
                  "role",
                  "system",
                  "content",
                  "[KNOWN RESULT ARGUMENT BINDING]\n"
                      + binding.path("tool").asText()
                      + "."
                      + binding.path("argument").asText()
                      + " = "
                      + path(artifact.path("value"), binding.path("path"))));
        } catch (Exception ignored) {
        }
    }
    if (c.path("workflow").isObject()) {
      if (s.at("/workflow/active_phase").isNull())
        messages.add(
            obj(
                "role",
                "system",
                "content",
                "[WORKFLOW STATE]\nAll configured phases have verified completion evidence. Tools are forbidden; provide the final answer."));
      else {
        JsonNode p = phase(c, s.at("/workflow/active_phase").asText());
        messages.add(
            obj(
                "role",
                "system",
                "content",
                "[WORKFLOW STATE]\nActive phase: "
                    + p.path("id").asText()
                    + "\n"
                    + p.path("instruction").asText()
                    + "\nOutstanding required evidence:\n"
                    + String.join("\n", missing(s, p))));
      }
    }
    if ((!c.path("workflow").isObject() || s.at("/workflow/active_phase").isNull())
        && !f.path("result_checks").isEmpty())
      messages.add(
          obj(
              "role",
              "system",
              "content",
              "[FINAL SYNTHESIS]\nReturn all required executed artifacts exactly, using the captured tables/numbers/JSON above. Do not use placeholders, duplicate rows, or guessed values."));
    return messages;
  }

  void reject(ExecutionCoordinator x, String code, String feedback) {
    ObjectNode s = x.state;
    DurableState.record(s, "decision_rejected", obj("code", code, "message", feedback));
    s.put("consecutive_invalid_responses", s.path("consecutive_invalid_responses").asInt() + 1);
    s.set("last_error", obj("code", code, "message", feedback, "recoverable", true));
    if (!code.equals("tool_call_rejected"))
      DurableState.message(s, obj("role", "user", "content", "Runtime correction: " + feedback));
    x.persist(
        obj(
            "type",
            "interim_result",
            "step",
            "response_rejected",
            "data",
            obj(
                "code",
                code,
                "message",
                feedback,
                "recoverable",
                true,
                "consecutive_invalid_responses",
                s.path("consecutive_invalid_responses"))));
    if (s.path("consecutive_invalid_responses").asInt()
        >= x.config.at("/execution/max_consecutive_invalid_responses").asInt(3))
      fail(
          x,
          "invalid_response_limit",
          "Consecutive invalid-response limit reached. " + feedback + " Resume to retry.");
    else observeProgress(x);
  }

  void fail(ExecutionCoordinator x, String code, String message) {
    x.state.put("status", "failed");
    x.state.put("final_answer", message);
    x.state.set("last_error", obj("code", code, "message", message, "recoverable", true));
    x.persist(obj("type", "error", "code", code, "message", message, "recoverable", true));
  }

  static String progressFingerprint(ObjectNode s, ObjectNode c) {
    ObjectNode artifacts = obj();
    s.path("completion_artifacts")
        .fields()
        .forEachRemaining(
            e ->
                artifacts.set(
                    e.getKey(),
                    obj(
                        "tool",
                        e.getValue().path("tool"),
                        "arguments",
                        e.getValue().path("arguments"),
                        "value",
                        e.getValue().path("value"))));
    return sha(
        defaultJson(
            arr(
                s.path("workflow"),
                artifacts,
                c.path("workflow").isObject() ? arr() : s.path("progress_seen_outputs"))));
  }

  void observeProgress(ExecutionCoordinator x) {
    ObjectNode s = x.state, c = x.config;
    String current = progressFingerprint(s, c);
    if (!current.equals(s.path("progress_fingerprint").asText())) {
      s.put("progress_fingerprint", current);
      s.put("no_progress_steps", 0);
      return;
    }
    s.put("no_progress_steps", s.path("no_progress_steps").asInt() + 1);
    int limit = c.at("/execution/max_no_progress_steps").asInt(0),
        count = s.path("no_progress_steps").asInt();
    if (limit > 0 && count == Math.max(1, limit / 2)) {
      String message =
          "No new task evidence in "
              + count
              + " decisions. Repeating successful calls does not complete missing requirements.";
      DurableState.message(s, obj("role", "user", "content", message));
      x.persist(
          obj(
              "type",
              "interim_result",
              "step",
              "no_progress_warning",
              "data",
              obj("no_progress_steps", count, "message", message)));
    }
    if (limit > 0 && count >= limit)
      fail(
          x,
          "no_progress_limit",
          "No new task evidence in "
              + limit
              + " decisions. Resume after correcting the workflow or instructions.");
  }

  void compact(ExecutionCoordinator x) {
    ObjectNode s = x.state, c = x.config;
    JsonNode m = c.path("memory");
    for (int pass = 0; pass < m.path("compaction_max_passes").asInt(3); pass++) {
      ObjectNode measured = ModelClient.budget(llm.decisionRequest(c, payload(s, c)), c);
      s.set("request_budget", measured);
      int before = measured.path("estimated_input_tokens").asInt(),
          step = s.path("step_count").asInt();
      boolean hard = !measured.path("fits").asBoolean();
      boolean cadence =
          m.path("compaction_interval_steps").isNumber()
              && step - s.path("last_compaction_step").asInt()
                  >= m.path("compaction_interval_steps").asInt();
      boolean grown =
          !s.hasNonNull("last_compaction_estimated_tokens")
              || before
                  >= s.path("last_compaction_estimated_tokens").asInt()
                      + m.path("compaction_min_growth_tokens").asInt(512);
      boolean soft = before >= m.path("max_tokens").asInt() && grown;
      if (!hard && !cadence && !soft) return;
      var split =
          CompactionContext.split(
              (ArrayNode) s.path("memory"), m.path("raw_turns_to_keep").asInt());
      ArrayNode historical = split.historical(), raw = split.retained();
      String skip = null;
      if (historical.isEmpty()) skip = "no_historical_groups";
      else if (!hard && c.path("workflow").isObject() && s.at("/workflow/active_phase").isNull())
        skip = "completed_workflow";
      else if (!hard
          && s.hasNonNull("last_compaction_attempt_step")
          && step - s.path("last_compaction_attempt_step").asInt()
              < m.path("compaction_min_interval_steps").asInt()) skip = "minimum_spacing";
      if (skip != null) {
        skip(s, skip, hard, before);
        return;
      }
      boolean hybrid = m.path("hybrid_reflection").asBoolean(true);
      String goal = s.path("goal").asText(),
          previous =
              s.hasNonNull("compacted_context") ? s.path("compacted_context").asText() : null;
      while (!historical.isEmpty()
          && !ModelClient.budget(
                  llm.summaryRequest(c, goal, wires(historical), previous, hybrid, null), c)
              .path("fits")
              .asBoolean()) {
        int boundary = historical.size() - 1;
        while (boundary > 0 && historical.get(boundary).path("role").asText().equals("tool"))
          boundary--;
        ArrayNode next = arr();
        for (int i = boundary; i < historical.size(); i++) next.add(historical.get(i));
        next.addAll(raw);
        raw = next;
        while (historical.size() > boundary) historical.remove(historical.size() - 1);
      }
      if (historical.isEmpty()) {
        skip(s, "summarizer_source_unaffordable", hard, before);
        return;
      }
      ObjectNode floor = s.deepCopy();
      ArrayNode floorMemory = arr(s.path("memory").get(0));
      floorMemory.addAll(raw);
      floor.set("memory", floorMemory);
      floor.putNull("compacted_context");
      int floorInput = llm.estimate(c, payload(floor, c)),
          reclaimable = Math.max(0, before - floorInput);
      double pressure =
          (double) before
              / Math.max(
                  1,
                  m.path("context_window_tokens").asInt()
                      - measured.path("reserved_output_tokens").asInt()
                      - m.path("safety_margin_tokens").asInt());
      int requiredReduction =
          hard
              ? 1
              : Math.max(
                  1,
                  Math.max(
                      m.path("optional_compaction_min_reduction_tokens").asInt(),
                      (int)
                          Math.ceil(
                              before
                                  * m.path("optional_compaction_min_reduction_ratio").asDouble())));
      if (!hard && pressure < m.path("optional_compaction_min_pressure_ratio").asDouble()) {
        skip(s, "low_pressure", false, before);
        return;
      }
      if (!hard
          && reclaimable
              < Math.max(
                  m.path("optional_compaction_min_reclaimable_tokens").asInt(),
                  requiredReduction)) {
        skip(s, "insufficient_reclaimable_history", false, before);
        return;
      }
      String source = SessionRepository.source(goal, wires(historical), previous),
          sourceHash = sha(source);
      if (sourceHash.equals(s.path("last_compaction_attempt_sha256").asText())) {
        skip(s, "unchanged_failed_source", hard, before);
        return;
      }
      s.put("last_compaction_attempt_step", step);
      String captured =
          hash(arr(s.path("memory"), s.path("compacted_context"), s.path("step_count")));
      ArrayNode sourceIds = arr(), retainedIds = arr();
      for (JsonNode msg : historical) sourceIds.add(msg.path("message_id"));
      for (JsonNode msg : raw) retainedIds.add(msg.path("message_id"));
      boolean accepted = false;
      int acceptedReduction = 0;
      String feedback = null;
      ObjectNode triggerCounts = s.with("compaction_trigger_counts");
      String trigger = hard ? "hard_budget" : cadence ? "cadence" : "soft_threshold";
      triggerCounts.put(trigger, triggerCounts.path(trigger).asInt() + 1);
      ObjectNode assessment =
          obj(
              "step",
              step,
              "trigger",
              trigger,
              "before_estimated_tokens",
              before,
              "pressure_ratio",
              pressure,
              "protected_estimated_tokens",
              floorInput,
              "reclaimable_estimated_tokens",
              reclaimable,
              "required_reduction_tokens",
              requiredReduction);
      DurableState.diagnostic(
          s,
          "compaction_decisions",
          assessment.deepCopy().put("outcome", "triggered").put("reason", trigger));
      for (int attempt = 0; attempt < m.path("compaction_max_attempts").asInt(2); attempt++) {
        x.checkpoint(true);
        String proposal = null;
        ObjectNode provenance =
            obj(
                "source_message_ids",
                sourceIds,
                "source_sha256",
                sourceHash,
                "previous_ledger_id",
                s.path("active_ledger_id"),
                "retained_message_ids",
                retainedIds);
        try {
          proposal = llm.compact(c, goal, wires(historical), previous, hybrid, feedback).block();
          String normalized =
              CompactionContext.normalize(
                  proposal, goal, source, m.path("max_summary_tokens").asInt(4096), hybrid);
          if (!captured.equals(
              hash(arr(s.path("memory"), s.path("compacted_context"), s.path("step_count")))))
            throw new IllegalArgumentException("Stale compaction source");
          ObjectNode staged = s.deepCopy();
          ArrayNode next = arr(s.path("memory").get(0));
          next.addAll(raw);
          staged.set("memory", next);
          staged.put("compacted_context", normalized);
          int after = llm.estimate(c, payload(staged, c));
          if (before - after < requiredReduction)
            throw new IllegalArgumentException("Insufficient complete-request reduction");
          ObjectNode ledger = ledger(normalized, goal, sourceHash, hybrid);
          provenance.put("proposal", proposal);
          provenance.put("content", normalized);
          provenance.set("ledger", ledger);
          provenance.set(
              "efficiency",
              assessment
                  .deepCopy()
                  .put("after_estimated_tokens", after)
                  .put("reduction_tokens", before - after)
                  .put("reduction_ratio", (double) (before - after) / Math.max(1, before)));
          ObjectNode record = DurableState.record(s, "compaction_summary", provenance);
          s.set("memory", next);
          s.put("compacted_context", normalized);
          s.set("ledger_state", ledger);
          s.set("request_budget", ModelClient.budget(llm.decisionRequest(c, payload(s, c)), c));
          s.set("active_ledger_id", record.path("message_id"));
          s.put("last_compaction_step", step);
          s.put("last_compaction_estimated_tokens", after);
          s.putNull("last_compaction_attempt_sha256");
          DurableState.diagnostic(s, "compaction_events", tree(normalized));
          DurableState.diagnostic(
              s,
              "compaction_attempts",
              obj(
                  "accepted",
                  true,
                  "step_count",
                  step,
                  "before_estimated_tokens",
                  before,
                  "after_estimated_tokens",
                  after,
                  "reduction_tokens",
                  before - after,
                  "source_sha256",
                  sourceHash));
          accepted = true;
          acceptedReduction = before - after;
          break;
        } catch (ExecutionCoordinator.Cancelled | ApiFailure e) {
          throw e;
        } catch (Exception error) {
          feedback = error.getMessage();
          if (proposal == null && error instanceof ModelClient.ModelFailure failure)
            proposal = failure.summaryProposal;
          provenance.set("proposal", tree(proposal));
          provenance.put("reason", feedback == null ? error.getClass().getSimpleName() : feedback);
          DurableState.record(s, "compaction_rejected", provenance);
          DurableState.diagnostic(
              s,
              "compaction_attempts",
              obj(
                  "accepted",
                  false,
                  "step_count",
                  step,
                  "reason",
                  provenance.get("reason"),
                  "source_sha256",
                  sourceHash,
                  "before_estimated_tokens",
                  before));
        }
      }
      if (!accepted) {
        s.put("last_compaction_attempt_sha256", sourceHash);
        return;
      }
      x.persist(
          obj(
              "type",
              "interim_result",
              "step",
              "compaction",
              "data",
              obj(
                  "compaction_count",
                  s.path("diagnostic_counts").path("compaction_events"),
                  "estimated_reduction",
                  acceptedReduction)));
      if (ModelClient.budget(llm.decisionRequest(c, payload(s, c)), c).path("fits").asBoolean())
        return;
    }
  }

  static ArrayNode wires(JsonNode messages) {
    ArrayNode a = arr();
    for (JsonNode message : messages) a.add(DurableState.wire(message));
    return a;
  }

  static void skip(ObjectNode s, String reason, boolean hard, int before) {
    ObjectNode counts = s.with("compaction_skip_counts");
    counts.put(reason, counts.path(reason).asInt() + 1);
    DurableState.diagnostic(
        s,
        "compaction_decisions",
        obj(
            "outcome",
            "skipped",
            "reason",
            reason,
            "hard",
            hard,
            "estimated_input_tokens",
            before,
            "step_count",
            s.path("step_count")));
  }

  static ObjectNode ledger(String canonical, String goal, String sourceHash, boolean hybrid) {
    List<String> sections = new ArrayList<>();
    if (hybrid) {
      for (String tag :
          List.of("USER_GOAL", "GLOBAL_LESSON_LEDGER", "DEAD_ENDS", "CURRENT_LOCAL_PIVOT")) {
        var matcher = Pattern.compile("(?s)<" + tag + ">(.*?)</" + tag + ">").matcher(canonical);
        if (!matcher.find()) throw new IllegalArgumentException("Invalid canonical ledger");
        sections.add(
            matcher
                .group(1)
                .strip()
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&"));
      }
    } else {
      var matcher =
          Pattern.compile(
                  "(?ms)^- (?:Core Objective|Universal Truths Discovered|Dead Ends|Current Local Pivot):\\n(.*?)(?=^- (?:Core Objective|Universal Truths Discovered|Dead Ends|Current Local Pivot):|\\z)")
              .matcher(canonical);
      while (matcher.find()) sections.add(matcher.group(1).strip());
    }
    return obj(
        "goal",
        goal,
        "model_claims",
        sections.get(1).lines().toList(),
        "dead_ends",
        sections.get(2).lines().toList(),
        "hypothesis",
        sections.get(3),
        "source_sha256",
        sourceHash,
        "verification",
        "unverified_model_summary");
  }
}
