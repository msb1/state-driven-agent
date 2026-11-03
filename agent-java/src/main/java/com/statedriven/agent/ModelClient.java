package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.time.Duration;
import java.util.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@Component
final class ModelClient {
  final Environment env;
  final WebClient http =
      WebClient.builder().codecs(c -> c.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)).build();

  ModelClient(ObjectMapper ignored, Environment env) {
    this.env = env;
  }

  ObjectNode decisionRequest(ObjectNode c, ArrayNode messages) {
    boolean none = c.at("/execution/tool_policy").asText().equals("none"),
        single = c.at("/llm/tool_call_mode").asText().equals("single");
    ArrayNode normalizedWire = singleWire(messages);
    ArrayNode wire = single ? normalizedWire : messages.deepCopy();
    ObjectNode body =
        obj(
            "model",
            c.path("model"),
            "messages",
            wire,
            "temperature",
            c.at("/llm/temperature"),
            "max_tokens",
            c.at("/llm/max_output_tokens").isNumber()
                ? c.at("/llm/max_output_tokens")
                : c.at("/memory/reserved_output_tokens"));
    if (none) {
      body.put("tool_choice", "none");
      wire.insert(
          0,
          obj(
              "role",
              "system",
              "content",
              "Tools are disabled for this task. Return the requested final answer directly."));
    } else {
      body.set("tools", toolDefinitions(c));
      body.put("tool_choice", "auto");
      if (single) {
        body.put("parallel_tool_calls", false);
        wire.insert(
            0,
            obj(
                "role",
                "system",
                "content",
                "Call at most one configured function per response. Workflow phase labels are not function names."));
      }
    }
    return body;
  }

  static ArrayNode singleWire(ArrayNode messages) {
    ArrayNode wire = arr();
    for (int i = 0; i < messages.size(); ) {
      JsonNode m = messages.get(i++);
      if (m.path("role").asText().equals("assistant")
          && m.path("tool_calls").isArray()
          && !m.path("tool_calls").isEmpty()) {
        Map<String, JsonNode> results = new HashMap<>();
        for (int j = 0; j < m.path("tool_calls").size(); j++) {
          if (i >= messages.size())
            throw new IllegalArgumentException("Incomplete tool-call history");
          JsonNode r = messages.get(i++);
          if (!r.path("role").asText().equals("tool")
              || results.put(r.path("tool_call_id").asText(), r) != null)
            throw new IllegalArgumentException("Invalid tool-call history");
        }
        Set<String> seen = new HashSet<>();
        for (JsonNode call : m.path("tool_calls")) {
          String id = call.path("id").asText();
          if (id.isBlank() || !seen.add(id) || !results.containsKey(id))
            throw new IllegalArgumentException("Uncorrelated tool history");
          ObjectNode assistant = (ObjectNode) m.deepCopy();
          assistant.set("tool_calls", arr(call));
          if (seen.size() > 1) assistant.remove("content");
          wire.add(assistant);
          wire.add(results.get(id).deepCopy());
        }
      } else {
        if (m.path("role").asText().equals("tool"))
          throw new IllegalArgumentException("Orphaned tool result");
        wire.add(m.deepCopy());
      }
    }
    return wire;
  }

  ArrayNode toolDefinitions(ObjectNode c) {
    ArrayNode tools = arr();
    for (JsonNode t : c.path("tools")) {
      ObjectNode f = (ObjectNode) t.deepCopy();
      f.remove("recovery");
      tools.add(obj("type", "function", "function", f));
    }
    return tools;
  }

  static int estimate(JsonNode body) {
    return encoded(body).getBytes(java.nio.charset.StandardCharsets.UTF_8).length
        + 32 * body.path("messages").size();
  }

  int estimate(ObjectNode c, ArrayNode messages) {
    return estimate(decisionRequest(c, messages));
  }

  static ObjectNode budget(JsonNode body, ObjectNode c) {
    int input = estimate(body),
        reserve =
            Math.max(
                c.at("/memory/reserved_output_tokens").asInt(1024),
                body.path("max_tokens").asInt());
    int margin = c.at("/memory/safety_margin_tokens").asInt(256),
        window = c.at("/memory/context_window_tokens").asInt(32768);
    return obj(
        "estimated_input_tokens",
        input,
        "reserved_output_tokens",
        reserve,
        "safety_margin_tokens",
        margin,
        "context_window_tokens",
        window,
        "fits",
        (long) input + reserve + margin <= window,
        "estimator",
        "UTF-8 compact JSON bytes + 32 per message; conservative estimate, not provider usage");
  }

  Mono<ObjectNode> decide(ObjectNode c, ArrayNode messages) {
    ObjectNode body = decisionRequest(c, messages);
    if (!budget(body, c).path("fits").asBoolean())
      return Mono.error(
          new ModelFailure("context_budget_exceeded", "Request exceeds configured context budget"));
    return post(c, body)
        .map(reply -> normalize(reply, c.at("/llm/tool_call_mode").asText().equals("single")));
  }

  ObjectNode normalize(JsonNode reply, boolean single) {
    JsonNode choice = reply.at("/choices/0"), message = choice.path("message");
    if (choice.isMissingNode() || !message.isObject())
      return obj(
          "type",
          "recoverable_error",
          "code",
          "invalid_model_response",
          "content",
          "Model response must contain choices[0].message.");
    JsonNode calls = message.path("tool_calls");
    if (!calls.isMissingNode() && !calls.isNull() && !calls.isArray())
      return obj(
          "type",
          "recoverable_error",
          "code",
          "invalid_tool_calls",
          "content",
          "tool_calls must be an array.");
    if (calls.isArray() && !calls.isEmpty()) {
      ArrayNode normalized = arr();
      for (JsonNode call : calls) {
        String raw = call.at("/function/arguments").asText();
        ObjectNode item =
            obj("id", call.path("id"), "name", call.at("/function/name"), "raw_arguments", raw);
        try {
          item.set("arguments", parse(raw));
        } catch (Exception error) {
          item.put("error", "Invalid JSON tool arguments: " + error.getMessage());
          item.putNull("arguments");
        }
        normalized.add(item);
      }
      return obj("type", "tools", "calls", normalized);
    }
    String text = message.path("content").asText("");
    if (!message.path("content").isTextual() || text.isBlank())
      return obj(
          "type",
          "recoverable_error",
          "code",
          "empty_response",
          "content",
          "Return a nonempty action or final answer.",
          "candidate_content",
          message.path("content").isTextual() ? text : null);
    return obj("type", "final", "content", text);
  }

  ObjectNode summaryRequest(
      ObjectNode c,
      String goal,
      ArrayNode historical,
      String existing,
      boolean hybrid,
      String feedback) {
    int limit = c.at("/memory/max_summary_tokens").asInt(4096);
    String prompt = "Summarize only the supplied historical prefix and previous ledger. Copy the original goal exactly. Do not reconstruct recent raw messages. Carry forward prior useful lessons and dead ends. Keep provisional hypotheses separate from observations; do not present guesses as verified facts. Use only numbers appearing in the source. Do not calculate or invent statistics. Fill every section, using '- None.' if empty. No prose outside the structure. Escape XML special characters when using the XML format. Keep output short, within " + limit + " UTF-8 bytes. ";
    prompt += hybrid
        ? "Return one XML document with root COMPACTED_STATE and exactly four text-only child elements, in this order: USER_GOAL, GLOBAL_LESSON_LEDGER, DEAD_ENDS, CURRENT_LOCAL_PIVOT. Use this complete first child verbatim: <USER_GOAL>" + CompactionContext.escape(goal.strip()) + "</USER_GOAL>. Write concise source-grounded lessons in GLOBAL_LESSON_LEDGER, failed approaches in DEAD_ENDS, and a provisional next direction in CURRENT_LOCAL_PIVOT. Close every element and the root with its matching closing tag. Escape XML special characters in section text. Do not emit Markdown fences, tool calls, JSON messages, nested elements, or placeholder text."
        : "Return exactly four ordered headings: '- Core Objective:', '- Universal Truths Discovered:', '- Dead Ends:', '- Current Local Pivot:'. Put the original goal verbatim below the first heading, then source-grounded lessons, failed approaches, and a provisional next direction below the others. Do not emit placeholder text or tool calls.";
    if (feedback != null && !feedback.isEmpty())
      prompt += " Previous replacement rejected: " + feedback + ". Generate a complete corrected replacement from the source. Preserve the full original goal; replace every placeholder with source-grounded text or '- None.' and check all closing tags.";
    return obj(
        "model",
        c.path("model"),
        "temperature",
        0,
        "max_tokens",
        c.at("/memory/summary_output_tokens").asInt(512),
        "messages",
        arr(
            obj("role", "system", "content", prompt),
            obj(
                "role",
                "user",
                "content",
                SessionRepository.source(goal, historical, existing))));
  }

  Mono<String> compact(
      ObjectNode c, String goal, ArrayNode historical, String existing, boolean hybrid) {
    return compact(c, goal, historical, existing, hybrid, null);
  }

  Mono<String> compact(
      ObjectNode c,
      String goal,
      ArrayNode historical,
      String existing,
      boolean hybrid,
      String feedback) {
    ObjectNode body = summaryRequest(c, goal, historical, existing, hybrid, feedback);
    if (!budget(body, c).path("fits").asBoolean())
      return Mono.error(
          new ModelFailure("context_budget_exceeded", "Summary request exceeds context budget"));
    return post(c, body)
        .map(
            reply -> {
              String text = reply.at("/choices/0/message/content").asText();
              if (reply.at("/choices/0/finish_reason").asText().equals("length")) {
                ModelFailure failure =
                    new ModelFailure("invalid_summary", "Summary response was truncated");
                failure.summaryProposal = text;
                throw failure;
              }
              if (text.isBlank())
                throw new ModelFailure("empty_response", "Empty summary response");
              return text;
            });
  }

  Mono<JsonNode> post(ObjectNode c, ObjectNode body) {
    String base = c.at("/llm/base_url").asText().replaceAll("/+$", "");
    String primary = base.endsWith("/chat/completions") ? base : base + "/chat/completions";
    Duration timeout = Duration.ofMillis((long) (c.at("/llm/timeout_seconds").asDouble(90) * 1000));
    String key =
        env.getProperty(c.at("/llm/api_key_env").asText("OPENAI_API_KEY"), "local-not-required");
    Mono<JsonNode> request = postUrl(primary, key, body, timeout);
    if (base.endsWith("/v1"))
      request =
          request.onErrorResume(
              WebClientResponseException.NotFound.class,
              e ->
                  postUrl(
                      base.substring(0, base.length() - 3) + "/chat/completions",
                      key,
                      body,
                      timeout));
    return request.onErrorMap(ModelClient::classify);
  }

  static ModelFailure classify(Throwable error) {
    if (error instanceof ModelFailure failure) return failure;
    if (error instanceof WebClientResponseException response) {
      JsonNode body;
      try {
        body = parse(response.getResponseBodyAsString());
      } catch (Exception invalid) {
        body = obj();
      }
      String text =
          body.path("error").isObject()
              ? body.at("/error/message").asText()
              : body.path("error").asText();
      String lower = text.toLowerCase(Locale.ROOT);
      ModelFailure failure =
          lower.contains("single tool-call")
                  || lower.contains("single tool call")
                  || lower.contains("jinja")
                  || lower.contains("rendering prompt")
              ? new ModelFailure(
                  "model_template_error", "Model prompt template could not render the request.")
              : new ModelFailure(
                  "model_http_error",
                  "Model service returned HTTP " + response.getStatusCode().value() + ".");
      if (failure.code.equals("model_template_error"))
        failure.diagnostic.set(
            "details",
            obj(
                "reason",
                lower.contains("single tool-call") || lower.contains("single tool call")
                    ? "multiple_tool_calls_unsupported"
                    : "prompt_render_failed"));
      if (body.hasNonNull("proxy_transport_error")) failure = proxyFailure(body);
      failure.diagnostic.put("http_status", response.getStatusCode().value());
      return failure;
    }
    for (Throwable cause = error; cause != null; cause = cause.getCause())
      if (cause.getClass().getSimpleName().contains("Timeout"))
        return new ModelFailure("model_timeout", "Model request timed out.");
    return new ModelFailure("model_transport_error", "Model transport failed.");
  }

  static ModelFailure proxyFailure(JsonNode body) {
    boolean timeout = body.path("proxy_transport_error").asText().contains("Timeout");
    return new ModelFailure(
        timeout ? "model_timeout" : "model_transport_error",
        timeout ? "Upstream model request timed out." : "Upstream model transport failed.");
  }

  Mono<JsonNode> postUrl(String url, String key, ObjectNode body, Duration timeout) {
    return http.post()
        .uri(url)
        .header("Authorization", "Bearer " + key)
        .bodyValue(body)
        .retrieve()
        .bodyToMono(String.class)
        .map(
            raw -> {
              JsonNode reply;
              try {
                reply = parse(raw);
              } catch (Exception error) {
                throw new ModelFailure(
                    "invalid_model_response", "Model service returned invalid JSON data.");
              }
              if (reply.hasNonNull("proxy_transport_error")) throw proxyFailure(reply);
              return reply;
            })
        .timeout(timeout);
  }

  static final class ModelFailure extends RuntimeException {
    final String code;
    final ObjectNode diagnostic;
    String summaryProposal;

    ModelFailure(String code, String message) {
      super(message);
      this.code = code;
      String category =
          code.equals("model_template_error")
              ? "template"
              : code.equals("model_timeout")
                  ? "timeout"
                  : code.equals("model_transport_error")
                      ? "transport"
                      : code.equals("model_http_error")
                          ? "provider"
                          : code.contains("summary") || code.startsWith("context_")
                              ? "context"
                              : "response";
      diagnostic =
          obj(
              "code",
              code,
              "message",
              message,
              "recoverable",
              true,
              "category",
              category,
              "http_status",
              null,
              "details",
              obj());
    }
  }
}
