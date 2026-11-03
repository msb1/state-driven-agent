package com.statedriven.agent;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
final class ConfigRepository {
  record Snapshot(String ref, String sha256, ObjectNode config) {}

  private static final Pattern ENV = Pattern.compile("\\$\\{([A-Z0-9_]+)(?::-([^}]*))?}");
  private final ObjectMapper yaml =
      new ObjectMapper(new YAMLFactory())
          .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private final Path root;
  private final Environment environment;
  private final String mainModel;
  private final Map<String, String> dotenv;

  @org.springframework.beans.factory.annotation.Autowired
  ConfigRepository(ObjectMapper ignored, Environment environment) {
    this(
        ignored,
        environment,
        Files.isRegularFile(Path.of("../contract.md")) ? Path.of("../.env") : Path.of(".env"));
  }

  ConfigRepository(ObjectMapper ignored, Environment environment, Path environmentFile) {
    this.environment = environment;
    Path fallback = Path.of("../config").toAbsolutePath().normalize();
    Path fromRepo = Path.of("config").toAbsolutePath().normalize();
    String configured = environment.getProperty("AGENT_CONFIG_ROOT");
    root =
        Path.of(
                configured == null
                    ? (Files.isDirectory(fallback) ? fallback : fromRepo).toString()
                    : configured)
            .toAbsolutePath()
            .normalize();
    dotenv = dotenv(environmentFile);
    if (environment instanceof org.springframework.core.env.ConfigurableEnvironment configurable)
      configurable.getPropertySources().addLast(
          new org.springframework.core.env.MapPropertySource("agentDotenv", new java.util.HashMap<>(dotenv)));
    mainModel = dotenv.get("MAIN_MODEL");
  }

  Path root() {
    return root;
  }

  Snapshot resolve(String ref) {
    try {
      if (ref == null || ref.isBlank() || Path.of(ref).isAbsolute())
        throw new IllegalArgumentException("config_ref must be a non-empty relative YAML path");
      Path p = root.resolve(ref).normalize();
      if (!p.startsWith(root)
          || !p.toRealPath().startsWith(root.toRealPath())
          || !Files.isRegularFile(p)
          || !(ref.endsWith(".yaml") || ref.endsWith(".yml")))
        throw new IllegalArgumentException(
            "config_ref must identify a YAML file within AGENT_CONFIG_ROOT");
      byte[] b = Files.readAllBytes(p);
      String source =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
              .decode(java.nio.ByteBuffer.wrap(b))
              .toString();
      JsonNode raw = expandNode(yaml.readTree(source));
      if (raw == null || !raw.path("agent").isObject())
        throw new IllegalArgumentException("config must contain an 'agent' mapping");
      ObjectNode a = (ObjectNode) raw.path("agent");
      normalize(a);
      validate(a);
      return new Snapshot(
          root.relativize(p).toString().replace('\\', '/'),
          hex(MessageDigest.getInstance("SHA-256").digest(b)),
          a);
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  List<Snapshot> list() {
    try (var paths = Files.walk(root)) {
      return paths
          .filter(
              p ->
                  Files.isRegularFile(p)
                      && (p.toString().endsWith(".yaml") || p.toString().endsWith(".yml")))
          .sorted()
          .map(
              p -> {
                try {
                  return resolve(root.relativize(p).toString());
                } catch (Exception e) {
                  return null;
                }
              })
          .filter(Objects::nonNull)
          .toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  private JsonNode expandNode(JsonNode n) {
    if (n.isTextual())
      return com.fasterxml.jackson.databind.node.TextNode.valueOf(expand(n.asText()));
    if (n.isObject()) {
      ObjectNode o = (ObjectNode) n;
      var keys = new ArrayList<String>();
      o.fieldNames().forEachRemaining(keys::add);
      for (String k : keys) o.set(k, expandNode(o.get(k)));
    } else if (n.isArray())
      for (int i = 0; i < n.size(); i++)
        ((com.fasterxml.jackson.databind.node.ArrayNode) n).set(i, expandNode(n.get(i)));
    return n;
  }

  private static void defaults(ObjectNode n, ObjectNode d) {
    d.fields()
        .forEachRemaining(
            e -> {
              if (!n.has(e.getKey())) n.set(e.getKey(), e.getValue());
            });
  }

  static final ObjectNode MEMORY_DEFAULTS =
      JsonSupport.obj(
          "hybrid_reflection",
          true,
          "run_max_steps",
          null,
          "compaction_interval_steps",
          null,
          "context_window_tokens",
          32768,
          "reserved_output_tokens",
          1024,
          "safety_margin_tokens",
          256,
          "max_summary_tokens",
          4096,
          "summary_output_tokens",
          512,
          "compaction_max_attempts",
          2,
          "compaction_max_passes",
          3,
          "compaction_min_growth_tokens",
          512,
          "compaction_min_interval_steps",
          0,
          "optional_compaction_min_pressure_ratio",
          0.0,
          "optional_compaction_min_reclaimable_tokens",
          0,
          "optional_compaction_min_reduction_tokens",
          0,
          "optional_compaction_min_reduction_ratio",
          0.0);

  static void normalize(ObjectNode a) {
    for (String k : List.of("name", "system_prompt", "model", "llm", "memory", "tools"))
      if (!a.has(k)) throw new IllegalArgumentException("agent missing " + k);
    defaults(
        (ObjectNode) a.path("llm"),
        JsonSupport.obj(
            "api_key_env",
            "OPENAI_API_KEY",
            "temperature",
            0.1,
            "timeout_seconds",
            90.0,
            "tool_call_mode",
            "native",
            "max_output_tokens",
            null));
    defaults((ObjectNode) a.path("memory"), MEMORY_DEFAULTS.deepCopy());
    if (!a.has("output")) a.putObject("output");
    defaults((ObjectNode) a.path("output"), JsonSupport.obj("verbose_setup", false));
    if (!a.has("execution")) a.putObject("execution");
    defaults(
        (ObjectNode) a.path("execution"),
        JsonSupport.obj(
            "argument_bindings",
            JsonSupport.arr(),
            "tool_policy",
            "auto",
            "max_consecutive_invalid_responses",
            3,
            "max_no_progress_steps",
            null));
    if (!a.has("completion")) a.putObject("completion");
    defaults(
        (ObjectNode) a.path("completion"),
        JsonSupport.obj(
            "instructions",
            null,
            "forbidden_patterns",
            JsonSupport.arr(),
            "result_checks",
            JsonSupport.arr(),
            "exact",
            null,
            "pattern",
            null,
            "json_schema",
            null));
    for (String key : List.of("temperature", "timeout_seconds"))
      if (a.path("llm").path(key).isNumber())
        ((ObjectNode) a.path("llm")).put(key, a.path("llm").path(key).asDouble());
    for (String key :
        List.of(
            "optional_compaction_min_pressure_ratio", "optional_compaction_min_reduction_ratio"))
      if (a.path("memory").path(key).isNumber())
        ((ObjectNode) a.path("memory")).put(key, a.path("memory").path(key).asDouble());
    if (!a.has("workflow")) a.putNull("workflow");
    for (JsonNode tool : a.path("tools")) {
      ObjectNode t = (ObjectNode) tool;
      if (!t.has("recovery")) t.putNull("recovery");
      else if (t.path("recovery").isObject())
        defaults(
            (ObjectNode) t.path("recovery"),
            JsonSupport.obj("mode", "reconcile", "max_attempts", 2));
    }
    for (JsonNode check : a.at("/completion/result_checks"))
      defaults(
          (ObjectNode) check,
          JsonSupport.obj(
              "arguments",
              JsonSupport.obj(),
              "result",
              JsonSupport.obj(),
              "path",
              JsonSupport.arr(),
              "answer_path",
              JsonSupport.arr()));
    for (JsonNode binding : a.at("/execution/argument_bindings"))
      defaults((ObjectNode) binding, JsonSupport.obj("path", JsonSupport.arr()));
    for (JsonNode phase : a.at("/workflow/phases")) {
      defaults(
          (ObjectNode) phase,
          JsonSupport.obj(
              "allowed_tools",
              null,
              "completion",
              JsonSupport.arr(),
              "transitions",
              JsonSupport.arr()));
      for (JsonNode transition : phase.path("transitions")) {
        defaults(
            (ObjectNode) transition,
            JsonSupport.obj("when", "phase_complete", "evidence", JsonSupport.arr()));
        for (JsonNode evidence : transition.path("evidence")) evidenceDefaults(evidence);
      }
      for (JsonNode evidence : phase.path("completion")) evidenceDefaults(evidence);
    }
  }

  private static void evidenceDefaults(JsonNode n) {
    defaults(
        (ObjectNode) n,
        JsonSupport.obj(
            "required",
            true,
            "arguments",
            JsonSupport.obj(),
            "result",
            JsonSupport.obj(),
            "content_contains",
            JsonSupport.arr()));
  }

  static String resolvedHash(ObjectNode c) {
    ObjectNode n = c.deepCopy();
    ObjectNode l = (ObjectNode) n.path("llm"),
        e = (ObjectNode) n.path("execution"),
        m = (ObjectNode) n.path("memory"),
        f = (ObjectNode) n.path("completion");
    if (l.path("tool_call_mode").asText().equals("native")) l.remove("tool_call_mode");
    if (l.path("max_output_tokens").isNull()) l.remove("max_output_tokens");
    if (e.path("tool_policy").asText().equals("auto")) e.remove("tool_policy");
    if (e.path("max_no_progress_steps").isNull()) e.remove("max_no_progress_steps");
    for (String k : List.of("argument_bindings")) if (e.path(k).isEmpty()) e.remove(k);
    for (String k : List.of("result_checks", "forbidden_patterns"))
      if (f.path(k).isEmpty()) f.remove(k);
    if (f.path("instructions").isNull()) f.remove("instructions");
    for (JsonNode t : n.path("tools"))
      if (t.path("recovery").isNull()) ((ObjectNode) t).remove("recovery");
    MEMORY_DEFAULTS
        .fields()
        .forEachRemaining(
            x -> {
              if (!x.getKey().equals("hybrid_reflection")
                  && JsonSupport.equal(m.path(x.getKey()), x.getValue())) m.remove(x.getKey());
            });
    return JsonSupport.sha(JsonSupport.encoded(n));
  }

  static void validate(ObjectNode a) {
    validateShapes(a);
    Set<String> tools = new HashSet<>();
    for (JsonNode t : a.path("tools")) {
      String name = t.path("name").asText();
      if (name.isBlank() || !tools.add(name) || !FixtureTools.NAMES.contains(name))
        throw new IllegalArgumentException("Duplicate or unavailable tool: " + name);
      SchemaChecks.schema(t.path("parameters"));
      if (!t.at("/parameters/type").asText().equals("object"))
        throw new IllegalArgumentException("Tool parameters must be object");
      if (t.path("recovery").isObject()) {
        if (!Set.of("read_only", "repeatable", "idempotent", "reconcile")
            .contains(t.at("/recovery/mode").asText()))
          throw new IllegalArgumentException("Invalid recovery mode");
        range(t.path("recovery"), "max_attempts", 1, 10);
      }
    }
    if (!Set.of("native", "single").contains(a.at("/llm/tool_call_mode").asText())
        || !Set.of("auto", "none").contains(a.at("/execution/tool_policy").asText()))
      throw new IllegalArgumentException("Invalid tool policy");
    if (a.at("/llm/timeout_seconds").asDouble() <= 0)
      throw new IllegalArgumentException("Invalid timeout");
    if (!a.at("/llm/max_output_tokens").isNull())
      range(a.path("llm"), "max_output_tokens", 1, Integer.MAX_VALUE);
    JsonNode mem = a.path("memory");
    for (String k :
        List.of(
            "max_tokens",
            "max_steps",
            "raw_turns_to_keep",
            "context_window_tokens",
            "reserved_output_tokens",
            "max_summary_tokens",
            "summary_output_tokens",
            "compaction_min_growth_tokens")) range(mem, k, 1, Integer.MAX_VALUE);
    for (String k :
        List.of(
            "safety_margin_tokens",
            "compaction_min_interval_steps",
            "optional_compaction_min_reclaimable_tokens",
            "optional_compaction_min_reduction_tokens")) range(mem, k, 0, Integer.MAX_VALUE);
    range(mem, "compaction_max_attempts", 1, 5);
    range(mem, "compaction_max_passes", 1, 10);
    for (String k : List.of("run_max_steps", "compaction_interval_steps"))
      if (!mem.path(k).isNull()) range(mem, k, 1, Integer.MAX_VALUE);
    for (String k :
        List.of(
            "optional_compaction_min_pressure_ratio", "optional_compaction_min_reduction_ratio"))
      if (!mem.path(k).isNumber() || mem.path(k).asDouble() < 0 || mem.path(k).asDouble() > 1)
        throw new IllegalArgumentException("Invalid ratio");
    if (mem.path("reserved_output_tokens").asLong() + mem.path("safety_margin_tokens").asLong()
        >= mem.path("context_window_tokens").asLong())
      throw new IllegalArgumentException("Invalid context budget");
    range(a.path("execution"), "max_consecutive_invalid_responses", 1, 50);
    if (!a.at("/execution/max_no_progress_steps").isNull())
      range(a.path("execution"), "max_no_progress_steps", 2, Integer.MAX_VALUE);
    Set<String> labels = new HashSet<>();
    for (JsonNode check : a.at("/completion/result_checks")) {
      if (!tools.contains(check.path("tool").asText())
          || check.path("label").asText().isBlank()
          || !labels.add(check.path("label").asText())
          || !Set.of("markdown_table", "number", "json_value")
              .contains(check.path("kind").asText()))
        throw new IllegalArgumentException("Invalid result check");
    }
    Set<String> bindings = new HashSet<>();
    for (JsonNode binding : a.at("/execution/argument_bindings")) {
      JsonNode tool = null;
      for (JsonNode t : a.path("tools"))
        if (t.path("name").asText().equals(binding.path("tool").asText())) tool = t;
      if (tool == null
          || !tool.at("/parameters/properties").has(binding.path("argument").asText())
          || !labels.contains(binding.path("source_label").asText())
          || !bindings.add(binding.path("tool").asText() + ":" + binding.path("argument").asText()))
        throw new IllegalArgumentException("Invalid argument binding");
    }
    for (String key : List.of("pattern"))
      if (!a.path("completion").path(key).isNull())
        Pattern.compile(a.path("completion").path(key).asText());
    for (JsonNode pattern : a.at("/completion/forbidden_patterns"))
      Pattern.compile(pattern.asText());
    if (!a.at("/completion/json_schema").isNull())
      SchemaChecks.schema(a.at("/completion/json_schema"));
    if (!a.at("/completion/exact").isNull() && a.at("/completion/exact").asText().isBlank())
      throw new IllegalArgumentException("Empty exact answer");
    JsonNode w = a.path("workflow");
    if (w.isNull()) return;
    Set<String> ids = new HashSet<>();
    for (JsonNode p : w.path("phases"))
      if (p.path("id").asText().isBlank() || !ids.add(p.path("id").asText()))
        throw new IllegalArgumentException("workflow phase IDs must be unique");
    if (!ids.contains(w.path("entry_phase").asText()))
      throw new IllegalArgumentException("Invalid workflow entry");
    for (JsonNode p : w.path("phases")) {
      for (JsonNode tool : p.path("allowed_tools"))
        if (!tools.contains(tool.asText()))
          throw new IllegalArgumentException("Unknown allowed tool");
      for (JsonNode evidence : p.path("completion"))
        if (!tools.contains(evidence.path("tool").asText()))
          throw new IllegalArgumentException("Unknown evidence tool");
      for (JsonNode t : p.path("transitions")) {
        String to = t.path("to").asText();
        if (!"complete".equals(to) && !ids.contains(to))
          throw new IllegalArgumentException("Unknown phase edge");
        if (!Set.of("always", "evidence", "phase_complete").contains(t.path("when").asText())
            || ("evidence".equals(t.path("when").asText()) && t.path("evidence").isEmpty()))
          throw new IllegalArgumentException("Invalid evidence transition");
        for (JsonNode evidence : t.path("evidence"))
          if (!tools.contains(evidence.path("tool").asText()))
            throw new IllegalArgumentException("Unknown evidence tool");
      }
    }
  }

  private static void shape(boolean valid, String description) {
    if (!valid) throw new IllegalArgumentException("Invalid " + description);
  }

  private static void text(JsonNode n, String key) {
    shape(n.path(key).isTextual(), key + " string");
  }

  private static void array(JsonNode n, String key) {
    shape(n.path(key).isArray(), key + " array");
  }

  private static void pathShape(JsonNode n, String key) {
    array(n, key);
    for (JsonNode part : n.path(key))
      shape(part.isTextual() || part.isIntegralNumber(), key + " path component");
  }

  private static void evidenceShape(JsonNode n) {
    shape(n.isObject(), "evidence");
    text(n, "tool");
    text(n, "description");
    shape(n.path("required").isBoolean(), "required flag");
    shape(n.path("arguments").isObject() && n.path("result").isObject(), "evidence constraints");
    array(n, "content_contains");
    for (JsonNode term : n.path("content_contains")) shape(term.isTextual(), "evidence text");
  }

  private static void validateShapes(ObjectNode a) {
    JsonSupport.validate(a);
    for (String k : List.of("name", "system_prompt", "model")) text(a, k);
    for (String k : List.of("llm", "memory", "execution", "completion", "output"))
      shape(a.path(k).isObject(), k + " object");
    array(a, "tools");
    JsonNode l = a.path("llm"),
        m = a.path("memory"),
        e = a.path("execution"),
        f = a.path("completion");
    for (String k : List.of("base_url", "api_key_env", "tool_call_mode")) text(l, k);
    shape(
        l.path("temperature").isNumber() && l.path("timeout_seconds").isNumber(),
        "model numeric policy");
    shape(
        m.path("hybrid_reflection").isBoolean() && a.at("/output/verbose_setup").isBoolean(),
        "boolean policy");
    for (JsonNode tool : a.path("tools")) {
      shape(tool.isObject(), "tool declaration");
      text(tool, "name");
      text(tool, "description");
      shape(tool.path("parameters").isObject(), "tool schema");
      shape(tool.path("recovery").isObject() || tool.path("recovery").isNull(), "recovery policy");
    }
    array(e, "argument_bindings");
    for (JsonNode b : e.path("argument_bindings")) {
      shape(b.isObject(), "argument binding");
      for (String k : List.of("tool", "argument", "source_label")) text(b, k);
      pathShape(b, "path");
    }
    array(f, "result_checks");
    array(f, "forbidden_patterns");
    for (JsonNode pattern : f.path("forbidden_patterns"))
      shape(pattern.isTextual(), "forbidden pattern");
    for (String k : List.of("instructions", "exact", "pattern"))
      shape(f.path(k).isNull() || f.path(k).isTextual(), k + " completion string");
    for (JsonNode check : f.path("result_checks")) {
      shape(check.isObject(), "result check");
      for (String k : List.of("kind", "tool", "label")) text(check, k);
      shape(
          check.path("arguments").isObject() && check.path("result").isObject(),
          "result constraints");
      pathShape(check, "path");
      pathShape(check, "answer_path");
    }
    JsonNode w = a.path("workflow");
    if (w.isNull()) return;
    shape(w.isObject(), "workflow");
    text(w, "entry_phase");
    array(w, "phases");
    for (JsonNode phase : w.path("phases")) {
      shape(phase.isObject(), "workflow phase");
      for (String k : List.of("id", "name", "instruction")) text(phase, k);
      shape(
          phase.path("allowed_tools").isNull() || phase.path("allowed_tools").isArray(),
          "allowed_tools");
      for (JsonNode tool : phase.path("allowed_tools")) shape(tool.isTextual(), "allowed tool");
      array(phase, "completion");
      array(phase, "transitions");
      for (JsonNode evidence : phase.path("completion")) evidenceShape(evidence);
      for (JsonNode transition : phase.path("transitions")) {
        shape(transition.isObject(), "transition");
        text(transition, "to");
        text(transition, "when");
        array(transition, "evidence");
        for (JsonNode evidence : transition.path("evidence")) evidenceShape(evidence);
      }
    }
  }

  private static void range(JsonNode n, String key, int min, int max) {
    JsonNode v = n.path(key);
    if (!v.isIntegralNumber() || v.asLong() < min || v.asLong() > max)
      throw new IllegalArgumentException("Invalid " + key);
  }

  private static Map<String, String> dotenv(Path file) {
    Map<String, String> values = new HashMap<>();
    try {
      for (String line : Files.readAllLines(file)) {
        Matcher m =
            Pattern.compile("^\\s*(?:export\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(.*?)\\s*$").matcher(line);
        if (m.matches()) {
          String value = m.group(2);
          if (value.length() > 1
              && ((value.startsWith("\"") && value.endsWith("\""))
                  || (value.startsWith("'") && value.endsWith("'"))))
            value = value.substring(1, value.length() - 1);
          else value = value.replaceFirst("\\s+#.*$", "");
          values.put(m.group(1), value);
        }
      }
    } catch (java.io.IOException ignored) {
    }
    return values;
  }

  private String expand(String s) {
    Matcher m = ENV.matcher(s);
    StringBuffer b = new StringBuffer();
    while (m.find())
      m.appendReplacement(
          b,
          Matcher.quoteReplacement(
              m.group(1).equals("MAIN_MODEL") && mainModel != null
                  ? mainModel
                  : environment.getProperty(m.group(1), dotenv.getOrDefault(m.group(1), m.group(2) == null ? "" : m.group(2)))));
    m.appendTail(b);
    return b.toString();
  }

  private static String hex(byte[] b) {
    StringBuilder s = new StringBuilder();
    for (byte x : b) s.append(String.format("%02x", x));
    return s.toString();
  }
}
