package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import java.util.*;
import java.util.regex.Pattern;

final class SchemaChecks {
  static final Set<String> KEYS =
      Set.of(
          "type",
          "properties",
          "required",
          "additionalProperties",
          "items",
          "enum",
          "const",
          "minimum",
          "maximum",
          "exclusiveMinimum",
          "exclusiveMaximum",
          "minLength",
          "maxLength",
          "pattern",
          "minItems",
          "maxItems",
          "description",
          "title",
          "default",
          "examples",
          "$schema");

  static void schema(JsonNode s) {
    if (!s.isObject()) throw new IllegalArgumentException("Schema must be an object");
    s.fieldNames()
        .forEachRemaining(
            k -> {
              if (!KEYS.contains(k))
                throw new IllegalArgumentException("Unsupported schema keyword: " + k);
            });
    if (s.has("type")
        && !Set.of("object", "array", "string", "boolean", "integer", "number", "null")
            .contains(s.path("type").asText()))
      throw new IllegalArgumentException("Unsupported schema type");
    if (s.has("properties") && !s.path("properties").isObject())
      throw new IllegalArgumentException("Invalid properties");
    s.path("properties").forEach(SchemaChecks::schema);
    if (s.has("required")) {
      if (!s.path("required").isArray()) throw new IllegalArgumentException("Invalid required");
      for (JsonNode v : s.path("required"))
        if (!v.isTextual()) throw new IllegalArgumentException("Invalid required name");
    }
    if (s.has("items")) schema(s.get("items"));
    if (s.has("additionalProperties") && !s.get("additionalProperties").isBoolean())
      schema(s.get("additionalProperties"));
    if (s.has("enum") && (!s.get("enum").isArray() || s.get("enum").isEmpty()))
      throw new IllegalArgumentException("Empty enum");
    for (String k : List.of("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"))
      if (s.has(k) && (!s.get(k).isNumber() || !Double.isFinite(s.get(k).doubleValue())))
        throw new IllegalArgumentException("Invalid numeric boundary");
    for (String k : List.of("minLength", "maxLength", "minItems", "maxItems"))
      if (s.has(k) && (!s.get(k).isIntegralNumber() || s.get(k).asLong() < 0))
        throw new IllegalArgumentException("Invalid length boundary");
    if (s.has("pattern")) {
      if (!s.get("pattern").isTextual()) throw new IllegalArgumentException("Invalid pattern");
      Pattern.compile(s.get("pattern").asText());
    }
  }

  static List<String> validate(JsonNode v, JsonNode s) {
    List<String> errors = new ArrayList<>();
    validate(v, s, "$", errors);
    return errors;
  }

  private static void validate(JsonNode v, JsonNode s, String path, List<String> errors) {
    String type = s.path("type").asText();
    boolean numeric = v.isNumber() && Double.isFinite(v.doubleValue());
    boolean ok =
        switch (type) {
          case "object" -> v.isObject();
          case "array" -> v.isArray();
          case "string" -> v.isTextual();
          case "boolean" -> v.isBoolean();
          case "number" -> numeric;
          case "integer" -> numeric && v.decimalValue().stripTrailingZeros().scale() <= 0;
          case "null" -> v.isNull();
          default -> true;
        };
    if (!ok) {
      errors.add(path + ": expected " + type + " without coercion");
      return;
    }
    if (s.has("enum")) {
      boolean match = false;
      for (JsonNode item : s.get("enum")) if (equal(v, item)) match = true;
      if (!match) errors.add(path + ": invalid enum");
    }
    if (s.has("const") && !equal(v, s.get("const"))) errors.add(path + ": invalid constant");
    if (v.isObject()) {
      for (JsonNode key : s.path("required"))
        if (!v.has(key.asText())) errors.add(path + "." + key.asText() + ": required");
      var it = v.fields();
      while (it.hasNext()) {
        var e = it.next();
        if (s.path("properties").has(e.getKey()))
          validate(
              e.getValue(), s.path("properties").get(e.getKey()), path + "." + e.getKey(), errors);
        else if (s.path("additionalProperties").isBoolean()
            && !s.path("additionalProperties").asBoolean())
          errors.add(path + "." + e.getKey() + ": additional property");
        else if (s.path("additionalProperties").isObject())
          validate(e.getValue(), s.get("additionalProperties"), path + "." + e.getKey(), errors);
      }
    }
    if (numeric)
      for (String key : List.of("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"))
        if (s.has(key)) {
          int cmp = v.decimalValue().compareTo(s.get(key).decimalValue());
          if (switch (key) {
            case "minimum" -> cmp < 0;
            case "maximum" -> cmp > 0;
            case "exclusiveMinimum" -> cmp <= 0;
            default -> cmp >= 0;
          }) errors.add(path + ": violates " + key);
        }
    if (v.isTextual()) {
      int length = v.asText().codePointCount(0, v.asText().length());
      if (length < s.path("minLength").asInt(0)
          || length > s.path("maxLength").asInt(Integer.MAX_VALUE))
        errors.add(path + ": string length");
      if (s.has("pattern")
          && !Pattern.compile(s.get("pattern").asText()).matcher(v.asText()).find())
        errors.add(path + ": pattern");
    }
    if (v.isArray()) {
      if (v.size() < s.path("minItems").asInt(0)
          || v.size() > s.path("maxItems").asInt(Integer.MAX_VALUE))
        errors.add(path + ": array length");
      if (s.has("items"))
        for (int i = 0; i < v.size(); i++)
          validate(v.get(i), s.get("items"), path + "[" + i + "]", errors);
    }
  }
}
