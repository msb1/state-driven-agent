package com.statedriven.agent;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.erdtman.jcs.JsonCanonicalizer;

final class JsonSupport {
  static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  static ObjectNode obj(Object... pairs) {
    ObjectNode n = JSON.createObjectNode();
    for (int i = 0; i < pairs.length; i += 2) n.set((String) pairs[i], tree(pairs[i + 1]));
    return n;
  }

  static ArrayNode arr(Object... values) {
    ArrayNode n = JSON.createArrayNode();
    for (Object v : values) n.add(tree(v));
    return n;
  }

  static JsonNode tree(Object v) {
    return v instanceof JsonNode n ? n : JSON.valueToTree(v);
  }

  static JsonNode parse(String text) {
    try {
      JsonNode n = JSON.readTree(text);
      if (n == null) throw new IllegalArgumentException("Missing JSON value");
      validate(n);
      return n;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
    }
  }

  static void validate(JsonNode n) {
    if (n.isNumber() && !Double.isFinite(n.doubleValue()))
      throw new IllegalArgumentException("Nonfinite number");
    if (n.isTextual()) unicode(n.textValue());
    if (n.isObject()) {
      n.fieldNames().forEachRemaining(JsonSupport::unicode);
    }
    if (n.isContainerNode()) n.forEach(JsonSupport::validate);
  }

  static void unicode(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (Character.isHighSurrogate(ch)) {
        if (++i >= s.length() || !Character.isLowSurrogate(s.charAt(i)))
          throw new IllegalArgumentException("Invalid Unicode");
      } else if (Character.isLowSurrogate(ch))
        throw new IllegalArgumentException("Invalid Unicode");
    }
  }

  static byte[] canonical(Object value) {
    try {
      JsonNode n = tree(value);
      validate(n);
      if (n.isIntegralNumber()
          && n.bigIntegerValue().abs().compareTo(java.math.BigInteger.valueOf(9007199254740991L))
              > 0) throw new IllegalArgumentException("Unsafe integer; use a string");
      checkIntegers(n);
      byte[] wrapped = new JsonCanonicalizer("[" + n + "]").getEncodedUTF8();
      return java.util.Arrays.copyOfRange(wrapped, 1, wrapped.length - 1);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot canonicalize JSON", e);
    }
  }

  static void checkIntegers(JsonNode n) {
    if (n.isIntegralNumber()
        && n.bigIntegerValue().abs().compareTo(java.math.BigInteger.valueOf(9007199254740991L)) > 0)
      throw new IllegalArgumentException("Unsafe integer; use a string");
    if (n.isContainerNode()) n.forEach(JsonSupport::checkIntegers);
  }

  static String hash(Object value) {
    return sha(canonical(value));
  }

  static String sha(String value) {
    return sha(value.getBytes(StandardCharsets.UTF_8));
  }

  static String sha(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  // Existing config, archive and integrity hashes use sorted Python JSON, not JCS.
  static String encoded(Object value) {
    return sorted(tree(value), false);
  }

  static String spaced(Object value) {
    return sorted(tree(value), true);
  }

  private static String sorted(JsonNode n, boolean spaces) {
    String comma = spaces ? ", " : ",", colon = spaces ? ": " : ":";
    if (n.isObject()) {
      List<String> keys = new ArrayList<>();
      n.fieldNames().forEachRemaining(keys::add);
      keys.sort(
          (a, b) -> {
            int[] x = a.codePoints().toArray(), y = b.codePoints().toArray();
            for (int i = 0; i < Math.min(x.length, y.length); i++)
              if (x[i] != y[i]) return Integer.compare(x[i], y[i]);
            return Integer.compare(x.length, y.length);
          });
      List<String> items = new ArrayList<>();
      for (String k : keys) items.add(pythonString(k) + colon + sorted(n.get(k), spaces));
      return "{" + String.join(comma, items) + "}";
    }
    if (n.isArray()) {
      List<String> items = new ArrayList<>();
      n.forEach(v -> items.add(sorted(v, spaces)));
      return "[" + String.join(comma, items) + "]";
    }
    if (n.isTextual()) return pythonString(n.asText());
    if (n.isFloatingPointNumber()) return pythonFloat(n);
    return n.toString();
  }

  static String defaultJson(Object value) {
    String text = spaced(value);
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c > 127) out.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
      else out.append(c);
    }
    return out.toString();
  }

  static String pythonString(String value) {
    StringBuilder b = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> b.append("\\\"");
        case '\\' -> b.append("\\\\");
        case '\b' -> b.append("\\b");
        case '\f' -> b.append("\\f");
        case '\n' -> b.append("\\n");
        case '\r' -> b.append("\\r");
        case '\t' -> b.append("\\t");
        default -> {
          if (c < 32) b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
          else b.append(c);
        }
      }
    }
    return b.append('"').toString();
  }

  static String pythonFloat(JsonNode n) {
    double d = n.doubleValue();
    if (d == 0) return Double.doubleToRawLongBits(d) < 0 ? "-0.0" : "0.0";
    try {
      String wrapped = new JsonCanonicalizer("[" + n + "]").getEncodedString();
      String canonical = wrapped.substring(1, wrapped.length() - 1);
      java.math.BigDecimal value = new java.math.BigDecimal(canonical).stripTrailingZeros();
      int exponent = value.precision() - value.scale() - 1;
      if (exponent >= -4 && exponent < 16) {
        String plain = value.toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
      }
      String digits = value.unscaledValue().abs().toString();
      String mantissa =
          (value.signum() < 0 ? "-" : "")
              + digits.charAt(0)
              + (digits.length() > 1 ? "." + digits.substring(1) : "");
      return mantissa
          + "e"
          + (exponent < 0 ? "-" : "+")
          + String.format(java.util.Locale.ROOT, "%02d", Math.abs(exponent));
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid float", e);
    }
  }

  static String uuid() {
    return UUID.randomUUID().toString();
  }

  static String uuid(String id) {
    if (id == null
        || !id.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
      throw new ApiFailure(422, "invalid_uuid", "Expected a UUID");
    return UUID.fromString(id).toString();
  }

  static String actionKey(String ns, String sid, String eid) {
    return hash(arr("agent-action-v1", ns, uuid(sid), uuid(eid)));
  }

  static String payloadHash(String tool, String target, JsonNode args) {
    return hash(arr("agent-payload-v1", tool, target, args));
  }

  static boolean equal(JsonNode a, JsonNode b) {
    if (a == null || b == null) return a == b;
    if (a.isNumber() && b.isNumber()) return a.decimalValue().compareTo(b.decimalValue()) == 0;
    if (a.isObject() && b.isObject()) {
      if (a.size() != b.size()) return false;
      var it = a.fields();
      while (it.hasNext()) {
        var e = it.next();
        if (!equal(e.getValue(), b.get(e.getKey()))) return false;
      }
      return true;
    }
    if (a.isArray() && b.isArray()) {
      if (a.size() != b.size()) return false;
      for (int i = 0; i < a.size(); i++) if (!equal(a.get(i), b.get(i))) return false;
      return true;
    }
    return a.equals(b);
  }

  static boolean contains(JsonNode actual, JsonNode expected) {
    if (expected == null || expected.isMissingNode()) return true;
    if (expected.isObject()) {
      if (!actual.isObject()) return false;
      var it = expected.fields();
      while (it.hasNext()) {
        var e = it.next();
        if (!contains(actual.path(e.getKey()), e.getValue())) return false;
      }
      return true;
    }
    if (expected.isArray()) {
      if (!actual.isArray()) return false;
      for (JsonNode e : expected) {
        boolean found = false;
        for (JsonNode a : actual) if (equal(a, e)) found = true;
        if (!found) return false;
      }
      return true;
    }
    return equal(actual, expected);
  }

  static JsonNode path(JsonNode n, JsonNode path) {
    for (JsonNode key : path) {
      n =
          key.isIntegralNumber()
              ? n.path(key.asInt() < 0 && n.isArray() ? n.size() + key.asInt() : key.asInt())
              : n.path(key.asText());
      if (n.isMissingNode()) throw new IllegalArgumentException("Missing result path");
    }
    return n;
  }
}

class ApiFailure extends RuntimeException {
  final int status;
  final String code;

  ApiFailure(int status, String code, String message) {
    super(message);
    this.status = status;
    this.code = code;
  }

  ObjectNode body() {
    return JsonSupport.obj(
        "type", "error", "code", code, "message", getMessage(), "recoverable", status == 409);
  }
}
