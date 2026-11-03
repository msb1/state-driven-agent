package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.*;

final class CompletionChecks {
  static ArrayNode table(String text) {
    ArrayNode rows = arr();
    for (String raw : text.split("\\R")) {
      String line = raw.strip();
      if (line.startsWith("|") && line.endsWith("|")) {
        String[] cells = line.substring(1, line.length() - 1).split("\\|", -1);
        boolean separator = true;
        ArrayNode row = arr();
        for (String cell : cells) {
          cell = cell.strip();
          row.add(cell);
          if (!cell.matches(":?-{3,}:?")) separator = false;
        }
        if (!separator) rows.add(row);
      } else if (!rows.isEmpty()) break;
    }
    if (rows.size() < 2) throw new IllegalArgumentException("A complete table is required");
    Set<String> keys = new HashSet<>();
    for (int i = 1; i < rows.size(); i++)
      if (rows.get(i).size() != rows.get(0).size() || !keys.add(rows.get(i).get(0).asText()))
        throw new IllegalArgumentException("Duplicate or incomplete table rows");
    return rows;
  }

  static JsonNode extract(JsonNode check, String output) {
    if (check.path("kind").asText().equals("markdown_table")) return table(output);
    JsonNode v = path(parse(output), check.path("path"));
    if (check.path("kind").asText().equals("number") && !v.isNumber())
      throw new IllegalArgumentException("Result is not numeric");
    return v;
  }

  static boolean matches(JsonNode check, String answer, JsonNode expected) {
    String kind = check.path("kind").asText();
    try {
      if (kind.equals("json_value"))
        return equal(path(parse(answer), check.path("answer_path")), expected);
      if (kind.equals("number")) {
        var matcher =
            Pattern.compile(
                    Pattern.quote(check.path("label").asText())
                        + "\\s*\\$?\\s*([-+]?(?:\\d{1,3}(?:,\\d{3})+|\\d+)(?:\\.\\d+)?)(?![\\d,]|\\.\\d)")
                .matcher(answer);
        if (!matcher.find()) return false;
        BigDecimal n = new BigDecimal(matcher.group(1).replace(",", ""));
        return !matcher.find() && n.compareTo(expected.decimalValue()) == 0;
      }
      ArrayNode actual = table(answer);
      if (!actual.get(0).equals(expected.get(0)) || actual.size() != expected.size()) return false;
      Map<String, JsonNode> rows = new HashMap<>();
      for (int i = 1; i < actual.size(); i++)
        rows.put(actual.get(i).get(0).asText(), actual.get(i));
      for (int i = 1; i < expected.size(); i++) {
        JsonNode wanted = expected.get(i), got = rows.get(wanted.get(0).asText());
        if (got == null || got.size() != wanted.size()) return false;
        for (int j = 1; j < wanted.size(); j++) {
          String a = got.get(j).asText(), b = wanted.get(j).asText();
          if (b.matches("[-+]?\\d+") && !a.matches("[-+]?\\d+")) return false;
          try {
            if (new BigDecimal(a).compareTo(new BigDecimal(b)) != 0) return false;
          } catch (NumberFormatException error) {
            if (!a.equals(b)) return false;
          }
        }
      }
      return true;
    } catch (Exception error) {
      return false;
    }
  }

  static String describe(JsonNode check, JsonNode value) {
    if (check.path("kind").asText().equals("markdown_table")) {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < value.size(); i++) {
        List<String> cells = new ArrayList<>();
        value.get(i).forEach(v -> cells.add(v.asText()));
        b.append("| ").append(String.join(" | ", cells)).append(" |\n");
        if (i == 0)
          b.append("| ")
              .append(String.join(" | ", Collections.nCopies(cells.size(), "---")))
              .append(" |\n");
      }
      return b.toString();
    }
    if (check.path("kind").asText().equals("number"))
      return check.path("label").asText() + " " + value;
    return "Expected JSON value at " + check.path("answer_path") + ": " + value;
  }
}
