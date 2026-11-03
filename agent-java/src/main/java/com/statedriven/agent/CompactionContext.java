package com.statedriven.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

/** Pure staging helpers: these never mutate the live session. */
final class CompactionContext {
  private static final List<String> SECTIONS =
      List.of("USER_GOAL", "GLOBAL_LESSON_LEDGER", "DEAD_ENDS", "CURRENT_LOCAL_PIVOT");
  private static final Pattern NUMBER =
      Pattern.compile(
          "(?<![\\w.,+-])[-+]?\\d+(?:,\\d{3})*(?:\\.\\d+)?(?:[eE][-+]?\\d+)?(?!\\w|[.,]\\d)",
          Pattern.UNICODE_CHARACTER_CLASS);

  record Split(ArrayNode historical, ArrayNode retained) {}

  static Split split(ArrayNode memory, int minimum) {
    if (memory.isEmpty() || !"user".equals(memory.get(0).path("role").asText()))
      throw new IllegalArgumentException("Missing immutable goal message");
    List<Integer> boundaries = new ArrayList<>();
    for (int i = 1; i < memory.size(); ) {
      boundaries.add(i);
      JsonNode message = memory.get(i++);
      if ("tool".equals(message.path("role").asText()))
        throw new IllegalArgumentException("Orphaned tool result");
      if (!message.has("tool_calls")) continue;
      Set<String> pending = new HashSet<>();
      for (JsonNode call : message.path("tool_calls")) {
        String id = call.path("id").asText();
        if (id.isBlank() || !pending.add(id))
          throw new IllegalArgumentException("Invalid or duplicate call ID");
      }
      while (!pending.isEmpty()) {
        if (i >= memory.size()) throw new IllegalArgumentException("Incomplete tool group");
        JsonNode result = memory.get(i++);
        if (!"tool".equals(result.path("role").asText())
            || !pending.remove(result.path("tool_call_id").asText()))
          throw new IllegalArgumentException("Uncorrelated tool result");
      }
    }
    int cut = memory.size();
    for (int i = boundaries.size() - 1; i >= 0 && memory.size() - cut < minimum; i--)
      cut = boundaries.get(i);
    ArrayNode historical = memory.arrayNode(), retained = memory.arrayNode();
    for (int i = 1; i < memory.size(); i++)
      (i < cut ? historical : retained).add(memory.get(i).deepCopy());
    return new Split(historical, retained);
  }

  static String normalize(
      String proposal, String goal, String source, int byteLimit, boolean hybrid) {
    if (proposal == null || proposal.isBlank()) throw new IllegalArgumentException("Empty summary");
    bounded(proposal, byteLimit);
    String text = proposal.strip();
    var fence =
        Pattern.compile("(?is)^```(?:xml|text)?[ \\t]*\\r?\\n(.*)\\r?\\n```$").matcher(text);
    if (fence.matches()) text = fence.group(1).strip();
    List<String> values = new ArrayList<>();
    if (hybrid) {
      if (text.contains("<!") || text.contains("<?"))
        throw new IllegalArgumentException("Forbidden XML declaration");
      try {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        var builder = factory.newDocumentBuilder();
        builder.setErrorHandler(
            new org.xml.sax.helpers.DefaultHandler() {
              @Override
              public void fatalError(org.xml.sax.SAXParseException e)
                  throws org.xml.sax.SAXException {
                throw e;
              }

              @Override
              public void error(org.xml.sax.SAXParseException e) throws org.xml.sax.SAXException {
                throw e;
              }
            });
        Element root = builder.parse(new InputSource(new StringReader(text))).getDocumentElement();
        if (!root.getTagName().equals("COMPACTED_STATE") || root.hasAttributes())
          throw new IllegalArgumentException("Invalid summary root");
        Map<String, String> sections = new HashMap<>();
        for (Node n = root.getFirstChild(); n != null; n = n.getNextSibling()) {
          if (n instanceof Element e) {
            if (!SECTIONS.contains(e.getTagName()) || sections.containsKey(e.getTagName()))
              throw new IllegalArgumentException("Unknown or duplicate section");
            sections.put(e.getTagName(), flatten(e));
          } else if (!n.getTextContent().isBlank())
            throw new IllegalArgumentException("Extra summary text");
        }
        if (sections.size() != 4) throw new IllegalArgumentException("Missing summary section");
        sections.put("USER_GOAL", goal.strip());
        for (String section : SECTIONS) values.add(sections.get(section));
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalArgumentException("Malformed summary XML", e);
      }
    } else {
      List<String> headings =
          List.of(
              "Core Objective", "Universal Truths Discovered", "Dead Ends", "Current Local Pivot");
      var matches =
          Pattern.compile(
                  "(?m)^- (Core Objective|Universal Truths Discovered|Dead Ends|Current Local Pivot):\\s*")
              .matcher(text);
      List<Integer> starts = new ArrayList<>(), ends = new ArrayList<>();
      List<String> found = new ArrayList<>();
      while (matches.find()) {
        starts.add(matches.start());
        ends.add(matches.end());
        found.add(matches.group(1));
      }
      if (!found.equals(headings) || !text.substring(0, starts.get(0)).isBlank())
        throw new IllegalArgumentException("Missing or unordered summary heading");
      for (int i = 0; i < 4; i++)
        values.add(
            i == 0
                ? goal.strip()
                : text.substring(ends.get(i), i + 1 < 4 ? starts.get(i + 1) : text.length())
                    .strip());
    }

    for (String value : values)
      if (value.isBlank() || value.equals("...") || value.equals("…"))
        throw new IllegalArgumentException("Empty summary section");
    if (!numbers(source).containsAll(numbers(String.join("\n", values))))
      throw new IllegalArgumentException("Unsupported numeric claim");
    StringBuilder canonical = new StringBuilder();
    if (hybrid) {
      canonical.append("<COMPACTED_STATE>");
      for (int i = 0; i < 4; i++)
        canonical
            .append('<')
            .append(SECTIONS.get(i))
            .append(">\n")
            .append(escape(i == 0 ? goal : values.get(i)))
            .append("\n</")
            .append(SECTIONS.get(i))
            .append(">");
      canonical.append("</COMPACTED_STATE>");
    } else {
      var headings =
          List.of(
              "Core Objective", "Universal Truths Discovered", "Dead Ends", "Current Local Pivot");
      for (int i = 0; i < 4; i++) {
        if (i > 0) canonical.append('\n');
        canonical
            .append("- ")
            .append(headings.get(i))
            .append(":\n")
            .append(i == 0 ? goal : values.get(i));
      }
    }
    bounded(canonical.toString(), byteLimit);
    return canonical.toString();
  }

  private static String flatten(Element element) {
    if (element.hasAttributes()) throw new IllegalArgumentException("Summary attributes forbidden");
    StringBuilder text = new StringBuilder();
    for (Node n = element.getFirstChild(); n != null; n = n.getNextSibling()) {
      if (n instanceof Element e) text.append('\n').append(flatten(e)).append('\n');
      else text.append(n.getTextContent());
    }
    return String.join(
        "\n", text.toString().lines().map(String::strip).filter(s -> !s.isEmpty()).toList());
  }

  static String escape(String value) {
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  private static void bounded(String value, int limit) {
    if (value.getBytes(StandardCharsets.UTF_8).length > limit)
      throw new IllegalArgumentException("Summary byte limit exceeded");
  }

  private static Set<BigDecimal> numbers(String value) {
    Set<BigDecimal> numbers = new HashSet<>();
    var matcher = NUMBER.matcher(value);
    while (matcher.find())
      numbers.add(new BigDecimal(matcher.group().replace(",", "")).stripTrailingZeros());
    return numbers;
  }
}
