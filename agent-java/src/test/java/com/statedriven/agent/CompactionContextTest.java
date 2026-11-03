package com.statedriven.agent;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class CompactionContextTest {
  private final ObjectMapper json = new ObjectMapper();

  private String proposal(String lessons) {
    return "<COMPACTED_STATE><CURRENT_LOCAL_PIVOT>Continue</CURRENT_LOCAL_PIVOT><USER_GOAL>model paraphrase</USER_GOAL><DEAD_ENDS>None</DEAD_ENDS><GLOBAL_LESSON_LEDGER>"
        + lessons
        + "</GLOBAL_LESSON_LEDGER></COMPACTED_STATE>";
  }

  @Test
  void normalizesFencesNestedMarkupAndRestoresGoal() {
    String normalized =
        CompactionContext.normalize(
            "```xml\n"
                + proposal("<lesson>Keep 83</lesson><lesson>Use evidence</lesson>")
                + "\n```",
            "goal & <scope>",
            "goal & <scope> 83",
            4096,
            true);
    assertTrue(normalized.contains("goal &amp; &lt;scope&gt;"));
    assertTrue(normalized.contains("Keep 83\nUse evidence"));
    assertTrue(normalized.indexOf("USER_GOAL") < normalized.indexOf("GLOBAL_LESSON_LEDGER"));
    assertEquals(
        normalized,
        CompactionContext.normalize(normalized, "goal & <scope>", "goal & <scope> 83", 4096, true));
  }

  @Test
  void rejectsUnsupportedNumbersAndPlaceholders() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CompactionContext.normalize(proposal("Count 84"), "goal", "Count 83", 4096, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> CompactionContext.normalize(proposal("…"), "goal", "source", 4096, true));
  }

  @Test
  void rejectsMalformedOrUnsafeStructures() {
    for (String invalid :
        new String[] {
          "prefix " + proposal("Keep"),
          proposal("<lesson a='x'>Keep</lesson>"),
          "<!DOCTYPE x>" + proposal("Keep"),
          proposal("Keep").replace("</COMPACTED_STATE>", ""),
          proposal("Keep").replace("<DEAD_ENDS>None</DEAD_ENDS>", ""),
          proposal("Keep")
              .replace("<DEAD_ENDS>None</DEAD_ENDS>", "<USER_GOAL>duplicate</USER_GOAL>")
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> CompactionContext.normalize(invalid, "goal", "source", 4096, true));
    }
  }

  @Test
  void boundsOriginalAndCanonicalUtf8Bytes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CompactionContext.normalize(proposal("雪".repeat(100)), "goal", "source", 300, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CompactionContext.normalize(
                proposal("Keep"), "long goal ".repeat(100), "source", 400, true));
  }

  @Test
  void retainsWholeParallelCallGroup() throws Exception {
    var memory =
        (com.fasterxml.jackson.databind.node.ArrayNode)
            json.readTree(
                """
            [{"role":"user","content":"goal"},
             {"role":"user","content":"older"},
             {"role":"assistant","tool_calls":[{"id":"a"},{"id":"b"}]},
             {"role":"tool","tool_call_id":"b"},{"role":"tool","tool_call_id":"a"}]
            """);
    var split = CompactionContext.split(memory, 1);
    assertEquals(1, split.historical().size());
    assertEquals(3, split.retained().size());
    assertEquals("assistant", split.retained().get(0).path("role").asText());
    assertEquals(5, memory.size());
  }

  @Test
  void rejectsIncompleteAndOrphanedGroups() throws Exception {
    for (String raw :
        new String[] {
          "[{\"role\":\"user\"},{\"role\":\"tool\",\"tool_call_id\":\"a\"}]",
          "[{\"role\":\"user\"},{\"role\":\"assistant\",\"tool_calls\":[{\"id\":\"a\"}]}]",
          "[{\"role\":\"user\"},{\"role\":\"assistant\",\"tool_calls\":[{\"id\":\"a\"},{\"id\":\"a\"}]}]"
        }) {
      var memory = (com.fasterxml.jackson.databind.node.ArrayNode) json.readTree(raw);
      assertThrows(IllegalArgumentException.class, () -> CompactionContext.split(memory, 1));
    }
  }

  @Test
  void plainModeRestoresGoal() {
    String proposal =
        "- Core Objective: paraphrase\n- Universal Truths Discovered: Keep\n- Dead Ends: None\n- Current Local Pivot: Continue";
    String normalized = CompactionContext.normalize(proposal, "immutable", "source", 4096, false);
    assertTrue(normalized.startsWith("- Core Objective:\nimmutable"));
  }
}
