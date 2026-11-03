package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.node.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class CompactionIntegrationTest {
  static String summary(String lesson) {
    return "```xml\n<COMPACTED_STATE><USER_GOAL>paraphrase</USER_GOAL><GLOBAL_LESSON_LEDGER><lesson>"
        + lesson
        + "</lesson></GLOBAL_LESSON_LEDGER><DEAD_ENDS>None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>Continue from executed evidence.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>\n```";
  }

  static void history(RuntimeRegressionTest.Harness h) {
    for (int i = 0; i < 6; i++)
      DurableState.message(
          h.s,
          obj(
              "role",
              i % 2 == 0 ? "user" : "assistant",
              "content",
              "historical context ".repeat(150)));
    DurableState.message(h.s, obj("role", "user", "content", "recent request"));
    DurableState.message(h.s, obj("role", "assistant", "content", "recent answer"));
  }

  static RuntimeRegressionTest.Harness harness() {
    ObjectNode c = RuntimeRegressionTest.config();
    c.with("memory").put("max_tokens", 1000);
    var h = new RuntimeRegressionTest.Harness(c);
    when(h.llm.summaryRequest(
            any(),
            anyString(),
            any(),
            nullable(String.class),
            anyBoolean(),
            nullable(String.class)))
        .thenCallRealMethod();
    history(h);
    return h;
  }

  @Test
  void stagedAcceptancePreservesRecentMessagesAndArchivesProvenance() {
    try (var h = harness()) {
      h.x.start();
      String last = h.s.path("memory").get(h.s.path("memory").size() - 1).toString();
      when(h.llm.compact(
              any(),
              anyString(),
              any(),
              nullable(String.class),
              anyBoolean(),
              nullable(String.class)))
          .thenReturn(Mono.just(summary("Preserve constraints.")));
      h.engine.compact(h.x);
      assertEquals(3, h.s.path("memory").size());
      assertEquals(last, h.s.path("memory").get(2).toString());
      assertEquals("unverified_model_summary", h.s.at("/ledger_state/verification").asText());
      assertEquals("Inspect the log", h.s.at("/ledger_state/goal").asText());
      assertTrue(h.s.path("compaction_savings_total").asLong() > 0);
      var record =
          h.transcript.stream()
              .filter(r -> r.path("kind").asText().equals("compaction_summary"))
              .findFirst()
              .orElseThrow();
      assertEquals(6, record.at("/payload/source_message_ids").size());
      assertEquals(h.s.path("active_ledger_id"), record.path("message_id"));
      assertTrue(record.at("/payload/proposal").asText().startsWith("```xml"));
    }
  }

  @Test
  void rejectedProposalsPreserveHistoryAndUnchangedSourceDoesNotRetry() {
    try (var h = harness()) {
      h.x.start();
      String original = h.s.path("memory").toString();
      when(h.llm.compact(
              any(),
              anyString(),
              any(),
              nullable(String.class),
              anyBoolean(),
              nullable(String.class)))
          .thenReturn(Mono.just(summary("Invented count 999.")));
      h.engine.compact(h.x);
      assertEquals(original, h.s.path("memory").toString());
      assertTrue(h.s.path("compacted_context").isNull());
      assertEquals(2, h.s.path("compaction_attempts").size());
      assertEquals(2, h.s.path("transcript_pending").size());
      h.engine.compact(h.x);
      verify(h.llm, times(2))
          .compact(
              any(),
              anyString(),
              any(),
              nullable(String.class),
              anyBoolean(),
              nullable(String.class));
      assertEquals(1, h.s.at("/compaction_skip_counts/unchanged_failed_source").asInt());
    }
  }

  @Test
  void providerTruncationIsNeverInstalled() {
    try (var h = harness()) {
      h.x.start();
      String original = h.s.path("memory").toString();
      when(h.llm.compact(
              any(),
              anyString(),
              any(),
              nullable(String.class),
              anyBoolean(),
              nullable(String.class)))
          .thenReturn(Mono.error(new ModelClient.ModelFailure("summary_truncated", "truncated")));
      h.engine.compact(h.x);
      assertEquals(original, h.s.path("memory").toString());
      assertTrue(h.s.path("compacted_context").isNull());
      assertEquals(2, h.s.path("diagnostic_rejections").asInt());
    }
  }

  @Test
  void optionalSpacingCannotBypassHardDecisionProtection() {
    try (var h = harness()) {
      h.c
          .with("memory")
          .put("context_window_tokens", 8000)
          .put("compaction_min_interval_steps", 100);
      h.s.put("last_compaction_attempt_step", 0);
      h.x.start();
      when(h.llm.compact(
              any(),
              anyString(),
              any(),
              nullable(String.class),
              anyBoolean(),
              nullable(String.class)))
          .thenReturn(Mono.just(summary("Preserve constraints.")));
      h.engine.compact(h.x);
      assertNotEquals(
          "minimum_spacing", h.s.path("compaction_decisions").path(0).path("reason").asText());
    }
  }
}
