package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class ConfigurationTest {
  @Test
  void normalizedConfigurationHashMatchesPythonReference() {
    assertEquals(
        "382fc66706afc29e6c8fe809711ec817b0c85787af75feec72d633ff785fa27c",
        ConfigRepository.resolvedHash(RuntimeRegressionTest.config()));
  }

  @Test
  void everyCurrentProfileResolvesWithoutModelSubstitution() throws Exception {
    var root = java.nio.file.Files.createTempDirectory("java-profiles-");
    java.nio.file.Files.createDirectory(root.resolve("config"));
    java.nio.file.Files.writeString(root.resolve(".env"), "MAIN_MODEL=test-exact-selected-model\n");
    try (var paths = java.nio.file.Files.list(java.nio.file.Path.of("../config"))) {
      for (var path : paths.toList())
        java.nio.file.Files.copy(path, root.resolve("config").resolve(path.getFileName()));
    }
    var env =
        new MockEnvironment()
            .withProperty("AGENT_CONFIG_ROOT", root.resolve("config").toString())
            .withProperty("MAIN_MODEL", "overridden-process-model")
            .withProperty("OPENAI_BASE_URL", "http://provider.invalid/v1");
    var repo = new ConfigRepository(JSON, env, root.resolve(".env"));
    for (String ref :
        java.util.List.of(
            "log-analysis.yaml",
            "revenue-analysis.yaml",
            "employee-audit.yaml",
            "compaction-stress.yaml",
            "tool-recovery.yaml",
            "completion-no-workflow.yaml",
            "idempotency-fixture.yaml",
            "socket-integration.yaml")) {
      var snapshot = repo.resolve(ref);
      assertEquals(
          ref.equals("socket-integration.yaml")
              ? "socket-integration"
              : "test-exact-selected-model",
          snapshot.config().path("model").asText(),
          ref);
      assertEquals(64, snapshot.sha256().length());
      assertEquals(snapshot.sha256(), repo.resolve(ref).sha256());
    }
  }

  @Test
  void rejectsEscapesMissingFilesAndAbsolutePaths() {
    var repo =
        new ConfigRepository(
            JSON, new MockEnvironment().withProperty("AGENT_CONFIG_ROOT", "../config"));
    for (String path : java.util.List.of("../contract.md", "/tmp/config.yaml", "missing.yaml", ""))
      assertThrows(IllegalArgumentException.class, () -> repo.resolve(path));
  }

  @Test
  void duplicateToolsAndUnregisteredEvidenceFailValidation() {
    var c = RuntimeRegressionTest.config();
    c.withArray("tools").add(c.path("tools").get(0).deepCopy());
    final var duplicate = c;
    assertThrows(IllegalArgumentException.class, () -> ConfigRepository.validate(duplicate));
    c = RuntimeRegressionTest.config();
    c.set("workflow", RuntimeRegressionTest.workflow());
    ConfigRepository.normalize(c);
    ((com.fasterxml.jackson.databind.node.ObjectNode) c.at("/workflow/phases/0/completion/0"))
        .put("tool", "unknown");
    final var invalid = c;
    assertThrows(IllegalArgumentException.class, () -> ConfigRepository.validate(invalid));
  }

  @Test
  void unsupportedSchemaAssertionsAndCoercionFail() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaChecks.schema(obj("type", "object", "oneOf", arr())));
    assertFalse(SchemaChecks.validate(tree(true), obj("type", "integer")).isEmpty());
    assertFalse(SchemaChecks.validate(tree("1"), obj("type", "integer")).isEmpty());
    assertTrue(SchemaChecks.validate(tree(1.0), obj("type", "integer")).isEmpty());
    assertFalse(
        SchemaChecks.validate(obj("x", 2), obj("type", "object", "additionalProperties", false))
            .isEmpty());
  }

  @Test
  void requestNormalizationIsStrictAndStable() {
    assertEquals(
        AgentController.request("{}", "run"),
        AgentController.request("{\"max_steps\":null}", "run"));
    for (String raw :
        java.util.List.of(
            "{\"max_steps\":true}", "{\"max_steps\":\"2\"}", "{\"max_steps\":0}", "{\"extra\":2}"))
      assertThrows(ApiFailure.class, () -> AgentController.request(raw, "run"));
    assertThrows(IllegalArgumentException.class, () -> AgentController.request("{} {}", "run"));
  }

  @Test
  void pythonCanonicalSerializationHandlesControlUnicodeAndScientificNumbers() {
    assertEquals(
        "{\"a\": [1e-07, 1e+16, 0.0001, 1.0, -0.0], \"text\": \"\\u001f雪\"}",
        spaced(obj("text", "\u001f雪", "a", arr(1e-7, 1e16, 0.0001, 1.0, -0.0))));
    assertEquals("{\"�\":1,\"😀\":2}", encoded(obj("😀", 2, "�", 1)));
  }
}
