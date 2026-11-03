package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;

class ModelDiagnosticsTest {
  @Test
  void templateProviderTransportAndTimeoutRemainDistinct() {
    var template =
        WebClientResponseException.create(
            400,
            "Bad Request",
            HttpHeaders.EMPTY,
            "{\"error\":{\"message\":\"Jinja rendering prompt failed; secret=do-not-expose\"}}"
                .getBytes(StandardCharsets.UTF_8),
            StandardCharsets.UTF_8);
    var failure = ModelClient.classify(template);
    assertEquals("model_template_error", failure.code);
    assertEquals("template", failure.diagnostic.path("category").asText());
    assertFalse(failure.diagnostic.toString().contains("do-not-expose"));
    var http =
        WebClientResponseException.create(
            503, "Unavailable", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
    assertEquals("model_http_error", ModelClient.classify(http).code);
    assertEquals(
        "model_timeout", ModelClient.classify(new java.util.concurrent.TimeoutException()).code);
    assertEquals("model_transport_error", ModelClient.classify(new IllegalStateException()).code);
  }

  @Test
  void proxyFailureCarriesOnlyBoundedLabels() {
    var error =
        ModelClient.proxyFailure(obj("proxy_transport_error", "ReadTimeout", "body", "secret"));
    assertEquals("model_timeout", error.code);
    assertFalse(error.diagnostic.toString().contains("secret"));
  }
}
