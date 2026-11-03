package com.statedriven.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AgentApplication {
  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("admin")) {
      if (args.length < 3)
        throw new IllegalArgumentException(
            "admin verify PATH | admin export SESSION PATH | admin prune SESSION PATH [KEEP_DIAGNOSTICS KEEP_EVENTS]");
      if (args[1].equals("verify")) {
        System.out.println(RetentionAdmin.verify(java.nio.file.Path.of(args[2])));
        return;
      }
      if (args.length < 4 || !java.util.Set.of("export", "prune").contains(args[1]))
        throw new IllegalArgumentException("Expected admin export|prune SESSION_UUID ARCHIVE_PATH");
      SpringApplication application = new SpringApplication(AgentApplication.class);
      application.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
      try (var context = application.run("--agent.administration=true")) {
        var repository = context.getBean(SessionRepository.class);
        String sid = JsonSupport.uuid(args[2]);
        var path = java.nio.file.Path.of(args[3]);
        System.out.println(
            switch (args[1]) {
              case "export" -> RetentionAdmin.export(repository, sid, path);
              case "prune" ->
                  RetentionAdmin.prune(
                      repository,
                      sid,
                      path,
                      args.length > 4 ? Integer.parseInt(args[4]) : 32,
                      args.length > 5 ? Integer.parseInt(args[5]) : 100);
              default -> throw new IllegalArgumentException("Unknown administration operation");
            });
      }
      return;
    }
    SpringApplication.run(AgentApplication.class, args);
  }

  @Bean
  CommandLineRunner schema(
      SessionRepository sessions, org.springframework.core.env.Environment env) {
    return args -> {
      sessions.initialize().block();
      if (!env.getProperty("agent.administration", Boolean.class, false))
        StartupRecovery.recover(sessions);
      sessions.markReady();
    };
  }

  @Bean
  ObjectMapper objectMapper() {
    return JsonSupport.JSON.copy();
  }

  @Bean
  org.springframework.web.server.WebFilter readiness(SessionRepository repository) {
    return (exchange, chain) -> {
      if (repository.isReady()) return chain.filter(exchange);
      exchange.getResponse().setStatusCode(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE);
      return exchange.getResponse().setComplete();
    };
  }

  @Bean
  org.springdoc.core.customizers.OpenApiCustomizer commandsDocumentation() {
    return api -> {
      if (api.getPaths() != null)
        api.getPaths()
            .values()
            .forEach(
                path -> {
                  var post = path.getPost();
                  if (post != null) {
                    post.addParametersItem(
                        new io.swagger.v3.oas.models.parameters.Parameter()
                            .in("header")
                            .name("Idempotency-Key")
                            .required(true)
                            .description(
                                "Retain the same key for retries of the same logical request.")
                            .schema(
                                new io.swagger.v3.oas.models.media.StringSchema()
                                    .minLength(1)
                                    .maxLength(255)
                                    .pattern("^[!-~]+$")));
                    post.getResponses()
                        .addApiResponse(
                            "202",
                            new io.swagger.v3.oas.models.responses.ApiResponse()
                                .description(
                                    "Existing command is in progress or requires recovery"))
                        .addApiResponse(
                            "409",
                            new io.swagger.v3.oas.models.responses.ApiResponse()
                                .description(
                                    "Key reused with changed content, or ownership conflict"));
                  }
                });
    };
  }

  @Bean
  OpenAPI openApi() {
    return new OpenAPI().info(new Info().title("State-Driven AI Agent").version("2.0.0"));
  }
}
