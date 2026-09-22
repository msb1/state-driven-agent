package com.statedriven.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;

@SpringBootApplication
public class AgentApplication {
  public static void main(String[] args) { SpringApplication.run(AgentApplication.class, args); }
  @Bean CommandLineRunner schema(SessionRepository sessions) { return args -> sessions.initialize().block(); }
  @Bean ObjectMapper objectMapper() { return new ObjectMapper(); }
  @Bean OpenAPI openApi() { return new OpenAPI().info(new Info().title("State-Driven AI Agent").version("0.2.0")); }
}
