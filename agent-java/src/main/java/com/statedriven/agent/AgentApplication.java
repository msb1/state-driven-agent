package com.statedriven.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AgentApplication {
  public static void main(String[] args) { SpringApplication.run(AgentApplication.class, args); }
  @Bean CommandLineRunner schema(SessionRepository sessions) { return args -> sessions.initialize().block(); }
  @Bean ObjectMapper objectMapper() { return new ObjectMapper(); }
}
