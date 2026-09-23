package com.statedriven.agent;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "CreateSessionRequest")
record CreateSessionRequest(
    @Schema(description = "The user's goal; becomes the first session-memory message.", minLength = 1, requiredMode = Schema.RequiredMode.REQUIRED) String user_prompt,
    @Schema(description = "Relative YAML path inside the server-approved config repository.", defaultValue = "test-case-1.yaml", minLength = 1) String config_ref) {}

@Schema(name = "RunRequest")
record RunRequest(@Schema(minimum = "1", maximum = "50", nullable = true) Integer max_steps) {}

@Schema(name = "ResumeSessionRequest", description = "Continue a terminal session, optionally with a new user turn.")
record ResumeSessionRequest(
    @Schema(minimum = "1", maximum = "50", nullable = true) Integer max_steps,
    @Schema(minLength = 1, nullable = true) String content) {}

@Schema(name = "AppendUserMessageRequest")
record AppendUserMessageRequest(
    @Schema(description = "Additional user input appended to the active session memory.", minLength = 1, requiredMode = Schema.RequiredMode.REQUIRED) String content) {}
