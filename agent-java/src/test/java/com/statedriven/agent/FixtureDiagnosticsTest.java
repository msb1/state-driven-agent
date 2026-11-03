package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class FixtureDiagnosticsTest {
  @Test
  void employeeFailurePayloadsMatchPythonReferenceWithoutDatabaseAccess() {
    FixtureTools tools =
        new FixtureTools(
            null,
            JSON,
            new MockEnvironment()
                .withProperty(
                    "AGENT_FIXTURE_DATABASE_URL", "postgresql://test:test@invalid.local/test"));
    String[] modes = {"naive", "currency_cleaned", "null_cleaned"},
        hashes =
            {
              "0b45074e005993b5ae5bd3b6b8388012f4f6a435cef8c366151fdd2d8f8d574a",
              "f03f5f868f31a5647c1fd5355c16d3e2e0ddd045d92523c673737001d428afff",
              "ccee6b280db6c863e9292393055078deb9653d52e4d9e009ad5aa0b3803a6ed8"
            };
    for (int i = 0; i < modes.length; i++)
      assertEquals(
          hashes[i], sha(tools.execute("audit_employee_phase_1", obj("parser", modes[i])).block()));
    assertEquals(
        "412cf2eff340f45271d7008743b760a45b67fb69246aac9fa9e58a4f1c04f504",
        sha(
            tools
                .execute(
                    "calculate_new_york_median_salary",
                    obj("normalize_city", false, "exclude_invalid_salary", false))
                .block()));
    assertEquals(
        "a041eaec26bb0593984e5a54335ebc7fd276550c0dfb1e7884b95eaccc5b6f70",
        sha(
            tools
                .execute(
                    "calculate_new_york_median_salary",
                    obj("normalize_city", false, "exclude_invalid_salary", true))
                .block()));
  }
}
