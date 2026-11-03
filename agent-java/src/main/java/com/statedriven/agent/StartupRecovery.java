package com.statedriven.agent;

import static com.statedriven.agent.JsonSupport.*;

import com.fasterxml.jackson.databind.node.ObjectNode;

final class StartupRecovery {
  static void recover(SessionRepository repo) {
    for (ObjectNode run :
        repo.query(
            "SELECT r.session_id,r.run_id,r.command_id FROM agent_runs r JOIN agent_sessions s USING(session_id) WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp()) ORDER BY r.started_at LIMIT 100")) {
      ObjectNode origin = repo.commandStore().get(run.path("command_id").asText(), null);
      if (origin == null) continue;
      CommandStore.Command cmd = repo.commandStore().claim(origin, true);
      if (cmd == null) continue;
      try (cmd;
          var x = new ExecutionCoordinator(repo, run.path("session_id").asText(), cmd)) {
        if (!x.state.path("run_id").equals(run.path("run_id"))
            || !x.state.path("run_status").asText().equals("running")) continue;
        if (x.state.path("status").asText().equals("complete"))
          x.finish("complete", "terminal_snapshot_recovered");
        else {
          x.state.put("status", "failed");
          x.state.put(
              "final_answer", "Abandoned run detected; use /resume to reconcile pending work.");
          x.state.set(
              "last_error",
              obj(
                  "code",
                  "run_abandoned",
                  "message",
                  x.state.path("final_answer"),
                  "recoverable",
                  true));
          x.finish("interrupted", "worker_abandoned");
        }
      } catch (ApiFailure error) {
        if (!error.code.equals("ownership_conflict")) throw error;
      }
    }
  }
}
