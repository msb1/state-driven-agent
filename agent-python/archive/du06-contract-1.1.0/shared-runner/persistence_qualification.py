"""Run shared API scenarios and verify their retained PostgreSQL ledger evidence."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import hashlib
from uuid import uuid4

from dotenv import dotenv_values

from idempotency_contract import CONTRACT, SCHEMA_VERSION
from persistence_ledger import capture, verify
from persistence_scenarios import Scenarios
from persistence_support import APIReplay, ROOT, require, setup_checks

UNEXERCISED = {
    'DU-06-C01-fresh-schema-initialization': 'Administrative schema setup is intentionally outside persist.sh.',
    'DU-06-C02-stage-rollback': 'Requires interrupting a repository transaction at each write stage; the common API exposes no stage boundary.',
    'DU-06-C03-stale-repository-proposal': 'Requires holding and submitting two native repository proposals at the same revision; the common API serializes or reloads requests.',
    'DU-06-C04-lost-database-commit-ack': 'Requires suppressing a confirmed PostgreSQL COMMIT response and retrying the same native proposal.',
    'DU-06-C05-concurrent-startup-recovery': 'Requires controlling worker process startup and lease expiry for the selected agent.',
    'V2-C05-recipient-recovery': 'Requires a target recipient that supports durable keys and an externally controlled lost response.',
    'V2-C06-uncertain-outcome-recovery': 'Requires controlling recipient outcomes and authoritative verification across recovery.',
    'V2-C06-recipient-window-expiry': 'Requires exercising a recipient with a declared finite deduplication window.',
    'V2-C09-actual-telemetry-pruning': 'Ledger reads and a retention plan do not execute administrative pruning.',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session-id', action='append', default=[], help='Additionally verify a retained session from test-cases.sh or aux-tests.sh')
    parser.add_argument('--timeout', type=float, default=600, help='HTTP watchdog seconds (default 600)')
    parser.add_argument('--restart', action='store_true', default=True, help='Pause for target server restart (default)')
    parser.add_argument('--skip-restart', action='store_false', dest='restart', help='Skip restart; result remains incomplete')
    args = parser.parse_args()
    require(args.timeout > 0, '--timeout must be positive')
    env = {**dotenv_values(ROOT / '.env'), **os.environ}
    directory = ROOT / 'archive' / 'persistence' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid4().hex[:8])
    directory.mkdir(parents=True, mode=0o700)
    # Evidence may include user prompts and tool outputs. Restrict newly created files.
    os.umask(0o077)
    result = {'contract_version': CONTRACT, 'storage_schema': SCHEMA_VERSION,
              'status': 'running', 'scope': 'implementation-agnostic agent API scenarios and PostgreSQL ledger verification',
              'agent_conformance': 'not_assessed', 'cases': {},
              'qualification': 'shared persistence scenarios through the target API; no language-specific repository tests',
              'outside_scope': ['fresh schema creation (administrative setup)', 'whole contract 2.0.0 conformance'],
              'database_access': 'read-only SQL; normal agent API writes; all ledger evidence retained'}
    source_files = [*sorted((ROOT / 'scripts').glob('*.py')),
                    ROOT / 'scripts/pyproject.toml', ROOT / 'scripts/uv.lock', ROOT / 'persist.sh',
                    ROOT / 'schema/postgres.sql', ROOT / 'schema/idempotency-vectors.json',
                    ROOT / 'schema/layout-v6.json']
    result['source_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in source_files}
    required = ['schema-and-fixture-read-validation', 'V2-C02-C03-command-replay-and-atomic-ledger',
                'V2-C01-strict-request-rejection', 'DU-05-DU-07-bounded-run-resume-ledger',
                'DU-08-history-pages-versus-postgres', 'V2-C08-SSE-reconnect-without-effects',
                'DU-09-retention-protection-plan', 'DU-07-DU-08-socket-cancellation-and-resume-ledger',
                'DU-06-C06-restart', *UNEXERCISED]
    result['cases'] = {name: {'status': 'not_exercised', 'reason': 'Scenario has not completed'}
                       for name in required}
    for name, reason in UNEXERCISED.items():
        result['cases'][name]['reason'] = reason
    report_path = directory / 'result.json'
    probe = None

    def save():
        report_path.write_text(json.dumps(result, indent=2) + '\n')

    def case(name, callback):
        print(f'RUN {name}', flush=True)
        started = time.monotonic()
        try:
            details = callback()
            outcome = {'status': 'passed', 'evidence': details}
        except Exception as error:
            outcome = {'status': 'failed', 'error': f'{type(error).__name__}: {error}'}
        outcome['elapsed_seconds'] = time.monotonic() - started
        result['cases'][name] = outcome
        print(f"{outcome['status'].upper()} {name}" + (': ' + outcome['error'] if 'error' in outcome else ''), flush=True)
        save()
        return outcome['status'] == 'passed'

    print(f'Evidence: {directory}', flush=True)
    if not args.restart:
        result['cases']['DU-06-C06-restart'] = {'status': 'not_exercised', 'reason': 'Run with --restart for an operator-controlled server restart.'}
    save()
    try:
        database_url = env.get('AGENT_SESSION_DATABASE_URL')
        require(database_url, 'Set AGENT_SESSION_DATABASE_URL in repository .env')
        base_url = env.get('AGENT_URL') or f"http://127.0.0.1:{env.get('PORT', '8081')}"
        result['target'] = base_url
        require(case('schema-and-fixture-read-validation', lambda: asyncio.run(setup_checks(
            database_url, env.get('AGENT_FIXTURE_DATABASE_URL') or database_url))),
            'Schema/fixture prerequisite failed; API scenarios were not started')
        probe = APIReplay(base_url, database_url)
        import httpx
        probe.http.timeout = httpx.Timeout(args.timeout)
        health = probe.http.get('/health')
        require(health.status_code == 200 and health.json().get('contract_version') == CONTRACT
                and health.json().get('storage_schema') == SCHEMA_VERSION,
                'Target must advertise contract 2.0.0 and schema 6 before any scenario writes')
        result['runtime'] = health.json()
        scenarios = Scenarios(probe, directory)
        case('V2-C02-C03-command-replay-and-atomic-ledger', scenarios.api_replay)
        case('V2-C01-strict-request-rejection', scenarios.strict_requests)
        if case('DU-05-DU-07-bounded-run-resume-ledger', scenarios.workflow_resume):
            case('DU-08-history-pages-versus-postgres', scenarios.history_pages)
            case('V2-C08-SSE-reconnect-without-effects', scenarios.event_reconnect)
            case('DU-09-retention-protection-plan', scenarios.retention_boundary)
        else:
            for name in ('DU-08-history-pages-versus-postgres', 'V2-C08-SSE-reconnect-without-effects', 'DU-09-retention-protection-plan'):
                result['cases'][name] = {'status': 'not_exercised', 'reason': 'Workflow prerequisite failed.'}
        def socket_lifecycle():
            from disconnect_integration_test import Harness, run_suite
            target = directory / 'socket-lifecycle'
            harness = Harness(base_url, env.get('AUX_SOCKET_MODEL_URL') or 'http://127.0.0.1:18089/v1',
                              'socket-integration.yaml', target, min(args.timeout, 120))
            try:
                run_suite(harness)
            finally:
                try:
                    harness.diagnostics()
                finally:
                    harness.close()
                    for created in sorted(target.glob('*-created.json')):
                        sid = json.loads(created.read_text())['session']['id']
                        if sid not in probe.sessions:
                            probe.sessions.append(sid)
            evidence = []
            for created in sorted(target.glob('*-created.json')):
                sid = json.loads(created.read_text())['session']['id']
                evidence.append(scenarios.snapshot(sid, 'socket-lifecycle-ledger')[1])
            require(len(evidence) == 2, 'Socket suite did not retain both connected and cancelled/resumed sessions')
            return {'tests': 3, 'directory': str(target), 'ledger': evidence}
        case('DU-07-DU-08-socket-cancellation-and-resume-ledger', socket_lifecycle)
        for index, sid in enumerate(args.session_id):
            def retained(sid=sid, index=index):
                rows, evidence = capture(database_url, sid, directory / f'retained-{index}.json')
                return {**evidence, 'verification': verify(rows)}
            case(f'retained-session-{index}', retained)
        if args.restart:
            def restart():
                require(hasattr(probe, 'creation'), 'Command scenario prerequisite failed')
                sid, body, key, response = probe.creation
                before, evidence = scenarios.snapshot(sid, 'before-restart')
                input(f'Restart the target server at {base_url} in its terminal, then press Enter here: ')
                health = probe.http.get('/health')
                require(health.status_code == 200 and health.json().get('contract_version') == CONTRACT,
                        'Restarted target is unavailable or incompatible')
                hydrated = probe.http.get(f'/sessions/{sid}')
                require(hydrated.status_code == 200 and hydrated.json()['revision'] == before['agent_sessions'][0]['revision'],
                        'Restart changed the hydrated revision')
                probe.replay('/sessions', body, key, response, sid)
                after, final = scenarios.snapshot(sid, 'after-restart')
                require(before == after, 'Restart/replay changed durable history')
                return {'before': evidence, 'after': final, 'restart_control': 'operator confirmation; no automated process identity assertion'}
            case('DU-06-C06-restart', restart)
        result['ledger_evidence'] = scenarios.evidence
    except Exception as error:
        result['cases']['configuration-or-harness'] = {'status': 'failed', 'error': f'{type(error).__name__}: {error}'}
    finally:
        if probe:
            for index, sid in enumerate(probe.sessions):
                try:
                    _, evidence = capture(probe.database_url, sid, directory / f'final-session-{index}.json')
                    result.setdefault('final_ledger_evidence', []).append(evidence)
                except Exception as error:
                    result['cases'][f'final-evidence-{index}'] = {'status': 'failed', 'error': f'{type(error).__name__}: {error}'}
            probe.close()
        statuses = [v['status'] for v in result['cases'].values()]
        result['status'] = 'failed' if 'failed' in statuses else 'incomplete' if 'not_exercised' in statuses else 'passed'
        result['persistence_qualification'] = result['status']
        result['agent_conformance'] = 'not_assessed'  # Whole-contract coverage belongs to all shared suites.
        save()
        for name, outcome in result['cases'].items():
            if outcome['status'] == 'not_exercised':
                print(f"NOT EXERCISED {name}: {outcome['reason']}", flush=True)
        print(f"{result['status'].upper()}: {report_path}", flush=True)
    return 0 if result['status'] == 'passed' else 1 if result['status'] == 'failed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
