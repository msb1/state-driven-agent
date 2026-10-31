"""Execute the twelve contract-2.0.0 DU-06 cases against any conforming agent."""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

from dotenv import dotenv_values

from du06_cases import Cases
from du06_client import Client
from du06_controls import NotExercised
from idempotency_contract import CONTRACT, SCHEMA_VERSION
from persistence_support import ROOT, require, setup_checks

CASE_NAMES = (
    'Shared schema and identity', 'Completed-command replay', 'Conflicting key reuse',
    'Concurrent duplicates', 'Replay after subsequent work', 'Outcome recovery after missing response',
    'Interruption and atomicity', 'Restart durability', 'Concurrent ownership and revision consistency',
    'State and outcome consistency', 'Ledger relationships and immutability', 'Protected recovery evidence',
)


def case_measurements(client, case_id):
    requests = [r for r in client.requests if r['case'] == case_id]
    snapshots = [r for r in client.snapshots if r.get('case') == case_id]
    grouped = {}
    for snapshot in snapshots:
        grouped.setdefault(snapshot['session_id'], []).append(snapshot)
    deltas = {}
    for sid, values in grouped.items():
        if len(values) > 1:
            deltas[sid] = {'revision_delta': (values[-1]['verification']['revision']-values[0]['verification']['revision']
                                             if all('verification' in value for value in (values[0], values[-1])) else None),
                          'record_count_deltas': {table: values[-1]['counts'][table]-values[0]['counts'][table]
                                                  for table in values[0]['counts']}}
    observations = [r for r in client.observations if r['case'] == case_id]
    for snapshot in snapshots:
        observations.extend(snapshot.get('command_states', []))
    return {'submitted_requests': len(requests), 'distinct_request_keys': len({r['key'] for r in requests}),
            'request_count_scope': 'mutating POSTs; verification GETs are not counted as logical attempts',
            'http_statuses': [r.get('http_status') for r in requests],
            'transport_failures': [r['transport_error'] for r in requests if 'transport_error' in r],
            'request_evidence': [r.get('evidence_path') for r in requests if r.get('evidence_path')],
            'observed_command_states': observations, 'ledger_deltas': deltas,
            'settlements': [item for item in getattr(client, 'settlements', []) if item['case'] == case_id],
            'snapshots': snapshots}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=float, default=180, help='Per-observation HTTP/settlement watchdog seconds')
    parser.add_argument('--second-agent-url', help='Second worker URL (or SECOND_AGENT_URL, default local port 8082)')
    parser.add_argument('--session-id', action='append', default=[], help='Additionally inspect retained session UUID')
    parser.add_argument('--non-interactive', action='store_true', help='Record required operator steps as not_exercised instead of prompting')
    parser.add_argument('--skip-restart', action='store_true', help='Do not request restart; C08 remains not_exercised')
    parser.add_argument('--skip-pruning', action='store_true', help='Check retention declarations only; C12 remains not_exercised')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    env = {**dotenv_values(ROOT / '.env'), **os.environ}
    os.umask(0o077)
    directory = ROOT / 'archive/persistence' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid4().hex[:8])
    directory.mkdir(parents=True, mode=0o700)
    report = {'contract_version': CONTRACT, 'storage_schema': SCHEMA_VERSION, 'suite': 'DU-06 contract 2.0.0',
              'status': 'running', 'scope': 'DU-06-V2-C01 through DU-06-V2-C12; common API and read-only PostgreSQL',
              'agent_conformance': 'not_assessed', 'watchdog_seconds': args.timeout,
              'cases': {f'DU-06-V2-C{n:02d}': {'name': name, 'status': 'not_exercised', 'reason': 'Not reached'}
                        for n, name in enumerate(CASE_NAMES, 1)},
              'database_access': 'SELECT/read-only inspection; mutations only through agent API or operator administrative tooling',
              'excluded_claims': ['fresh schema creation', 'exact internal write-stage coverage',
                                  'suppressed PostgreSQL COMMIT response', 'whole-contract conformance']}
    sources = [*sorted((ROOT / 'scripts').glob('*.py')), ROOT / 'persist.sh', ROOT / 'scripts/pyproject.toml',
               ROOT / 'scripts/uv.lock', ROOT / 'schema/postgres.sql', ROOT / 'schema/layout-v6.json',
               ROOT / 'schema/idempotency-vectors.json']
    report['source_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    output = directory / 'result.json'
    client = cases = None

    def save():
        temporary = output.with_suffix('.tmp')
        temporary.write_text(json.dumps(report, indent=2)+'\n')
        temporary.replace(output)

    print(f'DU-06 contract {CONTRACT}; evidence: {directory}', flush=True)
    print('C07 deliberately disconnects a test stream. C08 requests a server restart. C09 needs a second worker.\n'
          'C12 requests verified export/pruning of a test-owned terminal session through normal administrative tooling.', flush=True)
    save()
    try:
        database_url = env.get('AGENT_SESSION_DATABASE_URL')
        require(database_url, 'Set AGENT_SESSION_DATABASE_URL in repository .env')
        base_url = env.get('AGENT_URL') or f"http://127.0.0.1:{env.get('PORT', '8081')}"
        second = args.second_agent_url or env.get('SECOND_AGENT_URL') or f"http://127.0.0.1:{env.get('SECOND_PORT', '8082')}"
        report['target'], report['second_worker'] = base_url, second
        client = Client(base_url, database_url, directory, args.timeout)
        health = client.http.get('/health')
        require(health.status_code == 200 and health.json().get('contract_version') == CONTRACT and
                health.json().get('storage_schema') == SCHEMA_VERSION, 'Target must advertise contract 2.0.0 / schema 6')
        report['runtime'] = health.json()
        # Validation precedes all test writes. It cannot provision or repair a database.
        report['setup'] = asyncio.run(setup_checks(database_url, env.get('AGENT_FIXTURE_DATABASE_URL') or database_url))
        cases = Cases(client, env.get('AUX_SOCKET_MODEL_URL') or 'http://127.0.0.1:18089/v1', second,
                      interactive=not args.non_interactive and sys.stdin.isatty(),
                      skip_restart=args.skip_restart, skip_pruning=args.skip_pruning,
                      fixture_url=env.get('AGENT_FIXTURE_DATABASE_URL') or database_url)
        for index, name in enumerate(CASE_NAMES, 1):
            identity = f'DU-06-V2-C{index:02d}'
            client.case_id = identity
            print(f'RUN {identity}: {name}', flush=True)
            started = time.monotonic()
            result = {'name': name, 'status': 'running'}
            report['cases'][identity] = result
            save()
            try:
                result['evidence'] = getattr(cases, f'c{index:02d}')()
                result['status'] = 'passed'
                result['ledger_invariant_violations'] = 0
            except NotExercised as error:
                result.update(status='not_exercised', reason=str(error))
            except Exception as error:
                result.update(status='failed', error=f'{type(error).__name__}: {error}')
            finally:
                if result['status'] == 'running':
                    result.update(status='not_exercised', reason='Execution interrupted before scenario completed')
                result['elapsed_seconds'] = time.monotonic()-started
                result['measurements'] = case_measurements(client, identity)
                report['subchecks'] = cases.subchecks
                save()
                message = result.get('error') or result.get('reason') or ''
                print(f"{result['status'].upper()} {identity}"+(': '+message if message else ''), flush=True)
        for sid in args.session_id:
            client.case_id = 'supplemental-ledger'
            client.sessions.append(sid) if sid not in client.sessions else None
            _, evidence = client.observe(sid, 'supplemental')
            report.setdefault('supplemental_sessions', []).append(evidence)
    except Exception as error:
        report['harness_error'] = f'{type(error).__name__}: {error}'
        print('FAIL '+report['harness_error'], flush=True)
    finally:
        if client is not None:
            client.case_id = 'final-ledger'
            for sid in dict.fromkeys(client.sessions):
                try:
                    _, evidence = client.observe(sid, 'final')
                    report.setdefault('final_ledger', []).append(evidence)
                except Exception as error:
                    report.setdefault('final_ledger_errors', []).append({'session_id': sid, 'error': f'{type(error).__name__}: {error}'})
            report['retained_sessions'] = list(dict.fromkeys(client.sessions))
            report['observed_commands'] = client.observations
            report['settlements'] = getattr(client, 'settlements', [])
            report['requests'] = client.requests
            (directory / 'requests.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in client.requests))
            client.close()
        states = [value['status'] for value in report['cases'].values()]
        failed = 'failed' in states or report.get('harness_error') or report.get('final_ledger_errors')
        report['status'] = 'failed' if failed else 'incomplete' if any(status != 'passed' for status in states) else 'passed'
        report['du06_qualification'] = report['status']
        report['coverage'] = {'passed': states.count('passed'), 'failed': states.count('failed'),
                              'not_exercised': sum(status not in ('passed', 'failed') for status in states), 'required': 12}
        save()
        print(f"{report['status'].upper()}: {report['coverage']}\nEvidence: {output}", flush=True)
    return 0 if report['status'] == 'passed' else 1 if report['status'] == 'failed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
