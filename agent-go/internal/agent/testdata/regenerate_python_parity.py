"""Refresh Go request goldens from current Python without model or database calls.

Run from repository root with agent-python/.venv/bin/python and this script path.
"""
import json
import os
from pathlib import Path
from agent_python.config import ConfigRepository
from agent_python.engine import AgentEngine
from agent_python.models import Message
from agent_python.transcript import append_message

os.environ['MAIN_MODEL'] = 'parity-model'
path = Path(__file__).with_name('python_request_parity.json')
data = json.loads(path.read_text())
repository = ConfigRepository()
for case in data['cases']:
    snapshot = repository.resolve(case['ref'])
    config = snapshot.config.model_copy(deep=True)
    config.llm.tool_call_mode = case['mode']
    config.model = 'parity-model'
    engine = AgentEngine(config)
    state = engine.create_session('Parity regression goal.', case['ref'], snapshot.sha256)
    variant = case['variant']
    if variant in ('complete', 'complete_artifacts'):
        state.workflow.active_phase = None
        state.workflow.completed_phases = [p.id for p in config.workflow.phases]
        if variant == 'complete_artifacts':
            state.completion_artifacts = {'0': {'value': [['City','Valid headcount','Average age'],['Austin','199','42.62']]}, '1': {'value': 92500.0}}
    elif variant == 'after_inspection':
        append_message(state, Message(role='assistant', tool_calls=[{'id':'inspection','type':'function','function':{'name':'inspect_log','arguments':'{"start_line":1,"line_count":10}'}}]))
        append_message(state, Message(role='tool', name='inspect_log', tool_call_id='inspection', content='First log excerpt.'))
    elif variant == 'compacted':
        state.compacted_context = 'Retained historical claims.'
    elif variant != 'initial':
        state.workflow.active_phase = variant
    case['payload'] = engine._decision_payload(state)
summary_config = repository.resolve('log-analysis.yaml').config
summary_config.model = 'parity-model'
engine = AgentEngine(summary_config)
state = engine.create_session('Parity regression goal.')
for name in data['argument_feedback']:
    data['argument_feedback'][name] = engine._argument_feedback(state,name,{}, {t.name:t for t in engine.config.tools})
for summary in data['summaries']:
    summary['payload'] = engine.llm.compaction_payload(state.goal,[Message(role='user',content='Historical observation: café — 😀.')],None,summary['hybrid'])
data['source'] = 'Generated offline from current agent-python runtime by regenerate_python_parity.py; no model or database calls.'
path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
