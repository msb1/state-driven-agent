"""Workflow retries retain model turns and correlate every returned tool call."""
import unittest
from unittest.mock import AsyncMock, patch

from agent_python.config import ConfigRepository
from agent_python.engine import AgentEngine


class WorkflowConversation(unittest.IsolatedAsyncioTestCase):
    def engine(self):
        return AgentEngine(ConfigRepository().resolve('log-analysis.yaml').config)

    async def test_batch_is_recorded_with_correlated_results(self):
        engine = self.engine()
        state = engine.create_session('Analyze the log.')
        engine.llm.decide = AsyncMock(return_value={'type':'tools','calls':[
            {'id':str(i),'name':'inspect_log','arguments':{'start_line':1,'line_count':1}}
            for i in (1,2)]})
        engine._compact_if_needed = AsyncMock(return_value=False)
        with patch('agent_python.engine.execute', AsyncMock(return_value='Observed log line.')):
            await engine.run(state, max_steps=1)
        calls = [m for m in state.memory if m.role=='assistant' and m.tool_calls]
        self.assertEqual(len(calls),1)
        self.assertEqual(len(calls[0].tool_calls),2)
        results = [m for m in state.memory if m.role=='tool']
        self.assertEqual({m.tool_call_id for m in results},{'1','2'})
        self.assertFalse(any('exactly one function call' in (m.content or '') for m in state.memory))

    async def test_premature_final_keeps_assistant_before_user_correction(self):
        engine = self.engine()
        state = engine.create_session('Analyze the log.')
        engine.llm.decide = AsyncMock(return_value={'type':'final','content':'Premature answer.'})
        engine._compact_if_needed = AsyncMock(return_value=False)
        await engine.run(state,max_steps=1)
        self.assertEqual([m.role for m in state.memory],['user','assistant','user'])
        self.assertEqual(state.memory[1].content,'Premature answer.')
