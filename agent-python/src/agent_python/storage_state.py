"""Hot projection and append-only diagnostic outbox, independent of prompts."""
import copy

FAMILIES = ('compaction_events','compaction_attempts','compaction_decisions')
RECENT = 32


def record_diagnostic(state, family, payload):
    if family not in FAMILIES:
        raise ValueError('Unknown diagnostic family')
    ordinal = state.diagnostic_counts.get(family,0)+1
    state.diagnostic_counts[family] = ordinal
    value = copy.deepcopy(payload)
    state.diagnostics_pending.append({'family':family,'ordinal':ordinal,'payload':value})
    values = getattr(state,family)
    values.append(value)
    setattr(state,family,values[-RECENT:])
    state.diagnostics_complete = all(state.diagnostic_counts.get(k,0)<=len(getattr(state,k)) for k in FAMILIES)
    if family == 'compaction_attempts':
        if not payload.get('accepted',False):
            state.diagnostic_rejections += 1
        else:
            reduction = payload.get('reduction_tokens')
            if reduction is None and isinstance(payload.get('before_estimated_tokens'),int) and isinstance(payload.get('after_estimated_tokens'),int):
                reduction = payload['before_estimated_tokens']-payload['after_estimated_tokens']
            if type(reduction) is int:
                state.compaction_savings_total += reduction
            else:
                state.compaction_savings_known = False


def stage_diagnostics(state):
    """Import an older unjournaled state exactly once; compiled writes use record()."""
    if state.diagnostic_counts:
        return
    original = {key:copy.deepcopy(getattr(state,key)) for key in FAMILIES}
    state.diagnostic_rejections,state.compaction_savings_total = 0,0
    for key in FAMILIES:
        setattr(state,key,[])
        for value in original[key]:
            record_diagnostic(state,key,value)
    accepted = sum(bool(v.get('accepted')) for v in original['compaction_attempts'])
    if accepted != len(original['compaction_events']):
        state.compaction_savings_known = False


def hot_snapshot(state):
    return state.model_dump(mode='json',exclude={
        'transcript_pending','pending_commit_id','memory','compacted_context','ledger_state',
        'diagnostics_complete',*FAMILIES})


def legacy_snapshot(state):
    return state.model_dump(mode='json',exclude={'transcript_pending','pending_commit_id'})
