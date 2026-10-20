"""Stable transcript identities and a transient outbox, independent of prompt memory."""
import copy
from .models import Message, SessionState, TranscriptEntry


def stage_messages(state: SessionState, messages=None) -> None:
    for message in state.memory if messages is None else messages:
        if message.sequence is None:
            message.sequence = state.next_transcript_sequence
            state.next_transcript_sequence += 1
            state.transcript_pending.append(TranscriptEntry(
                message_id=message.message_id, sequence=message.sequence, kind='message',
                payload=message.wire_payload(), run_id=state.run_id,
                decision_step=state.step_count, created_at=message.created_at))


def append_message(state: SessionState, message: Message) -> None:
    stage_messages(state)  # Number any earlier unstaged messages before the new turn.
    state.memory.append(message)
    stage_messages(state, [message])


def archive_record(state: SessionState, kind: str, payload: dict) -> TranscriptEntry:
    entry = TranscriptEntry(sequence=state.next_transcript_sequence, kind=kind,
                            payload=copy.deepcopy(payload), run_id=state.run_id, decision_step=state.step_count)
    state.next_transcript_sequence += 1
    state.transcript_pending.append(entry)
    return entry
