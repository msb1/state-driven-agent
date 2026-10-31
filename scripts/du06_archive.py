"""Independent verifier for the shared sda-archive-2 JSONL evidence format."""
import hashlib
import json
from uuid import UUID

from persistence_ledger import TABLES
from persistence_support import require


def encoded_line(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)+'\n').encode()


def inspect_archive(path):
    total = hashlib.sha256()
    hashes = {name: hashlib.sha256() for name in TABLES}
    rows = {name: [] for name in TABLES}
    watermarks = {'events': 0, 'diagnostics': {}}
    manifest = None
    with open(path, 'rb') as stream:
        header = json.loads(next(stream, b''))
        require(set(header) == {'format', 'archive_id', 'session_id', 'snapshot_revision'} and
                header['format'] == 'sda-archive-2' and type(header['snapshot_revision']) is int and header['snapshot_revision'] > 0,
                'Invalid archive header')
        UUID(header['archive_id']); UUID(header['session_id'])
        total.update(encoded_line(header))
        for raw in stream:
            value = json.loads(raw)
            require(manifest is None, 'Trailing archive content after manifest')
            if 'manifest' in value:
                require(set(value) == {'manifest'}, 'Invalid manifest record')
                manifest = value['manifest']
                continue
            require(set(value) == {'table', 'row'} and value['table'] in rows and
                    value['row']['session_id'] == header['session_id'], 'Unknown table or cross-session archive row')
            table, record = value['table'], value['row']
            data = encoded_line(value)
            hashes[table].update(data)
            total.update(data)
            rows[table].append(record)
            if table == 'agent_session_events':
                watermarks['events'] = max(watermarks['events'], record['event_id'])
            elif table == 'agent_session_diagnostics':
                family = record['family']
                require(family in ('compaction_decisions', 'compaction_attempts', 'compaction_events'), 'Unknown diagnostic family')
                watermarks['diagnostics'][family] = max(watermarks['diagnostics'].get(family, 0), record['ordinal'])
    require(manifest is not None and set(manifest) == {'sha256', 'counts', 'table_sha256', 'watermarks'}, 'Missing or invalid archive manifest')
    require(manifest['sha256'] == total.hexdigest() and manifest['counts'] == {k: len(v) for k, v in rows.items()} and
            manifest['table_sha256'] == {k: v.hexdigest() for k, v in hashes.items()} and manifest['watermarks'] == watermarks,
            'Archive checksum/count/watermark mismatch')
    return header, manifest, rows
