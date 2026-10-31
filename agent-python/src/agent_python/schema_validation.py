"""Read-only PostgreSQL layout verification for contract 2.0.0 setup/tests."""

import hashlib
import json
import re
from pathlib import Path

SCHEMA = Path(__file__).resolve().parents[3] / "schema"


def tables(definition):
    return re.findall(r"CREATE TABLE (\w+)", (SCHEMA / definition).read_text())


async def layout(connection, definition):
    cursor = await connection.execute("SELECT current_schema()")
    schema = (await cursor.fetchone())[0]
    result = {}
    for table in tables(definition):
        cursor = await connection.execute("""SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
            pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a
            JOIN pg_class t ON t.oid=a.attrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid=t.oid AND d.adnum=a.attnum
            WHERE n.nspname=%s AND t.relname=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum""", (schema, table))
        columns = await cursor.fetchall()
        cursor = await connection.execute("""SELECT c.conname,c.contype,c.condeferrable,c.condeferred,
            pg_get_constraintdef(c.oid,true) FROM pg_constraint c
            JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=%s AND t.relname=%s AND c.contype<>'n' ORDER BY c.conname""", (schema, table))
        constraints = await cursor.fetchall()
        cursor = await connection.execute("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s ORDER BY indexname", (schema, table))
        indexes = await cursor.fetchall()
        def normalize(value):
            if isinstance(value, str):
                return value.replace(f'"{schema}".', "").replace(f"{schema}.", "")
            if isinstance(value, (list, tuple)):
                return [normalize(item) for item in value]
            return value
        result[table] = normalize([columns, constraints, indexes])
    return result


async def validate(connection, definition, validation):
    manifest = json.loads((SCHEMA / "layout-v6.json").read_text())[definition]
    source_hash = hashlib.sha256((SCHEMA / definition).read_bytes()).hexdigest()
    if source_hash != manifest["source_sha256"]:
        raise RuntimeError(f"Layout manifest does not match {definition}; review the schema change")
    await connection.execute((SCHEMA / validation).read_text())
    actual = await layout(connection, definition)
    expected = manifest["tables"]
    differences = [table for table in expected if actual.get(table) != expected[table]]
    if differences:
        raise RuntimeError("Schema 6 layout differs (columns/types/defaults/constraints/indexes): " + ", ".join(differences))
