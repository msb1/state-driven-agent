#!/usr/bin/env bash
# Contract 2.0.0 DU-06 C01–C12: common API operations and read-only ledger verification.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
exec uv run --project scripts python scripts/persistence_qualification.py "$@"
