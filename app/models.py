from dataclasses import dataclass
from typing import Any


@dataclass
class ReplayOutcome:
    decision: str
    fix_applied: str
    discard_reason: str | None
    target_table: str | None
    error: str | None = None


def row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    return {key: row[key] for key in row.keys()}
