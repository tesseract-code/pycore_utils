import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import List, Any, Optional, Dict

from pycore.settings.msg import OperationType

@dataclass
class ChangeMetadata:
    """Audit trail for settings changes."""
    timestamp: datetime
    operation: OperationType
    field_path: str
    old_value: Any
    new_value: Any
    changed_by: str
    reason: Optional[str] = None
    sequence: int = 0

    def to_dict(self) -> Dict:
        return {
            'timestamp': self.timestamp.isoformat(),
            'operation': self.operation.value,
            'field_path': self.field_path,
            'old_value': repr(self.old_value),
            'new_value': repr(self.new_value),
            'changed_by': self.changed_by,
            'reason': self.reason,
            'sequence': self.sequence
        }


@dataclass
class ChangeNotification:
    """Published when settings change."""
    metadata: ChangeMetadata
    current_settings: Any

    def to_dict(self) -> Dict:
        return {
            'metadata': self.metadata.to_dict(),
            'timestamp': self.metadata.timestamp.isoformat()
        }


class SettingsHistory:
    """Thread-safe history of settings changes."""

    def __init__(self, max_size: int = 1000):
        self._history: List[ChangeMetadata] = []
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def add(self, metadata: ChangeMetadata):
        """Add change to history."""
        async with self._lock:
            self._history.append(metadata)
            if len(self._history) > self._max_size:
                self._history.pop(0)

    async def get_recent(self, count: int = 10) -> List[ChangeMetadata]:
        """Get most recent changes."""
        async with self._lock:
            return self._history[-count:]

    async def get_for_field(self, field_path: str) -> List[ChangeMetadata]:
        """Get all changes for a specific field."""
        async with self._lock:
            return [m for m in self._history if m.field_path == field_path]
