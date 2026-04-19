from dataclasses import dataclass, field
from enum import unique, StrEnum
from typing import Optional, Any, Dict


@unique
class OperationType(StrEnum):
    """Settings operation types."""
    GET = "get"
    SET = "set"
    UPDATE = "update"
    DELETE = "delete"
    RESET = "reset"
    VALIDATE = "validate"
    SNAPSHOT = "snapshot"


@dataclass
class SettingsRequest:
    """Request to settings server."""
    operation: OperationType
    field_path: Optional[str] = None
    value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    request_id: str = field(default_factory=lambda: str(id(object())))


@dataclass
class SettingsResponse:
    """Response from settings server."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    request_id: Optional[str] = None
    sequence: int = 0


