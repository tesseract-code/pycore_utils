from typing import Protocol, Any


class SettingsProvider(Protocol):
    """Protocol for any object that can provide settings."""

    def get_copy(self) -> dict[str, Any]: ...
