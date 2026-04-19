import copy
from dataclasses import is_dataclass
from typing import Any


class SettingsAccessor:
    """Helper to get/set nested fields in settings objects."""

    @staticmethod
    def get_nested(obj: Any, path: str) -> Any:
        """
        Get nested field value.
        Supports both nested paths (e.g., "database.host") and flat fields (e.g., "zoom").
        """
        if '.' not in path:
            # Simple field access for flat structures (NamedTuples, simple dataclasses)
            if isinstance(obj, dict):
                return obj[path]
            elif hasattr(obj, path):
                return getattr(obj, path)
            else:
                raise KeyError(f"Field '{path}' not found")

        # Nested path access
        parts = path.split('.')
        current = obj

        for part in parts:
            if isinstance(current, dict):
                current = current[part]
            elif hasattr(current, part):
                current = getattr(current, part)
            else:
                raise KeyError(f"Field '{part}' not found in path '{path}'")

        return current

    @staticmethod
    def set_nested(obj: Any, path: str, value: Any) -> Any:
        """
        Set nested field value. Returns modified object.
        Supports both nested paths and flat fields.
        For immutable types (NamedTuple, frozen dataclass), creates a new instance.
        """
        if '.' not in path:
            # Simple field update for flat structures
            if isinstance(obj, dict):
                obj = copy.deepcopy(obj)
                obj[path] = value
                return obj
            elif isinstance(obj, tuple) and hasattr(obj, '_replace'):
                # NamedTuple - use _replace
                return obj._replace(**{path: value})
            elif is_dataclass(obj):
                obj = copy.deepcopy(obj)
                setattr(obj, path, value)
                return obj
            else:
                # Mutable object
                obj = copy.copy(obj)
                setattr(obj, path, value)
                return obj

        # Nested path access
        parts = path.split('.')

        # Make deep copy for safety
        if isinstance(obj, dict):
            obj = copy.deepcopy(obj)
        elif is_dataclass(obj):
            obj = copy.deepcopy(obj)
        else:
            obj = copy.copy(obj)

        # Navigate to parent
        current = obj
        for part in parts[:-1]:
            if isinstance(current, dict):
                current = current[part]
            else:
                current = getattr(current, part)

        # Set final value
        final_key = parts[-1]
        if isinstance(current, dict):
            current[final_key] = value
        else:
            setattr(current, final_key, value)

        return obj

    @staticmethod
    def validate_path(obj: Any, path: str) -> bool:
        """Check if path exists in object."""
        try:
            SettingsAccessor.get_nested(obj, path)
            return True
        except (KeyError, AttributeError):
            return False
