from typing import Dict, List, Callable, Any, Optional, Type


class SettingsValidator:
    """Validation framework for settings changes."""

    def __init__(self):
        self._validators: Dict[str, List[Callable]] = {}

    def register(self, field_path: str, validator: Callable[[Any], bool]):
        """Register validator for a field path."""
        if field_path not in self._validators:
            self._validators[field_path] = []
        self._validators[field_path].append(validator)

    def validate(self, field_path: str, value: Any) -> tuple[
        bool, Optional[str]]:
        """Validate a value. Returns (is_valid, error_message)."""
        validators = self._validators.get(field_path, [])

        for validator in validators:
            try:
                if not validator(value):
                    return False, f"Validation failed for {field_path}"
            except Exception as e:
                return False, f"Validator error: {str(e)}"

        return True, None

    @staticmethod
    def type_validator(expected_type: Type):
        """Create a type checking validator."""

        def validator(value: Any) -> bool:
            return isinstance(value, expected_type)

        return validator

    @staticmethod
    def range_validator(min_val: Any, max_val: Any):
        """Create a range checking validator."""

        def validator(value: Any) -> bool:
            return min_val <= value <= max_val

        return validator
