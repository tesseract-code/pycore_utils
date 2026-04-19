"""
dataclass.py - Type-guided serialization for Python dataclasses
"""
import dataclasses
import inspect
import typing
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pycore.serialize.base import BaseSerializer
from pycore.serialize.msgpack import MsgPackSerializer

def _get_origin(tp: Any) -> Any:
    return getattr(tp, "__origin__", None)


def _get_args(tp: Any) -> tuple:
    return getattr(tp, "__args__", ()) or ()


class DataclassSerializer(BaseSerializer):
    """
    Type-guided serialization for Python dataclasses.

    Converts dataclasses to plain dicts for any inner serializer, then
    reconstructs the original typed objects on deserialization by following
    the dataclass field type annotations.

    Handles
    -------
    - Nested dataclasses (arbitrary depth)
    - Optional[T] / T | None
    - List[T], list[T]
    - Dict[K, V], dict[K, V]
    - Enum subclasses (stored as their .value)
    - datetime, date (ISO 8601)
    - UUID (str form)
    - Decimal (str form)
    - Falls back to identity for unknown types (passes value through)

    Parameters
    ----------
    base    : inner serializer (default: MsgPackSerializer)
    strict  : if True, raise on unrecognized fields in the payload rather
              than silently ignoring them (default: False for forward compat)
    """

    def __init__(
            self,
            base: BaseSerializer | None = None,
            strict: bool = False,
    ) -> None:
        self._base = base or MsgPackSerializer()
        self._strict = strict

    # ── Encoding (dataclass → plain dict) ────────────────────────────────────

    def _encode_value(self, value: Any) -> Any:
        """Recursively convert a value to a JSON-safe primitive."""
        if is_dataclass(value) and not isinstance(value, type):
            return {f.name: self._encode_value(getattr(value, f.name))
                    for f in fields(value)}
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (list, tuple)):
            encoded = [self._encode_value(v) for v in value]
            return encoded
        if isinstance(value, dict):
            return {k: self._encode_value(v) for k, v in value.items()}
        return value

    # ── Decoding (plain dict → typed dataclass) ───────────────────────────────

    def _decode_value(self, value: Any, type_hint: Any) -> Any:
        """Reconstruct a typed value from a plain primitive using a type hint."""
        if type_hint is None or type_hint is inspect.Parameter.empty:
            return value

        origin = _get_origin(type_hint)
        args = _get_args(type_hint)

        # Optional[T] / Union[T, None]
        if origin is typing.Union:
            non_none = [a for a in args if a is not type(None)]
            if value is None:
                return None
            # Try each non-None type in order (most specific first)
            for candidate in non_none:
                try:
                    return self._decode_value(value, candidate)
                except (TypeError, ValueError):
                    continue
            return value

        # List[T] / list[T]
        if origin in (list,) or type_hint is list:
            inner_type = args[0] if args else None
            if not isinstance(value, (list, tuple)):
                raise TypeError(f"Expected list, got {type(value).__name__}")
            return [self._decode_value(v, inner_type) for v in value]

        # Dict[K, V]
        if origin in (dict,) or type_hint is dict:
            k_type = args[0] if len(args) > 0 else None
            v_type = args[1] if len(args) > 1 else None
            return {
                self._decode_value(k, k_type): self._decode_value(v, v_type)
                for k, v in value.items()
            }

        # Dataclass
        if is_dataclass(type_hint) and isinstance(value, dict):
            return self._dict_to_dataclass(value, type_hint)

        # Enum
        if isinstance(type_hint, type) and issubclass(type_hint, Enum):
            return type_hint(value)

        # datetime / date — note: datetime check MUST come before date
        if type_hint is datetime:
            return datetime.fromisoformat(value) if isinstance(value,
                                                               str) else value
        if type_hint is date:
            return date.fromisoformat(value) if isinstance(value,
                                                           str) else value

        # UUID
        if type_hint is UUID:
            return UUID(value) if isinstance(value, str) else value

        # Decimal
        if type_hint is Decimal:
            return Decimal(str(value))

        # Primitive — return as-is (str, int, float, bool, None)
        return value

    def _dict_to_dataclass(self, data: dict, dc_type: type) -> Any:
        """Reconstruct a dataclass instance from a plain dict."""
        if not is_dataclass(dc_type):
            raise TypeError(f"{dc_type} is not a dataclass")

        field_map = {f.name: f for f in fields(dc_type)}

        # get_type_hints() resolves string annotations but fails for types
        # defined in local scopes (e.g. in test functions). Fall back to
        # __annotations__ which holds the actual type objects when PEP 563
        # (`from __future__ import annotations`) is NOT in effect.
        try:
            import sys
            module = sys.modules.get(dc_type.__module__)
            hints = typing.get_type_hints(
                dc_type,
                globalns=vars(module) if module else {},
            )
        except Exception:
            hints = dc_type.__annotations__

        kwargs: dict[str, Any] = {}

        for field_name, field_obj in field_map.items():
            if field_name not in data:
                if field_obj.default is not dataclasses.MISSING:
                    kwargs[field_name] = field_obj.default
                elif field_obj.default_factory is not dataclasses.MISSING:  # type: ignore[misc]
                    kwargs[field_name] = field_obj.default_factory()
                else:
                    raise ValueError(
                        f"DataclassSerializer: missing required field "
                        f"{field_name!r} for {dc_type.__name__}"
                    )
                continue
            kwargs[field_name] = self._decode_value(data[field_name],
                                                    hints.get(field_name))

        if self._strict:
            extra = set(data) - set(field_map)
            if extra:
                raise ValueError(
                    f"DataclassSerializer (strict): unexpected fields "
                    f"{extra} for {dc_type.__name__}"
                )

        return dc_type(**kwargs)

    def serialize(self, data: Any) -> bytes:
        if not (is_dataclass(data) and not isinstance(data, type)):
            raise TypeError(
                f"DataclassSerializer.serialize expects a dataclass instance, "
                f"got {type(data).__name__!r}"
            )
        encoded = self._encode_value(data)
        return self._base.serialize(encoded)

    def deserialize(self, raw: bytes, target_type: type | None = None) -> Any:
        """
        Deserialize bytes to a Python object.

        Parameters
        ----------
        raw         : bytes from serialize()
        target_type : dataclass class to reconstruct into. Required unless you
                      only need the raw dict (e.g. for inspection).
        """
        decoded = self._base.deserialize(raw)
        if target_type is None:
            return decoded
        return self._dict_to_dataclass(decoded, target_type)
