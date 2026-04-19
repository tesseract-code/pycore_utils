import json
from datetime import datetime, date
from types import MappingProxyType
from typing import Any

# Unique sentinel avoids collision with real user dict keys
_SENTINEL = "__pyserial_type__"


class DictSerializer:
    """
    Serializes dicts containing types that JSON doesn't support natively.

    Supported extra types: datetime, date, set, bytes, tuple.
    Subclass and override ENCODERS/DECODERS to extend.
    """

    # MappingProxyType prevents accidental mutation of class-level state
    ENCODERS: MappingProxyType = MappingProxyType({
        datetime: lambda v: {_SENTINEL: "datetime", "value": v.isoformat()},
        date:     lambda v: {_SENTINEL: "date",     "value": v.isoformat()},
        set:      lambda v: {_SENTINEL: "set",      "value": sorted(v)},  # sorted for determinism
        bytes:    lambda v: {_SENTINEL: "bytes",    "value": v.hex()},
        tuple:    lambda v: {_SENTINEL: "tuple",    "value": list(v)},
    })

    DECODERS: MappingProxyType = MappingProxyType({
        "datetime": lambda v: datetime.fromisoformat(v),
        "date":     lambda v: date.fromisoformat(v),
        "set":      lambda v: set(v),
        "bytes":    lambda v: bytes.fromhex(v),
        "tuple":    lambda v: tuple(v),
    })

    def __init__(self) -> None:
        # Instance-level copies prevent cross-instance mutation
        self._encoders: dict = dict(self.ENCODERS)
        self._decoders: dict = dict(self.DECODERS)

    def _preprocess(self, obj: Any) -> Any:
        """
        Recursively walk the structure and replace special types with their
        tagged-dict form BEFORE json.dumps runs.

        This is necessary because json.dumps converts tuples → JSON arrays
        natively, bypassing the `default` hook entirely. Preprocessing ensures
        tuples (and any other natively-serializable special types) are tagged.
        """
        # Exact type match first — avoids datetime→date subclass ambiguity
        encoder = self._encoders.get(type(obj))
        if encoder is not None:
            tagged = encoder(obj)
            # Recursively preprocess the tagged value (e.g. tuple of datetimes)
            if isinstance(tagged, dict) and "value" in tagged:
                tagged = {**tagged, "value": self._preprocess(tagged["value"])}
            return tagged

        # Subclass fallback (e.g. a custom date subclass)
        for typ, enc in self._encoders.items():
            if isinstance(obj, typ):
                tagged = enc(obj)
                if isinstance(tagged, dict) and "value" in tagged:
                    tagged = {**tagged, "value": self._preprocess(tagged["value"])}
                return tagged

        if isinstance(obj, dict):
            return {k: self._preprocess(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._preprocess(v) for v in obj]

        return obj  # primitive — JSON handles it natively

    def _decode_hook(self, obj: dict) -> Any:
        if _SENTINEL not in obj:
            return obj

        t = obj[_SENTINEL]

        # BUG FIX: raise on unknown tags instead of silently returning raw dict
        if t not in self._decoders:
            raise ValueError(
                f"Unknown type tag {t!r} — no decoder registered. "
                f"Known tags: {list(self._decoders)}"
            )

        # BUG FIX: wrap decoder errors with context to aid debugging
        try:
            return self._decoders[t](obj["value"])
        except Exception as e:
            raise ValueError(
                f"Failed to decode type {t!r} from value {obj['value']!r}"
            ) from e

    def serialize(self, data: Any, **kwargs) -> str:
        """Serialize a dict (or any JSON-compatible structure) to a JSON string."""
        return json.dumps(self._preprocess(data), **kwargs)

    def deserialize(self, raw: str) -> Any:
        """Deserialize a JSON string back to a Python object, restoring custom types."""
        return json.loads(raw, object_hook=self._decode_hook)
