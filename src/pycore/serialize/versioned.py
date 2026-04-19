"""
VersionedSerializer

Problem: serialized data outlives the code that wrote it. A field gets
renamed, a type changes, a key is removed. Old payloads in a cache or a
message queue can no longer be deserialized without a migration layer.

Pattern: embed a schema version number in every payload (a single-byte
prefix or a header dict). On deserialization, detect the version and run
the registered migration chain to bring the payload up to current schema.
This is how Django migrations, Avro schema evolution, and the CocoIndex
"routing byte" pattern all work.

The migration chain is a dict: {from_version: migration_fn}. Migrations
compose: v1 → v2 → v3 via repeated lookup.
"""

import base64
import json
from typing import Callable, Any

from pycore.serialize.base import BaseSerializer


class VersionedSerializer(BaseSerializer):
    """
    Wraps any BaseSerializer and adds schema version tracking + migration.

    Usage
    -----
        ser = VersionedSerializer(base=MsgPackSerializer(), current_version=3)
        ser.register_migration(1, lambda d: {**d, "new_field": "default"})
        ser.register_migration(2, lambda d: {**d, "renamed": d.pop("old_name")})

        payload = ser.serialize(my_dict)      # writes version=3 header
        data = ser.deserialize(old_payload)   # auto-migrates v1 → v2 → v3

    Wire format (JSON envelope around base-serialized payload)
    -----------------------------------------------------------
    {
      "__schema_version__": 3,
      "__payload__": <base64-encoded bytes from inner serializer>
    }

    Using a JSON envelope keeps the version number human-readable and
    inspectable even when the inner payload is binary (msgpack).
    """

    _VERSION_KEY = "__schema_version__"
    _PAYLOAD_KEY = "__payload__"

    def __init__(self, base: BaseSerializer, current_version: int = 1) -> None:
        if current_version < 1:
            raise ValueError("current_version must be >= 1")
        self._base = base
        self._current_version = current_version
        # {from_version: migration_fn(data: dict) -> dict}
        self._migrations: dict[int, Callable[[Any], Any]] = {}

    @property
    def current_version(self) -> int:
        return self._current_version

    def register_migration(self, from_version: int,
                           fn: Callable[[Any], Any]) -> "VersionedSerializer":
        """
        Register a migration from `from_version` to `from_version + 1`.
        Returns self for chaining.
        """
        if from_version >= self._current_version:
            raise ValueError(
                f"Migration from v{from_version} would overshoot "
                f"current version v{self._current_version}"
            )
        if from_version in self._migrations:
            raise ValueError(f"Migration from v{from_version} already registered")
        self._migrations[from_version] = fn
        return self

    def _migrate(self, data: Any, from_version: int) -> Any:
        """Walk the migration chain: from_version → … → current_version."""
        version = from_version
        while version < self._current_version:
            migration = self._migrations.get(version)
            if migration is None:
                raise ValueError(
                    f"No migration registered from v{version} to v{version + 1}. "
                    f"Registered migrations: {sorted(self._migrations)}"
                )
            try:
                data = migration(data)
            except Exception as exc:
                raise ValueError(
                    f"Migration from v{version} to v{version + 1} failed"
                ) from exc
            version += 1
        return data

    def serialize(self, data: Any) -> bytes:
        inner = self._base.serialize(data)
        envelope = {
            self._VERSION_KEY: self._current_version,
            self._PAYLOAD_KEY: base64.b64encode(inner).decode("ascii"),
        }
        return json.dumps(envelope).encode("utf-8")

    def deserialize(self, raw: bytes) -> Any:
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("VersionedSerializer: malformed envelope") from exc

        if self._VERSION_KEY not in envelope:
            raise ValueError(
                f"VersionedSerializer: missing {self._VERSION_KEY!r} in envelope"
            )

        payload_version = envelope[self._VERSION_KEY]
        if not isinstance(payload_version, int) or payload_version < 1:
            raise ValueError(
                f"VersionedSerializer: invalid version {payload_version!r}"
            )

        inner_bytes = base64.b64decode(envelope[self._PAYLOAD_KEY])
        data = self._base.deserialize(inner_bytes)

        if payload_version < self._current_version:
            data = self._migrate(data, from_version=payload_version)
        elif payload_version > self._current_version:
            raise ValueError(
                f"VersionedSerializer: payload is v{payload_version} but "
                f"this serializer only understands up to v{self._current_version}. "
                f"Upgrade your serializer."
            )

        return data
