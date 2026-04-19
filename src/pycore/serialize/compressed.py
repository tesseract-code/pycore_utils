
"""
compressed.py Compressed Serializer

Wraps any serializer and compresses the output. The compression algorithm
is stored as a 1-byte magic header so the decompressor can auto-detect it
without out-of-band configuration — the same pattern used by spaCy's srsly.

Supported algorithms (pluggable via strategy pattern):
    zlib  — stdlib, fast, good general-purpose compression
    gzip  — stdlib, compatible with gzip tooling

The threshold parameter prevents compressing small payloads where the
compressed form would actually be larger than the original.
"""
import gzip
from enum import StrEnum, unique
from typing import Any

import zlib

from pycore.serialize.base import BaseSerializer


@unique
class CompressionAlgo(StrEnum):
    ZLIB = "zlib"
    GZIP = "gzip"


# Magic byte prefix stored before compressed payload for auto-detection
_COMPRESSION_MAGIC: dict[CompressionAlgo, bytes] = {
    CompressionAlgo.ZLIB: b"\x5a",  # 'Z'
    CompressionAlgo.GZIP: b"\x1f",  # standard gzip magic byte 1
}

_MAGIC_TO_ALGO: dict[bytes, CompressionAlgo] = {
    v: k for k, v in _COMPRESSION_MAGIC.items()
}


class CompressedSerializer(BaseSerializer):
    """
    Transparent compression wrapper around any BaseSerializer.

    Auto-detects the algorithm on deserialization from a 1-byte magic prefix.
    Falls back gracefully to uncompressed if the payload has no recognized
    magic byte (allows gradual rollout alongside legacy uncompressed data).

    Parameters
    ----------
    base        : inner serializer
    algorithm   : CompressionAlgo.ZLIB (default) or .GZIP
    level       : compression level 0–9 (default 6, same as zlib default)
    threshold   : min raw bytes before compressing (default 256).
                  Payloads smaller than this are stored uncompressed with a
                  0x00 prefix to keep the magic-byte protocol consistent.
    """

    _NO_COMPRESSION_MAGIC = b"\x00"

    def __init__(
            self,
            base: BaseSerializer,
            algorithm: CompressionAlgo = CompressionAlgo.ZLIB,
            level: int = 6,
            threshold: int = 256,
    ) -> None:
        self._base = base
        self._algorithm = algorithm
        self._level = level
        self._threshold = threshold

    def _compress(self, data: bytes) -> bytes:
        if self._algorithm == CompressionAlgo.ZLIB:
            return zlib.compress(data, level=self._level)
        if self._algorithm == CompressionAlgo.GZIP:
            return gzip.compress(data, compresslevel=self._level)
        raise ValueError(f"Unknown algorithm: {self._algorithm}")

    @staticmethod
    def _decompress(algo: CompressionAlgo, data: bytes) -> bytes:
        try:
            if algo == CompressionAlgo.ZLIB:
                return zlib.decompress(data)
            if algo == CompressionAlgo.GZIP:
                return gzip.decompress(data)
        except Exception as exc:
            raise ValueError(
                f"CompressedSerializer: {algo.value} decompression failed"
            ) from exc
        raise ValueError(f"Unknown algorithm: {algo}")

    def serialize(self, data: Any) -> bytes:
        raw = self._base.serialize(data)
        if len(raw) < self._threshold:
            return self._NO_COMPRESSION_MAGIC + raw
        compressed = self._compress(raw)
        magic = _COMPRESSION_MAGIC[self._algorithm]
        return magic + compressed

    def deserialize(self, raw: bytes) -> bytes:
        if not raw:
            raise ValueError("CompressedSerializer: empty payload")
        magic = raw[:1]
        payload = raw[1:]

        if magic == self._NO_COMPRESSION_MAGIC:
            return self._base.deserialize(payload)

        algo = _MAGIC_TO_ALGO.get(magic)
        if algo is None:
            # Legacy payload with no magic byte — try to deserialize as-is
            # This enables rolling deployment alongside old uncompressed data
            return self._base.deserialize(raw)

        decompressed = self._decompress(algo, payload)
        return self._base.deserialize(decompressed)

    def stats(self, data: Any) -> dict:
        """Return compression ratio and savings for a given payload."""
        raw = self._base.serialize(data)
        compressed_full = self.serialize(data)
        ratio = len(compressed_full) / len(raw) if raw else 1.0
        return {
            "raw_bytes": len(raw),
            "compressed_bytes": len(compressed_full),
            "ratio": round(ratio, 4),
            "savings_pct": round((1 - ratio) * 100, 1),
        }
