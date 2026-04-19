from abc import ABC, abstractmethod
from typing import Any, TypeVar

class BaseSerializer(ABC):
    """
    Minimal interface every serializer must implement.
    Compose serializers via wrapping (Compressed(Encrypted(MsgPack()))).
    """

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        ...

    @abstractmethod
    def deserialize(self, raw: bytes) -> Any:
        ...

    def roundtrip(self, data: Any) -> Any:
        """Convenience: serialize then immediately deserialize."""
        return self.deserialize(self.serialize(data))
