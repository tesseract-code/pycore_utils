import logging
from typing import TypeVar, Optional, Type


class ContextAdapter(logging.LoggerAdapter):
    """
    Wraps a logger to automatically handle context.
    Usage:
        logger = ContextAdapter(logging.getLogger(__name__), {})
        logger.info("User logged in", user_id=123)
    """

    def process(self, msg, kwargs):
        context = kwargs.pop('extra', {})
        # Merge kwargs into context if passed directly (e.g. logger.info("msg", key=val))
        # Note: LoggerAdapter standard usage puts context in the constructor,
        # but here we allow per-call context.
        context.update(kwargs)

        if context:
            # Format the message to your specific style
            ctx_str = " ".join([f"{k}={v}" for k, v in context.items()])
            return f"{msg} | {ctx_str}", {}  # Return empty kwargs to avoid double passing

        return msg, kwargs


T = TypeVar('T')


def with_logger(
        cls: Optional[Type[T]] = None,
        *,
        attr_name: str = "_logger",
        logger_name: Optional[str] = None
) -> Type[T]:
    """Class decorator to add a logger to a class.

    Args:
        cls: Class to decorate
        attr_name: Name of the logger attribute
        logger_name: Custom logger name. If None, uses fully qualified class name
    """

    def wrap(cls: Type[T]) -> Type[T]:
        if attr_name in cls.__dict__:
            import warnings
            warnings.warn(
                f"Class {cls.__name__} already defines attribute '{attr_name}'. Overwriting.",
                UserWarning,
                stacklevel=2
            )

        if logger_name is not None:
            name = logger_name
        else:
            if cls.__module__ is None:
                raise ValueError(
                    f"Class {cls.__qualname__} has no __module__ attribute")
            name = f"{cls.__module__}.{cls.__qualname__}"

        setattr(cls, attr_name, ContextAdapter(logging.getLogger(name), {}))
        return cls

    if cls is None:
        return wrap
    return wrap(cls)
