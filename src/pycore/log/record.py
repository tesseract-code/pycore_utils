import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class LogRecordData:
    """
    Serializable log record data structure.
    """
    name: str
    level: int
    pathname: str
    lineno: int
    msg: str
    args: tuple
    exc_info: Optional[str]
    func: Optional[str]
    created: float
    msecs: float
    levelname: str
    process: int
    thread: int
    threadName: str

    @classmethod
    def from_log_record(cls, record: logging.LogRecord) -> 'LogRecordData':
        """Create from a logging.LogRecord."""
        return cls(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=record.getMessage(),
            args=(),  # Already formatted in msg
            exc_info=cls._format_exc_info(
                record.exc_info) if record.exc_info else None,
            func=record.funcName,
            created=record.created,
            msecs=record.msecs,
            levelname=record.levelname,
            process=record.process,
            thread=record.thread,
            threadName=record.threadName
        )

    @staticmethod
    def _format_exc_info(exc_info) -> str:
        """Format exception info as string."""
        import traceback
        return ''.join(traceback.format_exception(*exc_info))

    def to_log_record(self) -> logging.LogRecord:
        """Convert back to a logging.LogRecord."""
        record = logging.LogRecord(
            name=self.name,
            level=self.level,
            pathname=self.pathname,
            lineno=self.lineno,
            msg=self.msg,
            args=(),
            exc_info=None,
            func=self.func
        )
        record.created = self.created
        record.msecs = self.msecs
        record.levelname = self.levelname
        record.process = self.process
        record.thread = self.thread
        record.threadName = self.threadName
        return record
