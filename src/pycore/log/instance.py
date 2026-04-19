"""
TCP logging module with JSON serialization.
"""

import json
import logging
import logging.handlers
import socket
import socketserver
import ssl
import struct
import sys
import threading
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Optional, List, Callable, Final

from pycore.circuit import CircuitBreaker
from pycore.log.record import LogRecordData
from pycore.retry import retry

# Constants
DEFAULT_LOG_LEVEL: Final[int] = logging.DEBUG
DEFAULT_PORT = 4069
DEFAULT_TIMEOUT = 5.0
DEFAULT_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 5
MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB max per log message
SERVER_START_TIMEOUT = 2.0
THREAD_JOIN_TIMEOUT = 5.0


class JSONSocketHandler(logging.Handler):
    """
    TCP logging handler using JSON serialization.
    """

    def __init__(
            self,
            host: str,
            port: int,
            use_ssl: bool = True,
            ssl_cafile: Optional[str] = None,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,
            timeout: float = DEFAULT_TIMEOUT
    ):
        super().__init__()

        # Validate inputs
        self._validate_host(host)
        self._validate_port(port)

        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.timeout = timeout
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile

        self._sock: Optional[socket.socket] = None
        self._ssl_context: Optional[ssl.SSLContext] = None
        self._lock = threading.RLock()
        self._breaker = CircuitBreaker(
            failure_threshold=3,
            reset_timeout=1.0,
            half_open_max_calls=2,
            rolling_window_size=50,
            use_time_based_decay=True,
            decay_factor=0.5  # Reduce failures by half on each success
        )

        if self.use_ssl:
            self._setup_ssl_context()

    @staticmethod
    def _validate_host(host: str) -> None:
        """Validate host parameter."""
        if not host or not isinstance(host, str):
            raise ValueError("Host must be a non-empty string")

    @staticmethod
    def _validate_port(port: int) -> None:
        """Validate port parameter."""
        if not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError("Port must be an integer between 1 and 65535")

    def _setup_ssl_context(self) -> None:
        """Setup SSL context with proper certificate validation."""
        try:
            self._ssl_context = ssl.create_default_context()

            self._ssl_context.check_hostname = True
            self._ssl_context.verify_mode = ssl.CERT_REQUIRED

            if self.ssl_cafile:
                if not Path(self.ssl_cafile).exists():
                    raise FileNotFoundError(
                        f"CA file not found: {self.ssl_cafile}")
                self._ssl_context.load_verify_locations(self.ssl_cafile)

            if self.ssl_certfile and self.ssl_keyfile:
                if not Path(self.ssl_certfile).exists():
                    raise FileNotFoundError(
                        f"Cert file not found: {self.ssl_certfile}")
                if not Path(self.ssl_keyfile).exists():
                    raise FileNotFoundError(
                        f"Key file not found: {self.ssl_keyfile}")
                self._ssl_context.load_cert_chain(
                    self.ssl_certfile,
                    self.ssl_keyfile
                )
        except Exception as e:
            raise RuntimeError(f"Failed to setup SSL context: {e}") from e

    def _make_socket(self) -> socket.socket:
        """Create a socket with optional SSL wrapping."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)

        try:
            sock.connect((self.host, self.port))

            if self.use_ssl and self._ssl_context:
                sock = self._ssl_context.wrap_socket(
                    sock,
                    server_hostname=self.host
                )

            return sock
        except Exception:
            sock.close()
            raise

    def _get_socket(self) -> Optional[socket.socket]:
        """Get or create a socket connection."""
        with self._lock:
            if self._sock is None and self._breaker.can_execute():
                try:
                    self._sock = self._make_socket()
                except Exception:
                    # Connection failed, don't raise
                    return None
            return self._sock

    def _close_socket(self) -> None:
        """Close the socket connection."""
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                finally:
                    self._sock = None

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record by sending it over the network as JSON.

        Args:
            record: The log record to emit
        """
        # Check circuit breaker first - avoid unnecessary work
        if not self._breaker.can_execute():
            return

        try:
            # Convert to JSON-safe format
            log_data = LogRecordData.from_log_record(record)
            json_str = json.dumps(asdict(log_data))
            json_bytes = json_str.encode('utf-8')

            # Check size limit
            if len(json_bytes) > MAX_MESSAGE_SIZE:
                raise ValueError(
                    f"Log message exceeds max size of {MAX_MESSAGE_SIZE} bytes")

            # Send with length prefix (same protocol as SocketHandler)
            data = struct.pack('>L', len(json_bytes)) + json_bytes

            decorated = retry(max_attempts=2)(self._get_socket)
            sock = decorated()

            if sock:
                try:
                    with self._lock:
                        sock.sendall(data)
                    self._breaker.record_success()  # Outside lock
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self._close_socket()
                    self._breaker.record_failure()
                    # Don't re-raise - let handleError deal with it below
            else:
                # Socket creation failed after retries
                self._breaker.record_failure()

        except Exception as e:
            self._close_socket()
            self.handleError(record)

    def close(self) -> None:
        """Close the handler and clean up resources."""
        self._close_socket()
        super().close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


class JSONLogRecordStreamHandler(socketserver.StreamRequestHandler):
    """
    Handler for processing JSON-encoded log records.
    """

    def handle(self) -> None:
        """Handle multiple log records from a single connection."""
        while True:
            try:
                # Read the length prefix
                chunk = self.connection.recv(4)
                if len(chunk) < 4:
                    break

                msg_len = struct.unpack('>L', chunk)[0]

                # Validate message size
                if msg_len > MAX_MESSAGE_SIZE:
                    logging.getLogger(__name__).error(
                        f"Message size {msg_len} exceeds max {MAX_MESSAGE_SIZE}"
                    )
                    break

                # Read the JSON data
                data = b''
                while len(data) < msg_len:
                    chunk = self.connection.recv(min(msg_len - len(data), 8192))
                    if not chunk:
                        raise ConnectionError(
                            "Connection closed while reading data")
                    data += chunk

                # Safely deserialize JSON
                self._process_json_record(data)

            except (EOFError, ConnectionResetError, ConnectionError):
                break
            except json.JSONDecodeError as e:
                logging.getLogger(__name__).error(f"Invalid JSON received: {e}")
                break
            except Exception as e:
                logging.getLogger(__name__).error(
                    f"Error handling log record: {e}")
                break

    def _process_json_record(self, data: bytes) -> None:
        """Process a JSON-encoded log record."""
        try:
            json_data = json.loads(data.decode('utf-8'))
            log_data = LogRecordData(**json_data)
            record = log_data.to_log_record()

            # Pass to server for handling
            if hasattr(self.server, 'handle_log_record'):
                self.server.handle_log_record(record)
        except Exception as e:
            logging.getLogger(__name__).error(
                f"Failed to process log record: {e}")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    Threaded TCP server with proper resource management.
    """

    allow_reuse_address = True
    daemon_threads = True  # Daemon threads for clean shutdown

    def __init__(self, server_address, request_handler_class):
        super().__init__(server_address, request_handler_class)
        self._received_records: List[logging.LogRecord] = []
        self._records_lock = threading.RLock()

    def handle_log_record(self, record: logging.LogRecord) -> None:
        """Thread-safe log record handling."""
        with self._records_lock:
            self._received_records.append(record)

    def get_received_messages(self) -> List[str]:
        """Get all received log messages (thread-safe)."""
        with self._records_lock:
            return [record.getMessage() for record in self._received_records]

    def get_received_records(self) -> List[logging.LogRecord]:
        """Get copy of received records (thread-safe)."""
        with self._records_lock:
            return self._received_records.copy()

    def clear_records(self) -> None:
        """Clear all received records (thread-safe)."""
        with self._records_lock:
            self._received_records.clear()


class TCPLogServer:
    """
    Main TCP log server with SSL support and proper lifecycle management.
    """

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 0,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,
            ssl_cafile: Optional[str] = None
    ):
        self._validate_config(host, port, ssl_certfile, ssl_keyfile, ssl_cafile)

        self.host = host
        self.port = port
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.ssl_cafile = ssl_cafile

        self._ssl_context: Optional[ssl.SSLContext] = None
        self._server: Optional[ThreadedTCPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._started_event = threading.Event()
        self._running = False
        self._logger = logging.getLogger(f"{__name__}.TCPLogServer")

        if ssl_certfile and ssl_keyfile:
            self._setup_ssl_context()

    @staticmethod
    def _validate_config(host: str, port: int, ssl_certfile: Optional[str],
                         ssl_keyfile: Optional[str],
                         ssl_cafile: Optional[str]) -> None:
        """Validate server configuration."""
        if not host or not isinstance(host, str):
            raise ValueError("Host must be a non-empty string")

        if not isinstance(port, int) or port < 0 or port > 65535:
            raise ValueError("Port must be an integer between 0 and 65535")

        if ssl_certfile and not Path(ssl_certfile).exists():
            raise FileNotFoundError(f"SSL cert file not found: {ssl_certfile}")

        if ssl_keyfile and not Path(ssl_keyfile).exists():
            raise FileNotFoundError(f"SSL key file not found: {ssl_keyfile}")

        if ssl_cafile and not Path(ssl_cafile).exists():
            raise FileNotFoundError(f"SSL CA file not found: {ssl_cafile}")

        if bool(ssl_certfile) != bool(ssl_keyfile):
            raise ValueError(
                "Both ssl_certfile and ssl_keyfile must be provided together")

    def _setup_ssl_context(self) -> None:
        """Setup SSL context for the server."""
        try:
            self._ssl_context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH)
            self._ssl_context.load_cert_chain(
                certfile=self.ssl_certfile,
                keyfile=self.ssl_keyfile
            )

            if self.ssl_cafile:
                self._ssl_context.load_verify_locations(cafile=self.ssl_cafile)
                self._ssl_context.verify_mode = ssl.CERT_REQUIRED
                self._logger.info(
                    "SSL enabled with client certificate verification")
            else:
                self._ssl_context.verify_mode = ssl.CERT_NONE
                self._logger.warning(
                    "SSL enabled WITHOUT client certificate verification")

        except Exception as e:
            raise RuntimeError(f"Failed to setup SSL context: {e}") from e

    def start(self) -> None:
        """Start the TCP log server."""
        if self._running:
            raise RuntimeError("Server is already running")

        try:
            # Create the server
            self._server = ThreadedTCPServer(
                (self.host, self.port),
                JSONLogRecordStreamHandler
            )

            # Wrap with SSL if configured
            if self._ssl_context:
                self._server.socket = self._ssl_context.wrap_socket(
                    self._server.socket,
                    server_side=True
                )

            # Get actual port
            self.port = self._server.server_address[1]

            # Start server thread
            self._running = True
            self._started_event.clear()
            self._server_thread = threading.Thread(
                target=self._run_server,
                name="TCPLogServerThread",
                daemon=False  # Non-daemon for proper cleanup
            )
            self._server_thread.start()

            # Wait for server to start
            if not self._started_event.wait(timeout=SERVER_START_TIMEOUT):
                raise RuntimeError("Server failed to start within timeout")

            self._logger.info(
                f"TCP log server started on {self.host}:{self.port} "
                f"(SSL: {bool(self._ssl_context)})"
            )

        except Exception as e:
            self._running = False
            if self._server:
                self._server.server_close()
                self._server = None
            raise RuntimeError(f"Failed to start TCP log server: {e}") from e

    def _run_server(self) -> None:
        """Run the server main loop."""
        try:
            self._started_event.set()
            if self._server:
                self._server.serve_forever()
        except Exception as e:
            if self._running:
                self._logger.error(f"Server error: {e}")
        finally:
            self._started_event.set()  # In case we failed early

    def stop(self) -> None:
        """Stop the TCP log server with proper cleanup."""
        if not self._running:
            return

        self._running = False

        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception as e:
                self._logger.error(f"Error shutting down server: {e}")

        if self._server_thread and self._server_thread.is_alive():
            self._server_thread.join(timeout=THREAD_JOIN_TIMEOUT)
            if self._server_thread.is_alive():
                self._logger.warning(
                    "Server thread did not stop within timeout")

        self._server = None
        self._server_thread = None
        self._logger.info("TCP log server stopped")

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running and self._server is not None

    def get_received_messages(self) -> List[str]:
        """Get received log messages."""
        if self._server:
            return self._server.get_received_messages()
        return []

    def get_received_records(self) -> List[logging.LogRecord]:
        """Get received log records."""
        if self._server:
            return self._server.get_received_records()
        return []

    def clear_records(self) -> None:
        """Clear received records."""
        if self._server:
            self._server.clear_records()

    def __enter__(self):
        """Context manager entry - starts the server."""
        if not self.is_running:
            self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops the server."""
        self.stop()
        return False


class ReplacementExceptHook:
    """
    Exception handler that logs exceptions reaching the top level instead of exiting.

    This class provides a safer exception handling mechanism for GUI applications
    by logging uncaught exceptions rather than terminating the application.
    """

    def __init__(
            self,
            logger: Optional[logging.Logger] = None,
            old_excepthook: Optional[Callable] = None
    ) -> None:
        """
        Initialize the exception hook.

        Args:
            logger: Logger instance to use (defaults to root logger)
            old_excepthook: Previous exception hook to chain to
        """
        self.logger = logger if logger is not None else logging.getLogger()
        self.old_excepthook = old_excepthook

    def __call__(self, etype: type, evalue: BaseException, tb) -> None:
        """
        Handle an uncaught exception.

        Args:
            etype: Exception type
            evalue: Exception value
            tb: Traceback object
        """
        try:
            exc_info = (etype, evalue, tb)
            self.logger.error('Exception raised to toplevel', exc_info=exc_info)
        except Exception as e:
            # Last resort error handling
            print(f"Error in exception hook: {e}", file=sys.stderr)
            import traceback
            traceback.print_exception(etype, evalue, tb)

        if self.old_excepthook is not None:
            try:
                self.old_excepthook(etype, evalue, tb)
            except Exception as e:
                print(f"Error in old exception hook: {e}", file=sys.stderr)


def replace_excepthook(
        logger: Optional[logging.Logger] = None,
        passthrough: bool = True
) -> ReplacementExceptHook:
    """
    Replace the system exception hook with a logging exception hook.

    Args:
        logger: Logger instance to use (defaults to root logger)
        passthrough: Whether to chain to the original exception hook

    Returns:
        The installed ReplacementExceptHook instance
    """
    old = sys.excepthook if passthrough else None
    replacement = ReplacementExceptHook(logger=logger, old_excepthook=old)
    sys.excepthook = replacement
    return replacement


def setup_basic_logging(
        level: int = logging.INFO,
        console: bool = True,
        log_file: Optional[str] = None
) -> None:
    """
    Setup basic logging configuration.

    Args:
        level: Logging level
        console: Enable console logging
        log_file: Optional file path for file logging
    """
    logging.getLogger().handlers.clear()
    logging.getLogger().setLevel(level)

    if console:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=DEFAULT_MAX_BYTES,
            backupCount=DEFAULT_BACKUP_COUNT
        )
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)


def setup_tcp_logging(
        host: str,
        port: int,
        level: int = logging.INFO,
        use_ssl: bool = True,
        ssl_cafile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_keyfile: Optional[str] = None
) -> JSONSocketHandler:
    """
    Setup TCP logging with JSON serialization.

    Args:
        host: Server host
        port: Server port
        level: Logging level
        use_ssl: Enable SSL
        ssl_cafile: CA certificate file
        ssl_certfile: Client certificate file
        ssl_keyfile: Client key file

    Returns:
        Configured handler
    """
    handler = JSONSocketHandler(
        host=host,
        port=port,
        use_ssl=use_ssl,
        ssl_cafile=ssl_cafile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile
    )
    handler.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    return handler


@contextmanager
def tcp_log_server_context(host: str = 'localhost', port: int = 0, **kwargs):
    """
    Context manager for TCP log server that auto-starts and stops.

    Example:
        with tcp_log_server_context('localhost', 9020) as server:
            logger = logging.getLogger('test')
            handler = setup_tcp_logging('localhost', server.port, use_ssl=False)
            logger.info("Test message")
    """
    server = TCPLogServer(host, port, **kwargs)
    server.start()
    try:
        yield server
    finally:
        server.stop()
