"""
Integrated LogManager with TCP logging support for local and remote processes.

Supports:
- Local logging (same process)
- TCP server for receiving logs from remote processes
- Process-safe port management
- Singleton pattern with process awareness
"""
import json
import logging
import multiprocessing
import socket
import sys
import threading
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Any, Type

from PyQt6.QtCore import QObject, pyqtSlot, pyqtSignal
from PyQt6.QtWidgets import QApplication

from cross_platform.qt6_utils.qtgui.src.qtgui.log_widget import LogWidget
from pycore.log.instance import (
    TCPLogServer,
    JSONSocketHandler,
)
from qtcore.meta import QSingletonMeta


class PortManager:
    """
    Manages port allocation for TCP logging across processes.

    Uses a lock file mechanism to ensure only one process binds to a port.
    """

    DEFAULT_PORT = 9020
    PORT_RANGE = range(9020, 9030)  # Try 10 ports

    @staticmethod
    def get_lock_file(port: int) -> Path:
        """Get the lock file path for a given port."""
        import tempfile
        return Path(tempfile.gettempdir()) / f"tcp_log_server_{port}.lock"

    @staticmethod
    def is_port_available(port: int) -> bool:
        """Check if a port is available for binding."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
            sock.close()
            return True
        except OSError:
            return False

    @classmethod
    def acquire_port(cls, preferred_port: Optional[int] = None) -> Optional[
        int]:
        """
        Acquire an available port, creating a lock file.

        Args:
            preferred_port: Preferred port number, or None to auto-select

        Returns:
            Port number if successful, None otherwise
        """
        ports_to_try = [preferred_port] if preferred_port else cls.PORT_RANGE

        for port in ports_to_try:
            if not cls.is_port_available(port):
                continue

            lock_file = cls.get_lock_file(port)
            try:
                # Try to create lock file exclusively
                if not lock_file.exists():
                    lock_file.write_text(str(port))
                    return port
            except (IOError, OSError):
                continue

        return None

    @classmethod
    def release_port(cls, port: int) -> None:
        """Release a port by removing its lock file."""
        print("Releasing port: ", port)
        lock_file = cls.get_lock_file(port)
        try:
            if lock_file.exists():
                lock_file.unlink()
        except (IOError, OSError):
            raise

    @classmethod
    def is_server_running(cls, port: int) -> bool:
        """
        Check if a TCP log server is running on a specific port.

        Args:
            port: Port number to check

        Returns:
            True if server is running and responding, False otherwise
        """
        # Check if port is in use
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(0.5)
            sock.connect(('localhost', port))
            sock.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    @classmethod
    def find_active_server_port(cls) -> Optional[int]:
        """Find the port of an active TCP log server."""
        for port in cls.PORT_RANGE:
            lock_file = cls.get_lock_file(port)
            if lock_file.exists():
                # Verify the server is actually running
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.settimeout(0.5)
                    sock.connect(('localhost', port))
                    sock.close()
                    return port
                except (socket.timeout, ConnectionRefusedError, OSError):
                    # Lock file exists but server isn't running - clean up
                    cls.release_port(port)
        return None

    def clean_stale_locks(self, output_json: bool = False):
        """Remove stale lock files."""
        from pathlib import Path
        import tempfile

        lock_dir = Path(tempfile.gettempdir())
        lock_files = list(lock_dir.glob("tcp_log_server_*.lock"))

        removed = []
        failed = []

        for lock_file in lock_files:
            try:
                port = int(lock_file.read_text().strip())
                if not PortManager.is_server_running(port):
                    lock_file.unlink()
                    removed.append({'file': str(lock_file), 'port': port})
            except Exception as e:
                failed.append({'file': str(lock_file), 'error': str(e)})

        if output_json:
            result = {
                'removed': removed,
                'failed': failed,
                'removed_count': len(removed),
                'failed_count': len(failed)
            }
            print(json.dumps(result, indent=2))
        else:
            if removed:
                print(f"✓ Removed {len(removed)} stale lock file(s):")
                for info in removed:
                    print(f"  • Port {info['port']}: {info['file']}")
            else:
                print("No stale lock files to remove")

            if failed:
                print(f"\n✗ Failed to remove {len(failed)} file(s):")
                for info in failed:
                    print(f"  • {info['file']}: {info['error']}")


class TCPLogBridge(QObject):
    """
    Bridge between TCP log server and Qt log widget.

    Receives log records from TCP server and emits them as Qt signals
    for thread-safe GUI updates.
    """

    log_received = pyqtSignal(object)  # Emits LogRecord

    def __init__(self, parent=None):
        super().__init__(parent)
        self._server: Optional[TCPLogServer] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._running = False

    def start_server(self, port: int = 0) -> int:
        """
        Start the TCP log server.

        Args:
            port: Port to listen on (0 for auto-assign)

        Returns:
            Actual port number being used
        """
        if self._server and self._server.is_running:
            return self._server.port

        self._server = TCPLogServer('localhost', port)
        self._server.start()

        # Start polling thread to check for new messages
        self._running = True
        self._poll_thread = threading.Thread(
            target=self._poll_messages,
            daemon=True,
            name="TCPLogBridge-Poller"
        )
        self._poll_thread.start()

        logging.info(f"TCP log server started on port {self._server.port}")
        return self._server.port

    def stop_server(self) -> None:
        """Stop the TCP log server."""
        self._running = False

        if self._poll_thread:
            self._poll_thread.join(timeout=2.0)

        if self._server:
            self._server.stop()
            self._server = None

        logging.info("TCP log server stopped")

    def _poll_messages(self) -> None:
        """Poll for new log messages and emit signals."""
        last_count = 0

        while self._running and self._server:
            try:
                records = self._server.get_received_records()
                current_count = len(records)

                # Emit new records
                if current_count > last_count:
                    for record in records[last_count:]:
                        self.log_received.emit(record)
                    last_count = current_count

                # Sleep briefly to avoid busy-waiting
                threading.Event().wait(0.1)

            except Exception as e:
                logging.error(f"Error polling TCP log messages: {e}")

    @property
    def port(self) -> Optional[int]:
        """Get the server port."""
        return self._server.port if self._server else None

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._server is not None and self._server.is_running


# ============================================================================
# Enhanced LogManager with TCP Support
# ============================================================================

class LogManager(QObject, metaclass=QSingletonMeta):
    """
    Thread-safe manager for application-wide logging with TCP support.

    Supports two modes:
    1. Local mode: Logs from the same process
    2. Server mode: Receives logs from remote processes via TCP

    In server mode, it can run as a standalone log viewer process.
    """

    # Class-level singleton (per-process)
    _instance = None
    _instance_lock = threading.RLock()
    _initialized = False

    def __new__(cls):
        """Implement singleton pattern per process."""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self) -> None:
        """Initialize the log manager with default state."""
        # Only initialize once
        if LogManager._initialized:
            return

        super().__init__()

        # Initialize instance variables
        self._log_widget: Optional['LogWidget'] = None
        self._original_excepthook: Any = None
        self._handler: Optional[logging.Handler] = None
        self._app: Optional[QApplication] = None
        self._initialization_lock = threading.RLock()

        # TCP support
        self._tcp_bridge: Optional[TCPLogBridge] = None
        self._tcp_port: Optional[int] = None
        self._is_server_mode = False

        self._initialized = False

    def initialize(
            self,
            app: QApplication,
            log_widget: 'LogWidget',
            level: int = logging.DEBUG,
            auto_show: bool = True,
            enable_tcp_server: bool = False,
            tcp_port: Optional[int] = None
    ) -> 'LogWidget':
        """
        Initialize the logging system.

        Args:
            app: QApplication instance
            log_widget: LogWidget instance to display logs
            level: Logging level (default: DEBUG)
            auto_show: Whether to show log widget immediately
            enable_tcp_server: Enable TCP server for remote logging
            tcp_port: TCP port (None for auto-select)

        Returns:
            The configured log widget

        Raises:
            RuntimeError: If initialization fails
        """
        with self._initialization_lock:
            if self._log_widget is not None:
                logging.debug("LogManager already initialized")
                return self._log_widget

            try:
                self._app = app
                self._log_widget = log_widget
                self._original_excepthook = sys.excepthook

                # Configure log widget
                log_widget.setWindowTitle("Application Log Viewer")
                log_widget.resize(1400, 700)

                # Setup local logging handler
                self._handler = log_widget.get_handler()
                self._handler.setLevel(level)

                # Configure root logger
                root_logger = logging.getLogger()
                root_logger.addHandler(self._handler)
                root_logger.setLevel(level)

                # Setup TCP server if requested
                if enable_tcp_server:
                    self._setup_tcp_server(tcp_port)

                # Install exception hook
                sys.excepthook = self._exception_hook

                # Connect cleanup to application quit
                app.aboutToQuit.connect(self.cleanup)

                # Show widget if requested
                if auto_show:
                    log_widget.show()

                logging.info("Logging system initialized successfully")
                if self._tcp_port:
                    logging.info(
                        f"TCP log server listening on port {self._tcp_port}")

                self._initialized = True
                return log_widget

            except Exception as e:
                # Reset state on failure
                self._cleanup_state()
                raise RuntimeError(
                    f"Failed to initialize LogManager: {e}") from e

    def _setup_tcp_server(self, port: Optional[int] = None) -> None:
        """Setup TCP server for receiving remote logs."""
        try:
            # Acquire a port
            acquired_port = PortManager.acquire_port(port)
            if acquired_port is None:
                raise RuntimeError("Failed to acquire TCP port")

            # Create and start TCP bridge
            self._tcp_bridge = TCPLogBridge()
            self._tcp_bridge.log_received.connect(self._handle_remote_log)

            actual_port = self._tcp_bridge.start_server(acquired_port)
            self._tcp_port = actual_port
            self._is_server_mode = True

            # Update lock file with actual port
            if actual_port != acquired_port:
                PortManager.release_port(acquired_port)
                PortManager.acquire_port(actual_port)

        except Exception as e:
            logging.error(f"Failed to setup TCP server: {e}")
            raise

    @pyqtSlot(object)
    def _handle_remote_log(self, record: logging.LogRecord) -> None:
        """Handle a log record received from TCP server."""
        if self._handler:
            self._handler.emit(record)

    def _exception_hook(
            self,
            exc_type: Type[BaseException],
            exc_value: BaseException,
            exc_traceback: Any
    ) -> None:
        """Custom exception hook for uncaught exceptions."""
        if issubclass(exc_type, KeyboardInterrupt):
            if self._original_excepthook:
                self._original_excepthook(exc_type, exc_value, exc_traceback)
            return

        try:
            tb_text = ''.join(
                traceback.format_exception(exc_type, exc_value, exc_traceback))

            logging.critical(
                f"Uncaught exception: {exc_type.__name__}: {exc_value}\n{tb_text}",
                exc_info=(exc_type, exc_value, exc_traceback)
            )

            self._ensure_log_widget_visible()

        except Exception as log_error:
            if self._original_excepthook:
                self._original_excepthook(exc_type, exc_value, exc_traceback)
            else:
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
            logging.error(f"Failed to log exception: {log_error}")

    def _ensure_log_widget_visible(self) -> None:
        """Ensure log widget is visible and raised to front."""
        if self._log_widget and not self._log_widget.isVisible():
            self._log_widget.show()
            self._log_widget.raise_()
            self._log_widget.activateWindow()

    def _cleanup_state(self) -> None:
        """Clean up internal state."""
        self._log_widget = None
        self._handler = None
        self._app = None
        if self._tcp_port:
            PortManager.release_port(self._tcp_port)
            PortManager.clean_stale_locks()
        self._tcp_port = None

    @pyqtSlot()
    def cleanup(self) -> None:
        """Cleanup logging resources."""
        logging.info("Shutting down logging system")

        try:
            # Stop TCP server
            if self._tcp_bridge:
                self._tcp_bridge.stop_server()
                self._tcp_bridge = None

            # Release port
            if self._tcp_port:
                PortManager.release_port(self._tcp_port)
                self._tcp_port = None

            # Remove handler from root logger
            if self._handler:
                root_logger = logging.getLogger()
                root_logger.removeHandler(self._handler)
                self._handler.close()
                self._handler = None

            # Restore original exception hook
            if self._original_excepthook:
                sys.excepthook = self._original_excepthook
                self._original_excepthook = None

            # Close log widget
            if self._log_widget:
                # if self._log_widget.isVisible():
                # self._log_widget.close()
                self._log_widget = None

            self._cleanup_state()
            logging.info("Logging system shutdown complete")

        except Exception as e:
            logging.error(f"Error during LogManager cleanup: {e}")

    @property
    def is_initialized(self) -> bool:
        """Check if logging system is initialized."""
        return self._log_widget is not None

    @property
    def log_widget(self) -> Optional['LogWidget']:
        """Get the current log widget instance."""
        return self._log_widget

    @property
    def tcp_port(self) -> Optional[int]:
        """Get the TCP server port (None if not enabled)."""
        return self._tcp_port

    @property
    def is_server_mode(self) -> bool:
        """Check if running in TCP server mode."""
        return self._is_server_mode

    def show_log_widget(self) -> None:
        """Show the log widget window."""
        self._ensure_log_widget_visible()

    def hide_log_widget(self) -> None:
        """Hide the log widget window."""
        if self._log_widget:
            self._log_widget.hide()

    def __del__(self) -> None:
        """Destructor to ensure cleanup."""
        if hasattr(self, '_handler') and self._handler:
            self.cleanup()


class RemoteLogServerProcess:
    """
    Manages a log server running in a separate process.

    This allows you to spawn a log viewer in another process and
    connect to it from your main application.
    """

    def __init__(self, port: Optional[int] = None, max_lines: int = 10000,
                 font_size: int = 10):
        """
        Initialize remote log server configuration.

        Args:
            port: TCP port (None for auto-select)
            max_lines: Maximum lines in log widget
            font_size: Font size for log display
        """
        self.port = port
        self.max_lines = max_lines
        self.font_size = font_size
        self._process: Optional[multiprocessing.Process] = None
        self._actual_port: Optional[int] = None
        self._port_queue = multiprocessing.Queue()

    def _server_worker(self):
        """Worker function that runs the log server."""
        try:
            from PyQt6.QtWidgets import QApplication
            import sys

            # Create Qt application
            app = QApplication(sys.argv)
            log_widget = LogWidget(max_lines=self.max_lines,
                                   font_size=self.font_size)

            # Setup TCP server
            widget, actual_port = setup_tcp_server_logging(
                app, log_widget, port=self.port, auto_show=True
            )

            log_widget.setWindowTitle(f"Remote Log Server")

            # Send actual port back to parent process
            self._port_queue.put(actual_port)

            # Run event loop
            sys.exit(app.exec())

        except Exception as e:
            self._port_queue.put(None)
            print(f"Error in server worker: {e}")
            import traceback
            traceback.print_exc()

    def start(self) -> int:
        """
        Start the log server in a separate process.

        Returns:
            The actual port number the server is listening on

        Raises:
            RuntimeError: If server fails to start
        """
        if self._process and self._process.is_alive():
            raise RuntimeError("Server process already running")

        # Start server process
        self._process = multiprocessing.Process(
            target=self._server_worker,
            name="RemoteLogServer",
            daemon=False
        )
        self._process.start()

        # Wait for server to start and report its port
        try:
            self._actual_port = self._port_queue.get(timeout=10.0)
            if self._actual_port is None:
                raise RuntimeError("Server failed to start")
        except Exception as e:
            self.stop()
            raise RuntimeError(f"Failed to start log server: {e}") from e

        # Verify server is actually running
        import time
        max_retries = 10
        for i in range(max_retries):
            if PortManager.is_server_running(self._actual_port):
                return self._actual_port
            time.sleep(0.5)

        self.stop()
        raise RuntimeError("Server started but not responding")

    def stop(self):
        """Stop the log server process."""
        if self._process:
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=5.0)
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join(timeout=2.0)
            self._process = None

        # Clean up port
        if self._actual_port:
            PortManager.release_port(self._actual_port)
            self._actual_port = None

    @property
    def is_running(self) -> bool:
        """Check if server process is running."""
        return (self._process is not None and
                self._process.is_alive() and
                self._actual_port is not None and
                PortManager.is_server_running(self._actual_port))

    @property
    def actual_port(self) -> Optional[int]:
        """Get the actual port the server is running on."""
        return self._actual_port

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False


def setup_local_logging(
        app: QApplication,
        log_widget: 'LogWidget',
        level: int = logging.DEBUG,
        auto_show: bool = True
) -> 'LogWidget':
    """
    Setup local logging (same process).

    Args:
        app: QApplication instance
        log_widget: LogWidget instance
        level: Logging level
        auto_show: Whether to show log widget immediately

    Returns:
        The configured log widget
    """
    return LogManager().initialize(
        app, log_widget, level, auto_show,
        enable_tcp_server=False
    )


def setup_tcp_server_logging(
        app: QApplication,
        log_widget: 'LogWidget',
        port: Optional[int] = None,
        level: int = logging.DEBUG,
        auto_show: bool = True
) -> tuple['LogWidget', int]:
    """
    Setup TCP server logging (receives logs from remote processes).

    Args:
        app: QApplication instance
        log_widget: LogWidget instance
        port: TCP port (None for auto-select)
        level: Logging level
        auto_show: Whether to show log widget immediately

    Returns:
        Tuple of (log_widget, port_number)
    """
    widget = LogManager().initialize(
        app, log_widget, level, auto_show,
        enable_tcp_server=True,
        tcp_port=port
    )
    return widget, LogManager().tcp_port


def setup_remote_client_logging(
        server_host: str = 'localhost',
        server_port: Optional[int] = None,
        level: int = logging.DEBUG,
        use_ssl: bool = False
) -> bool:
    """
    Setup logging to send to a remote TCP log server.

    This is called by client processes to send their logs to the server.

    Args:
        server_host: Server hostname
        server_port: Server port (None to auto-detect)
        level: Logging level
        use_ssl: Enable SSL

    Returns:
        True if successful, False otherwise
    """
    try:
        # Auto-detect server port if not specified
        if server_port is None:
            server_port = PortManager.find_active_server_port()
            if server_port is None:
                logging.error("No active TCP log server found")
                return False

        # Setup TCP handler
        handler = JSONSocketHandler(
            server_host,
            server_port,
            use_ssl=use_ssl
        )
        handler.setLevel(level)

        # Add to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(level)

        logging.info(
            f"Remote logging configured to {server_host}:{server_port}")
        return True

    except Exception as e:
        logging.error(f"Failed to setup remote logging: {e}")
        return False


@contextmanager
def remote_log_manager(
        port: Optional[int] = None,
        max_lines: int = 10000,
        font_size: int = 10,
        auto_connect: bool = True
):
    """
    Context manager that starts a log server in a separate process.

    This spawns a PyQt6 log viewer in another process and optionally
    connects the current process to it.

    Args:
        port: TCP port (None for auto-select)
        max_lines: Maximum lines in log widget
        font_size: Font size for log display
        auto_connect: Automatically connect current process to server

    Yields:
        RemoteLogServerProcess instance with .actual_port property

    Example:
        ```python
        with remote_log_manager() as log_server:
            print(f"Log server running on port {log_server.actual_port}")

            # Your logging automatically goes to the remote viewer
            logging.info("Hello from main process!")

            # Do your work here
            my_application()
        # Server automatically stops when context exits
        ```

    Example without auto-connect:
        ```python
        with remote_log_manager(auto_connect=False) as log_server:
            # Manually connect when ready
            setup_remote_client_logging(server_port=log_server.actual_port)
            logging.info("Now connected!")
        ```
    """
    server = RemoteLogServerProcess(port, max_lines, font_size)

    try:
        # Start server
        actual_port = server.start()

        # Auto-connect if requested
        if auto_connect:
            success = setup_remote_client_logging(server_port=actual_port)
            if not success:
                logging.warning(
                    f"Failed to auto-connect to log server on port {actual_port}")

        yield server

    finally:
        server.stop()


def run_standalone_log_server(port: Optional[int] = None):
    """
    Run a standalone TCP log server process.

    This can be run in a separate process to collect logs from multiple clients.

    Args:
        port: TCP port (None for auto-select)
    """
    from PyQt6.QtWidgets import QApplication
    import sys

    app = QApplication(sys.argv)
    log_widget = LogWidget(max_lines=10000, font_size=10)

    try:
        widget, actual_port = setup_tcp_server_logging(
            app, log_widget, port=port, auto_show=True
        )

        log_widget.setWindowTitle(f"TCP Log Server - Port {actual_port}")

        print(f"TCP Log Server running on port {actual_port}")
        print(
            f"Clients can connect using: setup_remote_client_logging(server_port={actual_port})")
        print("Press Ctrl+C to stop...")

        sys.exit(app.exec())

    except KeyboardInterrupt:
        print("\nShutting down...")
        LogManager().cleanup()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TCP Log Server")
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=None,
        help='TCP port to listen on (auto-select if not specified)'
    )

    args = parser.parse_args()
    run_standalone_log_server(port=args.port)
