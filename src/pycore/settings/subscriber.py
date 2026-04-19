import asyncio
import pickle
from typing import Optional, Callable

import zmq

from pycore.settings.server import ZMQActor
from pycore.settings.history import ChangeNotification


class SettingsSubscriber(ZMQActor):
    """
    Async subscriber for settings changes.
    Implements Suicidal Snail pattern for sequence checking.
    """

    def __init__(self,
                 pub_endpoint: str = "tcp://127.0.0.1:6001",
                 callback: Optional[Callable] = None):
        super().__init__("SettingsSubscriber")

        self.pub_endpoint = pub_endpoint
        self.callback = callback
        self.sub = None
        self.last_sequence = -1

    async def start(self):
        """Initialize subscriber."""
        await super().start()

        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.setsockopt(zmq.LINGER, 0)
        self.sub.setsockopt(zmq.RCVHWM, 100)  # High water mark
        self.sub.connect(self.pub_endpoint)

        self.logger.info(f"Connected to {self.pub_endpoint}")

        # Start listening
        self._tasks.append(asyncio.create_task(self._listen_loop()))

    def subscribe(self, field_path: str = ""):
        """Subscribe to field changes."""
        topic = f"settings.{field_path}" if field_path else "settings"
        self.sub.subscribe(topic.encode())
        self.logger.info(f"Subscribed to: {topic}")

    async def _listen_loop(self):
        """Listen for notifications with sequence checking."""
        while self._running:
            try:
                msg = await self.sub.recv_multipart()
                topic, data = msg[0], msg[1]

                notification: ChangeNotification = pickle.loads(data)
                seq = notification.metadata.sequence

                # SUICIDAL SNAIL LOGIC
                if self.last_sequence >= 0 and seq > self.last_sequence + 1:
                    self.logger.critical(
                        f"SNAIL ALERT: Expected seq {self.last_sequence + 1}, "
                        f"got {seq}. Missed {seq - self.last_sequence - 1} updates!"
                    )
                    # In production, might reconnect and resync here
                    self._running = False
                    return

                self.last_sequence = seq

                # Call user callback
                if self.callback:
                    try:
                        if asyncio.iscoroutinefunction(self.callback):
                            await self.callback(notification)
                        else:
                            self.callback(notification)
                    except Exception as e:
                        self.logger.error(f"Callback error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Subscriber error: {e}")
