from __future__ import annotations

import logging
from threading import RLock

from signaldeck_sdk import Message, MessageListener


class InMemoryMessageBus:
    def __init__(self, logger: logging.Logger):
        self._logger = logger
        self._lock = RLock()
        self._listeners: list[MessageListener] = []

    def subscribe(self, listener: MessageListener):
        with self._lock:
            self._listeners.append(listener)

        def unsubscribe() -> None:
            with self._lock:
                try:
                    self._listeners.remove(listener)
                except ValueError:
                    pass

        return unsubscribe

    def publish(self, message: Message) -> None:
        # Snapshot erstellen, damit während der Listener-Aufrufe
        # kein Lock gehalten werden muss.
        with self._lock:
            listeners = tuple(self._listeners)
        self._logger.info(
            "Publishing message from '%s' to %d listeners.", message.source, len(listeners))
        for listener in listeners:
            try:
                listener(message)
            except Exception:
                self._logger.exception(
                    "Message listener failed for message from '%s'",
                    message.source,
                )