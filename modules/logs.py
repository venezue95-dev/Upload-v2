"""
logs.py
=======
Buffer de logs recientes para el panel de administración.

No escribe credenciales ni configura un archivo adicional: conserva únicamente
los últimos registros del proceso mientras el bot está encendido.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import List

_MAX_RECORDS = 2000
_records = deque(maxlen=_MAX_RECORDS)
_lock = threading.Lock()


class RecentLogHandler(logging.Handler):
    """Handler thread-safe que conserva los mensajes más recientes."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:  # pragma: no cover
            message = record.getMessage()
        with _lock:
            _records.append(message)


_HANDLER = RecentLogHandler()
_HANDLER.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
))


def install() -> None:
    """Instala el buffer una sola vez en el logger raíz."""
    root = logging.getLogger()
    if _HANDLER not in root.handlers:
        root.addHandler(_HANDLER)


def recent(limit: int = 40) -> List[str]:
    """Devuelve los últimos registros, del más antiguo al más reciente."""
    with _lock:
        return list(_records)[-max(1, min(int(limit), _MAX_RECORDS)):]
