"""
logs.py
=======
Registro persistente para el panel de administración.

Los eventos de auditoría se guardan en data/audit.log con hora de Cuba y se
pueden consultar completos desde Telegram, divididos en mensajes si superan el límite.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import deque
from datetime import datetime
from typing import List
from zoneinfo import ZoneInfo

from config import CONFIG

_MAX_MEMORY_RECORDS = 2000
_MEMORY = deque(maxlen=_MAX_MEMORY_RECORDS)
_LOCK = threading.RLock()
_CUBA = ZoneInfo("America/Havana")
_LOG_PATH = os.path.join(CONFIG.data_dir, "audit.log")


class CubaFormatter(logging.Formatter):
    """Formatter que siempre convierte la marca a la zona horaria de Cuba."""

    def formatTime(self, record, datefmt=None):  # noqa: N802
        value = datetime.fromtimestamp(record.created, _CUBA)
        return value.strftime(datefmt or "%Y-%m-%d %H:%M:%S %Z")


class PersistentLogHandler(logging.Handler):
    """Handler thread-safe que conserva memoria reciente y escribe cada evento."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:  # pragma: no cover
            message = record.getMessage()
        with _LOCK:
            _MEMORY.append(message)
            try:
                os.makedirs(os.path.dirname(_LOG_PATH) or ".", exist_ok=True)
                with open(_LOG_PATH, "a", encoding="utf-8") as fh:
                    fh.write(message + "\n")
            except OSError:
                # El logging nunca debe tumbar el bot si el disco no está disponible.
                pass


_HANDLER = PersistentLogHandler()
_HANDLER.setFormatter(CubaFormatter(
    "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
))


def install() -> None:
    """Instala el handler solo para eventos explícitos de auditoría."""
    audit_logger = logging.getLogger("s3_bot.audit")
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False
    if _HANDLER not in audit_logger.handlers:
        audit_logger.addHandler(_HANDLER)


audit = logging.getLogger("s3_bot.audit")


def all_records() -> List[str]:
    """Lee todos los registros persistentes, conservando el orden cronológico."""
    with _LOCK:
        try:
            with open(_LOG_PATH, encoding="utf-8") as fh:
                return [line.rstrip("\n") for line in fh if line.rstrip("\n")]
        except OSError:
            return list(_MEMORY)


def split_records(records: List[str], max_chars: int = 3400) -> List[List[str]]:
    """Divide todos los registros por líneas para respetar el límite de Telegram."""
    groups: List[List[str]] = []
    current: List[str] = []
    size = 0
    for record in records or ["Todavía no hay registros."]:
        # Se deja margen para <pre>, encabezado y escape HTML.
        record_size = len(record) + 1
        if current and size + record_size > max_chars:
            groups.append(current)
            current, size = [], 0
        current.append(record)
        size += record_size
    if current:
        groups.append(current)
    return groups


def log_path() -> str:
    return _LOG_PATH
