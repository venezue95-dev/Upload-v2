"""
config.py
=========
Lectura y validación de las variables de entorno del bot.

Toda la configuración se carga UNA sola vez al importar el módulo y se
expone en el singleton ``CONFIG``.

Se mantienen los nombres antiguos de las variables (API_ID, MAX_FILE_MB,
S3_CHUNK_SIZE_MB, ...) y se añaden nuevas para la cola escalable:

    Cola            QUEUE_MAX_CONCURRENT, QUEUE_MAX_PER_USER,
                    QUEUE_MAX_TOTAL, QUEUE_MAX_WAITING_PER_USER,
                    RATE_LIMIT_SEC, TASK_TIMEOUT_MIN, FAIR_SCHEDULING,
                    TASK_AUTO_RETRIES
    S3              S3_GLOBAL_WORKERS, S3_TIMEOUT, S3_KEY_PREFIX,
                    S3_BASE_URL, S3_RETRY_BASE_DELAY, S3_RETRY_MAX_DELAY
    Telegram        TELEGRAM_WORKERS, TG_CONCURRENT_DOWNLOADS
    Descargas       STREAM_MODE, MIN_FREE_GB, ALLOW_PRIVATE_URLS
    UI              PROGRESS_INTERVAL_SEC, LOG_LEVEL
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional

# Carga del .env en local (en Docker las variables llegan por env_file).
# load_dotenv() no sobrescribe variables ya definidas en el entorno.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv es opcional
    pass

# ----------------------------------------------------------------------
# Helpers de lectura.
# ----------------------------------------------------------------------


def _split_ints(raw: str) -> List[int]:
    """Convierte '123,456,789' -> [123, 456, 789]. Vacío -> []. """
    if not raw:
        return []
    out: List[int] = []
    for chunk in raw.replace(";", ",").split(","):
        chunk = chunk.strip()
        if chunk:
            try:
                out.append(int(chunk))
            except ValueError:
                raise ValueError(
                    f"BOT_ALLOWED_USERS contiene un ID no entero: {chunk!r}"
                )
    return out


def _get_str(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _get_int(key: str, default: int = 0) -> int:
    raw = os.getenv(key, str(default)).strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except ValueError:
        raise ValueError(f"La variable {key}={raw!r} debe ser un número entero.")


def _get_float(key: str, default: float = 0.0) -> float:
    raw = os.getenv(key, str(default)).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"La variable {key}={raw!r} debe ser un número.")


def _get_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on", "si", "sí")


# ----------------------------------------------------------------------
# Bloques de configuración.
# ----------------------------------------------------------------------


@dataclass
class TelegramConfig:
    api_id: int
    api_hash: str
    bot_token: str
    session_name: str = "s3_bot_session"
    # Workers del dispatcher de Pyrogram. Los handlers son ligeros (solo
    # encolan), así que un número alto mantiene el bot responsivo.
    workers: int = 12
    # Descargas simultáneas desde Telegram (semáforo interno de Pyrogram).
    max_concurrent_transmissions: int = 4

    def validate(self) -> None:
        if not self.api_id:
            raise ValueError("ERROR: Falta API_ID (debes obtenerlo en my.telegram.org).")
        if not self.api_hash:
            raise ValueError("ERROR: Falta API_HASH.")
        if not self.bot_token:
            raise ValueError("ERROR: Falta BOT_TOKEN (habla con @BotFather).")
        if self.workers < 1:
            raise ValueError("TELEGRAM_WORKERS debe ser >= 1.")
        if self.max_concurrent_transmissions < 1:
            raise ValueError("TG_CONCURRENT_DOWNLOADS debe ser >= 1.")


@dataclass
class S3Config:
    """Configuración del cliente toDus S3."""

    base_url: Optional[str] = None
    key_prefix: str = "users"
    chunk_size_mb: int = 50
    max_parallel: int = 3
    global_workers: int = 6
    max_retries: int = 5
    retry_base_delay: float = 2.0
    retry_max_delay: float = 30.0
    timeout: int = 300
    max_file_mb: int = 800
    inline: bool = False

    def validate(self) -> None:
        if self.chunk_size_mb < 1:
            raise ValueError("S3_CHUNK_SIZE_MB debe ser >= 1 MB.")
        if self.max_parallel < 1:
            raise ValueError("S3_MAX_PARALLEL debe ser >= 1.")
        if self.global_workers < 1:
            raise ValueError("S3_GLOBAL_WORKERS debe ser >= 1.")
        if self.max_retries < 1:
            raise ValueError("S3_MAX_RETRIES debe ser >= 1.")


@dataclass
class QueueConfig:
    """Parámetros del planificador de tareas."""

    max_concurrent_tasks: int = 4      # tareas activas en TODO el bot
    max_active_per_user: int = 1       # tareas activas por usuario
    max_queue_total: int = 200         # tareas en espera (global)
    max_queue_per_user: int = 15       # tareas en espera por usuario
    rate_limit_sec: int = 3            # anti-spam entre envíos
    task_timeout_min: int = 240        # duración máxima de una tarea
    fair_scheduling: bool = True       # reparto justo entre usuarios
    auto_retries: int = 1              # reintentos automáticos de la tarea
    owner_priority: int = 5            # prioridad extra para el owner

    def validate(self) -> None:
        if self.max_concurrent_tasks < 1:
            raise ValueError("QUEUE_MAX_CONCURRENT debe ser >= 1.")
        if self.max_active_per_user < 1:
            raise ValueError("QUEUE_MAX_PER_USER debe ser >= 1.")
        if self.max_queue_total < 1:
            raise ValueError("QUEUE_MAX_TOTAL debe ser >= 1.")
        if self.task_timeout_min < 1:
            raise ValueError("TASK_TIMEOUT_MIN debe ser >= 1.")


@dataclass
class AppConfig:
    telegram: TelegramConfig
    s3: S3Config
    queue: QueueConfig

    allowed_users: List[int] = field(default_factory=list)
    owner_id: Optional[int] = None

    # Rutas
    download_dir: str = "downloads"
    data_dir: str = "data"
    work_dir: str = "."
    settings_file: str = "settings.json"
    state_file: str = "queue_state.json"
    history_db: str = "history.db"

    # Comportamiento
    progress_interval: float = 3.0     # segundos entre ediciones del progreso
    stream_mode: str = "auto"          # auto | on | off
    min_free_gb: float = 2.0           # espacio libre mínimo en disco
    allow_private_urls: bool = False   # permitir URLs a IP privadas
    log_level: str = "INFO"

    def validate(self) -> None:
        self.telegram.validate()
        self.s3.validate()
        self.queue.validate()
        if self.stream_mode not in ("auto", "on", "off"):
            raise ValueError("STREAM_MODE debe ser: auto | on | off")
        if self.progress_interval < 1:
            raise ValueError("PROGRESS_INTERVAL_SEC debe ser >= 1 segundo.")


# ----------------------------------------------------------------------
# Carga.
# ----------------------------------------------------------------------


def load_config() -> AppConfig:
    """Lee el entorno y devuelve un AppConfig validado."""
    work_dir = _get_str("WORK_DIR", ".") or "."

    tg = TelegramConfig(
        api_id=_get_int("API_ID", 0),
        api_hash=_get_str("API_HASH"),
        bot_token=_get_str("BOT_TOKEN"),
        session_name=_get_str("BOT_SESSION_NAME", "s3_bot_session"),
        workers=_get_int("TELEGRAM_WORKERS", 12),
        max_concurrent_transmissions=_get_int("TG_CONCURRENT_DOWNLOADS", 4),
    )

    s3 = S3Config(
        base_url=_get_str("S3_BASE_URL") or None,
        key_prefix=_get_str("S3_KEY_PREFIX", "users").strip("/") or "users",
        chunk_size_mb=_get_int("S3_CHUNK_SIZE_MB", 50),
        max_parallel=_get_int("S3_MAX_PARALLEL", 3),
        global_workers=_get_int("S3_GLOBAL_WORKERS", 6),
        max_retries=_get_int("S3_MAX_RETRIES", 5),
        retry_base_delay=_get_float("S3_RETRY_BASE_DELAY", 2.0),
        retry_max_delay=_get_float("S3_RETRY_MAX_DELAY", 30.0),
        timeout=_get_int("S3_TIMEOUT", 300),
        max_file_mb=_get_int("MAX_FILE_MB", 800),
        inline=_get_bool("S3_INLINE", False),
    )

    q = QueueConfig(
        max_concurrent_tasks=_get_int("QUEUE_MAX_CONCURRENT", 4),
        max_active_per_user=_get_int("QUEUE_MAX_PER_USER", 1),
        max_queue_total=_get_int("QUEUE_MAX_TOTAL", 200),
        max_queue_per_user=_get_int("QUEUE_MAX_WAITING_PER_USER", 15),
        rate_limit_sec=_get_int("RATE_LIMIT_SEC", 3),
        task_timeout_min=_get_int("TASK_TIMEOUT_MIN", 240),
        fair_scheduling=_get_bool("FAIR_SCHEDULING", True),
        auto_retries=_get_int("TASK_AUTO_RETRIES", 1),
        owner_priority=_get_int("OWNER_PRIORITY", 5),
    )

    owner_raw = _get_str("BOT_OWNER_ID")
    owner_id = int(owner_raw) if owner_raw else None

    data_dir = _get_str("DATA_DIR", os.path.join(work_dir, "data")) or "data"
    cfg = AppConfig(
        telegram=tg,
        s3=s3,
        queue=q,
        allowed_users=_split_ints(_get_str("BOT_ALLOWED_USERS")),
        owner_id=owner_id,
        download_dir=_get_str("DOWNLOAD_DIR", "downloads"),
        data_dir=data_dir,
        work_dir=work_dir,
        settings_file=_get_str("SETTINGS_FILE", os.path.join(work_dir, "settings.json")),
        state_file=_get_str("STATE_FILE", os.path.join(data_dir, "queue_state.json")),
        history_db=_get_str("HISTORY_DB", os.path.join(data_dir, "history.db")),
        progress_interval=_get_float("PROGRESS_INTERVAL_SEC", 3.0),
        stream_mode=_get_str("STREAM_MODE", "auto").lower(),
        min_free_gb=_get_float("MIN_FREE_GB", 2.0),
        allow_private_urls=_get_bool("ALLOW_PRIVATE_URLS", False),
        log_level=_get_str("LOG_LEVEL", "INFO").upper(),
    )

    cfg.validate()
    return cfg


CONFIG: AppConfig = load_config()
