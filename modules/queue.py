"""
queue.py
========
Cola de tareas escalable para muchos usuarios (20-40+ simultáneos).

Qué hace
--------
- ``UploadTask``: estado completo de una subida (progreso, partes, métricas,
  posición en la cola, errores…).
- ``QueueManager``: planificador con pool global de trabajadores, cupos por
  usuario, control de admisión, prioridades con anti-inanición, cancelación,
  ETA por tarea y persistencia del estado para sobrevivir reinicios.

Cómo reparte el trabajo
-----------------------
1. ``submit()`` valida los cupos (global y por usuario) y encola la tarea.
2. Un único planificador asíncrono decide cuál arranca, respetando:
   ``max_concurrent_tasks`` (global) y ``max_active_per_user`` (por usuario).
3. Con ``fair_scheduling`` se prefiere a usuarios que no tienen ninguna tarea
   activa: así nadie queda bloqueado detrás de quien sube 20 archivos.
4. Cada tarea activa corre en su propio ``asyncio.Task`` y el trabajo pesado
   (descarga/subida) se delega a hilos/IO asíncrono, de modo que el bot
   sigue respondiendo a comandos y botones mientras sube.
5. Al terminar, el resultado se guarda en SQLite (historial y estadísticas).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
import uuid
from collections import deque
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from .progress import RateMeter, SpeedMeter
from .runtime_settings import RUNTIME_SETTINGS
from .speedstats import SPEEDS

log = logging.getLogger("s3_bot.queue")


# ----------------------------------------------------------------------
# Estados.
# ----------------------------------------------------------------------
class TaskStatus(str, Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_final(self) -> bool:
        return self in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)

    @property
    def is_active(self) -> bool:
        return self in (TaskStatus.DOWNLOADING, TaskStatus.UPLOADING)


STATUS_ICON: Dict[TaskStatus, str] = {
    TaskStatus.QUEUED: "⏳",
    TaskStatus.DOWNLOADING: "📥",
    TaskStatus.UPLOADING: "📤",
    TaskStatus.COMPLETED: "✅",
    TaskStatus.FAILED: "❌",
    TaskStatus.CANCELLED: "🛑",
}

STATUS_LABEL: Dict[TaskStatus, str] = {
    TaskStatus.QUEUED: "en cola",
    TaskStatus.DOWNLOADING: "descargando",
    TaskStatus.UPLOADING: "subiendo",
    TaskStatus.COMPLETED: "completada",
    TaskStatus.FAILED: "fallida",
    TaskStatus.CANCELLED: "cancelada",
}


# ----------------------------------------------------------------------
# Errores.
# ----------------------------------------------------------------------
class TaskCancelled(Exception):
    """El usuario (o el apagado del bot) canceló la tarea."""


# ----------------------------------------------------------------------
# Tarea.
# ----------------------------------------------------------------------
class UploadTask:
    """Estado de una subida. Todos los contadores son seguros entre hilos."""

    AVG_SIZE_FALLBACK = 20 * 1024 * 1024  # 20 MB cuando no se conoce el tamaño

    def __init__(
        self,
        *,
        user_id: int,
        chat_id: int,
        message_id: int = 0,
        filename: str = "archivo.bin",
        file_size: int = 0,
        source: str = "telegram",
        url: Optional[str] = None,
        username: str = "",
        priority: int = 0,
        max_attempts: int = 1,
        task_id: Optional[str] = None,
        resumed: bool = False,
    ) -> None:
        self.task_id = task_id or uuid.uuid4().hex[:10]
        self.user_id = user_id
        self.username = username or ""
        self.chat_id = chat_id
        self.message_id = message_id
        self.status_message_id: Optional[int] = None

        self.filename = filename
        self.file_size = int(file_size or 0)
        self.source = source                  # 'telegram' | 'url'
        self.url = url
        self.media_format = "mp4"
        self.media_quality = "720"
        self.requested_chunk_mb = 0
        self.priority = int(priority)
        self.resumed = resumed

        self.status: TaskStatus = TaskStatus.QUEUED
        self.phase: str = ""                  # texto que se muestra como título
        self.mode: str = "classic"            # 'classic' | 'stream'

        # Tiempos
        self.created_at = time.time()
        self.queued_at = self.created_at
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None

        # Progreso
        self.bytes_transferred = 0            # subido de verdad a S3
        self.parts_done = 0
        self.parts_total = 0
        self.chunk_size = 0
        self.parallel_parts = max(1, int(RUNTIME_SETTINGS.get("max_parallel") or 1))
        self._inflight: Dict[int, float] = {}   # parte -> t0 (monotonic)
        self._part_fetched: Dict[int, int] = {}  # parte -> bytes descargados
        self._part_uploaded: Dict[int, int] = {} # parte -> bytes enviados
        self._fetched_total = 0                  # descarga completa (modo clásico)
        self._processed_peak = 0                 # progreso monótono
        self._lock = threading.Lock()
        self._meter = SpeedMeter()
        self._download_meter = SpeedMeter()
        # La subida se mide por partes (el PUT no informa bytes intermedios):
        # si no hay medición propia se usa la velocidad global aprendida.
        self._upload_meter = RateMeter(prior_fn=SPEEDS.upload_estimate)
        # Si el backend informa bytes enviados en vivo, la velocidad de subida
        # se mide sobre el total real (suma de todas las partes en vuelo).
        self.upload_reports_bytes = False
        self._live_meter = SpeedMeter()
        self.speed = 0.0
        self._mono_start = time.monotonic()

        # Reanudación de una tarea fallida (no volver a subir lo ya subido)
        self.resume_folder: Optional[str] = None
        self.resumed_parts: List[int] = []       # partes reutilizadas

        # Cola
        self.queue_position: int = 0
        self.queue_length: int = 0
        self.queue_eta: Optional[float] = None

        # Resultado
        self.s3_base: Optional[str] = None
        self.keys: List[str] = []
        self.keys_by_part: Dict[int, str] = {}
        self.urls: List[str] = []
        self.error: Optional[str] = None
        self.failed_part: Optional[str] = None
        self.attempts = 1
        self.max_attempts = max(1, int(max_attempts))
        self.deleted_from_s3 = False
        self.retried_as: Optional[str] = None   # id de la copia creada al reintentar
        # True si la tarea quedó a medias por un apagado: se reanuda al arrancar.
        self.interrupted = False

        # Cancelación (thread-safe: los hilos de subida la consultan)
        self.cancel_flag = threading.Event()
        self._cancel_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Propiedades.
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Descarga: varios orígenes cuentan por separado
    # ------------------------------------------------------------------
    @property
    def bytes_fetched(self) -> int:
        """Bytes descargados del origen (suma de partes + descarga completa)."""
        with self._lock:
            return self._fetched_total + sum(self._part_fetched.values())

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_flag.is_set()

    @property
    def is_final(self) -> bool:
        return self.status.is_final

    @property
    def is_active(self) -> bool:
        return self.status.is_active

    @property
    def is_retryable(self) -> bool:
        """Solo las tareas fallidas o canceladas se pueden reintentar."""
        return self.status in (TaskStatus.FAILED, TaskStatus.CANCELLED)

    @property
    def has_links(self) -> bool:
        return bool(self.urls) and not self.deleted_from_s3

    def short_name(self, limit: int = 40) -> str:
        return self.filename if len(self.filename) <= limit else self.filename[: limit - 1] + "…"

    @property
    def upload_speed(self) -> float:
        """
        Velocidad de subida (bytes/s del total de partes en paralelo).

        Con bytes en vivo se usa la medición real; si no, la medida por partes
        terminadas o, en su defecto, la estimación global aprendida.
        """
        if self.upload_reports_bytes and self._live_meter.speed > 0:
            return self._live_meter.speed
        return self._upload_meter.value()

    @property
    def upload_speed_measured(self) -> bool:
        """True si la velocidad de subida ya tiene mediciones reales."""
        if self.upload_reports_bytes and self._live_meter.speed > 0:
            return True
        return self._upload_meter.measured

    def uploaded_bytes_exact(self) -> int:
        """Bytes realmente enviados: partes terminadas + bytes de partes en vuelo."""
        with self._lock:
            return int(self.bytes_transferred + sum(self._part_uploaded.values()))

    @property
    def download_speed(self) -> float:
        return self._download_meter.speed

    def expected_part_length(self, part_number: int) -> int:
        """Longitud esperada (bytes) de la parte 1-based."""
        if not self.chunk_size or not self.file_size:
            return 0
        offset = (part_number - 1) * self.chunk_size
        return max(0, min(self.chunk_size, self.file_size - offset))

    # ------------------------------------------------------------------
    # Mutadores (thread-safe).
    # ------------------------------------------------------------------
    def plan_parts(self, chunk_size: int, num_parts: int) -> None:
        with self._lock:
            self.chunk_size = chunk_size
            self.parts_total = num_parts

    def part_started(self, part_number: int) -> None:
        with self._lock:
            self._inflight[part_number] = time.monotonic()
            self._part_uploaded[part_number] = 0

    def note_uploaded(self, part_number: int, sent: int) -> None:
        """
        Bytes enviados de una parte en el intento actual.

        Se limita a la longitud esperada de la parte (nunca cuenta de más) y
        solo se admite en partes que siguen en vuelo: un callback tardío de
        una parte ya terminada no debe volver a sumar.
        """
        with self._lock:
            if part_number not in self._inflight:
                return
            value = max(0, int(sent or 0))
            length = self.expected_part_length(part_number)
            if length > 0:
                value = min(value, length)
            self._part_uploaded[part_number] = value

    def reset_part_upload(self, part_number: int) -> None:
        """Un reintento de la parte empieza otra vez desde 0 bytes enviados."""
        with self._lock:
            if part_number in self._inflight:
                self._part_uploaded[part_number] = 0

    def part_finished(self, part_number: int, length: int, seconds: float = 0.0) -> None:
        """Marca una parte como subida y alimenta el velocímetro de subida."""
        with self._lock:
            started = self._inflight.pop(part_number, None)
            self.parts_done += 1
            self.bytes_transferred += max(0, length)
            self._part_uploaded.pop(part_number, None)
        if length > 0:
            elapsed = seconds or ((time.monotonic() - started) if started else 0.0)
            rate = self._upload_meter.note(length, elapsed)
            if rate > 0:
                # La velocidad global se aprende con cada parte terminada.
                SPEEDS.note_upload(length, elapsed)

    def part_failed(self, part_number: int) -> None:
        with self._lock:
            self._inflight.pop(part_number, None)
            self._part_uploaded.pop(part_number, None)

    # --- descarga por partes (modo streaming) ---
    def note_fetched(self, part_number: int, written: int) -> None:
        """Bytes escritos de una parte concreta (monótono: nunca retrocede)."""
        with self._lock:
            previous = self._part_fetched.get(part_number, 0)
            if written > previous:
                self._part_fetched[part_number] = written

    def part_fetched_done(self, part_number: int, length: int) -> None:
        with self._lock:
            self._part_fetched[part_number] = max(self._part_fetched.get(part_number, 0), length)

    # --- descarga completa (modo clásico) ---
    def add_fetched(self, n: int) -> None:
        with self._lock:
            self._fetched_total += max(0, n)

    def set_fetched(self, n: int) -> None:
        with self._lock:
            self._fetched_total = max(0, n)

    def cancel(self, reason: str = "cancelada por el usuario") -> None:
        self._cancel_reason = reason
        self.cancel_flag.set()

    @property
    def cancel_reason(self) -> Optional[str]:
        return self._cancel_reason

    def mark_started(self) -> None:
        self.started_at = time.time()
        self._mono_start = time.monotonic()
        self.queue_position = 0
        self.queue_length = 0
        self._meter.reset()
        self._download_meter.reset()
        self._upload_meter.reset()
        self._live_meter.reset()
        self._processed_peak = 0
        self.speed = 0.0

    def reset_for_retry(self) -> None:
        """Pone a cero el progreso antes de repetir el intento (reintento automático)."""
        with self._lock:
            self._inflight.clear()
            self._part_fetched.clear()
            self._part_uploaded.clear()
            self._fetched_total = 0
            self.bytes_transferred = 0
            self.parts_done = 0
            self.keys_by_part.clear()
        self.failed_part = None
        self._processed_peak = 0
        self._meter.reset()
        self._download_meter.reset()
        self._upload_meter.reset()
        self._live_meter.reset()
        self.speed = 0.0

    def begin_upload_phase(self) -> None:
        """Separa el progreso visual de descarga del progreso de subida."""
        with self._lock:
            self._processed_peak = 0
        self._live_meter.reset()
        self.speed = 0.0

    def mark_finished(self, status: TaskStatus, error: Optional[str] = None) -> None:
        self.status = status
        self.error = error
        self.finished_at = time.time()
        with self._lock:
            self._inflight.clear()

    # ------------------------------------------------------------------
    # Métricas.
    # ------------------------------------------------------------------
    def sample_speed(self) -> float:
        """
        Refresca la velocidad de descarga (la de subida se alimenta por partes).

        Lo llama el notificador cada pocos segundos para cada tarea activa.
        """
        if self.status.is_final:
            return self.speed
        mono = time.monotonic()
        self._download_meter.update(self.bytes_fetched, mono)
        if self.upload_reports_bytes:
            self._live_meter.update(self.uploaded_bytes_exact(), mono)
        self.speed = self.current_speed()
        return self.speed

    def current_speed(self) -> float:
        """Velocidad relevante según la fase en la que esté la tarea."""
        if self.mode == "stream":
            # En streaming avanzan las dos cosas: la que manda es la más lenta.
            return min(
                v for v in (self.upload_speed or 1e18, self.download_speed or 1e18)
            ) if (self.upload_speed or self.download_speed) else 0.0
        if self.status == TaskStatus.DOWNLOADING:
            return self.download_speed or self.upload_speed
        return self.upload_speed or self.download_speed

    def _inflight_credit(self) -> float:
        """
        Avance estimado de las partes que están subiéndose ahora mismo.

        El PUT de una parte no informa de bytes intermedios, así que se estima
        con la velocidad conocida (propia o global aprendida). Se limita al 90 %
        de cada parte: el 10 % restante solo se da cuando la parte confirma.
        """
        if not self.parts_total:
            return 0.0
        if self.upload_reports_bytes:
            return 0.0      # hay bytes reales: no se inventa nada
        speed = self.upload_speed
        if speed <= 0:
            return 0.0
        now = time.monotonic()
        with self._lock:
            inflight = dict(self._inflight)
            uploaded_by_part = dict(self._part_uploaded)
        credit = 0.0
        for part_number, t0 in inflight.items():
            if uploaded_by_part.get(part_number, 0) > 0:
                continue
            length = self.expected_part_length(part_number)
            if length <= 0:
                continue
            credit += min(0.9 * length, max(0.0, (now - t0) * speed))
        return credit

    def processed_bytes(self) -> int:
        """Bytes 'terminados' según la fase (monótono: nunca retrocede)."""
        if self.status == TaskStatus.DOWNLOADING and self.mode != "stream":
            value = int(self.bytes_fetched)
        else:
            with self._lock:
                uploaded = sum(self._part_uploaded.values())
            value = int(self.bytes_transferred + uploaded + self._inflight_credit())
        if value > self._processed_peak:
            self._processed_peak = value
        return self._processed_peak

    def progress_percent(self) -> float:
        total = self.file_size or 0
        if total <= 0:
            return 0.0
        if self.status == TaskStatus.DOWNLOADING and self.mode != "stream":
            return min(100.0, 100.0 * self.bytes_fetched / total)
        return min(100.0, 100.0 * self.processed_bytes() / total)

    def download_percent(self) -> float:
        total = self.file_size or 0
        if total <= 0:
            return 0.0
        return min(100.0, 100.0 * self.bytes_fetched / total)

    def eta_seconds(self) -> Optional[float]:
        """
        ETA según la fase; en streaming usa el cuello de botella real.

        En subidas por partes el tiempo NO es `restante / velocidad`: si se
        suben 3 partes a la vez, el motor sube 3 chunks en el tiempo de uno.
        Se calcula en "oleadas" (waves) porque además así se acierta al final,
        cuando quedan menos partes que el paralelismo configurado.
        """
        total = self.file_size or 0
        if total <= 0:
            return None

        remaining_upload = max(0, total - self.processed_bytes())

        if self.mode == "stream":
            remaining_download = max(0, total - self.bytes_fetched)
            estimates = []
            up = self.upload_speed
            down = self.download_speed
            if up > 0:
                estimates.append(remaining_upload / up)
            if down > 0:
                estimates.append(remaining_download / down)
            return max(estimates) if estimates else None

        if self.status == TaskStatus.DOWNLOADING:
            remaining = max(0, total - self.bytes_fetched)
            # No reutilizar la estimación de subida: hasta que la descarga
            # tenga muestras reales, el texto debe mostrar "calculando…".
            speed = self.download_speed
            if speed <= 0:
                return None
            return remaining / speed

        speed = self.upload_speed
        if speed <= 0:
            return None
        if self.upload_reports_bytes and self._live_meter.speed > 0:
            # La velocidad medida ya es el total de todas las partes en paralelo.
            return remaining_upload / speed
        return self._waves_seconds(remaining_upload, speed)

    def _waves_seconds(self, remaining: float, speed: float) -> float:
        """
        Tiempo restante subiendo `remaining` bytes por partes en paralelo.

        1) Las partes que YA están en vuelo terminan cuando les queda su trozo:
           se cuenta la más lenta (todas corren a la vez).
        2) Las que aún no han empezado van en "oleadas" de `workers` partes.

        Sin el paso 1 la cuenta final se quedaba en "restante/velocidad" con
        una parte ya al 90 %, es decir, sobreestimaba el último tramo.
        """
        if remaining <= 0 or speed <= 0:
            return 0.0
        chunk = self.chunk_size or 0
        if chunk <= 0 or self.parts_total <= 1:
            return remaining / speed

        now = time.monotonic()
        with self._lock:
            inflight = dict(self._inflight)

        drain = 0.0
        for number, started_at in inflight.items():
            length = self.expected_part_length(number)
            if length <= 0:
                continue
            made = min(float(length), max(0.0, (now - started_at) * speed))
            drain = max(drain, (length - made) / speed)

        workers = max(1, min(self.parallel_parts or 1, self.parts_total))
        parts_left = int(math.ceil(remaining / float(chunk)))
        queued = max(0, parts_left - len(inflight))
        waves = int(math.ceil(queued / float(workers))) if queued > 0 else 0
        return drain + waves * (chunk / float(speed))

    def mode_guess_stream(self) -> bool:
        """¿Es probable que esta tarea se ejecute en modo streaming?"""
        return RUNTIME_SETTINGS.get("pipeline_mode", "auto") in ("stream",)

    def is_estimated(self) -> bool:
        """True si las cifras de subida son estimadas (sin medición propia)."""
        return not self.upload_speed_measured

    def duration(self) -> float:
        start = self.started_at or self.created_at
        end = self.finished_at or time.time()
        return max(0.0, end - start)

    def avg_speed(self) -> float:
        dur = self.duration()
        if dur <= 0:
            return 0.0
        done = self.bytes_transferred or self.bytes_fetched
        return done / dur if done else 0.0

    def max_attempts_total(self) -> int:
        return self.max_attempts

    def estimate_bytes(self) -> int:
        """Tamaño estimado de trabajo pendiente (para ETA de la cola)."""
        if self.file_size:
            return max(0, self.file_size - self.bytes_transferred)
        return self.AVG_SIZE_FALLBACK

    # ------------------------------------------------------------------
    # Serialización (persistencia entre reinicios).
    # ------------------------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "username": self.username,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "status_message_id": self.status_message_id,
            "filename": self.filename,
            "file_size": self.file_size,
            "source": self.source,
            "url": self.url,
            "media_format": self.media_format,
            "media_quality": self.media_quality,
            "requested_chunk_mb": self.requested_chunk_mb,
            "priority": self.priority,
            "status": self.status.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "resumed": True,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "UploadTask":
        task = cls(
            user_id=int(data.get("user_id", 0)),
            chat_id=int(data.get("chat_id", 0)),
            message_id=int(data.get("message_id", 0)),
            filename=data.get("filename", "archivo.bin"),
            file_size=int(data.get("file_size", 0)),
            source=data.get("source", "telegram"),
            url=data.get("url"),
            username=data.get("username", ""),
            priority=int(data.get("priority", 0)),
            max_attempts=int(data.get("max_attempts", 1)),
            task_id=data.get("task_id"),
            resumed=True,
        )
        task.media_format = data.get("media_format", "mp4")
        task.media_quality = data.get("media_quality", "720")
        task.requested_chunk_mb = int(data.get("requested_chunk_mb") or 0)
        task.status_message_id = data.get("status_message_id")
        task.attempts = int(data.get("attempts", 1))
        created = data.get("created_at")
        if created:
            task.created_at = float(created)
            task.queued_at = float(created)
        return task

    @classmethod
    def from_history(cls, row: dict) -> "UploadTask":
        """
        Reconstruye una tarea terminada a partir de su fila en el historial.

        Así los botones de un mensaje antiguo (enlaces, borrar, reintentar)
        siguen funcionando aunque la tarea ya no esté en memoria.
        """
        user_id = int(row.get("user_id") or 0)
        task = cls(
            user_id=user_id,
            chat_id=int(row.get("chat_id") or user_id),
            message_id=int(row.get("message_id") or 0),
            filename=row.get("filename") or "archivo.bin",
            file_size=int(row.get("file_size") or 0),
            source=row.get("source") or "telegram",
            url=row.get("url") or None,
            username=row.get("username") or "",
            task_id=row.get("task_id"),
        )
        task.media_format = row.get("media_format") or "mp4"
        task.media_quality = row.get("media_quality") or "720"
        try:
            task.status = TaskStatus(row.get("status") or "completed")
        except ValueError:
            task.status = TaskStatus.FAILED
        task.keys = list(row.get("keys") or [])
        task.urls = list(row.get("urls") or [])
        task.parts_total = task.parts_done = int(row.get("parts") or 0)
        task.s3_base = row.get("base") or None
        task.error = row.get("error") or None
        task.deleted_from_s3 = bool(row.get("deleted_at"))
        created = float(row.get("created_at") or time.time())
        finished = float(row.get("finished_at") or created)
        task.created_at = task.queued_at = created
        task.finished_at = finished
        task.started_at = max(created, finished - float(row.get("duration") or 0.0))
        if task.status == TaskStatus.COMPLETED:
            task.bytes_transferred = task.file_size
        return task


# ----------------------------------------------------------------------
# Gestor de cola.
# ----------------------------------------------------------------------
class QueueManager:
    """Planificador central. Un único por proceso (``QUEUE``)."""

    def __init__(self, cfg=None) -> None:
        from config import CONFIG  # import local: evita ciclos al importar

        self.cfg = cfg or CONFIG
        self._tasks: Dict[str, UploadTask] = {}
        self._waiting: List[UploadTask] = []
        self._running: Dict[str, asyncio.Task] = {}
        self._user_active: Dict[int, int] = {}
        self._user_waiting: Dict[int, int] = {}
        self._last_submit: Dict[int, float] = {}
        self._bytes_window: Deque[Tuple[float, int]] = deque(maxlen=600)

        self.paused = False
        self.paused_by: Optional[int] = None

        self.started_at = time.time()
        self.completed_count = 0
        self.failed_count = 0
        self.cancelled_count = 0
        self.timeout_count = 0

        self._client = None
        self._runner: Optional[Callable[..., Any]] = None
        self._sched_task: Optional[asyncio.Task] = None
        self._wake: Optional[asyncio.Event] = None
        self._running_flag = False
        self._last_save = 0.0
        self._last_beat = 0.0
        self._dirty = False

    # ------------------------------------------------------------------
    # Ciclo de vida.
    # ------------------------------------------------------------------
    def attach(self, client, runner: Callable[..., Any]) -> None:
        """Registra el cliente de Pyrogram y el ejecutor de tareas."""
        self._client = client
        self._runner = runner

    async def start(self) -> None:
        self._wake = asyncio.Event()
        self._running_flag = True
        self._heartbeat()
        self._sched_task = asyncio.create_task(self._scheduler(), name="queue-scheduler")
        log.info(
            "Cola iniciada | tareas simultáneas=%d | por usuario=%d | espera máx=%d",
            RUNTIME_SETTINGS.max_concurrent_tasks,
            RUNTIME_SETTINGS.max_active_per_user,
            RUNTIME_SETTINGS.max_queue_total,
        )

    async def stop(self, *, cancel_running: bool = False) -> None:
        """
        Detiene el planificador y guarda el estado.

        Importante para plataformas como Railway que apagan el contenedor con
        SIGTERM y matan el proceso unos segundos después: el estado se guarda
        ANTES de cancelar las tareas, para que nada pendiente se pierda aunque
        el apagado se corte a la mitad.
        """
        # Las tareas que queden a medias se guardan como "interrumpidas" para
        # reanudarlas en el próximo arranque (esto es lo que salva el trabajo
        # cuando Railway manda SIGTERM y reinicia el contenedor).
        for pending in self._tasks.values():
            if not pending.is_final:
                pending.interrupted = True
        self._mark_dirty()
        self._save_state(force=True)
        self._running_flag = False
        if self._wake:
            self._wake.set()
        if self._sched_task:
            self._sched_task.cancel()
        if cancel_running:
            for task in list(self._running.values()):
                task.cancel("apagado del bot")
            for running in list(self._running.values()):
                try:
                    # Al cancelar una tarea, `wait_for` relanza CancelledError
                    # (BaseException, no Exception): hay que capturarlo o el
                    # apagado se cortaría antes de guardar el estado.
                    await asyncio.wait_for(running, timeout=15)
                except BaseException:  # noqa: BLE001
                    pass
        self._save_state(force=True)

    def wake(self) -> None:
        if self._wake:
            self._wake.set()

    # ------------------------------------------------------------------
    # Admisión.
    # ------------------------------------------------------------------
    def check_rate_limit(self, user_id: int) -> Optional[int]:
        """Devuelve los segundos que faltan para poder volver a enviar."""
        limit = RUNTIME_SETTINGS.rate_limit_sec
        if limit <= 0:
            return None
        last = self._last_submit.get(user_id, 0.0)
        remaining = limit - (time.time() - last)
        if remaining > 0:
            return int(remaining) + 1
        return None

    def touch_rate_limit(self, user_id: int) -> None:
        self._last_submit[user_id] = time.time()

    def user_load(self, user_id: int) -> Tuple[int, int]:
        return self._user_active.get(user_id, 0), self._user_waiting.get(user_id, 0)

    def find_duplicate_url(self, user_id: int, url: str) -> Optional[UploadTask]:
        """Tarea activa del usuario con la misma URL (evita repetidos)."""
        for task in self._tasks.values():
            if (
                task.user_id == user_id
                and task.url == url
                and not task.is_final
            ):
                return task
        return None

    def submit(self, task: UploadTask, *, force: bool = False) -> Tuple[bool, str]:
        """
        Encola una tarea. Devuelve ``(ok, mensaje)``.

        Motivos de rechazo: cola pausada, cupo del usuario, cola global llena.
        ``force=True`` se usa al reanudar tareas tras un reinicio: ya estaban
        admitidas antes del apagado, así que no se vuelven a rechazar por cupos.
        """
        owner = force or self._is_owner_task(task)
        if self.paused and not owner:
            return False, (
                "⏸ <b>La cola está pausada</b> por el administrador.\n"
                "Intenta de nuevo en unos minutos."
            )

        active, waiting = self.user_load(task.user_id)
        max_active = RUNTIME_SETTINGS.max_active_per_user
        max_waiting = RUNTIME_SETTINGS.max_queue_per_user

        if not owner and active + waiting >= max_active + max_waiting:
            return False, (
                f"🚦 <b>Límite alcanzado</b>\n\n"
                f"Ya tienes <b>{active + waiting}</b> tareas "
                f"({active} activas + {waiting} en espera).\n"
                f"Espera a que terminen o usa /cancelar."
            )

        total_outstanding = len(self._waiting) + len(self._running)
        if not owner and total_outstanding >= RUNTIME_SETTINGS.max_queue_total:
            return False, (
                "🌩 <b>El sistema está saturado ahora mismo</b>\n\n"
                f"Hay {total_outstanding} tareas en cola (máximo "
                f"{RUNTIME_SETTINGS.max_queue_total}).\n"
                "Vuelve a intentarlo en unos minutos 🙏"
            )

        if task.is_cancelled:
            return False, "🛑 La tarea fue cancelada antes de entrar en la cola."

        self._tasks[task.task_id] = task
        task.queued_at = time.time()
        self._waiting.append(task)
        self._user_waiting[task.user_id] = self._user_waiting.get(task.user_id, 0) + 1
        self._refresh_positions()
        self._mark_dirty()
        self.wake()
        return True, "ok"

    # ------------------------------------------------------------------
    # Cancelación / eliminación.
    # ------------------------------------------------------------------
    def get(self, task_id: str) -> Optional[UploadTask]:
        return self._tasks.get(task_id)

    def cancel(self, task_id: str, reason: str = "cancelada por el usuario") -> bool:
        task = self._tasks.get(task_id)
        if not task or task.is_final:
            return False
        task.cancel(reason)
        if task in self._waiting:
            self._drop_waiting(task, status=TaskStatus.CANCELLED)
        self._refresh_positions()
        self.wake()
        return True

    def cancel_user(self, user_id: int) -> int:
        count = 0
        for task in list(self._tasks.values()):
            if task.user_id == user_id and not task.is_final:
                if self.cancel(task.task_id):
                    count += 1
        return count

    def cancel_all(self) -> int:
        count = 0
        for task in list(self._tasks.values()):
            if not task.is_final and self.cancel(task.task_id, "cola cancelada por el owner"):
                count += 1
        return count

    def forget(self, task_id: str) -> bool:
        """Quita una tarea terminada del registro en memoria."""
        task = self._tasks.get(task_id)
        if not task or not task.is_final:
            return False
        del self._tasks[task_id]
        return True

    def cleanup(self, max_age_seconds: int = 3600) -> int:
        """Borra del registro las tareas terminadas antiguas."""
        now = time.time()
        removed = 0
        for task_id, task in list(self._tasks.items()):
            if task.is_final and task.finished_at and (now - task.finished_at) > max_age_seconds:
                del self._tasks[task_id]
                removed += 1
        # El anti-spam solo necesita recordar a quien envió hace pocos segundos.
        stale = time.time() - 3600
        for user_id, ts in list(self._last_submit.items()):
            if ts < stale:
                del self._last_submit[user_id]
        if removed:
            self._mark_dirty()
        return removed

    # ------------------------------------------------------------------
    # Consultas.
    # ------------------------------------------------------------------
    def user_tasks(self, user_id: int, limit: int = 30) -> List[UploadTask]:
        tasks = [t for t in self._tasks.values() if t.user_id == user_id]
        tasks.sort(key=lambda t: t.created_at)
        return tasks[-limit:]

    def user_active_tasks(self, user_id: int) -> List[UploadTask]:
        return [t for t in self.user_tasks(user_id, limit=200) if not t.is_final]

    def active_tasks(self) -> List[UploadTask]:
        live = list(self._waiting) + [t for t in self._tasks.values() if t.status.is_active]
        live.sort(key=lambda t: t.created_at)
        return live

    def all_tasks(self, limit: int = 60) -> List[UploadTask]:
        tasks = sorted(self._tasks.values(), key=lambda t: t.created_at)
        return tasks[-limit:]

    def counts(self) -> dict:
        waiting = len(self._waiting)
        active = sum(1 for t in self._tasks.values() if t.status.is_active)
        users = {t.user_id for t in self._waiting} | {
            t.user_id for t in self._tasks.values() if t.status.is_active
        }
        return {
            "waiting": waiting,
            "active": active,
            "max_concurrent": RUNTIME_SETTINGS.max_concurrent_tasks,
            "users_waiting": len(users),
            "completed": self.completed_count,
            "failed": self.failed_count,
            "cancelled": self.cancelled_count,
            "uptime": time.time() - self.started_at,
            "global_speed": self.global_speed(),
            "paused": self.paused,
        }

    def global_speed(self) -> float:
        """Bytes/s agregados de los últimos 60 s (todo el bot)."""
        now = time.monotonic()
        while self._bytes_window and now - self._bytes_window[0][0] > 60:
            self._bytes_window.popleft()
        if len(self._bytes_window) < 2:
            return 0.0
        span = max(0.5, self._bytes_window[-1][0] - self._bytes_window[0][0])
        total = sum(n for _, n in self._bytes_window)
        return total / span

    def note_bytes(self, n: int) -> None:
        if n > 0:
            self._bytes_window.append((time.monotonic(), int(n)))

    # ------------------------------------------------------------------
    # Planificación.
    # ------------------------------------------------------------------
    def _is_owner_task(self, task: UploadTask) -> bool:
        return (
            self.cfg.owner_id is not None
            and task.user_id == self.cfg.owner_id
            and task.priority >= self.cfg.queue.owner_priority
        )

    def _effective_priority(self, task: UploadTask) -> float:
        """Prioridad + antigüedad (anti-inanición: +1 cada minuto de espera)."""
        if self._is_owner_task(task):
            return 1_000_000.0
        waited_min = (time.time() - task.queued_at) / 60.0
        return task.priority + waited_min

    def _dispatch_key(self, task: UploadTask):
        """
        Clave de orden (menor = antes).

        Con FAIR_SCHEDULING=true (por defecto) primero van los usuarios que no
        tienen ninguna tarea activa: nadie se queda detrás de quien sube 20
        archivos. Dentro de cada grupo manda la prioridad (owner) y la
        antigüedad (anti-inanición).
        """
        if self._is_owner_task(task):
            fairness = 0
        elif self.cfg.queue.fair_scheduling:
            fairness = self._user_active.get(task.user_id, 0)
        else:
            fairness = 0
        return (fairness, -self._effective_priority(task), task.queued_at)

    # Cuántas tareas puede arrancar el owner POR ENCIMA del límite global
    # ("carril rápido": su trabajo no espera detrás de la cola, pero tampoco
    # puede lanzar 15 subidas a la vez y saturar el servidor).
    OWNER_EXTRA_SLOTS = 1

    def _pick_next(self) -> Optional[UploadTask]:
        settings = RUNTIME_SETTINGS
        running = len(self._running)
        regular_slot_free = running < settings.max_concurrent_tasks
        owner_slot_free = running < settings.max_concurrent_tasks + self.OWNER_EXTRA_SLOTS

        candidates = []
        for task in self._waiting:
            if task.is_cancelled:
                continue
            if self._is_owner_task(task):
                # El owner no tiene cupo por usuario y puede usar el carril rápido,
                # incluso con la cola pausada.
                if owner_slot_free:
                    candidates.append(task)
                continue
            if self.paused or not regular_slot_free:
                continue
            if self._user_active.get(task.user_id, 0) >= settings.max_active_per_user:
                continue
            candidates.append(task)
        if not candidates:
            return None
        return min(candidates, key=self._dispatch_key)

    def _start_task(self, task: UploadTask) -> None:
        self._waiting.remove(task)
        self._user_waiting[task.user_id] = max(0, self._user_waiting.get(task.user_id, 1) - 1)
        self._user_active[task.user_id] = self._user_active.get(task.user_id, 0) + 1
        task.status = TaskStatus.DOWNLOADING
        task.phase = "Preparando"
        task.interrupted = False
        task.mark_started()
        self._refresh_positions()

        timeout_sec = max(
            60,
            int(RUNTIME_SETTINGS.get("task_timeout_min", self.cfg.queue.task_timeout_min)) * 60,
        )
        try:
            self._running[task.task_id] = asyncio.create_task(
                self._run_task(task, timeout_sec), name=f"task-{task.task_id}"
            )
        except Exception as exc:  # pragma: no cover
            log.exception("[%s] no se pudo arrancar: %s", task.task_id, exc)
            task.mark_finished(TaskStatus.FAILED, f"no se pudo arrancar: {exc}")
            self._finish_task(task)
            return

        log.info(
            "[%s] ▶️ arranca | usuario=%s | %s | %.1f MB",
            task.task_id,
            task.user_id,
            task.filename,
            (task.file_size or 0) / 1024 / 1024,
        )

    async def _run_task(self, task: UploadTask, timeout_sec: int) -> None:
        try:
            await asyncio.wait_for(self._runner(self._client, task), timeout=timeout_sec)
            if not task.is_final:
                if task.is_cancelled:
                    task.mark_finished(TaskStatus.CANCELLED)
                else:
                    task.mark_finished(TaskStatus.FAILED, "La tarea terminó sin estado final")
        except asyncio.TimeoutError:
            task.cancel("tiempo máximo excedido")
            task.mark_finished(
                TaskStatus.FAILED,
                f"Tiempo máximo excedido ({timeout_sec // 60} min)",
            )
            self.timeout_count += 1
            log.warning("[%s] ⏰ timeout", task.task_id)
        except asyncio.CancelledError:
            task.cancel("apagado del bot")
            if not task.is_final:
                task.mark_finished(TaskStatus.CANCELLED, "Bot apagado")
            raise
        except TaskCancelled:
            if not task.is_final:
                task.mark_finished(TaskStatus.CANCELLED)
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ in ("S3Error", "ChunkUploadError"):
                log.warning("[%s] falló la subida: %s", task.task_id, exc)
            else:
                log.exception("[%s] error no controlado: %s", task.task_id, exc)
            if not task.is_final:
                task.mark_finished(TaskStatus.FAILED, str(exc))
        finally:
            self._finish_task(task)

    def _finish_task(self, task: UploadTask) -> None:
        self._running.pop(task.task_id, None)
        self._user_active[task.user_id] = max(0, self._user_active.get(task.user_id, 1) - 1)
        if task.status == TaskStatus.COMPLETED:
            self.completed_count += 1
        elif task.status == TaskStatus.FAILED:
            self.failed_count += 1
        elif task.status == TaskStatus.CANCELLED:
            self.cancelled_count += 1
        # Una tarea cortada por el apagado NO ha terminado: se reanuda al
        # arrancar, así que ni se anota en el historial ni se avisa al usuario.
        shutdown_pause = task.interrupted and task.cancel_reason == "apagado del bot"
        if not shutdown_pause:
            from . import storage
            storage.release_quota(task.task_id)
            self._record_history(task)
        self._refresh_positions()
        self._mark_dirty()
        self.wake()
        size = int(task.file_size or task.bytes_transferred or task.bytes_fetched or 0)
        from .logs import audit
        if task.status == TaskStatus.COMPLETED:
            audit.info(
                "[%s] ✅ subida completada | usuario=%s | archivo=%r | peso=%d bytes | "
                "duracion=%.1fs | enlaces_enviados=%s",
                task.task_id,
                task.user_id,
                task.filename,
                size,
                task.duration(),
                " || ".join(task.urls or []) or "ninguno",
            )
        else:
            audit.info(
                "[%s] ⏹ %s | usuario=%s | archivo=%r | peso=%d bytes | duracion=%.1fs",
                task.task_id,
                task.status.value,
                task.user_id,
                task.filename,
                size,
                task.duration(),
            )
        if shutdown_pause:
            return
        # Avisar al notificador (import diferido para evitar ciclos).
        try:
            from . import notifier

            notifier.on_task_finished(task)
        except Exception:  # pragma: no cover
            pass

    def _drop_waiting(self, task: UploadTask, status: TaskStatus) -> None:
        if task in self._waiting:
            self._waiting.remove(task)
            self._user_waiting[task.user_id] = max(0, self._user_waiting.get(task.user_id, 1) - 1)
        task.mark_finished(status)
        from . import storage
        storage.release_quota(task.task_id)
        if status == TaskStatus.CANCELLED:
            self.cancelled_count += 1
        self._record_history(task)
        try:
            from . import notifier

            notifier.on_task_finished(task)
        except Exception:  # pragma: no cover
            pass

    def _record_history(self, task: UploadTask) -> None:
        try:
            from . import storage

            storage.record_task(task, self.cfg)
        except Exception as exc:  # pragma: no cover
            log.debug("No se pudo guardar el historial: %s", exc)

    # ------------------------------------------------------------------
    # Posiciones y ETA de la cola.
    # ------------------------------------------------------------------
    def _refresh_positions(self) -> None:
        if not self._waiting:
            return
        order = sorted(self._waiting, key=self._dispatch_key)
        settings = RUNTIME_SETTINGS
        slots = max(1, settings.max_concurrent_tasks)
        speed = self.global_speed()
        if speed <= 0:
            # Sin tráfico ahora mismo: usamos la velocidad media aprendida por
            # el bot (y si aún no hay muestras, una referencia conservadora).
            speed = SPEEDS.upload_estimate()

        pending_running = 0
        for task in self._tasks.values():
            if task.status.is_active:
                pending_running += task.estimate_bytes()

        # Cuántas tareas activas caben por usuario (para estimar varias a la vez)
        cum = float(pending_running)
        for idx, task in enumerate(order, start=1):
            task.queue_position = idx
            task.queue_length = len(order)
            task.queue_eta = cum / (slots * speed) if speed > 0 else None
            cum += task.estimate_bytes()

    async def _scheduler(self) -> None:
        last_cleanup = 0.0
        while self._running_flag:
            try:
                assert self._wake is not None
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()

                # _pick_next respeta la pausa (salvo el carril rápido del owner).
                while True:
                    task = self._pick_next()
                    if task is None:
                        break
                    self._start_task(task)

                now = time.time()
                if now - self._last_beat > 15:
                    self._heartbeat()
                if now - last_cleanup > 300:
                    last_cleanup = now
                    self.cleanup(max_age_seconds=3600)
                    self._persist_if_dirty()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("Error en el planificador: %s", exc)
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Persistencia.
    # ------------------------------------------------------------------
    def _mark_dirty(self) -> None:
        self._dirty = True
        self._persist_if_dirty()

    def _persist_if_dirty(self) -> None:
        if not self._dirty:
            return
        now = time.time()
        if now - self._last_save < 2.0:
            return
        self._save_state()

    def _save_state(self, force: bool = False) -> None:
        if not self._dirty and not force:
            return
        path = self.cfg.state_file
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            pending = [
                t.to_dict()
                for t in self._tasks.values()
                if not t.is_final or t.interrupted
            ]
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"saved_at": time.time(), "tasks": pending}, fh, indent=2)
            os.replace(tmp, path)
            self._dirty = False
            self._last_save = time.time()
        except Exception as exc:  # pragma: no cover
            log.warning("No se pudo guardar el estado de la cola: %s", exc)

    def _heartbeat(self) -> None:
        """Escribe un latido en disco para el HEALTHCHECK de Docker."""
        try:
            path = os.path.join(os.path.dirname(self.cfg.state_file) or ".", "heartbeat")
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"{time.time():.0f} activas={len(self._running)} "
                         f"espera={len(self._waiting)}\n")
            self._last_beat = time.time()
        except Exception as exc:  # pragma: no cover
            log.debug("No se pudo escribir el latido: %s", exc)

    def load_state(self) -> List[UploadTask]:
        """Tareas pendientes del último apagado (para reanudarlas)."""
        path = self.cfg.state_file
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            log.warning("Estado de cola ilegible (%s); se ignora", exc)
            return []

        tasks: List[UploadTask] = []
        for raw in data.get("tasks", []):
            try:
                tasks.append(UploadTask.from_dict(raw))
            except Exception as exc:  # pragma: no cover
                log.warning("Tarea guardada inválida: %s", exc)
        if tasks:
            log.info("↻ %d tarea(s) recuperadas del último apagado", len(tasks))
        return tasks

    # ------------------------------------------------------------------
    # Utilidades varias.
    # ------------------------------------------------------------------
    def user_ids(self) -> List[int]:
        ids = {t.user_id for t in self._tasks.values()}
        try:
            from . import storage

            ids |= set(storage.all_user_ids(self.cfg))
        except Exception:  # pragma: no cover
            pass
        return sorted(ids)

    def shutdown_running(self) -> int:
        """Cancela tareas activas (usado al apagar el bot)."""
        count = 0
        for task in self._tasks.values():
            if not task.is_final:
                task.interrupted = True
            if task.status.is_active and not task.is_cancelled:
                task.cancel("apagado del bot")
                count += 1
        for task in list(self._waiting):
            task.interrupted = True
            task.cancel("apagado del bot")
        self._save_state(force=True)
        return count


# ----------------------------------------------------------------------
# Singleton global.
# ----------------------------------------------------------------------
QUEUE = QueueManager()
