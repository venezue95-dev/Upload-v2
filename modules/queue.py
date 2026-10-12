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
import os
import threading
import time
import uuid
from collections import deque
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from .progress import SpeedMeter
from .runtime_settings import RUNTIME_SETTINGS

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
        self.file_size = int(file_size or 0)
        self.bytes_fetched = 0                # descargado del origen
        self.bytes_transferred = 0            # subido de verdad a S3
        self.parts_done = 0
        self.parts_total = 0
        self.chunk_size = 0
        self._inflight: Dict[int, float] = {}  # parte -> t0 (monotonic)
        self._lock = threading.Lock()
        self._meter = SpeedMeter()
        self.speed = 0.0
        self._mono_start = time.monotonic()

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
        # True si la tarea quedó a medias por un apagado: se reanuda al arrancar.
        self.interrupted = False

        # Cancelación (thread-safe: los hilos de subida la consultan)
        self.cancel_flag = threading.Event()
        self._cancel_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Propiedades.
    # ------------------------------------------------------------------
    @property
    def is_cancelled(self) -> bool:
        return self.cancel_flag.is_set()

    @property
    def is_final(self) -> bool:
        return self.status.is_final

    @property
    def is_active(self) -> bool:
        return self.status.is_active

    def short_name(self, limit: int = 40) -> str:
        return self.filename if len(self.filename) <= limit else self.filename[: limit - 1] + "…"

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

    def part_finished(self, part_number: int, length: int) -> None:
        with self._lock:
            self._inflight.pop(part_number, None)
            self.parts_done += 1
            self.bytes_transferred += max(0, length)

    def part_failed(self, part_number: int) -> None:
        with self._lock:
            self._inflight.pop(part_number, None)

    def add_fetched(self, n: int) -> None:
        with self._lock:
            self.bytes_fetched += max(0, n)

    def set_fetched(self, n: int) -> None:
        with self._lock:
            self.bytes_fetched = max(0, n)

    def cancel(self, reason: str = "cancelada por el usuario") -> None:
        self._cancel_reason = reason
        self.cancel_flag.set()

    @property
    def cancel_reason(self) -> Optional[str]:
        return self._cancel_reason

    def mark_started(self) -> None:
        self.started_at = time.time()
        self._mono_start = time.monotonic()
        self._meter.reset()

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
        """Llama el notificador cada pocos segundos: actualiza la velocidad."""
        mono = time.monotonic()
        if self.status.is_final:
            return self.speed
        self.speed = self._meter.update(self.bytes_transferred, mono)
        return self.speed

    def _inflight_credit(self) -> float:
        """Crédito parcial de las partes en vuelo (para que la barra fluya)."""
        if self.speed <= 0 or not self.parts_total:
            return 0.0
        now = time.monotonic()
        credit = 0.0
        with self._lock:
            inflight = dict(self._inflight)
        for part_number, t0 in inflight.items():
            length = self.expected_part_length(part_number)
            if length <= 0:
                continue
            credit += min(0.92 * length, max(0.0, (now - t0) * self.speed))
        return credit

    def processed_bytes(self) -> int:
        if self.status == TaskStatus.DOWNLOADING and self.mode != "stream":
            return int(self.bytes_fetched)
        return int(self.bytes_transferred + self._inflight_credit())

    def progress_percent(self) -> float:
        total = self.file_size or 0
        if total <= 0:
            return 0.0
        if self.status == TaskStatus.DOWNLOADING and self.mode != "stream":
            return min(100.0, 100.0 * self.bytes_fetched / total)
        return min(100.0, 100.0 * self.processed_bytes() / total)

    def eta_seconds(self) -> Optional[float]:
        total = self.file_size or 0
        if total <= 0 or self.speed <= 0:
            return None
        return max(0.0, (total - self.processed_bytes()) / self.speed)

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
        task.status_message_id = data.get("status_message_id")
        task.attempts = int(data.get("attempts", 1)) + 1
        created = data.get("created_at")
        if created:
            task.created_at = float(created)
            task.queued_at = float(created)
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

    def submit(self, task: UploadTask) -> Tuple[bool, str]:
        """
        Encola una tarea. Devuelve ``(ok, mensaje)``.

        Motivos de rechazo: cola pausada, cupo del usuario, cola global llena.
        """
        if self.paused:
            return False, (
                "⏸ <b>La cola está pausada</b> por el administrador.\n"
                "Intenta de nuevo en unos minutos."
            )

        active, waiting = self.user_load(task.user_id)
        max_active = RUNTIME_SETTINGS.max_active_per_user
        max_waiting = RUNTIME_SETTINGS.max_queue_per_user

        if active + waiting >= max_active + max_waiting:
            return False, (
                f"🚦 <b>Límite alcanzado</b>\n\n"
                f"Ya tienes <b>{active + waiting}</b> tareas "
                f"({active} activas + {waiting} en espera).\n"
                f"Espera a que terminen o usa /cancelar."
            )

        total_outstanding = len(self._waiting) + len(self._running)
        if total_outstanding >= RUNTIME_SETTINGS.max_queue_total:
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
    def _effective_priority(self, task: UploadTask) -> float:
        """Prioridad + antigüedad (anti-inanición: +1 cada minuto de espera)."""
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
        fairness = (
            self._user_active.get(task.user_id, 0)
            if self.cfg.queue.fair_scheduling
            else 0
        )
        return (fairness, -self._effective_priority(task), task.queued_at)

    def _pick_next(self) -> Optional[UploadTask]:
        settings = RUNTIME_SETTINGS
        if len(self._running) >= settings.max_concurrent_tasks:
            return None
        if self.paused:
            return None

        candidates = []
        for task in self._waiting:
            if task.is_cancelled:
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
        self._record_history(task)
        self._refresh_positions()
        self._mark_dirty()
        self.wake()
        log.info(
            "[%s] ⏹ %s | %s | %.1f MB en %.0fs",
            task.task_id,
            task.status.value,
            task.filename,
            (task.bytes_transferred or task.bytes_fetched) / 1024 / 1024,
            task.duration(),
        )
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
            speed = 700 * 1024  # referencia conservadora: 700 KB/s

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

                if not self.paused:
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
