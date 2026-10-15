"""
views.py
========
Composición de TEXTO + TECLADO de cada pantalla del bot. Los comandos y los
botones llaman a las mismas funciones, así que /cola y el botón "Mi cola"
muestran exactamente lo mismo (antes estaba duplicado y se desincronizaba).

Cada ``*_view`` devuelve ``(texto_html, InlineKeyboardMarkup | None)``.
"""

from __future__ import annotations

import asyncio
import os
import platform
import shutil
import threading
import time
from typing import List, Optional, Tuple

from config import CONFIG
from . import keyboards as K
from . import storage
from . import texts as T
from .queue import QUEUE, TaskStatus, UploadTask
from .runtime_settings import (
    CYCLE_LABELS,
    CYCLE_UNITS,
    RUNTIME_SETTINGS,
    SETTINGS_PAGES,
)
from .s3_client import get_client
from .speedstats import SPEEDS

View = Tuple[str, Optional[object]]

QUEUE_PER_PAGE = 6
HISTORY_PER_PAGE = 5
_BOOT_TS = time.time()
_OWNER_NAME: dict = {"value": "", "at": 0.0}


# ----------------------------------------------------------------------
# Inicio y ayuda.
# ----------------------------------------------------------------------
async def owner_display_name(client) -> str:
    """@usuario del owner (cacheado 10 min: evita una llamada a la API por /start)."""
    now = time.time()
    if _OWNER_NAME["value"] and now - _OWNER_NAME["at"] < 600:
        return _OWNER_NAME["value"]
    name = str(CONFIG.owner_id or "no configurado")
    if CONFIG.owner_id and client is not None:
        try:
            owner = await client.get_users(CONFIG.owner_id)
            name = f"@{owner.username}" if owner.username else (owner.first_name or name)
        except Exception:  # noqa: BLE001
            pass
    _OWNER_NAME.update(value=name, at=now)
    return name


def home_view(user_id: int, *, is_owner_user: bool, admin_name: str = "") -> View:
    return (
        T.welcome_text(user_id, public=not CONFIG.allowed_users, is_owner=is_owner_user,
                       cfg=CONFIG, settings=RUNTIME_SETTINGS, admin_name=admin_name),
        K.main_menu_keyboard(is_owner_user=is_owner_user),
    )


def help_view(page: int) -> View:
    page = max(1, min(page, T.HELP_PAGE_COUNT))
    return T.help_text(page), K.help_keyboard(page)


def sites_view() -> View:
    return T.sites_text(), K.panel_keyboard()


def chunk_view(user_id: int) -> View:
    current = storage.get_user_chunk_size(user_id)
    shown = f"<b>{current} MB</b>" if current else "<b>valor global del bot</b>"
    return (
        "🧩 <b>Tamaño de chunks</b>\n\n"
        f"Tu configuración actual: {shown}\n\n"
        "Elige una opción para las tareas nuevas. Las tareas que ya están en cola "
        "conservan su tamaño.\n\n"
        "También puedes usar <code>/chunk 350</code> para indicar un valor personalizado "
        "entre 1 y 2000 MB.",
        K.chunk_keyboard(current),
    )


# ----------------------------------------------------------------------
# Cola.
# ----------------------------------------------------------------------
def _display_order(tasks: List[UploadTask]) -> List[UploadTask]:
    """En curso/en espera primero (más antiguas arriba) y luego las terminadas, recientes primero."""
    live = sorted((t for t in tasks if not t.is_final), key=lambda t: t.created_at)
    done = sorted((t for t in tasks if t.is_final),
                  key=lambda t: t.finished_at or t.created_at, reverse=True)
    return live + done


def queue_view(user_id: int, scope: str, page: int, *, is_owner_user: bool) -> View:
    global_view = scope == "g" and is_owner_user
    scope = "g" if global_view else "m"
    tasks = _display_order(
        QUEUE.all_tasks(limit=120) if global_view else QUEUE.user_tasks(user_id, limit=60)
    )
    pages = max(1, (len(tasks) + QUEUE_PER_PAGE - 1) // QUEUE_PER_PAGE)
    page = max(1, min(page, pages))
    return (
        T.queue_text(user_id, tasks, page, QUEUE_PER_PAGE, global_view=global_view),
        K.queue_keyboard(tasks, page, QUEUE_PER_PAGE, scope=scope, is_owner_user=is_owner_user),
    )


def cancel_all_view(user_id: int, scope: str, *, is_owner_user: bool) -> Optional[View]:
    """Confirmación de "cancelar todas". None si no hay nada que cancelar."""
    global_scope = scope == "g" and is_owner_user
    pending = QUEUE.active_tasks() if global_scope else QUEUE.user_active_tasks(user_id)
    if not pending:
        return None
    return (
        T.cancel_all_text(pending, global_scope=global_scope),
        K.confirm_cancel_all_keyboard("g" if global_scope else "m"),
    )


# ----------------------------------------------------------------------
# Historial y fichas de tarea.
# ----------------------------------------------------------------------
def history_view(user_id: int, page: int) -> View:
    rows = storage.user_history(user_id, limit=100)
    pages = max(1, (len(rows) + HISTORY_PER_PAGE - 1) // HISTORY_PER_PAGE)
    page = max(1, min(page, pages))
    return (
        T.history_text(rows, page, HISTORY_PER_PAGE),
        K.history_keyboard(rows, page, HISTORY_PER_PAGE, pages=pages),
    )


def search_view(user_id: int, query: str) -> View:
    rows = storage.search_user_history(user_id, query, limit=HISTORY_PER_PAGE * 2)
    per_page = max(1, len(rows))
    return (
        T.history_text(rows, 1, per_page, title=f"🔍 Búsqueda «{query[:30]}»"),
        K.history_keyboard(rows, 1, per_page, paged=False),
    )


def task_message(task: UploadTask) -> View:
    """El mensaje de la propia tarea según su estado (progreso, resultado, error…)."""
    status = task.status
    if status == TaskStatus.QUEUED:
        text = T.queued_text(task)
    elif status.is_active:
        text = T.progress_text(task)
    elif status == TaskStatus.COMPLETED:
        text = T.final_text(task)
    elif status == TaskStatus.CANCELLED:
        text = T.cancelled_text(task)
    else:
        text = T.failed_text(task)
    return text, K.task_message_keyboard(task)


def card_view(task: UploadTask, ctx: str, viewer_id: int, *, manage: bool) -> View:
    foreign = manage and task.user_id != viewer_id
    can_drop = manage and QUEUE.get(task.task_id) is task
    return (
        T.task_card_text(task, show_user=foreign),
        K.task_card_keyboard(task, ctx, can_drop=can_drop),
    )


def delete_confirm_view(task: UploadTask, ctx: str) -> View:
    return (
        "<b>🗑 ¿Borrar de toDus?</b>\n\n"
        f"📄 {T.code(T.shorten(task.filename, 44))}\n"
        f"🧩 {len(task.urls)} archivo(s) en el almacenamiento\n\n"
        + T.i("Los enlaces dejarán de funcionar para quien los tenga."),
        K.confirm_delete_keyboard(task.task_id, ctx),
    )


# ----------------------------------------------------------------------
# Estado, cuota, ranking.
# ----------------------------------------------------------------------
async def ping_storage(timeout: float = 12.0) -> Tuple[bool, str]:
    """Comprueba el endpoint de toDus sin bloquear el event loop."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(lambda: get_client().ping()), timeout=timeout
        )
    except asyncio.TimeoutError:
        return False, "el almacenamiento no respondió a tiempo"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


async def status_view(client) -> View:
    try:
        me = await client.get_me()
        bot_line = f"✅ {T.code('@' + (me.username or str(me.id)))} en línea"
    except Exception as exc:  # noqa: BLE001
        bot_line = f"⚠️ {T.esc(T.shorten(str(exc), 80))}"
    ok, detail = await ping_storage()
    return (
        T.status_text(
            bot_line=bot_line,
            storage_line=f"{'✅' if ok else '❌'} {T.esc(T.shorten(detail, 90))}",
            settings=RUNTIME_SETTINGS,
            counts=QUEUE.counts(),
            paused=QUEUE.paused,
            speeds_line=SPEEDS.summary(),
        ),
        K.panel_keyboard(refresh="menu:status"),
    )


def quota_view(user_id: int) -> View:
    limit = int(RUNTIME_SETTINGS.get("daily_quota_mb", 0) or 0) * 1024 * 1024
    return (
        T.quota_text(used=storage.bytes_today(user_id), limit=limit),
        K.panel_keyboard(refresh="menu:quota"),
    )


def top_view(user_id: int, *, is_owner_user: bool) -> View:
    return (
        T.top_users_text(storage.top_users(10), viewer_id=user_id,
                         is_owner_user=is_owner_user, owner_id=int(CONFIG.owner_id or 0)),
        K.panel_keyboard(refresh="menu:top"),
    )


# ----------------------------------------------------------------------
# Administración (owner).
# ----------------------------------------------------------------------
def admin_view() -> View:
    return (
        T.admin_text(QUEUE.counts(), QUEUE.paused, bool(RUNTIME_SETTINGS.get("maintenance", False))),
        K.admin_keyboard(QUEUE.paused),
    )


def stats_view() -> View:
    return (
        T.stats_text(storage.global_stats(), storage.top_users(5), QUEUE.counts()),
        K.panel_keyboard(refresh="menu:stats", back=("Admin", "menu:admin")),
    )


def system_info() -> dict:
    """Disco, memoria, CPU y temporales (bloqueante: llamar desde un hilo)."""
    root = CONFIG.download_dir if os.path.isdir(CONFIG.download_dir) else "."
    disk = shutil.disk_usage(root)
    temp_size = temp_files = 0
    for folder, _dirs, files in os.walk(CONFIG.download_dir):
        for name in files:
            try:
                temp_size += os.path.getsize(os.path.join(folder, name))
                temp_files += 1
            except OSError:
                pass

    mem_total = mem_used = 0
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            entries = {}
            for line in fh:
                key, _, value = line.partition(":")
                entries[key.strip()] = value.strip()
        mem_total = int(entries.get("MemTotal", "0 kB").split()[0]) * 1024
        available = int(entries.get("MemAvailable", "0 kB").split()[0]) * 1024
        mem_used = max(0, mem_total - available)
    except Exception:  # pragma: no cover
        pass
    try:
        load = ", ".join(f"{v:.2f}" for v in os.getloadavg())
    except Exception:  # pragma: no cover
        load = "n/d"

    return {
        "disk_free": disk.free,
        "disk_total": disk.total,
        "disk_percent": (disk.used / disk.total * 100) if disk.total else 0.0,
        "mem_total": mem_total,
        "mem_used": mem_used,
        "cpu": os.cpu_count() or 1,
        "load": load,
        "threads": threading.active_count(),
        "temp_size": temp_size,
        "temp_files": temp_files,
        "uptime": time.time() - _BOOT_TS,
        "python": platform.python_version(),
    }


async def system_view() -> View:
    info = await asyncio.to_thread(system_info)
    return T.system_text(info), K.panel_keyboard(refresh="menu:sys", back=("Admin", "menu:admin"))


def settings_view(page: int, *, is_owner_user: bool) -> View:
    pages = len(SETTINGS_PAGES)
    page = max(1, min(page, pages))
    keys = SETTINGS_PAGES[page - 1]
    values = RUNTIME_SETTINGS.as_dict()
    return (
        T.settings_text(values, page, pages, is_owner_user=is_owner_user,
                        keys=keys, labels=CYCLE_LABELS, units=CYCLE_UNITS),
        K.settings_keyboard(keys, values, CYCLE_LABELS, CYCLE_UNITS,
                            page, pages, is_owner_user=is_owner_user),
    )
