"""
admin.py
========
Comandos de administración (solo owner):

- /stats      → estadísticas globales + top usuarios.
- /usuarios   → lista de usuarios que han usado el bot.
- /anuncio    → envía un mensaje a todos los usuarios conocidos.
- /pausar     → pausa la cola (deja terminar lo que está en curso).
- /reanudar   → reactiva la cola.
- /sistema    → disco, memoria, CPU, temporales.
- /limpiar    → cancela la cola, limpia temporales y purga historial antiguo.
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import time

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from config import CONFIG
from .. import storage, texts as T
from ..queue import QUEUE
from .common import OWNER, reply

log = logging.getLogger("s3_bot.admin")
_boot_ts = time.time()


def register(app: Client) -> None:
    @app.on_message(filters.command(["stats", "estadisticas"]) & OWNER)
    async def _stats(client: Client, message: Message):
        data = storage.global_stats()
        top = storage.top_users(5)
        counts = QUEUE.counts()
        await reply(message, T.stats_text(data, top, counts), keyboard=T.admin_keyboard(QUEUE.paused))

    @app.on_message(filters.command(["usuarios", "users"]) & OWNER)
    async def _usuarios(client: Client, message: Message):
        ids = QUEUE.user_ids()
        if not ids:
            await reply(message, "👥 Todavía no hay usuarios registrados.")
            return
        lines = [f"<b>👥 Usuarios conocidos ({len(ids)})</b>", ""]
        for user_id in ids[:60]:
            stats = storage.user_stats(user_id) or {}
            active, waiting = QUEUE.user_load(user_id)
            who = stats.get("username") or ""
            from ..progress import human_size

            lines.append(
                f"• {T.code(str(user_id))} {T.esc(T.shorten(who, 18))} — "
                f"{human_size(stats.get('bytes', 0))} en {stats.get('uploads', 0)} subidas"
                + (f" · 🟢 {active} ⏳ {waiting}" if (active or waiting) else "")
            )
        if len(ids) > 60:
            lines.append(T.i(f"… y {len(ids) - 60} más"))
        await reply(message, "\n".join(lines))

    @app.on_message(filters.command("anuncio") & OWNER)
    async def _anuncio(client: Client, message: Message):
        text = (message.text or "").partition(" ")[2].strip()
        if not text:
            await reply(
                message,
                "Uso: " + T.code("/anuncio Tu mensaje aquí") + "\n"
                + T.i("Se enviará a todos los usuarios que han usado el bot."),
            )
            return
        ids = QUEUE.user_ids()
        sent = failed = 0
        await reply(message, f"📣 Enviando a {len(ids)} usuarios…")
        for user_id in ids:
            try:
                await client.send_message(
                    user_id,
                    f"📣 <b>Aviso del administrador</b>\n\n{T.esc(text)}",
                    parse_mode=ParseMode.HTML,
                )
                sent += 1
            except Exception:  # noqa: BLE001
                failed += 1
            await asyncio.sleep(0.05)   # evita el flood control
        await reply(message, f"✅ Enviado a {sent} usuarios · ❌ {failed} fallidos")

    @app.on_message(filters.command("pausar") & OWNER)
    async def _pausar(client: Client, message: Message):
        QUEUE.paused = True
        QUEUE.paused_by = message.from_user.id
        QUEUE.wake()
        await reply(
            message,
            "⏸ <b>Cola pausada</b>\n\n"
            "Las tareas en curso terminan, pero no arrancará ninguna nueva.\n"
            "Usa /reanudar para continuar.",
            keyboard=T.admin_keyboard(True),
        )

    @app.on_message(filters.command("reanudar") & OWNER)
    async def _reanudar(client: Client, message: Message):
        QUEUE.paused = False
        QUEUE.paused_by = None
        QUEUE.wake()
        await reply(
            message,
            "▶️ <b>Cola reanudada</b>",
            keyboard=T.admin_keyboard(False),
        )

    @app.on_message(filters.command(["sistema", "sys"]) & OWNER)
    async def _sistema(client: Client, message: Message):
        info = await asyncio.to_thread(_system_info)
        await reply(message, T.system_text(info))

    @app.on_message(filters.command("limpiar") & OWNER)
    async def _limpiar(client: Client, message: Message):
        cancelled = QUEUE.cancel_all()
        removed_files = 0
        for root, dirs, files in os.walk(CONFIG.download_dir):
            for name in files:
                try:
                    os.remove(os.path.join(root, name))
                    removed_files += 1
                except OSError:
                    pass
        purged = storage.purge_older_than(90)
        storage.init()  # asegura WAL tras el purge
        await reply(
            message,
            "<b>🧹 Limpieza completada</b>\n\n"
            f"• Tareas canceladas: {T.code(cancelled)}\n"
            f"• Temporales borrados: {T.code(removed_files)}\n"
            f"• Registros de historial antiguos: {T.code(purged)}\n\n"
            + T.i("El historial reciente y las estadísticas se conservan."),
        )


def _system_info() -> dict:
    disk = shutil.disk_usage(CONFIG.download_dir if os.path.isdir(CONFIG.download_dir) else ".")
    temp_size = 0
    temp_files = 0
    for root, _dirs, files in os.walk(CONFIG.download_dir):
        for name in files:
            try:
                temp_size += os.path.getsize(os.path.join(root, name))
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

    import threading

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
        "uptime": time.time() - _boot_ts,
        "python": platform.python_version(),
    }
