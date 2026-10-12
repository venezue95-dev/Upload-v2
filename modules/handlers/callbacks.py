"""
callbacks.py
============
Enrutador único de todos los botones inline.

Formato de ``callback_data``:

    noop
    menu:<home|cola|hist|status|stats|sys|set|help:N>
    cola:<refresh|global|page:N|cancelall|cancelall:yes>
    hist:page:N
    t:<cancel|card|retry|links|drop>:<task_id>
    t:delyes[:yes]:<task_id>
    set:<cycle|view>:<clave> · set:<page:N|refresh|reset>
    adm:<pause|resume>
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from config import CONFIG
from .. import progress as P
from .. import storage, texts as T
from ..notifier import send_links_txt
from ..queue import QUEUE
from ..runtime_settings import RUNTIME_SETTINGS
from ..s3_client import S3Client
from .admin import _system_info
from .basic import _ping_storage
from .cola import build_history_view, build_queue_view, retry_task
from .common import can_manage, is_owner
from .settings import build_settings_view

log = logging.getLogger("s3_bot.callbacks")


def register(app: Client) -> None:
    @app.on_callback_query()
    async def _router(client: Client, cq: CallbackQuery):
        data = cq.data or ""
        user_id = cq.from_user.id

        try:
            await _dispatch(client, cq, data, user_id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Callback %s falló: %s", data, exc)
            try:
                await cq.answer("⚠️ Algo salió mal. Intenta de nuevo.", show_alert=True)
            except Exception:
                pass


async def _dispatch(client: Client, cq: CallbackQuery, data: str, user_id: int) -> None:
    if data == "noop":
        await cq.answer()
        return

    # ---------------------------------------------------------- menú
    if data.startswith("menu:"):
        await _menu(client, cq, data, user_id)
        return

    # ---------------------------------------------------------- cola
    if data.startswith("cola:"):
        await _cola(client, cq, data, user_id)
        return

    if data.startswith("hist:"):
        parts = data.split(":")
        if len(parts) >= 3 and parts[1] in ("links", "delete"):
            await _history_action(client, cq, parts, user_id)
            return
        page = _int(data.rsplit(":", 1)[-1], 1)
        text, keyboard = build_history_view(user_id, page)
        await cq.answer()
        await _edit(cq, text, keyboard)
        return

    # ---------------------------------------------------------- tareas
    if data.startswith("t:"):
        await _task_action(client, cq, data, user_id)
        return

    # ---------------------------------------------------------- settings
    if data.startswith("set:"):
        await _settings(cq, data, user_id)
        return

    # ---------------------------------------------------------- admin
    if data.startswith("adm:"):
        await _admin(cq, data, user_id)
        return

    await cq.answer()


# ----------------------------------------------------------------------
# Menú.
# ----------------------------------------------------------------------
async def _menu(client: Client, cq: CallbackQuery, data: str, user_id: int) -> None:
    action = data.split(":", 1)[1]
    is_own = is_owner(user_id)

    if action == "home":
        await cq.answer()
        await _edit(
            cq,
            T.welcome_text(user_id, public=not CONFIG.allowed_users, is_owner=is_own,
                           cfg=CONFIG, settings=RUNTIME_SETTINGS),
            T.main_menu_keyboard(is_owner_user=is_own),
        )
        return

    if action.startswith("help"):
        page = _int(action.split(":")[-1], 1)
        await cq.answer()
        await _edit(cq, T.help_text(page), T.help_keyboard(page))
        return

    if action == "cola":
        text, keyboard = build_queue_view(user_id, 1, is_owner_user=is_own)
        await cq.answer()
        await _edit(cq, text, keyboard)
        return

    if action == "hist":
        text, keyboard = build_history_view(user_id, 1)
        await cq.answer()
        await _edit(cq, text, keyboard)
        return

    if action == "status":
        await cq.answer("🔍 Comprobando…")
        try:
            me = await client.get_me()
            bot_line = f"✅ {T.code('@' + (me.username or str(me.id)))} en línea"
        except Exception as exc:  # noqa: BLE001
            bot_line = f"⚠️ {T.esc(T.shorten(str(exc), 60))}"
        ok, detail = await _ping_storage()
        text = T.status_text(
            bot_line=bot_line,
            storage_line=f"{'✅' if ok else '❌'} {T.esc(T.shorten(detail, 80))}",
            settings=RUNTIME_SETTINGS,
            counts=QUEUE.counts(),
            paused=QUEUE.paused,
        )
        keyboard = T.admin_keyboard(QUEUE.paused) if can_manage(user_id) else None
        await _edit(cq, text, keyboard)
        return

    if action in ("stats", "sys") and not can_manage(user_id):
        await cq.answer("⛔️ Solo el owner.", show_alert=True)
        return

    if action == "stats":
        data_stats = storage.global_stats()
        text = T.stats_text(data_stats, storage.top_users(5), QUEUE.counts())
        await cq.answer()
        await _edit(cq, text, T.admin_keyboard(QUEUE.paused))
        return

    if action == "sys":
        await cq.answer("🖥 Leyendo recursos…")
        info = await asyncio.to_thread(_system_info)
        await _edit(cq, T.system_text(info), T.admin_keyboard(QUEUE.paused))
        return

    if action == "set":
        if not can_manage(user_id):
            await cq.answer("⛔️ Solo el owner.", show_alert=True)
            return
        text, keyboard = build_settings_view(1, is_owner_user=True)
        await cq.answer()
        await _edit(cq, text, keyboard)
        return

    await cq.answer()


# ----------------------------------------------------------------------
# Acciones persistentes del historial.
# ----------------------------------------------------------------------
async def _history_action(client: Client, cq: CallbackQuery, parts: list, user_id: int) -> None:
    """Acciones sobre una subida guardada, incluso después de reiniciar."""
    action = parts[1]
    confirmed = len(parts) >= 4 and parts[2] == "yes"
    task_id = parts[3] if confirmed else parts[2]
    row = storage.get_task(task_id, user_id)
    if not row:
        await cq.answer("⌛️ Ese archivo ya no está en el historial.", show_alert=True)
        return
    if action == "links":
        if not row.get("urls"):
            await cq.answer("ℹ️ Los enlaces ya fueron eliminados.", show_alert=True)
            return
        await cq.answer("📄 Enviando enlaces…")
        historical_task = SimpleNamespace(**row)
        historical_task.parts_total = row.get("parts", 0)
        await send_links_txt(client, cq.message.chat.id, historical_task)
        return
    if not row.get("urls") or not row.get("keys"):
        await cq.answer("ℹ️ Este archivo ya fue eliminado.", show_alert=True)
        return
    if not confirmed:
        await cq.answer()
        await _edit(
            cq,
            f"<b>🗑 ¿Borrar este archivo de toDus?</b>\n\n"
            f"📄 {T.code(T.shorten(row['filename'], 44))}\n"
            + T.i("Los enlaces dejarán de funcionar."),
            T.history_delete_confirm_keyboard(task_id),
        )
        return
    historical_task = SimpleNamespace(**row)
    historical_task.deleted_from_s3 = False
    await _delete_from_s3(cq, historical_task)


# ----------------------------------------------------------------------
# Cola.
# ----------------------------------------------------------------------
async def _cola(client: Client, cq: CallbackQuery, data: str, user_id: int) -> None:
    action = data.split(":", 1)[1]
    is_own = is_owner(user_id)

    if action.startswith("page"):
        page = _int(action.split(":")[-1], 1)
        text, keyboard = build_queue_view(user_id, page, is_owner_user=is_own)
        await cq.answer()
        await _edit(cq, text, keyboard)
        return

    if action in ("refresh", "global"):
        global_view = action == "global" and can_manage(user_id)
        text, keyboard = build_queue_view(user_id, 1, is_owner_user=is_own,
                                          global_view=global_view)
        await cq.answer("🔄 Actualizado")
        await _edit(cq, text, keyboard)
        return

    if action.startswith("cancelall"):
        if action == "cancelall:yes":
            if is_owner(user_id):
                count = QUEUE.cancel_all()
                scope = "toda la cola"
            else:
                count = QUEUE.cancel_user(user_id)
                scope = "tus tareas"
            await cq.answer(f"🛑 {count} tarea(s) canceladas")
            text, keyboard = build_queue_view(
                user_id, 1, is_owner_user=is_own,
                global_view=is_owner(user_id) and scope == "toda la cola",
            )
            await _edit(cq, text, keyboard)
            return
        pending = QUEUE.active_tasks() if is_owner(user_id) else QUEUE.user_active_tasks(user_id)
        if not pending:
            await cq.answer("📭 No hay tareas en curso." if is_owner(user_id) else "📭 No tienes tareas en curso.")
            return
        await cq.answer()
        await _edit(
            cq,
            f"<b>🛑 ¿Cancelar {'toda la cola' if is_owner(user_id) else 'tus ' + str(len(pending)) + ' tarea(s)'}?</b>\n\n"
            + "\n".join(
                f"• {T.code(T.shorten(t.filename, 36))} — {T.esc(T.STATUS_LABEL[t.status])}"
                for t in pending[:10]
            ),
            T.cancel_all_keyboard(),
        )
        return

    await cq.answer()


# ----------------------------------------------------------------------
# Acciones sobre una tarea.
# ----------------------------------------------------------------------
async def _task_action(client: Client, cq: CallbackQuery, data: str, user_id: int) -> None:
    parts = data.split(":")
    kind = parts[1]
    # t:delyes:yes:<id>  →  kind='delyes', parts[2]='yes', id en parts[3]
    if kind == "delyes" and len(parts) >= 4 and parts[2] == "yes":
        task_id = parts[3]
    else:
        task_id = parts[-1]

    task = QUEUE.get(task_id)
    if task is None:
        await cq.answer("⌛️ Esa tarea ya no está en memoria (ver /historial).", show_alert=True)
        return
    if task.user_id != user_id and not can_manage(user_id):
        await cq.answer("⛔️ Esa tarea no es tuya.", show_alert=True)
        return

    if kind == "cancel":
        if task.is_final:
            await cq.answer("ℹ️ Esa tarea ya terminó.", show_alert=True)
            return
        QUEUE.cancel(task_id)
        await cq.answer("🛑 Cancelando…")
        await asyncio.sleep(0.4)
        text, keyboard = build_queue_view(user_id, 1, is_owner_user=is_owner(user_id))
        await _edit(cq, text, keyboard)
        return

    if kind == "card":
        await cq.answer()
        await _edit(cq, _task_card(task), T.task_keyboard(task, is_owner_user=can_manage(user_id)))
        return

    if kind == "links":
        await cq.answer("📄 Enviando…")
        await send_links_txt(client, cq.message.chat.id, task)
        return

    if kind == "retry":
        await cq.answer("♻️ Reintentando…")
        ok, detail = await retry_task(client, task, user_id)
        if not ok:
            await cq.answer(detail, show_alert=True)
            return
        text, keyboard = build_queue_view(user_id, 1, is_owner_user=is_owner(user_id))
        await _edit(cq, text, keyboard)
        return

    if kind == "delyes" and len(parts) >= 4 and parts[2] == "yes":
        await _delete_from_s3(cq, task)
        return

    if kind == "delyes":
        if not task.urls:
            await cq.answer("ℹ️ Esa tarea no tiene enlaces.", show_alert=True)
            return
        await cq.answer()
        await _edit(
            cq,
            f"<b>🗑 ¿Borrar de toDus?</b>\n\n"
            f"📄 {T.code(T.shorten(task.filename, 44))}\n"
            f"🧩 {len(task.urls)} archivo(s) en el almacenamiento\n\n"
            + T.i("Los enlaces dejarán de funcionar para quien los tenga."),
            T.delete_confirm_keyboard(task_id),
        )
        return

    if kind == "drop":
        if not can_manage(user_id):
            await cq.answer("⛔️ Solo el owner.", show_alert=True)
            return
        QUEUE.forget(task_id)
        await cq.answer("🧹 Quitada de la lista")
        text, keyboard = build_queue_view(user_id, 1, is_owner_user=True)
        await _edit(cq, text, keyboard)
        return

    await cq.answer()


async def _delete_from_s3(cq: CallbackQuery, task) -> None:
    """Borra los objetos de toDus (se llama desde el botón de confirmación)."""
    await cq.answer("🗑 Borrando…")
    s3 = S3Client(CONFIG.s3)
    ok, failed = await asyncio.to_thread(s3.delete_keys, list(task.keys))
    task.deleted_from_s3 = True
    task.urls = []
    storage.mark_deleted(task.task_id)
    await _edit(
        cq,
        f"<b>🗑 Borrado del almacenamiento</b>\n\n"
        f"📄 {T.code(T.shorten(task.filename, 44))}\n"
        f"✅ Eliminados: {T.code(ok)} · ❌ Fallidos: {T.code(failed)}\n\n"
        + T.i("Los enlaces ya no están disponibles."),
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🗂 Mi historial", callback_data="menu:hist")],
            [InlineKeyboardButton("🏠 Menú", callback_data="menu:home")],
        ]),
    )


def _task_card(task) -> str:
    lines = [
        f"<b>{T.STATUS_ICON[task.status]} Detalle de la tarea</b>",
        "",
        f"🆔 {T.code(task.task_id)}",
        f"📄 {T.code(T.shorten(task.filename, 48))}",
        f"📦 {T.esc(P.human_size_compact(task.file_size))}",
        f"👤 {T.esc('@' + task.username if task.username else str(task.user_id))}",
        f"🧭 Estado: {T.esc(T.STATUS_LABEL[task.status])}"
        + (f" · {T.esc(task.phase)}" if task.phase else ""),
        f"⏱️ Duración: {T.esc(P.human_time(task.duration()))}",
    ]
    if task.parts_total:
        lines.append(f"🧩 Partes: {T.code(f'{task.parts_done}/{task.parts_total}')}")
    if task.status.is_active:
        lines.append(f"⚡️ {T.esc(P.human_speed(task.speed))} · "
                     f"⏳ {T.esc(P.human_time(task.eta_seconds()))}")
        lines.append(f"📍 Posición en cola: {T.code(task.queue_position or 'ejecutando')}")
    if task.error:
        lines.append(f"\n❗️ {T.esc(T.shorten(task.error, 200))}")
    if task.urls and not task.deleted_from_s3:
        lines.append(f"\n<b>Enlaces ({len(task.urls)}):</b>")
        lines.append(T.pre("\n".join(task.urls[:8])))
    return T.cut("\n".join(lines))


# ----------------------------------------------------------------------
# Settings y admin.
# ----------------------------------------------------------------------
async def _settings(cq: CallbackQuery, data: str, user_id: int) -> None:
    parts = data.split(":")
    kind = parts[1]

    if kind == "view":
        await cq.answer("ℹ️ Solo el owner puede cambiar la configuración.")
        return

    if not can_manage(user_id):
        await cq.answer("⛔️ Solo el owner puede modificar la configuración.", show_alert=True)
        return

    page = 1
    if kind == "cycle":
        key = parts[-1]
        try:
            value = RUNTIME_SETTINGS.cycle(key)
            page = RUNTIME_SETTINGS.page_for(key)
            await cq.answer(f"✅ {key} = {value}")
        except KeyError:
            await cq.answer("⚠️ Opción inválida.", show_alert=True)
            return
    elif kind == "page":
        page = _int(parts[-1], 1)
        await cq.answer()
    elif kind == "reset":
        RUNTIME_SETTINGS.reset()
        await cq.answer("♻️ Valores del .env restaurados")
    elif kind == "refresh":
        await cq.answer("🔄")

    text, keyboard = build_settings_view(page, is_owner_user=True)
    await _edit(cq, text, keyboard)


async def _admin(cq: CallbackQuery, data: str, user_id: int) -> None:
    if not can_manage(user_id):
        await cq.answer("⛔️ Solo el owner.", show_alert=True)
        return
    action = data.split(":", 1)[1]
    if action == "pause":
        QUEUE.paused = True
        QUEUE.paused_by = user_id
        QUEUE.wake()
        await cq.answer("⏸ Cola pausada")
    elif action == "resume":
        QUEUE.paused = False
        QUEUE.paused_by = None
        QUEUE.wake()
        await cq.answer("▶️ Cola reanudada")
    else:
        await cq.answer()
        return
    await _edit(cq, T.status_text(
        bot_line="✅ en línea",
        storage_line="—",
        settings=RUNTIME_SETTINGS,
        counts=QUEUE.counts(),
        paused=QUEUE.paused,
    ), T.admin_keyboard(QUEUE.paused))


# ----------------------------------------------------------------------
# Utilidades.
# ----------------------------------------------------------------------
async def _edit(cq: CallbackQuery, text: str, keyboard=None) -> None:
    message = cq.message
    if message is None:
        return
    try:
        await message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
    except Exception as exc:  # noqa: BLE001
        if "not modified" in str(exc).lower():
            return
        log.debug("No se pudo editar el mensaje del callback: %s", exc)


def _int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
