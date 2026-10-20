"""
callbacks.py
============
Enrutador ÚNICO de los botones inline (formato de ``callback_data`` en
``keyboards.py``).

Reglas que cumple todo botón:
- Se responde UNA sola vez a cada pulsación (``Ctx.answer`` ignora la segunda;
  antes "Reintentando…" + un error posterior fallaba con QUERY_ID_INVALID).
- Se comprueba acceso (autorizado, no baneado) y propiedad de la tarea.
- Los botones de un MENSAJE DE TAREA (ctx ``m``) nunca lo convierten en un
  panel: actúan sobre la tarea y el mensaje sigue siendo el de la tarea.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery

from .. import actions
from .. import keyboards as K
from .. import storage, views
from ..notifier import send_links_txt
from ..queue import QUEUE
from ..runtime_settings import RUNTIME_SETTINGS
from .common import can_manage, is_authorized, is_owner
from . import media

log = logging.getLogger("s3_bot.callbacks")


# ----------------------------------------------------------------------
# Contexto de una pulsación.
# ----------------------------------------------------------------------
class Ctx:
    def __init__(self, client: Client, cq: CallbackQuery) -> None:
        self.client = client
        self.cq = cq
        self.user_id = cq.from_user.id
        self.is_owner = is_owner(self.user_id)
        self.manage = can_manage(self.user_id)
        self._answered = False

    async def answer(self, text: str = "", alert: bool = False) -> None:
        if self._answered:
            return
        self._answered = True
        try:
            await self.cq.answer(text, show_alert=alert)
        except Exception as exc:  # noqa: BLE001
            log.debug("answer falló: %s", exc)

    async def show(self, view) -> None:
        """Edita el mensaje del botón con una vista (texto, teclado)."""
        message = self.cq.message
        if message is None or view is None:
            return
        text, keyboard = view
        try:
            await message.edit_text(
                text, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True, reply_markup=keyboard,
            )
        except Exception as exc:  # noqa: BLE001
            if "not modified" not in str(exc).lower():
                log.warning("No se pudo editar el mensaje del botón: %s", exc)


def register(app: Client) -> None:
    @app.on_callback_query()
    async def _router(client: Client, cq: CallbackQuery):
        ctx = Ctx(client, cq)
        data = K.normalize_legacy(cq.data or "")
        try:
            if await _guard(ctx):
                await dispatch(ctx, data)
        except Exception as exc:  # noqa: BLE001
            log.exception("Callback %s falló: %s", data, exc)
            await ctx.answer("⚠️ Algo salió mal. Inténtalo de nuevo.", alert=True)
        finally:
            await ctx.answer()


async def _guard(ctx: Ctx) -> bool:
    """Acceso: autorizado, no baneado y (en grupos) panel propio."""
    if not is_authorized(ctx.user_id):
        await ctx.answer("🔒 Este bot es privado.", alert=True)
        return False
    if storage.is_banned(ctx.user_id):
        await ctx.answer("⛔️ Tu cuenta está bloqueada.", alert=True)
        return False
    message = ctx.cq.message
    original = getattr(message, "reply_to_message", None)
    asker = getattr(getattr(original, "from_user", None), "id", None)
    if asker and asker != ctx.user_id and not ctx.manage:
        await ctx.answer("Este panel es de otro usuario. Escribe /start para abrir el tuyo.",
                         alert=True)
        return False
    return True


# ----------------------------------------------------------------------
# Despacho.
# ----------------------------------------------------------------------
async def dispatch(ctx: Ctx, data: str) -> None:
    parts = data.split(":")
    head = parts[0]
    if head == "menu":
        await _menu(ctx, parts[1:])
    elif head == "cola":
        await _cola(ctx, parts[1:])
    elif head == "hist":
        await _history(ctx, parts[1:])
    elif head == "t":
        await _task(ctx, parts[1:])
    elif head == "set":
        await _settings(ctx, parts[1:])
    elif head == "adm":
        await _admin(ctx, parts[1:])
    elif head == "media":
        await media.handle_callback(ctx, parts[1:])
    # "noop" y desconocidos: solo se responde (lo hace el finally del router).


async def _owner_only(ctx: Ctx) -> bool:
    if ctx.manage:
        return True
    await ctx.answer("⛔️ Solo el owner.", alert=True)
    return False


def _int(value: Optional[str], default: int = 1) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


# ----------------------------------------------------------------------
# Menú y paneles.
# ----------------------------------------------------------------------
async def _menu(ctx: Ctx, args: list) -> None:
    action = args[0] if args else "home"

    if action == "home":
        name = await views.owner_display_name(ctx.client)
        await ctx.show(views.home_view(ctx.user_id, is_owner_user=ctx.is_owner, admin_name=name))
    elif action == "help":
        await ctx.show(views.help_view(_int(args[1] if len(args) > 1 else None)))
    elif action == "sites":
        await ctx.show(views.sites_view())
    elif action == "cola":
        await ctx.show(views.queue_view(ctx.user_id, "m", 1, is_owner_user=ctx.is_owner))
    elif action == "hist":
        await ctx.show(views.history_view(ctx.user_id, 1))
    elif action == "status":
        await ctx.answer("🔍 Comprobando…")
        await ctx.show(await views.status_view(ctx.client))
    elif action == "quota":
        await ctx.show(views.quota_view(ctx.user_id, is_owner_user=ctx.is_owner))
    elif action == "top":
        await ctx.show(views.top_view(ctx.user_id, is_owner_user=ctx.is_owner))
    elif action in ("admin", "stats", "sys", "set", "logs"):
        if not await _owner_only(ctx):
            return
        if action == "admin":
            await ctx.show(views.admin_view())
        elif action == "stats":
            await ctx.show(views.stats_view())
        elif action == "sys":
            await ctx.answer("🖥 Leyendo recursos…")
            await ctx.show(await views.system_view())
        elif action == "logs":
            log_views = views.logs_views()
            await ctx.show(log_views[0])
            if len(log_views) > 1:
                chat_id = ctx.cq.message.chat.id
                for text, _keyboard in log_views[1:]:
                    await ctx.client.send_message(
                        chat_id,
                        text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
        else:
            await ctx.show(views.settings_view(1, is_owner_user=True))


# ----------------------------------------------------------------------
# Cola.
# ----------------------------------------------------------------------
async def _cola(ctx: Ctx, args: list) -> None:
    if not args:
        return
    sub = args[0]

    if sub in ("m", "g"):
        if sub == "g" and not await _owner_only(ctx):
            return
        await ctx.show(views.queue_view(ctx.user_id, sub, _int(args[1] if len(args) > 1 else None),
                                        is_owner_user=ctx.is_owner))
        return

    if sub in ("cx", "cxy") and len(args) > 1:
        scope = args[1]
        if scope == "g" and not await _owner_only(ctx):
            return
        scope = "g" if scope == "g" else "m"
        if sub == "cx":
            view = views.cancel_all_view(ctx.user_id, scope, is_owner_user=ctx.is_owner)
            if view is None:
                await ctx.answer("📭 No hay tareas en curso.")
                return
            await ctx.show(view)
            return
        count = QUEUE.cancel_all() if scope == "g" else QUEUE.cancel_user(ctx.user_id)
        await ctx.answer(f"🛑 {count} tarea(s) cancelada(s)")
        await ctx.show(views.queue_view(ctx.user_id, scope, 1, is_owner_user=ctx.is_owner))


# ----------------------------------------------------------------------
# Historial (solo paginación; las acciones son de tarea, ctx "h").
# ----------------------------------------------------------------------
async def _history(ctx: Ctx, args: list) -> None:
    if args and args[0] == "p":
        await ctx.show(views.history_view(ctx.user_id, _int(args[1] if len(args) > 1 else None)))


# ----------------------------------------------------------------------
# Acciones sobre una tarea.
# ----------------------------------------------------------------------
async def _task(ctx: Ctx, args: list) -> None:
    if len(args) < 2:
        return
    action, task_id = args[0], args[1]
    ctx_code = args[2] if len(args) > 2 else K.MSG
    kind, page = K.parse_ctx(ctx_code)

    task = actions.find_task(task_id, ctx.user_id, manage=ctx.manage)
    if task is None:
        await ctx.answer("⌛️ No encuentro esa tarea (ya no está en el historial).", alert=True)
        return

    async def refresh() -> None:
        """Vuelve a pintar el sitio desde donde se pulsó el botón."""
        if kind == "m":
            await ctx.show(views.task_message(task))
        else:
            await ctx.show(views.card_view(task, ctx_code, ctx.user_id, manage=ctx.manage))

    if action == "card":
        await refresh()

    elif action == "back":
        await refresh()

    elif action == "cancel":
        if task.is_final:
            await ctx.answer("ℹ️ Esa tarea ya terminó.", alert=True)
            return
        cancelled = QUEUE.cancel(task_id)
        await ctx.answer("🛑 Cancelando…" if cancelled else "ℹ️ La tarea ya terminó.", alert=not cancelled)
        if kind != "m":
            await asyncio.sleep(0.4)
            await refresh()

    elif action == "links":
        if not task.has_links:
            await ctx.answer("ℹ️ Esta tarea no tiene enlaces (¿ya la borraste?).", alert=True)
            return
        await ctx.answer("📄 Enviando enlaces…")
        await send_links_txt(ctx.client, ctx.cq.message.chat.id, task)

    elif action == "retry":
        ok, detail = await actions.retry_task(ctx.client, task)
        await ctx.answer(detail, alert=not ok)
        if ok:
            if kind == "m":
                # El mensaje fallido ya no ofrece reintentar (la copia tiene el suyo).
                await ctx.show(views.task_message(task))
            else:
                await refresh()

    elif action == "del":
        if not task.has_links or not task.keys:
            await ctx.answer("ℹ️ No hay nada que borrar.", alert=True)
            return
        await ctx.show(views.delete_confirm_view(task, ctx_code))

    elif action == "delok":
        if not task.has_links or not task.keys:
            await ctx.answer("ℹ️ No hay nada que borrar.", alert=True)
            return
        await ctx.answer("🗑 Borrando…")
        deleted, failed = await actions.delete_task_files(task)
        if failed:
            # Ya se respondió al botón; las partes que fallaron conservan su
            # enlace en la ficha para poder repetir el borrado.
            log.warning("[%s] borrado parcial: %d ok, %d fallidos", task_id, deleted, failed)
        await refresh()

    elif action == "drop":
        if not await _owner_only(ctx):
            return
        QUEUE.forget(task_id)
        await ctx.answer("🧹 Quitada de la lista")
        scope = "g" if kind == "g" else "m"
        await ctx.show(views.queue_view(ctx.user_id, scope, page, is_owner_user=True))


# ----------------------------------------------------------------------
# Ajustes y administración.
# ----------------------------------------------------------------------
async def _settings(ctx: Ctx, args: list) -> None:
    if not args or not await _owner_only(ctx):
        return
    kind = args[0]
    page = 1
    if kind == "cycle" and len(args) > 1:
        key = args[1]
        try:
            value = RUNTIME_SETTINGS.cycle(key)
        except KeyError:
            await ctx.answer("⚠️ Opción inválida.", alert=True)
            return
        page = RUNTIME_SETTINGS.page_for(key)
        await ctx.answer(f"✅ {key} = {value}")
    elif kind == "page":
        page = _int(args[1] if len(args) > 1 else None)
    elif kind == "reset":
        RUNTIME_SETTINGS.reset()
        await ctx.answer("♻️ Valores del .env restaurados")
    await ctx.show(views.settings_view(page, is_owner_user=True))


async def _admin(ctx: Ctx, args: list) -> None:
    if not args or not await _owner_only(ctx):
        return
    if args[0] == "pause":
        QUEUE.paused, QUEUE.paused_by = True, ctx.user_id
        await ctx.answer("⏸ Cola pausada")
    elif args[0] == "resume":
        QUEUE.paused, QUEUE.paused_by = False, None
        await ctx.answer("▶️ Cola reanudada")
    else:
        return
    QUEUE.wake()
    await ctx.show(views.admin_view())
