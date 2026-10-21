"""
group_access.py
===============
Verificación obligatoria de que el usuario está unido al grupo requerido.

Se comprueba cada interacción privada y se escuchan los cambios de estado del
grupo para cancelar en el acto las tareas en cola o en curso cuando alguien
sale o deja de pertenecer a él. Los chats grupales se ignoran silenciosamente.
"""
from __future__ import annotations

import logging
from typing import Optional, Tuple

from pyrogram import Client, StopPropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import CONFIG
from ..queue import QUEUE

log = logging.getLogger("s3_bot.group_access")
VERIFY_CALLBACK = "group_access:verify"
EXIT_REASON = "dejó de estar unido al grupo obligatorio @todusupload"


def is_user_in_group(status, is_member: bool = False) -> bool:
    """Acepta miembros, administradores/owner y restringidos que sigan dentro."""
    name = getattr(status, "name", None) or str(status).rsplit(".", 1)[-1]
    name = str(name).upper()
    return name in {"MEMBER", "ADMINISTRATOR", "OWNER", "CREATOR"} or (
        name == "RESTRICTED" and bool(is_member)
    )


async def verify_group_join(client: Client, user_id: int) -> Tuple[bool, bool]:
    """Devuelve (está_unido, consulta_exitosa); los errores no confirman acceso."""
    if CONFIG.required_group_id is None:
        return False, False
    try:
        member = await client.get_chat_member(CONFIG.required_group_id, user_id)
    except Exception as exc:  # noqa: BLE001 - fallar cerrado ante errores de Telegram
        if type(exc).__name__ == "UserNotParticipant":
            return False, True
        log.debug("No se pudo verificar el grupo para usuario %s: %s", user_id, type(exc).__name__)
        return False, False
    return is_user_in_group(
        getattr(member, "status", ""),
        bool(getattr(member, "is_member", False)),
    ), True


def group_access_text(
    *, verification_failed: bool = False, cancelled_count: int = 0
) -> str:
    if verification_failed:
        opening = (
            "⚠️ <b>No pude verificar si sigues unido al grupo</b>\n\n"
            "Por seguridad, el bot no puede continuar hasta confirmarlo. "
            "Inténtalo otra vez en unos segundos.\n\n"
        )
    else:
        opening = (
            "🔒 <b>Unirte al grupo es obligatorio para usar este bot</b>\n\n"
            "Debes seguir unido a @todusupload. Si sales, se bloquea el acceso y "
            "se cancelan tus tareas pendientes o en curso.\n\n"
        )
    if cancelled_count:
        opening += cancelled_tasks_text(cancelled_count) + "\n\n"
    if CONFIG.required_group_url:
        opening += "Únete al grupo y luego pulsa «Ya me uní» para verificarlo."
    else:
        opening += "El administrador aún no configuró el enlace del grupo."
    return opening


def group_access_keyboard() -> InlineKeyboardMarkup:
    rows = []
    if CONFIG.required_group_url:
        rows.append([
            InlineKeyboardButton("👥 Unirme al grupo", url=CONFIG.required_group_url)
        ])
    rows.append([
        InlineKeyboardButton("✅ Ya me uní", callback_data=VERIFY_CALLBACK)
    ])
    return InlineKeyboardMarkup(rows)


def cancelled_tasks_text(count: int) -> str:
    noun = "tarea" if count == 1 else "tareas"
    return (
        f"🛑 Marqué {count} {noun} para cancelación porque estaban en cola o en curso "
        "cuando saliste del grupo. Las activas se están deteniendo."
    )


def group_denial_text(
    *, verification_failed: bool = False, cancelled_count: int = 0
) -> str:
    if verification_failed:
        text = (
            "⚠️ <b>No pude verificar si sigues unido al grupo.</b>\n\n"
            "Por seguridad no se añadió la tarea. Inténtalo de nuevo en unos segundos."
        )
    else:
        text = (
            "🔒 <b>Para usar este bot debes seguir unido a @todusupload.</b>\n\n"
            "No se añadió la tarea. Únete al grupo y vuelve a enviar tu solicitud."
        )
    if cancelled_count:
        text += "\n\n" + cancelled_tasks_text(cancelled_count)
    if CONFIG.required_group_url:
        text += f"\n\nGrupo: {CONFIG.required_group_url}"
    return text


def _is_private_chat(chat) -> bool:
    chat_type = getattr(chat, "type", None)
    name = getattr(chat_type, "name", None) or str(chat_type).rsplit(".", 1)[-1]
    return str(name).upper() == "PRIVATE"


def _is_required_group(chat) -> bool:
    if chat is None:
        return False
    group_id = CONFIG.required_group_id
    if isinstance(group_id, int):
        return getattr(chat, "id", None) == group_id
    username = (getattr(chat, "username", None) or "").lstrip("@").casefold()
    return bool(username and username == str(group_id).lstrip("@").casefold())


async def notify_cancelled_tasks(
    client: Client,
    user_id: int,
    count: int,
    *,
    verification_failed: bool = False,
    explanation: Optional[str] = None,
) -> None:
    """Explica por DM por qué se detuvieron las tareas del usuario."""
    if count <= 0:
        return
    if explanation:
        reason = explanation
    elif verification_failed:
        reason = "No pude verificar que siguieras unido al grupo; por seguridad detuve tus tareas."
    else:
        reason = "Dejaste de estar unido al grupo obligatorio @todusupload."
    text = (
        f"🛑 <b>{cancelled_tasks_text(count)}</b>\n\n"
        f"{reason}\nVuelve a unirte y usa el bot por privado para continuar."
    )
    try:
        await client.send_message(
            user_id,
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=group_access_keyboard(),
        )
    except Exception as exc:  # noqa: BLE001 - el usuario pudo bloquear el bot
        log.info("No se pudo avisar por privado al usuario %s: %s", user_id, type(exc).__name__)


async def cancel_user_tasks(
    client: Client,
    user_id: int,
    *,
    reason: str = EXIT_REASON,
    notify: bool = False,
    verification_failed: bool = False,
) -> int:
    count = QUEUE.cancel_user(user_id, reason=reason)
    if notify and count:
        await notify_cancelled_tasks(
            client, user_id, count, verification_failed=verification_failed
        )
    return count


async def handle_group_member_update(client: Client, update: ChatMemberUpdated) -> int:
    """Cancela las tareas cuando el usuario pasa de estar dentro a quedar fuera."""
    if not _is_required_group(update.chat):
        return 0
    old_member = update.old_chat_member
    new_member = update.new_chat_member
    if old_member is not None and not is_user_in_group(
        old_member.status, bool(getattr(old_member, "is_member", False))
    ):
        return 0
    if new_member is not None and is_user_in_group(
        new_member.status, bool(getattr(new_member, "is_member", False))
    ):
        return 0
    changed_member = new_member or old_member
    user = getattr(changed_member, "user", None)
    if user is None:
        return 0
    count = await cancel_user_tasks(client, user.id, notify=True)
    if count:
        log.info("Canceladas %d tareas del usuario %s al salir de @todusupload", count, user.id)
    return count


async def _send_prompt(
    message: Message,
    *,
    verification_failed: bool,
    cancelled_count: int = 0,
) -> None:
    try:
        await message.reply_text(
            group_access_text(
                verification_failed=verification_failed,
                cancelled_count=cancelled_count,
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=group_access_keyboard(),
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo mostrar el aviso de grupo: %s", type(exc).__name__)


async def _edit_callback_prompt(
    client: Client,
    query: CallbackQuery,
    *,
    verification_failed: bool,
    cancelled_count: int = 0,
) -> None:
    text = group_access_text(
        verification_failed=verification_failed,
        cancelled_count=cancelled_count,
    )
    keyboard = group_access_keyboard()
    try:
        if query.message is not None:
            await query.message.edit_text(
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
        else:
            await client.send_message(
                query.from_user.id,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo actualizar el aviso del grupo: %s", type(exc).__name__)


async def _confirm_group_join(client: Client, query: CallbackQuery) -> None:
    allowed, verified = await verify_group_join(client, query.from_user.id)
    cancelled = 0
    if not allowed and verified:
        cancelled = await cancel_user_tasks(client, query.from_user.id)
    try:
        await query.answer(
            "Verificación completada." if allowed else (
                "Aún no apareces unido al grupo." if verified
                else "No pude verificarlo; inténtalo de nuevo en unos segundos."
            ),
            show_alert=not allowed,
        )
    except Exception:  # noqa: BLE001
        pass
    if not allowed:
        await _edit_callback_prompt(
            client,
            query,
            verification_failed=not verified,
            cancelled_count=cancelled,
        )
        raise StopPropagation

    ready_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Abrir el bot", callback_data="menu:home")
    ]])
    try:
        if query.message is not None:
            await query.message.edit_text(
                "✅ <b>Verificación del grupo completada.</b>\n\nYa puedes usar el bot.",
                parse_mode=ParseMode.HTML,
                reply_markup=ready_markup,
            )
        else:
            await client.send_message(
                query.from_user.id,
                "✅ <b>Verificación del grupo completada.</b>\n\nYa puedes usar el bot.",
                parse_mode=ParseMode.HTML,
                reply_markup=ready_markup,
            )
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo mostrar la confirmación del grupo: %s", type(exc).__name__)
    raise StopPropagation


def register(app: Client) -> None:
    """Instala puertas globales y el detector de salida del grupo requerido."""
    @app.on_message(filters.all, group=-100)
    async def _message_gate(client: Client, message: Message):
        # Telegram entrega al bot admin mensajes del grupo; se ignoran sin respuesta.
        if not _is_private_chat(getattr(message, "chat", None)):
            raise StopPropagation
        user = getattr(message, "from_user", None)
        if user is None:
            raise StopPropagation
        allowed, verified = await verify_group_join(client, user.id)
        if allowed:
            return
        cancelled = 0
        if verified:
            cancelled = await cancel_user_tasks(client, user.id)
        await _send_prompt(
            message,
            verification_failed=not verified,
            cancelled_count=cancelled,
        )
        raise StopPropagation

    @app.on_callback_query(group=-100)
    async def _callback_gate(client: Client, query: CallbackQuery):
        if query.message is not None and not _is_private_chat(query.message.chat):
            try:
                await query.answer()
            except Exception:  # noqa: BLE001
                pass
            raise StopPropagation
        if (query.data or "") == VERIFY_CALLBACK:
            await _confirm_group_join(client, query)

        allowed, verified = await verify_group_join(client, query.from_user.id)
        if allowed:
            return
        cancelled = 0
        if verified:
            cancelled = await cancel_user_tasks(client, query.from_user.id)
        try:
            await query.answer(
                "Debes seguir unido al grupo para usar el bot." if verified
                else "No pude verificar si sigues unido al grupo; inténtalo de nuevo.",
                show_alert=True,
            )
        except Exception:  # noqa: BLE001
            pass
        await _edit_callback_prompt(
            client,
            query,
            verification_failed=not verified,
            cancelled_count=cancelled,
        )
        raise StopPropagation

    @app.on_chat_member_updated(group=-100)
    async def _group_member_updated(client: Client, update: ChatMemberUpdated):
        await handle_group_member_update(client, update)
