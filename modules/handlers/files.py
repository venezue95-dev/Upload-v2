"""
files.py
========
Vista unificada de archivos subidos y borrado seguro por posiciones.

Los usuarios ven únicamente sus archivos. El owner ve todos y puede borrar
selecciones concretas o todo excepto sus propias subidas.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import storage, texts as T
from .common import AUTHORIZED, is_owner, reply

PER_PAGE = 8


def rows_for(user_id: int, *, owner: Optional[bool] = None, limit: int = 500) -> List[dict]:
    """Devuelve los archivos todavía disponibles para el ámbito del usuario."""
    if owner is None:
        owner = is_owner(user_id)
    return storage.uploaded_files(None if owner else user_id, limit=limit)


def build_files_view(user_id: int, page: int = 1) -> Tuple[str, object]:
    owner = is_owner(user_id)
    rows = rows_for(user_id, owner=owner)
    pages = max(1, (len(rows) + PER_PAGE - 1) // PER_PAGE)
    page = max(1, min(page, pages))
    return (
        T.files_text(rows, page, PER_PAGE, is_owner_user=owner),
        T.files_keyboard(page, pages, rows, is_owner_user=owner),
    )


def parse_positions(raw: str, total: int) -> List[int]:
    """Convierte `1, 3 5` en posiciones únicas válidas, conservando el orden."""
    result: List[int] = []
    for token in re.split(r"[\s,;]+", raw.strip()):
        if not token or not token.isdigit():
            continue
        position = int(token)
        if 1 <= position <= total and position not in result:
            result.append(position)
    return result


def selected_rows(rows: Sequence[dict], positions: Iterable[int]) -> List[dict]:
    return [rows[position - 1] for position in positions if 1 <= position <= len(rows)]


def deletion_scope(rows: Sequence[dict], user_id: int, *, all_files: bool) -> List[dict]:
    """Aplica la regla de borrado total: el owner nunca borra sus archivos."""
    if all_files and is_owner(user_id):
        owner_id = int(user_id)
        return [row for row in rows if int(row.get("user_id", 0)) != owner_id]
    return list(rows)


def _selection_text(rows: Sequence[dict], title: str) -> str:
    lines = [f"<b>{title}</b>", ""]
    for row in rows[:15]:
        lines.append(
            f"• {T.code(T.shorten(row.get('filename') or 'archivo', 44))}"
            f" · {T.esc(row.get('username') or 'usuario')}"
        )
    if len(rows) > 15:
        lines.append(T.i(f"… y {len(rows) - 15} archivo(s) más"))
    return T.cut("\n".join(lines))


def register(app: Client) -> None:
    @app.on_message(filters.command(["archivos", "misarchivos", "files"]) & AUTHORIZED)
    async def _files(client: Client, message: Message):
        text, keyboard = build_files_view(message.from_user.id)
        await reply(message, text, keyboard)

    @app.on_message(filters.command(["eliminar", "borrar"]) & AUTHORIZED)
    async def _delete(client: Client, message: Message):
        user_id = message.from_user.id
        rows = rows_for(user_id)
        argument = (message.text or "").partition(" ")[2].strip().lower()
        if not rows:
            await reply(message, "📭 No tienes archivos disponibles para eliminar." if not is_owner(user_id)
                        else "📭 No hay archivos disponibles para eliminar.")
            return
        if argument in ("todo", "todos", "all"):
            targets = deletion_scope(rows, user_id, all_files=True)
            if not targets:
                await reply(message, "✅ No hay archivos de otros usuarios para eliminar. Tus archivos están protegidos.")
                return
            await reply(
                message,
                _selection_text(
                    targets,
                    "🧹 ¿Confirmas la eliminación total?" if is_owner(user_id)
                    else "🧹 ¿Confirmas eliminar todos tus archivos?",
                ) + "\n\n" + T.i("Esta acción eliminará los enlaces del almacenamiento y no se puede deshacer."),
                keyboard=T.files_delete_all_confirm_keyboard(is_owner_user=is_owner(user_id)),
            )
            return

        positions = parse_positions(argument, len(rows))
        if not positions:
            await reply(
                message,
                "Uso: " + T.code("/eliminar 1,3,5") + "\n"
                + T.i("Las posiciones corresponden a la lista de /archivos. También puedes usar /eliminar todo."),
            )
            return
        targets = selected_rows(rows, positions)
        await reply(
            message,
            _selection_text(targets, f"🗑 ¿Eliminar {len(targets)} archivo(s) seleccionado(s)?")
            + "\n\n" + T.i("Los enlaces dejarán de funcionar."),
            keyboard=T.files_delete_positions_confirm_keyboard(positions),
        )
