"""
keyboards.py
============
TODOS los teclados inline y el formato de ``callback_data`` en un único sitio.

Dos tipos de mensajes, con reglas distintas
-------------------------------------------
1. **Mensaje de tarea** (en cola / progreso / resultado / error): sus botones
   solo actúan sobre ESA tarea. Nunca navegan (no hay "Menú" ni "Mi cola"):
   así el mensaje no se convierte en otra cosa mientras el notificador lo
   sigue actualizando.
2. **Paneles** (menú, /cola, /historial, estado, ajustes…): se navegan
   editando el mismo mensaje y todos acaban en 🏠 Inicio.

Formato de ``callback_data`` (máx. 64 bytes)
--------------------------------------------
    noop
    menu:<home|cola|hist|status|quota|top|admin|stats|sys|set>
    menu:help:<página>
    cola:<m|g>:<página>          lista propia (m) / global (g, solo owner)
    cola:cx:<m|g>                pedir confirmación de "cancelar todas"
    cola:cxy:<m|g>               confirmar
    hist:p:<página>
    t:<acción>:<task_id>:<ctx>   acciones sobre una tarea (ver abajo)
    set:cycle:<clave> · set:page:<n> · set:reset
    adm:<pause|resume>

Acciones de tarea: card · back · cancel · retry · links · del · delok · drop
Contexto (ctx) = dónde está el botón y a dónde volver:
    m        el propio mensaje de la tarea
    q<n>     ficha abierta desde /cola, página n
    g<n>     ficha abierta desde la cola global, página n
    h<n>     ficha abierta desde /historial, página n
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup

from .queue import STATUS_ICON, TaskStatus
from .texts import HELP_PAGE_COUNT, shorten

MSG = "m"


# ----------------------------------------------------------------------
# callback_data.
# ----------------------------------------------------------------------
def cb_task(action: str, task_id: str, ctx: str = MSG) -> str:
    return f"t:{action}:{task_id}:{ctx}"


def media_format_keyboard(token: str) -> Markup:
    return Markup([
        [Btn("🎬 MP4", callback_data=f"media:format:{token}:mp4"),
         Btn("🎵 MP3", callback_data=f"media:format:{token}:mp3")],
        [Btn("🎧 M4A", callback_data=f"media:format:{token}:m4a"),
         Btn("🎼 OPUS", callback_data=f"media:format:{token}:opus")],
    ])


def media_quality_keyboard(token: str, qualities: Optional[Sequence[str]] = None) -> Markup:
    values = [str(q) for q in (
        qualities if qualities is not None else ("360", "480", "720", "1080", "1440")
    )]
    rows = []
    for start in range(0, len(values), 3):
        rows.append([
            Btn(f"{quality}p", callback_data=f"media:quality:{token}:{quality}")
            for quality in values[start:start + 3]
        ])
    rows.append([Btn("Máxima", callback_data=f"media:quality:{token}:max")])
    return Markup(rows)


def cb_cola(scope: str, page: int = 1) -> str:
    return f"cola:{scope}:{page}"


def cb_hist(page: int = 1) -> str:
    return f"hist:p:{page}"


def parse_ctx(ctx: str) -> Tuple[str, int]:
    """'q3' -> ('q', 3) · 'm' -> ('m', 1) · basura -> ('m', 1)."""
    kind = ctx[:1] if ctx[:1] in ("m", "q", "g", "h") else MSG
    try:
        page = max(1, int(ctx[1:])) if len(ctx) > 1 else 1
    except ValueError:
        page = 1
    return kind, page


def normalize_legacy(data: str) -> str:
    """
    Traduce el ``callback_data`` de versiones anteriores al formato actual, para
    que los botones de mensajes viejos que siguen en los chats no queden muertos.
    """
    parts = data.split(":")
    head = parts[0]
    if head == "cola" and len(parts) >= 2:
        sub = parts[1]
        if sub == "refresh":
            return cb_cola("m", 1)
        if sub == "global":
            return cb_cola("g", 1)
        if sub == "page" and len(parts) >= 3:
            return cb_cola("m", int(parts[2]) if parts[2].isdigit() else 1)
        if sub == "cancelall":
            return f"cola:{'cxy' if parts[-1] == 'yes' else 'cx'}:m"
    if head == "hist" and len(parts) >= 3:
        if parts[1] == "page":
            return cb_hist(int(parts[2]) if parts[2].isdigit() else 1)
        if parts[1] == "links":
            return cb_task("links", parts[2], "h1")
        if parts[1] == "delete":
            if parts[2] == "yes" and len(parts) >= 4:
                return cb_task("delok", parts[3], "h1")
            return cb_task("del", parts[2], "h1")
    if head == "t" and len(parts) >= 3 and len(parts) <= 4 and parts[1] in (
        "cancel", "card", "retry", "links", "drop", "delyes"
    ):
        action, task_id = parts[1], parts[-1]
        if action == "delyes":
            if len(parts) == 4 and parts[2] == "yes":
                return cb_task("delok", parts[3], MSG)
            return cb_task("del", task_id, MSG)
        if len(parts) == 3:        # formato antiguo, sin contexto
            return cb_task(action, task_id, "q1" if action in ("card", "drop") else MSG)
    return data


# ----------------------------------------------------------------------
# Piezas reutilizables.
# ----------------------------------------------------------------------
def home_row() -> List[Btn]:
    return [Btn("🏠 Inicio", callback_data="menu:home")]


def panel_keyboard(*, refresh: Optional[str] = None,
                   back: Optional[Tuple[str, str]] = None) -> Markup:
    """Teclado estándar de un panel: [🔄 Actualizar] [⬅️ <atrás>] / [🏠 Inicio]."""
    top: List[Btn] = []
    if refresh:
        top.append(Btn("🔄 Actualizar", callback_data=refresh))
    if back:
        top.append(Btn(f"⬅️ {back[0]}", callback_data=back[1]))
    rows = [top] if top else []
    rows.append(home_row())
    return Markup(rows)


def _nav_row(prefix_fn, page: int, pages: int) -> List[Btn]:
    nav: List[Btn] = []
    if page > 1:
        nav.append(Btn("⬅️", callback_data=prefix_fn(page - 1)))
    nav.append(Btn(f"{page}/{pages}", callback_data="noop"))
    if page < pages:
        nav.append(Btn("➡️", callback_data=prefix_fn(page + 1)))
    return nav


# ----------------------------------------------------------------------
# Menú principal y paneles sencillos.
# ----------------------------------------------------------------------
def main_menu_keyboard(*, is_owner_user: bool) -> Markup:
    rows = [
        [Btn("📋 Mi cola", callback_data="menu:cola"),
         Btn("🗂 Historial", callback_data="menu:hist")],
        [Btn("📊 Mi cuota", callback_data="menu:quota"),
         Btn("🏆 Top usuarios", callback_data="menu:top")],
        [Btn("📡 Estado", callback_data="menu:status"),
         Btn("📖 Ayuda", callback_data="menu:help:1")],
        [Btn("🌐 Sitios compatibles", callback_data="menu:sites")],
    ]
    if is_owner_user:
        rows.append([Btn("🛠 Administración", callback_data="menu:admin")])
    return Markup(rows)


def help_keyboard(page: int) -> Markup:
    return Markup([
        _nav_row(lambda n: f"menu:help:{n}", page, HELP_PAGE_COUNT),
        home_row(),
    ])


def admin_keyboard(paused: bool) -> Markup:
    toggle = (
        Btn("▶️ Reanudar cola", callback_data="adm:resume") if paused
        else Btn("⏸ Pausar cola", callback_data="adm:pause")
    )
    return Markup([
        [Btn("📈 Estadísticas", callback_data="menu:stats"),
         Btn("🖥 Sistema", callback_data="menu:sys")],
        [Btn("🌍 Cola global", callback_data=cb_cola("g", 1)),
         Btn("⚙️ Ajustes", callback_data="menu:set")],
        [toggle],
        home_row(),
    ])


# ----------------------------------------------------------------------
# Acciones de una tarea (compartidas por el mensaje y la ficha).
# ----------------------------------------------------------------------
def _task_action_rows(task, ctx: str) -> List[List[Btn]]:
    rows: List[List[Btn]] = []
    if not task.is_final:
        rows.append([Btn("🛑 Cancelar", callback_data=cb_task("cancel", task.task_id, ctx))])
    elif task.status == TaskStatus.COMPLETED:
        if task.has_links:
            first = [Btn("📄 Enlaces .txt", callback_data=cb_task("links", task.task_id, ctx))]
            if len(task.urls) == 1:
                first.append(Btn("🔗 Abrir", url=task.urls[0]))
            rows.append(first)
            rows.append([Btn("🗑 Borrar de toDus",
                             callback_data=cb_task("del", task.task_id, ctx))])
    elif task.is_retryable and not getattr(task, "retried_as", None):
        rows.append([Btn("🔁 Reintentar", callback_data=cb_task("retry", task.task_id, ctx))])
    return rows


def task_message_keyboard(task) -> Optional[Markup]:
    """Botones del mensaje de la tarea. Solo acciones, nada de navegación."""
    rows = _task_action_rows(task, MSG)
    return Markup(rows) if rows else None


def task_card_keyboard(task, ctx: str, *, can_drop: bool = False) -> Markup:
    """Ficha de una tarea abierta desde /cola o /historial."""
    kind, page = parse_ctx(ctx)
    rows = _task_action_rows(task, ctx)
    if can_drop and task.is_final and kind in ("q", "g"):
        rows.append([Btn("🧹 Quitar de la lista",
                         callback_data=cb_task("drop", task.task_id, ctx))])
    if kind == "h":
        rows.append([Btn("⬅️ Historial", callback_data=cb_hist(page))])
    else:
        scope = "g" if kind == "g" else "m"
        rows.append([Btn("⬅️ Cola", callback_data=cb_cola(scope, page))])
    rows.append(home_row())
    return Markup(rows)


def confirm_delete_keyboard(task_id: str, ctx: str) -> Markup:
    return Markup([[
        Btn("✅ Sí, borrar", callback_data=cb_task("delok", task_id, ctx)),
        Btn("↩️ No", callback_data=cb_task("back", task_id, ctx)),
    ]])


def confirm_cancel_all_keyboard(scope: str) -> Markup:
    return Markup([[
        Btn("✅ Sí, cancelar", callback_data=f"cola:cxy:{scope}"),
        Btn("↩️ No", callback_data=cb_cola(scope, 1)),
    ]])


# ----------------------------------------------------------------------
# Listas: cola e historial (un botón por elemento → su ficha).
# ----------------------------------------------------------------------
def queue_keyboard(tasks: Sequence, page: int, per_page: int, *,
                   scope: str, is_owner_user: bool) -> Markup:
    ctx = f"{scope}{page}"
    rows: List[List[Btn]] = []
    start = (page - 1) * per_page
    for idx, task in enumerate(tasks[start : start + per_page], start=start + 1):
        label = f"{idx}. {STATUS_ICON.get(task.status, '❔')} {shorten(task.filename, 30)}"
        rows.append([Btn(label, callback_data=cb_task("card", task.task_id, ctx))])

    pages = max(1, (len(tasks) + per_page - 1) // per_page)
    if pages > 1:
        rows.append(_nav_row(lambda n: cb_cola(scope, n), page, pages))

    actions = [Btn("🔄 Actualizar", callback_data=cb_cola(scope, page))]
    if any(not t.is_final for t in tasks):
        actions.append(Btn("🛑 Cancelar todas" if scope == "m" else "🛑 Cancelar todo",
                           callback_data=f"cola:cx:{scope}"))
    rows.append(actions)
    if is_owner_user:
        rows.append([
            Btn("🌍 Cola global", callback_data=cb_cola("g", 1)) if scope == "m"
            else Btn("👤 Mi cola", callback_data=cb_cola("m", 1))
        ])
    rows.append(home_row())
    return Markup(rows)


def history_keyboard(rows_data: Sequence[dict], page: int, per_page: int,
                     *, pages: Optional[int] = None, paged: bool = True) -> Markup:
    rows: List[List[Btn]] = []
    start = (page - 1) * per_page
    for idx, row in enumerate(rows_data[start : start + per_page], start=start + 1):
        icon = "🗑" if row.get("deleted_at") else "✅"
        label = f"{idx}. {icon} {shorten(row['filename'], 30)}"
        rows.append([Btn(label, callback_data=cb_task("card", row["task_id"], f"h{page}"))])
    total = pages or max(1, (len(rows_data) + per_page - 1) // per_page)
    if paged and total > 1:
        rows.append(_nav_row(cb_hist, page, total))
    rows.append(home_row())
    return Markup(rows)


# ----------------------------------------------------------------------
# Ajustes.
# ----------------------------------------------------------------------
def settings_keyboard(keys: Sequence[str], values: dict, labels: dict, units: dict,
                      page: int, pages: int, *, is_owner_user: bool) -> Markup:
    rows: List[List[Btn]] = []
    if is_owner_user:
        for key in keys:
            unit = units.get(key, "")
            value = values[key]
            shown = ("sí" if value else "no") if isinstance(value, bool) else value
            text = f"{labels[key]}: {shown} {unit}".strip()
            rows.append([Btn(text, callback_data=f"set:cycle:{key}")])
    rows.append(_nav_row(lambda n: f"set:page:{n}", page, pages))
    if is_owner_user:
        rows.append([
            Btn("♻️ Valores del .env", callback_data="set:reset"),
            Btn("🛠 Admin", callback_data="menu:admin"),
        ])
    rows.append(home_row())
    return Markup(rows)
