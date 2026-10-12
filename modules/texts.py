"""
texts.py
========
Todos los textos (HTML) y teclados inline del bot, centralizados para
que la interfaz sea coherente y fácil de retocar.

Reglas de estilo:
- Títulos con emoji + <b>, datos en <code>, notas en <i>.
- Los enlaces se muestran en <pre> para que Telegram ofrezca el botón
  "copiar" y se puedan copiar todos de una sola vez.
- Cada mensaje se mantiene por debajo del límite de 4096 caracteres.
"""

from __future__ import annotations

import html
import re
from typing import List, Optional, Sequence, TYPE_CHECKING

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from . import progress as P
from .queue import STATUS_ICON, STATUS_LABEL, TaskStatus

if TYPE_CHECKING:  # pragma: no cover
    from .queue import UploadTask

TG_LIMIT = 4000          # margen sobre el límite real de Telegram (4096)
MAX_LISTED_URLS = 40     # máximo de enlaces listados en un mensaje


# ----------------------------------------------------------------------
# Helpers de marcado.
# ----------------------------------------------------------------------
def esc(value) -> str:
    return html.escape(str(value if value is not None else ""), quote=False)


def b(value) -> str:
    return f"<b>{esc(value)}</b>"


def i(value) -> str:
    return f"<i>{esc(value)}</i>"


def code(value) -> str:
    return f"<code>{esc(value)}</code>"


def pre(value) -> str:
    return f"<pre>{esc(value)}</pre>"


def display_number(value) -> str:
    """Muestra 2.0 como 2, conservando decimales significativos."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _bold_non_link_text(text: str) -> str:
    """Pone en negrita el texto visible, dejando intactos los enlaces en <pre>."""
    parts = re.split(r"(<[^>]+>)", text)
    result: List[str] = []
    in_pre = False
    in_code = False
    bold_depth = 0
    for part in parts:
        if not part:
            continue
        if part.startswith("<") and part.endswith(">"):
            tag = part.lower()
            if tag.startswith("<pre"):
                in_pre = True
            elif tag.startswith("</pre"):
                in_pre = False
            elif tag.startswith("<code") and not tag.startswith("</"):
                in_code = True
            elif tag.startswith("</code"):
                in_code = False
            elif tag.startswith("<b") and not tag.startswith("</"):
                bold_depth += 1
            elif tag.startswith("</b"):
                bold_depth = max(0, bold_depth - 1)
            result.append(part)
        elif in_pre or in_code or bold_depth or not part.strip():
            result.append(part)
        else:
            result.append(f"<b>{part}</b>")
    return "".join(result)


def cut(text: str, limit: int = TG_LIMIT) -> str:
    """Recorta y estiliza un mensaje respetando el límite de Telegram."""
    text = _bold_non_link_text(text)
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n<i>… (recortado)</i>"


def shorten(name: str, limit: int = 42) -> str:
    if len(name) <= limit:
        return name
    keep = max(6, limit - 3)
    return name[: keep // 2] + "…" + name[-keep // 2 :]


# ----------------------------------------------------------------------
# Fichas de tarea.
# ----------------------------------------------------------------------
def status_badge(status: TaskStatus) -> str:
    return f"{STATUS_ICON.get(status, '❔')} {STATUS_LABEL.get(status, status.value)}"


def task_line(task: "UploadTask", index: Optional[int] = None) -> str:
    head = f"{index}. " if index is not None else ""
    line = (
        f"{head}{STATUS_ICON.get(task.status, '❔')} "
        f"{code(shorten(task.filename))} "
        f"<i>({P.human_size_compact(task.file_size)})</i>"
    )
    extra: List[str] = []
    if task.parts_total > 1:
        extra.append(f"🧩 {task.parts_done}/{task.parts_total}")
    if task.status in (TaskStatus.DOWNLOADING, TaskStatus.UPLOADING):
        extra.append(P.percent_str(task.progress_percent()))
        current_speed = (
            task.download_speed
            if task.status == TaskStatus.DOWNLOADING
            else task.upload_speed
        )
        extra.append(P.human_speed(current_speed))
        extra.append(f"ETA {P.human_time(task.eta_seconds())}")
    if task.status == TaskStatus.QUEUED and task.queue_position:
        extra.append(f"#{task.queue_position} en cola")
    if extra:
        line += "\n     " + " · ".join(extra)
    if task.status == TaskStatus.FAILED and task.error:
        line += f"\n     ❗️<i>{esc(shorten(task.error, 90))}</i>"
    return line


# ----------------------------------------------------------------------
# Bienvenida / ayuda.
# ----------------------------------------------------------------------
def welcome_text(user_id: int, *, public: bool, is_owner: bool, cfg,
                 settings, admin_name: str = "") -> str:
    modo = "🌍 público" if public else "🔒 privado"
    rol = "👑 owner" if is_owner else "👤 usuario"
    return cut(
        f"<b>☁️ toDus Upload</b>\n"
        f"<i>Convierte cualquier archivo o enlace en un enlace de descarga directa.</i>\n\n"
        f"<b>👤 Tu cuenta</b>\n"
        f"• ID: {code(user_id)}\n"
        f"• Rol: {esc(rol)} · Modo: {esc(modo)}\n"
        f"• 👑 Administrador: {b(admin_name or str(cfg.owner_id or 'no configurado'))}\n\n"
        f"<b>🚀 Cómo usarlo</b>\n"
        f"1️⃣ Reenvía (o envía) un <b>documento / vídeo / audio</b>.\n"
        f"2️⃣ O pega uno o varios <b>enlaces http(s)</b> directos.\n"
        f"3️⃣ El bot te da el <b>enlace directo</b> cuando termina.\n\n"
        f"<b>⚙️ Ajustes actuales</b>\n"
        f"• Máx. por archivo: {code(P.human_size_compact(settings.max_file_mb * 1024 * 1024))}\n"
        f"• Trozo (chunk): {code(str(settings.chunk_size_mb) + ' MB')}\n"
        f"• Cola: {code(str(settings.max_concurrent_tasks) + ' tareas a la vez')} · "
        f"{code(str(settings.max_active_per_user) + ' por usuario')}\n\n"
        f"<b>🧭 Comandos</b>\n"
        f"/cola — tus tareas y la posición en la cola\n"
        f"/historial — tus últimas subidas (con enlaces)\n"
        f"/cancelar — cancelar todo lo tuyo\n"
        f"/status — estado del bot y del almacenamiento\n"
        f"/settings — configuración (owner)\n"
        f"/help — ayuda detallada"
    )


HELP_PAGES = {
    1: (
        "<b>📖 Ayuda 1/3 · Lo básico</b>\n\n"
        "<b>Subir desde Telegram</b>\n"
        "Reenvía un documento, vídeo o audio al chat. Si el archivo supera "
        "el máximo configurado, el bot lo rechaza con un aviso claro.\n\n"
        "<b>Subir desde una URL</b>\n"
        "Pega un enlace directo (http/https). Puedes pegar <b>varios enlaces "
        "en un mismo mensaje</b> y se encolan todos.\n\n"
        "<b>Álbumes</b>\n"
        "Si reenvías un álbum de fotos/vídeos, cada archivo se encola por "
        "separado automáticamente.\n\n"
        "<b>Resultado</b>\n"
        "Cuando termina, recibes el nombre, el tamaño, el número de partes y "
        "los <b>enlaces directos</b> (en un bloque de copiado rápido)."
    ),
    2: (
        "<b>📖 Ayuda 2/3 · La cola</b>\n\n"
        "El bot trabaja con una <b>cola justa</b>: hay un número limitado de "
        "tareas simultáneas en todo el bot (ajustable) y cada usuario tiene su "
        "propio cupo, de forma que <b>nadie se queda bloqueado</b> porque otro "
        "subió 20 archivos.\n\n"
        "<b>Estados</b>\n"
        "⏳ en cola · 📥 descargando · 📤 subiendo · ✅ listo · ❌ error · 🛑 cancelado\n\n"
        "<b>Lo que puedes hacer</b>\n"
        "• /cola → ver posición, ETA y progreso de cada tarea.\n"
        "• Botón <i>Cancelar</i> en cada tarea.\n"
        "• /cancelar → cancelar todas tus tareas de golpe.\n"
        "• Botón <i>Reintentar</i> en las tareas que fallaron.\n"
        "• Botón <i>Enlaces .txt</i> para descargar la lista de enlaces."
    ),
    3: (
        "<b>📖 Ayuda 3/3 · Partes y consejos</b>\n\n"
        "<b>Archivos grandes = varias partes</b>\n"
        "El almacén no admite subidas multiparte, así que un archivo grande se "
        "divide en trozos (<code>nombre.part001.ext</code>, <code>part002</code>…). "
        "Cada trozo es un archivo independiente con su propio enlace.\n\n"
        "<b>Para recomponerlo</b>\n"
        "Descarga todos los trozos en la misma carpeta y únelos en orden "
        "(<i>7-Zip → Add to archive → Split</i>, o <code>cat parte.* > archivo</code> "
        "en Linux/macOS).\n\n"
        "<b>⚡️ Modo streaming</b>\n"
        "El bot descarga y sube <b>a la vez</b>, parte por parte: es más rápido "
        "y usa mucho menos disco. Si algún día falla, el owner puede poner "
        "<code>STREAM_MODE=off</code> en el <code>.env</code>.\n\n"
        "<b>Consejos</b>\n"
        "• Un archivo por mensaje = progreso más claro.\n"
        "• Espera a que termine una subida antes de mandar 10 más (o usa Álbum)."
    ),
}


def help_text(page: int) -> str:
    return cut(HELP_PAGES.get(page, HELP_PAGES[1]) + f"\n\n<i>Página {page}/3</i>")


# ----------------------------------------------------------------------
# Progreso.
# ----------------------------------------------------------------------
def progress_text(task: "UploadTask") -> str:
    total = task.file_size or task.bytes_transferred or 0
    pct = task.progress_percent()
    speed = task.speed
    eta = task.eta_seconds()

    header = f"<b>{esc(task.phase or 'Procesando')}</b>\n<b>{esc(shorten(task.filename, 48))}</b>"
    download_pct = (100.0 * task.bytes_fetched / total) if total else 0.0
    upload_pct = (100.0 * task.processed_bytes() / total) if total else 0.0
    lines = [header]
    if task.mode == "stream":
        lines += [
            f"<b>⬆️ {P.bar(upload_pct)} {P.percent_str(upload_pct)}</b>",
            f"<b>📤 Subido: {P.human_size_compact(task.processed_bytes())} / {P.human_size_compact(total)} · {P.human_speed(task.upload_speed)}</b>",
            f"<b>⬇️ {P.bar(download_pct)} {P.percent_str(download_pct)}</b>",
            f"<b>📥 Descargado: {P.human_size_compact(task.bytes_fetched)} / {P.human_size_compact(total)} · {P.human_speed(task.download_speed)}</b>",
        ]
    elif task.status == TaskStatus.DOWNLOADING:
        lines += [
            f"<b>⬇️ {P.bar(download_pct)} {P.percent_str(download_pct)}</b>",
            f"<b>📥 Descargando: {P.human_size_compact(task.bytes_fetched)} / {P.human_size_compact(total)} · {P.human_speed(task.download_speed)}</b>",
        ]
    else:
        lines += [
            f"<b>⬆️ {P.bar(upload_pct)} {P.percent_str(upload_pct)}</b>",
            f"<b>📤 Subiendo: {P.human_size_compact(task.processed_bytes())} / {P.human_size_compact(total)} · {P.human_speed(task.upload_speed)}</b>",
        ]
    lines.append(f"<b>⏳ ETA: {P.human_time_clock(eta)}</b>")

    detail: List[str] = []
    if task.parts_total > 1:
        detail.append(f"🧩 parte {task.parts_done}/{task.parts_total}")
    if task.mode == "stream":
        detail.append(f"⬇️ {P.human_size_compact(task.bytes_fetched)}")
    if task.queue_position:
        detail.append(f"#{task.queue_position} en cola")
    if detail:
        lines.append(" · ".join(detail))

    return cut("\n".join(lines))


def queued_text(task: "UploadTask") -> str:
    eta = task.queue_eta
    lines = [
        "<b>⏳ En cola</b>",
        b(shorten(task.filename, 48)),
        f"📦 {P.human_size_compact(task.file_size) if task.file_size else 'tamaño por determinar'}",
    ]
    if task.queue_position and task.queue_length:
        lines.append(f"📍 Posición: <b>#{task.queue_position}</b> de {task.queue_length}")
    else:
        lines.append("📍 Esperando turno…")
    if eta:
        lines.append(f"⏱️ Comienza en ~{P.human_time(eta)}")
    lines.append(i("Cuando empiece verás aquí el progreso en vivo."))
    return cut("\n".join(lines))


def final_text(task: "UploadTask") -> str:
    n = len(task.urls)
    head = (
        "<b>✅ Listo · enlace directo</b>" if n == 1
        else f"<b>✅ Listo · {n} partes subidas</b>"
    )
    body = [
        head,
        "",
        f"<b>📄 Archivo:</b> {b(shorten(task.filename, 52))}",
        f"📦 {P.human_size_compact(task.file_size)}"
        + (f" · 🧩 {task.parts_total} partes" if task.parts_total > 1 else ""),
        f"⏱️ {P.human_time(task.duration())} · ⚡️ media {P.human_speed(task.avg_speed())}",
    ]
    if task.source == "url" and task.url:
        body.append(f"🔗 origen: {code(shorten(task.url, 60))}")

    if n == 1:
        body += ["", "<b>Enlace:</b>", pre(task.urls[0])]
    else:
        listed = task.urls[:MAX_LISTED_URLS]
        block = "\n".join(f"{i+1:02d}. {u}" for i, u in enumerate(listed))
        body += ["", f"<b>Enlaces de las {n} partes:</b>", pre(block)]
        if n > MAX_LISTED_URLS:
            body.append(i(f"… y {n - MAX_LISTED_URLS} más (usa el botón .txt)"))
        body.append(i("Descarga todas las partes y únelas en orden para "
                      "reconstruir el archivo original."))
    return cut("\n".join(body))


def failed_text(task: "UploadTask") -> str:
    lines = [
        "<b>❌ La tarea falló</b>",
        "",
        f"<b>📄 Archivo:</b> {b(shorten(task.filename, 52))}",
        f"<b>🔁 Intento:</b> {b(f'{task.attempts}/{task.max_attempts_total()}')}",
    ]
    if task.failed_part:
        lines.append(f"<b>🔢 Parte:</b> {b(task.failed_part)}")
    if task.parts_done:
        lines.append(
            f"<b>🧩 Progreso:</b> {b(f'{task.parts_done}/{task.parts_total} partes subidas')}"
        )
    lines += ["", f"<b>⚠️ Detalle:</b> {i(shorten(task.error or 'sin detalle', 220))}"]
    if task.attempts < task.max_attempts_total():
        lines += ["", i("Puedes reintentar con el botón de abajo.")]
    return cut("\n".join(lines))


def cancelled_text(task: "UploadTask") -> str:
    return cut(
        "\n".join([
            "<b>🛑 Cancelado</b>",
            "",
            f"📄 {code(shorten(task.filename, 52))}",
            f"📦 {P.human_size_compact(task.processed_bytes())} de "
            f"{P.human_size_compact(task.file_size)}",
            i("Los archivos temporales se han borrado."),
        ])
    )


# ----------------------------------------------------------------------
# /cola
# ----------------------------------------------------------------------
def queue_text(user_id: int, tasks: Sequence["UploadTask"], page: int,
               per_page: int, *, global_view: bool = False) -> str:
    total_pages = max(1, (len(tasks) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    chunk = tasks[(page - 1) * per_page : page * per_page]

    if not tasks:
        return (
            "<b>📋 Cola vacía</b>\n\n"
            "No tienes tareas activas ni recientes.\n"
            "Reenvía un archivo o pega un enlace para empezar."
        )

    title = "<b>🌍 Cola global</b>" if global_view else "<b>📋 Tus tareas</b>"
    lines = [title, ""]
    if not chunk:
        lines.append(i("Nada en esta página."))
    for idx, task in enumerate(chunk, start=1 + (page - 1) * per_page):
        lines.append(task_line(task, index=idx))
        lines.append("")

    summary = _queue_summary_line(tasks)
    if summary:
        lines.append(summary)
    if total_pages > 1:
        lines.append(i(f"Página {page}/{total_pages}"))
    return cut("\n".join(lines))


def _queue_summary_line(tasks: Sequence["UploadTask"]) -> str:
    from .queue import TaskStatus as S

    active = sum(1 for t in tasks if t.status in (S.DOWNLOADING, S.UPLOADING))
    waiting = sum(1 for t in tasks if t.status == S.QUEUED)
    done = sum(1 for t in tasks if t.status == S.COMPLETED)
    failed = sum(1 for t in tasks if t.status == S.FAILED)
    parts = [
        f"🟢 {active} activas" if active else "",
        f"⏳ {waiting} en espera" if waiting else "",
        f"✅ {done} listas" if done else "",
        f"❌ {failed} fallidas" if failed else "",
    ]
    parts = [p for p in parts if p]
    return "<i>" + " · ".join(parts) + "</i>" if parts else ""


def queue_first_page(task: "UploadTask") -> int:
    return 1


# ----------------------------------------------------------------------
# /historial
# ----------------------------------------------------------------------
def history_text(rows: Sequence[dict], page: int, per_page: int) -> str:
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    chunk = rows[(page - 1) * per_page : page * per_page]

    if not rows:
        return "<b>🗂 Historial</b>\n\nTodavía no has subido nada."

    lines = [f"<b>🗂 Historial ({len(rows)})</b>", ""]
    for row in chunk:
        icon = {"completed": "✅", "failed": "❌", "cancelled": "🛑"}.get(row["status"], "❔")
        size = P.human_size_compact(row["file_size"])
        when = _rel_time(row["finished_at"] or row["created_at"])
        lines.append(
            f"{icon} {code(shorten(row['filename'], 34))} · {size} · {when}\n"
            f"     {code(row['task_id'])}"
        )
        if row["status"] == "completed" and row["urls"]:
            first = row["urls"][0] if isinstance(row["urls"], list) else str(row["urls"]).split("\n")[0]
            lines.append(f"     🔗 {code(shorten(first, 60))}")
        lines.append("")
    lines.append(i(f"Página {page}/{total_pages} · usa los botones para navegar"))
    return cut("\n".join(lines))


def _rel_time(ts: Optional[float]) -> str:
    import time

    if not ts:
        return "—"
    delta = max(0, int(time.time() - ts))
    if delta < 60:
        return f"hace {delta}s"
    if delta < 3600:
        return f"hace {delta // 60}m"
    if delta < 86400:
        return f"hace {delta // 3600}h"
    return f"hace {delta // 86400}d"


# ----------------------------------------------------------------------
# Estado / estadísticas.
# ----------------------------------------------------------------------
def status_text(*, bot_line: str, storage_line: str, settings, counts: dict,
                paused: bool) -> str:
    return cut(
        "\n".join([
            "<b>📊 Estado del sistema</b>",
            "",
            "<b>🤖 Telegram</b>",
            f"   {bot_line}",
            "",
            "<b>☁️ Almacenamiento (toDus)</b>",
            f"   {storage_line}",
            "",
            "<b>⚙️ Configuración activa</b>",
            f"   • Máx. archivo: {code(P.human_size_compact(settings.max_file_mb * 1024 * 1024))}",
            f"   • Chunk: {code(str(settings.chunk_size_mb) + ' MB')} · "
            f"workers/tarea: {code(settings.max_parallel)} · "
            f"globales: {code(settings.global_chunk_workers)}",
            f"   • Reintentos: {code(settings.max_retries)} · Modo: {code(settings.pipeline_mode)}",
            "",
            "<b>🧮 Cola</b>",
            f"   • Activas: {code(counts['active'])}/{counts['max_concurrent']}"
            + ("  🛑 <b>PAUSADA</b>" if paused else ""),
            f"   • En espera: {code(counts['waiting'])} · Usuarios en cola: {code(counts['users_waiting'])}",
            f"   • Cupo por usuario: {code(settings.max_active_per_user)} activa(s), "
            f"{code(settings.max_queue_per_user)} en espera",
            f"   • Completadas (sesión): {code(counts['completed'])} · "
            f"Fallidas: {code(counts['failed'])}",
            "",
            f"<i>Servicio activo desde {_uptime(counts['uptime'])}</i>",
        ])
    )


def stats_text(global_stats: dict, top: Sequence[dict], counts: dict) -> str:
    lines = [
        "<b>📈 Estadísticas globales</b>",
        "",
        f"👥 Usuarios únicos: <b>{global_stats['users']}</b>",
        f"✅ Subidas completadas: <b>{global_stats['completed']}</b>",
        f"❌ Fallidas: <b>{global_stats['failed']}</b> · 🛑 Canceladas: <b>{global_stats['cancelled']}</b>",
        f"📦 Datos subidos: <b>{P.human_size(global_stats['bytes'])}</b>",
        f"🧩 Partes generadas: <b>{global_stats['parts']}</b>",
        f"⏱️ Duración media: <b>{P.human_time(global_stats['avg_duration'])}</b>",
        "",
        "<b>🟢 Ahora mismo</b>",
        f"   • Activas: {counts['active']} · En espera: {counts['waiting']}",
        f"   • Velocidad global: {P.human_speed(counts['global_speed'])}",
        f"   • Usuarios en cola: {counts['users_waiting']}",
    ]
    if top:
        lines += ["", "<b>🏆 Top usuarios por datos subidos</b>"]
        for idx, row in enumerate(top, start=1):
            who = row["username"] or str(row["user_id"])
            lines.append(
                f"{idx}. {code(shorten(who, 20))} — {P.human_size(row['bytes'])} "
                f"({row['uploads']} subidas)"
            )
    return cut("\n".join(lines))


def system_text(info: dict) -> str:
    return cut(
        "\n".join([
            "<b>🖥 Recursos del servidor</b>",
            "",
            f"💾 Disco libre: <b>{P.human_size(info['disk_free'])}</b> "
            f"de {P.human_size(info['disk_total'])} "
            f"({info['disk_percent']:.1f}% usado)",
            f"🧠 Memoria: <b>{P.human_size(info['mem_used'])}</b> "
            f"de {P.human_size(info['mem_total'])}",
            f"⚙️ CPU: <b>{info['cpu']} núcleos</b> · Carga media: {info['load']}",
            f"🧵 Hilos activos: <b>{info['threads']}</b>",
            f"📂 Temporales: <b>{P.human_size(info['temp_size'])}</b> "
            f"({info['temp_files']} archivos)",
            f"⏳ Uptime del proceso: <b>{_uptime(info['uptime'])}</b>",
            f"🐍 Python: <b>{info['python']}</b>",
            "",
            i("Si el disco libre baja del mínimo configurado, el bot rechazará "
              "nuevas descargas para no llenar el servidor."),
        ])
    )


def _uptime(seconds: float) -> str:
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h >= 24:
        return f"{h // 24}d {h % 24}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


# ----------------------------------------------------------------------
# /settings
# ----------------------------------------------------------------------
def settings_text(values: dict, page: int, pages: int, *, is_owner_user: bool,
                   keys: Sequence[str], labels: dict, units: dict) -> str:
    lines = [
        f"<b>⚙️ Configuración · página {page}/{pages}</b>",
        "",
    ]
    for key in keys:
        value = values[key]
        unit = units.get(key, "")
        lines.append(f"• {esc(labels[key])}: {code(f'{display_number(value)} {unit}'.strip())}")
    lines += [
        "",
        i("Toca un valor para pasar al siguiente. Los cambios se aplican a las "
          "tareas nuevas y se guardan en disco."),
    ]
    if not is_owner_user:
        lines.append(i("ℹ️ Solo el owner puede modificar la configuración."))
    return cut("\n".join(lines))


# ----------------------------------------------------------------------
# Teclados inline.
# ----------------------------------------------------------------------
def main_menu_keyboard(*, is_owner_user: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📋 Mi cola", callback_data="menu:cola"),
            InlineKeyboardButton("🗂 Historial", callback_data="menu:hist"),
        ],
        [
            InlineKeyboardButton("📊 Estado", callback_data="menu:status"),
            InlineKeyboardButton("📖 Ayuda", callback_data="menu:help:1"),
        ],
    ]
    if is_owner_user:
        rows.append([
            InlineKeyboardButton("📈 Stats", callback_data="menu:stats"),
            InlineKeyboardButton("🖥 Sistema", callback_data="menu:sys"),
        ])
        rows.append([InlineKeyboardButton("⚙️ Configuración", callback_data="menu:set")])
    return InlineKeyboardMarkup(rows)


def help_keyboard(page: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"menu:help:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/3", callback_data="noop"))
    if page < 3:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"menu:help:{page+1}"))
    return InlineKeyboardMarkup([nav, [InlineKeyboardButton("🏠 Menú", callback_data="menu:home")]])


def queue_keyboard(tasks: Sequence["UploadTask"], page: int, per_page: int,
                   *, is_owner_user: bool, global_view: bool = False) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, task in enumerate(tasks[(page - 1) * per_page : page * per_page],
                               start=1 + (page - 1) * per_page):
        if task.status in (TaskStatus.QUEUED, TaskStatus.DOWNLOADING, TaskStatus.UPLOADING):
            rows.append([
                InlineKeyboardButton(
                    f"🛑 Cancelar {idx}",
                    callback_data=f"t:cancel:{task.task_id}",
                ),
                InlineKeyboardButton(
                    f"ℹ️ Detalle {idx}",
                    callback_data=f"t:card:{task.task_id}",
                ),
            ])
        elif task.status == TaskStatus.COMPLETED:
            rows.append([
                InlineKeyboardButton(
                    f"📄 Enlaces {idx}", callback_data=f"t:links:{task.task_id}"
                ),
                InlineKeyboardButton(
                    f"🗑 Borrar {idx}", callback_data=f"t:delyes:{task.task_id}"
                ),
            ])
        elif task.status == TaskStatus.FAILED:
            rows.append([
                InlineKeyboardButton(
                    f"🔁 Reintentar {idx}", callback_data=f"t:retry:{task.task_id}"
                ),
                InlineKeyboardButton("ℹ️ Detalle", callback_data=f"t:card:{task.task_id}"),
            ])

    total_pages = max(1, (len(tasks) + per_page - 1) // per_page)
    if total_pages > 1:
        nav = [
            InlineKeyboardButton("⬅️", callback_data=f"cola:page:{max(1, page-1)}"),
            InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"),
            InlineKeyboardButton("➡️", callback_data=f"cola:page:{min(total_pages, page+1)}"),
        ]
        rows.append(nav)

    actions: List[InlineKeyboardButton] = []
    if not global_view:
        actions.append(InlineKeyboardButton("🔄 Refrescar", callback_data="cola:refresh"))
        actions.append(InlineKeyboardButton("🛑 Cancelar todas", callback_data="cola:cancelall"))
        if is_owner_user:
            actions.append(InlineKeyboardButton("🌍 Vista global", callback_data="cola:global"))
    else:
        actions.append(InlineKeyboardButton("🔄 Refrescar", callback_data="cola:global"))
        actions.append(InlineKeyboardButton("◀️ Mi cola", callback_data="cola:refresh"))
    rows.append(actions)
    rows.append([InlineKeyboardButton("🏠 Menú", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def task_keyboard(task: "UploadTask", *, is_owner_user: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if task.status in (TaskStatus.QUEUED, TaskStatus.DOWNLOADING, TaskStatus.UPLOADING):
        rows.append([InlineKeyboardButton("🛑 Cancelar", callback_data=f"t:cancel:{task.task_id}")])
    if task.status == TaskStatus.COMPLETED:
        rows.append([
            InlineKeyboardButton("📄 Enlaces .txt", callback_data=f"t:links:{task.task_id}"),
            InlineKeyboardButton("🗑 Borrar de S3", callback_data=f"t:delyes:{task.task_id}"),
        ])
    if task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
        rows.append([InlineKeyboardButton("🔁 Reintentar", callback_data=f"t:retry:{task.task_id}")])
    if is_owner_user and task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.COMPLETED):
        rows.append([InlineKeyboardButton("🧹 Quitar de la lista", callback_data=f"t:drop:{task.task_id}")])
    rows.append([InlineKeyboardButton("⬅️ Mi cola", callback_data="cola:refresh")])
    return InlineKeyboardMarkup(rows)


def cancel_all_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Sí, cancelar todo", callback_data="cola:cancelall:yes"),
        InlineKeyboardButton("↩️ No", callback_data="cola:refresh"),
    ]])


def delete_confirm_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Sí, borrar de S3", callback_data=f"t:delyes:yes:{task_id}"),
        InlineKeyboardButton("↩️ No", callback_data=f"t:card:{task_id}"),
    ]])


def history_keyboard(page: int, pages: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"hist:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
    if page < pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"hist:page:{page+1}"))
    return InlineKeyboardMarkup([nav, [InlineKeyboardButton("🏠 Menú", callback_data="menu:home")]])


def settings_keyboard(keys: Sequence[str], values: dict, labels: dict, units: dict,
                      page: int, pages: int, *, is_owner_user: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in keys:
        unit = units.get(key, "")
        text = f"{labels[key]}: {values[key]} {unit}".strip()
        cb = f"set:cycle:{key}" if is_owner_user else f"set:view:{key}"
        rows.append([InlineKeyboardButton(text, callback_data=cb)])
    nav: List[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"set:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data="noop"))
    if page < pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"set:page:{page+1}"))
    rows.append(nav)
    if is_owner_user:
        rows.append([
            InlineKeyboardButton("♻️ Valores por defecto", callback_data="set:reset"),
            InlineKeyboardButton("🔄 Refrescar", callback_data="set:refresh"),
        ])
    rows.append([InlineKeyboardButton("🏠 Menú", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def admin_keyboard(paused: bool) -> InlineKeyboardMarkup:
    toggle = (
        InlineKeyboardButton("▶️ Reanudar cola", callback_data="adm:resume")
        if paused
        else InlineKeyboardButton("⏸ Pausar cola", callback_data="adm:pause")
    )
    return InlineKeyboardMarkup([
        [toggle, InlineKeyboardButton("📈 Stats", callback_data="menu:stats")],
        [InlineKeyboardButton("🖥 Sistema", callback_data="menu:sys"),
         InlineKeyboardButton("🌍 Cola global", callback_data="cola:global")],
    ])


def links_buttons(task: "UploadTask") -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("📄 Descargar .txt", callback_data=f"t:links:{task.task_id}")]]
    if task.urls and not task.deleted_from_s3:
        rows.append([InlineKeyboardButton("🗑 Borrar de toDus", callback_data=f"t:delyes:{task.task_id}")])
    if len(task.urls) == 1:
        rows.append([InlineKeyboardButton("🔗 Abrir enlace", url=task.urls[0])])
    return InlineKeyboardMarkup(rows)


def progress_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Cancelar", callback_data=f"t:cancel:{task_id}"),
    ]])
