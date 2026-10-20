"""
texts.py
========
Todos los TEXTOS (HTML de Telegram) del bot. Los teclados están en
``keyboards.py`` y la composición texto + teclado en ``views.py``.

Reglas de estilo:
- Títulos con emoji + <b>, datos en <code>, notas en <i>.
- Los enlaces se muestran en <pre> para que Telegram ofrezca el botón
  "copiar" y se puedan copiar todos de una sola vez.
- Todo mensaje pasa por ``cut()``: queda por debajo del límite de 4096
  caracteres y SIEMPRE con el HTML bien cerrado (un <pre> sin cerrar hace
  que Telegram rechace el mensaje entero y el usuario no recibe sus enlaces).
"""

from __future__ import annotations

import html
import re
import time
from typing import List, Optional, Sequence, TYPE_CHECKING

from . import progress as P
from .queue import STATUS_ICON, STATUS_LABEL, TaskStatus

if TYPE_CHECKING:  # pragma: no cover
    from .queue import UploadTask

TG_LIMIT = 4000          # margen sobre el límite real de Telegram (4096)
URL_BLOCK_RESERVE = 700  # hueco que se deja a los textos que rodean a los enlaces


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
    """Muestra 2.0 como 2, los booleanos como sí/no, etc."""
    if isinstance(value, bool):
        return "sí" if value else "no"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def metric_speed(value: float, estimated: bool = False) -> str:
    """Velocidad lista para mostrar. '~' = todavía es una estimación."""
    if not value or value <= 0:
        return "calculando…"
    prefix = "~" if estimated else ""
    return f"{prefix}{P.human_speed(value)}"


_TAG_RE = re.compile(r"(<[^>]*>)")
_TAG_NAME_RE = re.compile(r"<\s*(/?)\s*([A-Za-z][A-Za-z0-9-]*)")
_NO_BOLD_INSIDE = {"pre", "code", "b", "strong", "a"}


def _tag_info(token: str):
    """('open'|'close', nombre) de una etiqueta, o None si no lo es."""
    match = _TAG_NAME_RE.match(token)
    if not match:
        return None
    return ("close" if match.group(1) else "open"), match.group(2).lower()


def _bold_non_link_text(text: str) -> str:
    """
    Pone en negrita todo el texto visible (estilo del bot), dejando intactos
    los bloques <pre>/<code> (enlaces) y lo que ya va en negrita.
    """
    result: List[str] = []
    stack: List[str] = []
    for part in _TAG_RE.split(text):
        if not part:
            continue
        info = _tag_info(part) if part.startswith("<") else None
        if info:
            kind, name = info
            if kind == "open":
                stack.append(name)
            elif name in stack:
                del stack[len(stack) - 1 - stack[::-1].index(name):]
            result.append(part)
        elif not part.strip() or any(tag in _NO_BOLD_INSIDE for tag in stack):
            result.append(part)
        else:
            result.append(f"<b>{part}</b>")
    return "".join(result)


def _open_tags(text: str) -> List[str]:
    stack: List[str] = []
    for token in _TAG_RE.findall(text):
        info = _tag_info(token)
        if not info:
            continue
        kind, name = info
        if kind == "open":
            stack.append(name)
        elif name in stack:
            del stack[len(stack) - 1 - stack[::-1].index(name):]
    return stack


def cut(text: str, limit: int = TG_LIMIT) -> str:
    """
    Estiliza un mensaje y lo recorta (si hace falta) SIN dejar etiquetas abiertas.

    Se corta en un salto de línea y se cierran las etiquetas pendientes; así
    Telegram nunca recibe un <pre> o <b> sin cerrar.
    """
    text = _bold_non_link_text(text)
    if len(text) <= limit:
        return text

    note = "\n<i>… (recortado)</i>"
    budget = limit - len(note) - 40          # 40 = hueco para cerrar etiquetas
    head = text[:budget]
    newline = head.rfind("\n")
    if newline > budget // 2:
        head = head[:newline]
    # Nunca dejar una etiqueta o una entidad a medias.
    head = re.sub(r"<[^>]*$", "", head)
    head = re.sub(r"&[A-Za-z0-9#]*$", "", head)
    closers = "".join(f"</{name}>" for name in reversed(_open_tags(head)))
    return head + closers + note


def shorten(name: str, limit: int = 42) -> str:
    if len(name) <= limit:
        return name
    keep = max(6, limit - 3)
    return name[: keep // 2] + "…" + name[-(keep // 2):]


def mask_id(user_id) -> str:
    """123456789 -> '…6789' (para no enseñar IDs completos a otros usuarios)."""
    return "…" + str(user_id)[-4:]


def who(task: "UploadTask") -> str:
    return f"@{task.username}" if task.username else str(task.user_id)


def _urls_block(urls: Sequence[str], room: int, *, numbered: bool) -> tuple:
    """
    Construye el bloque de enlaces ocupando como máximo ``room`` caracteres.
    Devuelve (texto_sin_escapar, cuántos_enlaces_caben).
    """
    lines: List[str] = []
    used = 0
    for idx, url in enumerate(urls, start=1):
        line = f"{idx:02d}. {url}" if numbered else url
        cost = len(esc(line)) + 1
        if used + cost > room:
            break
        lines.append(line)
        used += cost
    return "\n".join(lines), len(lines)


# ----------------------------------------------------------------------
# Fichas de tarea.
# ----------------------------------------------------------------------
def status_badge(status: TaskStatus) -> str:
    return f"{STATUS_ICON.get(status, '❔')} {STATUS_LABEL.get(status, status.value)}"


def task_line(task: "UploadTask", index: Optional[int] = None,
              show_user: bool = False) -> str:
    head = f"{index}. " if index is not None else ""
    line = (
        f"{head}{STATUS_ICON.get(task.status, '❔')} "
        f"{code(shorten(task.filename))} "
        f"<i>({P.human_size_compact(task.file_size)})</i>"
    )
    if show_user:
        line += f" · {esc(who(task))}"
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
        extra.append(metric_speed(current_speed))
        eta = task.eta_seconds()
        extra.append(f"ETA {P.human_time(eta) if eta is not None else 'calculando…'}")
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
        f"1️⃣ Reenvía (o envía) un <b>documento / video / audio / foto</b>.\n"
        f"2️⃣ O pega uno o varios <b>enlaces http(s)</b> directos.\n"
        f"3️⃣ Usa <code>/descargar URL mp3</code> para sitios compatibles.\n"
        f"4️⃣ El bot te da el <b>enlace directo</b> cuando termina.\n\n"
        f"<b>⚙️ Ajustes actuales</b>\n"
        f"• Máx. por archivo: {code(P.human_size_compact(settings.max_file_mb * 1024 * 1024))}\n"
        f"• Trozo (chunk): {code(str(settings.chunk_size_mb) + ' MB')}\n"
        f"• Cola: {code(str(settings.max_concurrent_tasks) + ' tareas a la vez')} · "
        f"{code(str(settings.max_active_per_user) + ' por usuario')}\n\n"
        f"<b>🧭 Comandos</b>\n"
        f"/cola — tus tareas y su posición\n"
        f"/historial — tus subidas (con enlaces)\n"
        f"/buscar — buscar en tu historial\n"
        f"/cancelar — cancelar todo lo tuyo\n"
        f"/status — estado del bot\n"
        f"/help — ayuda detallada"
    )


HELP_PAGES = {
    1: (
        "<b>📖 Ayuda 1/3 · Lo básico</b>\n\n"
        "<b>Subir desde Telegram</b>\n"
        "Reenvía un documento, video, audio o foto al chat. Si el archivo supera "
        "el máximo configurado, el bot lo rechaza con un aviso claro.\n\n"
        "<b>Subir desde una URL</b>\n"
        "Pega un enlace directo (http/https). Puedes pegar <b>varios enlaces "
        "en un mismo mensaje</b> y se encolan todos.\n\n"
        "<b>Descargar desde plataformas</b>\n"
        "Usa <code>/descargar URL</code> para video MP4 (720p por defecto), o "
        "<code>/descargar URL mp3</code> para audio. También puedes indicar "
        "<code>mp4 1080</code> u otra calidad disponible. La calidad depende "
        "del contenido y del sitio.\n\n"
        "<b>Álbumes</b>\n"
        "Si reenvías un álbum de fotos/videos, cada archivo se encola por "
        "separado automáticamente.\n\n"
        "<b>Resultado</b>\n"
        "Cuando termina, el mismo mensaje se convierte en el resultado con los "
        "<b>enlaces directos</b> (en un bloque de copiado rápido)."
    ),
    2: (
        "<b>📖 Ayuda 2/3 · La cola y los botones</b>\n\n"
        "El bot trabaja con una <b>cola justa</b>: hay un número limitado de "
        "tareas simultáneas en todo el bot y cada usuario tiene su propio cupo, "
        "de forma que <b>nadie se queda bloqueado</b> porque otro subió 20 archivos.\n\n"
        "<b>Estados</b>\n"
        "⏳ en cola · 📥 descargando · 📤 subiendo · ✅ listo · ❌ error · 🛑 cancelado\n\n"
        "<b>Cómo se usan los botones</b>\n"
        "• Cada mensaje de una subida tiene SUS botones: cancelar mientras "
        "avanza; enlaces, borrar o reintentar cuando termina.\n"
        "• /cola y /historial son listas: toca una tarea para abrir su ficha "
        "y desde ahí hacer cualquier acción.\n"
        "• 🏠 Inicio te lleva siempre al menú principal."
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
        "<b>Si una subida falla</b>\n"
        "El botón 🔁 Reintentar reanuda: las partes que ya estaban en toDus no "
        "se vuelven a subir.\n\n"
        "<b>Consejos</b>\n"
        "• Un archivo por mensaje = progreso más claro.\n"
        "• Espera a que termine una subida antes de mandar 10 más (o usa Álbum)."
    ),
}
HELP_PAGE_COUNT = len(HELP_PAGES)


def help_text(page: int) -> str:
    page = page if page in HELP_PAGES else 1
    return cut(HELP_PAGES[page] + f"\n\n<i>Página {page}/{HELP_PAGE_COUNT}</i>")


def sites_text() -> str:
    """Catálogo honesto: separa soporte probado de soporte experimental."""
    return cut(
        "<b>🌐 Sitios compatibles</b>\n\n"
        "<b>📁 Alojamientos de archivos</b>\n"
        "✅ MediaFire · Dropbox · Pixeldrain\n"
        "✅ GoFile · WeTransfer (enlaces públicos)\n"
        "🧪 File.io · Google Drive (enlaces públicos)\n\n"
        "<b>📱 Aplicaciones</b>\n"
        "✅ Google Play (aplicaciones gratuitas; requiere dispenser configurado)\n\n"
        "<b>🎬 Plataformas multimedia</b>\n"
        "✅ Instagram · TikTok · Facebook · Dailymotion\n"
        "✅ Vimeo · Twitch · Reddit · SoundCloud · Bilibili\n"
        "✅ Rumble · Pinterest · Streamable\n"
        "✅ YouTube (videos públicos y enlaces compatibles).\n\n"
        "<b>🔗 Enlaces directos</b>\n"
        "✅ Cualquier URL http(s) que entregue directamente un archivo.\n\n"
        "<b>🧪 Soporte experimental</b>\n"
        "1fichier · Ufile (pueden requerir una página pública simple)\n\n"
        "<b>🚫 No disponible actualmente</b>\n"
        "AnonFiles: servicio sin disponibilidad estable verificada.\n\n"
        "<i>Los enlaces privados, protegidos con contraseña, CAPTCHA o inicio de sesión "
        "pueden no funcionar aunque el sitio aparezca en la lista.</i>"
    )


# ----------------------------------------------------------------------
# Progreso (mensaje de la tarea mientras se ejecuta).
# ----------------------------------------------------------------------
def progress_text(task: "UploadTask") -> str:
    """
    Mensaje de progreso (se refresca cada pocos segundos).

    La subida informa bytes enviados por cada parte y conserva una estimación
    limitada solo durante los primeros instantes de una conexión.
    """
    total = task.file_size or 0
    estimated = task.is_estimated()
    elapsed = P.human_time_clock(task.duration())

    lines = [
        f"<b>{esc(task.phase or 'Procesando')}</b>",
        f"<b>{esc(shorten(task.filename, 48))}</b>",
    ]

    # Descarga y subida muestran sus métricas de bytes de forma independiente.
    priming = total > 0 and task.processed_bytes() <= 0 and task.bytes_fetched <= 0
    upload_pct = 100.0 * task.processed_bytes() / total if total else 0.0
    download_pct = task.download_percent()

    if task.mode == "stream":
        lines += [
            f"<b>⬆️ {P.bar(upload_pct)} {P.percent_str(upload_pct)}</b>",
            f"<b>📤 Subido: {P.human_size_compact(task.processed_bytes())} / "
            f"{P.human_size_compact(total)} · {metric_speed(task.upload_speed, estimated)}</b>",
            f"<b>⬇️ {P.bar(download_pct)} {P.percent_str(download_pct)}</b>",
            f"<b>📥 Descargado: {P.human_size_compact(task.bytes_fetched)} / "
            f"{P.human_size_compact(total)} · {P.human_speed(task.download_speed)}</b>",
        ]
    elif task.status == TaskStatus.DOWNLOADING:
        lines += [
            f"<b>⬇️ {P.bar(download_pct)} {P.percent_str(download_pct)}</b>",
            f"<b>📥 Descargando: {P.human_size_compact(task.bytes_fetched)} / "
            f"{P.human_size_compact(total)} · "
            f"{metric_speed(task.download_speed, task.download_speed <= 0)}</b>",
        ]
    else:
        lines += [
            f"<b>⬆️ {P.bar(upload_pct)} {P.percent_str(upload_pct)}</b>",
            f"<b>📤 Subiendo: {P.human_size_compact(task.processed_bytes())} / "
            f"{P.human_size_compact(total)} · {metric_speed(task.upload_speed, estimated)}</b>",
        ]

    eta = task.eta_seconds()
    eta_label = P.human_time_clock(eta) if eta is not None else "calculando…"
    lines.append(f"<b>⏳ Restante: {eta_label} · ⏱️ {elapsed}</b>")

    detail: List[str] = []
    if task.resumed_parts:
        detail.append(f"↻ {len(task.resumed_parts)} partes reanudadas")
    if task.parts_total > 1 and task.status == TaskStatus.UPLOADING:
        detail.append(f"🧩 parte {min(task.parts_done + 1, task.parts_total)}/{task.parts_total}")
    elif task.parts_total > 1:
        detail.append(f"🧩 {task.parts_done}/{task.parts_total} partes")
    if estimated and task.status != TaskStatus.DOWNLOADING:
        detail.append("📈 velocidad estimada" if not priming else "📈 estimando velocidad")
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
    if task.deleted_from_s3 or not task.urls:
        return cut("\n".join([
            "<b>🗑 Borrado de toDus</b>",
            "",
            f"<b>📄 Archivo:</b> {b(shorten(task.filename, 52))}",
            f"📦 {P.human_size_compact(task.file_size)}",
            i("Los enlaces ya no están disponibles."),
        ]))

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

    room = TG_LIMIT - URL_BLOCK_RESERVE - sum(len(x) + 1 for x in body)
    if n == 1:
        block, shown = _urls_block(task.urls, room, numbered=False)
        body += ["", "<b>Enlace:</b>", pre(block)]
    else:
        block, shown = _urls_block(task.urls, room, numbered=False)
        body += ["", f"<b>Enlaces de las {n} partes:</b>", pre(block)]
        if shown < n:
            body.append(i(f"… y {n - shown} más (usa el botón .txt)"))
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
    if task.is_retryable and not getattr(task, "retried_as", None):
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


def retry_queued_text(clone: "UploadTask", resumed: int = 0) -> str:
    lines = [
        "<b>♻️ Reintento en cola</b>",
        f"📄 {code(shorten(clone.filename, 44))}",
        f"🆔 {code(clone.task_id)} · intento {clone.attempts}/3",
    ]
    if resumed:
        lines.append(
            f"♻️ <b>Reanudando</b>: {b(str(resumed))} de {b(str(clone.parts_total))} "
            f"partes ya estaban en toDus — no se vuelven a subir."
        )
    return cut("\n".join(lines))


def task_card_text(task: "UploadTask", *, show_user: bool = False) -> str:
    """Ficha completa de una tarea (se abre desde /cola y /historial)."""
    lines = [
        f"<b>{STATUS_ICON.get(task.status, '❔')} Ficha de la tarea</b>",
        "",
        f"🆔 {code(task.task_id)}",
        f"📄 {code(shorten(task.filename, 48))}",
        f"📦 {esc(P.human_size_compact(task.file_size))}",
    ]
    if show_user:
        lines.append(f"👤 {esc(who(task))}")
    lines.append(
        f"🧭 Estado: {esc(STATUS_LABEL.get(task.status, task.status.value))}"
        + (f" · {esc(task.phase)}" if task.phase and task.status.is_active else "")
        + (" · 🗑 borrado de toDus" if task.deleted_from_s3 else "")
    )
    lines.append(f"⏱️ Duración: {esc(P.human_time(task.duration()))}")
    if task.parts_total:
        lines.append(f"🧩 Partes: {code(f'{task.parts_done}/{task.parts_total}')}")
    if task.status.is_active:
        lines.append(f"⚡️ {esc(P.human_speed(task.speed))} · "
                     f"⏳ {esc(P.human_time(task.eta_seconds()))}")
    if task.status == TaskStatus.QUEUED and task.queue_position:
        lines.append(f"📍 Posición en cola: {code(task.queue_position)}")
    if task.error:
        lines.append(f"\n❗️ {esc(shorten(task.error, 200))}")
    if task.has_links:
        room = TG_LIMIT - URL_BLOCK_RESERVE - sum(len(x) + 1 for x in lines)
        block, shown = _urls_block(task.urls, room, numbered=len(task.urls) > 1)
        lines += ["", f"<b>Enlaces ({len(task.urls)}):</b>", pre(block)]
        if shown < len(task.urls):
            lines.append(i(f"… y {len(task.urls) - shown} más (usa el botón .txt)"))
    return cut("\n".join(lines))


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
            + ("No hay tareas en el bot." if global_view
               else "No tienes tareas activas ni recientes.\n"
                    "Reenvía un archivo o pega un enlace para empezar.")
        )

    title = "<b>🌍 Cola global</b>" if global_view else "<b>📋 Tus tareas</b>"
    lines = [title, ""]
    for idx, task in enumerate(chunk, start=1 + (page - 1) * per_page):
        lines.append(task_line(task, index=idx, show_user=global_view))
        lines.append("")

    summary = _queue_summary_line(tasks)
    if summary:
        lines.append(summary)
    lines.append(i("Toca una tarea para ver sus acciones."))
    if total_pages > 1:
        lines.append(i(f"Página {page}/{total_pages}"))
    return cut("\n".join(lines))


def _queue_summary_line(tasks: Sequence["UploadTask"]) -> str:
    active = sum(1 for t in tasks if t.status in (TaskStatus.DOWNLOADING, TaskStatus.UPLOADING))
    waiting = sum(1 for t in tasks if t.status == TaskStatus.QUEUED)
    done = sum(1 for t in tasks if t.status == TaskStatus.COMPLETED)
    failed = sum(1 for t in tasks if t.status == TaskStatus.FAILED)
    parts = [
        f"🟢 {active} activas" if active else "",
        f"⏳ {waiting} en espera" if waiting else "",
        f"✅ {done} listas" if done else "",
        f"❌ {failed} fallidas" if failed else "",
    ]
    parts = [p for p in parts if p]
    return "<i>" + " · ".join(parts) + "</i>" if parts else ""


def cancel_all_text(tasks: Sequence["UploadTask"], *, global_scope: bool) -> str:
    title = "toda la cola" if global_scope else f"tus {len(tasks)} tarea(s)"
    lines = [f"<b>🛑 ¿Cancelar {title}?</b>", ""]
    lines += [
        f"• {code(shorten(t.filename, 36))} — {esc(STATUS_LABEL[t.status])}"
        for t in tasks[:10]
    ]
    if len(tasks) > 10:
        lines.append(i(f"… y {len(tasks) - 10} más"))
    return cut("\n".join(lines))


# ----------------------------------------------------------------------
# /historial
# ----------------------------------------------------------------------
def history_text(rows: Sequence[dict], page: int, per_page: int, *,
                 title: str = "🗂 Historial") -> str:
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    chunk = rows[(page - 1) * per_page : page * per_page]

    if not rows:
        return f"<b>{esc(title)}</b>\n\nNo hay subidas que mostrar."

    lines = [f"<b>{esc(title)} ({len(rows)})</b>", ""]
    for idx, row in enumerate(chunk, start=1 + (page - 1) * per_page):
        icon = "🗑" if row.get("deleted_at") else {
            "completed": "✅", "failed": "❌", "cancelled": "🛑"
        }.get(row["status"], "❔")
        size = P.human_size_compact(row["file_size"])
        when = _rel_time(row["finished_at"] or row["created_at"])
        parts = f" · 🧩 {row['parts']}" if (row.get("parts") or 0) > 1 else ""
        lines.append(f"{idx}. {icon} {code(shorten(row['filename'], 34))} · {size}{parts} · {when}")
    lines.append("")
    lines.append(i("Toca una subida para ver sus enlaces o borrarla."))
    if total_pages > 1:
        lines.append(i(f"Página {page}/{total_pages}"))
    return cut("\n".join(lines))


def _rel_time(ts: Optional[float]) -> str:
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
                paused: bool, speeds_line: str = "") -> str:
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
            "<b>📈 Velocidad aprendida</b>",
            f"   {esc(speeds_line) if speeds_line else '—'}",
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


def quota_text(*, used: int, limit: int) -> str:
    if limit <= 0:
        return cut(
            "<b>📊 Mi cuota</b>\n\n"
            f"• Usado en las últimas 24 horas: <b>{P.human_size(used)}</b>\n"
            "• Límite: <b>sin límite</b>\n\n"
            + i("La cuota, si se configura, se calcula sobre una ventana móvil de 24 horas.")
        )
    remaining = max(0, limit - used)
    pct = min(100.0, used * 100.0 / limit)
    return cut(
        "<b>📊 Mi cuota diaria</b>\n\n"
        f"• Usado: <b>{P.human_size(used)}</b> de <b>{P.human_size(limit)}</b>\n"
        f"• Disponible: <b>{P.human_size(remaining)}</b>\n"
        f"• Consumo: <b>{pct:.1f}%</b>\n\n"
        + i("Se calcula sobre las últimas 24 horas y se actualiza al completar subidas.")
    )


def top_users_text(rows: Sequence[dict], *, viewer_id: int = 0,
                   is_owner_user: bool = False, owner_id: int = 0) -> str:
    lines = [
        "<b>🏆 Top usuarios</b>",
        "<i>Ranking por espacio total subido</i>",
        "",
    ]
    if not rows:
        lines.append("Todavía no hay datos suficientes.")
    else:
        for idx, row in enumerate(rows, start=1):
            uid = int(row.get("user_id", 0) or 0)
            marker = " 👑" if uid and uid == owner_id else ""
            if uid == viewer_id:
                label, marker = "tú", marker + " ⭐"
            else:
                label = str(uid) if is_owner_user else mask_id(uid)
            lines.append(
                f"{idx}. {code(label)}{marker} · "
                f"{P.human_size(int(row.get('bytes', 0) or 0))} · "
                f"{int(row.get('uploads', 0) or 0)} subida(s)"
            )
    if not is_owner_user:
        lines += ["", i("Por privacidad solo se muestran los últimos 4 dígitos del ID.")]
    return cut("\n".join(lines))


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
            name = row["username"] or str(row["user_id"])
            lines.append(
                f"{idx}. {code(shorten(name, 20))} — {P.human_size(row['bytes'])} "
                f"({row['uploads']} subidas)"
            )
    return cut("\n".join(lines))


def logs_text(records: Sequence[str], part: int = 1, total: int = 1) -> str:
    """Panel de logs recientes visible únicamente para el owner."""
    lines = [
        f"<b>🧾 Logs completos · parte {part}/{total}</b>",
        "",
        "Hora: Cuba · archivo persistente: data/bot.log",
        "",
    ]
    if records:
        lines.append("<pre>" + esc("\n".join(records)) + "</pre>")
    return cut("\n".join(lines))


def admin_text(counts: dict, paused: bool, maintenance: bool) -> str:
    flags = []
    if paused:
        flags.append("⏸ cola PAUSADA")
    if maintenance:
        flags.append("🛠 mantenimiento ACTIVO")
    return cut("\n".join([
        "<b>🛠 Administración</b>",
        "",
        f"🟢 Activas: {counts['active']}/{counts['max_concurrent']} · "
        f"⏳ En espera: {counts['waiting']}",
        "🚦 " + (" · ".join(flags) if flags else "todo en marcha"),
        "",
        i("Comandos: /ban /unban /baneados /usuarios /anuncio /mantenimiento /limpiar"),
    ]))


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
    lines.append("")
    if is_owner_user:
        lines.append(i("Toca un valor para pasar al siguiente. Los cambios se aplican a las "
                       "tareas nuevas y se guardan en disco."))
    else:
        lines.append(i("ℹ️ Solo el owner puede modificar la configuración."))
    return cut("\n".join(lines))
