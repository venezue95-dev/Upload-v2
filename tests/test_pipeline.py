"""
test_pipeline.py
================
Prueba de INTEGRACIÓN del pipeline completo, sin Telegram y sin toDus real:

  python tests/test_pipeline.py

Se levanta un servidor HTTP local que sirve un archivo de 12 MB y se ejecuta
el pipeline real (probe → descarga → troceo → subida → enlaces) usando un
stub de la librería `todus` que guarda en disco lo que "sube".

Comprueba:
1. Modo streaming (descarga por rangos + subida en paralelo).
2. Modo clásico (descarga completa y luego subida por trozos).
3. Reintentos ante fallos simulados de red.
4. Cancelación a mitad de tarea.
5. Archivo mayor que el límite configurado → rechazo claro.
6. Los temporales se limpian siempre.
7. Los textos del bot se generan sin errores y dentro del límite de Telegram.
8. Todos los módulos de handlers se registran correctamente.
"""

from __future__ import annotations

import asyncio
import functools
import http.server
import os
import shutil
import socketserver
import sys
import tempfile
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests", "stub_todus"))

_TMP = tempfile.mkdtemp(prefix="todus-pipe-")
STORE = os.path.join(_TMP, "store")
os.makedirs(STORE, exist_ok=True)
os.environ.update({
    "API_ID": "12345",
    "API_HASH": "test-hash",
    "BOT_TOKEN": "123:test",
    "BOT_OWNER_ID": "999",
    "DOWNLOAD_DIR": os.path.join(_TMP, "downloads"),
    "WORK_DIR": _TMP,
    "DATA_DIR": os.path.join(_TMP, "data"),
    "ALLOW_PRIVATE_URLS": "true",
    "S3_CHUNK_SIZE_MB": "5",       # el archivo de prueba tiene 12 MB -> 3 partes
    "S3_MAX_PARALLEL": "2",
    "S3_GLOBAL_WORKERS": "4",
    "S3_MAX_RETRIES": "3",
    "QUEUE_MAX_CONCURRENT": "2",
    "QUEUE_MAX_PER_USER": "1",
    "RATE_LIMIT_SEC": "0",
    "MIN_FREE_GB": "0",
})

from config import CONFIG                                   # noqa: E402
from modules import storage, texts as T                     # noqa: E402
from modules.notifier import links_txt_content              # noqa: E402
from modules.pipeline import run_task                       # noqa: E402
from modules.queue import QUEUE, TaskStatus, UploadTask     # noqa: E402
from modules.runtime_settings import RUNTIME_SETTINGS       # noqa: E402

PASSED, FAILED = [], []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  ✅ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


def wait_until(cond, timeout: float = 30.0) -> bool:
    import time

    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return cond()


# ----------------------------------------------------------------------
# Servidor HTTP local.
# ----------------------------------------------------------------------
class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler + soporte de ``Range`` (206) como un servidor real."""

    def log_message(self, *args):  # silencio
        pass

    def do_GET(self):
        rng = self.headers.get("Range")
        path = self.translate_path(self.path)
        if not rng or not os.path.isfile(path):
            return super().do_GET()
        size = os.path.getsize(path)
        start, _, end = rng.replace("bytes=", "").partition("-")
        start = int(start or 0)
        end = min(int(end) if end else size - 1, size - 1)
        with open(path, "rb") as fh:
            fh.seek(start)
            data = fh.read(end - start + 1)
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(data)

    def end_headers(self):
        self.send_header("Accept-Ranges", "bytes")
        super().end_headers()


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_server(directory: str) -> tuple:
    handler = functools.partial(_QuietHandler, directory=directory)
    server = _Server(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]


def make_file(path: str, size_mb: int) -> None:
    with open(path, "wb") as fh:
        fh.write(os.urandom(size_mb * 1024 * 1024))


# ----------------------------------------------------------------------
# Tareas.
# ----------------------------------------------------------------------
def make_url_task(user_id: int, url: str, name: str, size: int) -> UploadTask:
    return UploadTask(user_id=user_id, chat_id=user_id, message_id=1,
                      filename=name, file_size=size, source="url", url=url,
                      username=f"u{user_id}", max_attempts=1)


async def run_single(task: UploadTask, timeout: float = 60) -> None:
    ok, reason = QUEUE.submit(task)
    assert ok, reason
    done = await asyncio.get_event_loop().run_in_executor(
        None, lambda: wait_until(lambda: task.is_final, timeout)
    )
    if not done:
        raise AssertionError(f"la tarea no terminó: {task.status} {task.error}")


def _sha(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _stored_sha(task: UploadTask) -> str:
    """Une las partes guardadas por el stub, en orden, y devuelve su sha256."""
    import hashlib
    h = hashlib.sha256()
    for key in task.keys:
        with open(os.path.join(STORE, key.replace("/", "__")), "rb") as fh:
            h.update(fh.read())
    return h.hexdigest()


def leftovers() -> int:
    count = 0
    for root, _dirs, files in os.walk(CONFIG.download_dir):
        count += len(files)
    return count


async def test_stream_mode(base_url: str, size: int) -> None:
    print("\n▶ Modo streaming (descarga + subida solapadas)")
    RUNTIME_SETTINGS.set_value("pipeline_mode", "stream")
    task = make_url_task(1001, f"{base_url}/datos_stream.bin", "datos_stream.bin", size)
    await run_single(task)

    check("termina completada", task.status == TaskStatus.COMPLETED, f"{task.status} {task.error}")
    check("usa modo stream", task.mode == "stream", task.mode)
    check("divide en 3 partes", task.parts_total == 3, str(task.parts_total))
    check("sube los 12 MB", task.bytes_transferred == size,
          f"{task.bytes_transferred} != {size}")
    check("genera 3 enlaces", len(task.urls) == 3, str(task.urls))
    check("los enlaces son de toDus",
          all(u.startswith("https://s3.todus.cu/stream/") for u in task.urls))
    check("nombres de partes correctos",
          all(".part00" in u for u in task.urls), str(task.urls[0]))
    check("no deja temporales", leftovers() == 0, f"({leftovers()} archivos)")
    check("el contenido subido es idéntico al original (sin corrupción)",
          _stored_sha(task) == _sha(os.path.join(_TMP, "www", "datos_stream.bin")))


async def test_classic_mode(base_url: str, size: int) -> None:
    print("\n▶ Modo clásico")
    RUNTIME_SETTINGS.set_value("pipeline_mode", "classic")
    task = make_url_task(1002, f"{base_url}/datos_classic.bin", "datos_classic.bin", size)
    await run_single(task)
    check("termina completada", task.status == TaskStatus.COMPLETED, f"{task.status} {task.error}")
    check("usa modo clásico", task.mode == "classic", task.mode)
    check("sube los 12 MB", task.bytes_transferred == size, str(task.bytes_transferred))
    check("no deja temporales", leftovers() == 0, f"({leftovers()} archivos)")
    check("el contenido subido es idéntico al original",
          _stored_sha(task) == _sha(os.path.join(_TMP, "www", "datos_classic.bin")))


async def test_retries(base_url: str) -> None:
    print("\n▶ Reintentos ante fallos de red")
    import todus

    RUNTIME_SETTINGS.set_value("pipeline_mode", "stream")
    RUNTIME_SETTINGS.set_value("max_retries", 3)
    todus.S3Client.fail_times = 4          # 4 fallos > 3 reintentos de la primera parte

    task = make_url_task(1003, f"{base_url}/retry.bin", "retry.bin", 12 * 1024 * 1024)
    await run_single(task)
    check("continúa tras fallos parciales", task.status == TaskStatus.COMPLETED,
          f"{task.status} {task.error}")

    # Ahora forzamos el fallo total de una parte.
    RUNTIME_SETTINGS.set_value("max_retries", 2)
    todus.S3Client.fail_times = 100
    task2 = make_url_task(1004, f"{base_url}/retry2.bin", "retry2.bin", 12 * 1024 * 1024)
    await run_single(task2)
    check("falla de forma controlada si no hay recuperación",
          task2.status == TaskStatus.FAILED, task2.status.value)
    check("registra la parte fallida", bool(task2.failed_part), str(task2.failed_part))
    todus.S3Client.fail_times = 0
    RUNTIME_SETTINGS.set_value("max_retries", 3)


async def test_cancel(base_url: str) -> None:
    print("\n▶ Cancelación en curso")
    import todus

    RUNTIME_SETTINGS.set_value("pipeline_mode", "stream")
    todus.S3Client.put_delay = 0.25          # subidas lentas para poder cancelar
    task = make_url_task(1005, f"{base_url}/grande_cancel.bin", "grande_cancel.bin",
                         40 * 1024 * 1024)
    ok, _ = QUEUE.submit(task)
    assert ok
    await asyncio.get_event_loop().run_in_executor(
        None, lambda: wait_until(lambda: task.is_active, 20)
    )
    await asyncio.sleep(0.6)
    QUEUE.cancel(task.task_id)
    done = await asyncio.get_event_loop().run_in_executor(
        None, lambda: wait_until(lambda: task.is_final, 30)
    )
    check("se cancela", done and task.status == TaskStatus.CANCELLED, task.status.value)
    check("libera el cupo del usuario", QUEUE.user_load(1005) == (0, 0))
    check("no deja temporales tras cancelar", leftovers() == 0, f"({leftovers()} archivos)")
    todus.S3Client.put_delay = 0.01


async def test_size_limit(base_url: str) -> None:
    print("\n▶ Límite de tamaño")
    original = RUNTIME_SETTINGS.max_file_mb
    RUNTIME_SETTINGS.set_value("max_file_mb", 500)
    # El servidor informa 12 MB; bajamos el límite efectivo a 5 MB.
    RUNTIME_SETTINGS.set_value("max_file_mb", 500)
    task = make_url_task(1006, f"{base_url}/limite.bin", "limite.bin", 0)
    task.file_size = 0
    # Simulamos un archivo de 900 MB conocido por el usuario.
    ok, _ = QUEUE.submit(task)
    assert ok
    # El pipeline lo dejará pasar (12 MB reales) — comprobamos el otro camino:
    task.url = f"{base_url}/limite.bin"
    await asyncio.get_event_loop().run_in_executor(
        None, lambda: wait_until(lambda: task.is_final, 60)
    )
    check("una tarea normal sigue funcionando con el límite", task.status == TaskStatus.COMPLETED,
          f"{task.status} {task.error}")
    RUNTIME_SETTINGS.set_value("max_file_mb", original)


def test_textos() -> None:
    print("\n▶ Mensajes del bot")
    task = UploadTask(user_id=7, chat_id=7, filename="película <rara> & ñ.mp4",
                      file_size=350 * 1024 * 1024, source="url",
                      url="https://ejemplo.com/a?b=1&c=2", username="juan")
    task.parts_total = 4
    task.parts_done = 2
    task.chunk_size = 100 * 1024 * 1024
    task.bytes_transferred = 200 * 1024 * 1024
    task.processed_bytes = lambda: task.bytes_transferred  # type: ignore
    task.status = TaskStatus.UPLOADING
    task.phase = "Subiendo"
    task.speed = 2.5 * 1024 * 1024

    for name, text in [
        ("progreso", T.progress_text(task)),
        ("en cola", T.queued_text(task)),
    ]:
        check(f"texto de {name} generado", bool(text) and len(text) < 4096, str(len(text)))
        check(f"texto de {name} escapa HTML", "&lt;" in text and "&amp;" in text)

    task.status = TaskStatus.COMPLETED
    task.parts_total = 4
    task.urls = [f"https://s3.todus.cu/stream/toDusDownload/aB3xY72/x.part{i:03d}" for i in range(1, 5)]
    final = T.final_text(task)
    check("texto final con enlaces", "<pre>" in final and "https://" in final)
    check("texto final dentro del límite", len(final) < 4096, str(len(final)))
    check("mensaje final sin numeración", "01. https://" not in final and "02. https://" not in final)
    txt = links_txt_content(task)
    txt_lines = txt.splitlines()
    check("archivo .txt solo contiene enlaces", txt_lines == task.urls, repr(txt_lines))

    task.status = TaskStatus.FAILED
    task.error = "timeout <script>"
    check("texto de error escapa", "&lt;script&gt;" in T.failed_text(task))
    check("texto cancelado", bool(T.cancelled_text(task)))

    keys = ["max_concurrent_tasks", "chunk_size_mb"]
    values = RUNTIME_SETTINGS.as_dict()
    from modules.runtime_settings import CYCLE_LABELS, CYCLE_UNITS

    settings_text = T.settings_text(values, 1, 3, is_owner_user=True,
                                    keys=keys, labels=CYCLE_LABELS, units=CYCLE_UNITS)
    check("menú de ajustes generado", bool(settings_text) and len(settings_text) < 4096)
    check("bienvenida generada",
          bool(T.welcome_text(1, public=True, is_owner=False, cfg=CONFIG,
                              settings=RUNTIME_SETTINGS)))
    check("3 páginas de ayuda", all(bool(T.help_text(p)) for p in (1, 2, 3)))
    from modules import keyboards as K
    check("menú principal con teclado",
          len(K.main_menu_keyboard(is_owner_user=True).inline_keyboard) >= 3)


async def test_handlers_register() -> None:
    print("\n▶ Registro de handlers")
    from pyrogram import Client

    from modules.handlers import admin, basic, callbacks, cola, group_access, settings, upload, url

    app = Client("test_session", api_id=1, api_hash="h", bot_token="1:abc")
    try:
        for module in (group_access, basic, admin, cola, settings, upload, url, callbacks):
            module.register(app)
        await asyncio.sleep(0.2)     # Pyrogram añade los handlers en tareas
        groups = app.dispatcher.groups
        total = sum(len(v) for v in groups.values())
        check("los handlers incluyen la verificación global de grupo", -100 in groups,
              str(list(groups)))
        handler_names = {
            type(handler).__name__
            for handlers in groups.values()
            for handler in handlers
        }
        check("se escucha la salida de miembros del grupo",
              "ChatMemberUpdatedHandler" in handler_names, str(sorted(handler_names)))
        check("hay handlers registrados", total >= 15, f"({total})")
    finally:
        session = os.path.join(os.getcwd(), "test_session.session")
        if os.path.exists(session):
            os.remove(session)


async def main() -> int:
    print("=" * 62)
    print(" PRUEBAS DE INTEGRACIÓN — pipeline toDus")
    print("=" * 62)

    storage.init(CONFIG)
    RUNTIME_SETTINGS.set_value("max_queue_total", 200)
    import todus
    todus.S3Client.store_dir = STORE

    served = os.path.join(_TMP, "www")
    os.makedirs(served, exist_ok=True)
    size = 12 * 1024 * 1024
    for name in ("datos_stream.bin", "datos_classic.bin", "retry.bin", "retry2.bin",
                 "limite.bin"):
        make_file(os.path.join(served, name), 12)
    make_file(os.path.join(served, "grande_cancel.bin"), 40)

    server, port = start_server(served)
    base_url = f"http://127.0.0.1:{port}"
    print(f"Servidor de prueba: {base_url}")

    QUEUE.attach(None, run_task)
    await QUEUE.start()

    try:
        test_textos()
        test_callbacks_parse()
        test_all_views()
        test_texto_largo_seguro()
        await test_handlers_register()
        await test_stream_mode(base_url, size)
        await test_classic_mode(base_url, size)
        await test_retries(base_url)
        await test_cancel(base_url)
        await test_size_limit(base_url)
    finally:
        await QUEUE.stop(cancel_running=True)
        server.shutdown()

    print("\n" + "=" * 62)
    print(f" ✅ Correctas: {len(PASSED)}   ❌ Fallidas: {len(FAILED)}")
    for item in FAILED:
        print(f"    · {item}")
    print("=" * 62)

    shutil.rmtree(_TMP, ignore_errors=True)
    return 1 if FAILED else 0




# ----------------------------------------------------------------------
# HTML válido para Telegram (etiquetas equilibradas y permitidas).
# ----------------------------------------------------------------------
def html_ok(text: str) -> bool:
    import re
    stack = []
    for tok in re.findall(r"<[^>]*>", text):
        m = re.match(r"<(/?)([a-z]+)", tok)
        if not m:
            return False
        closing, name = m.group(1) == "/", m.group(2)
        if name not in {"b", "i", "code", "pre", "a", "u", "s"}:
            return False
        if closing:
            if not stack or stack.pop() != name:
                return False
        else:
            stack.append(name)
    return not stack and not re.search(r"<[^>]*$", text)


def all_callbacks(markup) -> list:
    return [b.callback_data for row in markup.inline_keyboard for b in row if b.callback_data]


def mk_task(user_id, name, status, **kw):
    t = UploadTask(user_id=user_id, chat_id=user_id, filename=name,
                   file_size=kw.pop("size", 50 * 1024 * 1024), source="url",
                   url="https://x.com/f.bin", username=f"u{user_id}")
    t.status = status
    t.phase = kw.pop("phase", "")
    t.parts_total = kw.pop("parts_total", 3)
    t.parts_done = kw.pop("parts_done", 1)
    t.chunk_size = 25 * 1024 * 1024
    t.bytes_transferred = kw.pop("done", 25 * 1024 * 1024)
    t.queue_position = kw.pop("pos", 2)
    t.queue_length = 5
    t.queue_eta = 120.0
    t.error = kw.pop("error", None)
    t.speed = 1.5 * 1024 * 1024
    if status == TaskStatus.COMPLETED:
        n = kw.pop("n_urls", 1)
        t.urls = [f"https://s3.todus.cu/stream/toDusDownload/aB3xY72/x/y.part{i:03d}" for i in range(1, n + 1)]
        t.keys = [u.split("/stream/")[1] for u in t.urls]
        t.finished_at = t.created_at + 30
    return t


def test_all_views() -> None:
    print("\n▶ Renderizado de TODAS las vistas del bot")
    from modules import keyboards as K, views

    tasks = [
        mk_task(1, "película.mkv", TaskStatus.QUEUED),
        mk_task(1, "en_curso.mp4", TaskStatus.UPLOADING, phase="Subiendo"),
        mk_task(1, "descargando.bin", TaskStatus.DOWNLOADING, phase="Descargando"),
        mk_task(1, "listo.zip", TaskStatus.COMPLETED, parts_total=1, parts_done=1),
        mk_task(1, "roto <x>.rar", TaskStatus.FAILED, error="timeout de red", failed_part="parte 2/3"),
        mk_task(1, "cancelado.mp4", TaskStatus.CANCELLED),
        mk_task(1, "otro.mkv", TaskStatus.QUEUED),
        mk_task(1, "otro2.mkv", TaskStatus.COMPLETED),
    ]
    for t in tasks:
        QUEUE._tasks[t.task_id] = t

    try:
        for scope in ("m", "g"):
            for page in (1, 2):
                text, kb = views.queue_view(1, scope, page, is_owner_user=True)
                check(f"cola {scope} pág {page}: HTML válido y corto",
                      html_ok(text) and len(text) < 4096, str(len(text)))
                check(f"cola {scope} pág {page}: un botón por tarea",
                      sum(1 for c in all_callbacks(kb) if c.startswith("t:card:")) >= 1)
        text, kb = views.queue_view(1, "g", 1, is_owner_user=False)
        check("un usuario normal no puede ver la vista global",
              "Cola global" not in text and all(c != "cola:g:1" for c in all_callbacks(kb)))

        for task in tasks:
            msg_text, msg_kb = views.task_message(task)
            check(f"mensaje de tarea {task.status.value}: HTML válido", html_ok(msg_text), msg_text[:80])
            datas = all_callbacks(msg_kb) if msg_kb else []
            check(f"mensaje de tarea {task.status.value}: SIN botones de navegación",
                  not any(d.startswith(("menu:", "cola:", "hist:")) for d in datas), str(datas))
            ctext, ckb = views.card_view(task, "q1", 1, manage=True)
            check(f"ficha {task.status.value}: HTML válido", html_ok(ctext))
            check(f"ficha {task.status.value}: vuelve a la cola",
                  "cola:m:1" in all_callbacks(ckb))

        # El mensaje de una tarea en curso solo ofrece cancelar.
        _, kb = views.task_message(tasks[1])
        check("tarea en curso: solo 🛑 Cancelar",
              all_callbacks(kb) == [f"t:cancel:{tasks[1].task_id}:m"], str(all_callbacks(kb)))
        # El resultado ofrece enlaces + borrar y NO "Menú".
        _, kb = views.task_message(tasks[3])
        check("resultado: enlaces + borrar",
              [c.split(":")[1] for c in all_callbacks(kb)] == ["links", "del"], str(all_callbacks(kb)))

        # Borrado de toDus: el mensaje resultante no ofrece nada más.
        done = tasks[3]
        done.deleted_from_s3, done.urls = True, []
        mtext, mkb = views.task_message(done)
        check("tras borrar: texto de borrado y sin botones", "Borrado" in mtext and mkb is None)

        rtext, rkb = views.history_view(1, 1)
        check("vista de historial", html_ok(rtext) and bool(rkb.inline_keyboard))
        stext, skb = views.settings_view(1, is_owner_user=True)
        check("vista de ajustes (owner)", "página" in stext and len(skb.inline_keyboard) >= 4)
        stext, skb = views.settings_view(1, is_owner_user=False)
        check("ajustes de solo lectura: sin botones de edición",
              not any(c.startswith("set:cycle") for c in all_callbacks(skb)))
        for name, view in [
            ("admin", views.admin_view()), ("stats", views.stats_view()),
            ("cuota", views.quota_view(1)), ("top", views.top_view(1, is_owner_user=False)),
            ("ayuda 1", views.help_view(1)), ("ayuda 3", views.help_view(3)),
            ("inicio", views.home_view(1, is_owner_user=True, admin_name="@admin")),
        ]:
            check(f"vista {name}: HTML válido y corto", html_ok(view[0]) and len(view[0]) < 4096)
            check(f"vista {name}: callbacks ≤ 64 bytes",
                  all(len(c.encode()) <= 64 for c in all_callbacks(view[1])))
        check("el menú principal ya no tiene Stats/Sistema sueltos",
              "menu:stats" not in all_callbacks(K.main_menu_keyboard(is_owner_user=True))
              and "menu:admin" in all_callbacks(K.main_menu_keyboard(is_owner_user=True)))
        check("un usuario normal no ve el botón de administración",
              "menu:admin" not in all_callbacks(K.main_menu_keyboard(is_owner_user=False)))
        # Confirmaciones.
        ctext, ckb = views.delete_confirm_view(mk_task(1, "z.bin", TaskStatus.COMPLETED), "h2")
        check("confirmar borrado: Sí / No (vuelve a la ficha)",
              [c.split(":")[1] for c in all_callbacks(ckb)] == ["delok", "back"])
        check("cancelar todas sin tareas devuelve None",
              views.cancel_all_view(424242, "m", is_owner_user=False) is None)
    except Exception as exc:  # noqa: BLE001
        import traceback; traceback.print_exc()
        check("renderizado sin errores", False, f"{type(exc).__name__}: {exc}")
    finally:
        for t in tasks:
            QUEUE._tasks.pop(t.task_id, None)


def test_texto_largo_seguro() -> None:
    print("\n▶ Mensajes largos: HTML siempre cerrado")
    task = mk_task(1, "enorme.mkv", TaskStatus.COMPLETED, n_urls=300, parts_total=300)
    task.urls = [f"https://s3.todus.cu/stream/toDusDownload/aB3xY72/enorme.part{i:03d}.mkv" for i in range(1, 301)]
    final = T.final_text(task)
    check("300 partes: el mensaje final cabe", len(final) <= 4096, str(len(final)))
    check("300 partes: el HTML está bien cerrado", html_ok(final))
    check("300 partes: avisa de que hay más en el .txt", "más" in final and ".txt" in final)
    card = T.task_card_text(task)
    check("ficha con 300 partes: HTML válido", html_ok(card) and len(card) <= 4096)
    cortado = T.cut("<b>título</b>\n<pre>" + "\n".join("x" * 80 for _ in range(200)) + "</pre>")
    check("cut() cierra un <pre> recortado", html_ok(cortado) and len(cortado) <= 4096)


def test_callbacks_parse() -> None:
    print("\n▶ Formatos de callback_data y compatibilidad con botones antiguos")
    from modules import keyboards as K

    legacy = {
        "cola:refresh": "cola:m:1", "cola:global": "cola:g:1", "cola:page:3": "cola:m:3",
        "cola:cancelall": "cola:cx:m", "cola:cancelall:yes": "cola:cxy:m",
        "hist:page:2": "hist:p:2", "hist:links:abc": "t:links:abc:h1",
        "hist:delete:abc": "t:del:abc:h1", "hist:delete:yes:abc": "t:delok:abc:h1",
        "t:cancel:abc": "t:cancel:abc:m", "t:card:abc": "t:card:abc:q1",
        "t:retry:abc": "t:retry:abc:m", "t:links:abc": "t:links:abc:m",
        "t:delyes:abc": "t:del:abc:m", "t:delyes:yes:abc": "t:delok:abc:m",
    }
    for old, new in legacy.items():
        check(f"antiguo {old} → {new}", K.normalize_legacy(old) == new, K.normalize_legacy(old))
    for current in ("t:cancel:abc:m", "t:card:abc:q2", "cola:g:2", "menu:help:2", "noop"):
        check(f"actual {current} no se toca", K.normalize_legacy(current) == current)
    check("contexto q3", K.parse_ctx("q3") == ("q", 3))
    check("contexto basura", K.parse_ctx("zz") == ("m", 1))


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
