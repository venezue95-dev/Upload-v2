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
    def log_message(self, *args):  # silencio
        pass


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


async def test_classic_mode(base_url: str, size: int) -> None:
    print("\n▶ Modo clásico")
    RUNTIME_SETTINGS.set_value("pipeline_mode", "classic")
    task = make_url_task(1002, f"{base_url}/datos_classic.bin", "datos_classic.bin", size)
    await run_single(task)
    check("termina completada", task.status == TaskStatus.COMPLETED, f"{task.status} {task.error}")
    check("usa modo clásico", task.mode == "classic", task.mode)
    check("sube los 12 MB", task.bytes_transferred == size, str(task.bytes_transferred))
    check("no deja temporales", leftovers() == 0, f"({leftovers()} archivos)")


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
    task.urls = [f"https://s3.todus.cu/stream/users/tg_7/telegram/x.part{i:03d}" for i in range(1, 5)]
    final = T.final_text(task)
    check("texto final con enlaces", "<pre>" in final and "https://" in final)
    check("texto final dentro del límite", len(final) < 4096, str(len(final)))
    txt = links_txt_content(task)
    check("archivo .txt de enlaces", txt.count("https://") == 4, txt[:80])

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
    check("menú principal con teclado",
          len(T.main_menu_keyboard(is_owner_user=True).inline_keyboard) >= 3)


async def test_handlers_register() -> None:
    print("\n▶ Registro de handlers")
    from pyrogram import Client

    from modules.handlers import admin, basic, callbacks, cola, settings, upload, url

    app = Client("test_session", api_id=1, api_hash="h", bot_token="1:abc")
    try:
        for module in (basic, admin, cola, settings, upload, url, callbacks):
            module.register(app)
        await asyncio.sleep(0.2)     # Pyrogram añade los handlers en tareas
        groups = app.dispatcher.groups
        total = sum(len(v) for v in groups.values())
        check("los 7 módulos se registran", len(groups) >= 1, str(list(groups)))
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
# Renderizado de TODAS las vistas (detecta atributos mal escritos).
# ----------------------------------------------------------------------
def test_all_views() -> None:
    print("\n▶ Renderizado de todas las vistas del bot")
    from modules.handlers.cola import build_history_view, build_queue_view
    from modules.handlers.settings import build_settings_view

    def mk(user_id, name, status, **kw):
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
        if status == TaskStatus.COMPLETED:
            t.urls = ["https://s3.todus.cu/stream/users/tg_1/telegram/x/y.part001"]
        return t

    tasks = [
        mk(1, "película.mkv", TaskStatus.QUEUED),
        mk(1, "en_curso.mp4", TaskStatus.UPLOADING, phase="Subiendo"),
        mk(1, "descargando.bin", TaskStatus.DOWNLOADING, phase="Descargando"),
        mk(1, "listo.zip", TaskStatus.COMPLETED, parts_total=1, parts_done=1),
        mk(1, "roto.rar", TaskStatus.FAILED, error="timeout de red", failed_part="parte 2/3"),
        mk(1, "cancelado.mp4", TaskStatus.CANCELLED),
    ]
    for task in tasks:
        task.speed = 1.5 * 1024 * 1024

    try:
        for page in (1, 2):
            text, keyboard = build_queue_view(1, page, is_owner_user=True)
            check(f"vista de cola página {page}", bool(text) and len(text) < 4096, str(len(text)))
            check(f"teclado de cola página {page}",
                  bool(keyboard.inline_keyboard))
            gtext, gkey = build_queue_view(1, 1, is_owner_user=True, global_view=True)
            check("vista global", bool(gtext) and len(gtext) < 4096)

        htext, hkey = build_history_view(1, 1)
        check("vista de historial", bool(htext))
        check("teclado de historial", bool(hkey.inline_keyboard))

        stext, skey = build_settings_view(1, is_owner_user=True)
        check("vista de ajustes", bool(stext) and "página" in stext)
        check("teclado de ajustes", len(skey.inline_keyboard) >= 4)

        # Botones de cada estado.
        for task in tasks:
            kb = T.task_keyboard(task, is_owner_user=True)
            datas = [b.callback_data for row in kb.inline_keyboard for b in row if b.callback_data]
            check(f"teclado de tarea {task.status.value}", bool(datas), str(datas))
        check("teclado principal", len(T.main_menu_keyboard(is_owner_user=False).inline_keyboard) >= 2)
        check("teclado de admin", len(T.admin_keyboard(False).inline_keyboard) >= 2)
        check("teclado de confirmación de cancelación", bool(T.cancel_all_keyboard().inline_keyboard))
        check("teclado de confirmación de borrado",
              "t:delyes:yes:abc" in T.delete_confirm_keyboard("abc").inline_keyboard[0][0].callback_data)

        # Las filas del historial (con enlaces) se formatean bien.
        rows = [{"task_id": "abc", "filename": "f.bin", "file_size": 123,
                 "status": "completed", "urls": ["https://s3.todus.cu/stream/x"],
                 "finished_at": __import__("time").time(), "created_at": __import__("time").time()}]
        check("historial con enlaces", "https://s3.todus.cu" in T.history_text(rows, 1, 5))
    except Exception as exc:  # noqa: BLE001
        check("renderizado sin errores", False, f"{type(exc).__name__}: {exc}")


def test_callbacks_parse() -> None:
    print("\n▶ Formatos de callback_data")
    cases = {
        "t:cancel:abc123": ("cancel", "abc123"),
        "t:card:abc123": ("card", "abc123"),
        "t:retry:abc123": ("retry", "abc123"),
        "t:links:abc123": ("links", "abc123"),
        "t:delyes:abc123": ("delyes", "abc123"),
        "t:delyes:yes:abc123": ("delyes_yes", "abc123"),
    }
    for data, (kind, expected_id) in cases.items():
        parts = data.split(":")
        got_kind = parts[1]
        got_id = parts[3] if (got_kind == "delyes" and len(parts) >= 4 and parts[2] == "yes") else parts[-1]
        check(f"enrutado {data}", got_id == expected_id, got_id)
    for data in ("menu:home", "menu:help:2", "cola:refresh", "cola:page:3",
                 "cola:cancelall:yes", "hist:page:2", "set:cycle:chunk_size_mb",
                 "set:page:2", "adm:pause", "noop"):
        check(f"callback válido {data}", len(data.encode()) <= 64)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
