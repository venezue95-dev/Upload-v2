"""
test_extras.py
==============
Pruebas de las funciones nuevas de la v1.3 (auditoría completa del bot):

  · Velocidad aprendida persistente (SpeedStats): EWMA, límites, siembra.
  · Base de datos: baneos, cuota diaria, búsqueda en el historial, borrado.
  · Ajustes en caliente: los 3 ajustes nuevos y las páginas del teclado.
  · Verificación de integridad (HEAD) tolerante a endpoints sin HEAD.
  · Semáforo dinámico (limiter) y su cambio de límite en caliente.
  · Reanudación de partes ya subidas (`_resume_state`) sin repetir trabajo.
  · Textos: números, estado con velocidades aprendidas, escapado.
  · Plantilla .env y README coherentes con las claves de config.

  python tests/test_extras.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests", "stub_todus"))

_TMP = tempfile.mkdtemp(prefix="todus-extras-")
os.environ.update({
    "API_ID": "1", "API_HASH": "h", "BOT_TOKEN": "1:a", "BOT_OWNER_ID": "999",
    "WORK_DIR": _TMP, "DATA_DIR": os.path.join(_TMP, "data"),
    "DOWNLOAD_DIR": os.path.join(_TMP, "downloads"),
    "RATE_LIMIT_SEC": "0", "MIN_FREE_GB": "0",
})

from config import CONFIG                                  # noqa: E402
from modules import progress as P                          # noqa: E402
from modules import storage, texts as T                    # noqa: E402
from modules.limiter import DynamicSemaphore               # noqa: E402
from modules.queue import TaskStatus, UploadTask           # noqa: E402
from modules import runtime_settings as RS                    # noqa: E402
from modules.runtime_settings import RUNTIME_SETTINGS      # noqa: E402
from modules.speedstats import SPEEDS, SpeedStats          # noqa: E402

PASSED, FAILED = [], []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  ✅ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


class FakeTask:
    """Tarea mínima para probar storage.record_task sin pipeline."""

    def __init__(self, **kw) -> None:
        self.task_id = kw.get("task_id", "t1")
        self.user_id = kw.get("user_id", 42)
        self.username = kw.get("username", "ana")
        self.chat_id = 42
        self.filename = kw.get("filename", "video.mp4")
        self.file_size = kw.get("file_size", 10 * 1024 * 1024)
        self.source = kw.get("source", "telegram")
        self.url = kw.get("url")
        self.status = kw.get("status", TaskStatus.COMPLETED)
        self.mode = kw.get("mode", "classic")
        self.error = kw.get("error")
        self.parts_total = kw.get("parts_total", 1)
        self.parts_done = kw.get("parts_done", 1)
        self.bytes_transferred = kw.get("bytes_transferred", self.file_size)
        self.bytes_fetched = 0
        self.keys = kw.get("keys", [])
        self.keys_by_part = kw.get("keys_by_part", {})
        self.s3_base = kw.get("s3_base", "")
        self.urls = kw.get("urls", [])
        self.created_at = kw.get("created_at", time.time() - 10)
        self.finished_at = kw.get("finished_at", time.time())

    def duration(self) -> float:
        return max(0.0, self.finished_at - self.created_at)

    def to_dict(self):
        return {}


async def main() -> int:
    print("=" * 66)
    print(" PRUEBAS DE FUNCIONES NUEVAS (v1.3)")
    print("=" * 66)

    # ------------------------------------------------------------------
    print("\n▶ Velocidad aprendida (SpeedStats)")
    st = SpeedStats(path=os.path.join(_TMP, "st.json"))
    check("sin datos usa una referencia conservadora", st.upload_estimate() > 0,
          str(st.upload_estimate()))
    check("sin datos el resumen lo dice", "por defecto" in st.summary(), st.summary())
    st.note_upload(10 * 1024 * 1024, 4.0)          # 2,5 MB/s
    first = st.upload_bps
    check("la primera medición pondera mucho", first > 2.0 * 1024 * 1024, str(first))
    st.note_upload(10 * 1024 * 1024, 40.0)         # 256 KB/s
    check("una medición lenta baja la estimación", st.upload_bps < first)
    st.note_upload(1024, 0.0)
    check("una medición de duración 0 no rompe nada", st.upload_bps > 0)
    st.note_upload(10**13, 0.001)
    check("una velocidad absurda se descarta",
          32 * 1024 <= st.upload_bps <= 200 * 1024 * 1024, str(st.upload_bps))
    st.upload_bps = 0.0
    st.seed_from_history(4 * 1024 * 1024, 5 * 1024 * 1024)
    check("se siembra desde el historial (media de subida)",
          abs(st.upload_bps - 4 * 1024 * 1024) < 64 * 1024, str(st.upload_bps))
    check("la estimación usa el mínimo entre subida y bajada",
          st.upload_estimate() > 0)
    resumen = st.summary()
    check("el resumen dice que está aprendido", "aprendida" in resumen, resumen)

    # ------------------------------------------------------------------
    print("\n▶ Base de datos: historial, baneos, cuota y búsqueda")
    storage.init(CONFIG)
    storage.record_task(FakeTask(task_id="a1", filename="playa.mp4"), CONFIG)
    storage.record_task(FakeTask(task_id="a2", filename="concierto.mkv",
                                 user_id=43, username="luis"), CONFIG)
    hist = storage.user_history(42)
    check("el historial guarda la tarea", len(hist) == 1, str(len(hist)))
    check("el historial guarda el tamaño",
          hist[0]["file_size"] == 10 * 1024 * 1024, str(hist[0].get("file_size")))
    check("la búsqueda encuentra por nombre",
          len(storage.search_user_history(42, "play")) == 1)
    check("la búsqueda no inventa resultados",
          storage.search_user_history(42, "zzz") == [])
    check("se puede recuperar una tarea por id",
          storage.get_task("a1", 42) is not None)
    check("no se puede leer la tarea de otro usuario",
          storage.get_task("a1", 999) is None)
    storage.mark_deleted("a1")
    borrada = storage.get_task("a1", 42)
    check("el borrado limpia claves y enlaces",
          borrada is not None and not borrada["keys"] and not borrada["urls"])
    check("tras borrarla sigue existiendo el registro",
          storage.get_task("a1", 42) is not None)

    check("un usuario normal no está baneado", storage.is_banned(42) is False)
    storage.ban_user(42, "spam")
    check("el baneo funciona", storage.is_banned(42) is True)
    lista = storage.banned_users()
    check("el baneo aparece en la lista",
          any(int(b["user_id"]) == 42 for b in lista), str(lista))
    check("el motivo se guarda",
          any("spam" in (b.get("reason") or "") for b in lista))
    check("desbanear funciona", storage.unban_user(42) is True)
    check("ya no está baneado", storage.is_banned(42) is False)
    check("desbanear dos veces no revienta", storage.unban_user(42) is False)

    hoy = storage.bytes_today(43)
    check("la cuota diaria cuenta los bytes de hoy",
          hoy == 10 * 1024 * 1024, str(hoy))
    check("la cuota es por usuario", storage.bytes_today(777) == 0)

    stats = storage.global_stats()
    check("las estadísticas globales cuentan tareas completadas",
          stats.get("completed", 0) >= 2, str(stats)[:110])
    check("las estadísticas globales cuentan usuarios",
          stats.get("users", 0) >= 2, str(stats)[:110])
    up, down = storage.avg_speeds()
    check("avg_speeds no revienta sin muestras", up >= 0 and down >= 0)

    # ------------------------------------------------------------------
    print("\n▶ Ajustes en caliente y teclado")
    for key in ("verify_upload", "daily_quota_mb", "maintenance"):
        check(f"existe el ajuste '{key}'", key in RUNTIME_SETTINGS.as_dict())
        paginas_con_clave = [i for i, page in enumerate(RS.SETTINGS_PAGES) if key in page]
        check(f"el ajuste '{key}' tiene etiqueta y página",
              bool(paginas_con_clave) and key in RS.CYCLE_LABELS,
              str(paginas_con_clave))
    check("verify_upload es booleano",
          isinstance(RUNTIME_SETTINGS.get("verify_upload"), bool))
    check("la cuota diaria es un número",
          isinstance(RUNTIME_SETTINGS.get("daily_quota_mb"), int))
    RUNTIME_SETTINGS.set_value("verify_upload", False)
    check("se puede cambiar en caliente",
          RUNTIME_SETTINGS.get("verify_upload") is False)
    RUNTIME_SETTINGS.set_value("verify_upload", True)
    RUNTIME_SETTINGS.set_value("daily_quota_mb", 10240)
    check("la cuota acepta un valor de la lista",
          RUNTIME_SETTINGS.get("daily_quota_mb") == 10240)
    check("los ajustes se guardan en el disco persistente (volumen)",
          os.path.exists(CONFIG.settings_file), CONFIG.settings_file)
    check("settings.json vive dentro del volumen /app/data",
          os.path.dirname(CONFIG.settings_file).rstrip("/") ==
          CONFIG.work_dir.rstrip("/"), CONFIG.settings_file)
    check("el teclado tiene al menos 3 páginas de ajustes",
          len(RS.SETTINGS_PAGES) >= 3, str(len(RS.SETTINGS_PAGES)))
    todas = [k for page in RS.SETTINGS_PAGES for k in page]
    check("ningún ajuste se repite entre páginas",
          len(todas) == len(set(todas)), str([k for k in todas if todas.count(k) > 1]))
    check("todos los ajustes del teclado tienen etiqueta",
          all(k in RS.CYCLE_LABELS for k in todas),
          str([k for k in todas if k not in RS.CYCLE_LABELS]))
    check("los 3 ajustes nuevos están en el teclado",
          all(k in todas for k in ("verify_upload", "daily_quota_mb", "maintenance")),
          str([k for k in ("verify_upload", "daily_quota_mb", "maintenance")
               if k not in todas]))
    check("el ajuste de mantenimiento tiene etiqueta clara",
          "mantenimiento" in RS.CYCLE_LABELS["maintenance"].lower(),
          RS.CYCLE_LABELS["maintenance"])

    # ------------------------------------------------------------------
    print("\n▶ Verificación de integridad y cliente S3")
    from modules.s3_client import S3Client

    class BackendFalso:
        """Imita al backend: head() devuelve el dict real de la librería todus."""

        name = "falso"
        respuesta = None

        def head(self, key):
            if isinstance(self.respuesta, Exception):
                raise self.respuesta
            return self.respuesta

        def ping(self):
            return True, "ok"

    cliente = S3Client.__new__(S3Client)     # sin construir el backend real
    falso = BackendFalso()
    cliente.backend = falso
    falso.respuesta = None
    ok, detalle = cliente.verify_part("k", 1024)
    check("si HEAD no está soportado no se invalida la subida", ok is True, detalle)
    falso.respuesta = {"size": 1024}
    ok, detalle = cliente.verify_part("k", 1024)
    check("tamaño correcto verificado", ok is True, detalle)
    falso.respuesta = {"size": 10}
    ok, detalle = cliente.verify_part("k", 1024)
    check("tamaño distinto detectado", ok is False, detalle)
    falso.respuesta = {"size": 0}
    ok, _ = cliente.verify_part("k", 1024)
    check("un tamaño sin informar no invalida la subida", ok is True)
    falso.respuesta = RuntimeError("500 del endpoint")
    ok, _ = cliente.verify_part("k", 1024)
    check("un error del endpoint no invalida la subida", ok is True)

    print("\n▶ Semáforo dinámico (limiter)")
    sem = DynamicSemaphore(2, limit_fn=lambda: 2)
    check("el límite inicial es el indicado", sem.limit == 2)
    await sem.acquire()
    await sem.acquire()
    check("bloquea por encima del límite", sem.active == 2)
    pide = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0.05)
    check("la petición extra espera", pide.done() is False)
    sem.release()
    await asyncio.sleep(0.05)
    check("al liberar entra la que esperaba", pide.done() is True)
    sem.release()
    check("no baja de cero", sem.active >= 0)

    # ------------------------------------------------------------------
    print("\n▶ Reanudación de partes ya subidas")
    from modules.actions import resume_state as _resume_state

    tarea = UploadTask(user_id=7, chat_id=7, filename="x.mkv", file_size=100)
    tarea.s3_base = "20260101-120000-ab12cd"
    tarea.keys_by_part = {1: "users/tg_7/telegram/20260101-120000-ab12cd/x.part001.mkv",
                          3: "users/tg_7/telegram/20260101-120000-ab12cd/x.part003.mkv"}
    folder, parts = _resume_state(tarea)
    check("detecta la carpeta del intento anterior", folder == "20260101-120000-ab12cd", str(folder))
    check("detecta las partes ya subidas (con huecos)", sorted(parts) == [1, 3], str(parts))
    # Tras un reinicio solo queda el historial: el nº de parte sale del nombre.
    storage.record_task(FakeTask(
        task_id="r1", user_id=7, status=TaskStatus.FAILED, parts_total=3,
        keys_by_part={3: "users/tg_7/telegram/20260202-000000-ffffff/y.part003.mkv",
                      1: "users/tg_7/telegram/20260202-000000-ffffff/y.part001.mkv"},
    ), CONFIG)
    huerfana = UploadTask(user_id=7, chat_id=7, filename="y.mkv", file_size=100, task_id="r1")
    folder2, parts2 = _resume_state(huerfana)
    check("desde el historial se conservan los números de parte",
          sorted(parts2) == [1, 3] and folder2 == "20260202-000000-ffffff", f"{folder2} {parts2}")
    vacia = UploadTask(user_id=7, chat_id=7, filename="y.mkv", file_size=100)
    check("una tarea nueva no reanuda nada", _resume_state(vacia) == (None, {}))

    # ------------------------------------------------------------------
    print("\n▶ Textos y utilidades")
    check("human_size legible", P.human_size(1536) == "1.5 KB", P.human_size(1536))
    check("human_time legible", P.human_time(90) == "1m 30s", P.human_time(90))
    check("human_speed legible", "MB/s" in P.human_speed(2.5 * 1024 * 1024))
    check("display_number sin ceros inútiles",
          T.display_number(2.0) == "2" and T.display_number(1.5) == "1.5")
    check("display_number con booleanos", T.display_number(True) == "sí")
    check("el escapado de HTML es correcto",
          T.esc("<b>&<") == "&lt;b&gt;&amp;&lt;", T.esc("<b>&<"))
    check("shorten recorta", len(T.shorten("a" * 100, 10)) <= 11)
    check("la barra está completa al 100 %",
          P.bar(100.0, 8).count("⬢") == 8)
    check("el porcentaje se redondea", P.percent_str(33.33) == "33,3 %".replace(" ", "")
          or "33" in P.percent_str(33.33), P.percent_str(33.33))

    estado = T.status_text(
        bot_line="conectado", storage_line="ok",
        settings=RUNTIME_SETTINGS,
        counts={"active": 0, "waiting": 0, "users_waiting": 0, "completed": 3,
                "failed": 0, "max_concurrent": 3, "uptime": 120},
        paused=False, speeds_line="subida 2.50 MB/s (3 muestras)",
    )
    check("status_text muestra las velocidades aprendidas",
          "2.50 MB/s" in estado and "Velocidad aprendida" in estado)
    check("status_text cabe en Telegram", len(estado) < 4096, str(len(estado)))

    # ------------------------------------------------------------------
    print("\n▶ Avisos y registro en canal")
    from modules import notifier

    class ClienteFalso:
        def __init__(self) -> None:
            self.mensajes = []

        async def send_message(self, chat_id, text, **kw):
            self.mensajes.append((chat_id, text))
            return None

    cliente = ClienteFalso()
    notifier._client = cliente

    tarea = UploadTask(user_id=55, chat_id=55, filename="pelicula.mkv",
                       file_size=100 * 1024 * 1024)
    tarea.task_id = "abc123"
    tarea.username = "ana"
    tarea.status = TaskStatus.COMPLETED
    tarea.parts_total = 2
    tarea.mark_started()

    CONFIG.log_channel_id = -1001234567890
    notifier0 = notifier._log_to_channel
    await notifier0(tarea)
    check("la subida completada se registra en el canal",
          len(cliente.mensajes) == 1 and cliente.mensajes[0][0] == -1001234567890,
          str(cliente.mensajes)[:120])
    check("el registro trae nombre, tamaño y usuario",
          "pelicula.mkv" in cliente.mensajes[0][1]
          and "ana" in cliente.mensajes[0][1], cliente.mensajes[0][1][:200])
    check("el registro cabe en un mensaje de Telegram",
          len(cliente.mensajes[0][1]) < 4096)

    CONFIG.log_channel_id = None
    cliente.mensajes.clear()
    await notifier0(tarea)
    check("sin canal configurado no se envía nada", cliente.mensajes == [])

    CONFIG.owner_id = 999
    tarea.status = TaskStatus.FAILED
    tarea.error = "timeout del endpoint"
    notifier._owner_alert_at = 0
    await notifier._alert_owner(tarea)
    check("el aviso de fallo llega al owner",
          cliente.mensajes and cliente.mensajes[-1][0] == 999,
          str(cliente.mensajes)[:120])
    check("el aviso incluye el motivo del fallo",
          "timeout del endpoint" in cliente.mensajes[-1][1])
    cliente.mensajes.clear()
    await notifier._alert_owner(tarea)
    check("no se repite el aviso en bucle (límite por minuto)",
          cliente.mensajes == [])
    notifier._client = None

    print("\n▶ Coherencia de configuración y documentación")
    from pathlib import Path

    env_example = (Path(ROOT) / ".env.example")
    if env_example.exists():
        contenido = env_example.read_text(encoding="utf-8")
        for clave in ("BOT_TOKEN", "BOT_OWNER_ID", "S3_CHUNK_SIZE_MB"):
            check(f".env.example documenta {clave}", clave in contenido)
    else:
        check(".env.example existe", False)

    readme = (Path(ROOT) / "README.md").read_text(encoding="utf-8")
    for comando in ("/buscar", "/ban", "/unban", "/baneados", "/mantenimiento",
                    "/status"):
        check(f"el README menciona {comando}", comando in readme)

    cam = (Path(ROOT) / "CAMBIOS.md").read_text(encoding="utf-8")
    check("CAMBIOS.md explica el arreglo del progreso",
          "progreso" in cam.lower() and "RateMeter" in cam)

    print("\n" + "=" * 66)
    print(f" ✅ Correctas: {len(PASSED)}   ❌ Fallidas: {len(FAILED)}")
    for item in FAILED:
        print(f"    · {item}")
    print("=" * 66)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
