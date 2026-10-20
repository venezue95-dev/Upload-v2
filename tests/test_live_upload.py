"""
test_live_upload.py
===================
Prueba REAL (servidor HTTP local) del progreso de subida en vivo:

 1. El PUT envía el archivo íntegro con Content-Length, Content-Type deducido
    del NOMBRE visible (no del temporal .tmp) y Content-Disposition.
 2. El callback de bytes es monótono, llega al tamaño total y se reparte en el
    tiempo (no un único salto al final).
 3. Cancelar a mitad de una parte aborta el PUT sin esperar a que termine.
 4. Un código no 2xx se informa como error.
 5. Integrado en la tarea: bytes reales, velocidad medida, ETA coherente y sin
    «crédito» inventado cuando el backend informa bytes.
 6. Los reintentos de una parte reinician su contador sin falsear el total.
 7. El SpeedMeter no cae a 0 B/s en los huecos entre bloques.

  python tests/test_live_upload.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests", "stub_todus"))

_TMP = tempfile.mkdtemp(prefix="todus-live-")
os.environ.update({
    "API_ID": "1", "API_HASH": "h", "BOT_TOKEN": "1:a", "BOT_OWNER_ID": "999",
    "WORK_DIR": _TMP, "DATA_DIR": os.path.join(_TMP, "data"),
    "DOWNLOAD_DIR": os.path.join(_TMP, "downloads"),
    "S3_CHUNK_SIZE_MB": "50", "RATE_LIMIT_SEC": "0", "MIN_FREE_GB": "0",
})

import requests                                              # noqa: E402
import todus                                                 # noqa: E402
from modules import progress as P                            # noqa: E402
from modules.queue import TaskCancelled, TaskStatus, UploadTask  # noqa: E402
from modules import s3_client                                # noqa: E402

PASSED, FAILED = [], []


def check(name, cond, detail=""):
    (PASSED if cond else FAILED).append(name if cond else f"{name} {detail}")
    print(f"  {'✅' if cond else '❌'} {name} {'' if cond else detail}")


RECEIVED = {}
STATE = {"status": 200, "delay": 0.0}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silencio
        pass

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = b""
        while len(body) < length:
            chunk = self.rfile.read(min(65536, length - len(body)))
            if not chunk:
                break
            body += chunk
            if STATE["delay"]:
                time.sleep(STATE["delay"])
        RECEIVED[self.path] = {"body": body, "headers": dict(self.headers)}
        self.send_response(STATE["status"])
        self.send_header("Content-Length", "0")
        self.end_headers()


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{server.server_address[1]}"


class FakeClient:
    """Cliente mínimo: solo API pública (session + build_url), sin _request."""
    chunk_size = 64 * 1024

    def __init__(self, **kw):
        self.session = requests.Session()

    def build_url(self, key):
        return f"{BASE}/{key}"

    def put(self, **kw):  # solo se usaría en el modo de reserva
        raise AssertionError("no debería usarse el put() de la librería")


todus.S3Client = FakeClient
backend = s3_client._LowLevelBackend()
SIZE = 3 * 1024 * 1024
path = os.path.join(_TMP, "p001.tmp")
payload = os.urandom(SIZE)
open(path, "wb").write(payload)


def main() -> int:
    print("\n▶ 1 · PUT en vivo contra un servidor local")
    check("el backend declara progreso en vivo", backend.supports_progress is True)
    STATE.update(status=200, delay=0.01)
    seen = []
    t0 = time.monotonic()
    backend.put_file(path, "u/video.part001.mp4", "Película ñ.mp4",
                     progress=lambda n: seen.append((time.monotonic() - t0, n)))
    got = RECEIVED["/u/video.part001.mp4"]
    check("el servidor recibió el archivo íntegro", got["body"] == payload)
    check("Content-Length correcto", got["headers"].get("Content-Length") == str(SIZE))
    check("Content-Type deducido del nombre (video/mp4)",
          got["headers"].get("Content-Type") == "video/mp4", got["headers"].get("Content-Type"))
    check("Content-Disposition con nombre UTF-8",
          "filename*=UTF-8''" in got["headers"].get("Content-Disposition", ""))
    values = [n for _, n in seen]
    check("callback monótono", all(a <= b for a, b in zip(values, values[1:])))
    check("llega al tamaño total", values and values[-1] == SIZE, str(values[-1:]))
    check("progreso repartido en el tiempo (≥ 20 avisos)", len(values) >= 20, str(len(values)))
    mid = [n for t, n in seen if t > (seen[-1][0] / 2)]
    check("a mitad del tiempo no está ni en 0 ni en 100 %",
          mid and 0 < mid[0] < SIZE, str(mid[:1]))

    print("\n▶ 2 · Cancelación a mitad de parte")
    STATE.update(delay=0.02)
    flag = {"stop": False}
    sent = []

    def cb(n):
        sent.append(n)
        if n > SIZE // 4:
            flag["stop"] = True

    t0 = time.monotonic()
    try:
        backend.put_file(path, "u/cancel.bin", "cancel.bin", progress=cb,
                         cancelled=lambda: flag["stop"])
        check("la subida se cancela", False, "no lanzó TaskCancelled")
    except TaskCancelled:
        check("la subida se cancela", True)
    except Exception as exc:  # noqa: BLE001
        check("la subida se cancela", False, repr(exc))
    check("se aborta antes de enviar todo", sent and sent[-1] < SIZE, str(sent[-1:]))

    print("\n▶ 3 · Error HTTP")
    STATE.update(status=500, delay=0)
    backend._progress_confirmed = True      # ya funcionó antes: el error es real
    try:
        backend.put_file(path, "u/err.bin", "err.bin", progress=lambda n: None)
        check("HTTP 500 lanza error", False)
    except s3_client.S3Error as exc:
        check("HTTP 500 lanza error", "500" in str(exc), str(exc))
    STATE.update(status=201)
    backend.put_file(path, "u/created.bin", "created.bin", progress=lambda n: None)
    check("HTTP 201 se acepta como éxito", True)
    STATE.update(status=200)

    print("\n▶ 4 · Integración con la tarea (bytes reales, velocidad, ETA)")
    total = 40 * 1024 * 1024
    chunk = 20 * 1024 * 1024
    task = UploadTask(user_id=1, chat_id=1, filename="a.bin", file_size=total)
    task.status = TaskStatus.UPLOADING
    task.mode = "classic"
    task.plan_parts(chunk, 2)
    task.upload_reports_bytes = True
    task.mark_started()
    task.part_started(1)
    mono = time.monotonic()
    task.sample_speed()
    check("sin bytes aún no se inventa progreso", task.processed_bytes() == 0)
    rate = 4 * 1024 * 1024
    for i in range(1, 7):
        task.note_uploaded(1, int(i * rate * 0.5))
        task._live_meter.update(task.uploaded_bytes_exact(), mono + i * 0.5)
    check("progreso = bytes realmente enviados",
          task.processed_bytes() == int(6 * rate * 0.5), str(task.processed_bytes()))
    check("velocidad medida ≈ 4 MB/s",
          abs(task.upload_speed - rate) < 0.6 * rate, f"{task.upload_speed/1048576:.2f}")
    check("ya no se marca como estimada", task.is_estimated() is False)
    eta = task.eta_seconds()
    expected_eta = (total - 3 * rate) / rate
    check("ETA = restante / velocidad medida",
          eta is not None and abs(eta - expected_eta) < expected_eta * 0.4,
          f"{eta} vs {expected_eta}")
    task.note_uploaded(1, 10 * chunk)
    check("no cuenta más bytes de los que mide la parte",
          task.processed_bytes() <= chunk, str(task.processed_bytes()))

    print("\n▶ 5 · Reintento de una parte")
    before = task.processed_bytes()
    task.reset_part_upload(1)
    task.note_uploaded(1, 1024 * 1024)
    check("la pantalla no retrocede al reintentar", task.processed_bytes() >= before)
    task.part_finished(1, chunk, 5.0)
    check("al terminar la parte se cuenta una sola vez",
          task.bytes_transferred == chunk and task.uploaded_bytes_exact() == chunk)
    task.note_uploaded(1, chunk)
    check("un aviso tardío no suma otra vez", task.uploaded_bytes_exact() == chunk)

    print("\n▶ 6 · Velocidad de bajada sin parpadeos a 0")
    m = P.SpeedMeter()
    m.update(0, 100.0)
    m.update(1 * 1024 * 1024, 101.0)
    base = m.speed
    for k in range(1, 4):
        m.update(1 * 1024 * 1024, 101.0 + 0.6 * k)   # hueco entre bloques
    check("un hueco corto no pone la velocidad a 0", m.speed > 0, str(m.speed))
    m.update(2 * 1024 * 1024, 103.2)
    check("el siguiente bloque promedia toda la ventana", 0 < m.speed <= base * 1.5)
    m2 = P.SpeedMeter()
    m2.update(0, 0.0)
    m2.update(1000, 1.0)
    for k in range(2, 40):
        m2.update(1000, float(k))
    check("una parada larga sí lleva la velocidad a 0", m2.speed == 0.0, str(m2.speed))

    print("\n" + "=" * 62)
    print(f" ✅ Correctas: {len(PASSED)}   ❌ Fallidas: {len(FAILED)}")
    for f in FAILED:
        print("   -", f)
    print("=" * 62)
    server.shutdown()
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
