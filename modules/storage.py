"""
storage.py
==========
Historial y estadísticas persistentes en SQLite (``data/history.db``).

Guarda cada tarea terminada para:
- /historial del usuario (con enlaces),
- /stats y /usuarios del owner,
- estadísticas para la ETA de la cola (tamaño y velocidad medios),
- la lista de usuarios del bot (para /anuncio).

El acceso es desde varios hilos, así que se usa un lock y ``check_same_thread=False``.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("s3_bot.storage")

_LOCK = threading.RLock()
_CONN: Optional[sqlite3.Connection] = None
# Los filtros de Telegram consultan "¿está baneado?" en CADA mensaje: se guarda
# en memoria (se carga al abrir la base y se actualiza con ban/unban).
_BANNED: set = set()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id      TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    username     TEXT,
    filename     TEXT,
    file_size    INTEGER DEFAULT 0,
    source       TEXT,
    status       TEXT,
    parts        INTEGER DEFAULT 0,
    keys         TEXT DEFAULT '',
    urls         TEXT DEFAULT '',
    error        TEXT,
    created_at   REAL,
    finished_at  REAL,
    duration     REAL,
    base         TEXT DEFAULT '',
    deleted_at   REAL,
    chat_id      INTEGER,
    message_id   INTEGER,
    url          TEXT,
    media_format TEXT DEFAULT 'mp4',
    media_quality TEXT DEFAULT '720'
);
CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks(user_id, finished_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS bans (
    user_id   INTEGER PRIMARY KEY,
    reason    TEXT,
    banned_at REAL
);

CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    first_seen  REAL,
    last_seen   REAL,
    uploads     INTEGER DEFAULT 0,
    failed      INTEGER DEFAULT 0,
    bytes       INTEGER DEFAULT 0,
    parts       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS quota_reservations (
    task_id      TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    file_size    INTEGER NOT NULL,
    reserved_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quota_res_user ON quota_reservations(user_id);

CREATE TABLE IF NOT EXISTS user_preferences (
    user_id       INTEGER PRIMARY KEY,
    chunk_size_mb INTEGER NOT NULL
);
"""


def init(cfg=None) -> None:
    """Abre la base de datos (idempotente)."""
    global _CONN
    from config import CONFIG

    cfg = cfg or CONFIG
    path = cfg.history_db
    with _LOCK:
        if _CONN is not None:
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        # Migraciones suaves para bases de datos creadas con versiones anteriores.
        for ddl in (
            "ALTER TABLE tasks ADD COLUMN base TEXT DEFAULT ''",
            "ALTER TABLE tasks ADD COLUMN deleted_at REAL",
            "ALTER TABLE tasks ADD COLUMN chat_id INTEGER",
            "ALTER TABLE tasks ADD COLUMN message_id INTEGER",
            "ALTER TABLE tasks ADD COLUMN url TEXT",
            "ALTER TABLE tasks ADD COLUMN media_format TEXT DEFAULT 'mp4'",
            "ALTER TABLE tasks ADD COLUMN media_quality TEXT DEFAULT '720'",
        ):
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.commit()
        _BANNED.clear()
        _BANNED.update(int(r["user_id"]) for r in conn.execute("SELECT user_id FROM bans"))
        _CONN = conn
        log.info("Historial SQLite listo en %s", path)


def _conn() -> sqlite3.Connection:
    if _CONN is None:
        init()
    assert _CONN is not None
    return _CONN


# ----------------------------------------------------------------------
# Escritura.
# ----------------------------------------------------------------------
def _ordered_keys(task) -> List[str]:
    """Claves de la tarea ordenadas por nº de parte (también en tareas fallidas)."""
    by_part = getattr(task, "keys_by_part", None) or {}
    if by_part:
        return [by_part[n] for n in sorted(by_part) if by_part[n]]
    return list(getattr(task, "keys", None) or [])


def record_task(task, cfg=None) -> None:
    """Guarda (o actualiza) una tarea terminada y acumula estadísticas."""
    try:
        conn = _conn()
        keys = _ordered_keys(task)
        with _LOCK:
            conn.execute(
                """
                INSERT INTO tasks (task_id, user_id, username, filename, file_size,
                                   source, status, parts, keys, urls, error,
                                   created_at, finished_at, duration, base,
                                   chat_id, message_id, url, media_format, media_quality)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status, parts=excluded.parts, keys=excluded.keys,
                    urls=excluded.urls, error=excluded.error,
                    finished_at=excluded.finished_at, duration=excluded.duration,
                    base=excluded.base, chat_id=excluded.chat_id,
                    message_id=excluded.message_id, url=excluded.url,
                    media_format=excluded.media_format,
                    media_quality=excluded.media_quality
                """,
                (
                    task.task_id,
                    task.user_id,
                    task.username or "",
                    task.filename,
                    int(task.file_size or 0),
                    task.source,
                    task.status.value,
                    int(task.parts_done or 0),
                    "\n".join(keys),
                    "\n".join(task.urls or []),
                    (task.error or "")[:500],
                    float(task.created_at or time.time()),
                    float(task.finished_at or time.time()),
                    float(task.duration()),
                    task.s3_base or "",
                    int(getattr(task, "chat_id", 0) or 0),
                    int(getattr(task, "message_id", 0) or 0),
                    getattr(task, "url", None) or "",
                    getattr(task, "media_format", "mp4"),
                    getattr(task, "media_quality", "720"),
                ),
            )
            size = int(task.bytes_transferred or task.file_size or 0)
            parts = int(task.parts_done or 0)
            if task.status.value == "completed":
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, first_seen, last_seen,
                                       uploads, failed, bytes, parts)
                    VALUES (?,?,?,?,1,0,?,?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username=excluded.username,
                        last_seen=excluded.last_seen,
                        uploads=users.uploads+1,
                        bytes=users.bytes+excluded.bytes,
                        parts=users.parts+excluded.parts
                    """,
                    (task.user_id, task.username or "", time.time(), time.time(), size, parts),
                )
            else:
                # Solo los fallos cuentan como "fallidas"; cancelar no es fallar.
                failed = 1 if task.status.value == "failed" else 0
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, first_seen, last_seen,
                                       uploads, failed, bytes, parts)
                    VALUES (?,?,?,?,0,?,0,0)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username=excluded.username,
                        last_seen=excluded.last_seen,
                        failed=users.failed+excluded.failed
                    """,
                    (task.user_id, task.username or "", time.time(), time.time(), failed),
                )
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("record_task falló: %s", exc)


def mark_deleted(task_id: str) -> None:
    """Marca la subida como borrada de toDus (se vacían claves y enlaces)."""
    try:
        conn = _conn()
        with _LOCK:
            conn.execute(
                "UPDATE tasks SET keys='', urls='', deleted_at=? WHERE task_id=?",
                (time.time(), task_id),
            )
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("mark_deleted falló: %s", exc)


def update_links(task_id: str, keys: List[str], urls: List[str]) -> None:
    """Borrado parcial: se conservan solo las claves/enlaces que siguen en toDus."""
    try:
        conn = _conn()
        with _LOCK:
            conn.execute(
                "UPDATE tasks SET keys=?, urls=? WHERE task_id=?",
                ("\n".join(keys), "\n".join(urls), task_id),
            )
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("update_links falló: %s", exc)


def touch_user(user_id: int, username: str = "") -> None:
    """Registra al usuario la primera vez que habla (para /anuncio y /usuarios)."""
    try:
        conn = _conn()
        now = time.time()
        with _LOCK:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_seen, last_seen)
                VALUES (?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=CASE WHEN excluded.username!='' THEN excluded.username
                                  ELSE users.username END,
                    last_seen=excluded.last_seen
                """,
                (user_id, username or "", now, now),
            )
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.debug("touch_user falló: %s", exc)


def get_user_chunk_size(user_id: int) -> Optional[int]:
    """Devuelve el chunk del usuario o None para usar el ajuste global."""
    try:
        row = _conn().execute(
            "SELECT chunk_size_mb FROM user_preferences WHERE user_id=?", (user_id,)
        ).fetchone()
        return int(row[0]) if row else None
    except Exception as exc:  # pragma: no cover
        log.debug("get_user_chunk_size falló: %s", exc)
        return None


def set_user_chunk_size(user_id: int, size_mb: int) -> None:
    with _LOCK:
        conn = _conn()
        conn.execute(
            "INSERT INTO user_preferences(user_id, chunk_size_mb) VALUES(?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET chunk_size_mb=excluded.chunk_size_mb",
            (user_id, int(size_mb)),
        )
        conn.commit()


def clear_user_chunk_size(user_id: int) -> None:
    with _LOCK:
        conn = _conn()
        conn.execute("DELETE FROM user_preferences WHERE user_id=?", (user_id,))
        conn.commit()


# ----------------------------------------------------------------------
# Lectura.
# ----------------------------------------------------------------------
def user_history(user_id: int, limit: int = 60) -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT * FROM tasks WHERE user_id=? AND status='completed'
            ORDER BY finished_at DESC LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [_row(r) for r in rows]


def user_recent(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE user_id=? ORDER BY finished_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [_row(r) for r in rows]


def get_task(task_id: str, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Obtiene una tarea histórica.

    Con ``user_id`` solo si pertenece a ese usuario; con ``None`` (uso del
    owner) devuelve la tarea de cualquiera.
    """
    conn = _conn()
    with _LOCK:
        if user_id is None:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id=? AND user_id=?",
                (task_id, user_id),
            ).fetchone()
    return _row(row) if row else None


def global_stats() -> Dict[str, Any]:
    conn = _conn()
    with _LOCK:
        totals = conn.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM users)                                    AS users,
              SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)             AS completed,
              SUM(CASE WHEN status='failed'    THEN 1 ELSE 0 END)             AS failed,
              SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)             AS cancelled,
              SUM(CASE WHEN status='completed' THEN parts ELSE 0 END)         AS parts,
              AVG(CASE WHEN status='completed' THEN duration END)             AS avg_duration
            FROM tasks
            """
        ).fetchone()
        bytes_row = conn.execute(
            "SELECT COALESCE(SUM(bytes),0) AS bytes FROM users"
        ).fetchone()

    return {
        "users": int(totals["users"] or 0),
        "completed": int(totals["completed"] or 0),
        "failed": int(totals["failed"] or 0),
        "cancelled": int(totals["cancelled"] or 0),
        "parts": int(totals["parts"] or 0),
        "avg_duration": float(totals["avg_duration"] or 0.0),
        "bytes": int(bytes_row["bytes"] or 0),
    }


def top_users(limit: int = 5) -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT user_id, username, uploads, bytes, failed
            FROM users ORDER BY bytes DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def user_stats(user_id: int) -> Dict[str, Any]:
    conn = _conn()
    with _LOCK:
        row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    return dict(row) if row else {}


def all_user_ids(cfg=None) -> List[int]:
    try:
        conn = _conn()
        with _LOCK:
            rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [int(r["user_id"]) for r in rows]
    except Exception:  # pragma: no cover
        return []


def avg_file_size() -> int:
    """Tamaño medio de las subidas completadas (para la ETA de la cola)."""
    try:
        conn = _conn()
        with _LOCK:
            row = conn.execute(
                "SELECT AVG(file_size) AS avg_size FROM tasks WHERE status='completed'"
            ).fetchone()
        return int(row["avg_size"] or 0)
    except Exception:  # pragma: no cover
        return 0


def purge_older_than(days: int = 90) -> int:
    """Borra historial antiguo (mantenimiento)."""
    cutoff = time.time() - days * 86400
    try:
        conn = _conn()
        with _LOCK:
            cur = conn.execute("DELETE FROM tasks WHERE finished_at < ?", (cutoff,))
            conn.commit()
        return cur.rowcount or 0
    except Exception:  # pragma: no cover
        return 0


# ----------------------------------------------------------------------
# Velocidades medias (siembran el estimador de progreso).
# ----------------------------------------------------------------------
def avg_speeds() -> Tuple[float, float]:
    """(bytes/s de subida, bytes/s de bajada) a partir del historial real."""
    try:
        conn = _conn()
        with _LOCK:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(file_size),0) AS total_bytes,
                       COALESCE(SUM(CASE WHEN duration > 0 THEN duration ELSE 0 END),0)
                           AS total_time
                FROM tasks WHERE status='completed'
                """
            ).fetchone()
        total_bytes = float(row["total_bytes"] or 0)
        total_time = float(row["total_time"] or 0)
        upload = (total_bytes / total_time) if total_time > 0 else 0.0
        return upload, 0.0
    except Exception:  # pragma: no cover
        return 0.0, 0.0


# ----------------------------------------------------------------------
# Cuota diaria por usuario.
# ----------------------------------------------------------------------
def bytes_today(user_id: int) -> int:
    """Bytes completados más bytes reservados en las últimas 24 h."""
    try:
        conn = _conn()
        cutoff = time.time() - 86400
        with _LOCK:
            row = conn.execute(
                """
                SELECT
                    (SELECT COALESCE(SUM(file_size),0) FROM tasks
                     WHERE user_id=? AND status='completed' AND finished_at>=?)
                    +
                    (SELECT COALESCE(SUM(file_size),0) FROM quota_reservations
                     WHERE user_id=?) AS total
                """,
                (user_id, cutoff, user_id),
            ).fetchone()
        return int(row["total"] or 0)
    except Exception:  # pragma: no cover
        return 0


def reserve_quota(user_id: int, task_id: str, file_size: int, *, owner: bool = False) -> Tuple[bool, int, int]:
    """Reserva cuota de forma atómica para una tarea admitida en la cola.

    Devuelve (permitida, usado_incluidas_reservas, límite_bytes). El owner y
    la cuota 0 no reservan nada. La fila persiste para sobrevivir reinicios.
    """
    size = max(0, int(file_size or 0))
    from .runtime_settings import RUNTIME_SETTINGS

    limit_mb = int(RUNTIME_SETTINGS.get("daily_quota_mb", 0) or 0)
    limit = limit_mb * 1024 * 1024
    if owner or limit <= 0 or size <= 0:
        return True, bytes_today(user_id), limit
    conn = _conn()
    cutoff = time.time() - 86400
    with _LOCK:
        row = conn.execute(
            """
            SELECT
                (SELECT COALESCE(SUM(file_size),0) FROM tasks
                 WHERE user_id=? AND status='completed' AND finished_at>=?)
                +
                (SELECT COALESCE(SUM(file_size),0) FROM quota_reservations
                 WHERE user_id=? AND task_id<>?) AS total
            """,
            (user_id, cutoff, user_id, task_id),
        ).fetchone()
        used = int(row["total"] or 0)
        if used + size > limit:
            return False, used, limit
        conn.execute(
            "INSERT OR REPLACE INTO quota_reservations(task_id,user_id,file_size,reserved_at) VALUES (?,?,?,?)",
            (task_id, user_id, size, time.time()),
        )
        conn.commit()
        return True, used + size, limit


def release_quota(task_id: str) -> None:
    """Libera la reserva al terminar, fallar o cancelar una tarea."""
    try:
        conn = _conn()
        with _LOCK:
            conn.execute("DELETE FROM quota_reservations WHERE task_id=?", (task_id,))
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("release_quota falló: %s", exc)


def reconcile_quota_reservations(active_task_ids: List[str]) -> None:
    """Elimina reservas huérfanas después de cargar el estado de la cola."""
    try:
        conn = _conn()
        with _LOCK:
            if active_task_ids:
                marks = ",".join("?" for _ in active_task_ids)
                conn.execute(f"DELETE FROM quota_reservations WHERE task_id NOT IN ({marks})", active_task_ids)
            else:
                conn.execute("DELETE FROM quota_reservations")
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("reconcile_quota_reservations falló: %s", exc)


# ----------------------------------------------------------------------
# Baneos.
# ----------------------------------------------------------------------
def ban_user(user_id: int, reason: str = "") -> None:
    conn = _conn()
    with _LOCK:
        conn.execute(
            "INSERT INTO bans (user_id, reason, banned_at) VALUES (?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, "
            "banned_at=excluded.banned_at",
            (user_id, reason[:200], time.time()),
        )
        conn.commit()
        _BANNED.add(int(user_id))


def unban_user(user_id: int) -> bool:
    conn = _conn()
    with _LOCK:
        cur = conn.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
        conn.commit()
        _BANNED.discard(int(user_id))
    return bool(cur.rowcount)


def is_banned(user_id: int) -> bool:
    """Consulta en memoria (se llama en cada mensaje y cada pulsación)."""
    if _CONN is None:
        try:
            init()
        except Exception:  # pragma: no cover
            return False
    return int(user_id) in _BANNED


def banned_users() -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute("SELECT * FROM bans ORDER BY banned_at DESC").fetchall()
    return [dict(r) for r in rows]


# ----------------------------------------------------------------------
# Búsqueda en el historial.
# ----------------------------------------------------------------------
def search_user_history(user_id: int, query: str, limit: int = 25) -> List[Dict[str, Any]]:
    """Subidas completadas del usuario cuyo nombre contiene ``query``."""
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    conn = _conn()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT * FROM tasks
            WHERE user_id=? AND status='completed' AND filename LIKE ? ESCAPE '\\'
            ORDER BY finished_at DESC LIMIT ?
            """,
            (user_id, f"%{escaped}%", limit),
        ).fetchall()
    return [_row(r) for r in rows]


def _row(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    data["keys"] = [k for k in (data.get("keys") or "").split("\n") if k]
    data["urls"] = [u for u in (data.get("urls") or "").split("\n") if u]
    return data
