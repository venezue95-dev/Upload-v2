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
    base         TEXT DEFAULT ''
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
        # Migración suave para bases de datos creadas antes de la v1.3.
        try:
            conn.execute("ALTER TABLE tasks ADD COLUMN base TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.commit()
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
def record_task(task, cfg=None) -> None:
    """Guarda (o actualiza) una tarea terminada y acumula estadísticas."""
    try:
        conn = _conn()
        with _LOCK:
            conn.execute(
                """
                INSERT INTO tasks (task_id, user_id, username, filename, file_size,
                                   source, status, parts, keys, urls, error,
                                   created_at, finished_at, duration, base)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status, parts=excluded.parts, keys=excluded.keys,
                    urls=excluded.urls, error=excluded.error,
                    finished_at=excluded.finished_at, duration=excluded.duration,
                    base=excluded.base
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
                    "\n".join(task.keys or []),
                    "\n".join(task.urls or []),
                    (task.error or "")[:500],
                    float(task.created_at or time.time()),
                    float(task.finished_at or time.time()),
                    float(task.duration()),
                    task.s3_base or "",
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
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, first_seen, last_seen,
                                       uploads, failed, bytes, parts)
                    VALUES (?,?,?,?,0,1,0,0)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username=excluded.username,
                        last_seen=excluded.last_seen,
                        failed=users.failed+1
                    """,
                    (task.user_id, task.username or "", time.time(), time.time()),
                )
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("record_task falló: %s", exc)


def mark_deleted(task_id: str) -> None:
    """Elimina del historial persistido las claves y URLs borradas de toDus."""
    try:
        conn = _conn()
        with _LOCK:
            conn.execute("UPDATE tasks SET keys='', urls='' WHERE task_id=?", (task_id,))
            conn.commit()
    except Exception as exc:  # pragma: no cover
        log.warning("mark_deleted falló: %s", exc)


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


def get_task(task_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene una tarea histórica únicamente si pertenece al usuario."""
    conn = _conn()
    with _LOCK:
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
    """Bytes subidos por el usuario en las últimas 24 h (para la cuota)."""
    try:
        conn = _conn()
        cutoff = time.time() - 86400
        with _LOCK:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(file_size),0) AS total FROM tasks
                WHERE user_id=? AND status='completed' AND finished_at>=?
                """,
                (user_id, cutoff),
            ).fetchone()
        return int(row["total"] or 0)
    except Exception:  # pragma: no cover
        return 0


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


def unban_user(user_id: int) -> bool:
    conn = _conn()
    with _LOCK:
        cur = conn.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
        conn.commit()
    return bool(cur.rowcount)


def is_banned(user_id: int) -> bool:
    try:
        conn = _conn()
        with _LOCK:
            row = conn.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,)).fetchone()
        return row is not None
    except Exception:  # pragma: no cover
        return False


def banned_users() -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute("SELECT * FROM bans ORDER BY banned_at DESC").fetchall()
    return [dict(r) for r in rows]


# ----------------------------------------------------------------------
# Búsqueda en el historial.
# ----------------------------------------------------------------------
def search_user_history(user_id: int, query: str, limit: int = 25) -> List[Dict[str, Any]]:
    conn = _conn()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT * FROM tasks WHERE user_id=? AND filename LIKE ?
            ORDER BY finished_at DESC LIMIT ?
            """,
            (user_id, f"%{query}%", limit),
        ).fetchall()
    return [_row(r) for r in rows]


def _row(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    data["keys"] = [k for k in (data.get("keys") or "").split("\n") if k]
    data["urls"] = [u for u in (data.get("urls") or "").split("\n") if u]
    return data
