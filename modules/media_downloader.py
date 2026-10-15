"""
media_downloader.py
===================
Adaptador pequeño y controlado para yt-dlp.

No acepta cookies de usuarios ni URLs arbitrarias fuera del comando explícito
/descargar. El bot usa este módulo para obtener metadatos y generar un archivo
local que luego pasa por el pipeline normal de toDus.
"""

from __future__ import annotations

import logging
import os
import re
import base64
import tempfile
from typing import Callable, Dict, Optional
from urllib.parse import urlparse

from config import CONFIG
from .queue import TaskCancelled, UploadTask

log = logging.getLogger("s3_bot.media")

SUPPORTED_HOST_MARKERS = (
    "youtube.com", "youtu.be", "instagram.com", "tiktok.com", "vm.tiktok.com",
    "facebook.com", "fb.watch", "twitter.com", "x.com", "vimeo.com",
    "twitch.tv", "reddit.com", "soundcloud.com", "dailymotion.com",
    "bilibili.com", "rumble.com", "pinterest.com", "streamable.com",
)
FORMATS = {"mp4", "mp3", "m4a", "opus", "wav"}
QUALITIES = {"144", "240", "360", "480", "720", "1080", "1440", "2160", "max"}


def is_media_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().lstrip("www.")
    return any(host == marker or host.endswith("." + marker) for marker in SUPPORTED_HOST_MARKERS)


def normalize_options(media_format: str = "mp4", quality: str = "720") -> tuple[str, str]:
    media_format = (media_format or "mp4").lower().strip()
    quality = (quality or "720").lower().strip().rstrip("p")
    if media_format not in FORMATS:
        raise ValueError("formato no válido; usa mp4, mp3, m4a, opus o wav")
    if quality not in QUALITIES:
        raise ValueError("calidad no válida; usa 360, 480, 720, 1080, 1440, 2160 o max")
    return media_format, quality


def _ydl_options(media_format: str, quality: str, *, output_dir: str,
                 download: bool, hook: Optional[Callable[[dict], None]] = None) -> dict:
    media_format, quality = normalize_options(media_format, quality)
    height = ""
    if quality != "max":
        height = f"[height<={quality}]"

    if media_format == "mp4":
        selector = f"bestvideo{height}+bestaudio/best{height}/best"
    elif media_format == "m4a":
        selector = "bestaudio[ext=m4a]/bestaudio/best"
    else:
        selector = "bestaudio/best"

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "nocheckcertificate": False,
        "restrictfilenames": True,
        "windowsfilenames": True,
        "outtmpl": os.path.join(output_dir, "%(id)s_%(title).120s.%(ext)s"),
        "format": selector,
        "noplaylist": True,
        "continuedl": True,
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "ignoreerrors": False,
        "overwrites": False,
        "writethumbnail": False,
        "writesubtitles": False,
        "postprocessors": [],
    }
    if media_format == "mp4":
        opts["merge_output_format"] = "mp4"
    if media_format in {"mp3", "opus", "wav"}:
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": media_format,
            "preferredquality": "192" if media_format == "mp3" else None,
        }]
    elif media_format == "m4a":
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "m4a",
            "preferredquality": "192",
        }]
    if not download:
        opts["skip_download"] = True
    cookiefile = _cookiefile_from_environment()
    if cookiefile:
        opts["cookiefile"] = cookiefile
    user_agent = os.getenv("YTDLP_USER_AGENT", "").strip()
    if user_agent:
        opts["http_headers"] = {"User-Agent": user_agent}
    if hook:
        opts["progress_hooks"] = [hook]
    return opts


def _cookiefile_from_environment() -> Optional[str]:
    """Materializa cookies Base64 de Railway sin guardarlas en el repositorio."""
    encoded = os.getenv("YTDLP_COOKIES_B64", "").strip()
    if not encoded:
        return None
    try:
        content = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise RuntimeError("YTDLP_COOKIES_B64 no contiene Base64 válido") from exc
    if not content.startswith((b"# HTTP Cookie File", b"# Netscape HTTP Cookie File")):
        raise RuntimeError("YTDLP_COOKIES_B64 no contiene un cookies.txt Netscape válido")
    directory = os.path.join(CONFIG.data_dir, "secrets")
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError:
        directory = os.path.join(tempfile.gettempdir(), "todus-bot-secrets")
        os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "youtube-cookies.txt")
    existing = None
    if os.path.exists(path):
        with open(path, "rb") as handle:
            existing = handle.read()
    if existing != content:
        with open(path, "wb") as handle:
            handle.write(content)
        os.chmod(path, 0o600)
    return path


def _require_yt_dlp():
    try:
        import yt_dlp
        return yt_dlp
    except ImportError as exc:
        raise RuntimeError(
            "El motor multimedia no está instalado. Añade yt-dlp al despliegue."
        ) from exc


def probe_media(url: str, media_format: str = "mp4", quality: str = "720") -> Dict:
    """Obtiene nombre y tamaño estimado sin descargar el archivo."""
    if not is_media_url(url):
        raise ValueError("el dominio no está habilitado para descargas multimedia")
    yt_dlp = _require_yt_dlp()
    opts = _ydl_options(media_format, quality, output_dir=".", download=False)
    opts["logger"] = log
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info:
            raise RuntimeError("no se pudieron obtener metadatos del enlace")
        requested = info.get("requested_formats") or []
        size = info.get("filesize") or info.get("filesize_approx") or 0
        if requested:
            size = sum((x.get("filesize") or x.get("filesize_approx") or 0) for x in requested)
        title = info.get("title") or info.get("id") or "descarga"
        ext = media_format
        return {
            "size": int(size or 0),
            "filename": _safe_filename(f"{title}.{ext}"),
            "duration": int(info.get("duration") or 0),
            "extractor": info.get("extractor_key") or info.get("extractor") or "media",
        }


def download_media(url: str, dest_dir: str, task: UploadTask,
                   media_format: str = "mp4", quality: str = "720") -> str:
    """Descarga un único medio y devuelve su ruta final."""
    if not is_media_url(url):
        raise ValueError("el dominio no está habilitado para descargas multimedia")
    yt_dlp = _require_yt_dlp()
    os.makedirs(dest_dir, exist_ok=True)

    def hook(data: dict) -> None:
        if task.is_cancelled:
            raise TaskCancelled("cancelada por el usuario")
        status = data.get("status")
        if status == "downloading":
            total = data.get("total_bytes") or data.get("total_bytes_estimate") or task.file_size
            downloaded = int(data.get("downloaded_bytes") or 0)
            if total and not task.file_size:
                task.file_size = int(total)
            task.set_fetched(downloaded)
        elif status == "finished":
            total = int(data.get("total_bytes") or data.get("total_bytes_estimate") or task.file_size or 0)
            if total:
                task.set_fetched(total)

    opts = _ydl_options(media_format, quality, output_dir=dest_dir, download=True, hook=hook)
    opts["logger"] = log
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])

    candidates = []
    for name in os.listdir(dest_dir):
        path = os.path.join(dest_dir, name)
        if os.path.isfile(path):
            candidates.append(path)
    if not candidates:
        raise RuntimeError("yt-dlp no generó ningún archivo")
    path = max(candidates, key=os.path.getmtime)
    if os.path.getsize(path) <= 0:
        raise RuntimeError("yt-dlp generó un archivo vacío")
    task.set_fetched(os.path.getsize(path))
    return path


def _safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", name).strip(" .")
    return name[:180] or "descarga.mp4"
