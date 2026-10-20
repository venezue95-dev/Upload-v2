"""
media_downloader.py
===================
Adaptador pequeño y controlado para yt-dlp.

Las cookies, si se configuran, se pasan a yt-dlp mediante ``cookiefile``.
El bot usa el extractor estándar de yt-dlp para obtener metadatos y generar un
archivo local que luego pasa por el pipeline normal de toDus.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from typing import Callable, Dict, Optional
from urllib.parse import urlparse

from config import CONFIG
from .queue import TaskCancelled, UploadTask

log = logging.getLogger("s3_bot.media")

SUPPORTED_HOST_MARKERS = (
    "youtube.com", "youtu.be",
    "instagram.com", "tiktok.com", "vm.tiktok.com",
    "facebook.com", "fb.watch", "twitter.com", "x.com", "vimeo.com",
    "twitch.tv", "reddit.com", "soundcloud.com", "dailymotion.com",
    "bilibili.com", "rumble.com", "pinterest.com", "streamable.com",
)
FORMATS = {"mp4", "mp3", "m4a", "opus", "wav"}
QUALITIES = {"144", "240", "360", "480", "720", "1080", "1440", "2160", "max"}


def is_media_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().lstrip("www.")
    return any(host == marker or host.endswith("." + marker) for marker in SUPPORTED_HOST_MARKERS)


def is_youtube_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().lstrip("www.")
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


def is_facebook_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().lstrip("www.")
    return host == "facebook.com" or host.endswith(".facebook.com") or host == "fb.watch"


def normalize_options(media_format: str = "mp4", quality: str = "720") -> tuple[str, str]:
    media_format = (media_format or "mp4").lower().strip()
    quality = (quality or "720").lower().strip().rstrip("p")
    if media_format not in FORMATS:
        raise ValueError("formato no válido; usa mp4, mp3, m4a, opus o wav")
    if quality not in QUALITIES:
        raise ValueError("calidad no válida; usa 360, 480, 720, 1080, 1440, 2160 o max")
    return media_format, quality


def available_video_qualities(info: dict) -> list[str]:
    """Devuelve alturas de vídeo válidas presentes en la respuesta de yt-dlp."""
    found = set()
    for fmt in info.get("formats") or []:
        if fmt.get("vcodec") in (None, "none"):
            continue
        height = int(fmt.get("height") or 0)
        if str(height) in QUALITIES and height > 0:
            found.add(str(height))
    return sorted(found, key=int)


def _ydl_options(media_format: str, quality: str, *, output_dir: str,
                 url: Optional[str] = None,
                 download: bool, hook: Optional[Callable[[dict], None]] = None,
                 use_cookies: bool = True) -> dict:
    media_format, quality = normalize_options(media_format, quality)
    height = ""
    if quality != "max":
        height = f"[height<={quality}]"

    if media_format == "mp4":
        if url and is_facebook_url(url):
            # Facebook suele devolver únicamente los identificadores sd/hd y no
            # informa height; el selector por altura no puede distinguirlos.
            selector = ("sd/best[height<=480]/best" if quality != "max" and int(quality) <= 480
                        else "hd/best[height<=%s]/best" % quality if quality != "max"
                        else "hd/best")
        else:
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
    if use_cookies:
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
    """Materializa cookies Netscape multilinea de YTDLP_COOKIES."""
    content = os.getenv("YTDLP_COOKIES", "")
    if not content.strip():
        return None
    content_bytes = content.replace("\r\n", "\n").encode("utf-8")
    if not content_bytes.startswith((b"# HTTP Cookie File", b"# Netscape HTTP Cookie File")):
        raise RuntimeError("YTDLP_COOKIES no contiene un cookies.txt Netscape válido")
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
    if existing != content_bytes:
        with open(path, "wb") as handle:
            handle.write(content_bytes)
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


def _youtube_cookie_modes(url: str) -> tuple[bool, ...]:
    """Usa primero el flujo público de yt-dlp y deja las cookies como respaldo."""
    if is_youtube_url(url) and os.getenv("YTDLP_COOKIES", "").strip():
        return (False, True)
    return (True,)


def probe_media(url: str, media_format: str = "mp4", quality: str = "720") -> Dict:
    """Obtiene nombre y tamaño estimado sin descargar el archivo."""
    if not is_media_url(url):
        raise ValueError("el dominio no está habilitado para descargas multimedia")
    yt_dlp = _require_yt_dlp()
    last_error = None
    for use_cookies in _youtube_cookie_modes(url):
        try:
            opts = _ydl_options(
                media_format, quality, output_dir=".", url=url,
                download=False, use_cookies=use_cookies,
            )
            opts["logger"] = log
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    raise RuntimeError("no se pudieron obtener metadatos del enlace")
                size = _estimate_selected_size(info, media_format, quality)
                title = info.get("title") or info.get("id") or "descarga"
                ext = media_format
                return {
                    "size": int(size or 0),
                    "filename": _safe_filename(f"{title}.{ext}"),
                    "duration": int(info.get("duration") or 0),
                    "extractor": info.get("extractor_key") or info.get("extractor") or "media",
                    "qualities": available_video_qualities(info),
                }
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if use_cookies:
                raise
            log.info("yt-dlp sin cookies no pudo extraer %s; probando cookies de respaldo", url)
    raise last_error  # type: ignore[misc]


def _format_size(fmt: dict) -> int:
    return int(fmt.get("filesize") or fmt.get("filesize_approx") or 0)


def _format_size_or_bitrate(fmt: dict, duration: int = 0) -> int:
    size = _format_size(fmt)
    if size:
        return size
    bitrate = fmt.get("tbr") or fmt.get("abr") or 0
    return int(float(bitrate) * 1000 / 8 * duration) if bitrate and duration else 0


def _estimate_selected_size(info: dict, media_format: str, quality: str) -> int:
    """Estima el tamaño del formato elegido, no el tamaño genérico del vídeo."""
    duration = int(info.get("duration") or 0)
    requested = info.get("requested_formats") or []
    if requested:
        total = sum(_format_size_or_bitrate(fmt, duration) for fmt in requested)
        if total:
            return total
    formats = [fmt for fmt in (info.get("formats") or [])
               if _format_size_or_bitrate(fmt, duration)]
    if not formats:
        return int(info.get("filesize") or info.get("filesize_approx") or 0)
    audio = [f for f in formats if f.get("vcodec") == "none" and f.get("acodec") not in (None, "none")]
    if media_format in {"mp3", "m4a", "opus", "wav"}:
        if media_format == "m4a":
            preferred = [f for f in audio if f.get("ext") == "m4a"] or audio
        else:
            preferred = audio
        return _format_size_or_bitrate(
            max(preferred or formats, key=lambda f: f.get("abr") or f.get("tbr") or 0), duration
        )
    limit = None if quality == "max" else int(quality)
    video = [f for f in formats if f.get("vcodec") not in (None, "none")]
    if limit:
        under = [f for f in video if int(f.get("height") or 0) <= limit]
        video = under or video
    video_best = max(video or formats, key=lambda f: (int(f.get("height") or 0), f.get("tbr") or 0))
    if video_best.get("acodec") not in (None, "none"):
        return _format_size_or_bitrate(video_best, duration)
    audio_best = max(audio, key=lambda f: f.get("abr") or f.get("tbr") or 0) if audio else None
    return _format_size_or_bitrate(video_best, duration) + (
        _format_size_or_bitrate(audio_best, duration) if audio_best else 0
    )


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
            if total and not task.file_size and media_format == "mp4":
                task.file_size = int(total)
            task.set_fetched(downloaded)
        elif status == "finished":
            total = int(data.get("total_bytes") or data.get("total_bytes_estimate") or task.file_size or 0)
            if total:
                task.set_fetched(total)

    last_error = None
    for use_cookies in _youtube_cookie_modes(url):
        try:
            opts = _ydl_options(
                media_format, quality, output_dir=dest_dir, url=url,
                download=True, hook=hook, use_cookies=use_cookies,
            )
            opts["logger"] = log
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            break
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if use_cookies:
                raise
            log.info("yt-dlp sin cookies no pudo descargar %s; probando cookies de respaldo", url)
    if last_error is not None and len(_youtube_cookie_modes(url)) == 1:
        raise last_error

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
    final_size = os.path.getsize(path)
    task.file_size = final_size
    task.set_fetched(final_size)
    return path


def _safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", name).strip(" .")
    return name[:180] or "descarga.mp4"
