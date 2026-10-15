"""
Resolución de páginas públicas de alojamientos de archivos.

El resultado es una URL temporal/directa que continúa por el descargador HTTP
existente. No intenta saltar CAPTCHA, login, paywalls ni controles de acceso.
"""
from __future__ import annotations

import html
import time
import json
import re
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests

from .downloader import USER_AGENT, _safe_request, check_url

FILE_HOST_MARKERS = (
    "mediafire.com", "dropbox.com", "pixeldrain.com",
)


def is_filehost_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().lstrip("www.")
    return any(host == marker or host.endswith("." + marker) for marker in FILE_HOST_MARKERS)


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().lstrip("www.")


def _dropbox_direct(url: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["dl"] = "1"
    return urlunparse(parsed._replace(query=urlencode(query)))


def _pixeldrain_direct(url: str) -> Optional[str]:
    match = re.search(r"/(?:u|d)/([A-Za-z0-9_-]+)", urlparse(url).path)
    return f"https://pixeldrain.com/api/file/{match.group(1)}" if match else None


def _mediafire_direct(page_url: str, session: requests.Session) -> str:
    response = _safe_request(
        session, "GET", page_url, timeout=(15, 30),
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
    )
    try:
        response.raise_for_status()
        body = html.unescape(response.text)
    finally:
        response.close()

    patterns = (
        r'id=["\']downloadButton["\'][^>]*href=["\']([^"\']+)',
        r'href=["\']([^"\']+)["\'][^>]*id=["\']downloadButton["\']',
        r'(?:(?:download_link|downloadLink)[^>]*href|href[^>]*(?:download_link|downloadLink))=["\']([^"\']+)',
    )
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            candidate = urljoin(page_url, html.unescape(match.group(1)))
            ok, why = check_url(candidate)
            if not ok:
                raise RuntimeError(f"enlace de MediaFire rechazado: {why}")
            return candidate
    token_matches = re.findall(
        r'id=["\']deferredDownloadButton["\'][^>]*data-security-token=["\']([^"\']+)',
        body, re.IGNORECASE,
    )
    token = next((value for value in reversed(token_matches) if "..." not in value), None)
    if token:
        endpoint = urljoin(page_url, "/download_link.php")
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": page_url,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }
        for _ in range(3):
            response = _safe_request(
                session, "POST", endpoint, timeout=(15, 30), headers=headers,
                data={"security_token": html.unescape(token)},
            )
            try:
                data = json.loads(response.text)
            finally:
                response.close()
            if data.get("status") == "delay":
                time.sleep(min(5, max(1, int(data.get("retry_after") or 2))))
                continue
            candidate = data.get("download_url")
            if data.get("status") == "success" and candidate:
                candidate = urljoin(page_url, html.unescape(candidate))
                ok, why = check_url(candidate)
                if not ok:
                    raise RuntimeError(f"enlace de MediaFire rechazado: {why}")
                return candidate
            break
    raise RuntimeError("MediaFire no mostró un enlace público de descarga")


def resolve_download_url(url: str, session: Optional[requests.Session] = None) -> str:
    """Convierte una página de archivo pública en una URL descargable."""
    if not is_filehost_url(url):
        return url
    host = _host(url)
    if host == "dropbox.com" or host.endswith(".dropbox.com"):
        return _dropbox_direct(url)
    if host == "pixeldrain.com" or host.endswith(".pixeldrain.com"):
        return _pixeldrain_direct(url) or url
    if host == "mediafire.com" or host.endswith(".mediafire.com"):
        own = session or requests.Session()
        return _mediafire_direct(url, own)
    return url
