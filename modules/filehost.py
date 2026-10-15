"""
Resolución de páginas públicas de alojamientos de archivos.

El resultado es una URL temporal/directa que continúa por el descargador HTTP
existente. No intenta saltar CAPTCHA, login, paywalls ni controles de acceso.
"""
from __future__ import annotations

import hashlib
import html
import json
import re
import time
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests

from .downloader import USER_AGENT, _safe_request, check_url

FILE_HOST_MARKERS = (
    "mediafire.com", "dropbox.com", "pixeldrain.com", "file.io",
    "drive.google.com", "docs.google.com", "gofile.io", "we.tl",
    "wetransfer.com", "1fichier.com", "ufile.io",
)


def is_filehost_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return any(host == marker or host.endswith("." + marker) for marker in FILE_HOST_MARKERS)


def _host(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def _dropbox_direct(url: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["dl"] = "1"
    return urlunparse(parsed._replace(query=urlencode(query)))


def _pixeldrain_direct(url: str) -> Optional[str]:
    match = re.search(r"/(?:u|d)/([A-Za-z0-9_-]+)", urlparse(url).path)
    return f"https://pixeldrain.com/api/file/{match.group(1)}" if match else None


def _google_drive_direct(url: str) -> Optional[str]:
    parsed = urlparse(url)
    match = re.search(r"/(?:file/d|document/d|spreadsheets/d|presentation/d)/([A-Za-z0-9_-]+)", parsed.path)
    if not match:
        file_id = dict(parse_qsl(parsed.query, keep_blank_values=True)).get("id")
    else:
        file_id = match.group(1)
    return (
        f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
        if file_id else None
    )


def _mediafire_direct(page_url: str, session: requests.Session) -> str:
    response = _safe_request(session, "GET", page_url, timeout=(15, 30),
                             headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
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
        headers = {"User-Agent": USER_AGENT, "Referer": page_url,
                   "X-Requested-With": "XMLHttpRequest", "Accept": "application/json"}
        for _ in range(3):
            response = _safe_request(session, "POST", endpoint, timeout=(15, 30), headers=headers,
                                     data={"security_token": html.unescape(token)})
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


def _gofile_direct(page_url: str, session: requests.Session) -> str:
    match = re.search(r"/d/([A-Za-z0-9]+)", urlparse(page_url).path)
    if not match:
        raise RuntimeError("enlace de GoFile inválido: falta el identificador de contenido")
    user_agent = session.headers.get("User-Agent", USER_AGENT)
    slot = int(time.time()) // 14400
    token = hashlib.sha256(f"{user_agent}::en-US::::{slot}::12af056dacea0b".encode()).hexdigest()
    endpoint = f"https://api.gofile.io/contents/{match.group(1)}?cache=true&sortField=createTime&sortDirection=1"
    response = _safe_request(session, "GET", endpoint, timeout=(15, 30), headers={
        "User-Agent": user_agent, "X-Website-Token": token, "X-BL": "en-US"})
    try:
        data = json.loads(response.text)
    finally:
        response.close()
    if data.get("status") != "ok":
        raise RuntimeError("GoFile no devolvió un contenido público disponible")
    content = data.get("data") or {}
    if content.get("type") == "folder":
        files = [v for v in (content.get("children") or {}).values() if v.get("type") == "file"]
        if len(files) != 1:
            raise RuntimeError("GoFile contiene varios archivos; envía el enlace individual")
        content = files[0]
    candidate = content.get("link")
    if not candidate:
        raise RuntimeError("GoFile no entregó un enlace de descarga")
    return candidate


def _wetransfer_direct(page_url: str, session: requests.Session) -> str:
    session.headers.update({"User-Agent": USER_AGENT, "X-Requested-With": "XMLHttpRequest"})
    landing = _safe_request(session, "GET", page_url, timeout=(15, 30), headers=session.headers)
    try:
        final_url = landing.url
        body = landing.text
    finally:
        landing.close()
    pattern = r"(?:wetransfer\.com/(?:downloads?/)?)([^/]+)(?:/([^/]+))?/([^/?#]+)"
    match = re.search(pattern, final_url) or re.search(pattern, body)
    if not match:
        raise RuntimeError("WeTransfer no mostró un identificador de transferencia válido")
    transfer_id, middle, security_hash = match.groups()
    payload = {"intent": "entire_transfer", "security_hash": security_hash}
    if middle:
        payload["recipient_id"] = middle
    api = f"https://wetransfer.com/api/v4/transfers/{transfer_id}/download"
    response = _safe_request(session, "POST", api, timeout=(15, 30), headers=session.headers, json=payload)
    try:
        data = json.loads(response.text)
    finally:
        response.close()
    candidate = data.get("direct_link")
    if not candidate:
        raise RuntimeError("WeTransfer no devolvió un enlace directo; puede haber expirado")
    return candidate


def _generic_filehost_page(page_url: str, session: requests.Session) -> str:
    response = _safe_request(session, "GET", page_url, timeout=(15, 30), headers={"User-Agent": USER_AGENT})
    try:
        body = html.unescape(response.text)
    finally:
        response.close()
    candidates = re.findall(r'href=["\']([^"\']+)["\'][^>]*>\s*(?:download|descargar|télécharger)\s*<', body, re.I)
    candidates += re.findall(r'href=["\']([^"\']*(?:download|dl)[^"\']*)["\']', body, re.I)
    for value in candidates:
        candidate = urljoin(page_url, value)
        ok, why = check_url(candidate)
        if ok:
            return candidate
    raise RuntimeError("el sitio no mostró un enlace público de descarga reconocible")


def resolve_download_url(url: str, session: Optional[requests.Session] = None) -> str:
    """Convierte una página pública en una URL descargable."""
    if not is_filehost_url(url):
        return url
    own = session or requests.Session()
    host = _host(url)
    if host == "dropbox.com" or host.endswith(".dropbox.com"):
        return _dropbox_direct(url)
    if host == "pixeldrain.com" or host.endswith(".pixeldrain.com"):
        return _pixeldrain_direct(url) or url
    if host in ("drive.google.com", "docs.google.com"):
        return _google_drive_direct(url) or url
    if host == "file.io" or host.endswith(".file.io"):
        return url
    if host == "mediafire.com" or host.endswith(".mediafire.com"):
        return _mediafire_direct(url, own)
    if host == "gofile.io" or host.endswith(".gofile.io"):
        return _gofile_direct(url, own)
    if host == "we.tl" or host == "wetransfer.com" or host.endswith(".wetransfer.com"):
        return _wetransfer_direct(url, own)
    if host == "1fichier.com" or host.endswith(".1fichier.com") or host == "ufile.io":
        return _generic_filehost_page(url, own)
    return url
