"""Registro central de handlers del bot."""

from . import basic, callbacks, cola, settings, admin, upload, url  # noqa: F401

__all__ = ["basic", "callbacks", "cola", "settings", "admin", "upload", "url"]
