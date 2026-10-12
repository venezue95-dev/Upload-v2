"""
limiter.py
==========
Semáforo asíncrono cuyo límite se puede cambiar en caliente.

Se usa para limitar cuántas partes se suben a la vez en TODO el bot: el
owner puede subir/bajar el valor desde /settings y se aplica al instante
sin reiniciar (cosa que un ``asyncio.Semaphore`` normal no permite).
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Callable, Deque


class DynamicSemaphore:
    def __init__(self, limit: int, limit_fn: Callable[[], int] | None = None) -> None:
        self._limit = max(1, int(limit))
        self._limit_fn = limit_fn
        self._active = 0
        self._waiters: Deque[asyncio.Future] = deque()

    @property
    def limit(self) -> int:
        if self._limit_fn is not None:
            try:
                self._limit = max(1, int(self._limit_fn()))
            except Exception:  # pragma: no cover
                pass
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self) -> None:
        while True:
            if self._active < self.limit:
                self._active += 1
                return
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._waiters.append(fut)
            await fut

    def release(self) -> None:
        self._active = max(0, self._active - 1)
        # Despertar a los que esperan (revalidan el límite al volver).
        while self._waiters and self._active < self.limit:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(True)

    async def __aenter__(self) -> "DynamicSemaphore":
        await self.acquire()
        return self

    async def __aexit__(self, *exc) -> None:
        self.release()
