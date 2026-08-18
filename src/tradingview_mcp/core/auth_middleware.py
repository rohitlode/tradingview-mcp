"""Bearer-token auth for the streamable-http transport (2026-08-18).

This server has no built-in auth (see ``proxy_manager.py``'s own docstring
and ``TradingViewMcpConnector``'s docstring, both of which flagged this as
a prerequisite "before any public/remote exposure"). It has been reachable
only on ``127.0.0.1`` until now. This module closes that gap for the one
scenario it actually needs to cover: reaching this server over a private
Tailscale network from another machine you control -- NOT public internet
exposure, which this alone would not make safe (no rate limiting, no
per-token scoping, a single shared secret).

Usage: set ``MCP_BEARER_TOKEN`` in ``.env``; every request must then carry
``Authorization: Bearer <token>``. If the env var is unset, the middleware
is a no-op (so local-only ``stdio``/dev usage is unaffected) -- but a
non-loopback ``--host`` MUST have a token configured, enforced by
``server.py`` at startup, not silently allowed.
"""
from __future__ import annotations

import hmac
import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


def get_bearer_token() -> str | None:
    """The configured token, or ``None`` if auth is disabled (loopback-only use)."""
    token = os.environ.get("MCP_BEARER_TOKEN", "").strip()
    return token or None


_LOOPBACK_HOSTS = {"127.0.0.1", "::1"}


class BearerTokenMiddleware(BaseHTTPMiddleware):
    """Reject any non-loopback request without ``Authorization: Bearer <token>``.

    Loopback (127.0.0.1/::1) traffic is exempt on purpose: this exists to
    gate REMOTE access (e.g. over Tailscale), not to require every existing
    localhost consumer (this same machine's TSS instance) to be reconfigured
    with a token just to keep working. Uses ``hmac.compare_digest``
    (constant-time) rather than ``==`` so response timing can't be used to
    guess the token character-by-character.
    """

    def __init__(self, app, token: str) -> None:
        super().__init__(app)
        self._token = token

    async def dispatch(self, request: Request, call_next):
        client_host = request.client.host if request.client else None
        if client_host in _LOOPBACK_HOSTS:
            return await call_next(request)
        auth = request.headers.get("authorization", "")
        scheme, _, presented = auth.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(presented, self._token):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)
