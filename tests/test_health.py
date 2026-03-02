# tests/test_health.py

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from datetime import datetime, timedelta
from unittest.mock import MagicMock, AsyncMock

import bot as bot_module
from bot import handle_healthz, handle_readyz


@pytest.fixture(autouse=True)
def _reset_health_state(monkeypatch):
    monkeypatch.setattr(bot_module, "_cache_ready", False)
    monkeypatch.setattr(bot_module, "_last_sync_ok", None)
    monkeypatch.setattr(bot_module, "_sheet_cache", {})
    yield


def _make_request():
    return MagicMock(spec=web.Request)


# ── Liveness ──

@pytest.mark.asyncio
async def test_healthz_always_200():
    """Liveness отвечает 200 всегда — процесс жив."""
    resp = await handle_healthz(_make_request())
    assert resp.status == 200
    assert b"alive" in resp.body


@pytest.mark.asyncio
async def test_healthz_before_cache_loaded():
    """Liveness 200 даже до загрузки кэша."""
    bot_module._cache_ready = False
    resp = await handle_healthz(_make_request())
    assert resp.status == 200


# ── Readiness ──

@pytest.mark.asyncio
async def test_readyz_not_ready_before_sync():
    """Readiness 503, если кэш ещё не загружен."""
    bot_module._cache_ready = False
    resp = await handle_readyz(_make_request())
    assert resp.status == 503
    assert b"not ready" in resp.body


@pytest.mark.asyncio
async def test_readyz_not_ready_no_sync():
    """Readiness 503, если sync ни разу не прошёл."""
    bot_module._cache_ready = True
    bot_module._last_sync_ok = None
    resp = await handle_readyz(_make_request())
    assert resp.status == 503


@pytest.mark.asyncio
async def test_readyz_ready_after_sync():
    """Readiness 200 после успешной синхронизации."""
    bot_module._cache_ready = True
    bot_module._last_sync_ok = datetime.now()
    bot_module._sheet_cache = {"массаж": [], "макияж": []}
    resp = await handle_readyz(_make_request())
    assert resp.status == 200
    assert b"ready" in resp.body
    assert b"last_sync" in resp.body


@pytest.mark.asyncio
async def test_readyz_stale_sync():
    """Readiness 503, если синхронизация устарела."""
    bot_module._cache_ready = True
    bot_module._last_sync_ok = datetime.now() - timedelta(minutes=15)
    bot_module.SYNC_STALE_MINUTES = 10
    resp = await handle_readyz(_make_request())
    assert resp.status == 503
    assert b"stale" in resp.body


@pytest.mark.asyncio
async def test_readyz_fresh_sync():
    """Readiness 200, если синхронизация свежая."""
    bot_module._cache_ready = True
    bot_module._last_sync_ok = datetime.now() - timedelta(minutes=1)
    bot_module.SYNC_STALE_MINUTES = 10
    bot_module._sheet_cache = {"массаж": []}
    resp = await handle_readyz(_make_request())
    assert resp.status == 200


@pytest.mark.asyncio
async def test_readyz_returns_cached_events_count():
    """Readiness показывает количество закэшированных событий."""
    bot_module._cache_ready = True
    bot_module._last_sync_ok = datetime.now()
    bot_module._sheet_cache = {"массаж": [], "макияж": [], "гадалки": []}
    resp = await handle_readyz(_make_request())
    assert resp.status == 200
    assert b'"cached_events": 3' in resp.body