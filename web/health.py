from aiohttp import web
from datetime import datetime
from core.interfaces import IBookingRepository
from core.config import SYNC_STALE_MINUTES

class HealthServer:
    def __init__(self, repo: IBookingRepository, port: int):
        self.repo = repo
        self.port = port

    async def handle_healthz(self, request: web.Request):
        return web.json_response({"status": "alive"})

    async def handle_readyz(self, request: web.Request):
        last_sync = self.repo.get_last_sync_time()
        if not last_sync or (datetime.now() - last_sync).total_seconds() > SYNC_STALE_MINUTES * 60:
            return web.json_response({"status": "not ready"}, status=503)
        return web.json_response({"status": "ready"})

    async def start(self):
        app = web.Application()
        app.router.add_get("/healthz", self.handle_healthz)
        app.router.add_get("/readyz", self.handle_readyz)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()