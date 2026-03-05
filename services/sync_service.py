import asyncio
import logging
from ..infrastructure.cached_google_sheets import GoogleSheetsRepository

async def run_sync_loop(repo: GoogleSheetsRepository, interval: int = 60):
    """Фоновая задача для сброса данных в Sheets"""
    while True:
        await asyncio.sleep(interval)
        try:
            logging.info("Начинаю пакетную выгрузку данных в Google Sheets...")
            await repo.flush_to_sheets()
            logging.info("Выгрузка завершена.")
        except Exception as e:
            logging.error(f"Ошибка при выгрузке в Sheets: {e}")