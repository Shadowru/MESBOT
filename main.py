import asyncio
import logging
from aiogram import Bot, Dispatcher
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.config import TELEGRAM_TOKEN, GOOGLE_CREDS_PATH, GOOGLE_SHEET_URL, OPENAI_API_KEY, HEALTH_PORT
from infrastructure.google_sheets import GoogleSheetsRepository
from infrastructure.openai_service import OpenAILLMService
from services.booking_service import BookingService
from presentation.handlers import router
from web.health import HealthServer

#async def run_sync_loop(repo: GoogleSheetsRepository, interval: int = 60):
#    """Фоновая задача для сброса данных в Sheets"""
#    while True:
#        await asyncio.sleep(interval)
#        try:
#            logging.info("Начинаю пакетную выгрузку данных в Google Sheets...")
#            await repo.flush_to_sheets()
#            logging.info("Выгрузка завершена.")
#        except Exception as e:
#            logging.error(f"Ошибка при выгрузке в Sheets: {e}")

async def main():
    logging.basicConfig(level=logging.INFO)

    # 1. Инициализация инфраструктуры (Repositories & Services)
    repo = GoogleSheetsRepository(GOOGLE_CREDS_PATH, GOOGLE_SHEET_URL)
    llm = OpenAILLMService(OPENAI_API_KEY)
    
    # 2. Инициализация бизнес-логики
    booking_service = BookingService(repo)

    # 3. Первичная синхронизация и запуск фоновых задач
    await repo.sync()

    #asyncio.create_task(run_sync_loop(repo, interval=20)) # 60 секунд

    scheduler = AsyncIOScheduler()
    scheduler.add_job(repo.sync, "interval", minutes=2)
    scheduler.start()

    # 4. Запуск Health Check сервера
    health_server = HealthServer(repo, HEALTH_PORT)
    await health_server.start()

    # 5. Настройка Telegram бота
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    # Прокидываем зависимости в хэндлеры (Dependency Injection)
    # В Aiogram 3 все ключи из workflow_data попадают в аргументы хэндлеров
    try:
        await dp.start_polling(bot, booking_service=booking_service, llm=llm)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())