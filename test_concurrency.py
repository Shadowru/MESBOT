import asyncio
from unittest.mock import AsyncMock
from services.booking_service import BookingService
from core.models import BookingRecord

# 1. Мок репозитория с задержкой (имитация сети Google Sheets)
class MockRepo:
    def __init__(self):
        self.records = []
        self.lock = asyncio.Lock()

    async def get_records(self, event):
        # Имитация задержки сети
        await asyncio.sleep(0.1) 
        return [r for r in self.records if r.event == event]

    async def add_record(self, record):
        # Имитация задержки записи
        await asyncio.sleep(0.1)
        self.records.append(record)

    async def delete_record(self, event, user_id):
        self.records = [r for r in self.records if not (r.event == event and r.user_id == user_id)]

# 2. Функция для имитации пользователя
async def simulate_user(service, user_id, event, time_str):
    print(f"Пользователь {user_id} пытается записаться...")
    res = await service.execute_booking(
        user_id=str(user_id),
        username=f"user_{user_id}",
        full_name=f"Name {user_id}",
        event=event,
        time_str=time_str
    )
    print(f"Пользователь {user_id} результат: {res}")
    return res

async def main():
    repo = MockRepo()
    service = BookingService(repo)
    
    # Имитируем 10 пользователей, пытающихся занять 1 слот
    event = "массаж"
    time_str = "12:00"
    
    tasks = [simulate_user(service, i, event, time_str) for i in range(10)]
    
    print("--- Запуск теста конкурентности ---")
    results = await asyncio.gather(*tasks)
    
    successes = [r for r in results if r["ok"]]
    print(f"\nИтого успешных записей: {len(successes)}")
    print(f"Всего записей в базе: {len(repo.records)}")

if __name__ == "__main__":
    asyncio.run(main())