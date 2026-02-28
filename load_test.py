import asyncio
import time
import logging
from bot import execute_booking, sync_cache_with_google

logging.basicConfig(level=logging.INFO)

async def simulate_user(user_id: int, event: str, time_str: str):
    start_time = time.time()
    result = await execute_booking(
        user_id=user_id,
        username=f"@test_user_{user_id}",
        full_name=f"Test User {user_id}",
        event=event,
        time_str=time_str
    )
    elapsed = time.time() - start_time
    
    status = "✅ УСПЕХ" if result["ok"] else "❌ ОТКАЗ"
    print(f"User {user_id:04d} | Время: {elapsed:.2f}с | {status} | {result['text'].split(chr(10))[0]}")
    return result

async def run_load_test():
    print("⏳ Загрузка первоначального состояния из Google Sheets...")
    await sync_cache_with_google()
    
    print("\n🚀 НАЧАЛО НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ...")
    
    print("\n--- СЦЕНАРИЙ 1: 10 человек на 3 места (Массаж 11:00) ---")
    tasks = []
    for i in range(1, 11):
        tasks.append(simulate_user(1000 + i, "массаж", "11:00"))
    
    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r["ok"])
    print(f"\n📊 Итог Сценария 1: Записалось {success_count} из 10 (Ожидается ровно 3)")

if __name__ == "__main__":
    asyncio.run(run_load_test())