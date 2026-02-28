import asyncio
import time
import logging
from bot import execute_booking

logging.basicConfig(level=logging.INFO)

async def simulate_user(user_id: int, event: str, time_str: str):
    """Имитирует одного пользователя, пытающегося записаться"""
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
    print(f"User {user_id:03d} | Время ответа: {elapsed:.2f}с | {status} | {result['text'].split(chr(10))[0]}")
    return result

async def run_load_test():
    print("🚀 НАЧАЛО НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ...")
    
    # Сценарий 1: Жесткая конкуренция за один тайм-слот
    # 10 человек одновременно пытаются записаться на массаж в 11:00 (там всего 3 мастера)
    print("\n--- СЦЕНАРИЙ 1: 10 человек на 3 места (Массаж 11:00) ---")
    tasks = []
    for i in range(1, 11):
        tasks.append(simulate_user(1000 + i, "массаж", "11:00"))
    
    results = await asyncio.gather(*tasks)
    
    success_count = sum(1 for r in results if r["ok"])
    print(f"\n📊 Итог Сценария 1: Записалось {success_count} из 10 (Ожидается ровно 3)")

    # Сценарий 2: Конкуренция за разные услуги одновременно
    # Проверяем, не блокирует ли запись на массаж запись на макияж
    print("\n--- СЦЕНАРИЙ 2: Параллельная запись на разные услуги ---")
    tasks = [
        simulate_user(2001, "макияж", "10:00"),
        simulate_user(2002, "мастерская чехова", "12:00"),
        simulate_user(2003, "аромапсихолог", "14:00"),
        simulate_user(2004, "макияж", "10:00"),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Перед запуском убедитесь, что в тестовой таблице пустые строки на это время!
    asyncio.run(run_load_test())