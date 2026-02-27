import asyncio
import json
import logging
import os

from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


# Берем настройки из переменных окружения K8s
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
# Путь к файлу, который мы смонтируем через K8s Secrets
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "/app/secrets/google_creds.json")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()
llm_client = AsyncOpenAI(
    base_url="https://openai.api.proxyapi.ru/v1",
    api_key=OPENAI_API_KEY
    )

# === ПОДКЛЮЧЕНИЕ К GOOGLE SHEETS ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_url(GOOGLE_SHEET_URL)

# Конфигурация слотов и лимитов
EVENTS_CONFIG = {
    "аромапсихолог": {"sheet": "Аромапсихолог", "duration": 10, "capacity": 1, "start": "14:00", "end": "17:00"},
    "макияж": {"sheet": "Макияж", "duration": 10, "capacity": 4, "start": "10:00", "end": "12:00"},
    "нутрициолог": {"sheet": "Нутрициолог", "duration": 90, "capacity": 30, "start": "15:00", "end": "16:30"},
    "массаж": {"sheet": "Массаж", "duration": 10, "capacity": 2, "start": "11:00", "end": "17:10"},
    "гадалки": {"sheet": "Гадалки", "duration": 15, "capacity": 2, "start": "11:00", "end": "17:00"}
}

# === ФУНКЦИЯ АНАЛИЗА ТЕКСТА (NLP) ===
async def parse_intent(text: str) -> dict:
    prompt = f"""
    Ты бот-ассистент для записи на корпоративные мероприятия. 
    Доступные мероприятия: аромапсихолог, макияж, нутрициолог, массаж, гадалки.
    
    Определи намерение пользователя. Возможные действия (action):
    - "book" (запись)
    - "cancel" (отмена записи)
    - "reschedule" (перенос записи на другое время)
    - "availability" (пользователь спрашивает, какие есть свободные места/слоты)
    
    Правила:
    1. Извлеки action, название мероприятия и время.
    2. Если пользователь отменяет запись или спрашивает свободные слоты, время (time) может быть null.
    3. Если текст не относится к записи/отмене/переносу/вопросу о местах, верни null для всех полей.
    
    Ответь ТОЛЬКО валидным JSON: {{"action": "book|cancel|reschedule|availability", "event": "название_в_нижнем_регистре", "time": "HH:MM"}}
    Текст пользователя: {text}
    """
    response = await llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    try:
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logging.error(f"Ошибка парсинга JSON: {e}")
        return None
    
# === ФУНКЦИЯ НАПОМИНАНИЯ ===
async def send_reminder(user_id: int, event_name: str, time_str: str):
    await bot.send_message(
        chat_id=user_id,
        text=f"🔔 Напоминание! Ваша запись на **{event_name}** начнется ровно через 3 минуты (в {time_str}). Ждем вас!"
    )

# === ЛОГИКА ПРОВЕРКИ МАССАЖА (СЛОЖНЫЕ ПЕРЕРЫВЫ) ===
def check_massage_availability(time_str: str, current_bookings: list) -> str:
    # current_bookings - список занятых мастеров на это время, например ["Мастер 1"]
    breaks = {
        "Мастер 1": ["13:30", "13:40"],
        "Мастер 2": ["13:50", "14:00", "14:10", "14:20"] 
    }
    
    for master in ["Мастер 1", "Мастер 2"]:
        if time_str not in breaks.get(master, []) and master not in current_bookings:
            return master
    return None


# === ПОИСК СТРОКИ ПОЛЬЗОВАТЕЛЯ ===
def get_user_row_index(worksheet, user_id: str) -> int:
    # Получаем все значения первого столбца (ID)
    ids = worksheet.col_values(1)
    try:
        # +1 потому что индексы в gspread начинаются с 1
        return ids.index(str(user_id)) + 1
    except ValueError:
        return None
    
# === ГЕНЕРАЦИЯ И ПРОВЕРКА СВОБОДНЫХ СЛОТОВ ===
def get_available_slots(event: str, records: list) -> list:
    config = EVENTS_CONFIG[event]
    free_slots = []
    
    # Спец. логика для нутрициолога (одно время, много мест)
    if event == "нутрициолог":
        booked_count = len([r for r in records if str(r.get("Время", "")) == "15:00"])
        remaining = config["capacity"] - booked_count
        if remaining > 0:
            return [f"15:00 (Осталось мест: {remaining})"]
        return []

    # Генерируем все возможные слоты от start до end
    start_dt = datetime.strptime(config["start"], "%H:%M")
    end_dt = datetime.strptime(config["end"], "%H:%M")
    delta = timedelta(minutes=config["duration"])
    
    current_dt = start_dt
    while current_dt < end_dt:
        slot_str = current_dt.strftime("%H:%M")
        
        # Смотрим, сколько людей уже записано на этот слот
        bookings_at_slot = [r for r in records if str(r.get("Время", "")) == slot_str]
        
        if event == "массаж":
            busy_masters = [r.get("Мастер/Детали") for r in bookings_at_slot]
            # Если функция возвращает мастера, значит слот свободен
            if check_massage_availability(slot_str, busy_masters):
                free_slots.append(slot_str)
        else:
            # Для макияжа, гадалок и аромапсихолога
            if len(bookings_at_slot) < config["capacity"]:
                free_slots.append(slot_str)
                
        current_dt += delta
        
    return free_slots

def format_slots_message(slots: list) -> str:
    if not slots:
        return "К сожалению, свободных мест больше нет 😔"
    # Если слотов слишком много, показываем только первые 15, чтобы не спамить
    if len(slots) > 15:
        return ", ".join(slots[:15]) + " ... и другие более поздние."
    return ", ".join(slots)
    
# === ОБРАБОТЧИК СООБЩЕНИЙ ===
@dp.message()
async def handle_booking(message: types.Message):
    intent = await parse_intent(message.text)
    
    if not intent or not intent.get("action") or not intent.get("event"):
        await message.reply(
            "Здравствуйте! Я бот для записи на мероприятия.\n"
            "Вы можете написать мне:\n"
            "✅ *Запиши меня на массаж в 12:00*\n"
            "🔄 *Перенеси мой макияж на 11:30*\n"
            "❌ *Отмени мою запись к нутрициологу*\n\n"
            "Доступно: аромапсихолог, макияж, нутрициолог, массаж, гадалки.",
            parse_mode="Markdown"
        )
        return

    action = intent["action"]
    event = intent["event"].lower()
    time_str = intent.get("time")
    user_id_str = str(message.from_user.id)

    if event not in EVENTS_CONFIG:
        await message.reply("К сожалению, такого мероприятия нет. Есть: аромапсихолог, макияж, нутрициолог, массаж, гадалки.")
        return

    config = EVENTS_CONFIG[event]
    worksheet = sheet.worksheet(config["sheet"])
    job_id = f"{user_id_str}_{event}" # Уникальный ID для таймера напоминания

    # === ЛОГИКА ОТМЕНЫ (CANCEL) ===
    if action == "cancel":
        row_idx = get_user_row_index(worksheet, user_id_str)
        if row_idx:
            worksheet.delete_rows(row_idx) # Удаляем из таблицы
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id) # Удаляем напоминание
            await message.reply(f"🗑 Ваша запись на **{event.capitalize()}** успешно отменена.")
        else:
            await message.reply(f"У вас нет активной записи на **{event.capitalize()}**, отменять нечего.")
        return

    # Если это запись или перенос, нам обязательно нужно время
    if not time_str:
        await message.reply("Пожалуйста, укажите время (например, 14:20).")
        return

    # Получаем все записи для проверок
    records = worksheet.get_all_records()
    row_idx = get_user_row_index(worksheet, user_id_str)

    # === ЛОГИКА ПЕРЕНОСА (RESCHEDULE) ===
    if action == "reschedule":
        if not row_idx:
            await message.reply(f"У вас нет записи на **{event.capitalize()}**. Давайте сначала запишемся! Напишите 'Запиши меня на {event} в {time_str}'.")
            return
        # Для проверки лимитов временно "исключаем" текущую запись пользователя
        records = [r for r in records if str(r.get("ID", "")) != user_id_str]

    # === ЛОГИКА ЗАПИСИ (BOOK) ===
    elif action == "book":
        if row_idx:
            # Находим время, на которое он уже записан
            booked_time = next((r.get("Время", "") for r in records if str(r.get("ID", "")) == user_id_str), "неизвестно")
            await message.reply(f"❌ Вы уже записаны на **{event.capitalize()}** (ваше время: {booked_time}).\nЕсли хотите изменить время, напишите 'Перенеси мою запись на ...'.")
            return

    # --- ПРОВЕРКА ЛИМИТОВ И МАСТЕРОВ ДЛЯ НОВОГО ВРЕМЕНИ ---
    bookings_at_time = [r for r in records if str(r.get("Время", "")) == time_str]
    assigned_master = ""

    if event == "массаж":
        busy_masters = [r.get("Мастер/Детали") for r in bookings_at_time]
        assigned_master = check_massage_availability(time_str, busy_masters)
        if not assigned_master:
            await message.reply(f"К сожалению, на {time_str} все мастера заняты или у них перерыв. Выберите другое время.")
            return
    elif len(bookings_at_time) >= config["capacity"]:
        await message.reply(f"К сожалению, на {time_str} ({event}) уже нет мест. Выберите другое время.")
        return

    # --- ПРИМЕНЕНИЕ ИЗМЕНЕНИЙ В ТАБЛИЦУ ---
    if action == "reschedule":
        worksheet.delete_rows(row_idx) # Удаляем старую запись при переносе
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id) # Удаляем старое напоминание

    # Добавляем новую запись (для book и reschedule)
    worksheet.append_row([
        message.from_user.id,
        message.from_user.full_name,
        time_str,
        assigned_master if assigned_master else "Записано"
    ])

    # --- ПЛАНИРОВАНИЕ НАПОМИНАНИЯ ---
    try:
        now = datetime.now()
        event_time = datetime.strptime(time_str, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        reminder_time = event_time - timedelta(minutes=3)

        if reminder_time > now:
            scheduler.add_job(
                send_reminder, 
                'date', 
                run_date=reminder_time, 
                args=[message.from_user.id, event.capitalize(), time_str],
                id=job_id, # Устанавливаем ID для возможности удаления
                replace_existing=True
            )
    except Exception as e:
        logging.error(f"Ошибка при планировании времени: {e}")

    # --- ОТВЕТ ПОЛЬЗОВАТЕЛЮ ---
    if action == "reschedule":
        msg_reply = f"🔄 Ваша запись на **{event.capitalize()}** успешно перенесена на {time_str}!"
    else:
        msg_reply = f"✅ Вы успешно записаны на **{event.capitalize()}** в {time_str}!"
        
    if assigned_master:
        msg_reply += f"\nВаш мастер: {assigned_master}"
    if event == "нутрициолог":
        msg_reply += "\n📍 Место: Зал совещаний (5 этаж)"
        
    await message.reply(msg_reply)
    
# === ЗАПУСК БОТА ===
async def main():
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())