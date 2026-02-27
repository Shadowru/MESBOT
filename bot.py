import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import AsyncOpenAI
from dotenv import load_dotenv

import re
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

load_dotenv()

# === ПАМЯТЬ БОТА (FSM) ===
class BookingState(StatesGroup):
    waiting_for_time = State()
    
# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "google_creds.json")

if not GOOGLE_SHEET_URL:
    raise ValueError("Переменная GOOGLE_SHEET_URL не найдена!")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()
llm_client = AsyncOpenAI(base_url="https://openai.api.proxyapi.ru/v1", api_key=OPENAI_API_KEY)

# === ПОДКЛЮЧЕНИЕ К GOOGLE SHEETS ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_url(GOOGLE_SHEET_URL)

# === КОНФИГУРАЦИЯ И ОПИСАНИЯ УСЛУГ ===
EVENTS_CONFIG = {
    "аромапсихолог": {
        "sheet": "Аромапсихолог", "duration": 10, "capacity": 1, "start": "14:00", "end": "17:00",
        "desc": "🌸 **Аромапсихолог** — подбор индивидуальных эфирных масел для внутренней гармонии."
    },
    "макияж": {
        "sheet": "Макияж", "duration": 10, "capacity": 4, "start": "10:00", "end": "12:00",
        "desc": "💄 **Макияж** — легкий мейкап от визажистов, чтобы сиять весь день!"
    },
    "нутрициолог": {
        "sheet": "Нутрициолог", "duration": 90, "capacity": 30, "start": "15:00", "end": "16:30",
        "desc": "🥗 **Нутрициолог** — лекция о женском здоровье и энергии (Зал совещаний, 5 этаж)."
    },
    "массаж": {
        "sheet": "Массаж", "duration": 10, "capacity": 2, "start": "11:00", "end": "17:10",
        "desc": "💆‍♀️ **Массаж** — 10 минут релакса шейно-воротниковой зоны для снятия напряжения."
    },
    "гадалки": {
        "sheet": "Гадалки", "duration": 15, "capacity": 2, "start": "11:00", "end": "17:00",
        "desc": "🔮 **Таро и Гадалки** — узнайте, что готовят вам звезды и карты."
    }
}

# === СЛОВАРЬ СИНОНИМОВ ===
# Если нейросеть вернет левое слово, мы принудительно заменим его на правильный ключ
EVENT_ALIASES = {
    "гадалка": "гадалки",
    "таро": "гадалки",
    "таролог": "гадалки",
    "мэйкап": "макияж",
    "мейкап": "макияж",
    "психолог": "аромапсихолог",
    "арома": "аромапсихолог",
    "нутрицеолог": "нутрициолог", # на случай частых опечаток
    "нутрициолуг": "нутрициолог"
}

# === ФУНКЦИЯ АНАЛИЗА ТЕКСТА (NLP) ===
async def parse_intent(text: str) -> dict:
    prompt = f"""
    Ты заботливый бот-ассистент для записи девушек на корпоративные мероприятия. 
    Доступные мероприятия: аромапсихолог, макияж, нутрициолог, массаж, гадалки.
    
    Определи намерение пользователя. Возможные действия (action):
    - "book" (запись)
    - "cancel" (отмена записи)
    - "reschedule" (перенос записи на другое время)
    - "availability" (вопрос о свободных местах/слотах)
    - "info" (просьба рассказать об услугах подробнее)
    - "my_bookings" (просьба показать все свои записи, "куда я записана")
    
    Правила:
    1. Извлеки action, название мероприятия и время. Если это нутрициолог, время всегда 15:00.
    2. Если action это cancel, availability, info или my_bookings, время (time) может быть null.
    3. Если текст не относится к нашим услугам, верни null для всех полей.
    
    Ответь ТОЛЬКО валидным JSON: {{"action": "book|cancel|reschedule|availability|info|my_bookings", "event": "СТРОГО ОДНО ИЗ: аромапсихолог, макияж, нутрициолог, массаж, гадалки", "time": "HH:MM"}}
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

# === ВСПУМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def get_user_row_index(worksheet, user_id: str) -> int:
    ids = worksheet.col_values(1)
    try:
        return ids.index(str(user_id)) + 1
    except ValueError:
        return None

def check_massage_availability(time_str: str, current_bookings: list) -> str:
    breaks = {"Мастер 1": ["13:30", "13:40"], "Мастер 2": ["13:50", "14:00"], "Мастер 3": ["14:10", "14:20"]}
    for master in ["Мастер 1", "Мастер 2", "Мастер 3"]:
        if time_str not in breaks.get(master, []) and master not in current_bookings:
            return master
    return None

def get_available_slots(event: str, records: list) -> list:
    config = EVENTS_CONFIG[event]
    free_slots = []
    if event == "нутрициолог":
        booked_count = len([r for r in records if str(r.get("Время", "")) == "15:00"])
        remaining = config["capacity"] - booked_count
        if remaining > 0:
            return [f"15:00 (Осталось мест: {remaining})"]
        return []

    start_dt = datetime.strptime(config["start"], "%H:%M")
    end_dt = datetime.strptime(config["end"], "%H:%M")
    delta = timedelta(minutes=config["duration"])
    
    current_dt = start_dt
    while current_dt < end_dt:
        slot_str = current_dt.strftime("%H:%M")
        bookings_at_slot = [r for r in records if str(r.get("Время", "")) == slot_str]
        
        if event == "массаж":
            if check_massage_availability(slot_str, [r.get("Мастер/Детали") for r in bookings_at_slot]):
                free_slots.append(slot_str)
        else:
            if len(bookings_at_slot) < config["capacity"]:
                free_slots.append(slot_str)
        current_dt += delta
    return free_slots

def format_slots_message(slots: list) -> str:
    if not slots: return "К сожалению, свободных окошек больше не осталось 😔"
    return ", ".join(slots[:15]) + " ... и другие." if len(slots) > 15 else ", ".join(slots)

# === НОВЫЕ ФУНКЦИИ: ВСЕ ЗАПИСИ И ПРОВЕРКА НАЛОЖЕНИЙ ===
def get_all_user_bookings(user_id_str: str) -> list:
    """Собирает все записи пользователя по всем листам"""
    user_bookings = []
    for event_name, config in EVENTS_CONFIG.items():
        ws = sheet.worksheet(config["sheet"])
        records = ws.get_all_records()
        for row in records:
            if str(row.get("ID", "")) == user_id_str:
                user_bookings.append({
                    "event": event_name,
                    "time": str(row.get("Время", "")),
                    "duration": config["duration"]
                })
    return user_bookings

def check_time_conflict(new_event: str, new_time_str: str, user_bookings: list) -> tuple:
    """Проверяет, не пересекается ли новое время с уже существующими записями"""
    new_start = datetime.strptime(new_time_str, "%H:%M")
    new_end = new_start + timedelta(minutes=EVENTS_CONFIG[new_event]["duration"])

    for b in user_bookings:
        # Если это то же самое мероприятие (например, при переносе), пропускаем проверку с самим собой
        if b["event"] == new_event:
            continue
        
        b_start = datetime.strptime(b["time"], "%H:%M")
        b_end = b_start + timedelta(minutes=b["duration"])

        # Логика пересечения отрезков времени
        if new_start < b_end and new_end > b_start:
            return True, b["event"], b["time"]
            
    return False, None, None

# === ФУНКЦИЯ НАПОМИНАНИЯ ===
async def send_reminder(user_id: int, event_name: str, time_str: str):
    await bot.send_message(
        chat_id=user_id,
        text=f"✨ **Напоминалочка!**\nЗапись на **{event_name}** начнется через 3 минутки (в {time_str}). Ждем вас! 💖",
        parse_mode="Markdown"
    )

# === ОСНОВНОЙ ОБРАБОТЧИК ===
# === ОСНОВНОЙ ОБРАБОТЧИК ===
@dp.message()
async def handle_booking(message: types.Message, state: FSMContext):
    welcome_text = (
        "Привет, красавицы! 👋 Я ваш заботливый бот-помощник.\n"
        "Пишите мне свободно, например:\n"
        "✨ *«Запиши на массаж в 12:20»*\n"
        "🔄 *«Перенеси макияж на 11:30»*\n"
        "❌ *«Отмени нутрициолога»*\n"
        "📅 *«Какие есть окошки на гадалки?»*\n"
        "📋 *«Куда я записана?»*\n\n"
        "**Наши активности:**\n\n" + "\n\n".join([cfg["desc"] for cfg in EVENTS_CONFIG.values()])
    )

    intent = await parse_intent(message.text)
    current_state = await state.get_state()
    
    # --- ЛОГИКА ПАМЯТИ: ЕСЛИ МЫ ЖДЕМ ТОЛЬКО ВРЕМЯ ---
    if (not intent or not intent.get("action")) and current_state == BookingState.waiting_for_time.state:
        match = re.search(r'(\d{1,2})[.,:\s-]+(\d{2})', message.text)
        if match:
            hours, minutes = match.groups()
            time_str = f"{int(hours):02d}:{minutes}"
            
            # Достаем из памяти бота, куда человек хотел записаться
            data = await state.get_data()
            action = data.get('action')
            event = data.get('event')
            
            await state.clear() # Очищаем память
        else:
            if message.text.lower() in ["отмена", "отмени", "cancel", "нет"]:
                await state.clear()
                await message.reply("Действие отменено 😊")
            else:
                await message.reply("Не могу распознать время. Пожалуйста, напишите цифрами в формате ЧЧ:ММ (например, 15:30) 🕒\nИли напишите 'Отмена'.")
            return
            
    # --- ОБЫЧНАЯ ЛОГИКА (ЕСЛИ ЧЕЛОВЕК НАПИСАЛ КОМАНДУ ЦЕЛИКОМ) ---
    else:
        if not intent or not intent.get("action"):
            await message.reply(welcome_text, parse_mode="Markdown")
            await state.clear()
            return
            
        action = intent["action"]
        event = (intent.get("event") or "").lower()
        event = EVENT_ALIASES.get(event, event) if 'EVENT_ALIASES' in globals() else event
        time_str = intent.get("time")
        
        await state.clear() 

    user_id_str = str(message.from_user.id)
    username = f"@{message.from_user.username}" if message.from_user.username else "-"

    # --- ЛОГИКА: МОИ ЗАПИСИ ---
    if action == "my_bookings":
        wait_msg = await message.reply("⏳ Ищу ваши бьюти-планы, секундочку...")
        bookings = get_all_user_bookings(user_id_str)
        
        if not bookings:
            await wait_msg.edit_text("У вас пока нет ни одной записи. Давайте скорее запишемся! ✨")
            return
            
        msg_text = "📋 **Ваши планы на сегодня:**\n\n"
        for b in bookings:
            msg_text += f"🔸 **{b['event'].capitalize()}** — в {b['time']}\n"
        await wait_msg.edit_text(msg_text, parse_mode="Markdown")
        return

    # --- ЛОГИКА: ОТМЕНИТЬ ВООБЩЕ ВСЁ (С ПОДТВЕРЖДЕНИЕМ) ---
    if action == "cancel" and not event:
        bookings = get_all_user_bookings(user_id_str)
        if not bookings:
            await message.reply("У вас пока нет ни одной записи, отменять нечего 😊")
            return
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, отменить всё", callback_data="confirm_cancel_all")],
            [InlineKeyboardButton(text="❌ Нет, я передумала", callback_data="abort_cancel_all")]
        ])
        
        await message.reply(
            f"Вы точно хотите отменить **ВСЕ** ваши записи ({len(bookings)} шт.)? 😱",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        return

    # Если человек хочет записаться/перенести, но забыл указать куда
    if action in ["book", "reschedule", "availability"] and not event:
        await message.reply("Уточните, пожалуйста, о каком мероприятии идет речь? ✨\n(аромапсихолог, макияж, нутрициолог, массаж, гадалки)")
        return

    if action == "info" or event not in EVENTS_CONFIG:
        await message.reply(welcome_text, parse_mode="Markdown")
        return

    config = EVENTS_CONFIG[event]
    worksheet = sheet.worksheet(config["sheet"])
    job_id = f"{user_id_str}_{event}"
    records = worksheet.get_all_records()

    # --- ЛОГИКА: СВОБОДНЫЕ МЕСТА ---
    if action == "availability":
        free_slots = get_available_slots(event, records)
        await message.reply(f"📅 **Свободные окошки на {event.capitalize()}:**\n{format_slots_message(free_slots)}", parse_mode="Markdown")
        return

    # --- ЛОГИКА: ОТМЕНА КОНКРЕТНОЙ ЗАПИСИ ---
    if action == "cancel":
        row_idx = get_user_row_index(worksheet, user_id_str)
        if row_idx:
            worksheet.delete_rows(row_idx)
            if scheduler.get_job(job_id): scheduler.remove_job(job_id)
            await message.reply(f"🗑 Запись на **{event.capitalize()}** отменена. Ждем вас в другой раз 🌸", parse_mode="Markdown")
        else:
            await message.reply(f"У вас пока нет записи на **{event.capitalize()}** 😊", parse_mode="Markdown")
        return

    # --- ВКЛЮЧЕНИЕ РЕЖИМА ОЖИДАНИЯ ВРЕМЕНИ ---
    if not time_str:
        if action in ["book", "reschedule"]:
            await state.update_data(action=action, event=event)
            await state.set_state(BookingState.waiting_for_time)
            await message.reply(f"Пожалуйста, уточните время для записи на **{event.capitalize()}** (например, 14:20) 🕒", parse_mode="Markdown")
            return
        else:
            await message.reply("Пожалуйста, уточните время записи (например, 14:20) 🕒")
            return

    try:
        datetime.strptime(time_str, "%H:%M")
    except ValueError:
        await message.reply("Кажется, я не поняла время. Напишите его в формате ЧЧ:ММ (например, 15:30) 🕒")
        return

    row_idx = get_user_row_index(worksheet, user_id_str)

    # --- ЛОГИКА: ПЕРЕНОС ---
    if action == "reschedule":
        if not row_idx:
            await message.reply(f"У вас еще нет записи на **{event.capitalize()}**. Напишите 'Запиши меня на {event} в {time_str}' ✨", parse_mode="Markdown")
            return
        records = [r for r in records if str(r.get("ID", "")) != user_id_str]

    # --- ЛОГИКА: ЗАПИСЬ ---
    elif action == "book":
        if row_idx:
            booked_time = next((r.get("Время", "") for r in records if str(r.get("ID", "")) == user_id_str), "неизвестно")
            await message.reply(f"❌ Вы уже записаны на **{event.capitalize()}** (ваше время: {booked_time}).\nДля изменения напишите 'Перенеси мою запись на ...' 🔄", parse_mode="Markdown")
            return

    # --- 🛑 ПРОВЕРКА НАЛОЖЕНИЯ ПО ВРЕМЕНИ ---
    all_user_bookings = get_all_user_bookings(user_id_str)
    is_conflict, conflict_event, conflict_time = check_time_conflict(event, time_str, all_user_bookings)
    
    if is_conflict:
        await message.reply(
            f"Ой, накладочка! 😱\n"
            f"Вы не можете записаться на **{event.capitalize()}** в {time_str}, "
            f"так как в это время вы будете на **{conflict_event.capitalize()}** (запись на {conflict_time}).\n"
            f"Пожалуйста, выберите другое время! 🕒", 
            parse_mode="Markdown"
        )
        return

    # --- ПРОВЕРКА ЛИМИТОВ И МАСТЕРОВ ---
    bookings_at_time = [r for r in records if str(r.get("Время", "")) == time_str]
    assigned_master = ""

    if event == "массаж":
        busy_masters = [r.get("Мастер/Детали") for r in bookings_at_time]
        assigned_master = check_massage_availability(time_str, busy_masters)
        if not assigned_master:
            free_slots = get_available_slots(event, records)
            await message.reply(f"Ой, на {time_str} все мастера заняты или у них перерыв 😔\n\n💡 **Доступные окошки:**\n{format_slots_message(free_slots)}", parse_mode="Markdown")
            return
            
    elif len(bookings_at_time) >= config["capacity"]:
        free_slots = get_available_slots(event, records)
        await message.reply(f"Ой, на {time_str} ({event}) уже всё занято 😔\n\n💡 **Доступные окошки:**\n{format_slots_message(free_slots)}", parse_mode="Markdown")
        return

    # --- ЗАПИСЬ В ТАБЛИЦУ ---
    if action == "reschedule":
        worksheet.delete_rows(row_idx)
        if scheduler.get_job(job_id): scheduler.remove_job(job_id)

    worksheet.append_row([
        message.from_user.id,
        username,
        message.from_user.full_name,
        time_str,
        assigned_master if assigned_master else "Записано"
    ])

    # --- ПЛАНИРОВАНИЕ НАПОМИНАНИЯ ---
    try:
        now = datetime.now()
        event_time = datetime.strptime(time_str, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        reminder_time = event_time - timedelta(minutes=3)

        if reminder_time > now:
            scheduler.add_job(
                send_reminder, 'date', run_date=reminder_time, 
                args=[message.from_user.id, event.capitalize(), time_str],
                id=job_id, replace_existing=True
            )
    except Exception as e:
        logging.error(f"Ошибка при планировании времени: {e}")

    # --- ОТВЕТ ПОЛЬЗОВАТЕЛЮ ---
    if action == "reschedule":
        msg_reply = f"🔄 Супер! Мы перенесли вашу запись на **{event.capitalize()}**. Ждем вас в {time_str}!"
    else:
        msg_reply = f"🎉 Ура! Вы успешно записаны на **{event.capitalize()}** в {time_str}!"
        
    if assigned_master: msg_reply += f"\nВаш заботливый мастер: {assigned_master} 💆‍♀️"
    if event == "нутрициолог": msg_reply += "\n📍 Ждем вас: Зал совещаний (5 этаж) 🥗"
        
    await message.reply(msg_reply, parse_mode="Markdown")
    
@dp.callback_query(F.data == "confirm_cancel_all")
async def process_confirm_cancel_all(callback: types.CallbackQuery):
    # Обязательно отвечаем Telegram, что кнопка нажата (чтобы часики на кнопке пропали)
    await callback.answer()
    
    user_id_str = str(callback.from_user.id)
    
    # Меняем текст сообщения на "В процессе..."
    await callback.message.edit_text("⏳ Удаляю ваши записи, секундочку...")
    
    bookings = get_all_user_bookings(user_id_str)
    if not bookings:
        await callback.message.edit_text("Записей уже нет, отменять нечего 😊")
        return

    # Проходимся по всем таблицам и удаляем
    for b in bookings:
        ev_name = b["event"]
        ws = sheet.worksheet(EVENTS_CONFIG[ev_name]["sheet"])
        r_idx = get_user_row_index(ws, user_id_str)
        if r_idx:
            ws.delete_rows(r_idx)
        
        # Удаляем таймеры напоминаний
        j_id = f"{user_id_str}_{ev_name}"
        if scheduler.get_job(j_id):
            scheduler.remove_job(j_id)
            
    # Пишем финальный результат
    await callback.message.edit_text("🗑 Все ваши записи были успешно отменены! Будем рады видеть вас снова 🌸")


@dp.callback_query(F.data == "abort_cancel_all")
async def process_abort_cancel_all(callback: types.CallbackQuery):
    await callback.answer()
    # Если девушка передумала, просто меняем текст сообщения
    await callback.message.edit_text("Фух! Оставили всё как есть. Ждем вас на мероприятиях! 🥰")    

# === ЗАПУСК БОТА ===
async def main():
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())