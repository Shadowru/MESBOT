import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

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


# ══════════════════════════════════════════════
#  FSM
# ══════════════════════════════════════════════
class BookingState(StatesGroup):
    waiting_for_time = State()


# ══════════════════════════════════════════════
#  НАСТРОЙКИ / ПОДКЛЮЧЕНИЯ
# ══════════════════════════════════════════════
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "google_creds.json")

if not GOOGLE_SHEET_URL:
    raise ValueError("Переменная GOOGLE_SHEET_URL не найдена!")

bot        = Bot(token=TELEGRAM_TOKEN)
dp         = Dispatcher()
scheduler  = AsyncIOScheduler()
llm_client = AsyncOpenAI(base_url="https://openai.api.proxyapi.ru/v1", api_key=OPENAI_API_KEY)

scope     = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds     = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
gs_client = gspread.authorize(creds)
sheet     = gs_client.open_by_url(GOOGLE_SHEET_URL)


# ══════════════════════════════════════════════
#  КОНФИГУРАЦИЯ УСЛУГ
# ══════════════════════════════════════════════
EVENTS_CONFIG = {
    "аромапсихолог": {
        "sheet": "Аромапсихолог", "duration": 10, "capacity": 1,
        "start": "14:00", "end": "17:00",
        "desc": "🌸 **Аромапсихолог** — подбор индивидуальных эфирных масел для внутренней гармонии.",
    },
    "макияж": {
        "sheet": "Макияж", "duration": 10, "capacity": 4,
        "start": "10:00", "end": "12:00",
        "desc": "💄 **Макияж** — легкий мейкап от визажистов (4 мастера), чтобы сиять весь день!",
    },
    "нутрициолог": {
        "sheet": "Нутрициолог", "duration": 90, "capacity": 30,
        "start": "15:00", "end": "16:30",
        "desc": "🥗 **Нутрициолог** — лекция о женском здоровье и энергии (Зал совещаний, 5 этаж).",
    },
    "массаж": {
        "sheet": "Массаж", "duration": 10, "capacity": 3,
        "start": "11:00", "end": "17:10",
        "desc": "💆‍♀️ **Массаж** — 10 минут релакса (мастера: Виктор, Нарек, Ольга).",
    },
    "гадалки": {
        "sheet": "Гадалки", "duration": 15, "capacity": 2,
        "start": "11:00", "end": "17:00",
        "desc": (
            "🔮 **Таро и Гадалки** — узнайте, что готовят вам звезды и карты.\n"
            "   • Юлия — переговорка 614а\n"
            "   • Натэлла — переговорка №3, 1 этаж"
        ),
    },
}


# ══════════════════════════════════════════════
#  МАСТЕРА / СПЕЦИАЛИСТЫ
# ══════════════════════════════════════════════
MASTERS_CONFIG = {
    "массаж": [
        {"id": "Мастер №1 Виктор", "name": "Виктор", "label": "Мастер №1 Виктор",
         "location": "", "breaks": ["13:30", "13:40"]},
        {"id": "Мастер №2 Нарек",  "name": "Нарек",  "label": "Мастер №2 Нарек",
         "location": "", "breaks": ["13:50", "14:00"]},
        {"id": "Мастер №3 Ольга",  "name": "Ольга",  "label": "Мастер №3 Ольга",
         "location": "", "breaks": ["14:10", "14:20"]},
    ],
    "гадалки": [
        {"id": "Гадалка Юлия",   "name": "Юлия",   "label": "Гадалка Юлия",
         "location": "переговорка 614а",        "breaks": []},
        {"id": "Гадалка Натэлла", "name": "Натэлла", "label": "Гадалка Натэлла",
         "location": "переговорка №3, 1 этаж",  "breaks": []},
    ],
    "макияж": [
        {"id": "Визажист №1", "name": "Визажист №1", "label": "Визажист №1", "location": "", "breaks": []},
        {"id": "Визажист №2", "name": "Визажист №2", "label": "Визажист №2", "location": "", "breaks": []},
        {"id": "Визажист №3", "name": "Визажист №3", "label": "Визажист №3", "location": "", "breaks": []},
        {"id": "Визажист №4", "name": "Визажист №4", "label": "Визажист №4", "location": "", "breaks": []},
    ],
}


# ══════════════════════════════════════════════
#  СИНОНИМЫ
# ══════════════════════════════════════════════
EVENT_ALIASES = {
    "гадалка": "гадалки", "таро": "гадалки", "таролог": "гадалки",
    "мэйкап": "макияж", "мейкап": "макияж",
    "психолог": "аромапсихолог", "арома": "аромапсихолог",
    "нутрицеолог": "нутрициолог", "нутрициолуг": "нутрициолог",
}


# ══════════════════════════════════════════════
#  СКЛОНЕНИЯ РУССКОГО ЯЗЫКА
# ══════════════════════════════════════════════
EVENT_FORMS = {
    "аромапсихолог": {"to": "к аромапсихологу",  "at": "у аромапсихолога",  "acc": "аромапсихолога",  "title": "Аромапсихолог"},
    "макияж":        {"to": "на макияж",          "at": "на макияж",         "acc": "макияж",          "title": "Макияж"},
    "нутрициолог":   {"to": "к нутрициологу",     "at": "у нутрициолога",    "acc": "нутрициолога",    "title": "Нутрициолог"},
    "массаж":        {"to": "на массаж",          "at": "на массаж",         "acc": "массаж",          "title": "Массаж"},
    "гадалки":       {"to": "к гадалке",          "at": "у гадалок",         "acc": "гадалок",         "title": "Гадалки"},
}


def ef(event: str, form: str = "title") -> str:
    return EVENT_FORMS.get(event, {}).get(form, event.capitalize())


def plural_masters(n: int, event: str = "") -> str:
    if event == "гадалки":
        word = ("гадалка", "гадалки", "гадалок")
    elif event == "макияж":
        word = ("визажист", "визажиста", "визажистов")
    else:
        word = ("мастер", "мастера", "мастеров")
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} {word[0]}"
    elif 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14):
        return f"{n} {word[1]}"
    return f"{n} {word[2]}"


def plural_places(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} место"
    elif 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14):
        return f"{n} места"
    return f"{n} мест"


# ══════════════════════════════════════════════
#  БЛОКИРОВКА ПАРАЛЛЕЛЬНЫХ ЗАПИСЕЙ
# ══════════════════════════════════════════════
_booking_locks: dict[str, asyncio.Lock] = {}


def get_lock(event: str) -> asyncio.Lock:
    if event not in _booking_locks:
        _booking_locks[event] = asyncio.Lock()
    return _booking_locks[event]


# ══════════════════════════════════════════════
#  NLP: АНАЛИЗ ТЕКСТА
# ══════════════════════════════════════════════
async def parse_intent(text: str) -> dict:
    prompt = f"""
Ты заботливый бот-ассистент для записи девушек на корпоративные мероприятия. 
Доступные мероприятия: аромапсихолог, макияж, нутрициолог, массаж, гадалки.

Известные специалисты:
- Гадалки: Юлия, Натэлла
- Массаж: Виктор, Нарек, Ольга

Определи намерение пользователя. Возможные действия (action):
- "book" (запись — включая случаи, когда пользователь просто пишет название услуги: «массаж», «хочу на массаж», «гадалки»)
- "cancel" (отмена записи)
- "reschedule" (перенос записи на другое время)
- "availability" (вопрос о свободных местах/слотах)
- "info" (ТОЛЬКО если пользователь ЯВНО просит рассказать/описать/узнать подробности об услуге. Примеры: «расскажи про массаж», «что за услуги?», «какие активности есть?», «подробнее о макияже», «что вы предлагаете?»)
- "my_bookings" (просьба показать все свои записи)

КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА:
1. Если пользователь просто пишет название услуги («массаж», «гадалки», «макияж», «нутрициолог») — это action="book", НЕ "info"!
2. action="info" — ТОЛЬКО при явной просьбе узнать подробности/описание (слова: «расскажи», «что такое», «подробнее», «информация», «какие услуги»).
3. Если пользователь пишет что-то вроде «хочу массаж», «давай на массаж», «можно на массаж» — это "book".
4. Извлеки название мероприятия, время и предпочтительного мастера.
5. Если это нутрициолог, время всегда 15:00.
6. Если action это cancel, availability, info или my_bookings — time может быть null.
7. preferred_master — имя мастера/гадалки, если пользователь ЯВНО указал (напр. «к Юлии»). Иначе null.
8. Если event не указан при action="info", поставь event=null (пользователь спрашивает обо всех услугах).
9. Если текст не относится к услугам и не является приветствием, верни null для всех полей.

Ответь ТОЛЬКО валидным JSON:
{{"action":"...","event":"СТРОГО ОДНО ИЗ: аромапсихолог, макияж, нутрициолог, массаж, гадалки или null","time":"HH:MM или null","preferred_master":"имя или null"}}

Текст: {text}
"""
    response = await llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    try:
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logging.error(f"Ошибка парсинга JSON: {e}")
        return None


# ══════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════
def get_user_row_index(worksheet, user_id: str) -> int:
    ids = worksheet.col_values(1)
    try:
        return ids.index(str(user_id)) + 1
    except ValueError:
        return None


def find_available_master(event, time_str, bookings_at_time, preferred_name=None):
    if event not in MASTERS_CONFIG:
        return None, None
    masters  = MASTERS_CONFIG[event]
    busy_ids = [str(r.get("Мастер/Детали", "")) for r in bookings_at_time]
    if preferred_name:
        pn = preferred_name.lower().strip()
        matched = next((m for m in masters if pn in m["name"].lower() or pn in m["label"].lower()), None)
        if matched:
            if time_str in matched.get("breaks", []):
                return None, f"У **{matched['label']}** в {time_str} перерыв 😔"
            if matched["id"] in busy_ids:
                return None, f"**{matched['label']}** уже занят(а) в {time_str} 😔"
            return matched, None
    for m in masters:
        if time_str not in m.get("breaks", []) and m["id"] not in busy_ids:
            return m, None
    return None, None


def count_available_masters(event, time_str, bookings_at_time, preferred_name=None) -> int:
    if event not in MASTERS_CONFIG:
        return 0
    masters  = MASTERS_CONFIG[event]
    busy_ids = [str(r.get("Мастер/Детали", "")) for r in bookings_at_time]
    count = 0
    for m in masters:
        if time_str in m.get("breaks", []):
            continue
        if m["id"] in busy_ids:
            continue
        if preferred_name:
            pn = preferred_name.lower().strip()
            if pn not in m["name"].lower() and pn not in m["label"].lower():
                continue
        count += 1
    return count


def is_valid_slot_time(event: str, time_str: str) -> tuple:
    config = EVENTS_CONFIG[event]
    if event == "нутрициолог":
        return (True, None) if time_str == "15:00" else (
            False, "Лекция нутрициолога начинается строго в **15:00** 🕒"
        )
    start_dt = datetime.strptime(config["start"], "%H:%M")
    end_dt   = datetime.strptime(config["end"],   "%H:%M")
    req_dt   = datetime.strptime(time_str, "%H:%M")
    if req_dt < start_dt or req_dt >= end_dt:
        return False, (
            f"⏰ **{ef(event)}** работает с {config['start']} до {config['end']}.\n"
            f"Пожалуйста, выберите время в этом диапазоне!"
        )
    mins = int((req_dt - start_dt).total_seconds() / 60)
    dur  = config["duration"]
    if mins % dur != 0:
        prev = start_dt + timedelta(minutes=(mins // dur) * dur)
        nxt  = prev + timedelta(minutes=dur)
        opts = []
        if prev >= start_dt:
            opts.append(prev.strftime("%H:%M"))
        if nxt < end_dt:
            opts.append(nxt.strftime("%H:%M"))
        return False, (
            f"Записи {ef(event, 'at')} идут каждые {dur} мин.\n"
            f"Ближайшие слоты: **{', '.join(opts)}** 🕒"
        )
    return True, None


# ══════════════════════════════════════════════
#  ПОДСКАЗКИ СВОБОДНЫХ СЛОТОВ
# ══════════════════════════════════════════════
def get_suggested_slots(event, records, preferred_master=None, top_n=6) -> list:
    config = EVENTS_CONFIG[event]
    if event == "нутрициолог":
        booked = len([r for r in records if str(r.get("Время", "")) == "15:00"])
        rem = config["capacity"] - booked
        return [("15:00", rem)] if rem > 0 else []
    start_dt = datetime.strptime(config["start"], "%H:%M")
    end_dt   = datetime.strptime(config["end"],   "%H:%M")
    delta    = timedelta(minutes=config["duration"])
    slots = []
    cur = start_dt
    while cur < end_dt:
        s = cur.strftime("%H:%M")
        at_slot = [r for r in records if str(r.get("Время", "")) == s]
        if event in MASTERS_CONFIG:
            avail = count_available_masters(event, s, at_slot, preferred_master)
        else:
            avail = config["capacity"] - len(at_slot)
        if avail > 0:
            slots.append((s, avail))
        cur += delta
    slots.sort(key=lambda x: (-x[1], x[0]))
    return slots[:top_n]


def get_available_slots(event, records, preferred_master=None) -> list:
    config = EVENTS_CONFIG[event]
    free = []
    if event == "нутрициолог":
        booked = len([r for r in records if str(r.get("Время", "")) == "15:00"])
        rem = config["capacity"] - booked
        return [f"15:00 (осталось {plural_places(rem)})"] if rem > 0 else []
    start_dt = datetime.strptime(config["start"], "%H:%M")
    end_dt   = datetime.strptime(config["end"],   "%H:%M")
    delta    = timedelta(minutes=config["duration"])
    cur = start_dt
    while cur < end_dt:
        s = cur.strftime("%H:%M")
        at_slot = [r for r in records if str(r.get("Время", "")) == s]
        if event in MASTERS_CONFIG:
            avail = count_available_masters(event, s, at_slot, preferred_master)
            if avail > 0:
                free.append(f"{s} ({plural_masters(avail, event)})")
        else:
            avail = config["capacity"] - len(at_slot)
            if avail > 0:
                free.append(s)
        cur += delta
    return free


def format_slots_message(slots: list) -> str:
    if not slots:
        return "К сожалению, свободных окошек больше не осталось 😔"
    if len(slots) > 15:
        return ", ".join(slots[:15]) + " … и другие."
    return ", ".join(slots)


def build_slot_keyboard(event, suggested, preferred_master=None) -> InlineKeyboardMarkup:
    buttons = []
    for time_str, avail in suggested:
        if event in MASTERS_CONFIG:
            label = f"🕐 {time_str}  —  свободно {plural_masters(avail, event)}"
        elif event == "нутрициолог":
            label = f"🕐 {time_str}  —  осталось {plural_places(avail)}"
        else:
            label = f"🕐 {time_str}"
        cb = f"slot|{event}|{time_str}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=cb)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_services_keyboard() -> InlineKeyboardMarkup:
    """Кнопки для быстрого старта записи на каждую услугу."""
    icons = {
        "аромапсихолог": "🌸",
        "макияж": "💄",
        "нутрициолог": "🥗",
        "массаж": "💆‍♀️",
        "гадалки": "🔮",
    }
    buttons = []
    for ev_key in EVENTS_CONFIG:
        icon = icons.get(ev_key, "✨")
        buttons.append([InlineKeyboardButton(
            text=f"{icon} Записаться — {ef(ev_key)}",
            callback_data=f"start_book|{ev_key}",
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_master_display_info(event, master_id) -> str:
    if event in MASTERS_CONFIG:
        for m in MASTERS_CONFIG[event]:
            if m["id"] == master_id:
                info = m["label"]
                if m.get("location"):
                    info += f", {m['location']}"
                return info
    return master_id if master_id and master_id != "Записано" else ""


# ══════════════════════════════════════════════
#  ВСЕ ЗАПИСИ ПОЛЬЗОВАТЕЛЯ / НАЛОЖЕНИЯ
# ══════════════════════════════════════════════
def get_all_user_bookings(user_id_str: str) -> list:
    bookings = []
    for ev, cfg in EVENTS_CONFIG.items():
        ws = sheet.worksheet(cfg["sheet"])
        for row in ws.get_all_records():
            if str(row.get("ID", "")) == user_id_str:
                bookings.append({
                    "event":    ev,
                    "time":     str(row.get("Время", "")),
                    "duration": cfg["duration"],
                    "master":   str(row.get("Мастер/Детали", "")),
                })
    return bookings


def check_time_conflict(new_event, new_time_str, user_bookings):
    ns = datetime.strptime(new_time_str, "%H:%M")
    ne = ns + timedelta(minutes=EVENTS_CONFIG[new_event]["duration"])
    for b in user_bookings:
        if b["event"] == new_event:
            continue
        bs = datetime.strptime(b["time"], "%H:%M")
        be = bs + timedelta(minutes=b["duration"])
        if ns < be and ne > bs:
            return True, b["event"], b["time"]
    return False, None, None


# ══════════════════════════════════════════════
#  НАПОМИНАНИЕ
# ══════════════════════════════════════════════
async def send_reminder(user_id, event_name, time_str):
    await bot.send_message(
        user_id,
        f"✨ **Напоминалочка!**\n"
        f"Запись {ef(event_name.lower(), 'to')} начнётся через 3 минутки (в {time_str}). Ждём вас! 💖",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════
#  ЯДРО ЗАПИСИ
# ══════════════════════════════════════════════
async def execute_booking(
    user_id: int, username: str, full_name: str,
    event: str, time_str: str,
    preferred_master: str = None,
    is_reschedule: bool = False,
) -> dict:
    config    = EVENTS_CONFIG[event]
    worksheet = sheet.worksheet(config["sheet"])
    uid       = str(user_id)
    job_id    = f"{uid}_{event}"

    try:
        datetime.strptime(time_str, "%H:%M")
    except ValueError:
        return {"ok": False, "text": "Кажется, я не поняла время. Напишите в формате ЧЧ:ММ (например, 15:30) 🕒"}

    valid, err = is_valid_slot_time(event, time_str)
    if not valid:
        return {"ok": False, "text": err}

    async with get_lock(event):
        records = worksheet.get_all_records()
        row_idx = get_user_row_index(worksheet, uid)

        if is_reschedule:
            if not row_idx:
                return {"ok": False, "text":
                    f"У вас ещё нет записи {ef(event, 'to')}. "
                    f"Напишите «Запиши {ef(event, 'to')} в {time_str}» ✨"}
            records = [r for r in records if str(r.get("ID", "")) != uid]
        elif row_idx:
            bt = next((r.get("Время", "") for r in records if str(r.get("ID", "")) == uid), "?")
            return {"ok": False, "text":
                f"❌ Вы уже записаны {ef(event, 'to')} (время: {bt}).\n"
                f"Для изменения напишите «Перенеси {ef(event, 'acc')} на …» 🔄"}

        all_b = get_all_user_bookings(uid)
        conflict, c_ev, c_t = check_time_conflict(event, time_str, all_b)
        if conflict:
            return {"ok": False, "text":
                f"Ой, накладочка! 😱\n"
                f"В {time_str} вы будете {ef(c_ev, 'at')} (запись на {c_t}).\n"
                f"Пожалуйста, выберите другое время! 🕒"}

        at_time   = [r for r in records if str(r.get("Время", "")) == time_str]
        master    = None
        master_id = ""

        if event in MASTERS_CONFIG:
            master, merr = find_available_master(event, time_str, at_time, preferred_master)
            if merr:
                alts = get_available_slots(event, records, preferred_master)
                return {"ok": False, "text":
                    f"{merr}\n\n💡 **Свободные окошки:**\n{format_slots_message(alts)}"}
            if not master:
                alts = get_available_slots(event, records)
                return {"ok": False, "text":
                    f"На {time_str} все специалисты заняты 😔\n\n"
                    f"💡 **Свободные окошки:**\n{format_slots_message(alts)}"}
            master_id = master["id"]
        elif len(at_time) >= config["capacity"]:
            alts = get_available_slots(event, records)
            return {"ok": False, "text":
                f"На {time_str} ({ef(event)}) уже всё занято 😔\n\n"
                f"💡 **Свободные окошки:**\n{format_slots_message(alts)}"}

        if is_reschedule:
            worksheet.delete_rows(row_idx)
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)

        worksheet.append_row([user_id, username, full_name, time_str, master_id or "Записано"])

    try:
        now  = datetime.now()
        ev_t = datetime.strptime(time_str, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        rem  = ev_t - timedelta(minutes=3)
        if rem > now:
            scheduler.add_job(
                send_reminder, "date", run_date=rem,
                args=[user_id, event, time_str],
                id=job_id, replace_existing=True,
            )
    except Exception as e:
        logging.error(f"Ошибка напоминания: {e}")

    if is_reschedule:
        msg = f"🔄 Перенесли вашу запись {ef(event, 'to')}. Ждём вас в **{time_str}**!"
    else:
        msg = f"🎉 Вы успешно записаны {ef(event, 'to')} в **{time_str}**!"

    if master:
        if event == "гадалки":
            msg += f"\n🔮 Вас примет: **{master['label']}**"
        elif event == "массаж":
            msg += f"\n💆‍♀️ Ваш мастер: **{master['label']}**"
        elif event == "макияж":
            msg += f"\n💄 Ваш визажист: **{master['label']}**"
        else:
            msg += f"\n👩‍⚕️ Специалист: **{master['label']}**"
        if master.get("location"):
            msg += f"\n📍 {master['location']}"

    if event == "нутрициолог":
        msg += "\n📍 Зал совещаний, 5 этаж 🥗"

    return {"ok": True, "text": msg}


# ══════════════════════════════════════════════
#  ОБРАБОТЧИК СООБЩЕНИЙ
# ══════════════════════════════════════════════
@dp.message()
async def handle_booking(message: types.Message, state: FSMContext):
    # ── Текст приветствия (без кнопок — для fallback) ──
    welcome_text = (
        "Привет, красавицы! 👋 Я ваш заботливый бот-помощник.\n"
        "Пишите мне свободно, например:\n"
        "✨ *«Запиши на массаж в 12:20»*\n"
        "🔮 *«Запиши к гадалке Юлии на 15:00»*\n"
        "💆 *«Хочу к Виктору на массаж»*\n"
        "🔄 *«Перенеси макияж на 11:30»*\n"
        "❌ *«Отмени массаж»*\n"
        "📅 *«Какие окошки у Натэллы?»*\n"
        "📋 *«Куда я записана?»*\n"
        "ℹ️ *«Расскажи про услуги»* — подробности об активностях\n"
    )

    intent        = await parse_intent(message.text)
    current_state = await state.get_state()
    preferred_master = None

    # ── РЕЖИМ ОЖИДАНИЯ ВРЕМЕНИ ──
    if (not intent or not intent.get("action")) and current_state == BookingState.waiting_for_time.state:
        match = re.search(r"(\d{1,2})[.,:\s-]+(\d{2})", message.text)
        if match:
            h, m = match.groups()
            time_str = f"{int(h):02d}:{m}"
            data   = await state.get_data()
            action = data.get("action")
            event  = data.get("event")
            preferred_master = data.get("preferred_master")
            await state.clear()
        else:
            if message.text.lower().strip() in ("отмена", "отмени", "cancel", "нет"):
                await state.clear()
                await message.reply("Действие отменено 😊")
            else:
                await message.reply(
                    "Не могу распознать время 🤔\n"
                    "Напишите в формате ЧЧ:ММ (например, 15:30) или нажмите кнопку выше.\n"
                    "Для отмены напишите «Отмена»."
                )
            return

    # ── ОБЫЧНЫЙ РАЗБОР ──
    else:
        if not intent or not intent.get("action"):
            # Не распознали — показываем приветствие + кнопки услуг
            kb = build_services_keyboard()
            await message.reply(
                welcome_text + "\n**Или выберите услугу для записи:**",
                reply_markup=kb, parse_mode="Markdown",
            )
            await state.clear()
            return

        action           = intent["action"]
        event            = EVENT_ALIASES.get((intent.get("event") or "").lower(), (intent.get("event") or "").lower())
        time_str         = intent.get("time")
        preferred_master = intent.get("preferred_master")
        await state.clear()

    user_id_str = str(message.from_user.id)
    username    = f"@{message.from_user.username}" if message.from_user.username else "-"

    # ── МОИ ЗАПИСИ ──
    if action == "my_bookings":
        wait = await message.reply("⏳ Ищу ваши бьюти-планы…")
        bookings = get_all_user_bookings(user_id_str)
        if not bookings:
            await wait.edit_text("У вас пока нет ни одной записи. Давайте это исправим! ✨")
            return
        txt = "📋 **Ваши планы на сегодня:**\n\n"
        for b in bookings:
            line = f"🔸 **{ef(b['event'])}** — в {b['time']}"
            mi = get_master_display_info(b["event"], b.get("master", ""))
            if mi:
                line += f" ({mi})"
            if b["event"] == "нутрициолог":
                line += " 📍 Зал совещаний, 5 этаж"
            txt += line + "\n"
        await wait.edit_text(txt, parse_mode="Markdown")
        return

    # ── ОТМЕНИТЬ ВСЁ ──
    if action == "cancel" and not event:
        bookings = get_all_user_bookings(user_id_str)
        if not bookings:
            await message.reply("У вас нет записей, отменять нечего 😊")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, отменить всё", callback_data="confirm_cancel_all")],
            [InlineKeyboardButton(text="❌ Нет, я передумала", callback_data="abort_cancel_all")],
        ])
        await message.reply(
            f"Вы точно хотите отменить **все** записи ({len(bookings)} шт.)? 😱",
            reply_markup=kb, parse_mode="Markdown",
        )
        return

    # ══════════════════════════════════════════
    #  ИНФОРМАЦИЯ ОБ УСЛУГАХ (info)
    # ══════════════════════════════════════════
    if action == "info":
        if event and event in EVENTS_CONFIG:
            # ── Инфо об одной конкретной услуге + предложение записаться ──
            cfg = EVENTS_CONFIG[event]
            text = f"ℹ️ {cfg['desc']}\n\n⏰ Время работы: **{cfg['start']} — {cfg['end']}**"

            worksheet = sheet.worksheet(cfg["sheet"])
            records   = worksheet.get_all_records()
            suggested = get_suggested_slots(event, records, preferred_master)

            if suggested:
                text += "\n\n✨ **Хотите записаться? Вот свободные окошки:**"
                kb = build_slot_keyboard(event, suggested, preferred_master)
                await state.update_data(action="book", event=event, preferred_master=preferred_master)
                await state.set_state(BookingState.waiting_for_time)
                await message.reply(
                    text + "\n\nИли напишите время вручную (ЧЧ:ММ).",
                    reply_markup=kb, parse_mode="Markdown",
                )
            else:
                text += f"\n\nК сожалению, свободных окошек {ef(event, 'at')} не осталось 😔"
                await message.reply(text, parse_mode="Markdown")
        else:
            # ── Инфо обо ВСЕХ услугах + кнопки записи ──
            all_text = "✨ **Наши активности:**\n\n"
            all_text += "\n\n".join(cfg["desc"] for cfg in EVENTS_CONFIG.values())
            all_text += "\n\n👇 **Выберите услугу, чтобы записаться:**"
            kb = build_services_keyboard()
            await message.reply(all_text, reply_markup=kb, parse_mode="Markdown")
        return

    # ── НЕ УКАЗАНО МЕРОПРИЯТИЕ ──
    if action in ("book", "reschedule", "availability") and event not in EVENTS_CONFIG:
        # Предлагаем выбрать из кнопок
        kb = build_services_keyboard()
        await message.reply(
            "Уточните, пожалуйста, на какую услугу записаться? ✨\n\n"
            "👇 **Выберите:**",
            reply_markup=kb, parse_mode="Markdown",
        )
        return

    # ── Событие не распознано — fallback ──
    if event not in EVENTS_CONFIG:
        kb = build_services_keyboard()
        await message.reply(
            welcome_text + "\n**Или выберите услугу:**",
            reply_markup=kb, parse_mode="Markdown",
        )
        return

    worksheet = sheet.worksheet(EVENTS_CONFIG[event]["sheet"])
    records   = worksheet.get_all_records()

    # ── СВОБОДНЫЕ ОКОШКИ ──
    if action == "availability":
        free = get_available_slots(event, records, preferred_master)
        title = f"📅 **Свободные окошки {ef(event, 'at')}"
        if preferred_master:
            title += f" (у {preferred_master})"
        title += ":**"
        await message.reply(f"{title}\n{format_slots_message(free)}", parse_mode="Markdown")
        return

    # ── ОТМЕНА КОНКРЕТНОЙ ЗАПИСИ ──
    if action == "cancel":
        row_idx = get_user_row_index(worksheet, user_id_str)
        if row_idx:
            worksheet.delete_rows(row_idx)
            jid = f"{user_id_str}_{event}"
            if scheduler.get_job(jid):
                scheduler.remove_job(jid)
            await message.reply(f"🗑 Запись {ef(event, 'to')} отменена. Ждём в другой раз 🌸", parse_mode="Markdown")
        else:
            await message.reply(f"У вас нет записи {ef(event, 'to')} 😊", parse_mode="Markdown")
        return

    # ── НЕТ ВРЕМЕНИ → ПОДСКАЗКИ С КНОПКАМИ ──
    if not time_str:
        if action in ("book", "reschedule"):
            await state.update_data(action=action, event=event, preferred_master=preferred_master)
            await state.set_state(BookingState.waiting_for_time)

            suggested = get_suggested_slots(event, records, preferred_master)
            hint = f"Выберите время для записи {ef(event, 'to')}"
            if preferred_master:
                hint += f" (к {preferred_master})"
            hint += " 🕒\n\n✨ **Самые свободные окошки:**"

            if suggested:
                kb = build_slot_keyboard(event, suggested, preferred_master)
                await message.reply(hint + "\n\nИли напишите время вручную (ЧЧ:ММ).",
                                    reply_markup=kb, parse_mode="Markdown")
            else:
                await message.reply(
                    f"К сожалению, свободных окошек {ef(event, 'at')} не осталось 😔",
                    parse_mode="Markdown",
                )
                await state.clear()
            return
        await message.reply("Пожалуйста, уточните время (например, 14:20) 🕒")
        return

    # ── ВЫПОЛНЕНИЕ ЗАПИСИ ──
    result = await execute_booking(
        user_id=message.from_user.id,
        username=username,
        full_name=message.from_user.full_name,
        event=event,
        time_str=time_str,
        preferred_master=preferred_master,
        is_reschedule=(action == "reschedule"),
    )
    await message.reply(result["text"], parse_mode="Markdown")


# ══════════════════════════════════════════════
#  CALLBACK: ВЫБОР СЛОТА КНОПКОЙ
# ══════════════════════════════════════════════
@dp.callback_query(F.data.startswith("slot|"))
async def process_slot_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    parts    = callback.data.split("|")
    event    = parts[1]
    time_str = parts[2]

    data             = await state.get_data()
    action           = data.get("action", "book")
    preferred_master = data.get("preferred_master")
    await state.clear()

    user     = callback.from_user
    username = f"@{user.username}" if user.username else "-"

    await callback.message.edit_text(f"⏳ Записываю вас {ef(event, 'to')} на {time_str}…")

    result = await execute_booking(
        user_id=user.id,
        username=username,
        full_name=user.full_name,
        event=event,
        time_str=time_str,
        preferred_master=preferred_master,
        is_reschedule=(action == "reschedule"),
    )
    await callback.message.edit_text(result["text"], parse_mode="Markdown")


# ══════════════════════════════════════════════
#  CALLBACK: БЫСТРЫЙ СТАРТ ЗАПИСИ НА УСЛУГУ
# ══════════════════════════════════════════════
@dp.callback_query(F.data.startswith("start_book|"))
async def process_start_book(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    event = callback.data.split("|")[1]

    if event not in EVENTS_CONFIG:
        await callback.message.edit_text("Услуга не найдена 😔")
        return

    cfg       = EVENTS_CONFIG[event]
    worksheet = sheet.worksheet(cfg["sheet"])
    records   = worksheet.get_all_records()
    suggested = get_suggested_slots(event, records)

    if suggested:
        hint = (
            f"Выберите время для записи {ef(event, 'to')} 🕒\n\n"
            f"✨ **Самые свободные окошки:**"
        )
        kb = build_slot_keyboard(event, suggested)
        await state.update_data(action="book", event=event, preferred_master=None)
        await state.set_state(BookingState.waiting_for_time)
        await callback.message.edit_text(
            hint + "\n\nИли напишите время вручную (ЧЧ:ММ).",
            reply_markup=kb, parse_mode="Markdown",
        )
    else:
        await callback.message.edit_text(
            f"К сожалению, свободных окошек {ef(event, 'at')} не осталось 😔",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════
#  CALLBACK: ОТМЕНА ВСЕХ ЗАПИСЕЙ
# ══════════════════════════════════════════════
@dp.callback_query(F.data == "confirm_cancel_all")
async def process_confirm_cancel_all(callback: types.CallbackQuery):
    await callback.answer()
    uid = str(callback.from_user.id)
    await callback.message.edit_text("⏳ Удаляю записи…")

    bookings = get_all_user_bookings(uid)
    if not bookings:
        await callback.message.edit_text("Записей уже нет, отменять нечего 😊")
        return

    for b in bookings:
        ws = sheet.worksheet(EVENTS_CONFIG[b["event"]]["sheet"])
        ri = get_user_row_index(ws, uid)
        if ri:
            ws.delete_rows(ri)
        jid = f"{uid}_{b['event']}"
        if scheduler.get_job(jid):
            scheduler.remove_job(jid)

    await callback.message.edit_text("🗑 Все записи отменены! Будем рады видеть вас снова 🌸")


@dp.callback_query(F.data == "abort_cancel_all")
async def process_abort_cancel_all(callback: types.CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Фух! Оставили всё как есть. Ждём вас! 🥰")


# ══════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════
async def main():
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())