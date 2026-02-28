import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


# ══════════════════════════════════════════════
#  FSM
# ══════════════════════════════════════════════
class BookingState(StatesGroup):
    waiting_for_time = State()


# ══════════════════════════════════════════════
#  НАСТРОЙКИ / ПОДКЛЮЧЕНИЯ
# ══════════════════════════════════════════════
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "google_creds.json")

if not GOOGLE_SHEET_URL:
    raise ValueError("Переменная GOOGLE_SHEET_URL не найдена!")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()
llm_client = AsyncOpenAI(
    base_url="https://openai.api.proxyapi.ru/v1", api_key=OPENAI_API_KEY
)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
gs_client = gspread.authorize(creds)
sheet = gs_client.open_by_url(GOOGLE_SHEET_URL)


# ══════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════
EVENTS_CONFIG = {
    "аромапсихолог": {
        "sheet": "Аромапсихолог", "duration": 10, "capacity": 1,
        "start": "14:00", "end": "17:00",
        "desc": "🌸 **Аромапсихолог**",
    },
    "макияж": {
        "sheet": "Макияж", "duration": 10, "capacity": 4,
        "start": "10:00", "end": "12:00",
        "desc": "💄 **Макияж**",
    },
    "нутрициолог": {
        "sheet": "Нутрициолог", "duration": 90, "capacity": 30,
        "start": "15:00", "end": "16:30",
        "desc": "🥗 **Нутрициолог**", "fixed_time": "15:00",
    },
    "массаж": {
        "sheet": "Массаж", "duration": 10, "capacity": 3,
        "start": "11:00", "end": "17:10",
        "desc": "💆‍♀️ **Массаж**",
    },
    "гадалки": {
        "sheet": "Гадалки", "duration": 15, "capacity": 2,
        "start": "11:00", "end": "17:00",
        "desc": "🔮 **Таро и Гадалки**",
    },
    "мастерская чехова": {
        "sheet": "Мастерская Чехова", "duration": 60, "capacity": 10,
        "start": "11:00", "end": "17:00",
        "custom_slots": ["11:00", "12:00", "14:00", "15:00", "16:00"],
        "desc": "🎨 **Мастерская Чехова**",
    },
    "семейный нутрициолог": {
        "sheet": "Семейный нутрициолог", "duration": 90, "capacity": 30,
        "start": "15:00", "end": "16:30",
        "desc": "👨‍👩‍👧 **Семейный нутрициолог**", "fixed_time": "15:00",
    },
}

MASTERS_CONFIG = {
    "массаж": [
        {"id": "Мастер №1 Виктор", "name": "Виктор",
         "label": "Мастер №1 Виктор", "location": "",
         "breaks": ["13:30", "13:40"]},
        {"id": "Мастер №2 Нарек", "name": "Нарек",
         "label": "Мастер №2 Нарек", "location": "",
         "breaks": ["13:50", "14:00"]},
        {"id": "Мастер №3 Ольга", "name": "Ольга",
         "label": "Мастер №3 Ольга", "location": "",
         "breaks": ["14:10", "14:20"]},
    ],
    "гадалки": [
        {"id": "Гадалка Юлия", "name": "Юлия",
         "label": "Гадалка Юлия", "location": "переговорка 614а",
         "breaks": []},
        {"id": "Гадалка Натэлла", "name": "Натэлла",
         "label": "Гадалка Натэлла",
         "location": "переговорка №3, 1 этаж", "breaks": []},
    ],
    "макияж": [
        {"id": f"Визажист №{i}", "name": f"Визажист №{i}",
         "label": f"Визажист №{i}", "location": "", "breaks": []}
        for i in range(1, 5)
    ],
}

EVENT_ALIASES = {
    "гадалка": "гадалки", "таро": "гадалки", "таролог": "гадалки",
    "мэйкап": "макияж", "мейкап": "макияж",
    "психолог": "аромапсихолог", "арома": "аромапсихолог",
    "нутрицеолог": "нутрициолог", "нутрициолуг": "нутрициолог",
    "мастерская": "мастерская чехова", "чехов": "мастерская чехова",
    "чехова": "мастерская чехова", "багет": "мастерская чехова",
    "картина": "мастерская чехова",
    "семейный": "семейный нутрициолог",
    "сем нутрициолог": "семейный нутрициолог",
    "семейный нутрицеолог": "семейный нутрициолог",
}

EVENT_FORMS = {
    "аромапсихолог": {
        "to": "к аромапсихологу", "at": "у аромапсихолога",
        "acc": "аромапсихолога", "title": "Аромапсихолог",
    },
    "макияж": {
        "to": "на макияж", "at": "на макияж",
        "acc": "макияж", "title": "Макияж",
    },
    "нутрициолог": {
        "to": "к нутрициологу", "at": "у нутрициолога",
        "acc": "нутрициолога", "title": "Нутрициолог",
    },
    "массаж": {
        "to": "на массаж", "at": "на массаж",
        "acc": "массаж", "title": "Массаж",
    },
    "гадалки": {
        "to": "к гадалке", "at": "у гадалок",
        "acc": "гадалок", "title": "Гадалки",
    },
    "мастерская чехова": {
        "to": "в Мастерскую Чехова", "at": "в Мастерской Чехова",
        "acc": "Мастерскую Чехова", "title": "Мастерская Чехова",
    },
    "семейный нутрициолог": {
        "to": "к семейному нутрициологу",
        "at": "у семейного нутрициолога",
        "acc": "семейного нутрициолога",
        "title": "Семейный нутрициолог",
    },
}

EVENT_ICONS = {
    "аромапсихолог": "🌸", "макияж": "💄", "нутрициолог": "🥗",
    "массаж": "💆‍♀️", "гадалки": "🔮", "мастерская чехова": "🎨",
    "семейный нутрициолог": "👨‍👩‍👧",
}

WELCOME_TEXT = (
    "Привет, красавицы! 👋 Я ваш заботливый бот-помощник.\n"
    "Пишите мне свободно, например:\n"
    "✨ *«Запиши на массаж в 12:20»*\n"
    "🔮 *«Запиши к гадалке Юлии на 15:00»*\n"
    "🎨 *«Хочу в мастерскую Чехова на 14:00»*\n"
    "🔄 *«Перенеси макияж на 11:30»*\n"
    "❌ *«Отмени массаж»*\n"
    "📅 *«Какие окошки у Натэллы?»*\n"
    "📋 *«Моя программа»* — ваше расписание на день\n"
    "ℹ️ *«Расскажи про услуги»* — подробности\n"
)


# ══════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ТЕКСТОВЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════
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
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} {word[1]}"
    return f"{n} {word[2]}"


def plural_places(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} место"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} места"
    return f"{n} мест"


# ══════════════════════════════════════════════
#  СУПЕР-КЭШ (IN-MEMORY STATE)
# ══════════════════════════════════════════════
_booking_locks: dict[str, asyncio.Lock] = {}
_sheet_cache: dict[str, list] = {}


def get_lock(event: str) -> asyncio.Lock:
    if event not in _booking_locks:
        _booking_locks[event] = asyncio.Lock()
    return _booking_locks[event]


def _fetch_all_sheets_sync() -> dict:
    data = {}
    for ev, cfg in EVENTS_CONFIG.items():
        data[ev] = sheet.worksheet(cfg["sheet"]).get_all_records()
    return data


async def sync_cache_with_google():
    global _sheet_cache
    logging.info("Скачиваю данные из Google Sheets...")
    _sheet_cache = await asyncio.to_thread(_fetch_all_sheets_sync)
    logging.info("Данные успешно загружены в память!")


async def background_sync():
    global _sheet_cache
    try:
        _sheet_cache = await asyncio.to_thread(_fetch_all_sheets_sync)
    except Exception as e:
        logging.error(f"Фоновая синхронизация не удалась: {e}")


# ══════════════════════════════════════════════
#  NLP: АНАЛИЗ ТЕКСТА
# ══════════════════════════════════════════════
async def parse_intent(text: str) -> dict | None:
    prompt = (
        "Ты заботливый бот-ассистент для записи девушек на корпоративные мероприятия.\n"
        "Доступные мероприятия: аромапсихолог, макияж, нутрициолог, массаж, "
        "гадалки, мастерская чехова, семейный нутрициолог\n"
        "Определи action: book, cancel, reschedule, availability, info, my_bookings.\n"
        "Если просто название (массаж) -> book.\n"
        'Ответь JSON: {"action":"...","event":"...","time":"HH:MM","preferred_master":"..."}\n'
        f"Текст: {text}"
    )
    try:
        response = await llm_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        raw = response.choices[0].message.content
        # Извлекаем JSON даже если LLM обернул его в markdown
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group())
        return json.loads(raw)
    except Exception:
        return None


# ══════════════════════════════════════════════
#  ЛОГИКА ВАЛИДАЦИИ
# ══════════════════════════════════════════════
def find_available_master(event, time_str, bookings_at_time, preferred_name=None):
    if event not in MASTERS_CONFIG:
        return None, None
    masters = MASTERS_CONFIG[event]
    busy_ids = [str(r.get("Мастер/Детали", "")) for r in bookings_at_time]

    if preferred_name:
        pn = preferred_name.lower().strip()
        matched = next(
            (m for m in masters
             if pn in m["name"].lower() or pn in m["label"].lower()),
            None,
        )
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
    busy_ids = [str(r.get("Мастер/Детали", "")) for r in bookings_at_time]
    count = 0
    for m in MASTERS_CONFIG[event]:
        if time_str in m.get("breaks", []) or m["id"] in busy_ids:
            continue
        if preferred_name:
            pn = preferred_name.lower().strip()
            if pn not in m["name"].lower() and pn not in m["label"].lower():
                continue
        count += 1
    return count


def get_slot_list(event: str) -> list[str]:
    cfg = EVENTS_CONFIG[event]
    if "fixed_time" in cfg:
        return [cfg["fixed_time"]]
    if "custom_slots" in cfg:
        return list(cfg["custom_slots"])
    start_dt = datetime.strptime(cfg["start"], "%H:%M")
    end_dt = datetime.strptime(cfg["end"], "%H:%M")
    delta = timedelta(minutes=cfg["duration"])
    slots, cur = [], start_dt
    while cur < end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += delta
    return slots


def is_valid_slot_time(event: str, time_str: str) -> tuple[bool, str | None]:
    cfg = EVENTS_CONFIG[event]
    valid_slots = get_slot_list(event)

    if time_str in valid_slots:
        return True, None

    if "fixed_time" in cfg:
        return False, f"**{ef(event)}** начинается строго в **{cfg['fixed_time']}** 🕒"
    if "custom_slots" in cfg:
        return False, f"⏰ Доступные сеансы: **{', '.join(valid_slots)}**"

    start_dt = datetime.strptime(cfg["start"], "%H:%M")
    end_dt = datetime.strptime(cfg["end"], "%H:%M")
    req_dt = datetime.strptime(time_str, "%H:%M")

    if req_dt < start_dt or req_dt >= end_dt:
        return False, f"⏰ Рабочие часы: {cfg['start']} до {cfg['end']}."

    dur = cfg["duration"]
    mins = int((req_dt - start_dt).total_seconds() / 60)
    if mins % dur != 0:
        prev = start_dt + timedelta(minutes=(mins // dur) * dur)
        nxt = prev + timedelta(minutes=dur)
        opts = [
            t.strftime("%H:%M")
            for t in (prev, nxt)
            if start_dt <= t < end_dt
        ]
        return False, f"Ближайшие слоты: **{', '.join(opts)}** 🕒"
    return True, None


# ══════════════════════════════════════════════
#  UI И КЛАВИАТУРЫ
# ══════════════════════════════════════════════
def get_suggested_slots(event, records, preferred_master=None, top_n=6) -> list[tuple[str, int]]:
    cfg = EVENTS_CONFIG[event]
    slots = []
    for s in get_slot_list(event):
        at_slot = [r for r in records if str(r.get("Время", "")) == s]
        if event in MASTERS_CONFIG:
            avail = count_available_masters(event, s, at_slot, preferred_master)
        else:
            avail = cfg["capacity"] - len(at_slot)
        if avail > 0:
            slots.append((s, avail))
    slots.sort(key=lambda x: (-x[1], x[0]))
    return slots[:top_n]


def get_available_slots(event, records, preferred_master=None) -> list[str]:
    cfg = EVENTS_CONFIG[event]
    free = []
    for s in get_slot_list(event):
        at_slot = [r for r in records if str(r.get("Время", "")) == s]
        if event in MASTERS_CONFIG:
            avail = count_available_masters(event, s, at_slot, preferred_master)
        else:
            avail = cfg["capacity"] - len(at_slot)
        if avail > 0:
            label = (
                plural_masters(avail, event)
                if event in MASTERS_CONFIG
                else "осталось " + plural_places(avail)
            )
            free.append(f"{s} ({label})")
    return free


def format_slots_message(slots: list[str]) -> str:
    if not slots:
        return "К сожалению, свободных окошек больше не осталось 😔"
    text = ", ".join(slots[:15])
    if len(slots) > 15:
        text += " … и другие."
    return text


def _slot_button_label(event: str, time_str: str, avail: int) -> str:
    if event in MASTERS_CONFIG:
        return f"🕐 {time_str} — свободно {plural_masters(avail, event)}"
    return f"🕐 {time_str} — осталось {plural_places(avail)}"


def build_slot_keyboard(
    event: str,
    suggested: list[tuple[str, int]],
    action: str = "book",
) -> InlineKeyboardMarkup:
    """Кнопки слотов. action кодируется в callback_data → не зависим от FSM."""
    buttons = [
        [InlineKeyboardButton(
            text=_slot_button_label(event, t, a),
            callback_data=f"slot|{event}|{t}|{action}",
        )]
        for t, a in suggested
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_services_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            text=f"{EVENT_ICONS.get(ev, '✨')} Записаться — {ef(ev)}",
            callback_data=f"start_book|{ev}",
        )]
        for ev in EVENTS_CONFIG
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ══════════════════════════════════════════════
#  ПРОГРАММА И КОНФЛИКТЫ
# ══════════════════════════════════════════════
def get_all_user_bookings(user_id_str: str) -> list[dict]:
    bookings = []
    for ev, cfg in EVENTS_CONFIG.items():
        for row in _sheet_cache.get(ev, []):
            if str(row.get("ID", "")) == user_id_str:
                bookings.append({
                    "event": ev,
                    "time": str(row.get("Время", "")),
                    "duration": cfg["duration"],
                    "master": str(row.get("Мастер/Детали", "")),
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


def build_program_message(user_id_str: str) -> str | None:
    bookings = get_all_user_bookings(user_id_str)
    if not bookings:
        return None
    bookings.sort(key=lambda b: b["time"])

    lines = []
    for i, b in enumerate(bookings):
        is_last = i == len(bookings) - 1
        end_time = (
            datetime.strptime(b["time"], "%H:%M")
            + timedelta(minutes=b["duration"])
        ).strftime("%H:%M")
        icon = EVENT_ICONS.get(b["event"], "✨")
        prefix = "└" if is_last else "├"
        line = f"{prefix} **{b['time']} – {end_time}**  {icon} {ef(b['event'])}"

        details = []
        if b["event"] in MASTERS_CONFIG:
            for m in MASTERS_CONFIG[b["event"]]:
                if m["id"] == b.get("master"):
                    loc = f", {m['location']}" if m.get("location") else ""
                    details.append(m["label"] + loc)
        if b["event"] in ("нутрициолог", "семейный нутрициолог"):
            details.append("📍 Зал совещаний, 5 этаж")

        if details:
            indent = "   " if is_last else "│  "
            line += f"\n{indent}↳ _{', '.join(details)}_"
        lines.append(line)

    total = len(bookings)
    return f"📋 **Ваша бьюти-программа** ({total}/{len(EVENTS_CONFIG)}):\n\n" + "\n".join(lines)


async def send_program(chat_id: int, user_id_str: str):
    """Отправляет программу + кнопки для дальнейшей записи."""
    text = build_program_message(user_id_str)
    if text:
        remaining = [
            ev for ev in EVENTS_CONFIG
            if not any(
                str(r.get("ID", "")) == user_id_str
                for r in _sheet_cache.get(ev, [])
            )
        ]
        kb = None
        if remaining:
            buttons = [
                [InlineKeyboardButton(
                    text=f"{EVENT_ICONS.get(ev, '✨')} Записаться — {ef(ev)}",
                    callback_data=f"start_book|{ev}",
                )]
                for ev in remaining
            ]
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await bot.send_message(
            chat_id,
            text + ("\n\n✨ **Записаться ещё:**" if remaining else ""),
            reply_markup=kb,
            parse_mode="Markdown",
        )


async def send_reminder(user_id, event_name, time_str):
    await bot.send_message(
        user_id,
        f"✨ **Напоминалочка!**\n"
        f"Запись {ef(event_name.lower(), 'to')} начнётся через 3 минутки "
        f"(в {time_str}). Ждём вас! 💖",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════
#  ЯДРО ЗАПИСИ
# ══════════════════════════════════════════════
async def execute_booking(
    user_id: int,
    username: str,
    full_name: str,
    event: str,
    time_str: str,
    preferred_master: str = None,
    is_reschedule: bool = False,
) -> dict:
    cfg = EVENTS_CONFIG[event]
    uid = str(user_id)

    try:
        datetime.strptime(time_str, "%H:%M")
    except ValueError:
        return {"ok": False, "text": "Неверный формат времени 🕒"}

    valid, err = is_valid_slot_time(event, time_str)
    if not valid:
        return {"ok": False, "text": err}

    async with get_lock(event):
        records = _sheet_cache.get(event, [])
        user_row_exists = any(str(r.get("ID", "")) == uid for r in records)

        if is_reschedule:
            if not user_row_exists:
                return {"ok": False, "text": f"У вас нет записи {ef(event, 'to')}."}
        elif user_row_exists:
            bt = next(
                (r.get("Время", "") for r in records if str(r.get("ID", "")) == uid),
                "?",
            )
            return {
                "ok": False,
                "text": f"❌ Вы уже записаны {ef(event, 'to')} (время: {bt}).",
            }

        conflict, c_ev, c_t = check_time_conflict(
            event, time_str, get_all_user_bookings(uid)
        )
        if conflict:
            return {
                "ok": False,
                "text": f"Ой, накладочка! В {time_str} вы будете {ef(c_ev, 'at')}.",
            }

        at_time = [r for r in records if str(r.get("Время", "")) == time_str]
        master = None
        master_id = ""

        if event in MASTERS_CONFIG:
            master, merr = find_available_master(
                event, time_str, at_time, preferred_master
            )
            if not master:
                avail_text = format_slots_message(
                    get_available_slots(event, records)
                )
                return {
                    "ok": False,
                    "text": merr or f"На {time_str} все заняты 😔\n💡 Свободные: {avail_text}",
                }
            master_id = master["id"]
        elif len(at_time) >= cfg["capacity"]:
            avail_text = format_slots_message(get_available_slots(event, records))
            return {
                "ok": False,
                "text": f"На {time_str} всё занято 😔\n💡 Свободные: {avail_text}",
            }

        # Запись в Google Sheets
        ws = sheet.worksheet(cfg["sheet"])

        if is_reschedule:
            def delete_row_sync():
                ids = ws.col_values(1)
                uid_candidates = [str(v) for v in ids]
                if uid in uid_candidates:
                    ws.delete_rows(uid_candidates.index(uid) + 1)

            await asyncio.to_thread(delete_row_sync)
            _sheet_cache[event] = [
                r for r in _sheet_cache[event] if str(r.get("ID", "")) != uid
            ]

        new_record = {
            "ID": user_id,
            "Username": username,
            "ФИО": full_name,
            "Время": time_str,
            "Мастер/Детали": master_id or "Записано",
        }
        await asyncio.to_thread(
            ws.append_row,
            [user_id, username, full_name, time_str, master_id or "Записано"],
        )
        _sheet_cache[event].append(new_record)

    # Напоминание
    now = datetime.now()
    ev_t = datetime.strptime(time_str, "%H:%M").replace(
        year=now.year, month=now.month, day=now.day
    )
    rem = ev_t - timedelta(minutes=3)
    if rem > now:
        scheduler.add_job(
            send_reminder, "date", run_date=rem,
            args=[user_id, event, time_str],
            id=f"{uid}_{event}", replace_existing=True,
        )

    msg = f"🎉 Вы успешно записаны {ef(event, 'to')} в **{time_str}**!"
    if master:
        msg += f"\nСпециалист: **{master['label']}**"
        if master.get("location"):
            msg += f"\n📍 {master['location']}"
    return {"ok": True, "text": msg}


# ══════════════════════════════════════════════
#  ОБРАБОТЧИКИ СООБЩЕНИЙ
# ══════════════════════════════════════════════

def _resolve_event(raw: str | None) -> str | None:
    """Нормализует название мероприятия."""
    if not raw:
        return None
    key = raw.lower().strip()
    key = EVENT_ALIASES.get(key, key)
    return key if key in EVENTS_CONFIG else None


@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.reply(
        WELCOME_TEXT + "\n**Или выберите услугу для записи:**",
        reply_markup=build_services_keyboard(),
        parse_mode="Markdown",
    )


@dp.message()
async def handle_booking(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    intent = await parse_intent(message.text)

    # ── Если мы в режиме ожидания времени и NLP не распознал осмысленный интент ──
    if current_state == BookingState.waiting_for_time.state:
        has_meaningful_intent = (
            intent
            and intent.get("action")
            and intent.get("action") not in ("book", "reschedule")
        )
        # Если NLP вернул cancel / my_bookings / info — обрабатываем как новый интент
        # Иначе пытаемся извлечь время из текста
        if not has_meaningful_intent:
            data = await state.get_data()
            # Попытка извлечь время из текста
            match = re.search(r"(\d{1,2})[.,:\s-]+(\d{2})", message.text)
            # Также проверяем, вдруг NLP вернул time
            nlp_time = intent.get("time") if intent else None

            if match:
                h, m = match.groups()
                time_str = f"{int(h):02d}:{m}"
            elif nlp_time:
                time_str = nlp_time
            elif message.text.lower().strip() in ("отмена", "отмени", "назад"):
                await state.clear()
                return await message.reply("Действие отменено 😊")
            else:
                return await message.reply(
                    "Не могу распознать время 🤔 Напишите в формате ЧЧ:ММ или нажмите кнопку выше."
                )

            event = data.get("event")
            action = data.get("action", "book")
            preferred_master = data.get("preferred_master")
            await state.clear()

            if not event or event not in EVENTS_CONFIG:
                return await message.reply(
                    "Что-то пошло не так. Выберите услугу заново:",
                    reply_markup=build_services_keyboard(),
                )

            res = await execute_booking(
                message.from_user.id,
                f"@{message.from_user.username}",
                message.from_user.full_name,
                event, time_str, preferred_master,
                is_reschedule=(action == "reschedule"),
            )
            await message.reply(res["text"], parse_mode="Markdown")
            if res["ok"]:
                await send_program(message.chat.id, str(message.from_user.id))
            return

    # ── Стандартная обработка через NLP ──
    await state.clear()

    if not intent or not intent.get("action"):
        return await message.reply(
            WELCOME_TEXT + "\n**Или выберите услугу для записи:**",
            reply_markup=build_services_keyboard(),
            parse_mode="Markdown",
        )

    action = intent["action"]
    raw_event = (intent.get("event") or "").lower().strip()
    event = EVENT_ALIASES.get(raw_event, raw_event)
    time_str = intent.get("time")
    preferred_master = intent.get("preferred_master")
    uid = str(message.from_user.id)

    # ── Мои записи ──
    if action == "my_bookings":
        text = build_program_message(uid)
        if text:
            await message.reply(text, parse_mode="Markdown")
        else:
            await message.reply(
                "У вас пока нет записей!",
                reply_markup=build_services_keyboard(),
            )
        return

    # ── Инфо ──
    if action == "info":
        if event in EVENTS_CONFIG:
            cfg = EVENTS_CONFIG[event]
            text = f"ℹ️ {cfg['desc']}\n⏰ {cfg['start']} — {cfg['end']}"
            suggested = get_suggested_slots(
                event, _sheet_cache.get(event, []), preferred_master
            )
            if suggested:
                await state.update_data(
                    action="book", event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    text + "\n✨ Свободные окошки:",
                    reply_markup=build_slot_keyboard(event, suggested, "book"),
                )
            return await message.reply(text + "\nК сожалению, мест нет 😔")
        return await message.reply(
            "Выберите услугу:", reply_markup=build_services_keyboard()
        )

    # ── Проверяем, что мероприятие известно ──
    if event not in EVENTS_CONFIG:
        text = (
            "Уточните, пожалуйста, на какую услугу записаться? ✨\n\n👇 **Выберите:**"
            if action in ("book", "reschedule", "cancel", "availability")
            else WELCOME_TEXT + "\n**Или выберите услугу:**"
        )
        return await message.reply(
            text, reply_markup=build_services_keyboard(), parse_mode="Markdown"
        )

    # ── Отмена ──
    if action == "cancel":
        async with get_lock(event):
            records = _sheet_cache.get(event, [])
            if any(str(r.get("ID", "")) == uid for r in records):
                def delete_sync():
                    ws = sheet.worksheet(EVENTS_CONFIG[event]["sheet"])
                    ids = [str(v) for v in ws.col_values(1)]
                    if uid in ids:
                        ws.delete_rows(ids.index(uid) + 1)

                await asyncio.to_thread(delete_sync)
                _sheet_cache[event] = [
                    r for r in _sheet_cache[event]
                    if str(r.get("ID", "")) != uid
                ]
                job_id = f"{uid}_{event}"
                if scheduler.get_job(job_id):
                    scheduler.remove_job(job_id)
                await message.reply(f"🗑 Запись {ef(event, 'to')} отменена.")
                await send_program(message.chat.id, uid)
            else:
                await message.reply(f"У вас нет записи {ef(event, 'to')} 😊")
        return

    # ── Наличие мест ──
    if action == "availability":
        records = _sheet_cache.get(event, [])
        avail_list = get_available_slots(event, records, preferred_master)
        if avail_list:
            text = f"📅 Свободные окошки {ef(event, 'at')}:\n{format_slots_message(avail_list)}"
            suggested = get_suggested_slots(event, records, preferred_master)
            if suggested:
                await state.update_data(
                    action="book", event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    text + "\n\n✨ Хотите записаться?",
                    reply_markup=build_slot_keyboard(event, suggested, "book"),
                )
            return await message.reply(text)
        return await message.reply(
            f"Нет свободных окошек {ef(event, 'at')} 😔"
        )

    # ── Бронирование / перенос без указанного времени ──
    if not time_str:
        # Для мероприятий с fixed_time — бронируем сразу
        cfg = EVENTS_CONFIG[event]
        if "fixed_time" in cfg:
            time_str = cfg["fixed_time"]
        else:
            records = _sheet_cache.get(event, [])
            suggested = get_suggested_slots(event, records, preferred_master)
            if suggested:
                await state.update_data(
                    action=action, event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    f"Выберите время {ef(event, 'to')} 🕒",
                    reply_markup=build_slot_keyboard(event, suggested, action),
                )
            return await message.reply(
                f"Нет свободных окошек {ef(event, 'at')} 😔"
            )

    # ── Выполняем бронирование ──
    res = await execute_booking(
        message.from_user.id,
        f"@{message.from_user.username}",
        message.from_user.full_name,
        event, time_str, preferred_master,
        is_reschedule=(action == "reschedule"),
    )
    await message.reply(res["text"], parse_mode="Markdown")
    if res["ok"]:
        await send_program(message.chat.id, uid)


# ══════════════════════════════════════════════
#  CALLBACK-ОБРАБОТЧИКИ
# ══════════════════════════════════════════════

@dp.callback_query(F.data.startswith("slot|"))
async def process_slot(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("|")
    # slot|event|time|action
    event = parts[1]
    time_str = parts[2]
    action = parts[3] if len(parts) > 3 else "book"
    data = await state.get_data()
    await state.clear()

    try:
        await callback.message.edit_text(
            f"⏳ Записываю {ef(event, 'to')} на {time_str}…"
        )
        res = await execute_booking(
            callback.from_user.id,
            f"@{callback.from_user.username}",
            callback.from_user.full_name,
            event, time_str,
            preferred_master=data.get("preferred_master"),
            is_reschedule=(action == "reschedule"),
        )
        await callback.message.edit_text(res["text"], parse_mode="Markdown")
        if res["ok"]:
            await send_program(
                callback.message.chat.id, str(callback.from_user.id)
            )
    except Exception as e:
        logging.exception("Ошибка при обработке слота")
        await callback.message.edit_text(
            "Произошла ошибка при записи 😔 Попробуйте ещё раз.",
        )


@dp.callback_query(F.data.startswith("start_book|"))
async def process_start_book(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()  # FIX: чистим предыдущий стейт
    event = callback.data.split("|")[1]

    if event not in EVENTS_CONFIG:
        return await callback.message.edit_text("Услуга не найдена 😔")

    cfg = EVENTS_CONFIG[event]

    # FIX: для fixed_time — бронируем сразу, без показа кнопок
    if "fixed_time" in cfg:
        time_str = cfg["fixed_time"]
        records = _sheet_cache.get(event, [])
        at_time = [r for r in records if str(r.get("Время", "")) == time_str]
        avail = cfg["capacity"] - len(at_time)
        if avail <= 0:
            return await callback.message.edit_text(
                f"К сожалению, мест {ef(event, 'at')} больше нет 😔"
            )

        await callback.message.edit_text(
            f"⏳ Записываю {ef(event, 'to')} на {time_str}…"
        )
        try:
            res = await execute_booking(
                callback.from_user.id,
                f"@{callback.from_user.username}",
                callback.from_user.full_name,
                event, time_str,
            )
            await callback.message.edit_text(res["text"], parse_mode="Markdown")
            if res["ok"]:
                await send_program(
                    callback.message.chat.id, str(callback.from_user.id)
                )
        except Exception as e:
            logging.exception("Ошибка при бронировании fixed_time")
            await callback.message.edit_text(
                "Произошла ошибка при записи 😔 Попробуйте ещё раз."
            )
        return

    # Обычный поток — показываем слоты
    suggested = get_suggested_slots(event, _sheet_cache.get(event, []))
    if suggested:
        await state.update_data(action="book", event=event)
        await state.set_state(BookingState.waiting_for_time)
        await callback.message.edit_text(
            f"Выберите время {ef(event, 'to')} 🕒",
            reply_markup=build_slot_keyboard(event, suggested, "book"),
        )
    else:
        await callback.message.edit_text("К сожалению, мест нет 😔")


# ══════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════
async def main():
    await sync_cache_with_google()
    scheduler.add_job(background_sync, "interval", minutes=2)
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())