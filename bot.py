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
from aiohttp import web

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

SERVICE_DESCRIPTIONS = {
    "аромапсихолог": "Индивидуальная консультация с подбором аромата под ваше настроение",
    "макияж": "Профессиональный макияж от визажистов — обновите образ за 10 минут",
    "нутрициолог": "Групповая лекция о здоровом питании и полезных привычках",
    "массаж": "Расслабляющий экспресс-массаж — снимите напряжение",
    "гадалки": "Мистический сеанс Таро — загляните в будущее",
    "мастерская чехова": "Творческий мастер-класс — создайте картину в авторском багете",
    "семейный нутрициолог": "Консультация о правильном питании для всей семьи",
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
    "✨ **Добро пожаловать!** ✨\n\n"
    "Я помогу составить идеальную бьюти-программу на сегодня.\n\n"
    "💬 **Просто напишите**, например:\n"
    "  › _«Запиши на массаж в 12:20»_\n"
    "  › _«Хочу к гадалке Юлии на 15:00»_\n"
    "  › _«Перенеси макияж на 11:30»_\n"
    "  › _«Отмени массаж»_\n"
    "  › _«Отмени все»_\n"
    "  › _«Моя программа»_\n\n"
    "Или выберите услугу из списка 👇"
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
_user_locks: dict[str, asyncio.Lock] = {}
_sheet_cache: dict[str, list] = {}
_last_sync_ok: datetime | None = None
_cache_ready: bool = False

def get_lock(event: str) -> asyncio.Lock:
    if event not in _booking_locks:
        _booking_locks[event] = asyncio.Lock()
    return _booking_locks[event]


def get_user_lock(user_id: str) -> asyncio.Lock:
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


def _fetch_all_sheets_sync() -> dict:
    data = {}
    for ev, cfg in EVENTS_CONFIG.items():
        data[ev] = sheet.worksheet(cfg["sheet"]).get_all_records()
    return data


async def sync_cache_with_google():
    global _sheet_cache, _last_sync_ok, _cache_ready
    logging.info("Скачиваю данные из Google Sheets...")
    _sheet_cache = await asyncio.to_thread(_fetch_all_sheets_sync)
    _last_sync_ok = datetime.now()
    _cache_ready = True
    logging.info("Данные успешно загружены в память!")


async def background_sync():
    global _sheet_cache, _last_sync_ok
    try:
        _sheet_cache = await asyncio.to_thread(_fetch_all_sheets_sync)
        _last_sync_ok = datetime.now()
    except Exception as e:
        logging.error(f"Фоновая синхронизация не удалась: {e}")

# ══════════════════════════════════════════════
#  HEALTH CHECK SERVER
# ══════════════════════════════════════════════
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
SYNC_STALE_MINUTES = int(os.getenv("SYNC_STALE_MINUTES", "10"))

async def handle_healthz(request: web.Request) -> web.Response:
    return web.json_response({"status": "alive"}, status=200)


async def handle_readyz(request: web.Request) -> web.Response:
    errors = []
    if not _cache_ready:
        errors.append("cache not loaded yet")
    if _last_sync_ok is None:
        errors.append("no successful sync")
    elif (datetime.now() - _last_sync_ok).total_seconds() > SYNC_STALE_MINUTES * 60:
        errors.append(
            f"last sync was {_last_sync_ok.isoformat()}, "
            f"stale > {SYNC_STALE_MINUTES}m"
        )
    if errors:
        return web.json_response(
            {"status": "not ready", "errors": errors}, status=503
        )
    return web.json_response(
        {
            "status": "ready",
            "last_sync": _last_sync_ok.isoformat(),
            "cached_events": len(_sheet_cache),
        },
        status=200,
    )


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_get("/readyz", handle_readyz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    logging.info(f"Health server started on :{HEALTH_PORT}")
    return runner

# ══════════════════════════════════════════════
#  NLP: АНАЛИЗ ТЕКСТА
# ══════════════════════════════════════════════
async def parse_intent(text: str) -> dict | None:
    prompt = (                                                          # ← FIX: обновлённый промпт
        "Ты заботливый бот-ассистент для записи девушек на корпоративные мероприятия.\n"
        "Доступные мероприятия: аромапсихолог, макияж, нутрициолог, массаж, "
        "гадалки, мастерская чехова, семейный нутрициолог\n"
        "Определи action: book, cancel, cancel_all, reschedule, availability, info, my_bookings.\n"
        "Правила:\n"
        '- "отмени всё" / "отмени все" / "отменить все записи" / "удали всё" → action=cancel_all, event=""\n'
        '- "отмени массаж" → action=cancel, event="массаж"\n'
        '- "отмени" (без уточнения услуги) → action=cancel_all, event=""\n'
        "- Если просто название (массаж) -> book.\n"
        'Ответь JSON: {"action":"...","event":"...","time":"HH:MM","preferred_master":"..."}\n'
        "Если event не определён, верни пустую строку.\n"
        f"Текст: {text}"
    )
    try:
        response = await llm_client.chat.completions.create(
            model="openai/gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        raw = response.choices[0].message.content
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


def build_service_card(event: str) -> str:
    cfg = EVENTS_CONFIG[event]
    icon = EVENT_ICONS.get(event, "✨")
    title = ef(event)
    desc = SERVICE_DESCRIPTIONS.get(event, "")

    records = _sheet_cache.get(event, [])
    available = get_suggested_slots(event, records, top_n=99)
    total_slots = len(get_slot_list(event))

    lines = [f"{icon} **{title}**"]
    if desc:
        lines.append(f"_{desc}_")
    lines.append("")
    lines.append(f"🕐 {cfg['start']} — {cfg['end']}  ·  ⏱ {cfg['duration']} мин")

    if "fixed_time" in cfg:
        at_time = [r for r in records if str(r.get("Время", "")) == cfg["fixed_time"]]
        avail = cfg["capacity"] - len(at_time)
        lines.append(f"👥 Осталось {plural_places(max(avail, 0))}")
    elif available:
        lines.append(f"📊 Свободно {len(available)} из {total_slots} слотов")
    else:
        lines.append("⛔ Свободных мест нет")

    if event in MASTERS_CONFIG:
        n = len(MASTERS_CONFIG[event])
        lines.append(f"👩‍⚕️ Работают {plural_masters(n, event)}")

    return "\n".join(lines)


def build_services_keyboard(user_id: str = None) -> InlineKeyboardMarkup:
    buttons = []
    for ev in EVENTS_CONFIG:
        icon = EVENT_ICONS.get(ev, "✨")
        title = ef(ev)

        already_booked = False
        if user_id:
            records = _sheet_cache.get(ev, [])
            already_booked = any(str(r.get("ID", "")) == user_id for r in records)

        if already_booked:
            buttons.append([InlineKeyboardButton(
                text=f"✅ {title} — вы записаны",
                callback_data=f"my_booking_detail|{ev}",
            )])
        else:
            has_slots = bool(get_suggested_slots(ev, _sheet_cache.get(ev, []), top_n=1))
            if has_slots:
                buttons.append([InlineKeyboardButton(
                    text=f"{icon} {title}",
                    callback_data=f"start_book|{ev}",
                )])
            else:
                buttons.append([InlineKeyboardButton(
                    text=f"⛔ {title} — мест нет",
                    callback_data=f"no_slots|{ev}",
                )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_slot_keyboard(
    event: str,
    suggested: list[tuple[str, int]],
    action: str = "book",
) -> InlineKeyboardMarkup:
    buttons = []
    for t, a in suggested:
        if event in MASTERS_CONFIG:
            avail_text = plural_masters(a, event)
        else:
            avail_text = plural_places(a)
        buttons.append([InlineKeyboardButton(
            text=f"🕐 {t}  ·  {avail_text}",
            callback_data=f"slot|{event}|{t}|{action}",
        )])
    buttons.append([InlineKeyboardButton(
        text="← Назад к услугам",
        callback_data="back_to_services",
    )])
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

    total_events = len(EVENTS_CONFIG)
    booked_count = len(bookings)
    progress = "●" * booked_count + "○" * (total_events - booked_count)

    lines = [
        f"📋 **Ваша бьюти-программа**",
        f"  {progress}  {booked_count} из {total_events}",
        "",
    ]

    for i, b in enumerate(bookings):
        end_time = (
            datetime.strptime(b["time"], "%H:%M")
            + timedelta(minutes=b["duration"])
        ).strftime("%H:%M")
        icon = EVENT_ICONS.get(b["event"], "✨")

        line = f"  {icon}  **{b['time']} — {end_time}**  │  {ef(b['event'])}"

        details = []
        if b["event"] in MASTERS_CONFIG:
            for m in MASTERS_CONFIG[b["event"]]:
                if m["id"] == b.get("master"):
                    details.append(f"Специалист: {m['label']}")
                    if m.get("location"):
                        details.append(f"📍 {m['location']}")
        if b["event"] in ("нутрициолог", "семейный нутрициолог"):
            details.append("📍 Зал совещаний, 5 этаж")

        if details:
            for d in details:
                line += f"\n        ↳ _{d}_"
        lines.append(line)

    return "\n".join(lines)


async def send_program(chat_id: int, user_id_str: str):
    text = build_program_message(user_id_str)
    if not text:
        return

    remaining_bookable = []
    remaining_full = []
    for ev in EVENTS_CONFIG:
        already = any(
            str(r.get("ID", "")) == user_id_str
            for r in _sheet_cache.get(ev, [])
        )
        if already:
            continue
        has_slots = bool(get_suggested_slots(ev, _sheet_cache.get(ev, []), top_n=1))
        if has_slots:
            remaining_bookable.append(ev)
        else:
            remaining_full.append(ev)

    if remaining_bookable:
        buttons = [
            [InlineKeyboardButton(
                text=f"{EVENT_ICONS.get(ev, '✨')} {ef(ev)}",
                callback_data=f"start_book|{ev}",
            )]
            for ev in remaining_bookable
        ]
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        text += "\n\n✨ **Куда ещё можно записаться:**"
        await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="Markdown")
    elif not remaining_full:
        text += "\n\n🎉 **Вы записаны на все активности! Отличный день!**"
        await bot.send_message(chat_id, text, parse_mode="Markdown")
    else:
        text += "\n\n_На остальные активности мест пока нет — попробуйте позже_ 🤞"
        await bot.send_message(chat_id, text, parse_mode="Markdown")


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

    async with get_user_lock(uid):
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
                    "text": f"Вы уже записаны {ef(event, 'to')} на **{bt}** ✅\n"
                            f"_Чтобы перенести, напишите «перенеси {ef(event, 'acc')}»_",
                }

            conflict, c_ev, c_t = check_time_conflict(
                event, time_str, get_all_user_bookings(uid)
            )
            if conflict:
                return {
                    "ok": False,
                    "text": f"⚠️ Накладка: в **{time_str}** вы будете {ef(c_ev, 'at')}.\n"
                            f"_Выберите другое время_ 🕐",
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

            ws = sheet.worksheet(cfg["sheet"])

            if is_reschedule:
                def delete_row_sync():
                    ids = [str(v) for v in ws.col_values(1)]
                    if uid in ids:
                        ws.delete_rows(ids.index(uid) + 1)

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

    icon = EVENT_ICONS.get(event, "✨")
    end_time = (
        datetime.strptime(time_str, "%H:%M") + timedelta(minutes=cfg["duration"])
    ).strftime("%H:%M")

    msg_lines = [
        f"{'🔄 Перенесено' if is_reschedule else '✅ Записано'}!",
        "",
        f"  {icon}  **{ef(event)}**",
        f"  🕐  **{time_str} — {end_time}**",
    ]
    if master:
        msg_lines.append(f"  👩‍⚕️  {master['label']}")
        if master.get("location"):
            msg_lines.append(f"  📍  {master['location']}")
    if event in ("нутрициолог", "семейный нутрициолог"):
        msg_lines.append(f"  📍  Зал совещаний, 5 этаж")

    msg_lines.append("")
    msg_lines.append("_Напомню за 3 минуты до начала_ 🔔")

    return {"ok": True, "text": "\n".join(msg_lines)}


# ══════════════════════════════════════════════     ← FIX: новая функция отмены всех записей
#  ОТМЕНА ВСЕХ ЗАПИСЕЙ
# ══════════════════════════════════════════════
async def cancel_all_bookings(user_id: int) -> str:
    """Отменяет все записи пользователя. Возвращает текст ответа."""
    uid = str(user_id)
    bookings = get_all_user_bookings(uid)

    if not bookings:
        return "У вас нет активных записей 😊"

    cancelled = []
    for b in bookings:
        event = b["event"]
        async with get_lock(event):
            records = _sheet_cache.get(event, [])
            if any(str(r.get("ID", "")) == uid for r in records):
                def delete_sync(ev=event):
                    ws = sheet.worksheet(EVENTS_CONFIG[ev]["sheet"])
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
                icon = EVENT_ICONS.get(event, "✨")
                cancelled.append(f"  {icon} {ef(event)} ({b['time']})")

    if cancelled:
        lines = ["🗑 **Все записи отменены:**", ""] + cancelled
        return "\n".join(lines)
    return "У вас нет активных записей 😊"


# ══════════════════════════════════════════════     ← FIX: новая функция — клавиатура для выбора записи на отмену
#  КЛАВИАТУРА ОТМЕНЫ
# ══════════════════════════════════════════════
def build_cancel_keyboard(user_id_str: str) -> InlineKeyboardMarkup | None:
    """Строит клавиатуру с кнопками отмены для каждой записи пользователя."""
    bookings = get_all_user_bookings(user_id_str)
    if not bookings:
        return None

    buttons = []
    for b in bookings:
        icon = EVENT_ICONS.get(b["event"], "✨")
        buttons.append([InlineKeyboardButton(
            text=f"❌ {icon} {ef(b['event'])} — {b['time']}",
            callback_data=f"confirm_cancel|{b['event']}",
        )])

    if len(bookings) > 1:                                              # ← FIX: кнопка «Отменить всё»
        buttons.append([InlineKeyboardButton(
            text="🗑 Отменить все записи",
            callback_data="cancel_all_confirmed",
        )])

    buttons.append([InlineKeyboardButton(
        text="← Назад к услугам",
        callback_data="back_to_services",
    )])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ══════════════════════════════════════════════
#  ОБРАБОТЧИКИ СООБЩЕНИЙ
# ══════════════════════════════════════════════

def _resolve_event(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.lower().strip()
    key = EVENT_ALIASES.get(key, key)
    return key if key in EVENTS_CONFIG else None


@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = str(message.from_user.id)
    await message.reply(
        WELCOME_TEXT,
        reply_markup=build_services_keyboard(user_id=uid),
        parse_mode="Markdown",
    )


@dp.message()
async def handle_booking(message: types.Message, state: FSMContext):
    text_lower = message.text.lower().strip()
    uid = str(message.from_user.id)

    if text_lower in ["моя программа", "мои записи", "расписание", "программа"]:
        await state.clear()
        text = build_program_message(uid)
        if text:
            await message.reply(text, parse_mode="Markdown")
        else:
            await message.reply(
                "У вас пока нет записей 😊\n\n✨ **Выберите услугу:**",
                reply_markup=build_services_keyboard(user_id=uid),
                parse_mode="Markdown",
            )
        return

    # ── FIX: быстрая проверка на «отмени всё» без GPT ──
    cancel_all_patterns = [
        "отмени все", "отмени всё", "отменить все", "отменить всё",
        "удали все", "удали всё", "отмена всех", "отменить все записи",
        "удалить все записи", "отмени все записи",
    ]
    if text_lower in cancel_all_patterns:
        await state.clear()
        result = await cancel_all_bookings(message.from_user.id)
        await message.reply(result, parse_mode="Markdown")
        return

    current_state = await state.get_state()
    intent = await parse_intent(message.text)

    # ── Режим ожидания времени ──
    if current_state == BookingState.waiting_for_time.state:
        # ← FIX: расширенный список action'ов, которые прерывают FSM
        has_meaningful_intent = (
            intent
            and intent.get("action")
            and intent.get("action") not in ("book", "reschedule")
        )
        if not has_meaningful_intent:
            data = await state.get_data()
            match = re.search(r"(\d{1,2})[.,:\s-]+(\d{2})", message.text)
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
                    "Не могу распознать время 🤔\nНапишите в формате ЧЧ:ММ или нажмите кнопку выше."
                )

            event = data.get("event")
            action = data.get("action", "book")
            preferred_master = data.get("preferred_master")
            await state.clear()

            if not event or event not in EVENTS_CONFIG:
                return await message.reply(
                    "Что-то пошло не так. Выберите услугу заново:",
                    reply_markup=build_services_keyboard(user_id=uid),
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
                await send_program(message.chat.id, uid)
            return
        else:                                                           # ← FIX: если meaningful intent — выходим из FSM
            await state.clear()

    # ── Стандартная обработка через NLP ──
    # (state уже очищен выше или не был установлен)

    if not intent or not intent.get("action"):
        return await message.reply(
            WELCOME_TEXT,
            reply_markup=build_services_keyboard(user_id=uid),
            parse_mode="Markdown",
        )

    action = intent["action"]
    raw_event = (intent.get("event") or "").lower().strip()
    event = EVENT_ALIASES.get(raw_event, raw_event)
    time_str = intent.get("time")
    preferred_master = intent.get("preferred_master")

    # ── FIX: Отмена всех записей (через GPT) ──
    if action == "cancel_all":
        result = await cancel_all_bookings(message.from_user.id)
        await message.reply(result, parse_mode="Markdown")
        return

    # ── Мои записи ──
    if action == "my_bookings":
        text = build_program_message(uid)
        if text:
            await message.reply(text, parse_mode="Markdown")
        else:
            await message.reply(
                "У вас пока нет записей 😊\n\n✨ **Выберите услугу:**",
                reply_markup=build_services_keyboard(user_id=uid),
                parse_mode="Markdown",
            )
        return

    # ── Инфо ──
    if action == "info":
        if event in EVENTS_CONFIG:
            card = build_service_card(event)
            records = _sheet_cache.get(event, [])
            already_booked = any(str(r.get("ID", "")) == uid for r in records)

            if already_booked:
                user_rec = next(r for r in records if str(r.get("ID", "")) == uid)
                bt = str(user_rec.get("Время", ""))
                card += f"\n\n✅ **Вы уже записаны на {bt}**"
                buttons = [
                    [InlineKeyboardButton(
                        text="🔄 Перенести",
                        callback_data=f"start_reschedule|{event}",
                    ),
                    InlineKeyboardButton(
                        text="❌ Отменить",
                        callback_data=f"confirm_cancel|{event}",
                    )],
                    [InlineKeyboardButton(
                        text="← Назад к услугам",
                        callback_data="back_to_services",
                    )],
                ]
                return await message.reply(
                    card,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                    parse_mode="Markdown",
                )

            suggested = get_suggested_slots(event, records, preferred_master)
            if suggested:
                await state.update_data(
                    action="book", event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    card + "\n\n🕐 **Выберите время:**",
                    reply_markup=build_slot_keyboard(event, suggested, "book"),
                    parse_mode="Markdown",
                )
            return await message.reply(
                card + "\n\nК сожалению, мест нет 😔",
                parse_mode="Markdown",
            )
        return await message.reply(
            "✨ **Выберите услугу:**",
            reply_markup=build_services_keyboard(user_id=uid),
            parse_mode="Markdown",
        )

    # ── FIX: cancel без конкретного event — показать список записей для отмены ──
    if action == "cancel" and event not in EVENTS_CONFIG:
        bookings = get_all_user_bookings(uid)
        if not bookings:
            return await message.reply("У вас нет активных записей 😊")
        if len(bookings) == 1:
            # Одна запись — отменяем сразу
            single_event = bookings[0]["event"]
            async with get_lock(single_event):
                records = _sheet_cache.get(single_event, [])
                if any(str(r.get("ID", "")) == uid for r in records):
                    def delete_sync():
                        ws = sheet.worksheet(EVENTS_CONFIG[single_event]["sheet"])
                        ids = [str(v) for v in ws.col_values(1)]
                        if uid in ids:
                            ws.delete_rows(ids.index(uid) + 1)

                    await asyncio.to_thread(delete_sync)
                    _sheet_cache[single_event] = [
                        r for r in _sheet_cache[single_event]
                        if str(r.get("ID", "")) != uid
                    ]
                    job_id = f"{uid}_{single_event}"
                    if scheduler.get_job(job_id):
                        scheduler.remove_job(job_id)
                    await message.reply(
                        f"🗑 Запись {ef(single_event, 'to')} отменена."
                    )
                    await send_program(message.chat.id, uid)
                else:
                    await message.reply("У вас нет активных записей 😊")
            return

        # Несколько записей — показать клавиатуру выбора
        kb = build_cancel_keyboard(uid)
        return await message.reply(
            "❌ **Какую запись отменить?**",
            reply_markup=kb,
            parse_mode="Markdown",
        )

    # ── Мероприятие не распознано (для book/reschedule/availability) ──
    if event not in EVENTS_CONFIG:
        if action in ("book", "reschedule"):
            text = "Уточните, на какую услугу записаться? ✨\n\n👇 **Выберите:**"
        elif action == "availability":
            text = "Уточните, по какой услуге проверить наличие? ✨\n\n👇 **Выберите:**"
        else:
            text = WELCOME_TEXT
        return await message.reply(
            text, reply_markup=build_services_keyboard(user_id=uid), parse_mode="Markdown"
        )

    # ── Отмена конкретной услуги ──
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
        card = build_service_card(event)
        avail_list = get_available_slots(event, records, preferred_master)
        if avail_list:
            suggested = get_suggested_slots(event, records, preferred_master)
            if suggested:
                await state.update_data(
                    action="book", event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    card + "\n\n✨ **Записаться?**",
                    reply_markup=build_slot_keyboard(event, suggested, "book"),
                    parse_mode="Markdown",
                )
            return await message.reply(card, parse_mode="Markdown")
        return await message.reply(
            card + "\n\nСвободных мест нет 😔", parse_mode="Markdown"
        )

    # ── Бронирование / перенос без указанного времени ──
    if not time_str:
        cfg = EVENTS_CONFIG[event]
        if "fixed_time" in cfg:
            time_str = cfg["fixed_time"]
        else:
            records = _sheet_cache.get(event, [])
            suggested = get_suggested_slots(event, records, preferred_master)
            if suggested:
                card = build_service_card(event)
                await state.update_data(
                    action=action, event=event, preferred_master=preferred_master
                )
                await state.set_state(BookingState.waiting_for_time)
                return await message.reply(
                    card + "\n\n🕐 **Выберите время:**",
                    reply_markup=build_slot_keyboard(event, suggested, action),
                    parse_mode="Markdown",
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
    await state.clear()
    event = callback.data.split("|")[1]
    uid = str(callback.from_user.id)

    if event not in EVENTS_CONFIG:
        return await callback.message.edit_text("Услуга не найдена 😔")

    records = _sheet_cache.get(event, [])
    already = any(str(r.get("ID", "")) == uid for r in records)
    if already:
        bt = next((r.get("Время", "") for r in records if str(r.get("ID", "")) == uid), "?")
        await callback.message.edit_text(
            f"Вы уже записаны {ef(event, 'to')} на **{bt}** ✅\n"
            f"_Чтобы перенести, напишите «перенеси {ef(event, 'acc')}»_",
            parse_mode="Markdown",
        )
        return

    cfg = EVENTS_CONFIG[event]

    if "fixed_time" in cfg:
        time_str = cfg["fixed_time"]
        at_time = [r for r in records if str(r.get("Время", "")) == time_str]
        avail = cfg["capacity"] - len(at_time)
        if avail <= 0:
            return await callback.message.edit_text(
                f"К сожалению, мест {ef(event, 'at')} больше нет 😔"
            )
        await callback.message.edit_text(f"⏳ Записываю {ef(event, 'to')}…")
        try:
            res = await execute_booking(
                callback.from_user.id,
                f"@{callback.from_user.username}",
                callback.from_user.full_name,
                event, time_str,
            )
            await callback.message.edit_text(res["text"], parse_mode="Markdown")
            if res["ok"]:
                await send_program(callback.message.chat.id, uid)
        except Exception:
            logging.exception("Ошибка при бронировании fixed_time")
            await callback.message.edit_text(
                "Произошла ошибка при записи 😔 Попробуйте ещё раз."
            )
        return

    suggested = get_suggested_slots(event, records)
    if suggested:
        card = build_service_card(event)
        await state.update_data(action="book", event=event)
        await state.set_state(BookingState.waiting_for_time)
        await callback.message.edit_text(
            card + "\n\n🕐 **Выберите удобное время:**",
            reply_markup=build_slot_keyboard(event, suggested, "book"),
            parse_mode="Markdown",
        )
    else:
        await callback.message.edit_text(
            f"{EVENT_ICONS.get(event, '✨')} **{ef(event)}**\n\nК сожалению, все места заняты 😔",
            parse_mode="Markdown",
        )


@dp.callback_query(F.data.startswith("my_booking_detail|"))
async def process_booking_detail(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    event = callback.data.split("|")[1]
    uid = str(callback.from_user.id)
    records = _sheet_cache.get(event, [])
    user_rec = next((r for r in records if str(r.get("ID", "")) == uid), None)

    if not user_rec:
        return await callback.message.edit_text(
            f"Запись {ef(event, 'to')} не найдена 🤔",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="back_to_services")]
            ]),
        )

    time_str = str(user_rec.get("Время", ""))
    master_id = str(user_rec.get("Мастер/Детали", ""))
    icon = EVENT_ICONS.get(event, "✨")
    cfg = EVENTS_CONFIG[event]
    end_time = (
        datetime.strptime(time_str, "%H:%M") + timedelta(minutes=cfg["duration"])
    ).strftime("%H:%M")

    lines = [
        f"{icon} **{ef(event)}**",
        f"🕐 **{time_str} — {end_time}**",
    ]
    if event in MASTERS_CONFIG and master_id:
        for m in MASTERS_CONFIG[event]:
            if m["id"] == master_id:
                lines.append(f"👩‍⚕️ {m['label']}")
                if m.get("location"):
                    lines.append(f"📍 {m['location']}")
    if event in ("нутрициолог", "семейный нутрициолог"):
        lines.append("📍 Зал совещаний, 5 этаж")

    buttons = [
        [
            InlineKeyboardButton(text="🔄 Перенести", callback_data=f"start_reschedule|{event}"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"confirm_cancel|{event}"),
        ],
        [InlineKeyboardButton(text="← Назад к услугам", callback_data="back_to_services")],
    ]
    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("confirm_cancel|"))
async def process_confirm_cancel(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    event = callback.data.split("|")[1]
    uid = str(callback.from_user.id)

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
                r for r in _sheet_cache[event] if str(r.get("ID", "")) != uid
            ]
            job_id = f"{uid}_{event}"
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
            await callback.message.edit_text(f"🗑 Запись {ef(event, 'to')} отменена.")
            await send_program(callback.message.chat.id, uid)
        else:
            await callback.message.edit_text(f"У вас нет записи {ef(event, 'to')} 😊")


# ── FIX: callback для «Отменить все записи» ──
@dp.callback_query(F.data == "cancel_all_confirmed")
async def process_cancel_all_confirmed(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    result = await cancel_all_bookings(callback.from_user.id)
    await callback.message.edit_text(result, parse_mode="Markdown")


@dp.callback_query(F.data.startswith("start_reschedule|"))
async def process_start_reschedule(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    event = callback.data.split("|")[1]

    records = _sheet_cache.get(event, [])
    suggested = get_suggested_slots(event, records)
    if suggested:
        card = build_service_card(event)
        await state.update_data(action="reschedule", event=event)
        await state.set_state(BookingState.waiting_for_time)
        await callback.message.edit_text(
            f"🔄 **Перенос записи**\n\n{card}\n\n🕐 **Выберите новое время:**",
            reply_markup=build_slot_keyboard(event, suggested, "reschedule"),
            parse_mode="Markdown",
        )
    else:
        await callback.message.edit_text(
            f"Нет свободных окошек для переноса 😔",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="back_to_services")]
            ]),
        )


@dp.callback_query(F.data == "back_to_services")
async def process_back_to_services(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    uid = str(callback.from_user.id)
    await callback.message.edit_text(
        "✨ **Выберите услугу:**",
        reply_markup=build_services_keyboard(user_id=uid),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("no_slots|"))
async def process_no_slots(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer(
        "К сожалению, все места заняты 😔 Попробуйте позже!",
        show_alert=True,
    )


# ══════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════
async def main():
    health_runner = await start_health_server()

    await sync_cache_with_google()
    scheduler.add_job(background_sync, "interval", minutes=2)
    scheduler.start()

    try:
        await dp.start_polling(bot)
    finally:
        await health_runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())