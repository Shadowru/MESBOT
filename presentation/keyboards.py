from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.config import EVENTS_CONFIG, EVENT_ICONS, EVENT_FORMS
from services.booking_service import BookingService
from collections import defaultdict

def group_slots_by_hour(slots):
    groups = defaultdict(list)
    for s in slots:
        # Если s — это кортеж (время, кол-во), берем первый элемент
        time_str = s[0] if isinstance(s, (tuple, list)) else s
        hour = time_str.split(':')[0] + ":00"
        groups[hour].append(s)
    return dict(sorted(groups.items()))

async def build_services_keyboard(user_id: str, booking_service: BookingService) -> InlineKeyboardMarkup:
    buttons = []
    user_bookings = await booking_service.get_user_bookings(user_id)
    booked_events = [b.event for b in user_bookings]

    for ev in EVENTS_CONFIG:
        icon = EVENT_ICONS.get(ev, "✨")
        
        # Берем красивое название из конфига (EVENT_FORMS), 
        # а если его вдруг там нет — используем capitalize() как запасной вариант
        #title = EVENT_FORMS.get(ev, {}).get("title", ev.capitalize())
        title = EVENT_FORMS.get(ev, {}).get("title", ev.capitalize())
        
        if ev in booked_events:
            buttons.append([InlineKeyboardButton(text=f"✅ {title} — вы записаны", callback_data=f"my_booking_detail|{ev}")])
        else:
            suggested = await booking_service.get_suggested_slots(ev, top_n=1)
            if suggested:
                buttons.append([InlineKeyboardButton(text=f"{icon} {title}", callback_data=f"start_book|{ev}")])
            else:
                buttons.append([InlineKeyboardButton(text=f"⛔ {title} — мест нет", callback_data=f"no_slots|{ev}")])
                
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def build_slot_keyboard(event, slots, action="book", selected_hour=None):
    kb = []
    grouped = group_slots_by_hour(slots) # Сначала группируем
    
    print(slots.count)
    print(slots)
    
    # Если мы внутри часа (selected_hour есть) - показываем слоты
    if selected_hour:
        hour_slots = [s for s in slots if (s[0] if isinstance(s, (tuple, list)) else s).startswith(selected_hour.split(':')[0])]
        
        row = []
        for s in hour_slots:
            time_str = s[0] if isinstance(s, (tuple, list)) else s
            row.append(InlineKeyboardButton(text=time_str, callback_data=f"slot|{event}|{time_str}|{action}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        
        kb.append([InlineKeyboardButton(text="⬅️ Назад к часам", callback_data=f"back_to_hours|{event}|{action}")])
        
    # НОВАЯ ЛОГИКА: Если часов больше одного - показываем часы
    elif len(grouped) > 1:
        row = []
        for hour in grouped.keys():
            row.append(InlineKeyboardButton(text=hour, callback_data=f"hour|{event}|{hour}|{action}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        
    # Если час всего один - показываем сразу все слоты
    else:
        row = []
        for s in slots:
            time_str = s[0] if isinstance(s, (tuple, list)) else s
            row.append(InlineKeyboardButton(text=time_str, callback_data=f"slot|{event}|{time_str}|{action}"))
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)

    return InlineKeyboardMarkup(inline_keyboard=kb)

def build_masters_keyboard(event: str, time_str: str, masters: list, action: str = "book"):
    kb = []
    for m in masters:
        # callback_data: master|event|time|master_id|action
        data = f"master|{event}|{time_str}|{m['id']}|{action}"
        kb.append([InlineKeyboardButton(text=m['name'], callback_data=data)])
    kb.append([InlineKeyboardButton(text="← Назад", callback_data="back_to_services")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def get_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Моя программа")],
            [KeyboardButton(text="Все услуги")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False # Клавиатура будет висеть постоянно
    )