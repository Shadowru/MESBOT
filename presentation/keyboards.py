from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.config import EVENTS_CONFIG, EVENT_ICONS, EVENT_FORMS
from services.booking_service import BookingService

async def build_services_keyboard(user_id: str, booking_service: BookingService) -> InlineKeyboardMarkup:
    buttons = []
    user_bookings = await booking_service.get_user_bookings(user_id)
    booked_events = [b.event for b in user_bookings]

    for ev in EVENTS_CONFIG:
        icon = EVENT_ICONS.get(ev, "✨")
        
        # Берем красивое название из конфига (EVENT_FORMS), 
        # а если его вдруг там нет — используем capitalize() как запасной вариант
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

def build_slot_keyboard(event: str, suggested: list, action: str = "book") -> InlineKeyboardMarkup:
    buttons = []
    for t, a in suggested:
        buttons.append([InlineKeyboardButton(text=f"🕐 {t}  ·  свободно: {a}", callback_data=f"slot|{event}|{t}|{action}")])
    buttons.append([InlineKeyboardButton(text="← Назад к услугам", callback_data="back_to_services")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)