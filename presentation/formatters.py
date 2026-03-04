from core.config import EVENTS_CONFIG, EVENT_ICONS, SERVICE_DESCRIPTIONS, EVENT_FORMS, MASTERS_CONFIG
from core.models import BookingRecord
from typing import List

def ef(event, case="title"):
    return EVENT_FORMS.get(event, {}).get(case, event)

def build_service_card(event: str, available_slots: list) -> str:
    cfg = EVENTS_CONFIG[event]
    icon = EVENT_ICONS.get(event, "✨")
    desc = SERVICE_DESCRIPTIONS.get(event, "")
    
    total_available = sum(a for t, a in available_slots)
    
    #lines = [
    #    f"{icon} **{ef(event)}**",
    #    f"_{desc}_", "",
    #    f"🕐 {cfg['start']} — {cfg['end']}  ·  ⏱ {cfg['duration']} мин",
    #    f"📊 Свободно мест: {total_available}" if total_available else "⛔ Свободных мест нет"
    #]

    lines = [
        f"{icon} **{ef(event)}**",
        f"_{desc}_", "",
        f"🕐 {cfg['start']} — {cfg['end']}  ·  ⏱ {cfg['duration']} мин"
    ]

    return "\n".join(lines)

def build_program_message(bookings) -> str:
    if not bookings:
        return ""
    
    text = "📅 **Ваша программа на сегодня:**\n\n"
    
    # Сортируем записи по времени
    sorted_bookings = sorted(bookings, key=lambda x: x.time)
    
    for b in sorted_bookings:
        # Получаем красивое название услуги
        title = EVENT_FORMS.get(b.event, {}).get("title", b.event)
        
        # Пытаемся найти локацию
        location = "Не указана"
        
        # Если есть мастер, ищем его локацию в MASTERS_CONFIG
        if b.master_id and b.master_id != "Записано":
            masters = MASTERS_CONFIG.get(b.event, [])
            master = next((m for m in masters if m["id"] == b.master_id), None)
            if master:
                location = master.get("location", "Не указана")
        
        # Если локация всё еще "Не указана", можно попробовать взять дефолтную из конфига, 
        # если бы она там была, но пока берем из мастера.
        
        text += (
            f"• **{title}**\n"
            f"  🕐 Время: {b.time}\n"
            f"  📍 Место: {location}\n"
            f"  👤 Мастер: {b.master_id if b.master_id != 'Записано' else 'Любой'}\n\n"
        )
        
    return text