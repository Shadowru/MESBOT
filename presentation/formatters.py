from core.config import EVENTS_CONFIG, EVENT_ICONS, SERVICE_DESCRIPTIONS, EVENT_FORMS
from core.models import BookingRecord
from typing import List

def ef(event: str, form: str = "title") -> str:
    return EVENT_FORMS.get(event, {}).get(form, event.capitalize())

def build_service_card(event: str, available_slots: list) -> str:
    cfg = EVENTS_CONFIG[event]
    icon = EVENT_ICONS.get(event, "✨")
    desc = SERVICE_DESCRIPTIONS.get(event, "")
    
    total_available = sum(a for t, a in available_slots)
    
    lines = [
        f"{icon} **{ef(event)}**",
        f"_{desc}_", "",
        f"🕐 {cfg['start']} — {cfg['end']}  ·  ⏱ {cfg['duration']} мин",
        f"📊 Свободно мест: {total_available}" if total_available else "⛔ Свободных мест нет"
    ]
    return "\n".join(lines)

def build_program_message(bookings: List[BookingRecord]) -> str | None:
    if not bookings:
        return None
        
    bookings.sort(key=lambda b: b.time)
    total_events = len(EVENTS_CONFIG)
    booked_count = len(bookings)
    progress = "●" * booked_count + "○" * (total_events - booked_count)

    lines = [
        f"📋 **Ваша бьюти-программа**",
        f"  {progress}  {booked_count} из {total_events}",
        ""
    ]

    for b in bookings:
        icon = EVENT_ICONS.get(b.event, "✨")
        lines.append(f"  {icon}  **{b.time}**  │  {ef(b.event)}")
        if b.master_id and b.master_id != "Записано":
            lines.append(f"        ↳ _Специалист: {b.master_id}_")

    return "\n".join(lines)