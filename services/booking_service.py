import asyncio
import random
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
from core.interfaces import IBookingRepository
from core.models import BookingRecord
from core.config import EVENTS_CONFIG, MASTERS_CONFIG

class BookingService:
    def __init__(self, repo: IBookingRepository):
        self.repo = repo
        self._user_locks: Dict[str, asyncio.Lock] = {}
        self.global_lock = asyncio.Lock() # Глобальная блокировка

    def _get_user_lock(self, user_id: str) -> asyncio.Lock:
        if user_id not in self._user_locks:
            self._user_locks[user_id] = asyncio.Lock()
        return self._user_locks[user_id]

    def get_slot_list(self, event: str) -> List[str]:
        cfg = EVENTS_CONFIG[event]
        if "fixed_time" in cfg: return [cfg["fixed_time"]]
        if "custom_slots" in cfg: return list(cfg["custom_slots"])
        
        start_dt = datetime.strptime(cfg["start"], "%H:%M")
        end_dt = datetime.strptime(cfg["end"], "%H:%M")
        delta = timedelta(minutes=cfg["duration"])
        
        slots, cur = [], start_dt
        while cur < end_dt:
            slots.append(cur.strftime("%H:%M"))
            cur += delta
        return slots

    async def get_user_bookings(self, user_id: str) -> List[BookingRecord]:
        bookings = []
        for ev in EVENTS_CONFIG:
            records = await self.repo.get_records(ev)
            bookings.extend([r for r in records if r.user_id == user_id])
        return bookings

    async def get_suggested_slots(self, event: str, top_n: int = 100) -> List[Tuple[str, int]]:
        records = await self.repo.get_records(event)
        slots = []
        for s in self.get_slot_list(event):
            at_slot = [r for r in records if r.time == s]
            if event in MASTERS_CONFIG:
                busy_ids = [r.master_id for r in at_slot]
                avail = sum(1 for m in MASTERS_CONFIG[event] if s not in m.get("breaks", []) and m["id"] not in busy_ids)
            else:
                avail = EVENTS_CONFIG[event]["capacity"] - len(at_slot)
            
            if avail > 0:
                slots.append((s, avail))
        return sorted(slots, key=lambda x: x[0])[:top_n]

    def get_available_masters(self, event: str, time_str: str) -> List[dict]:
        """Возвращает список свободных мастеров на конкретное время."""
        if event not in MASTERS_CONFIG:
            return []
        
        # Получаем записи на это время
        records = self.repo.get_records(event) # Предполагаем, что метод синхронный или await
        # Если get_records асинхронный, нужно сделать await
        
        # Для простоты, если get_records возвращает список:
        at_time = [r for r in records if r.time == time_str]
        busy_ids = [r.master_id for r in at_time]
        
        available = [
            m for m in MASTERS_CONFIG[event] 
            if time_str not in m.get("breaks", []) and m["id"] not in busy_ids
        ]
        return available

    async def execute_booking(self, user_id: str, username: str, full_name: str, event: str, time_str: str, 
                              is_reschedule: bool = False, master_id: str = None, force: bool = False) -> dict:
        valid_slots = self.get_slot_list(event)
        if time_str not in valid_slots:
            return {"ok": False, "text": "invalid_time"}
        # ----------------------
        
        async with self.global_lock:
            async with self._get_user_lock(user_id):
            
                # 2. НОВАЯ ПРОВЕРКА: Проверка на пересечение времени с другими активностями
                # Проверка на пересечение (только если не принудительное подтверждение)
                if not force:
                    all_user_bookings = await self.get_user_bookings(user_id)
                    for b in all_user_bookings:
                        if b.time == time_str:
                            # Если это перенос, игнорируем старую запись
                            if is_reschedule and b.event == event:
                                continue
                            # Возвращаем статус конфликта
                            return {
                            "ok": False, 
                            "status": "conflict", 
                            "conflict_event": b.event,
                            "text": f"Конфликт времени с {b.event}" 
                            }
            
                records = await self.repo.get_records(event)
            
                if is_reschedule:
                    await self.repo.delete_record(event, user_id)
                elif any(r.user_id == user_id for r in records):
                    return {"ok": False, "text": "Вы уже записаны на эту услугу."}

            # Логика выбора мастера
                final_master_id = "Записано"
            
                if event in MASTERS_CONFIG:
                    at_time = [r for r in records if r.time == time_str]
                    busy_ids = [r.master_id for r in at_time]
                    available_masters = [
                        m for m in MASTERS_CONFIG[event] 
                        if time_str not in m.get("breaks", []) and m["id"] not in busy_ids
                    ]
                
                    if not available_masters:
                        return {"ok": False, "text": "Все мастера заняты на это время."}

                    # Если передан конкретный мастер (для гадалок)
                    if master_id:
                        selected_master = next((m for m in available_masters if m["id"] == master_id), None)
                        if not selected_master:
                            return {"ok": False, "text": "Этот специалист уже занят."}
                        final_master_id = selected_master["id"]
                    else:
                        # Случайный выбор для остальных
                        final_master_id = random.choice(available_masters)["id"]

                elif event in EVENTS_CONFIG and len([r for r in records if r.time == time_str]) >= EVENTS_CONFIG[event]["capacity"]:
                    return {"ok": False, "text": "Мест нет."}

                record = BookingRecord(user_id, username, full_name, event, time_str, final_master_id)
                await self.repo.add_record(record)
                return {"ok": True, "text": f"✅ Записано! {'Специалист: ' + final_master_id if final_master_id != 'Записано' else ''}"}
        
        
    async def cancel_all(self, user_id: str) -> str:
        bookings = await self.get_user_bookings(user_id)
        if not bookings: return "У вас нет записей."
        for b in bookings:
            await self.repo.delete_record(b.event, user_id)
        return "🗑 Все записи отменены."
    
    async def cancel_booking(self, user_id: str, event: str) -> str:
        records = await self.repo.get_records(event)
        if not any(r.user_id == user_id for r in records):
            return f"У вас нет записи на {event} 😊"
            
        await self.repo.delete_record(event, user_id)
        return f"🗑 Запись на {event} отменена."