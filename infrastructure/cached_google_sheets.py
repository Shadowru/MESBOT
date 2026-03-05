import asyncio
import gspread
from datetime import datetime
from typing import List, Optional, Dict
from oauth2client.service_account import ServiceAccountCredentials
from core.interfaces import IBookingRepository
from core.models import BookingRecord
from core.config import EVENTS_CONFIG

class GoogleSheetsRepository(IBookingRepository):
    def __init__(self, creds_path: str, sheet_url: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_url)
        
        self._cache: Dict[str, List[BookingRecord]] = {ev: [] for ev in EVENTS_CONFIG}
        self._locks: Dict[str, asyncio.Lock] = {ev: asyncio.Lock() for ev in EVENTS_CONFIG}

    async def sync(self) -> None:
        """Загрузка данных из Sheets в память (вызывать при старте)"""
        def fetch():
            data = {}
            for ev, cfg in EVENTS_CONFIG.items():
                ws = self.sheet.worksheet(cfg["sheet"])
                records = []
                # Используем get_all_records, чтобы получить список словарей
                for row in ws.get_all_records():
                    records.append(BookingRecord(
                        user_id=str(row.get("ID", "")),
                        username=str(row.get("Username", "")),
                        full_name=str(row.get("ФИО", "")),
                        event=ev,
                        time=str(row.get("Время", "")),
                        master_id=str(row.get("Мастер/Детали", ""))
                    ))
                data[ev] = records
            return data

        self._cache = await asyncio.to_thread(fetch)

    async def get_records(self, event: str) -> List[BookingRecord]:
        return self._cache.get(event, [])

    async def add_record(self, record: BookingRecord) -> None:
        # Работаем ТОЛЬКО с памятью
        async with self._locks[record.event]:
            self._cache[record.event].append(record)

    async def delete_record(self, event: str, user_id: str) -> None:
        # Работаем ТОЛЬКО с памятью
        async with self._locks[event]:
            self._cache[event] = [r for r in self._cache[event] if r.user_id != user_id]

    async def flush_to_sheets(self) -> None:
        """Выгрузка всего состояния памяти в Google Sheets"""
        def write():
            for ev, records in self._cache.items():
                ws = self.sheet.worksheet(EVENTS_CONFIG[ev]["sheet"])
                # Подготавливаем данные для записи
                # Заголовок (должен совпадать с тем, что ожидает get_all_records)
                data = [["ID", "Username", "ФИО", "Время", "Мастер/Детали"]]
                for r in records:
                    data.append([r.user_id, r.username, r.full_name, r.time, r.master_id])
                
                # Очищаем лист и записываем заново
                ws.clear()
                ws.append_rows(data)
        
        await asyncio.to_thread(write)
        
    def get_last_sync_time(self) -> Optional[datetime]:
        return self._last_syncopenai_service.py
    