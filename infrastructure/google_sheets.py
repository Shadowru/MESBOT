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
        self._last_sync: Optional[datetime] = None

    async def sync(self) -> None:
        def fetch():
            data = {}
            for ev, cfg in EVENTS_CONFIG.items():
                ws = self.sheet.worksheet(cfg["sheet"])
                records = []
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
        self._last_sync = datetime.now()

    async def get_records(self, event: str) -> List[BookingRecord]:
        return self._cache.get(event, [])

    async def add_record(self, record: BookingRecord) -> None:
        async with self._locks[record.event]:
            def append():
                ws = self.sheet.worksheet(EVENTS_CONFIG[record.event]["sheet"])
                ws.append_row([record.user_id, record.username, record.full_name, record.time, record.master_id])
            
            await asyncio.to_thread(append)
            self._cache[record.event].append(record)

    async def delete_record(self, event: str, user_id: str) -> None:
        async with self._locks[event]:
            def delete():
                ws = self.sheet.worksheet(EVENTS_CONFIG[event]["sheet"])
                ids = [str(v) for v in ws.col_values(1)]
                if user_id in ids:
                    ws.delete_rows(ids.index(user_id) + 1)

            await asyncio.to_thread(delete)
            self._cache[event] = [r for r in self._cache[event] if r.user_id != user_id]

    def get_last_sync_time(self) -> Optional[datetime]:
        return self._last_syncopenai_service.py
    