from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
from core.models import BookingRecord, Intent

class IBookingRepository(ABC):
    @abstractmethod
    async def get_records(self, event: str) -> List[BookingRecord]: pass

    @abstractmethod
    async def add_record(self, record: BookingRecord) -> None: pass

    @abstractmethod
    async def delete_record(self, event: str, user_id: str) -> None: pass

    @abstractmethod
    async def sync(self) -> None: pass

    @abstractmethod
    def get_last_sync_time(self) -> Optional[datetime]: pass

class ILLMService(ABC):
    @abstractmethod
    async def parse_intent(self, text: str) -> Optional[Intent]: pass