from dataclasses import dataclass
from typing import Optional

@dataclass
class BookingRecord:
    user_id: str
    username: str
    full_name: str
    event: str
    time: str
    master_id: str

@dataclass
class Intent:
    action: str
    event: Optional[str] = None
    time: Optional[str] = None
    preferred_master: Optional[str] = None