import json
import re
from openai import AsyncOpenAI
from core.interfaces import ILLMService
from core.models import Intent

class OpenAILLMService(ILLMService):
    def __init__(self, api_key: str):
        self.client = AsyncOpenAI(base_url="https://openai.api.proxyapi.ru/v1", api_key=api_key)

    async def parse_intent(self, text: str) -> Intent | None:
        prompt = (
            "Определи action: book, cancel, cancel_all, reschedule, availability, info, my_bookings.\n"
            'Ответь JSON: {"action":"...","event":"...","time":"HH:MM","preferred_master":"..."}\n'
            f"Текст: {text}"
        )
        try:
            response = await self.client.chat.completions.create(
                model="openai/gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            raw = response.choices[0].message.content
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            data = json.loads(m.group() if m else raw)
            return Intent(**data)
        except Exception:
            return None