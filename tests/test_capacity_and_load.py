# tests/test_capacity_and_load.py
"""
Тесты на:
1. Заполнение фиксированных слотов до предела capacity
2. Нагрузочные тесты (сотни параллельных запросов)
3. Проверка параллельных записей (race conditions)
"""

import asyncio
import time
import logging
import pytest
from collections import Counter
from unittest.mock import MagicMock, AsyncMock

import bot as bot_module
from bot import (
    execute_booking,
    get_all_user_bookings,
    get_slot_list,
    get_available_slots,
    get_suggested_slots,
    EVENTS_CONFIG,
    MASTERS_CONFIG,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  Фикстура: чистый кэш + моки перед каждым тестом
# ──────────────────────────────────────────────
@pytest.fixture(autouse=True)
def clean_state():
    """Сбрасываем кэш и блокировки перед каждым тестом."""
    bot_module._sheet_cache.clear()
    bot_module._user_locks.clear()
    bot_module._booking_locks.clear()

    mock_ws = MagicMock()
    mock_ws.get_all_records.return_value = []
    mock_ws.col_values.return_value = []
    mock_ws.append_row.return_value = None
    mock_ws.delete_rows.return_value = None

    mock_sheet = MagicMock()
    mock_sheet.worksheet.return_value = mock_ws

    bot_module.sheet = mock_sheet

    mock_scheduler = MagicMock()
    mock_scheduler.get_job.return_value = None
    bot_module.scheduler = mock_scheduler

    bot_module.bot = AsyncMock()

    yield {"worksheet": mock_ws, "sheet": mock_sheet}


# ╔══════════════════════════════════════════════════════════════╗
# ║  1. ЗАПОЛНЕНИЕ ФИКСИРОВАННЫХ СЛОТОВ ДО ПРЕДЕЛА CAPACITY    ║
# ╚══════════════════════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestFixedSlotCapacity:
    """Проверяем что для КАЖДОГО мероприятия capacity строго соблюдается."""

    async def test_aroma_capacity_1_per_slot(self):
        """Аромапсихолог: capacity=1, один человек на слот."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        r1 = await execute_booking(100, "@a", "A", "аромапсихолог", "14:00")
        assert r1["ok"] is True

        r2 = await execute_booking(200, "@b", "B", "аромапсихолог", "14:00")
        assert r2["ok"] is False
        assert "занято" in r2["text"].lower()

        # Но на другой слот — можно
        r3 = await execute_booking(200, "@b", "B", "аромапсихолог", "14:10")
        assert r3["ok"] is True

    async def test_aroma_fill_all_slots(self):
        """Заполняем ВСЕ слоты аромапсихолога, потом проверяем что больше нельзя."""
        bot_module._sheet_cache = {"аромапсихолог": []}
        all_slots = get_slot_list("аромапсихолог")

        for i, slot in enumerate(all_slots):
            r = await execute_booking(1000 + i, f"@u{i}", f"User{i}", "аромапсихолог", slot)
            assert r["ok"] is True, f"Слот {slot} должен быть доступен, ошибка: {r['text']}"

        # Все слоты заняты
        for slot in all_slots:
            r = await execute_booking(9999, "@last", "Last", "аромапсихолог", slot)
            assert r["ok"] is False, f"Слот {slot} должен быть занят!"

        # get_available_slots должен быть пуст
        avail = get_available_slots("аромапсихолог", bot_module._sheet_cache["аромапсихолог"])
        assert len(avail) == 0

    async def test_nutricionist_capacity_30(self):
        """Нутрициолог: fixed_time=15:00, capacity=30."""
        bot_module._sheet_cache = {"нутрициолог": []}

        for i in range(30):
            r = await execute_booking(1000 + i, f"@u{i}", f"User{i}", "нутрициолог", "15:00")
            assert r["ok"] is True, f"Запись #{i+1} должна пройти"

        # 31-й — отказ
        r = await execute_booking(9999, "@last", "Last", "нутрициолог", "15:00")
        assert r["ok"] is False
        assert len(bot_module._sheet_cache["нутрициолог"]) == 30

    async def test_семейный_nutricionist_capacity_30(self):
        """Семейный нутрициолог: fixed_time=15:00, capacity=30."""
        bot_module._sheet_cache = {"семейный нутрициолог": []}

        for i in range(30):
            r = await execute_booking(2000 + i, f"@u{i}", f"User{i}", "семейный нутрициолог", "15:00")
            assert r["ok"] is True

        r = await execute_booking(9999, "@last", "Last", "семейный нутрициолог", "15:00")
        assert r["ok"] is False

    async def test_massage_capacity_3_masters_per_slot(self):
        """Массаж: 3 мастера = максимум 3 записи на один слот."""
        bot_module._sheet_cache = {"массаж": []}

        results = []
        for i in range(3):
            r = await execute_booking(100 + i, f"@u{i}", f"U{i}", "массаж", "11:00")
            results.append(r)

        assert all(r["ok"] for r in results)

        # 4-й — нет
        r4 = await execute_booking(999, "@x", "X", "массаж", "11:00")
        assert r4["ok"] is False
        assert "заняты" in r4["text"].lower()

    async def test_massage_unique_masters_assigned(self):
        """Каждому пользователю назначается уникальный мастер."""
        bot_module._sheet_cache = {"массаж": []}

        for i in range(3):
            await execute_booking(100 + i, f"@u{i}", f"U{i}", "массаж", "11:00")

        masters = [r["Мастер/Детали"] for r in bot_module._sheet_cache["массаж"]]
        assert len(set(masters)) == 3, f"Мастера должны быть уникальны: {masters}"

    async def test_massage_fill_all_slots_all_masters(self):
        """Заполняем ВСЕ слоты массажа по 3 мастера (кроме перерывов)."""
        bot_module._sheet_cache = {"массаж": []}
        all_slots = get_slot_list("массаж")
        user_id = 10000

        for slot in all_slots:
            for attempt in range(3):
                r = await execute_booking(user_id, f"@u{user_id}", f"U{user_id}", "массаж", slot)
                user_id += 1
                # Может быть меньше 3 из-за перерывов мастеров
                if not r["ok"]:
                    break

        # Проверяем что нигде нет overbooking
        for slot in all_slots:
            at_slot = [
                r for r in bot_module._sheet_cache["массаж"]
                if str(r.get("Время", "")) == slot
            ]
            assert len(at_slot) <= 3, f"Overbooking в слоте {slot}: {len(at_slot)} записей"

    async def test_gadалки_capacity_2_per_slot(self):
        """Гадалки: 2 специалиста = максимум 2 на слот."""
        bot_module._sheet_cache = {"гадалки": []}

        r1 = await execute_booking(100, "@a", "A", "гадалки", "11:00")
        r2 = await execute_booking(200, "@b", "B", "гадалки", "11:00")
        assert r1["ok"] and r2["ok"]

        r3 = await execute_booking(300, "@c", "C", "гадалки", "11:00")
        assert r3["ok"] is False

    async def test_gadалки_fill_all_slots(self):
        """Заполняем ВСЕ слоты гадалок (capacity=2)."""
        bot_module._sheet_cache = {"гадалки": []}
        all_slots = get_slot_list("гадалки")
        uid = 5000

        for slot in all_slots:
            for _ in range(2):
                r = await execute_booking(uid, f"@u{uid}", f"U{uid}", "гадалки", slot)
                assert r["ok"] is True, f"Слот {slot}: {r['text']}"
                uid += 1

        # Всё занято
        for slot in all_slots:
            r = await execute_booking(uid, f"@u{uid}", f"U{uid}", "гадалки", slot)
            assert r["ok"] is False
            uid += 1

    async def test_makiyazh_capacity_4_per_slot(self):
        """Макияж: 4 визажиста = максимум 4 на слот."""
        bot_module._sheet_cache = {"макияж": []}

        for i in range(4):
            r = await execute_booking(100 + i, f"@u{i}", f"U{i}", "макияж", "10:00")
            assert r["ok"] is True

        r5 = await execute_booking(999, "@x", "X", "макияж", "10:00")
        assert r5["ok"] is False

    async def test_makiyazh_fill_all_slots(self):
        """Заполняем ВСЕ слоты макияжа (capacity=4)."""
        bot_module._sheet_cache = {"макияж": []}
        all_slots = get_slot_list("макияж")
        uid = 6000

        for slot in all_slots:
            for _ in range(4):
                r = await execute_booking(uid, f"@u{uid}", f"U{uid}", "макияж", slot)
                assert r["ok"] is True
                uid += 1

        # Проверяем что всё занято
        avail = get_available_slots("макияж", bot_module._sheet_cache["макияж"])
        assert len(avail) == 0

    async def test_chekhov_capacity_10_per_slot(self):
        """Мастерская Чехова: capacity=10, custom_slots."""
        bot_module._sheet_cache = {"мастерская чехова": []}

        for i in range(10):
            r = await execute_booking(100 + i, f"@u{i}", f"U{i}", "мастерская чехова", "14:00")
            assert r["ok"] is True

        r11 = await execute_booking(999, "@x", "X", "мастерская чехова", "14:00")
        assert r11["ok"] is False

    async def test_chekhov_fill_all_custom_slots(self):
        """Заполняем ВСЕ 5 кастомных слотов мастерской по 10 мест."""
        bot_module._sheet_cache = {"мастерская чехова": []}
        all_slots = get_slot_list("мастерская чехова")
        assert len(all_slots) == 5
        uid = 7000

        for slot in all_slots:
            for _ in range(10):
                r = await execute_booking(uid, f"@u{uid}", f"U{uid}", "мастерская чехова", slot)
                assert r["ok"] is True
                uid += 1

        total = len(bot_module._sheet_cache["мастерская чехова"])
        assert total == 50  # 5 слотов × 10 мест

        avail = get_available_slots("мастерская чехова", bot_module._sheet_cache["мастерская чехова"])
        assert len(avail) == 0

    async def test_capacity_exact_boundary(self):
        """Проверяем точную границу: capacity-1 → ok, capacity → ok, capacity+1 → fail."""
        for event, cfg in EVENTS_CONFIG.items():
            bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

            slot = get_slot_list(event)[0]
            effective_cap = (
                len(MASTERS_CONFIG[event])
                if event in MASTERS_CONFIG
                else cfg["capacity"]
            )

            # Для мастеров на слоте с перерывами — считаем реальную доступность
            if event in MASTERS_CONFIG:
                breaks_count = sum(
                    1 for m in MASTERS_CONFIG[event] if slot in m.get("breaks", [])
                )
                effective_cap -= breaks_count

            uid = 20000
            successes = 0
            for _ in range(effective_cap + 5):
                r = await execute_booking(uid, f"@u{uid}", f"U{uid}", event, slot)
                if r["ok"]:
                    successes += 1
                uid += 1

            assert successes == effective_cap, (
                f"{event} слот {slot}: ожидалось {effective_cap} записей, "
                f"получилось {successes}"
            )


# ╔══════════════════════════════════════════════════════════════╗
# ║  2. ПАРАЛЛЕЛЬНЫЕ ЗАПИСИ (RACE CONDITIONS)                  ║
# ╚══════════════════════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestParallelBookings:
    """Проверяем что блокировки (asyncio.Lock) предотвращают overbooking."""

    async def test_parallel_aroma_capacity_1(self):
        """10 пользователей одновременно → ровно 1 проходит."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        tasks = [
            execute_booking(100 + i, f"@u{i}", f"U{i}", "аромапсихолог", "14:00")
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        fail_count = sum(1 for r in results if not r["ok"])

        assert ok_count == 1, f"Должна пройти ровно 1 запись, прошло {ok_count}"
        assert fail_count == 9
        assert len(bot_module._sheet_cache["аромапсихолог"]) == 1

    async def test_parallel_massage_capacity_3(self):
        """20 пользователей одновременно на массаж → ровно 3."""
        bot_module._sheet_cache = {"массаж": []}

        tasks = [
            execute_booking(200 + i, f"@u{i}", f"U{i}", "массаж", "11:00")
            for i in range(20)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 3, f"Должно пройти 3, прошло {ok_count}"

        # Все мастера уникальные
        masters = [r["Мастер/Детали"] for r in bot_module._sheet_cache["массаж"]]
        assert len(set(masters)) == 3

    async def test_parallel_gadалки_capacity_2(self):
        """15 одновременно к гадалкам → ровно 2."""
        bot_module._sheet_cache = {"гадалки": []}

        tasks = [
            execute_booking(300 + i, f"@u{i}", f"U{i}", "гадалки", "12:00")
            for i in range(15)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 2

    async def test_parallel_makiyazh_capacity_4(self):
        """25 одновременно на макияж → ровно 4."""
        bot_module._sheet_cache = {"макияж": []}

        tasks = [
            execute_booking(400 + i, f"@u{i}", f"U{i}", "макияж", "10:00")
            for i in range(25)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 4

    async def test_parallel_nutricionist_capacity_30(self):
        """50 одновременно к нутрициологу → ровно 30."""
        bot_module._sheet_cache = {"нутрициолог": []}

        tasks = [
            execute_booking(500 + i, f"@u{i}", f"U{i}", "нутрициолог", "15:00")
            for i in range(50)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 30, f"Должно пройти 30, прошло {ok_count}"
        assert len(bot_module._sheet_cache["нутрициолог"]) == 30

    async def test_parallel_chekhov_capacity_10(self):
        """30 одновременно в мастерскую → ровно 10."""
        bot_module._sheet_cache = {"мастерская чехова": []}

        tasks = [
            execute_booking(600 + i, f"@u{i}", f"U{i}", "мастерская чехова", "14:00")
            for i in range(30)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 10

    async def test_parallel_different_slots_no_interference(self):
        """Параллельные записи на РАЗНЫЕ слоты не мешают друг другу."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        slots = get_slot_list("аромапсихолог")[:5]
        tasks = [
            execute_booking(700 + i, f"@u{i}", f"U{i}", "аромапсихолог", slot)
            for i, slot in enumerate(slots)
        ]
        results = await asyncio.gather(*tasks)

        assert all(r["ok"] for r in results), "Каждый слот свободен — все должны пройти"

    async def test_parallel_same_user_double_booking(self):
        """Один пользователь пытается записаться дважды параллельно."""
        bot_module._sheet_cache = {"массаж": []}

        tasks = [
            execute_booking(999, "@same", "Same", "массаж", "11:00"),
            execute_booking(999, "@same", "Same", "массаж", "11:10"),
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 1, "Один пользователь — одна запись на мероприятие"

    async def test_parallel_no_duplicate_ids_in_cache(self):
        """После параллельных записей нет дубликатов по ID в кэше."""
        bot_module._sheet_cache = {"массаж": []}

        tasks = [
            execute_booking(100 + i, f"@u{i}", f"U{i}", "массаж", "11:00")
            for i in range(10)
        ]
        await asyncio.gather(*tasks)

        ids = [r["ID"] for r in bot_module._sheet_cache["массаж"]]
        assert len(ids) == len(set(ids)), f"Дубликаты ID: {ids}"

    async def test_parallel_different_events_same_user(self):
        """Пользователь параллельно записывается на разные мероприятия без конфликта."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        tasks = [
            execute_booking(888, "@u", "U", "макияж", "10:00"),
            execute_booking(888, "@u", "U", "массаж", "11:00"),
            execute_booking(888, "@u", "U", "аромапсихолог", "14:00"),
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 3, f"Разные мероприятия без конфликта: {ok_count}/3"

    async def test_parallel_conflict_detection(self):
        """Параллельные записи на пересекающееся время разных мероприятий."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        # Нутрициолог 15:00 (90 мин, до 16:30) конфликтует с Мастерской 16:00 (60 мин)
        tasks = [
            execute_booking(777, "@u", "U", "нутрициолог", "15:00"),
            execute_booking(777, "@u", "U", "мастерская чехова", "16:00"),
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 1, "Конфликт по времени — только одна запись должна пройти"


# ╔══════════════════════════════════════════════════════════════╗
# ║  3. НАГРУЗОЧНЫЕ ТЕСТЫ                                      ║
# ╚══════════════════════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestLoadBooking:
    """Стресс-тесты с большим количеством параллельных запросов."""

    async def test_load_100_users_one_slot_aroma(self):
        """100 пользователей атакуют 1 слот аромапсихолога (capacity=1)."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        tasks = [
            execute_booking(i, f"@u{i}", f"U{i}", "аромапсихолог", "14:00")
            for i in range(100)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 1
        assert len(bot_module._sheet_cache["аромапсихолог"]) == 1
        logger.info(f"100 users → 1 slot aroma: {elapsed:.2f}s")

    async def test_load_200_users_massage(self):
        """200 пользователей атакуют 1 слот массажа (capacity=3)."""
        bot_module._sheet_cache = {"массаж": []}

        tasks = [
            execute_booking(i, f"@u{i}", f"U{i}", "массаж", "11:00")
            for i in range(200)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 3
        assert len(bot_module._sheet_cache["массаж"]) == 3
        logger.info(f"200 users → 1 slot massage: {elapsed:.2f}s")

    async def test_load_100_users_nutricionist(self):
        """100 пользователей на нутрициолога (capacity=30)."""
        bot_module._sheet_cache = {"нутрициолог": []}

        tasks = [
            execute_booking(i, f"@u{i}", f"U{i}", "нутрициолог", "15:00")
            for i in range(100)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 30
        logger.info(f"100 users → nutricionist: {elapsed:.2f}s")

    async def test_load_500_users_spread_across_slots(self):
        """500 пользователей на массаж, распределяясь по разным слотам."""
        bot_module._sheet_cache = {"массаж": []}
        all_slots = get_slot_list("массаж")

        tasks = [
            execute_booking(
                i, f"@u{i}", f"U{i}", "массаж", all_slots[i % len(all_slots)]
            )
            for i in range(500)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        total_records = len(bot_module._sheet_cache["массаж"])

        # Проверяем что нет overbooking ни в одном слоте
        slot_counts = Counter(
            str(r.get("Время", "")) for r in bot_module._sheet_cache["массаж"]
        )
        for slot, count in slot_counts.items():
            # Учитываем перерывы мастеров
            breaks_at_slot = sum(
                1 for m in MASTERS_CONFIG["массаж"]
                if slot in m.get("breaks", [])
            )
            max_cap = 3 - breaks_at_slot
            assert count <= max_cap, (
                f"Overbooking в слоте {slot}: {count} > {max_cap}"
            )

        logger.info(
            f"500 users spread → massage: {ok_count} ok, "
            f"{total_records} records, {elapsed:.2f}s"
        )

    async def test_load_300_users_chekhov(self):
        """300 пользователей в мастерскую Чехова (5 слотов × 10 мест = 50 макс)."""
        bot_module._sheet_cache = {"мастерская чехова": []}
        all_slots = get_slot_list("мастерская чехова")

        tasks = [
            execute_booking(
                i, f"@u{i}", f"U{i}", "мастерская чехова",
                all_slots[i % len(all_slots)]
            )
            for i in range(300)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        assert ok_count == 50, f"Ожидалось 50, прошло {ok_count}"

        slot_counts = Counter(
            str(r.get("Время", ""))
            for r in bot_module._sheet_cache["мастерская чехова"]
        )
        for slot, count in slot_counts.items():
            assert count <= 10, f"Overbooking в слоте {slot}: {count}"

        logger.info(f"300 users → chekhov: {ok_count}/50 ok, {elapsed:.2f}s")

    async def test_load_1000_users_all_events_simultaneously(self):
        """1000 пользователей записываются на ВСЕ мероприятия одновременно."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        tasks = []
        uid = 0
        for event in EVENTS_CONFIG:
            slots = get_slot_list(event)
            for i in range(150):
                slot = slots[i % len(slots)]
                tasks.append(
                    execute_booking(uid, f"@u{uid}", f"U{uid}", event, slot)
                )
                uid += 1

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        ok_count = sum(1 for r in results if r["ok"])
        total_records = sum(len(v) for v in bot_module._sheet_cache.values())

        assert ok_count == total_records, "ok_count и записи в кэше расходятся"

        # Проверяем capacity по каждому мероприятию и слоту
        for event, cfg in EVENTS_CONFIG.items():
            records = bot_module._sheet_cache[event]
            slot_counts = Counter(str(r.get("Время", "")) for r in records)

            for slot, count in slot_counts.items():
                if event in MASTERS_CONFIG:
                    breaks_at = sum(
                        1 for m in MASTERS_CONFIG[event]
                        if slot in m.get("breaks", [])
                    )
                    max_cap = len(MASTERS_CONFIG[event]) - breaks_at
                else:
                    max_cap = cfg["capacity"]

                assert count <= max_cap, (
                    f"OVERBOOKING! {event} слот {slot}: "
                    f"{count} записей > max {max_cap}"
                )

        logger.info(
            f"1000+ tasks across all events: {ok_count} ok, "
            f"{total_records} records, {elapsed:.2f}s"
        )

    async def test_load_rapid_book_cancel_rebook(self):
        """Быстрая последовательность: запись → отмена → перезапись, 50 раз."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        for i in range(50):
            uid = 30000 + i
            # Записываемся
            r1 = await execute_booking(
                uid, f"@u{uid}", f"U{uid}", "аромапсихолог", "14:00"
            )
            assert r1["ok"] is True

            # Отменяем (имитация)
            bot_module._sheet_cache["аромапсихолог"] = [
                r for r in bot_module._sheet_cache["аромапсихолог"]
                if str(r.get("ID", "")) != str(uid)
            ]

        # Слот должен быть свободен
        assert len(bot_module._sheet_cache["аромапсихолог"]) == 0

    async def test_load_reschedule_parallel(self):
        """20 пользователей записаны, все одновременно переносят."""
        bot_module._sheet_cache = {"массаж": []}
        all_slots = get_slot_list("массаж")

        # Сначала записываем 20 пользователей (по 3 на слот)
        uid = 40000
        booked = []
        for slot in all_slots[:7]:
            for _ in range(3):
                r = await execute_booking(uid, f"@u{uid}", f"U{uid}", "массаж", slot)
                if r["ok"]:
                    booked.append((uid, slot))
                uid += 1

        initial_count = len(bot_module._sheet_cache["массаж"])

        # Все одновременно переносят на другие слоты
        new_slots = all_slots[10:10 + len(booked)]
        if len(new_slots) < len(booked):
            new_slots = (new_slots * (len(booked) // len(new_slots) + 1))[:len(booked)]

        tasks = [
            execute_booking(
                uid, f"@u{uid}", f"U{uid}", "массаж",
                new_slot, is_reschedule=True
            )
            for (uid, _), new_slot in zip(booked, new_slots)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        final_count = len(bot_module._sheet_cache["массаж"])

        # Кол-во записей не должно превышать начальное
        assert final_count <= initial_count, (
            f"После переноса записей стало больше: {final_count} > {initial_count}"
        )
        logger.info(
            f"Reschedule parallel: {ok_count}/{len(booked)} ok, "
            f"{initial_count} → {final_count} records"
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║  4. ПРОВЕРКА КОНСИСТЕНТНОСТИ КЭША                          ║
# ╚══════════════════════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestCacheConsistency:
    """Проверяем что кэш остаётся консистентным после нагрузки."""

    async def test_cache_records_match_ok_results(self):
        """Количество записей в кэше = количество ok-ответов."""
        bot_module._sheet_cache = {"макияж": []}
        all_slots = get_slot_list("макияж")

        tasks = [
            execute_booking(i, f"@u{i}", f"U{i}", "макияж", all_slots[i % len(all_slots)])
            for i in range(100)
        ]
        results = await asyncio.gather(*tasks)

        ok_count = sum(1 for r in results if r["ok"])
        cache_count = len(bot_module._sheet_cache["макияж"])
        assert ok_count == cache_count

    async def test_no_phantom_records(self):
        """Записи failed пользователей не попадают в кэш."""
        bot_module._sheet_cache = {"аромапсихолог": []}

        tasks = [
            execute_booking(i, f"@u{i}", f"U{i}", "аромапсихолог", "14:00")
            for i in range(50)
        ]
        results = await asyncio.gather(*tasks)

        ok_ids = set()
        for i, r in enumerate(results):
            if r["ok"]:
                ok_ids.add(i)

        cache_ids = {r["ID"] for r in bot_module._sheet_cache["аромапсихолог"]}
        assert cache_ids == ok_ids, f"Фантомные записи: {cache_ids - ok_ids}"

    async def test_get_all_user_bookings_consistent(self):
        """get_all_user_bookings возвращает ровно то что записано."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        await execute_booking(555, "@u", "U", "массаж", "11:00")
        await execute_booking(555, "@u", "U", "макияж", "10:00")
        await execute_booking(555, "@u", "U", "аромапсихолог", "14:00")

        bookings = get_all_user_bookings("555")
        events = sorted(b["event"] for b in bookings)
        assert events == sorted(["массаж", "макияж", "аромапсихолог"])

    async def test_suggested_slots_reflect_capacity(self):
        """get_suggested_slots правильно отражает оставшуюся capacity после нагрузки."""
        bot_module._sheet_cache = {"массаж": []}

        # Забиваем 2 из 3 мастеров на 11:00
        await execute_booking(1, "@a", "A", "массаж", "11:00")
        await execute_booking(2, "@b", "B", "массаж", "11:00")

        suggested = get_suggested_slots("массаж", bot_module._sheet_cache["массаж"])
        slot_11 = next((s for s in suggested if s[0] == "11:00"), None)

        if slot_11:
            assert slot_11[1] == 1, f"Должен остаться 1 мастер, показано {slot_11[1]}"

    async def test_available_slots_empty_after_full_load(self):
        """После полной загрузки available_slots пуст."""
        event = "аромапсихолог"
        bot_module._sheet_cache = {event: []}
        all_slots = get_slot_list(event)

        uid = 50000
        for slot in all_slots:
            await execute_booking(uid, f"@u{uid}", f"U{uid}", event, slot)
            uid += 1

        avail = get_available_slots(event, bot_module._sheet_cache[event])
        assert len(avail) == 0, f"Есть свободные слоты после заполнения: {avail}"

        suggested = get_suggested_slots(event, bot_module._sheet_cache[event])
        assert len(suggested) == 0