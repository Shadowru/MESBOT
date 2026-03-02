# tests/test_bot.py

import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime, timedelta

import sys
import os


@pytest.fixture(autouse=True)
def _patch_externals(monkeypatch):
    """Мокаем все внешние вызовы на уровне модуля до каждого теста."""
    mock_worksheet = MagicMock()
    mock_worksheet.get_all_records.return_value = []
    mock_worksheet.col_values.return_value = []
    mock_worksheet.append_row.return_value = None
    mock_worksheet.delete_rows.return_value = None

    mock_sheet = MagicMock()
    mock_sheet.worksheet.return_value = mock_worksheet

    import bot as bot_module
    monkeypatch.setattr(bot_module, "sheet", mock_sheet)

    mock_scheduler = MagicMock()
    mock_scheduler.get_job.return_value = None
    monkeypatch.setattr(bot_module, "scheduler", mock_scheduler)

    mock_bot = AsyncMock()
    monkeypatch.setattr(bot_module, "bot", mock_bot)

    bot_module._sheet_cache.clear()
    bot_module._booking_locks.clear()
    bot_module._user_locks.clear()

    yield {
        "worksheet": mock_worksheet,
        "sheet": mock_sheet,
        "scheduler": mock_scheduler,
        "bot": mock_bot,
    }


import bot as bot_module
from bot import (
    ef,
    plural_masters,
    plural_places,
    _resolve_event,
    get_slot_list,
    is_valid_slot_time,
    find_available_master,
    count_available_masters,
    get_suggested_slots,
    get_available_slots,
    format_slots_message,
    get_all_user_bookings,
    check_time_conflict,
    build_program_message,
    build_slot_keyboard,
    build_services_keyboard,
    build_service_card,
    execute_booking,
    send_program,
    EVENTS_CONFIG,
    MASTERS_CONFIG,
    EVENT_ALIASES,
    SERVICE_DESCRIPTIONS,
    BookingState,
)


# ╔══════════════════════════════════════════════╗
# ║  1. ТЕКСТОВЫЕ ХЕЛПЕРЫ                       ║
# ╚══════════════════════════════════════════════╝


class TestEf:
    def test_title_form(self):
        assert ef("массаж") == "Массаж"
        assert ef("гадалки") == "Гадалки"
        assert ef("мастерская чехова") == "Мастерская Чехова"

    def test_to_form(self):
        assert ef("массаж", "to") == "на массаж"
        assert ef("гадалки", "to") == "к гадалке"
        assert ef("аромапсихолог", "to") == "к аромапсихологу"
        assert ef("мастерская чехова", "to") == "в Мастерскую Чехова"

    def test_at_form(self):
        assert ef("массаж", "at") == "на массаж"
        assert ef("нутрициолог", "at") == "у нутрициолога"

    def test_acc_form(self):
        assert ef("гадалки", "acc") == "гадалок"

    def test_unknown_event_fallback(self):
        assert ef("неизвестная_услуга") == "Неизвестная_услуга"
        assert ef("неизвестная_услуга", "to") == "Неизвестная_услуга"


class TestPluralMasters:
    def test_one_master(self):
        assert plural_masters(1) == "1 мастер"

    def test_two_masters(self):
        assert plural_masters(2) == "2 мастера"

    def test_five_masters(self):
        assert plural_masters(5) == "5 мастеров"

    def test_eleven(self):
        assert plural_masters(11) == "11 мастеров"

    def test_twenty_one(self):
        assert plural_masters(21) == "21 мастер"

    def test_gadалки_event(self):
        assert plural_masters(1, "гадалки") == "1 гадалка"
        assert plural_masters(2, "гадалки") == "2 гадалки"
        assert plural_masters(5, "гадалки") == "5 гадалок"

    def test_makiyazh_event(self):
        assert plural_masters(1, "макияж") == "1 визажист"
        assert plural_masters(3, "макияж") == "3 визажиста"
        assert plural_masters(7, "макияж") == "7 визажистов"


class TestPluralPlaces:
    def test_one(self):
        assert plural_places(1) == "1 место"

    def test_two(self):
        assert plural_places(2) == "2 места"

    def test_five(self):
        assert plural_places(5) == "5 мест"

    def test_eleven(self):
        assert plural_places(11) == "11 мест"

    def test_twenty_one(self):
        assert plural_places(21) == "21 место"

    def test_twenty_two(self):
        assert plural_places(22) == "22 места"


class TestResolveEvent:
    def test_direct_name(self):
        assert _resolve_event("массаж") == "массаж"
        assert _resolve_event("макияж") == "макияж"

    def test_alias(self):
        assert _resolve_event("гадалка") == "гадалки"
        assert _resolve_event("таро") == "гадалки"
        assert _resolve_event("мэйкап") == "макияж"
        assert _resolve_event("психолог") == "аромапсихолог"
        assert _resolve_event("чехова") == "мастерская чехова"

    def test_case_insensitive(self):
        assert _resolve_event("МАССАЖ") == "массаж"
        assert _resolve_event("Гадалка") == "гадалки"

    def test_with_spaces(self):
        assert _resolve_event("  массаж  ") == "массаж"

    def test_unknown(self):
        assert _resolve_event("пилатес") is None
        assert _resolve_event("") is None
        assert _resolve_event(None) is None


# ╔══════════════════════════════════════════════╗
# ║  2. ЛОГИКА СЛОТОВ                           ║
# ╚══════════════════════════════════════════════╝


class TestGetSlotList:
    def test_fixed_time_event(self):
        slots = get_slot_list("нутрициолог")
        assert slots == ["15:00"]

    def test_fixed_time_семейный(self):
        assert get_slot_list("семейный нутрициолог") == ["15:00"]

    def test_custom_slots(self):
        slots = get_slot_list("мастерская чехова")
        assert slots == ["11:00", "12:00", "14:00", "15:00", "16:00"]

    def test_generated_slots_massage(self):
        slots = get_slot_list("массаж")
        assert slots[0] == "11:00"
        assert slots[1] == "11:10"
        assert "17:00" in slots
        assert "17:10" not in slots

    def test_generated_slots_aroma(self):
        slots = get_slot_list("аромапсихолог")
        assert slots[0] == "14:00"
        assert slots[-1] == "16:50"
        assert len(slots) == 18

    def test_generated_slots_makiyazh(self):
        slots = get_slot_list("макияж")
        assert slots[0] == "10:00"
        assert slots[-1] == "11:50"
        assert len(slots) == 12


class TestIsValidSlotTime:
    def test_valid_fixed_time(self):
        ok, err = is_valid_slot_time("нутрициолог", "15:00")
        assert ok is True
        assert err is None

    def test_invalid_fixed_time(self):
        ok, err = is_valid_slot_time("нутрициолог", "16:00")
        assert ok is False
        assert "15:00" in err

    def test_valid_custom_slot(self):
        ok, err = is_valid_slot_time("мастерская чехова", "14:00")
        assert ok is True

    def test_invalid_custom_slot(self):
        ok, err = is_valid_slot_time("мастерская чехова", "13:00")
        assert ok is False
        assert "Доступные сеансы" in err

    def test_valid_generated_slot(self):
        ok, err = is_valid_slot_time("массаж", "11:00")
        assert ok is True

    def test_invalid_time_out_of_range_before(self):
        ok, err = is_valid_slot_time("массаж", "10:00")
        assert ok is False
        assert "Рабочие часы" in err

    def test_invalid_time_out_of_range_after(self):
        ok, err = is_valid_slot_time("массаж", "18:00")
        assert ok is False

    def test_invalid_time_not_on_grid(self):
        ok, err = is_valid_slot_time("массаж", "11:03")
        assert ok is False
        assert "Ближайшие слоты" in err

    def test_misaligned_slot_suggests_neighbors(self):
        ok, err = is_valid_slot_time("аромапсихолог", "14:05")
        assert ok is False
        assert "14:00" in err
        assert "14:10" in err


# ╔══════════════════════════════════════════════╗
# ║  3. ЛОГИКА МАСТЕРОВ                         ║
# ╚══════════════════════════════════════════════╝


class TestFindAvailableMaster:
    def test_no_masters_for_event(self):
        master, err = find_available_master("аромапсихолог", "14:00", [])
        assert master is None
        assert err is None

    def test_first_available_master_massage(self):
        master, err = find_available_master("массаж", "11:00", [])
        assert master is not None
        assert master["id"] == "Мастер №1 Виктор"
        assert err is None

    def test_skip_busy_master(self):
        busy = [{"Мастер/Детали": "Мастер №1 Виктор"}]
        master, err = find_available_master("массаж", "11:00", busy)
        assert master["id"] == "Мастер №2 Нарек"

    def test_all_busy(self):
        busy = [
            {"Мастер/Детали": "Мастер №1 Виктор"},
            {"Мастер/Детали": "Мастер №2 Нарек"},
            {"Мастер/Детали": "Мастер №3 Ольга"},
        ]
        master, err = find_available_master("массаж", "11:00", busy)
        assert master is None

    def test_master_on_break(self):
        master, err = find_available_master("массаж", "13:30", [])
        assert master is not None
        assert master["id"] != "Мастер №1 Виктор"

    def test_preferred_master_available(self):
        master, err = find_available_master("массаж", "11:00", [], "Ольга")
        assert master["id"] == "Мастер №3 Ольга"

    def test_preferred_master_busy(self):
        busy = [{"Мастер/Детали": "Мастер №3 Ольга"}]
        master, err = find_available_master("массаж", "11:00", busy, "Ольга")
        assert master is None
        assert "занят" in err

    def test_preferred_master_on_break(self):
        master, err = find_available_master("массаж", "13:30", [], "Виктор")
        assert master is None
        assert "перерыв" in err

    def test_gadалки_preferred(self):
        master, err = find_available_master("гадалки", "11:00", [], "Юлия")
        assert master["id"] == "Гадалка Юлия"

    def test_gadалки_both_busy(self):
        busy = [
            {"Мастер/Детали": "Гадалка Юлия"},
            {"Мастер/Детали": "Гадалка Натэлла"},
        ]
        master, err = find_available_master("гадалки", "11:00", busy)
        assert master is None

    def test_makiyazh_four_masters(self):
        master, err = find_available_master("макияж", "10:00", [])
        assert master is not None
        assert "Визажист" in master["id"]

    def test_makiyazh_all_four_busy(self):
        busy = [{"Мастер/Детали": f"Визажист №{i}"} for i in range(1, 5)]
        master, err = find_available_master("макияж", "10:00", busy)
        assert master is None


class TestCountAvailableMasters:
    def test_all_free(self):
        assert count_available_masters("массаж", "11:00", []) == 3

    def test_one_busy(self):
        busy = [{"Мастер/Детали": "Мастер №1 Виктор"}]
        assert count_available_masters("массаж", "11:00", busy) == 2

    def test_with_breaks(self):
        assert count_available_masters("массаж", "13:30", []) == 2

    def test_preferred_filter(self):
        assert count_available_masters("массаж", "11:00", [], "Виктор") == 1

    def test_preferred_on_break(self):
        assert count_available_masters("массаж", "13:30", [], "Виктор") == 0

    def test_no_masters_config(self):
        assert count_available_masters("аромапсихолог", "14:00", []) == 0

    def test_gadалки(self):
        assert count_available_masters("гадалки", "11:00", []) == 2

    def test_makiyazh(self):
        assert count_available_masters("макияж", "10:00", []) == 4


# ╔══════════════════════════════════════════════╗
# ║  4. ДОСТУПНОСТЬ И ФОРМАТИРОВАНИЕ            ║
# ╚══════════════════════════════════════════════╝


class TestGetSuggestedSlots:
    def test_empty_records(self):
        slots = get_suggested_slots("массаж", [])
        assert len(slots) > 0
        assert len(slots) <= 6
        for t, a in slots:
            assert a > 0

    def test_sorted_by_availability_desc(self):
        slots = get_suggested_slots("массаж", [])
        for i in range(len(slots) - 1):
            assert slots[i][1] >= slots[i + 1][1] or slots[i][0] <= slots[i + 1][0]

    def test_fully_booked_slot_excluded(self):
        records = [
            {"Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор", "ID": "1"},
            {"Время": "11:00", "Мастер/Детали": "Мастер №2 Нарек", "ID": "2"},
            {"Время": "11:00", "Мастер/Детали": "Мастер №3 Ольга", "ID": "3"},
        ]
        slots = get_suggested_slots("массаж", records)
        times = [t for t, _ in slots]
        assert "11:00" not in times

    def test_fixed_time_event(self):
        slots = get_suggested_slots("нутрициолог", [])
        assert len(slots) == 1
        assert slots[0][0] == "15:00"
        assert slots[0][1] == 30

    def test_fixed_time_full(self):
        records = [{"Время": "15:00", "ID": str(i)} for i in range(30)]
        slots = get_suggested_slots("нутрициолог", records)
        assert len(slots) == 0

    def test_top_n_limit(self):
        slots = get_suggested_slots("массаж", [], top_n=2)
        assert len(slots) <= 2

    def test_aroma_capacity_1(self):
        records = [{"Время": "14:00", "ID": "1"}]
        slots = get_suggested_slots("аромапсихолог", records)
        times = [t for t, _ in slots]
        assert "14:00" not in times


class TestGetAvailableSlots:
    def test_empty_returns_all(self):
        slots = get_available_slots("массаж", [])
        assert len(slots) > 0
        assert all("(" in s for s in slots)

    def test_gadалки_format(self):
        slots = get_available_slots("гадалки", [])
        assert any("гадал" in s for s in slots)

    def test_aroma_format(self):
        slots = get_available_slots("аромапсихолог", [])
        assert any("место" in s or "мест" in s for s in slots)


class TestFormatSlotsMessage:
    def test_empty(self):
        assert "не осталось" in format_slots_message([])

    def test_few(self):
        result = format_slots_message(["11:00 (3 мастера)", "11:10 (2 мастера)"])
        assert "11:00" in result
        assert "11:10" in result

    def test_many_truncated(self):
        slots = [f"{i}:00" for i in range(20)]
        result = format_slots_message(slots)
        assert "и другие" in result


# ╔══════════════════════════════════════════════╗
# ║  4b. RICH-КАРТОЧКА УСЛУГИ                   ║
# ╚══════════════════════════════════════════════╝


class TestBuildServiceCard:
    def test_contains_title(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        assert "Массаж" in card

    def test_contains_description(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        desc = SERVICE_DESCRIPTIONS["массаж"]
        assert desc in card

    def test_contains_time_range(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        assert "11:00" in card
        assert "17:10" in card

    def test_contains_duration(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        assert "10 мин" in card

    def test_shows_masters_count(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        assert "3 мастера" in card

    def test_shows_slots_availability(self):
        bot_module._sheet_cache = {"массаж": []}
        card = build_service_card("массаж")
        assert "Свободно" in card

    def test_fixed_time_shows_places(self):
        bot_module._sheet_cache = {"нутрициолог": []}
        card = build_service_card("нутрициолог")
        assert "30 мест" in card or "Осталось" in card

    def test_no_slots_shows_warning(self):
        all_slots = get_slot_list("аромапсихолог")
        records = [
            {"ID": i, "Время": s, "Мастер/Детали": "Записано"}
            for i, s in enumerate(all_slots)
        ]
        bot_module._sheet_cache = {"аромапсихолог": records}
        card = build_service_card("аромапсихолог")
        assert "нет" in card.lower() or "⛔" in card

    def test_all_events_have_descriptions(self):
        for event in EVENTS_CONFIG:
            assert event in SERVICE_DESCRIPTIONS, f"Нет описания для {event}"


# ╔══════════════════════════════════════════════╗
# ║  5. ЗАПИСИ ПОЛЬЗОВАТЕЛЯ И КОНФЛИКТЫ         ║
# ╚══════════════════════════════════════════════╝


class TestGetAllUserBookings:
    def test_no_bookings(self):
        bot_module._sheet_cache = {"массаж": [], "макияж": []}
        assert get_all_user_bookings("123") == []

    def test_one_booking(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
            "макияж": [],
        }
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 1
        assert bookings[0]["event"] == "массаж"
        assert bookings[0]["time"] == "11:00"

    def test_multiple_events(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
            "макияж": [{"ID": 123, "Время": "10:00", "Мастер/Детали": "V1"}],
            "гадалки": [],
        }
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 2
        events = {b["event"] for b in bookings}
        assert events == {"массаж", "макияж"}

    def test_ignores_other_users(self):
        bot_module._sheet_cache = {
            "массаж": [
                {"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"},
                {"ID": 456, "Время": "11:10", "Мастер/Детали": "M2"},
            ],
        }
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 1

    def test_duration_from_config(self):
        bot_module._sheet_cache = {
            "нутрициолог": [{"ID": 123, "Время": "15:00", "Мастер/Детали": "Записано"}],
        }
        bookings = get_all_user_bookings("123")
        assert bookings[0]["duration"] == 90


class TestCheckTimeConflict:
    def test_no_conflict_different_times(self):
        existing = [{"event": "макияж", "time": "10:00", "duration": 10}]
        conflict, _, _ = check_time_conflict("массаж", "11:00", existing)
        assert conflict is False

    def test_overlap_conflict(self):
        existing = [{"event": "макияж", "time": "10:00", "duration": 10}]
        conflict, ev, t = check_time_conflict("массаж", "10:05", existing)
        assert conflict is True
        assert ev == "макияж"

    def test_adjacent_no_conflict(self):
        existing = [{"event": "макияж", "time": "10:00", "duration": 10}]
        conflict, _, _ = check_time_conflict("массаж", "10:10", existing)
        assert conflict is False

    def test_long_event_conflict(self):
        existing = [{"event": "нутрициолог", "time": "15:00", "duration": 90}]
        conflict, _, _ = check_time_conflict("мастерская чехова", "16:00", existing)
        assert conflict is True

    def test_same_event_skipped(self):
        existing = [{"event": "массаж", "time": "11:00", "duration": 10}]
        conflict, _, _ = check_time_conflict("массаж", "11:00", existing)
        assert conflict is False

    def test_new_ends_before_existing_starts(self):
        existing = [{"event": "нутрициолог", "time": "15:00", "duration": 90}]
        conflict, _, _ = check_time_conflict("массаж", "14:00", existing)
        assert conflict is False

    def test_new_starts_after_existing_ends(self):
        existing = [{"event": "макияж", "time": "10:00", "duration": 10}]
        conflict, _, _ = check_time_conflict("массаж", "12:00", existing)
        assert conflict is False


# ╔══════════════════════════════════════════════╗
# ║  6. ПРОГРАММА ПОЛЬЗОВАТЕЛЯ                  ║
# ╚══════════════════════════════════════════════╝


class TestBuildProgramMessage:
    def test_no_bookings(self):
        bot_module._sheet_cache = {}
        assert build_program_message("123") is None

    def test_with_bookings(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
            "макияж": [{"ID": 123, "Время": "10:00", "Мастер/Детали": "Визажист №1"}],
        }
        text = build_program_message("123")
        assert text is not None
        assert "бьюти-программа" in text
        assert "11:00" in text
        assert "10:00" in text
        # UPDATED: новый формат прогресса «2 из 7»
        assert "2 из" in text

    def test_contains_progress_bar(self):
        """Новый формат с прогресс-баром ●○."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        text = build_program_message("123")
        assert "●" in text
        assert "○" in text

    def test_sorted_by_time(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "12:00", "Мастер/Детали": "M1"}],
            "макияж": [{"ID": 123, "Время": "10:00", "Мастер/Детали": "V1"}],
        }
        text = build_program_message("123")
        assert text.index("10:00") < text.index("12:00")

    def test_master_location_shown(self):
        bot_module._sheet_cache = {
            "гадалки": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Гадалка Юлия"}],
        }
        text = build_program_message("123")
        assert "Юлия" in text
        assert "614а" in text

    def test_nutricionist_location(self):
        bot_module._sheet_cache = {
            "нутрициолог": [{"ID": 123, "Время": "15:00", "Мастер/Детали": "Записано"}],
        }
        text = build_program_message("123")
        assert "5 этаж" in text


# ╔══════════════════════════════════════════════╗
# ║  7. КЛАВИАТУРЫ                              ║
# ╚══════════════════════════════════════════════╝


class TestBuildSlotKeyboard:
    def test_basic(self):
        suggested = [("11:00", 3), ("11:10", 2)]
        kb = build_slot_keyboard("массаж", suggested, "book")
        # UPDATED: +1 для кнопки «← Назад к услугам»
        assert len(kb.inline_keyboard) == 3
        assert "11:00" in kb.inline_keyboard[0][0].text
        assert kb.inline_keyboard[0][0].callback_data == "slot|массаж|11:00|book"

    def test_has_back_button(self):
        """Новая кнопка «← Назад к услугам»."""
        suggested = [("11:00", 3)]
        kb = build_slot_keyboard("массаж", suggested, "book")
        last_row = kb.inline_keyboard[-1]
        assert last_row[0].callback_data == "back_to_services"
        assert "Назад" in last_row[0].text

    def test_reschedule_action(self):
        suggested = [("11:00", 1)]
        kb = build_slot_keyboard("массаж", suggested, "reschedule")
        assert "reschedule" in kb.inline_keyboard[0][0].callback_data

    def test_masters_label(self):
        suggested = [("11:00", 2)]
        kb = build_slot_keyboard("массаж", suggested)
        assert "мастер" in kb.inline_keyboard[0][0].text

    def test_places_label(self):
        suggested = [("14:00", 1)]
        kb = build_slot_keyboard("аромапсихолог", suggested)
        assert "место" in kb.inline_keyboard[0][0].text

    def test_slot_label_format(self):
        """Новый формат: «🕐 11:00  ·  3 мастера»."""
        suggested = [("11:00", 3)]
        kb = build_slot_keyboard("массаж", suggested)
        text = kb.inline_keyboard[0][0].text
        assert "🕐" in text
        assert "·" in text


class TestBuildServicesKeyboard:
    def test_has_all_events(self):
        """Без user_id — все мероприятия видны."""
        kb = build_services_keyboard()
        assert len(kb.inline_keyboard) == len(EVENTS_CONFIG)

    def test_no_user_id_all_bookable(self):
        """Без user_id — все кнопки start_book (нет ✅/⛔)."""
        kb = build_services_keyboard()
        for row in kb.inline_keyboard:
            cb = row[0].callback_data
            assert cb.startswith("start_book|") or cb.startswith("no_slots|")

    def test_already_booked_shows_checkmark(self):
        """Если пользователь записан — кнопка показывает ✅."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        kb = build_services_keyboard(user_id="123")
        massage_row = None
        for row in kb.inline_keyboard:
            if "массаж" in row[0].callback_data.lower() or "Массаж" in row[0].text:
                massage_row = row
                break
        assert massage_row is not None
        assert "✅" in massage_row[0].text
        assert massage_row[0].callback_data.startswith("my_booking_detail|")

    def test_no_slots_shows_blocked(self):
        """Если мест нет — кнопка показывает ⛔."""
        all_slots = get_slot_list("аромапсихолог")
        records = [
            {"ID": i, "Время": s, "Мастер/Детали": "Записано"}
            for i, s in enumerate(all_slots)
        ]
        bot_module._sheet_cache = {"аромапсихолог": records}
        kb = build_services_keyboard(user_id="999")
        aroma_row = None
        for row in kb.inline_keyboard:
            if "аромапсихолог" in row[0].callback_data or "Аромапсихолог" in row[0].text:
                aroma_row = row
                break
        assert aroma_row is not None
        assert "⛔" in aroma_row[0].text
        assert aroma_row[0].callback_data.startswith("no_slots|")

    def test_available_shows_active_button(self):
        """Если есть места и не записан — обычная кнопка start_book."""
        bot_module._sheet_cache = {"массаж": []}
        kb = build_services_keyboard(user_id="999")
        massage_row = None
        for row in kb.inline_keyboard:
            if "start_book|массаж" in row[0].callback_data:
                massage_row = row
                break
        assert massage_row is not None
        assert "✅" not in massage_row[0].text
        assert "⛔" not in massage_row[0].text


# ╔══════════════════════════════════════════════╗
# ║  8. ЯДРО ЗАПИСИ (execute_booking)           ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestExecuteBooking:
    async def test_successful_booking_simple(self):
        """Запись на аромапсихолога (без мастеров)."""
        bot_module._sheet_cache = {"аромапсихолог": []}
        res = await execute_booking(123, "@user", "Test User", "аромапсихолог", "14:00")
        assert res["ok"] is True
        assert "14:00" in res["text"]
        assert len(bot_module._sheet_cache["аромапсихолог"]) == 1

    async def test_success_text_contains_checkmark(self):
        """Новый формат: ✅ Записано! вместо «успешно записаны»."""
        bot_module._sheet_cache = {"аромапсихолог": []}
        res = await execute_booking(123, "@user", "Test User", "аромапсихолог", "14:00")
        assert "✅" in res["text"]
        assert "Записано" in res["text"]

    async def test_success_shows_time_range(self):
        """Новый формат показывает диапазон: 14:00 — 14:10."""
        bot_module._sheet_cache = {"аромапсихолог": []}
        res = await execute_booking(123, "@user", "Test User", "аромапсихолог", "14:00")
        assert "14:10" in res["text"]  # end time

    async def test_success_shows_reminder_note(self):
        """Подтверждение содержит напоминание о нотификации."""
        bot_module._sheet_cache = {"аромапсихолог": []}
        res = await execute_booking(123, "@user", "Test User", "аромапсихолог", "14:00")
        assert "Напомню" in res["text"] or "🔔" in res["text"]

    async def test_successful_booking_with_master(self):
        """Запись на массаж — должен назначить мастера."""
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@user", "Test User", "массаж", "11:00")
        assert res["ok"] is True
        assert "Виктор" in res["text"] or "Мастер" in res["text"]

    async def test_invalid_time_format(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@user", "Test", "массаж", "abc")
        assert res["ok"] is False
        assert "формат" in res["text"].lower() or "Неверный" in res["text"]

    async def test_invalid_slot_time(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@user", "Test", "массаж", "11:03")
        assert res["ok"] is False
        assert "Ближайшие" in res["text"]

    async def test_out_of_hours(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@user", "Test", "массаж", "08:00")
        assert res["ok"] is False

    async def test_double_booking_rejected(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
        }
        res = await execute_booking(123, "@user", "Test", "массаж", "12:00")
        assert res["ok"] is False
        assert "уже записаны" in res["text"]

    async def test_double_booking_suggests_reschedule(self):
        """Новый UX: при дубле подсказывает команду переноса."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        res = await execute_booking(123, "@user", "Test", "массаж", "12:00")
        assert res["ok"] is False
        assert "перенеси" in res["text"].lower()

    async def test_capacity_full_simple_event(self):
        """Аромапсихолог: capacity=1, уже занято."""
        bot_module._sheet_cache = {
            "аромапсихолог": [{"ID": 999, "Время": "14:00", "Мастер/Детали": "Записано"}],
        }
        res = await execute_booking(123, "@user", "Test", "аромапсихолог", "14:00")
        assert res["ok"] is False
        assert "занято" in res["text"]

    async def test_all_masters_busy(self):
        bot_module._sheet_cache = {
            "массаж": [
                {"ID": 1, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"},
                {"ID": 2, "Время": "11:00", "Мастер/Детали": "Мастер №2 Нарек"},
                {"ID": 3, "Время": "11:00", "Мастер/Детали": "Мастер №3 Ольга"},
            ],
        }
        res = await execute_booking(123, "@user", "Test", "массаж", "11:00")
        assert res["ok"] is False
        assert "заняты" in res["text"]

    async def test_preferred_master(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(
            123, "@user", "Test", "массаж", "11:00", preferred_master="Ольга"
        )
        assert res["ok"] is True
        assert "Ольга" in res["text"]

    async def test_preferred_master_busy(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 999, "Время": "11:00", "Мастер/Детали": "Мастер №3 Ольга"}],
        }
        res = await execute_booking(
            123, "@user", "Test", "массаж", "11:00", preferred_master="Ольга"
        )
        assert res["ok"] is False
        assert "занят" in res["text"]

    async def test_preferred_master_on_break(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(
            123, "@user", "Test", "массаж", "13:30", preferred_master="Виктор"
        )
        assert res["ok"] is False
        assert "перерыв" in res["text"]

    async def test_time_conflict_with_another_event(self):
        bot_module._sheet_cache = {
            "массаж": [],
            "макияж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Визажист №1"}],
        }
        res = await execute_booking(123, "@user", "Test", "массаж", "11:00")
        assert res["ok"] is False
        # UPDATED: «накладочка» → «Накладка»
        assert "накладка" in res["text"].lower()

    async def test_no_conflict_adjacent_events(self):
        bot_module._sheet_cache = {
            "массаж": [],
            "макияж": [{"ID": 123, "Время": "10:50", "Мастер/Детали": "Визажист №1"}],
        }
        res = await execute_booking(123, "@user", "Test", "массаж", "11:00")
        assert res["ok"] is True

    async def test_reschedule_success(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
        }
        res = await execute_booking(
            123, "@user", "Test", "массаж", "12:00", is_reschedule=True
        )
        assert res["ok"] is True
        assert "12:00" in res["text"]
        records = bot_module._sheet_cache["массаж"]
        times = [str(r.get("Время", "")) for r in records if str(r.get("ID", "")) == "123"]
        assert "11:00" not in times
        assert "12:00" in times

    async def test_reschedule_text_format(self):
        """Перенос показывает «🔄 Перенесено»."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        res = await execute_booking(
            123, "@user", "Test", "массаж", "12:00", is_reschedule=True
        )
        assert "Перенесено" in res["text"]

    async def test_reschedule_no_existing_booking(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(
            123, "@user", "Test", "массаж", "12:00", is_reschedule=True
        )
        assert res["ok"] is False
        assert "нет записи" in res["text"]

    async def test_fixed_time_booking(self):
        bot_module._sheet_cache = {"нутрициолог": []}
        res = await execute_booking(123, "@user", "Test", "нутрициолог", "15:00")
        assert res["ok"] is True

    async def test_fixed_time_wrong_time(self):
        bot_module._sheet_cache = {"нутрициолог": []}
        res = await execute_booking(123, "@user", "Test", "нутрициолог", "16:00")
        assert res["ok"] is False

    async def test_nutricionist_capacity_30(self):
        bot_module._sheet_cache = {
            "нутрициолог": [
                {"ID": i, "Время": "15:00", "Мастер/Детали": "Записано"}
                for i in range(30)
            ],
        }
        res = await execute_booking(999, "@user", "Test", "нутрициолог", "15:00")
        assert res["ok"] is False

    async def test_custom_slots_valid(self):
        bot_module._sheet_cache = {"мастерская чехова": []}
        res = await execute_booking(123, "@user", "Test", "мастерская чехова", "14:00")
        assert res["ok"] is True

    async def test_custom_slots_invalid(self):
        bot_module._sheet_cache = {"мастерская чехова": []}
        res = await execute_booking(123, "@user", "Test", "мастерская чехова", "13:00")
        assert res["ok"] is False

    async def test_gadалки_with_location(self):
        bot_module._sheet_cache = {"гадалки": []}
        res = await execute_booking(123, "@user", "Test", "гадалки", "11:00")
        assert res["ok"] is True
        assert "Юлия" in res["text"] or "Гадалка" in res["text"]

    async def test_makiyazh_four_in_one_slot(self):
        bot_module._sheet_cache = {"макияж": []}
        for i in range(4):
            res = await execute_booking(100 + i, f"@u{i}", f"User {i}", "макияж", "10:00")
            assert res["ok"] is True
        res = await execute_booking(200, "@u5", "User 5", "макияж", "10:00")
        assert res["ok"] is False

    async def test_cache_mutation(self):
        bot_module._sheet_cache = {"аромапсихолог": []}
        res = await execute_booking(123, "@user", "Test", "аромапсихолог", "14:00")
        assert res["ok"] is True
        records = bot_module._sheet_cache["аромапсихолог"]
        assert len(records) == 1
        assert records[0]["ID"] == 123
        assert records[0]["Время"] == "14:00"


# ╔══════════════════════════════════════════════╗
# ║  9. send_program                             ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestSendProgram:
    async def test_no_bookings_nothing_sent(self):
        bot_module._sheet_cache = {}
        await send_program(123, "999")
        bot_module.bot.send_message.assert_not_called()

    async def test_with_bookings_sends_message(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
        }
        await send_program(123, "123")
        bot_module.bot.send_message.assert_called_once()
        call_args = bot_module.bot.send_message.call_args
        text = call_args.kwargs.get("text", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "программа" in text.lower()

    async def test_shows_remaining_services_buttons(self):
        """Если записан не на всё — показывает кнопки для оставшихся доступных."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
            "макияж": [],
            "гадалки": [],
            "аромапсихолог": [],
            "нутрициолог": [],
            "мастерская чехова": [],
            "семейный нутрициолог": [],
        }
        await send_program(999, "123")
        call_args = bot_module.bot.send_message.call_args
        kb = call_args.kwargs.get("reply_markup")
        assert kb is not None
        # 6 кнопок (все минус массаж), только доступные
        assert len(kb.inline_keyboard) == 6

    async def test_all_booked_congratulation(self):
        """Если записан на всё — поздравление без кнопок."""
        bot_module._sheet_cache = {
            ev: [{"ID": 123, "Время": get_slot_list(ev)[0], "Мастер/Детали": "X"}]
            for ev in EVENTS_CONFIG
        }
        await send_program(999, "123")
        call_args = bot_module.bot.send_message.call_args
        # FIX: text передаётся позиционно, не через kwargs
        text = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("text", "")
        assert "все активности" in text.lower() or "🎉" in text
        kb = call_args.kwargs.get("reply_markup")
        assert kb is None

# tests/test_bot.py
# Заменить test_remaining_full_shows_hint в классе TestSendProgram:

    async def test_remaining_full_shows_hint(self):
        """Если не записан, но мест нет — подсказка «попробуйте позже»."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}
        bot_module._sheet_cache["массаж"] = [
            {"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}
        ]
        uid = 500
        for ev in EVENTS_CONFIG:
            if ev == "массаж":
                continue
            cfg = EVENTS_CONFIG[ev]
            for slot in get_slot_list(ev):
                # FIX: для событий с мастерами — заполняем реальными ID
                if ev in MASTERS_CONFIG:
                    for m in MASTERS_CONFIG[ev]:
                        if slot not in m.get("breaks", []):
                            bot_module._sheet_cache[ev].append(
                                {"ID": uid, "Время": slot, "Мастер/Детали": m["id"]}
                            )
                            uid += 1
                else:
                    for _ in range(cfg["capacity"]):
                        bot_module._sheet_cache[ev].append(
                            {"ID": uid, "Время": slot, "Мастер/Детали": "Записано"}
                        )
                        uid += 1

        await send_program(999, "123")
        call_args = bot_module.bot.send_message.call_args
        text = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("text", "")
        assert "позже" in text.lower() or "нет" in text.lower()
        
# ╔══════════════════════════════════════════════╗
# ║  10. NLP (parse_intent)                      ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestParseIntent:
    async def test_returns_dict_on_success(self):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"action":"book","event":"массаж","time":"11:00","preferred_master":""}'))
        ]
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, return_value=mock_response):
            result = await bot_module.parse_intent("запиши на массаж в 11:00")
        assert result["action"] == "book"
        assert result["event"] == "массаж"
        assert result["time"] == "11:00"

    async def test_handles_markdown_wrapped_json(self):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='```json\n{"action":"book","event":"макияж","time":"10:30","preferred_master":""}\n```'))
        ]
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, return_value=mock_response):
            result = await bot_module.parse_intent("хочу на макияж")
        assert result is not None
        assert result["action"] == "book"

    async def test_returns_none_on_exception(self):
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, side_effect=Exception("API error")):
            result = await bot_module.parse_intent("привет")
        assert result is None

    async def test_returns_none_on_invalid_json(self):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="не JSON"))
        ]
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, return_value=mock_response):
            result = await bot_module.parse_intent("бла бла")
        assert result is None

    async def test_cancel_intent(self):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"action":"cancel","event":"массаж","time":"","preferred_master":""}'))
        ]
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, return_value=mock_response):
            result = await bot_module.parse_intent("отмени массаж")
        assert result["action"] == "cancel"

    async def test_my_bookings_intent(self):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"action":"my_bookings","event":"","time":"","preferred_master":""}'))
        ]
        with patch.object(bot_module.llm_client.chat.completions, "create", new_callable=AsyncMock, return_value=mock_response):
            result = await bot_module.parse_intent("моя программа")
        assert result["action"] == "my_bookings"


# ╔══════════════════════════════════════════════╗
# ║  11. КОНКУРЕНТНОСТЬ                          ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestConcurrency:
    async def test_concurrent_bookings_no_overbooking(self):
        bot_module._sheet_cache = {"аромапсихолог": []}
        results = await asyncio.gather(
            execute_booking(100, "@a", "A", "аромапсихолог", "14:00"),
            execute_booking(200, "@b", "B", "аромапсихолог", "14:00"),
        )
        successes = [r for r in results if r["ok"]]
        failures = [r for r in results if not r["ok"]]
        assert len(successes) == 1
        assert len(failures) == 1

    async def test_concurrent_masters_no_double_assign(self):
        bot_module._sheet_cache = {"массаж": []}
        results = await asyncio.gather(
            execute_booking(100, "@a", "A", "массаж", "11:00"),
            execute_booking(200, "@b", "B", "массаж", "11:00"),
            execute_booking(300, "@c", "C", "массаж", "11:00"),
        )
        successes = [r for r in results if r["ok"]]
        assert len(successes) == 3
        res = await execute_booking(400, "@d", "D", "массаж", "11:00")
        assert res["ok"] is False

    async def test_concurrent_masters_unique_assignment(self):
        bot_module._sheet_cache = {"массаж": []}
        await asyncio.gather(
            execute_booking(100, "@a", "A", "массаж", "11:00"),
            execute_booking(200, "@b", "B", "массаж", "11:00"),
            execute_booking(300, "@c", "C", "массаж", "11:00"),
        )
        masters = [r["Мастер/Детали"] for r in bot_module._sheet_cache["массаж"]]
        assert len(set(masters)) == 3


# ╔══════════════════════════════════════════════╗
# ║  12. EDGE CASES                              ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestEdgeCases:
    async def test_booking_boundary_start_time(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@u", "U", "массаж", "11:00")
        assert res["ok"] is True

    async def test_booking_boundary_last_slot(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@u", "U", "массаж", "17:00")
        assert res["ok"] is True

    async def test_booking_at_end_time_rejected(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(123, "@u", "U", "массаж", "17:10")
        assert res["ok"] is False

    async def test_reschedule_then_book_old_slot(self):
        bot_module._sheet_cache = {
            "аромапсихолог": [{"ID": 100, "Время": "14:00", "Мастер/Детали": "Записано"}],
        }
        res = await execute_booking(100, "@a", "A", "аромапсихолог", "14:10", is_reschedule=True)
        assert res["ok"] is True
        res = await execute_booking(200, "@b", "B", "аромапсихолог", "14:00")
        assert res["ok"] is True

    async def test_all_breaks_covered_massage(self):
        bot_module._sheet_cache = {"массаж": []}
        res = await execute_booking(100, "@u", "U", "массаж", "13:30")
        assert res["ok"] is True
        rec = bot_module._sheet_cache["массаж"][-1]
        assert rec["Мастер/Детали"] != "Мастер №1 Виктор"

    async def test_chekhov_all_custom_slots(self):
        for slot in ["11:00", "12:00", "14:00", "15:00", "16:00"]:
            ok, err = is_valid_slot_time("мастерская чехова", slot)
            assert ok is True, f"Slot {slot} should be valid, got error: {err}"

    async def test_chekhov_invalid_slot(self):
        ok, err = is_valid_slot_time("мастерская чехова", "13:00")
        assert ok is False

    async def test_string_id_matching(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 1

        bot_module._sheet_cache = {
            "массаж": [{"ID": "123", "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 1

    async def test_multiple_bookings_different_events(self):
        bot_module._sheet_cache = {
            "массаж": [],
            "макияж": [],
            "аромапсихолог": [],
        }
        r1 = await execute_booking(123, "@u", "U", "массаж", "11:00")
        assert r1["ok"] is True
        r2 = await execute_booking(123, "@u", "U", "макияж", "10:00")
        assert r2["ok"] is True
        r3 = await execute_booking(123, "@u", "U", "аромапсихолог", "14:00")
        assert r3["ok"] is True
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 3


# ╔══════════════════════════════════════════════╗
# ║  13. HANDLERS (INTEGRATION-STYLE)           ║
# ╚══════════════════════════════════════════════╝


def _make_message(text, user_id=123, username="testuser", full_name="Test User"):
    msg = AsyncMock()
    msg.text = text
    msg.chat = MagicMock(id=user_id)
    msg.from_user = MagicMock(id=user_id, username=username, full_name=full_name)
    msg.reply = AsyncMock()
    return msg


def _make_callback(data, user_id=123, username="testuser", full_name="Test User"):
    cb = AsyncMock()
    cb.data = data
    cb.from_user = MagicMock(id=user_id, username=username, full_name=full_name)
    cb.message = AsyncMock()
    cb.message.chat = MagicMock(id=user_id)
    cb.message.edit_text = AsyncMock()
    cb.answer = AsyncMock()
    return cb


def _make_state(data=None, state_val=None):
    st = AsyncMock()
    st.get_data = AsyncMock(return_value=data or {})
    st.get_state = AsyncMock(return_value=state_val)
    st.set_state = AsyncMock()
    st.update_data = AsyncMock()
    st.clear = AsyncMock()
    return st


@pytest.mark.asyncio
class TestCmdStart:
    async def test_clears_state_and_replies(self):
        msg = _make_message("/start")
        state = _make_state()
        await bot_module.cmd_start(msg, state)
        state.clear.assert_called_once()
        msg.reply.assert_called_once()
        call_text = msg.reply.call_args[0][0]
        # UPDATED: новый WELCOME_TEXT
        assert "добро пожаловать" in call_text.lower()

    async def test_includes_services_keyboard(self):
        """Клавиатура передаётся в /start."""
        msg = _make_message("/start")
        state = _make_state()
        await bot_module.cmd_start(msg, state)
        kb = msg.reply.call_args.kwargs.get("reply_markup")
        assert kb is not None


@pytest.mark.asyncio
class TestHandleBookingHandler:
    async def test_my_bookings_no_records(self):
        bot_module._sheet_cache = {}
        msg = _make_message("моя программа")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"my_bookings","event":"","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called_once()
        assert "нет записей" in msg.reply.call_args[0][0].lower()

    async def test_book_with_time(self):
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("запиши на массаж в 11:00")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"book","event":"массаж","time":"11:00","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called()
        first_reply = msg.reply.call_args_list[0][0][0]
        # UPDATED: «✅ Записано» вместо «успешно записаны»
        assert "записано" in first_reply.lower() or "записан" in first_reply.lower()

    async def test_book_without_time_shows_slots(self):
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("хочу на массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"book","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        state.set_state.assert_called_with(BookingState.waiting_for_time)
        msg.reply.assert_called_once()
        assert msg.reply.call_args.kwargs.get("reply_markup") is not None

    async def test_book_without_time_shows_service_card(self):
        """При выборе слота отображается rich-карточка услуги."""
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("хочу на массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"book","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        reply_text = msg.reply.call_args[0][0]
        assert "Массаж" in reply_text
        assert "Выберите" in reply_text or "время" in reply_text.lower()

    async def test_cancel_existing(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        msg = _make_message("отмени массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"cancel","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called()
        assert "отменена" in msg.reply.call_args_list[0][0][0].lower()
        assert len(bot_module._sheet_cache["массаж"]) == 0

    async def test_cancel_nonexistent(self):
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("отмени массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"cancel","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called_once()
        assert "нет записи" in msg.reply.call_args[0][0].lower()

    async def test_waiting_for_time_receives_time(self):
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("11:30")
        state = _make_state(
            data={"action": "book", "event": "массаж", "preferred_master": None},
            state_val=BookingState.waiting_for_time.state,
        )

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"","event":"","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        state.clear.assert_called()
        msg.reply.assert_called()

    async def test_waiting_for_time_cancel(self):
        msg = _make_message("отмена")
        state = _make_state(
            data={"action": "book", "event": "массаж"},
            state_val=BookingState.waiting_for_time.state,
        )

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(content='{}'))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        state.clear.assert_called()
        assert "отменено" in msg.reply.call_args[0][0].lower()

    async def test_unknown_text_shows_help(self):
        msg = _make_message("фывапролд")
        state = _make_state()

        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, side_effect=Exception("err")):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called_once()
        assert msg.reply.call_args.kwargs.get("reply_markup") is not None

    async def test_availability_action(self):
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("какие окошки на массаж?")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"availability","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called()
        call_text = msg.reply.call_args_list[0][0][0]
        # UPDATED: теперь показывается карточка с «Свободно» и «Записаться?»
        assert "свободно" in call_text.lower() or "записаться" in call_text.lower()

    async def test_info_action_with_service_card(self):
        """Info показывает rich-карточку услуги."""
        bot_module._sheet_cache = {"массаж": []}
        msg = _make_message("расскажи про массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"info","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        call_text = msg.reply.call_args[0][0]
        assert "Массаж" in call_text
        assert SERVICE_DESCRIPTIONS["массаж"] in call_text

    async def test_info_already_booked_shows_manage_buttons(self):
        """Если записан — инфо показывает кнопки Перенести/Отменить."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        msg = _make_message("расскажи про массаж")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"info","event":"массаж","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        call_text = msg.reply.call_args[0][0]
        assert "уже записаны" in call_text.lower() or "✅" in call_text
        kb = msg.reply.call_args.kwargs.get("reply_markup")
        assert kb is not None
        # Должны быть кнопки Перенести и Отменить
        all_cb = [btn.callback_data for row in kb.inline_keyboard for btn in row]
        assert any("start_reschedule" in cb for cb in all_cb)
        assert any("confirm_cancel" in cb for cb in all_cb)

    async def test_fixed_time_no_slot_selection(self):
        bot_module._sheet_cache = {"нутрициолог": []}
        msg = _make_message("запиши к нутрициологу")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"book","event":"нутрициолог","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called()
        first_reply = msg.reply.call_args_list[0][0][0]
        assert "15:00" in first_reply
        assert "записано" in first_reply.lower() or "записан" in first_reply.lower()

    async def test_unknown_event_shows_services(self):
        msg = _make_message("запиши на пилатес")
        state = _make_state()

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock(message=MagicMock(
            content='{"action":"book","event":"пилатес","time":"","preferred_master":""}'
        ))]
        with patch.object(bot_module.llm_client.chat.completions, "create",
                          new_callable=AsyncMock, return_value=mock_resp):
            await bot_module.handle_booking(msg, state)

        msg.reply.assert_called()
        assert msg.reply.call_args.kwargs.get("reply_markup") is not None


# ╔══════════════════════════════════════════════╗
# ║  14. CALLBACK HANDLERS                      ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestProcessSlot:
    async def test_successful_slot_booking(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("slot|массаж|11:00|book")
        state = _make_state(data={})

        await bot_module.process_slot(cb, state)

        cb.answer.assert_called_once()
        state.clear.assert_called_once()
        assert cb.message.edit_text.call_count >= 2
        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "записано" in last_text.lower() or "записан" in last_text.lower()

    async def test_slot_reschedule(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        cb = _make_callback("slot|массаж|12:00|reschedule")
        state = _make_state(data={"preferred_master": None})

        await bot_module.process_slot(cb, state)

        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "12:00" in last_text
        assert "Перенесено" in last_text

    async def test_slot_booking_failure(self):
        bot_module._sheet_cache = {
            "аромапсихолог": [{"ID": 999, "Время": "14:00", "Мастер/Детали": "Записано"}],
        }
        cb = _make_callback("slot|аромапсихолог|14:00|book")
        state = _make_state(data={})

        await bot_module.process_slot(cb, state)

        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "занято" in last_text.lower()

    async def test_slot_with_preferred_master(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("slot|массаж|11:00|book")
        state = _make_state(data={"preferred_master": "Ольга"})

        await bot_module.process_slot(cb, state)

        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "Ольга" in last_text

    async def test_slot_without_action_defaults_to_book(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("slot|массаж|11:00")
        state = _make_state(data={})

        await bot_module.process_slot(cb, state)

        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "записано" in last_text.lower() or "записан" in last_text.lower()


@pytest.mark.asyncio
class TestProcessStartBook:
    async def test_shows_slots_with_service_card(self):
        """Показывает rich-карточку + слоты."""
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("start_book|массаж")
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        cb.answer.assert_called_once()
        state.clear.assert_called()
        state.set_state.assert_called_with(BookingState.waiting_for_time)
        cb.message.edit_text.assert_called()
        # Текст содержит карточку
        call_text = cb.message.edit_text.call_args[0][0]
        assert "Массаж" in call_text
        kb = cb.message.edit_text.call_args.kwargs.get("reply_markup")
        assert kb is not None

    async def test_already_booked_shows_info(self):
        """Если уже записан — показывает инфо, не создаёт дубль."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        cb = _make_callback("start_book|массаж")
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "уже записаны" in text.lower()
        assert "11:00" in text

    async def test_fixed_time_books_immediately(self):
        bot_module._sheet_cache = {"нутрициолог": []}
        cb = _make_callback("start_book|нутрициолог")
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        state.clear.assert_called()
        state.set_state.assert_not_called()
        assert cb.message.edit_text.call_count >= 2
        last_text = cb.message.edit_text.call_args_list[-1][0][0]
        assert "15:00" in last_text

    async def test_fixed_time_full(self):
        bot_module._sheet_cache = {
            "нутрициолог": [
                {"ID": i, "Время": "15:00", "Мастер/Детали": "Записано"}
                for i in range(30)
            ],
        }
        cb = _make_callback("start_book|нутрициолог")
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "нет" in text.lower() or "мест" in text.lower()

    async def test_no_slots_available(self):
        all_slots = get_slot_list("аромапсихолог")
        records = [
            {"ID": i, "Время": s, "Мастер/Детали": "Записано"}
            for i, s in enumerate(all_slots)
        ]
        bot_module._sheet_cache = {"аромапсихолог": records}
        cb = _make_callback("start_book|аромапсихолог", user_id=999)
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "нет" in text.lower() or "мест" in text.lower() or "заняты" in text.lower()

    async def test_unknown_event(self):
        cb = _make_callback("start_book|пилатес")
        state = _make_state()

        await bot_module.process_start_book(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "не найдена" in text.lower()

    async def test_clears_previous_state(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("start_book|массаж")
        state = _make_state(
            data={"event": "макияж", "action": "book"},
            state_val=BookingState.waiting_for_time.state,
        )

        await bot_module.process_start_book(cb, state)

        state.clear.assert_called()


# ╔══════════════════════════════════════════════╗
# ║  14b. НОВЫЕ CALLBACK HANDLERS               ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestProcessBookingDetail:
    """Тест my_booking_detail — тап на ✅ показывает детали записи."""

    async def test_shows_booking_details(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Мастер №1 Виктор"}],
        }
        cb = _make_callback("my_booking_detail|массаж")
        state = _make_state()

        await bot_module.process_booking_detail(cb, state)

        cb.answer.assert_called_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "11:00" in text
        assert "Массаж" in text
        assert "Виктор" in text

    async def test_shows_manage_buttons(self):
        """Показывает кнопки Перенести и Отменить."""
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        cb = _make_callback("my_booking_detail|массаж")
        state = _make_state()

        await bot_module.process_booking_detail(cb, state)

        kb = cb.message.edit_text.call_args.kwargs.get("reply_markup")
        assert kb is not None
        all_cb = [btn.callback_data for row in kb.inline_keyboard for btn in row]
        assert any("start_reschedule|массаж" in c for c in all_cb)
        assert any("confirm_cancel|массаж" in c for c in all_cb)
        assert any("back_to_services" in c for c in all_cb)

    async def test_no_booking_found(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("my_booking_detail|массаж")
        state = _make_state()

        await bot_module.process_booking_detail(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "не найдена" in text.lower()

    async def test_shows_location_for_gadалки(self):
        bot_module._sheet_cache = {
            "гадалки": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "Гадалка Юлия"}],
        }
        cb = _make_callback("my_booking_detail|гадалки")
        state = _make_state()

        await bot_module.process_booking_detail(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "Юлия" in text
        assert "614а" in text


@pytest.mark.asyncio
class TestProcessConfirmCancel:
    """Тест confirm_cancel — отмена через кнопку."""

    async def test_cancels_booking(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        cb = _make_callback("confirm_cancel|массаж")
        state = _make_state()

        await bot_module.process_confirm_cancel(cb, state)

        cb.answer.assert_called_once()
        assert len(bot_module._sheet_cache["массаж"]) == 0
        text = cb.message.edit_text.call_args[0][0]
        assert "отменена" in text.lower()

    async def test_cancel_nonexistent(self):
        bot_module._sheet_cache = {"массаж": []}
        cb = _make_callback("confirm_cancel|массаж")
        state = _make_state()

        await bot_module.process_confirm_cancel(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "нет записи" in text.lower()


@pytest.mark.asyncio
class TestProcessStartReschedule:
    """Тест start_reschedule — перенос через кнопку."""

    async def test_shows_slots_for_reschedule(self):
        bot_module._sheet_cache = {
            "массаж": [{"ID": 123, "Время": "11:00", "Мастер/Детали": "M1"}],
        }
        cb = _make_callback("start_reschedule|массаж")
        state = _make_state()

        await bot_module.process_start_reschedule(cb, state)

        cb.answer.assert_called_once()
        state.set_state.assert_called_with(BookingState.waiting_for_time)
        text = cb.message.edit_text.call_args[0][0]
        assert "перенос" in text.lower()
        kb = cb.message.edit_text.call_args.kwargs.get("reply_markup")
        assert kb is not None
        # Кнопки содержат reschedule в callback_data
        slot_buttons = [
            btn for row in kb.inline_keyboard for btn in row
            if btn.callback_data.startswith("slot|")
        ]
        assert all("reschedule" in btn.callback_data for btn in slot_buttons)

    async def test_no_slots_for_reschedule(self):
        """Если нет свободных слотов — сообщение + кнопка Назад."""
        all_slots = get_slot_list("аромапсихолог")
        records = [
            {"ID": i, "Время": s, "Мастер/Детали": "Записано"}
            for i, s in enumerate(all_slots)
        ]
        bot_module._sheet_cache = {"аромапсихолог": records}
        cb = _make_callback("start_reschedule|аромапсихолог")
        state = _make_state()

        await bot_module.process_start_reschedule(cb, state)

        text = cb.message.edit_text.call_args[0][0]
        assert "нет" in text.lower()
        kb = cb.message.edit_text.call_args.kwargs.get("reply_markup")
        assert kb is not None
        assert any("back_to_services" in btn.callback_data
                    for row in kb.inline_keyboard for btn in row)


@pytest.mark.asyncio
class TestProcessBackToServices:
    """Тест back_to_services — навигация назад."""

    async def test_shows_services_keyboard(self):
        cb = _make_callback("back_to_services")
        state = _make_state()

        await bot_module.process_back_to_services(cb, state)

        cb.answer.assert_called_once()
        state.clear.assert_called()
        text = cb.message.edit_text.call_args[0][0]
        assert "услугу" in text.lower()
        kb = cb.message.edit_text.call_args.kwargs.get("reply_markup")
        assert kb is not None


@pytest.mark.asyncio
class TestProcessNoSlots:
    """Тест no_slots — popup alert вместо нового сообщения."""

    async def test_shows_alert(self):
        cb = _make_callback("no_slots|аромапсихолог")
        state = _make_state()

        await bot_module.process_no_slots(cb, state)

        cb.answer.assert_called_once()
        call_kwargs = cb.answer.call_args.kwargs
        assert call_kwargs.get("show_alert") is True
        assert "заняты" in cb.answer.call_args[0][0].lower()


# ╔══════════════════════════════════════════════╗
# ║  15. ПОЛНЫЕ СЦЕНАРИИ (END-TO-END STYLE)     ║
# ╚══════════════════════════════════════════════╝


@pytest.mark.asyncio
class TestFullScenarios:
    async def test_book_cancel_rebook(self):
        bot_module._sheet_cache = {"аромапсихолог": []}
        r1 = await execute_booking(123, "@u", "U", "аромапсихолог", "14:00")
        assert r1["ok"] is True

        r2 = await execute_booking(123, "@u", "U", "аромапсихолог", "14:10")
        assert r2["ok"] is False

        bot_module._sheet_cache["аромапсихолог"] = [
            r for r in bot_module._sheet_cache["аромапсихолог"]
            if str(r.get("ID", "")) != "123"
        ]

        r3 = await execute_booking(123, "@u", "U", "аромапсихолог", "14:20")
        assert r3["ok"] is True

    async def test_reschedule_flow(self):
        bot_module._sheet_cache = {"массаж": []}
        r1 = await execute_booking(123, "@u", "U", "массаж", "11:00")
        assert r1["ok"] is True

        r2 = await execute_booking(123, "@u", "U", "массаж", "12:00", is_reschedule=True)
        assert r2["ok"] is True
        assert "12:00" in r2["text"]

        bookings = get_all_user_bookings("123")
        assert all(b["time"] == "12:00" for b in bookings if b["event"] == "массаж")

    async def test_multi_event_day(self):
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        bookings_plan = [
            ("макияж", "10:00"),
            ("массаж", "11:00"),
            ("аромапсихолог", "14:00"),
            ("нутрициолог", "15:00"),
            ("мастерская чехова", "12:00"),
        ]
        for ev, t in bookings_plan:
            r = await execute_booking(123, "@u", "U", ev, t)
            assert r["ok"] is True

        all_bookings = get_all_user_bookings("123")
        assert len(all_bookings) == 5

    async def test_multi_event_with_conflict(self):
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        r1 = await execute_booking(123, "@u", "U", "нутрициолог", "15:00")
        assert r1["ok"] is True

        r2 = await execute_booking(123, "@u", "U", "мастерская чехова", "16:00")
        assert r2["ok"] is False
        # UPDATED: «накладочка» → «накладка»
        assert "накладка" in r2["text"].lower()

        r3 = await execute_booking(123, "@u", "U", "мастерская чехова", "11:00")
        assert r3["ok"] is True

    async def test_gadалки_two_users_same_time(self):
        bot_module._sheet_cache = {"гадалки": []}
        r1 = await execute_booking(100, "@a", "A", "гадалки", "11:00")
        assert r1["ok"] is True
        r2 = await execute_booking(200, "@b", "B", "гадалки", "11:00")
        assert r2["ok"] is True
        r3 = await execute_booking(300, "@c", "C", "гадалки", "11:00")
        assert r3["ok"] is False

    async def test_chekhov_capacity_10(self):
        bot_module._sheet_cache = {"мастерская чехова": []}
        for i in range(10):
            r = await execute_booking(100 + i, f"@u{i}", f"U{i}", "мастерская чехова", "14:00")
            assert r["ok"] is True
        r = await execute_booking(999, "@last", "Last", "мастерская чехова", "14:00")
        assert r["ok"] is False

    async def test_full_flow_with_callbacks(self):
        """E2E: start → выбрать услугу → выбрать слот → подтверждение → программа."""
        bot_module._sheet_cache = {ev: [] for ev in EVENTS_CONFIG}

        # 1. /start
        msg = _make_message("/start")
        state = _make_state()
        await bot_module.cmd_start(msg, state)
        kb = msg.reply.call_args.kwargs.get("reply_markup")
        assert kb is not None

        # 2. Тап на услугу
        cb = _make_callback("start_book|массаж")
        state = _make_state()
        await bot_module.process_start_book(cb, state)
        state.set_state.assert_called_with(BookingState.waiting_for_time)

        # 3. Тап на слот
        cb2 = _make_callback("slot|массаж|11:00|book")
        state2 = _make_state(data={"action": "book", "event": "массаж"})
        await bot_module.process_slot(cb2, state2)
        last_text = cb2.message.edit_text.call_args_list[-1][0][0]
        assert "записано" in last_text.lower() or "Записано" in last_text

        # 4. Проверяем запись
        bookings = get_all_user_bookings("123")
        assert len(bookings) == 1
        assert bookings[0]["event"] == "массаж"