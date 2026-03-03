import re
from aiogram import Router, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from services.booking_service import BookingService
from core.interfaces import ILLMService
from core.config import EVENTS_CONFIG, EVENT_ALIASES
from presentation.keyboards import build_services_keyboard, build_slot_keyboard
from presentation.formatters import build_service_card, build_program_message, ef

router = Router()

WELCOME_TEXT = (
    "✨ **Добро пожаловать!** ✨\n\n"
    "Я помогу составить идеальную бьюти-программу на сегодня.\n\n"
    "💬 **Просто напишите**, например:\n"
    "  › _«Запиши на массаж в 12:20»_\n"
    "  › _«Хочу в салон предчувствий на 15:00»_\n"
    "  › _«Перенеси макияж на 11:30»_\n"
    "  › _«Отмени массаж»_\n"
    "  › _«Отмени все»_\n"
    "  › _«Моя программа»_\n\n"
    "Или выберите услугу из списка 👇"
)

def _resolve_event(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.lower().strip()
    key = EVENT_ALIASES.get(key, key)
    return key if key in EVENTS_CONFIG else None

@router.message(CommandStart())
async def cmd_start(message: types.Message, booking_service: BookingService):
    user_id = str(message.from_user.id)
    kb = await build_services_keyboard(user_id, booking_service)
    await message.reply(WELCOME_TEXT, reply_markup=kb, parse_mode="Markdown")

@router.message()
async def handle_text(message: types.Message, llm: ILLMService, booking_service: BookingService):
    text_lower = message.text.lower().strip()
    user_id = str(message.from_user.id)

    # 1. Быстрые команды без LLM
# 1. Быстрые команды без LLM
    if text_lower in ["моя программа", "мои записи", "расписание", "программа"]:
        bookings = await booking_service.get_user_bookings(user_id)
        text = build_program_message(bookings)
        kb = await build_services_keyboard(user_id, booking_service) # Генерируем клавиатуру
        
        if text:
            await message.reply(text + "\n\n✨ **Доступные услуги для записи:**", reply_markup=kb, parse_mode="Markdown")
        else:
            await message.reply("У вас пока нет записей 😊\n\n✨ **Выберите услугу:**", reply_markup=kb, parse_mode="Markdown")
        return
    
    cancel_all_patterns = ["отмени все", "отмени всё", "отменить все", "отменить всё", "удали все", "удали всё"]
    if text_lower in cancel_all_patterns:
        res = await booking_service.cancel_all(user_id)
        return await message.reply(res, parse_mode="Markdown")

    # 2. Обработка через LLM
    processing_msg = await message.reply("⏳ Обрабатываю ваш запрос...")
    intent = await llm.parse_intent(message.text)

    if not intent or not intent.action:
        kb = await build_services_keyboard(user_id, booking_service)
        return await processing_msg.edit_text("Не совсем понял вас 🤔\n\n" + WELCOME_TEXT, reply_markup=kb, parse_mode="Markdown")

    action = intent.action
    event = _resolve_event(intent.event)
    time_str = intent.time

    # Обработка действий
    if action == "cancel_all":
        res = await booking_service.cancel_all(user_id)
        return await processing_msg.edit_text(res, parse_mode="Markdown")

    if action == "my_bookings":
        bookings = await booking_service.get_user_bookings(user_id)
        text = build_program_message(bookings)
        kb = await build_services_keyboard(user_id, booking_service) # Генерируем клавиатуру
        
        if text:
            return await processing_msg.edit_text(text + "\n\n✨ **Доступные услуги для записи:**", reply_markup=kb, parse_mode="Markdown")
        return await processing_msg.edit_text("У вас пока нет записей 😊\n\n✨ **Выберите услугу:**", reply_markup=kb, parse_mode="Markdown")

    if not event:
        kb = await build_services_keyboard(user_id, booking_service)
        return await processing_msg.edit_text("Уточните, о какой услуге речь? ✨\n\n👇 **Выберите:**", reply_markup=kb, parse_mode="Markdown")

    if action == "cancel":
        res = await booking_service.cancel_booking(user_id, event)
        return await processing_msg.edit_text(res, parse_mode="Markdown")

    if action == "info" or action == "availability":
        suggested = await booking_service.get_suggested_slots(event)
        card = build_service_card(event, suggested)
        if suggested:
            kb = build_slot_keyboard(event, suggested, "book")
            return await processing_msg.edit_text(card + "\n\n🕐 **Выберите время:**", reply_markup=kb, parse_mode="Markdown")
        return await processing_msg.edit_text(card + "\n\nК сожалению, мест нет 😔", parse_mode="Markdown")

    # Если action == "book" или "reschedule"
    if not time_str:
        # Если время не указано, показываем клавиатуру со слотами
        suggested = await booking_service.get_suggested_slots(event)
        if suggested:
            card = build_service_card(event, suggested)
            kb = build_slot_keyboard(event, suggested, action)
            return await processing_msg.edit_text(card + "\n\n🕐 **Выберите время:**", reply_markup=kb, parse_mode="Markdown")
        return await processing_msg.edit_text(f"Нет свободных окошек {ef(event, 'at')} 😔", parse_mode="Markdown")

# Очищаем строку со временем и проверяем, что это реальное время (цифры:цифры)
    time_str = time_str.strip() if time_str else ""
    is_valid_time = bool(re.match(r"^\d{1,2}:\d{2}$", time_str))

    # Если action == "book" или "reschedule"
    if not is_valid_time:
        # Если время не указано (или LLM вернула заглушку типа "HH:MM"), показываем слоты
        suggested = await booking_service.get_suggested_slots(event)
        if suggested:
            card = build_service_card(event, suggested)
            kb = build_slot_keyboard(event, suggested, action)
            return await processing_msg.edit_text(card + "\n\n🕐 **Выберите время:**", reply_markup=kb, parse_mode="Markdown")
        return await processing_msg.edit_text(f"Нет свободных окошек {ef(event, 'at')} 😔", parse_mode="Markdown")

    # Если время указано корректно, сразу записываем
    res = await booking_service.execute_booking(
        user_id=user_id,
        username=message.from_user.username or "",
        full_name=message.from_user.full_name,
        event=event,
        time_str=time_str,
        is_reschedule=(action == "reschedule")
    )
    await processing_msg.edit_text(res["text"], parse_mode="Markdown")


# --- Обработчики кнопок (Inline Callbacks) ---

@router.callback_query(F.data.startswith("start_book|"))
async def process_start_book(callback: types.CallbackQuery, booking_service: BookingService):
    event = callback.data.split("|")[1]
    user_id = str(callback.from_user.id)
    
    # Проверка, записан ли уже
    bookings = await booking_service.get_user_bookings(user_id)
    if any(b.event == event for b in bookings):
        return await callback.message.edit_text(f"Вы уже записаны на {ef(event)} ✅")

    suggested = await booking_service.get_suggested_slots(event)
    if not suggested:
        kb = await build_services_keyboard(user_id, booking_service)
        return await callback.message.edit_text("К сожалению, мест больше нет 😔", reply_markup=kb)
        
    card = build_service_card(event, suggested)
    await callback.message.edit_text(card + "\n\n🕐 **Выберите время:**", reply_markup=build_slot_keyboard(event, suggested))

@router.callback_query(F.data.startswith("slot|"))
async def process_slot(callback: types.CallbackQuery, booking_service: BookingService):
    parts = callback.data.split("|")
    event = parts[1]
    time_str = parts[2]
    action = parts[3] if len(parts) > 3 else "book"
    
    await callback.message.edit_text(f"⏳ Записываю {ef(event, 'to')} на {time_str}…")
    
    res = await booking_service.execute_booking(
        user_id=str(callback.from_user.id),
        username=callback.from_user.username or "",
        full_name=callback.from_user.full_name,
        event=event,
        time_str=time_str,
        is_reschedule=(action == "reschedule")
    )
    await callback.message.edit_text(res["text"], parse_mode="Markdown")

@router.callback_query(F.data == "back_to_services")
async def process_back_to_services(callback: types.CallbackQuery, booking_service: BookingService):
    user_id = str(callback.from_user.id)
    kb = await build_services_keyboard(user_id, booking_service)
    await callback.message.edit_text("✨ **Выберите услугу:**", reply_markup=kb, parse_mode="Markdown")

@router.callback_query(F.data.startswith("no_slots|"))
async def process_no_slots(callback: types.CallbackQuery):
    await callback.answer("К сожалению, все места заняты 😔 Попробуйте позже!", show_alert=True)
    
@router.callback_query(F.data.startswith("my_booking_detail|"))
async def process_my_booking_detail(callback: types.CallbackQuery, booking_service: BookingService):
    event = callback.data.split("|")[1]
    user_id = str(callback.from_user.id)
    
    # Ищем конкретную запись пользователя
    bookings = await booking_service.get_user_bookings(user_id)
    booking = next((b for b in bookings if b.event == event), None)
    
    if not booking:
        await callback.answer("Запись не найдена 😔", show_alert=True)
        kb = await build_services_keyboard(user_id, booking_service)
        return await callback.message.edit_text("✨ **Выберите услугу:**", reply_markup=kb, parse_mode="Markdown")
        
    # Формируем текст с деталями записи
    text = (
        f"✅ **Вы записаны на {ef(event)}**\n\n"
        f"🕐 **Время:** {booking.time}\n"
    )
    if booking.master_id and booking.master_id != "Записано":
        text += f"👤 **Специалист:** {booking.master_id}\n"
        
    # Клавиатура с кнопкой отмены и возврата назад
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"cancel_booking|{event}")],
        [InlineKeyboardButton(text="← Назад к услугам", callback_data="back_to_services")]
    ])
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")


@router.callback_query(F.data.startswith("cancel_booking|"))
async def process_cancel_booking_inline(callback: types.CallbackQuery, booking_service: BookingService):
    event = callback.data.split("|")[1]
    user_id = str(callback.from_user.id)
    
    # Отменяем запись
    res = await booking_service.cancel_booking(user_id, event)
    
    # Возвращаем главное меню с обновленными статусами
    kb = await build_services_keyboard(user_id, booking_service)
    await callback.message.edit_text(f"{res}\n\n✨ **Выберите услугу:**", reply_markup=kb, parse_mode="Markdown")    