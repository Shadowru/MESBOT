import re
from aiogram import Router, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from services.booking_service import BookingService
from core.interfaces import ILLMService
from core.config import EVENTS_CONFIG, EVENT_ALIASES
from core.config import MASTERS_CONFIG
from presentation.keyboards import build_services_keyboard, build_slot_keyboard, build_masters_keyboard
from presentation.formatters import build_service_card, build_program_message, ef
from presentation.keyboards import build_services_keyboard, build_slot_keyboard, get_main_menu_keyboard

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

async def handle_booking_result(callback: types.CallbackQuery, res: dict, event: str, time_str: str, master_id: str, action: str):
    # Если произошел конфликт по времени
    if not res.get("ok") and res.get("status") == "conflict":
        conflict_event = res["conflict_event"]
        # Если master_id None, превращаем в строку для callback_data
        m_id = master_id if master_id else "None"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Записаться всё равно", 
                                  callback_data=f"confirm_overlap|{event}|{time_str}|{m_id}|{action}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_services")]
        ])
        return await callback.message.edit_text(
            f"⚠️ **Внимание!** У вас уже есть запись на *{conflict_event}* в {time_str}.\n\n"
            "Вы уверены, что хотите записаться на эту активность тоже?", 
            reply_markup=kb, parse_mode="Markdown"
        )
    
    # Если всё ок или другая ошибка (например, "Мест нет")
    await callback.message.edit_text(res["text"], parse_mode="Markdown")

@router.message(CommandStart())
async def cmd_start(message: types.Message, booking_service: BookingService):
    user_id = str(message.from_user.id)
    kb = await build_services_keyboard(user_id, booking_service)
    
    # Отправляем приветствие с Inline-кнопками и Reply-клавиатуру
    await message.reply(
        WELCOME_TEXT, 
        reply_markup=kb, 
        parse_mode="Markdown"
    )
    # Отправляем сообщение с Reply-клавиатурой (она закрепится внизу)
    await message.answer("Используйте кнопки меню для навигации:", reply_markup=get_main_menu_keyboard())

@router.message(F.text == "Все услуги")
async def handle_all_activities(message: types.Message, booking_service: BookingService):
    user_id = str(message.from_user.id)
    kb = await build_services_keyboard(user_id, booking_service)
    await message.answer("✨ **Доступные услуги для записи:**", reply_markup=kb, parse_mode="Markdown")

@router.message()
async def handle_text(message: types.Message, llm: ILLMService, booking_service: BookingService):
    text_lower = message.text.lower().strip()
    user_id = str(message.from_user.id)

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

    # Проверяем, записан ли уже пользователь на эту услугу
    if action in ["book", "reschedule"]:
        bookings = await booking_service.get_user_bookings(user_id)
        existing_booking = next((b for b in bookings if b.event == event), None)

        # Если человек просто просит записать (book), но уже записан:
        if existing_booking and action == "book":
            text = (
                f"Вы уже записаны {ef(event, 'to')} ✅\n\n"
                f"🕐 **Ваше время:** {existing_booking.time}\n"
            )
            if existing_booking.master_id and existing_booking.master_id != "Записано":
                text += f"👤 **Специалист:** {existing_booking.master_id}\n"
            
            text += "\nЧто бы вы хотели сделать?"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Перенести время", callback_data=f"request_reschedule|{event}")],
                [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"cancel_booking|{event}")],
                [InlineKeyboardButton(text="← Назад к услугам", callback_data="back_to_services")]
            ])
            return await processing_msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")

    # Если action == "book" или "reschedule" (и мы прошли проверку выше)
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
        # 1. Если конфликт (пересечение времени)
    if not res.get("ok") and res.get("status") == "conflict":
        conflict_event = res["conflict_event"]
        # Здесь мы не можем использовать Inline-кнопки "Записаться всё равно", 
        # так как это обычное текстовое сообщение, а не callback.
        # Поэтому просто сообщаем о конфликте:
        return await processing_msg.edit_text(
            f"⚠️ **Внимание!** У вас уже есть запись на *{conflict_event}* в {time_str}.\n"
            "Пожалуйста, сначала отмените текущую запись или выберите другое время.",
            parse_mode="Markdown"
        )
        
    if not res.get("ok") and res.get("text") == "invalid_time":
        suggested = await booking_service.get_suggested_slots(event)
        if suggested:
            card = build_service_card(event, suggested)
            kb = build_slot_keyboard(event, suggested, action)
            return await processing_msg.edit_text(
                f"К сожалению, время {time_str} недоступно.\n\n" + card + "\n\n🕐 **Выберите доступное время:**", 
                reply_markup=kb, 
                parse_mode="Markdown"
            )
        return await processing_msg.edit_text(f"Время {time_str} недоступно, мест нет 😔", parse_mode="Markdown")

        # Если другая ошибка (например, "Все мастера заняты")
    
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
    
    if event == "салон предчувствий":
        records = await booking_service.repo.get_records(event)
        at_time = [r for r in records if r.time == time_str]
        busy_ids = [r.master_id for r in at_time]
        
        # Получаем список доступных мастеров
        available = [m for m in MASTERS_CONFIG[event] if m["id"] not in busy_ids]
        
        if not available:
            return await callback.answer("Все специалисты заняты на это время.", show_alert=True)
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            # Передаем индекс i вместо длинного ID
            [InlineKeyboardButton(text=m["name"], callback_data=f"master|{event}|{time_str}|{i}|{action}")]
            for i, m in enumerate(available)
        ] + [[InlineKeyboardButton(text="← Назад", callback_data="back_to_services")]])
        
        return await callback.message.edit_text(f"🔮 **Выберите специалиста на {time_str}:**", reply_markup=kb)

    # Для остальных услуг
    await callback.message.edit_text(f"⏳ Записываю {ef(event, 'to')} на {time_str}…")
    res = await booking_service.execute_booking(
        user_id=str(callback.from_user.id),
        username=callback.from_user.username or "",
        full_name=callback.from_user.full_name,
        event=event,
        time_str=time_str,
        is_reschedule=(action == "reschedule")
    )
    await handle_booking_result(callback, res, event, time_str, None, action)
    
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
    bookings = await booking_service.get_user_bookings(user_id)
    booking = next((b for b in bookings if b.event == event), None)
    
    if not booking:
        return await callback.answer("Запись не найдена")

    # Поиск локации
    location = "Не указана"
    if event in MASTERS_CONFIG and booking.master_id != "Записано":
        master = next((m for m in MASTERS_CONFIG[event] if m["id"] == booking.master_id), None)
        if master: location = master.get("location", "Не указана")
    
    text = (
        f"✅ **Вы записаны на {ef(event)}**\n\n"
        f"🕐 **Время:** {booking.time}\n"
        f"📍 **Локация:** {location}\n"
    )
    if booking.master_id and booking.master_id != "Записано":
        text += f"👤 **Специалист:** {booking.master_id}\n"
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Перенести время", callback_data=f"request_reschedule|{event}")],
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
    
@router.callback_query(F.data.startswith("request_reschedule|"))
async def process_request_reschedule(callback: types.CallbackQuery, booking_service: BookingService):
    event = callback.data.split("|")[1]
    
    # Получаем свободные слоты
    suggested = await booking_service.get_suggested_slots(event)
    if not suggested:
        return await callback.answer("К сожалению, других свободных мест нет 😔", show_alert=True)
        
    card = build_service_card(event, suggested)
    
    # Генерируем клавиатуру слотов, передавая action="reschedule"
    # Это скажет боту, что при выборе времени старую запись нужно удалить
    kb = build_slot_keyboard(event, suggested, action="reschedule")
    
    await callback.message.edit_text(
        card + "\n\n🔄 **Выберите новое время для переноса:**", 
        reply_markup=kb, 
        parse_mode="Markdown"
    )
    
@router.callback_query(F.data.startswith("master|"))
async def process_master_selection(callback: types.CallbackQuery, booking_service: BookingService):
    _, event, time_str, master_idx, action = callback.data.split("|")
    
    # Получаем мастера из конфига по индексу
    master = MASTERS_CONFIG[event][int(master_idx)]
    master_id = master["id"]
    
    res = await booking_service.execute_booking(
        user_id=str(callback.from_user.id),
        username=callback.from_user.username or "",
        full_name=callback.from_user.full_name,
        event=event,
        time_str=time_str,
        is_reschedule=(action == "reschedule"),
        master_id=master_id
    )
    await handle_booking_result(callback, res, event, time_str, master_id, action)
    
@router.callback_query(F.data.startswith("confirm_overlap|"))
async def process_confirm_overlap(callback: types.CallbackQuery, booking_service: BookingService):
    # Данные: confirm_overlap|event|time|master_id|action
    _, event, time_str, master_id, action = callback.data.split("|")
    
    # Вызываем запись с force=True
    res = await booking_service.execute_booking(
        user_id=str(callback.from_user.id),
        username=callback.from_user.username or "",
        full_name=callback.from_user.full_name,
        event=event,
        time_str=time_str,
        is_reschedule=(action == "reschedule"),
        master_id=master_id if master_id != "None" else None,
        force=True
    )
    await callback.message.edit_text(res["text"], parse_mode="Markdown")