# ... (импорты, конфигурация, BotService, lifespan) ...

# Пример констант
CONTACT_BUTTON_TEXT = "Поделиться номером телефона"
START_MESSAGE = "Нажмите кнопку Поделиться номером телефона внизу, чтобы узнать бонусный баланс."
NOT_FOUND_MESSAGE = "Бонусы для указанного номера не найдены."

@dp.message(F.text == "/start")
async def cmd_start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=CONTACT_BUTTON_TEXT, request_contact=True)]
        ],
        resize_keyboard=True
    )
    await message.answer(START_MESSAGE, reply_markup=keyboard)

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    # --- ПРОВЕРКА: принадлежит ли контакт отправителю ---
    if message.contact.user_id != message.from_user.id:
        await message.answer("❌ Вы можете проверить информацию только для своего номера телефона.")
        return
    # --- КОНЕЦ ПРОВЕРКИ ---

    phone_number = message.contact.phone_number
    user_id = message.from_user.id
    logger.info("Received contact from %s (user_id=%s)", phone_number, user_id)
    bot_service = app.state.bot_service
    
    # Записать событие
    try:
        await bot_service.log_usage_stat(user_id=user_id, phone=phone_number, command="contact")
    except Exception as e: # Логируем ошибку логирования, но не прерываем основной процесс
        logger.error(f"Failed to log usage stat for user {user_id}: {e}")

    try:
        guest_info = await bot_service.get_guest_bonus(phone_number)
    except Exception as e:
        logger.error(f"Failed to fetch bonus info for phone {phone_number} (user_id={user_id}): {e}")
        await message.answer("Произошла ошибка при получении данных. Попробуйте позже.")
        return

    if not guest_info:
        await message.answer(NOT_FOUND_MESSAGE)
        return

    try:
        bonus_amount = int(float(guest_info['bonus_balances']))
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not convert bonus_balances '{guest_info['bonus_balances']}' to int for user {user_id}: {e}")
        bonus_amount = 0

    response_text = (
        f"👋 {guest_info['first_name']}, у Вас накоплено бонусов {bonus_amount} рублей.\n"
        f"Ваш уровень лояльности — {guest_info['loyalty_level']}."
    )
    if bonus_amount > 0:
        response_text += f"\nСрок действия бонусов: до {guest_info['expire_date']}."

    try:
        await message.answer(response_text)
    except Exception as e:
        logger.error(f"Failed to send response to user {user_id}: {e}")
        # Важно: не отправляйте пользователю детали внутренней ошибки

# ... (остальной код FastAPI) ...

