print("START")  # Для диагностики запуска

import os
import asyncpg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types
from aiogram.types import Update

# Получаем токен бота и строку подключения из переменных окружения Amvera
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
app = FastAPI()

# Команда /start
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(
        types.KeyboardButton(
            text="Поделиться номером телефона",
            request_contact=True
        )
    )
    await message.answer(
        "Пожалуйста, поделитесь вашим номером телефона, чтобы узнать бонусный баланс.",
        reply_markup=keyboard
    )

# Функция для получения данных гостя из базы
async def get_guest_bonus(phone_number: str):
    clean_phone = phone_number[-10:]

    conn = await asyncpg.connect(DATABASE_URL)
    query = """
        SELECT first_name, loyalty_level, accumulated_bonuses, last_date_visit
        FROM bonuses_balance
        WHERE guest_phone = $1
    """
    row = await conn.fetchrow(query, clean_phone)
    await conn.close()

    if not row:
        return None

    last_visit = row["last_date_visit"]
    if not last_visit:
        expire_date = "Неизвестно"
    else:
        expire_date = (last_visit + relativedelta(months=12)).strftime("%d.%m.%Y")

    return {
        "first_name": row["first_name"],
        "loyalty_level": row["loyalty_level"],
        "accumulated_bonuses": row["accumulated_bonuses"],
        "expire_date": expire_date,
    }

# Обработка контакта
@dp.message_handler(content_types=types.ContentType.CONTACT)
async def handle_contact(message: types.Message):
    phone_number = message.contact.phone_number
    guest_info = await get_guest_bonus(phone_number)

    if not guest_info:
        await message.answer("Бонусы для указанного номера не найдены.")
        return

    response_text = (
        f"{guest_info['first_name']}, у Вас накоплено {guest_info['accumulated_bonuses']} бонусов.\n"
        f"Ваш уровень лояльности — {guest_info['loyalty_level']}.\n"
        f"Срок действия бонусов: до {guest_info['expire_date']}."
    )

    await message.answer(response_text)

# Webhook endpoint Telegram
@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    print("Webhook received:", data)  # Проверка поступающих данных
    update = Update(**data)
    await dp.process_update(update)
    return Response()

# Настройка webhook при запуске
@app.on_event("startup")
async def on_startup():
    webhook_url = "https://telegram-loyal-karinausadba.amvera.io/webhook"  # замените на ваш фактический домен
    print("Setting webhook:", webhook_url)
    await bot.set_webhook(webhook_url)

@app.on_event("shutdown")
async def on_shutdown():
    print("Deleting webhook")
    await bot.delete_webhook()

