import os
import asyncpg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Update
from contextlib import asynccontextmanager

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(F.text == "/start")
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

async def get_guest_bonus(phone_number: str, pool):
    clean_phone = phone_number[-10:]
    query = """
        SELECT first_name, loyalty_level, accumulated_bonuses, last_date_visit
        FROM bonuses_balance
        WHERE guest_phone = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, clean_phone)
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

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    # app — это global имя, но pool получаем через бэкап-глобал…
    from fastapi import Request
    pool = app.state.pool
    phone_number = message.contact.phone_number
    guest_info = await get_guest_bonus(phone_number, pool)
    if not guest_info:
        await message.answer("Бонусы для указанного номера не найдены.")
        return
    response_text = (
        f"{guest_info['first_name']}, у Вас накоплено {guest_info['accumulated_bonuses']} бонусов.\n"
        f"Ваш уровень лояльности — {guest_info['loyalty_level']}.\n"
        f"Срок действия бонусов: до {guest_info['expire_date']}."
    )
    await message.answer(response_text)

@asynccontextmanager
async def lifespan(app: FastAPI):
    webhook_url = "https://telegram-loyal-karinausadba.amvera.io/webhook"
    print("Setting webhook:", webhook_url)
    await bot.set_webhook(webhook_url)
    # Подключаем pool к state
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
    yield
    print("Deleting webhook")
    await bot.delete_webhook()
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    print("Webhook received:", data)
    update = Update(**data)
    await dp.feed_update(bot, update)
    return Response()


