import os
import logging
import asyncpg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Update, ReplyKeyboardMarkup, KeyboardButton
from contextlib import asynccontextmanager

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Environment ----------
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "8000"))

if not API_TOKEN:
    logger.error("Missing TELEGRAM_BOT_TOKEN environment variable")
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")
if not DATABASE_URL:
    logger.error("Missing DATABASE_URL environment variable")
    raise RuntimeError("Missing DATABASE_URL environment variable")

# ---------- Globals ----------
POOL = None
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------- Utilities ----------
def normalize_phone(phone: str) -> str:
    digits = ''.join(ch for ch in (phone or "") if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits

async def get_guest_bonus(phone_number: str, pool):
    if not phone_number:
        return None
    clean_phone = normalize_phone(phone_number)
    if not clean_phone:
        return None
    query = """
    SELECT first_name, loyalty_level, bonus_balances, last_date_visit
    FROM bonuses_balance
    WHERE phone = $1
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, clean_phone)
    except Exception as e:
        logger.exception("Database query failed")
        return None
    if not row:
        return None
    last_visit = row.get("last_date_visit")
    if not last_visit:
        expire_date = "Неизвестно"
    else:
        try:
            expire_date = (last_visit + relativedelta(months=12)).strftime("%d.%m.%Y")
        except Exception:
            expire_date = "Неизвестно"
    return {
        "first_name": row.get("first_name") or "Гость",
        "loyalty_level": row.get("loyalty_level") or "—",
        "bonus_balances": row.get("bonus_balances") or 0,
        "expire_date": expire_date,
    }

# ---------- Handlers ----------
@dp.message(F.text == "/start")
async def cmd_start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Поделиться номером телефона", request_contact=True)]
        ],
        resize_keyboard=True
    )
    await message.answer(
        "Пожалуйста, поделитесь вашим номером телефона, чтобы узнать бонусный баланс.",
        reply_markup=keyboard
    )

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    global POOL
    phone_number = message.contact.phone_number
    logger.info("Received contact from %s (user_id=%s)", phone_number, message.from_user.id)
    if not POOL:
        logger.error("Database pool not initialized")
        await message.answer("Извините, сервис временно недоступен. Попробуйте позже.")
        return
    guest_info = await get_guest_bonus(phone_number, POOL)
    if not guest_info:
        await message.answer("Бонусы для указанного номера не найдены.")
        return
    bonus_amount = int(float(guest_info['bonus_balances']))
    response_text = (
        f"👋 {guest_info['first_name']}, у Вас накоплено бонусов {bonus_amount} рублей.\n"
        f"Ваш уровень лояльности — {guest_info['loyalty_level']}.\n"
        f"Срок действия бонусов: до {guest_info['expire_date']}."
    )
    await message.answer(response_text)

# ---------- FastAPI lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global POOL
    logger.info("Creating DB pool")
    try:
        POOL = await asyncpg.create_pool(DATABASE_URL)
        logger.info("DB pool created")
    except Exception:
        logger.exception("Failed to create DB pool")
        raise
    if WEBHOOK_URL:
        try:
            logger.info("Setting Telegram webhook to %s", WEBHOOK_URL)
            await bot.set_webhook(WEBHOOK_URL)
            logger.info("Webhook set")
        except Exception:
            logger.exception("Failed to set webhook (continuing without webhook)")
    yield
    logger.info("Shutting down: deleting webhook and closing pool")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook (ignoring)")
    try:
        if POOL:
            await POOL.close()
            logger.info("DB pool closed")
    except Exception:
        logger.exception("Failed to close DB pool")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    logger.info("Webhook received: %s", data.get("message") or data.get("update_id"))
    try:
        update = Update(**data)
    except Exception:
        logger.exception("Failed to parse update")
        return Response(status_code=400)
    try:
        await dp.feed_update(bot, update)
    except Exception:
        logger.exception("Failed to feed update")
    return Response()

@app.get("/")
async def root():
    return {"status": "ok"}

# If you run uvicorn yourself: uvicorn main:app --host 0.0.0.0 --port $PORT

