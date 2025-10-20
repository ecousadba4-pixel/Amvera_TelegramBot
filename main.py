import os
import logging
import asyncpg
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Update, ReplyKeyboardMarkup, KeyboardButton
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

class BotService:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def normalize_phone(phone: str) -> str:
        digits = ''.join(ch for ch in (phone or "") if ch.isdigit())
        return digits[-10:] if len(digits) >= 10 else digits

    async def fetch_user_row(self, phone_number):
        """ Единая низкоуровневая функция получения строки пользователя по телефону """
        clean_phone = self.normalize_phone(phone_number)
        if not clean_phone:
            return None
        query = """
        SELECT first_name, loyalty_level, bonus_balances, last_date_visit
        FROM bonuses_balance
        WHERE phone = $1
        """
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, clean_phone)
        except Exception:
            logger.exception("Database query failed")
            return None

    def parse_guest_info(self, row):
        """ Конвертация строки БД в user dict для выдачи в боте """
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

    async def get_guest_bonus(self, phone_number):
        """ Единая точка входа во всю бизнес-логику выдачи бонусов """
        if not phone_number:
            return None
        row = await self.fetch_user_row(phone_number)
        return self.parse_guest_info(row)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Creating DB pool")
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("DB pool created")
    except Exception:
        logger.exception("Failed to create DB pool")
        raise
    app.state.bot_service = BotService(pool)
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
        await pool.close()
        logger.info("DB pool closed")
    except Exception:
        logger.exception("Failed to close DB pool")

app = FastAPI(lifespan=lifespan)

@dp.message(F.text == "/start")
async def cmd_start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Поделиться номером телефона", request_contact=True)]
        ],
        resize_keyboard=True
    )
    await message.answer(
        "Нажмите кнопку Поделиться номером телефона внизу, чтобы узнать бонусный баланс.",
        reply_markup=keyboard
    )

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    phone_number = message.contact.phone_number
    logger.info("Received contact from %s (user_id=%s)", phone_number, message.from_user.id)
    bot_service = app.state.bot_service
    guest_info = await bot_service.get_guest_bonus(phone_number)
    if not guest_info:
        await message.answer("Бонусы для указанного номера не найдены.")
        return
    # --- Код ниже скорректирован ---
    try:
        bonus_amount = int(float(guest_info['bonus_balances']))
    except Exception:
        bonus_amount = 0

    response_text = (
        f"👋 {guest_info['first_name']}, у Вас накоплено бонусов {bonus_amount} рублей.\n"
        f"Ваш уровень лояльности — {guest_info['loyalty_level']}."
    )
    if bonus_amount > 0:
        response_text += f"\nСрок действия бонусов: до {guest_info['expire_date']}."

    await message.answer(response_text)

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
