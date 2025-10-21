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

# --- Константы ---
# Переменные окружения
ENV_TELEGRAM_BOT_TOKEN = "TELEGRAM_BOT_TOKEN"
ENV_DATABASE_URL = "DATABASE_URL"
ENV_WEBHOOK_URL = "WEBHOOK_URL"
ENV_PORT = "PORT"

# Названия таблиц и столбцов (если нужно централизовать)
TABLE_BONUSES_BALANCE = "bonuses_balance"
COL_PHONE = "phone"
COL_FIRST_NAME = "first_name"
COL_LOYALTY_LEVEL = "loyalty_level"
COL_BONUS_BALANCES = "bonus_balances"
COL_LAST_DATE_VISIT = "last_date_visit"

TABLE_TELEGRAM_BOT_STATS = "telegram_bot_usage_stats"
COL_USER_ID = "user_id"
COL_PHONE_STATS = "phone"
COL_COMMAND = "command"

# Тексты сообщений и кнопок
MSG_START = "Нажмите кнопку Поделиться номером телефона внизу, чтобы узнать бонусный баланс."
BTN_SHARE_PHONE = "Поделиться номером телефона"
MSG_INVALID_CONTACT = "❌ Вы можете проверить информацию только для своего номера телефона."
MSG_NO_BONUS = "Бонусы для указанного номера не найдены."
MSG_BALANCE_TEMPLATE = "👋 {first_name}, у Вас накоплено бонусов {amount} рублей.\nВаш уровень лояльности — {level}."
MSG_EXPIRY_TEMPLATE = "\nСрок действия бонусов: до {date}."

# SQL Запросы
SQL_FETCH_USER = f"""
SELECT {COL_FIRST_NAME}, {COL_LOYALTY_LEVEL}, {COL_BONUS_BALANCES}, {COL_LAST_DATE_VISIT}
FROM {TABLE_BONUSES_BALANCE}
WHERE {COL_PHONE} = $1
"""

SQL_LOG_USAGE = f"""
INSERT INTO {TABLE_TELEGRAM_BOT_STATS} ({COL_USER_ID}, {COL_PHONE_STATS}, {COL_COMMAND})
VALUES ($1, $2, $3)
"""

# --- /Константы ---

API_TOKEN = os.getenv(ENV_TELEGRAM_BOT_TOKEN)
DATABASE_URL = os.getenv(ENV_DATABASE_URL)
WEBHOOK_URL = os.getenv(ENV_WEBHOOK_URL)
PORT = int(os.getenv(ENV_PORT, "8000"))

if not API_TOKEN:
    logger.error(f"Missing {ENV_TELEGRAM_BOT_TOKEN} environment variable")
    raise RuntimeError(f"Missing {ENV_TELEGRAM_BOT_TOKEN} environment variable")
if not DATABASE_URL:
    logger.error(f"Missing {ENV_DATABASE_URL} environment variable")
    raise RuntimeError(f"Missing {ENV_DATABASE_URL} environment variable")

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
        """ Получение строки пользователя по телефону """
        clean_phone = self.normalize_phone(phone_number)
        if not clean_phone:
            return None
        # Используем константу SQL_FETCH_USER
        query = SQL_FETCH_USER
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
        last_visit = row.get(COL_LAST_DATE_VISIT)
        if not last_visit:
            expire_date = "Неизвестно"
        else:
            try:
                expire_date = (last_visit + relativedelta(months=12)).strftime("%d.%m.%Y")
            except Exception as e:
                logger.warning(f"Failed to calculate expire date for {last_visit}: {e}")
                expire_date = "Неизвестно"
        return {
            "first_name": row.get(COL_FIRST_NAME) or "Гость",
            "loyalty_level": row.get(COL_LOYALTY_LEVEL) or "—",
            "bonus_balances": row.get(COL_BONUS_BALANCES) or 0,
            "expire_date": expire_date,
        }

    async def get_guest_bonus(self, phone_number):
        """ Единая точка входа во всю бизнес-логику выдачи бонусов """
        if not phone_number:
            return None
        row = await self.fetch_user_row(phone_number)
        return self.parse_guest_info(row)

    async def log_usage_stat(self, user_id, phone, command):
        """ Запись события использования бота """
        # Используем константу SQL_LOG_USAGE
        query = SQL_LOG_USAGE
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, user_id, phone, command)
        except Exception:
            logger.exception("Failed to log usage stat")

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
            logger.info(f"Setting Telegram webhook to {WEBHOOK_URL}")
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
            [KeyboardButton(text=BTN_SHARE_PHONE, request_contact=True)]
        ],
        resize_keyboard=True
    )
    await message.answer(MSG_START, reply_markup=keyboard)

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    # --- ПРОВЕРКА: принадлежит ли контакт отправителю ---
    if message.contact.user_id != message.from_user.id:
        await message.answer(MSG_INVALID_CONTACT)
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
        await message.answer(MSG_NO_BONUS)
        return

    try:
        bonus_amount = int(float(guest_info['bonus_balances']))
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not convert bonus_balances '{guest_info['bonus_balances']}' to int for user {user_id}: {e}")
        bonus_amount = 0

    response_text = MSG_BALANCE_TEMPLATE.format(
        first_name=guest_info['first_name'],
        amount=bonus_amount,
        level=guest_info['loyalty_level']
    )
    if bonus_amount > 0:
        response_text += MSG_EXPIRY_TEMPLATE.format(date=guest_info['expire_date'])

    try:
        await message.answer(response_text)
    except Exception as e:
        logger.error(f"Failed to send response to user {user_id}: {e}")
        # Важно: не отправляйте пользователю детали внутренней ошибки

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


