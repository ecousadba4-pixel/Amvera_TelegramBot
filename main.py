import os
import logging
from datetime import datetime
from typing import Optional
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Request, Response
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Update, ReplyKeyboardMarkup, KeyboardButton
from contextlib import asynccontextmanager
# Импорты Pydantic
from pydantic import BaseModel, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Модели Pydantic ---

class Settings(BaseSettings):
    telegram_bot_token: str
    database_url: str
    webhook_url: Optional[str] = None
    port: int = 8000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8')

class DBUserRow(BaseModel):
    first_name: Optional[str] = "Гость"
    loyalty_level: Optional[str] = "—"
    bonus_balances: Optional[float] = 0.0  # float, чтобы избежать проблем при int(float())
    last_date_visit: Optional[datetime] = None

class GuestInfo(BaseModel):
    first_name: str
    loyalty_level: str
    bonus_balances: int
    expire_date: str

# --- Загрузка настроек ---
settings = Settings()

# --- Константы ---
# Тексты сообщений и кнопок
MSG_START = "Нажмите кнопку Поделиться номером телефона внизу, чтобы узнать бонусный баланс."
BTN_SHARE_PHONE = "Поделиться номером телефона"
MSG_INVALID_CONTACT = "❌ Вы можете проверить информацию только для своего номера телефона."
MSG_NO_BONUS = "Бонусы для указанного номера не найдены."
MSG_BALANCE_TEMPLATE = "👋 {first_name}, у Вас накоплено бонусов {amount} рублей.\nВаш уровень лояльности — {level}."
MSG_EXPIRY_TEMPLATE = "\nСрок действия бонусов: до {date}."

# SQL Запросы
SQL_FETCH_USER = """
SELECT first_name, loyalty_level, bonus_balances, last_date_visit
FROM bonuses_balance
WHERE phone = $1
"""

SQL_LOG_USAGE = """
INSERT INTO telegram_bot_usage_stats (user_id, phone, command)
VALUES ($1, $2, $3)
"""

# --- Инициализация ---
API_TOKEN = settings.telegram_bot_token
DATABASE_URL = settings.database_url
WEBHOOK_URL = settings.webhook_url
PORT = settings.port

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

    async def fetch_user_row(self, phone_number) -> Optional[DBUserRow]:
        """ Получение строки пользователя по телефону """
        clean_phone = self.normalize_phone(phone_number)
        if not clean_phone:
            return None
        query = SQL_FETCH_USER
        try:
            async with self.pool.acquire() as conn:
                raw_row = await conn.fetchrow(query, clean_phone)
                if raw_row:
                    # Преобразуем строку из базы в модель Pydantic
                    return DBUserRow.model_validate(dict(raw_row))
                return None
        except Exception:
            logger.exception("Database query failed")
            return None

    def parse_guest_info(self, db_row: Optional[DBUserRow]) -> Optional[GuestInfo]:
        """ Конвертация строки БД в модель для выдачи в боте """
        if not db_row:
            return None

        last_visit = db_row.last_date_visit
        if not last_visit:
            expire_date = "Неизвестно"
        else:
            try:
                expire_date = (last_visit + relativedelta(months=12)).strftime("%d.%m.%Y")
            except Exception as e:
                logger.warning(f"Failed to calculate expire date for {last_visit}: {e}")
                expire_date = "Неизвестно"

        # Преобразуем бонусы к int
        try:
            bonus_amount = int(float(db_row.bonus_balances))
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not convert bonus_balances '{db_row.bonus_balances}' to int: {e}")
            bonus_amount = 0

        return GuestInfo(
            first_name=db_row.first_name,
            loyalty_level=db_row.loyalty_level,
            bonus_balances=bonus_amount,
            expire_date=expire_date
        )

    async def get_guest_bonus(self, phone_number) -> Optional[GuestInfo]:
        """ Единая точка входа во всю бизнес-логику выдачи бонусов """
        if not phone_number:
            return None
        db_row = await self.fetch_user_row(phone_number)
        return self.parse_guest_info(db_row)

    async def log_usage_stat(self, user_id: int, phone: str, command: str):
        """ Запись события использования бота """
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
        import asyncpg # Импорт внутри, чтобы не требовался при запуске других частей
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
        guest_info: Optional[GuestInfo] = await bot_service.get_guest_bonus(phone_number)
    except Exception as e:
        logger.error(f"Failed to fetch bonus info for phone {phone_number} (user_id={user_id}): {e}")
        await message.answer("Произошла ошибка при получении данных. Попробуйте позже.")
        return

    if not guest_info: # guest_info теперь гарантированно GuestInfo или None
        await message.answer(MSG_NO_BONUS)
        return

    # bonus_amount теперь int благодаря parse_guest_info
    response_text = MSG_BALANCE_TEMPLATE.format(
        first_name=guest_info.first_name,
        amount=guest_info.bonus_balances,
        level=guest_info.loyalty_level
    )
    if guest_info.bonus_balances > 0: # Теперь можно безопасно обращаться к .bonus_balances
        response_text += MSG_EXPIRY_TEMPLATE.format(date=guest_info.expire_date)

    try:
        await message.answer(response_text)
    except Exception as e:
        logger.error(f"Failed to send response to user {user_id}: {e}")

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



