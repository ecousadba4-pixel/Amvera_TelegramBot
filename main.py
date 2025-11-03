"""ASGI-приложение Telegram webhook сервера для Amvera."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, cast

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import get_settings

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SENSITIVE_KEYS = {"phone_number", "phone"}

MSG_START = (
    "Нажмите кнопку Поделиться номером телефона внизу, чтобы узнать бонусный баланс."
)
BTN_SHARE_PHONE = "Поделиться номером телефона"
MSG_INVALID_CONTACT = (
    "❌ Вы можете проверить информацию только для своего номера телефона."
)
MSG_NO_BONUS = "Бонусы для указанного номера не найдены."
MSG_BALANCE_TEMPLATE = (
    "👋 {first_name}, у Вас накоплено бонусов {amount} рублей.\nВаш уровень лояльности — {level}."
)
MSG_EXPIRY_TEMPLATE = "\nСрок действия бонусов: до {date}."

DEFAULT_BONUS_DATA: Dict[str, Dict[str, Any]] = {
    "79990000000": {
        "first_name": "Ирина",
        "loyalty_level": "gold",
        "bonus_balances": 1250,
        "last_visit": "2024-08-15",
    },
    "79995556677": {
        "first_name": "Алексей",
        "loyalty_level": "silver",
        "bonus_balances": 320,
        "last_visit": "2024-07-01",
    },
}


def mask_phone(phone: Any, visible_digits: int = 4) -> str:
    """Возвращает маску номера телефона, сохраняя последние цифры."""

    if phone is None:
        return "***"
    if not isinstance(phone, str):
        phone = str(phone)
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        return "***"
    visible_digits = max(0, visible_digits)
    suffix = digits[-visible_digits:] if visible_digits else ""
    masked_length = max(len(digits) - visible_digits, 0)
    masked = "*" * masked_length + suffix
    return masked


def sanitize_payload(payload: Any) -> Any:
    """Рекурсивно маскирует чувствительные данные в словарях/списках."""

    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            if key in SENSITIVE_KEYS and isinstance(value, str):
                sanitized[key] = mask_phone(value)
            else:
                sanitized[key] = sanitize_payload(value)
        return sanitized
    if isinstance(payload, list):
        return [sanitize_payload(item) for item in payload]
    return payload


def normalize_phone(phone: Any) -> str:
    """Привести телефон к формату из 11 цифр (Россия)."""

    if phone is None:
        return ""
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return digits if len(digits) == 11 else ""


def load_bonus_data(path: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Загрузить бонусные данные из JSON-файла или вернуть значения по умолчанию."""

    if not path:
        return {key: dict(value) for key, value in DEFAULT_BONUS_DATA.items()}

    try:
        with open(path, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except FileNotFoundError:
        logger.warning("Файл %s не найден. Используются тестовые данные.", path)
        return {key: dict(value) for key, value in DEFAULT_BONUS_DATA.items()}
    except json.JSONDecodeError as exc:
        logger.warning(
            "Не удалось прочитать файл %s (%s). Используются тестовые данные.", path, exc
        )
        return {key: dict(value) for key, value in DEFAULT_BONUS_DATA.items()}

    if not isinstance(payload, dict):
        logger.warning("Структура файла %s некорректна. Используются тестовые данные.", path)
        return {key: dict(value) for key, value in DEFAULT_BONUS_DATA.items()}

    normalized: Dict[str, Dict[str, Any]] = {}
    for phone, data in payload.items():
        if not isinstance(data, dict):
            continue
        normalized_phone = normalize_phone(phone)
        if normalized_phone:
            normalized[normalized_phone] = dict(data)
    if not normalized:
        logger.warning("В файле %s нет корректных записей. Используются тестовые данные.", path)
        return {key: dict(value) for key, value in DEFAULT_BONUS_DATA.items()}
    return normalized


class BotService:
    """Минималистичная логика работы с бонусами."""

    def __init__(self, bonus_data: Dict[str, Dict[str, Any]], default_expiry_days: int) -> None:
        self._bonus_data = bonus_data
        self._default_expiry_days = max(default_expiry_days, 1)
        self._stats: list[Dict[str, Any]] = []

    def log_usage_stat(self, user_id: Optional[int], phone: str, command: str) -> None:
        entry = {
            "user_id": user_id,
            "phone": mask_phone(phone),
            "command": command,
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        }
        self._stats.append(entry)
        logger.info("Событие: %s", entry)

    def get_guest_bonus(self, phone: str) -> Optional[Dict[str, Any]]:
        normalized = normalize_phone(phone)
        if not normalized:
            return None
        guest = self._bonus_data.get(normalized)
        if not guest:
            return None
        prepared = dict(guest)
        if not prepared.get("expire_date"):
            expire_date = self._calculate_expiry(prepared.get("last_visit"))
            if expire_date:
                prepared["expire_date"] = expire_date
        return prepared

    def _calculate_expiry(self, last_visit: Optional[str]) -> Optional[str]:
        if not last_visit:
            return None
        try:
            visit_dt = datetime.fromisoformat(last_visit)
        except ValueError:
            return None
        expire_date = visit_dt + timedelta(days=self._default_expiry_days)
        return expire_date.date().isoformat()

    @staticmethod
    def format_bonus_amount(value: Any) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            logger.warning("Не удалось преобразовать значение бонусов: %s", value)
            return 0


app = FastAPI(title="Telegram Loyal Bot")


@app.on_event("startup")
def on_startup() -> None:
    """Подготовить сервис при запуске приложения."""

    settings = get_settings()
    data = load_bonus_data(settings.bonus_data_file)
    app.state.settings = settings
    app.state.bot_service = BotService(data, settings.default_expiry_days)
    logger.info("ASGI-приложение инициализировано. Используется порт %s.", settings.port)


def _get_bot_service(request: Request) -> BotService:
    bot_service = getattr(request.app.state, "bot_service", None)
    if bot_service is None:
        settings = get_settings()
        data = load_bonus_data(settings.bonus_data_file)
        bot_service = BotService(data, settings.default_expiry_days)
        request.app.state.bot_service = bot_service
    return cast(BotService, bot_service)


def _bad_request(message: str) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": message})


@app.get("/")
async def read_root() -> Dict[str, Any]:
    """Простой health-check эндпоинт."""

    return {"status": "ok", "message": MSG_START, "button": BTN_SHARE_PHONE}


@app.post("/webhook")
async def webhook(request: Request):  # type: ignore[override]
    """Обработчик Telegram webhook."""

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return _bad_request("invalid json")

    if not isinstance(payload, dict):
        return _bad_request("invalid json")

    message = payload.get("message")
    if not isinstance(message, dict):
        return _bad_request("message field is required")

    logger.info(
        "Получено сообщение: %s",
        json.dumps(sanitize_payload(message), ensure_ascii=False),
    )

    contact = message.get("contact")
    if not isinstance(contact, dict):
        return _bad_request("contact field is required")
    phone_number = contact.get("phone_number")
    if not phone_number:
        return _bad_request("phone_number is required")

    from_user = message.get("from") if isinstance(message.get("from"), dict) else {}
    sender_id = from_user.get("id") if isinstance(from_user, dict) else None
    contact_user_id = contact.get("user_id")
    if (
        sender_id is not None
        and contact_user_id is not None
        and sender_id != contact_user_id
    ):
        return {"reply": MSG_INVALID_CONTACT}

    bot_service = _get_bot_service(request)
    bot_service.log_usage_stat(sender_id, phone_number, "contact")

    guest_info = bot_service.get_guest_bonus(phone_number)
    if not guest_info:
        return {"reply": MSG_NO_BONUS}

    first_name = guest_info.get("first_name") or from_user.get("first_name") or "Гость"
    bonus_amount = bot_service.format_bonus_amount(guest_info.get("bonus_balances"))
    level = guest_info.get("loyalty_level") or "-"
    response_text = MSG_BALANCE_TEMPLATE.format(
        first_name=first_name,
        amount=bonus_amount,
        level=level,
    )
    expire_date = guest_info.get("expire_date")
    if expire_date and bonus_amount > 0:
        response_text += MSG_EXPIRY_TEMPLATE.format(date=expire_date)

    return {"reply": response_text}


def run_server() -> None:
    """Запустить uvicorn-сервер локально."""

    settings = get_settings()
    uvicorn.run("main:app", host="0.0.0.0", port=settings.port, log_level="info")


if __name__ == "__main__":
    run_server()
