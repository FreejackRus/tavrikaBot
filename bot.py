import os
import io
import logging
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from iiko_client import IikoClient
from cashflow import (
    build_cashflow_tables_for_day,
    build_cashflow_tables_for_period,
    build_detailed_cashflow_for_period,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tavrika-bot")

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CLIENT_BASE_URL = os.environ.get("IIKO_BASE_URL") or "https://tavrika-wine-kitchen.iiko.it:443"
CLIENT_LOGIN = os.environ.get("IIKO_LOGIN")
CLIENT_PASSWORD = os.environ.get("IIKO_PASSWORD")

client = IikoClient(base_url=CLIENT_BASE_URL, login=CLIENT_LOGIN, password=CLIENT_PASSWORD)


def safe_reply_text(update: Update, text: str) -> None:
    try:
        update.message.reply_text(text)
    except Exception:
        logger.exception("Failed to reply text")


def safe_reply_document(update: Update, buffer: io.BytesIO, filename: str) -> None:
    try:
        update.message.reply_document(buffer.getvalue(), filename=filename)
    except Exception:
        logger.exception("Failed to reply document")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    safe_reply_text(update, "Привет! Я бот Tavrika. Используйте /cashflow для отчётов ДДС.")


# Helpers

def parse_dates(args: list[str]) -> tuple[Optional[datetime], Optional[datetime]]:
    if not args:
        return None, None
    try:
        if len(args) == 1:
            dt = datetime.strptime(args[0], "%Y-%m-%d")
            return dt, dt
        if len(args) >= 2:
            dt_from = datetime.strptime(args[0], "%Y-%m-%d")
            dt_to = datetime.strptime(args[1], "%Y-%m-%d")
            return dt_from, dt_to
    except Exception:
        return None, None
    return None, None


async def cashflow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    dt_from, dt_to = parse_dates(args)

    try:
        if dt_from and dt_to and dt_from == dt_to:
            prev_day = dt_from - timedelta(days=1)
            tables_today, tables_prev, xls = build_cashflow_tables_for_day(client, dt_from, prev_day)
            safe_reply_text(update, tables_today)
            safe_reply_text(update, tables_prev)
            safe_reply_document(update, xls, filename=f"cashflow_{dt_from.date()}.xlsx")
            return

        if dt_from and dt_to:
            tables, xls = build_cashflow_tables_for_period(client, dt_from, dt_to)
            safe_reply_text(update, tables)
            safe_reply_document(update, xls, filename=f"cashflow_{dt_from.date()}_{dt_to.date()}.xlsx")
            return

        # No dates: yesterday and today
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        tables_today, tables_yesterday, xls = build_cashflow_tables_for_day(
            client,
            datetime.combine(today, datetime.min.time()),
            datetime.combine(yesterday, datetime.min.time()),
        )
        safe_reply_text(update, tables_today)
        safe_reply_text(update, tables_yesterday)
        safe_reply_document(update, xls, filename=f"cashflow_{today}.xlsx")
    except Exception:
        logger.exception("cashflow handler failed")
        safe_reply_text(update, "Ошибка при формировании отчёта. Проверьте даты и попробуйте позже.")


async def cashflow_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    dt_from, dt_to = parse_dates(args)
    if not dt_from or not dt_to:
        safe_reply_text(update, "Укажите период: /cashflow_detail YYYY-MM-DD YYYY-MM-DD")
        return

    try:
        text, xls = build_detailed_cashflow_for_period(client, dt_from, dt_to)
        safe_reply_text(update, text)
        safe_reply_document(update, xls, filename=f"cashflow_detail_{dt_from.date()}_{dt_to.date()}.xlsx")
    except Exception:
        logger.exception("cashflow_detail handler failed")
        safe_reply_text(update, "Ошибка при формировании детального отчёта.")


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан. Укажите его в .env")

    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cashflow", cashflow))
    application.add_handler(CommandHandler("cashflow_detail", cashflow_detail))

    logger.info("Bot is running... Use Ctrl+C to stop.")
    application.run_polling()


if __name__ == "__main__":
    main()
