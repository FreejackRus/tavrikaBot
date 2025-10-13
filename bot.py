import os
import asyncio
import calendar
from datetime import date, timedelta
from typing import Optional

from dotenv import load_dotenv
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TimedOut, NetworkError
from telegram.request import HTTPXRequest
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

from iiko_client import IikoClient
from cashflow import export_excel_cashflow
# Local JSON loader no longer used in bot flow


def get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


# === Кнопочное меню и календарь ===

def _build_main_menu() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📅 За сегодня", callback_data="TODAY"),
            InlineKeyboardButton("📆 Выбрать день", callback_data="DAY"),
        ],
        [InlineKeyboardButton("🗓️ Выбрать период", callback_data="PERIOD")],
    ]
    return InlineKeyboardMarkup(rows)


def _build_calendar(year: int | None = None, month: int | None = None, mode: str = "DAY") -> InlineKeyboardMarkup:
    today = date.today()
    year = year or today.year
    month = month or today.month
    first_weekday, num_days = calendar.monthrange(year, month)

    header = [
        InlineKeyboardButton("←", callback_data=f"CAL:PREV:{year}-{month:02d}:{mode}"),
        InlineKeyboardButton(f"{month:02d}.{year}", callback_data="CAL:NOP"),
        InlineKeyboardButton("→", callback_data=f"CAL:NEXT:{year}-{month:02d}:{mode}"),
    ]

    rows = [header]
    week = []
    # Заполняем пустые клетки до начала месяца
    for _ in range(first_weekday):
        week.append(InlineKeyboardButton(" ", callback_data="CAL:NOP"))
    for d in range(1, num_days + 1):
        iso = date(year, month, d).isoformat()
        week.append(InlineKeyboardButton(str(d), callback_data=f"CAL:SET:{iso}:{mode}"))
        if len(week) == 7:
            rows.append(week)
            week = []
    if week:
        rows.append(week)
    rows.append([InlineKeyboardButton("Назад", callback_data="BACK_MAIN")])
    return InlineKeyboardMarkup(rows)


def _start_iiko_client() -> IikoClient:
    base_url = get_env("IIKO_BASE_URL")
    login = get_env("IIKO_LOGIN")
    password = get_env("IIKO_PASSWORD")
    return IikoClient(base_url=base_url, login=login, password=password)


def _generate_xlsx_for_day(iso_day: str) -> str:
    client = _start_iiko_client()
    preset_id = get_env("IIKO_OLAP_PRESET_ID")
    try:
        d = date.fromisoformat(iso_day)
    except Exception:
        d = date.today()
    date_from = d.isoformat()
    date_to = (d + timedelta(days=1)).isoformat()
    date_pre = (d - timedelta(days=1)).isoformat()
    raw_previous = client.fetch_olap_by_preset(preset_id, date_from=date_pre, date_to=date_from)
    raw_current = client.fetch_olap_by_preset(preset_id, date_from=date_from, date_to=date_to)
    out_path = f"{date_from}_ДДС.xlsx"
    return export_excel_cashflow(raw_previous, raw_current, date_from, path=out_path)


def _generate_xlsx_for_period(date_from: str, date_to: str) -> str:
    client = _start_iiko_client()
    preset_id = get_env("IIKO_OLAP_PRESET_ID")
    try:
        dfrom_dt = date.fromisoformat(date_from)
        dto_dt = date.fromisoformat(date_to)
        if dfrom_dt >= dto_dt:
            dto_dt = dfrom_dt + timedelta(days=1)
        dpre_dt = dfrom_dt - timedelta(days=1)
        date_from = dfrom_dt.isoformat()
        date_to = dto_dt.isoformat()
        date_pre = dpre_dt.isoformat()
    except Exception:
        # Фоллбэк: сегодня и завтра
        dfrom_dt = date.today()
        dpre_dt = dfrom_dt - timedelta(days=1)
        dto_dt = dfrom_dt + timedelta(days=1)
        date_pre = dpre_dt.isoformat()
        date_from = dfrom_dt.isoformat()
        date_to = dto_dt.isoformat()
    raw_previous = client.fetch_olap_by_preset(preset_id, date_from=date_pre, date_to=date_from)
    raw_current = client.fetch_olap_by_preset(preset_id, date_from=date_from, date_to=date_to)
    out_path = f"{date_from}_ДДС.xlsx"
    return export_excel_cashflow(raw_previous, raw_current, date_from, path=out_path)


async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    data = q.data or ""
    # короткий toast вместо текстовых сообщений
    try:
        await q.answer()
    except Exception:
        pass

    if data == "TODAY":
        # Только XLSX, без текстовых сообщений
        iso = date.today().isoformat()
        path = await asyncio.to_thread(_generate_xlsx_for_day, iso)
        await safe_reply_document(q.message, path, caption=f"Отчёт ДДС — {iso}")
        try:
            await q.edit_message_reply_markup(reply_markup=_build_main_menu())
        except Exception:
            pass
        return

    if data == "DAY":
        try:
            await q.edit_message_reply_markup(reply_markup=_build_calendar(mode="DAY"))
        except Exception:
            pass
        return

    if data == "PERIOD":
        context.user_data["period_state"] = "from"
        try:
            await q.edit_message_reply_markup(reply_markup=_build_calendar(mode="PERIOD_FROM"))
        except Exception:
            pass
        return

    if data == "BACK_MAIN":
        try:
            await q.edit_message_reply_markup(reply_markup=_build_main_menu())
        except Exception:
            pass
        return

    if data.startswith("CAL:"):
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        if action == "NOP":
            return
        mode = parts[3] if len(parts) > 3 else "DAY"

        if action in ("PREV", "NEXT"):
            ym = parts[2]
            y, m = map(int, ym.split("-"))
            base = date(y, m, 1)
            if action == "PREV":
                target = (base - timedelta(days=1)).replace(day=1)
            else:
                target = (base + timedelta(days=32)).replace(day=1)
            try:
                await q.edit_message_reply_markup(reply_markup=_build_calendar(target.year, target.month, mode=mode))
            except Exception:
                pass
            return

        if action == "SET":
            iso_day = parts[2] if len(parts) > 2 else date.today().isoformat()
            if mode == "DAY":
                path = await asyncio.to_thread(_generate_xlsx_for_day, iso_day)
                await safe_reply_document(q.message, path, caption=f"Отчёт ДДС — {iso_day}")
                try:
                    await q.edit_message_reply_markup(reply_markup=_build_main_menu())
                except Exception:
                    pass
                return
            elif mode == "PERIOD_FROM":
                context.user_data["period_from"] = iso_day
                # Переходим к выбору конечной даты в той же месяце
                try:
                    d = date.fromisoformat(iso_day)
                except Exception:
                    d = date.today()
                try:
                    await q.edit_message_reply_markup(reply_markup=_build_calendar(d.year, d.month, mode="PERIOD_TO"))
                except Exception:
                    pass
                context.user_data["period_state"] = "to"
                return
            elif mode == "PERIOD_TO":
                period_from = context.user_data.get("period_from")
                period_to = iso_day
                if not period_from:
                    # если начало отсутствует — считаем одиночным днём
                    path = await asyncio.to_thread(_generate_xlsx_for_day, period_to)
                    await safe_reply_document(q.message, path, caption=f"Отчёт ДДС — {period_to}")
                else:
                    path = await asyncio.to_thread(_generate_xlsx_for_period, period_from, period_to)
                    await safe_reply_document(q.message, path, caption=f"Отчёт ДДС — период {period_from} — {period_to}")
                # Сброс состояния и возврат в главное меню
                context.user_data.pop("period_from", None)
                context.user_data.pop("period_state", None)
                try:
                    await q.edit_message_reply_markup(reply_markup=_build_main_menu())
                except Exception:
                    pass
                return


# Устаревшая команда /cashflow удалена: используйте кнопки в /start

    # Старый блок генерации текстовых сообщений отчёта удалён; неизвестные колбэки игнорируем.
    return


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Выберите режим получения отчёта:",
        reply_markup=_build_main_menu(),
    )


# Безопасная отправка документов (XLSX) с повтором при таймауте


async def safe_reply_document(message, file_path: str, caption: str | None = None, retries: int = 2, delay_base: float = 2.0) -> None:
    for attempt in range(retries + 1):
        try:
            with open(file_path, "rb") as f:
                await message.reply_document(document=InputFile(f, filename=os.path.basename(file_path)), caption=caption)
            return
        except TimedOut:
            if attempt < retries:
                await asyncio.sleep(delay_base * (attempt + 1))
                continue
            raise
        except NetworkError:
            if attempt < retries:
                await asyncio.sleep(delay_base * (attempt + 1))
                continue
            raise


def main() -> None:
    load_dotenv()
    token = get_env("TELEGRAM_BOT_TOKEN")
    # Настраиваем увеличенные таймауты HTTPXRequest для предотвращения TimedOut
    http_request = HTTPXRequest(
        connection_pool_size=4,
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=15.0,
        pool_timeout=10.0,
        media_write_timeout=120.0,
    )

    app = Application.builder().token(token).request(http_request).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(_on_callback))

    print("Bot is running... Use Ctrl+C to stop.")
    app.run_polling()


if __name__ == "__main__":
    main()