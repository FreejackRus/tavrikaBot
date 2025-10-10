import os
import sys
# Ensure local vendor dependencies are available before third-party imports
sys.path.append(os.path.join(os.path.dirname(__file__), "vendor"))
import asyncio
import logging
from datetime import date, timedelta
from typing import Optional
import pandas as pd

from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.error import TimedOut, NetworkError
from telegram.request import HTTPXRequest
from telegram.ext import Application, CommandHandler, ContextTypes

from iiko_client import IikoClient
from cashflow import (
    build_cashflow_tables,
    build_cashflow_detailed_table,
    build_cashflow_tables_for_day,
    build_cashflow_detailed_table_for_day,
    export_to_excel,
    dataframe_to_text_table,
    RU_ACCOUNT_MAIN,
    RU_ACCOUNT_TRADES,
    CATEGORY_RU_MAP,
)


def get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


async def cashflow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Parse dates from args: support single day (YYYY-MM-DD) or range
    args = context.args
    day_mode = False
    if len(args) == 1:
        day = args[0]
        prev_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        date_from = prev_day
        date_to = day
        day_mode = True
    elif len(args) == 2:
        df_str, dt_str = args
        # If user passed equal dates, treat as single-day mode
        if df_str == dt_str:
            day = df_str
            prev_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
            date_from = prev_day
            date_to = day
            day_mode = True
        else:
            # Normalize order if start > end
            dfrom = date.fromisoformat(df_str)
            dto = date.fromisoformat(dt_str)
            if dfrom > dto:
                dfrom, dto = dto, dfrom
            date_from = dfrom.isoformat()
            date_to = dto.isoformat()
    else:
        # default: yesterday to today
        today = date.today()
        yesterday = today - timedelta(days=1)
        date_from = yesterday.isoformat()
        date_to = today.isoformat()

    base_url = get_env("IIKO_BASE_URL")
    login = get_env("IIKO_LOGIN")
    password = get_env("IIKO_PASSWORD")

    report_caption_period = (date_to if day_mode else f"{date_from} — {date_to}")
    await update.message.reply_text(f"Готовлю отчёт ДДС за {report_caption_period}...")

    try:
        # Выполняем тяжёлую часть синхронно в фоне, чтобы не блокировать event loop
        def generate_report():
            client = IikoClient(base_url=base_url, login=login, password=password)
            if day_mode:
                # iiko OLAP expects dateFrom < dateTo; используем полуоткрытые интервалы по суткам
                next_day = (date.fromisoformat(date_to) + timedelta(days=1)).isoformat()
                print(f"[cashflow day_mode] prev_day interval: [{date_from}, {date_to})")
                print(f"[cashflow day_mode] selected_day interval: [{date_to}, {next_day})")
                # Интервал предыдущего дня: [prev_day, day)
                raw_json_prev = client.fetch_olap_transactions(date_from=date_from, date_to=date_to)
                # Интервал выбранного дня: [day, next_day)
                raw_json_day = client.fetch_olap_transactions(date_from=date_to, date_to=next_day)
                res, day_df_local = build_cashflow_tables_for_day(raw_json_day, raw_json_prev)
                det, _ = build_cashflow_detailed_table_for_day(raw_json_day, raw_json_prev)
                # Export to excel: summary and detailed
                excel_sum = export_to_excel(res)
                excel_det = export_to_excel(det, path="cashflow_detailed.xlsx")
                return (res, day_df_local, None, excel_sum, excel_det)
            else:
                raw_json = client.fetch_olap_transactions(date_from=date_from, date_to=date_to)
                res, period_df_local = build_cashflow_tables(raw_json)
                det, _ = build_cashflow_detailed_table(raw_json)
                excel_sum = export_to_excel(res)
                excel_det = export_to_excel(det, path="cashflow_detailed.xlsx")
                return (res, None, period_df_local, excel_sum, excel_det)

        result, day_df, period_df, excel_path, excel_detailed_path = await asyncio.to_thread(generate_report)

        # Сформировать телеграм-сообщение в человекочитаемом виде
        def _fmt_money(v: float) -> str:
            try:
                return f"{v:,.2f}".replace(",", " ").replace(".", ",") + " р."
            except Exception:
                return str(v)

        def _fmt_date(d: str) -> str:
            from datetime import datetime
            return datetime.fromisoformat(d).strftime("%d.%m.%Y")

        def build_message_from_df(df: pd.DataFrame, summary_df: pd.DataFrame, caption_period: str) -> str:
            # Формирование сообщения HTML в виде моноширинной таблицы
            message = "💵 <b>ОТЧЕТ О ДВИЖЕНИИ ДЕНЕЖНЫХ СРЕДСТВ</b>\n"
            message += f"📅 <b>Дата/Период:</b> {caption_period}\n\n"
            message += "<code>"

            # Заголовки таблицы
            message += f"{'Тип статьи':<25} | {'Статья ДДС 1-го уровня':<25} | {'Статья ДДС 2-го уровня':<25} | {'Статья ДДС 3-го уровня':<25} | "
            message += f"{'ТОРГОВЫЕ КАССЫ':^48} | {'ГЛАВНАЯ КАССА':^48} | {'ИТОГО':^12}\n"
            message += f"{'':<25} | {'':<25} | {'':<25} | {'':<25} | "
            message += f"{'Нач.ост.':>12} {'Приход':>12} {'Расход':>12} {'Кон.ост.':>12} | "
            message += f"{'Нач.ост.':>12} {'Приход':>12} {'Расход':>12} {'Кон.ост.':>12} | "
            message += f"{'':<12}\n"
            message += "-" * 200 + "\n"

            # Если нет данных, показываем только остатки
            if len(summary_df) <= 2:  # только начальные и конечные остатки
                start_main = float(summary_df.loc[summary_df.index == "Остаток на начало", RU_ACCOUNT_MAIN].iloc[0])
                start_trades = float(summary_df.loc[summary_df.index == "Остаток на начало", RU_ACCOUNT_TRADES].iloc[0])
                end_main = float(summary_df.loc[summary_df.index == "Остаток на конец", RU_ACCOUNT_MAIN].iloc[0])
                end_trades = float(summary_df.loc[summary_df.index == "Остаток на конец", RU_ACCOUNT_TRADES].iloc[0])
                total_start = start_main + start_trades
                total_end = end_main + end_trades

                message += f"{'Остаток на начало':<25} | {'':<25} | {'':<25} | {'':<25} | "
                message += f"{_fmt_money(start_trades):>12} {'0':>12} {'0':>12} {_fmt_money(start_trades):>12} | "
                message += f"{_fmt_money(start_main):>12} {'0':>12} {'0':>12} {_fmt_money(start_main):>12} | "
                message += f"{_fmt_money(total_start):>12}\n"

                message += f"{'Остаток на конец':<25} | {'':<25} | {'':<25} | {'':<25} | "
                message += f"{_fmt_money(start_trades):>12} {'0':>12} {'0':>12} {_fmt_money(end_trades):>12} | "
                message += f"{_fmt_money(start_main):>12} {'0':>12} {'0':>12} {_fmt_money(end_main):>12} | "
                message += f"{_fmt_money(total_end):>12}\n"

                message += "</code>"
                return message

            # Обработка всех строк из summary_df
            current_type = None
            trades_start = trades_end = main_start = main_end = 0.0
            trades_in = trades_out = main_in = main_out = 0.0

            for idx, row in summary_df.iterrows():
                type_label = row["Тип статьи"]
                l1 = row["Статья ДДС 1-го уровня"]
                l2 = row["Статья ДДС 2-го уровня"]
                l3 = row["Статья ДДС 3-го уровня"]
                
                trades_val = float(row[RU_ACCOUNT_TRADES])
                main_val = float(row[RU_ACCOUNT_MAIN])
                total_val = float(row["Итого"])

                # Начальные остатки
                if type_label == "Остаток на начало":
                    trades_start = trades_val
                    main_start = main_val
                    message += f"{type_label:<25} | {l1:<25} | {l2:<25} | {l3:<25} | "
                    message += f"{_fmt_money(trades_val):>12} {'':>12} {'':>12} {'':>12} | "
                    message += f"{_fmt_money(main_val):>12} {'':>12} {'':>12} {'':>12} | "
                    message += f"{_fmt_money(total_val):>12}\n"
                    continue

                # Конечные остатки
                if type_label == "Остаток на конец":
                    trades_end = trades_val
                    main_end = main_val
                    message += f"{type_label:<25} | {l1:<25} | {l2:<25} | {l3:<25} | "
                    message += f"{_fmt_money(trades_start):>12} {_fmt_money(trades_in):>12} {_fmt_money(trades_out):>12} {_fmt_money(trades_val):>12} | "
                    message += f"{_fmt_money(main_start):>12} {_fmt_money(main_in):>12} {_fmt_money(main_out):>12} {_fmt_money(main_val):>12} | "
                    message += f"{_fmt_money(total_val):>12}\n"
                    continue

                # Заголовок типа (если изменился)
                if type_label != current_type:
                    current_type = type_label
                    message += f"{type_label:<25} | {'':<25} | {'':<25} | {'':<25} | "
                    message += f"{'':<48} | {'':<48} | {'':<12}\n"

                # Если это движение средств
                if l1:
                    if trades_val > 0:
                        trades_in += trades_val
                    else:
                        trades_out -= trades_val

                    if main_val > 0:
                        main_in += main_val
                    else:
                        main_out -= main_val

                    message += f"{'':<25} | {l1:<25} | {l2:<25} | {l3:<25} | "
                    message += f"{'':>12} {_fmt_money(trades_val if trades_val > 0 else 0):>12} {_fmt_money(-trades_val if trades_val < 0 else 0):>12} {'':>12} | "
                    message += f"{'':>12} {_fmt_money(main_val if main_val > 0 else 0):>12} {_fmt_money(-main_val if main_val < 0 else 0):>12} {'':>12} | "
                    message += f"{_fmt_money(total_val):>12}\n"

            message += "</code>"
            return message

        if day_mode:
            caption_date = _fmt_date(date_to)
            message_text = build_message_from_df(day_df, result, caption_date)
        else:
            # Период: формат вида 01.10.2025 — 10.10.2025
            caption_period = f"{_fmt_date(date_from)} — {_fmt_date(date_to)}"
            message_text = build_message_from_df(period_df, result, caption_period)

        # Отправляем HTML-форматированное сообщение безопасной обёрткой
        await safe_reply_html(update.message, message_text)

        # Send Excel files
        await safe_reply_document(update.message, excel_path, caption=f"Отчёт ДДС (сводная) {report_caption_period}")
        await safe_reply_document(update.message, excel_detailed_path, caption=f"Отчёт ДДС (детальная) {report_caption_period}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка при формировании отчёта: {e}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привет! Отправьте /cashflow или /cashflow YYYY-MM-DD YYYY-MM-DD"
    )


# Безопасные обёртки для отправки сообщений/документов с повтором при таймауте
async def safe_reply_html(message, text: str, retries: int = 2, delay_base: float = 2.0) -> None:
    for attempt in range(retries + 1):
        try:
            await message.reply_html(text)
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
    app.add_handler(CommandHandler("cashflow", cashflow_command))

    print("Bot is running... Use Ctrl+C to stop.")
    app.run_polling()


if __name__ == "__main__":
    main()