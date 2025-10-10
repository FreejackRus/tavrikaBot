from __future__ import annotations

import io
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd

RU_ACCOUNT_MAIN = "Операционная деятельность"
RU_ACCOUNT_TRADES = "Финансовая деятельность"
ACCOUNT_RU_MAP = {
    "Operational activity": RU_ACCOUNT_MAIN,
    "Financial activity": RU_ACCOUNT_TRADES,
}

CATEGORY_RU_MAP = {
    "Loan": "Займ",
    "Loans": "Займ",
    "Salary": "Зарплата",
    "Rent": "Аренда",
}


def _normalize_account(account_name: str) -> str:
    return ACCOUNT_RU_MAP.get(account_name, account_name)


def _normalize_category(category_name: str | None) -> str:
    if not category_name:
        return ""
    return CATEGORY_RU_MAP.get(category_name, category_name)


def fetch_movements_dataframe(client, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    rows = client.fetch_olap_transactions(dt_from, dt_to)
    df = pd.DataFrame(rows)

    # Normalize fields
    if "categoryName" in df.columns:
        df["categoryName"] = df["categoryName"].apply(_normalize_category)
    if "accountName" in df.columns:
        df["accountName"] = df["accountName"].apply(_normalize_account)

    # Amount sign: negative for expense, positive for income
    if "amount" in df.columns:
        df["amount"] = df.apply(lambda r: r["amount"] * (-1 if r.get("isExpense") else 1), axis=1)

    return df


def build_cashflow_tables_for_day(client, day: datetime, prev_day: datetime) -> tuple[str, str, io.BytesIO]:
    df_today = fetch_movements_dataframe(client, day, day)
    df_prev = fetch_movements_dataframe(client, prev_day, prev_day)

    df = pd.concat([df_today, df_prev], ignore_index=True)

    # Pivot by account and category
    pivot = pd.pivot_table(
        df,
        values="amount",
        index=["accountName", "categoryName"],
        aggfunc="sum",
        fill_value=0.0,
    )

    # Split tables
    pivot_today = pivot.loc[pivot.index.get_level_values(0).isin([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES])]
    # Union categories (fix duplicated loan mapping)
    categories_today = set(pivot_today.index.get_level_values(1))

    pivot_prev = pd.pivot_table(
        df_prev,
        values="amount",
        index=["accountName", "categoryName"],
        aggfunc="sum",
        fill_value=0.0,
    )
    categories_prev = set(pivot_prev.index.get_level_values(1))
    all_categories = categories_today.union(categories_prev)

    def _format_table(pvt: pd.DataFrame, title: str) -> str:
        lines = [title]
        for cat in sorted(all_categories):
            main_val = pvt.xs((RU_ACCOUNT_MAIN, cat), drop_level=False)["amount"].sum() if (RU_ACCOUNT_MAIN, cat) in pvt.index else 0.0
            trade_val = pvt.xs((RU_ACCOUNT_TRADES, cat), drop_level=False)["amount"].sum() if (RU_ACCOUNT_TRADES, cat) in pvt.index else 0.0
            lines.append(f"{cat}: Опер={main_val:.2f}, Фин={trade_val:.2f}")
        return "\n".join(lines)

    text_today = _format_table(pivot_today, f"ДДС за {day.date()}")
    text_prev = _format_table(pivot_prev, f"ДДС за {prev_day.date()}")

    # Excel export
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        pivot_today.reset_index().to_excel(writer, sheet_name="today", index=False)
        pivot_prev.reset_index().to_excel(writer, sheet_name="prev", index=False)
    buffer.seek(0)

    return text_today, text_prev, buffer


def build_cashflow_tables_for_period(client, dt_from: datetime, dt_to: datetime) -> tuple[str, io.BytesIO]:
    df = fetch_movements_dataframe(client, dt_from, dt_to)

    pivot = pd.pivot_table(
        df,
        values="amount",
        index=["accountName", "categoryName"],
        aggfunc="sum",
        fill_value=0.0,
    )

    # Build text
    lines: List[str] = [f"ДДС за период {dt_from.date()} — {dt_to.date()}"]
    for (acc, cat), row in pivot["amount"].items():
        lines.append(f"{acc} / {cat}: {row:.2f}")
    text = "\n".join(lines)

    # Excel export
    buffer = io.BytesIO()
    pivot.reset_index().to_excel(buffer, index=False)
    buffer.seek(0)
    return text, buffer


def build_detailed_cashflow_for_period(client, dt_from: datetime, dt_to: datetime) -> tuple[str, io.BytesIO]:
    df = fetch_movements_dataframe(client, dt_from, dt_to)

    # Detailed text: group by date, account, category
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    lines: List[str] = [f"Детальный ДДС за период {dt_from.date()} — {dt_to.date()}"]

    grouped = df.groupby(["date", "accountName", "categoryName"])]["amount"].sum().reset_index()
    for _, row in grouped.iterrows():
        lines.append(
            f"{row['date']} | {row['accountName']} | {row['categoryName']}: {row['amount']:.2f}"
        )
    text = "\n".join(lines)

    # Excel export
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)
    return text, buffer
