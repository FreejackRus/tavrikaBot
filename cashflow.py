import os
import pandas as pd
from typing import Dict, Any, Tuple


RU_ACCOUNT_MAIN = "Главная касса"
RU_ACCOUNT_TRADES = "Торговые кассы"

ACCOUNT_NAME_MAP = {
    "Main cash register": RU_ACCOUNT_MAIN,
    "Trade cash registers": RU_ACCOUNT_TRADES,
    RU_ACCOUNT_MAIN: RU_ACCOUNT_MAIN,
    RU_ACCOUNT_TRADES: RU_ACCOUNT_TRADES,
}

CATEGORY_RU_MAP = {
    # Common English -> Russian mappings
    "Internal transfer": "Внутреннее перемещение",
    "Invoices payment": "Оплата счетов",
    "Sales": "Выручка",
    "Revenue": "Выручка",
    "Loan": "Займ",
    "Loans": "Займ",
    # Already Russian values pass-through
    "Внутреннее перемещение": "Внутреннее перемещение",
    "Выручка": "Выручка",
}


def _normalize_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["AccountNorm"] = df["Account.Name"].map(ACCOUNT_NAME_MAP)
    df = df[df["AccountNorm"].isin({RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES})]
    return df


def build_cashflow_tables(raw_json: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw = raw_json.get("data", [])
    df = pd.json_normalize(raw)

    df = _normalize_accounts(df)

    # Ensure required columns exist
    for col in [
        "Sum.Incoming",
        "Sum.Outgoing",
        "FinalBalance.Money",
        "StartBalance.Money",
        "CashFlowCategory.HierarchyLevel1",
        "CashFlowCategory.HierarchyLevel2",
        "CashFlowCategory.HierarchyLevel3",
        "CashFlowCategory.Type",
    ]:
        if col not in df.columns:
            df[col] = 0

    cat_col = "CashFlowCategory.HierarchyLevel1"
    type_col = "CashFlowCategory.Type"

    # Отдельно выделим строки балансов (без категории) и движения (с категорией)
    balances_df = df[df[cat_col].isna()] if df[cat_col].isna().any() else df[df[cat_col] == None]
    # Include all flow types (OPERATIONAL and FINANCE)
    flows_mask = (df[cat_col].notna() if df[cat_col].notna().any() else df[cat_col] != None)
    flows_df = df[flows_mask]

    # 1) Остаток на начало (из строк без категории: StartBalance.Money)
    start_bal = (
        balances_df.groupby("AccountNorm")["StartBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )
    start_row = pd.DataFrame([start_bal.values], index=["Остаток на начало"], columns=[RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES])

    # 2) Обороты по категориям и типам: приход и расход отдельно
    incoming = (
        flows_df.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Incoming"].sum(min_count=1)
        .unstack(fill_value=0)
    )
    outgoing = (
        flows_df.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Outgoing"].sum(min_count=1)
        .unstack(fill_value=0)
    )

    # Map categories to Russian labels and aggregate duplicates
    if not incoming.empty:
        incoming.index = incoming.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        incoming = incoming.groupby(level=[0, 1]).sum()
    if not outgoing.empty:
        outgoing.index = outgoing.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        outgoing = outgoing.groupby(level=[0, 1]).sum()

    # Ensure columns exist
    for frame in (incoming, outgoing):
        for col in [RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]:
            if col not in frame.columns:
                frame[col] = 0

    # 3) Остаток на конец (из строк без категории: FinalBalance.Money)
    end_bal = (
        balances_df.groupby("AccountNorm")["FinalBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )
    end_row = pd.DataFrame([end_bal.values], index=["Остаток на конец"], columns=[RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES])

    # 4) Итоговая таблица в нужном порядке
    rows = []
    # Начальные остатки
    rows.append({
        "Тип статьи": "Остаток на начало",
        "Статья ДДС 1-го уровня": "",
        "Статья ДДС 2-го уровня": "",
        "Статья ДДС 3-го уровня": "",
        RU_ACCOUNT_MAIN: float(start_bal.get(RU_ACCOUNT_MAIN, 0)),
        RU_ACCOUNT_TRADES: float(start_bal.get(RU_ACCOUNT_TRADES, 0)),
    })

    # Движения по типам и категориям
    for flow_type in ["OPERATIONAL", "FINANCE"]:
        type_label = "Операционная деятельность" if flow_type == "OPERATIONAL" else "Финансовая деятельность"
        
        # Фильтруем категории для текущего типа
        type_incoming = incoming[incoming.index.get_level_values(0) == flow_type] if not incoming.empty else pd.DataFrame()
        type_outgoing = outgoing[outgoing.index.get_level_values(0) == flow_type] if not outgoing.empty else pd.DataFrame()
        
        if not type_incoming.empty or not type_outgoing.empty:
            # Заголовок раздела
            rows.append({
                "Тип статьи": type_label,
                "Статья ДДС 1-го уровня": "",
                "Статья ДДС 2-го уровня": "",
                "Статья ДДС 3-го уровня": "",
                RU_ACCOUNT_MAIN: 0,
                RU_ACCOUNT_TRADES: 0,
            })

            # Категории текущего типа
            # Собираем множество категорий корректно, избегая конкатенации списков с приоритетом
            cats_in = list(type_incoming.index.get_level_values(1)) if not type_incoming.empty else []
            cats_out = list(type_outgoing.index.get_level_values(1)) if not type_outgoing.empty else []
            categories = sorted(set(cats_in) | set(cats_out))

            for cat in categories:
                # Приход
                if not type_incoming.empty and cat in type_incoming.index.get_level_values(1):
                    cat_incoming = type_incoming.xs((flow_type, cat), level=[0, 1])
                    rows.append({
                        "Тип статьи": type_label,
                        "Статья ДДС 1-го уровня": cat,
                        "Статья ДДС 2-го уровня": "",
                        "Статья ДДС 3-го уровня": "",
                        RU_ACCOUNT_MAIN: float(cat_incoming.get(RU_ACCOUNT_MAIN, 0)),
                        RU_ACCOUNT_TRADES: float(cat_incoming.get(RU_ACCOUNT_TRADES, 0)),
                    })

                # Расход (отрицательные значения)
                if not type_outgoing.empty and cat in type_outgoing.index.get_level_values(1):
                    cat_outgoing = type_outgoing.xs((flow_type, cat), level=[0, 1])
                    rows.append({
                        "Тип статьи": type_label,
                        "Статья ДДС 1-го уровня": cat,
                        "Статья ДДС 2-го уровня": "",
                        "Статья ДДС 3-го уровня": "",
                        RU_ACCOUNT_MAIN: -float(cat_outgoing.get(RU_ACCOUNT_MAIN, 0)),
                        RU_ACCOUNT_TRADES: -float(cat_outgoing.get(RU_ACCOUNT_TRADES, 0)),
                    })

    # Конечные остатки
    rows.append({
        "Тип статьи": "Остаток на конец",
        "Статья ДДС 1-го уровня": "",
        "Статья ДДС 2-го уровня": "",
        "Статья ДДС 3-го уровня": "",
        RU_ACCOUNT_MAIN: float(end_bal.get(RU_ACCOUNT_MAIN, 0)),
        RU_ACCOUNT_TRADES: float(end_bal.get(RU_ACCOUNT_TRADES, 0)),
    })

    # Создаем DataFrame с нужными колонками
    result = pd.DataFrame(rows, columns=[
        "Тип статьи",
        "Статья ДДС 1-го уровня",
        "Статья ДДС 2-го уровня",
        "Статья ДДС 3-го уровня",
        RU_ACCOUNT_MAIN,
        RU_ACCOUNT_TRADES,
    ])

    # Add total column
    result["Итого"] = result[[RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]].sum(axis=1)

    return result, df


def build_cashflow_detailed_table(raw_json: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw = raw_json.get("data", [])
    df = pd.json_normalize(raw)

    df = _normalize_accounts(df)

    # Ensure required columns exist
    for col in [
        "Sum.Incoming",
        "Sum.Outgoing",
        "FinalBalance.Money",
        "StartBalance.Money",
        "CashFlowCategory.HierarchyLevel1",
        "CashFlowCategory.HierarchyLevel2",
        "CashFlowCategory.HierarchyLevel3",
        "CashFlowCategory.Type",
    ]:
        if col not in df.columns:
            df[col] = 0

    cat_col = "CashFlowCategory.HierarchyLevel1"
    type_col = "CashFlowCategory.Type"

    # Балансы без категории
    # NA masking: use isna()/notna() consistently, avoiding element-wise comparisons to None
    balances_df = df[df[cat_col].isna()]
    # Движения по периоду: включаем все типы (OPERATIONAL и FINANCE)
    flows_mask = df[cat_col].notna()
    flows_df = df[flows_mask]

    # Остаток на начало
    start_bal = (
        balances_df.groupby("AccountNorm")["StartBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )

    # Разбивка по категориям: приход и расход отдельно
    incoming = (
        flows_df.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Incoming"].sum(min_count=1)
        .unstack(fill_value=0)
    )
    outgoing = (
        flows_df.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Outgoing"].sum(min_count=1)
        .unstack(fill_value=0)
    )

    # Normalize categories to Russian (2-й уровень индекса)
    if not incoming.empty:
        incoming.index = incoming.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        incoming = incoming.groupby(level=[0, 1]).sum()
    if not outgoing.empty:
        outgoing.index = outgoing.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        outgoing = outgoing.groupby(level=[0, 1]).sum()

    # Ensure columns exist
    for frame in (incoming, outgoing):
        for col in [RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]:
            if col not in frame.columns:
                frame[col] = 0

    def _get_vals_by_type(frame: pd.DataFrame, flow_type: str, cat: str) -> Tuple[float, float]:
        if frame.empty or (flow_type, cat) not in frame.index:
            return (0.0, 0.0)
        row = frame.loc[(flow_type, cat)]
        return float(row.get(RU_ACCOUNT_MAIN, 0) or 0), float(row.get(RU_ACCOUNT_TRADES, 0) or 0)

    # Собираем строки детальной таблицы
    rows = []

    def add_row(type_label: str, l1: str = "", l2: str = "", l3: str = "", inout: str = "", main_val: float = 0.0, trade_val: float = 0.0):
        total = (main_val or 0) + (trade_val or 0)
        rows.append({
            "Тип статьи": type_label,
            "Статья ДДС 1-го уровня": l1,
            "Статья ДДС 2-ого уровня": l2,
            "Статья ДДС 3-ого уровня": l3,
            "Приход/Расход": inout,
            RU_ACCOUNT_MAIN: main_val,
            RU_ACCOUNT_TRADES: trade_val,
            "Итого": total,
        })

    # Row: Остаток на начало
    add_row("Остаток на начало", main_val=float(start_bal.get(RU_ACCOUNT_MAIN, 0)), trade_val=float(start_bal.get(RU_ACCOUNT_TRADES, 0)))

    # Движения по типам и категориям за период: включаем обе категории деятельности
    for flow_type in ["OPERATIONAL", "FINANCE"]:
        type_label = "Операционная деятельность" if flow_type == "OPERATIONAL" else "Финансовая деятельность"

        type_incoming = incoming[incoming.index.get_level_values(0) == flow_type] if not incoming.empty else pd.DataFrame()
        type_outgoing = outgoing[outgoing.index.get_level_values(0) == flow_type] if not outgoing.empty else pd.DataFrame()

        if not type_incoming.empty or not type_outgoing.empty:
            # Заголовок/раздел
            add_row(type_label)

            # Категории текущего типа
            cats_in = list(type_incoming.index.get_level_values(1)) if not type_incoming.empty else []
            cats_out = list(type_outgoing.index.get_level_values(1)) if not type_outgoing.empty else []
            categories = sorted(set(cats_in) | set(cats_out))

            for cat in categories:
                # Приход
                if not type_incoming.empty and cat in type_incoming.index.get_level_values(1):
                    in_main, in_trades = _get_vals_by_type(incoming, flow_type, cat)
                    add_row(type_label, cat, inout="Приход", main_val=in_main, trade_val=in_trades)

                # Расход (отображаем отрицательным)
                if not type_outgoing.empty and cat in type_outgoing.index.get_level_values(1):
                    out_main, out_trades = _get_vals_by_type(outgoing, flow_type, cat)
                    add_row(type_label, cat, inout="Расход", main_val=-out_main, trade_val=-out_trades)

    # Остаток на конец
    end_bal = (
        balances_df.groupby("AccountNorm")["FinalBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )
    add_row("Остаток на конец", main_val=float(end_bal.get(RU_ACCOUNT_MAIN, 0)), trade_val=float(end_bal.get(RU_ACCOUNT_TRADES, 0)))

    detailed = pd.DataFrame(rows, columns=[
        "Тип статьи",
        "Статья ДДС 1-го уровня",
        "Статья ДДС 2-ого уровня",
        "Статья ДДС 3-ого уровня",
        "Приход/Расход",
        RU_ACCOUNT_MAIN,
        RU_ACCOUNT_TRADES,
        "Итого",
    ])

    return detailed, df


def calculate_daily_movement(current: pd.DataFrame, previous: pd.DataFrame, account: str, type_col: str, cat_col: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate incoming and outgoing movements for the day by comparing current and previous data."""
    # Current day totals
    curr_incoming = (
        current.groupby([type_col, cat_col, "AccountNorm"])["Sum.Incoming"]  # type: ignore
        .sum(min_count=1)
        .unstack(fill_value=0)
    )
    curr_outgoing = (
        current.groupby([type_col, cat_col, "AccountNorm"])["Sum.Outgoing"]  # type: ignore
        .sum(min_count=1)
        .unstack(fill_value=0)
    )

    # Previous day totals
    prev_incoming = (
        previous.groupby([type_col, cat_col, "AccountNorm"])["Sum.Incoming"]  # type: ignore
        .sum(min_count=1)
        .unstack(fill_value=0)
    )
    prev_outgoing = (
        previous.groupby([type_col, cat_col, "AccountNorm"])["Sum.Outgoing"]  # type: ignore
        .sum(min_count=1)
        .unstack(fill_value=0)
    )

    # Calculate daily movements (current - previous)
    daily_incoming = curr_incoming.sub(prev_incoming, fill_value=0)
    daily_outgoing = curr_outgoing.sub(prev_outgoing, fill_value=0)

    return daily_incoming, daily_outgoing


def build_cashflow_tables_for_day(raw_json_day: Dict[str, Any], raw_json_prev: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    day = pd.json_normalize(raw_json_day.get("data", []))
    prev = pd.json_normalize(raw_json_prev.get("data", []))

    day = _normalize_accounts(day)
    prev = _normalize_accounts(prev)

    for df in (day, prev):
        for col in [
            "Sum.Incoming",
            "Sum.Outgoing",
            "FinalBalance.Money",
            "StartBalance.Money",
            "CashFlowCategory.HierarchyLevel1",
            "CashFlowCategory.HierarchyLevel2",
            "CashFlowCategory.HierarchyLevel3",
            "CashFlowCategory.Type",
        ]:
            if col not in df.columns:
                df[col] = 0

    cat_col = "CashFlowCategory.HierarchyLevel1"
    type_col = "CashFlowCategory.Type"

    # Start balance at beginning of day: previous day's final balances (no category)
    prev_balances = prev[prev[cat_col].isna()]
    start_bal = (
        prev_balances.groupby("AccountNorm")["FinalBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )

    # Day flows (include all types)
    flows_mask_day = (day[cat_col].notna() if day[cat_col].notna().any() else day[cat_col] != None)
    flows_mask_prev = (prev[cat_col].notna() if prev[cat_col].notna().any() else prev[cat_col] != None)
    flows_day = day[flows_mask_day]
    flows_prev = prev[flows_mask_prev]

    # Calculate daily movements
    incoming, outgoing = calculate_daily_movement(flows_day, flows_prev, "AccountNorm", type_col, cat_col)

    # Map categories to Russian labels and aggregate duplicates
    if not incoming.empty:
        incoming.index = incoming.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        incoming = incoming.groupby(level=[0, 1]).sum()
    if not outgoing.empty:
        outgoing.index = outgoing.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        outgoing = outgoing.groupby(level=[0, 1]).sum()

    # Ensure columns exist
    for frame in (incoming, outgoing):
        for col in [RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]:
            if col not in frame.columns:
                frame[col] = 0

    # Calculate end balance using daily movements
    net_by_acc = pd.Series(0, index=[RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES])
    if not incoming.empty and not outgoing.empty:
        for acc in [RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]:
            total_in = incoming.sum()[acc] if acc in incoming.columns else 0
            total_out = outgoing.sum()[acc] if acc in outgoing.columns else 0
            net_by_acc[acc] = total_in - total_out

    end_bal = start_bal + net_by_acc

    # Build result DataFrame
    rows = []
    # Начальные остатки
    rows.append({
        "Тип статьи": "Остаток на начало",
        "Статья ДДС 1-го уровня": "",
        "Статья ДДС 2-го уровня": "",
        "Статья ДДС 3-го уровня": "",
        RU_ACCOUNT_MAIN: float(start_bal.get(RU_ACCOUNT_MAIN, 0)),
        RU_ACCOUNT_TRADES: float(start_bal.get(RU_ACCOUNT_TRADES, 0)),
    })

    # Движения по типам и категориям
    for flow_type in ["OPERATIONAL", "FINANCE"]:
        type_label = "Операционная деятельность" if flow_type == "OPERATIONAL" else "Финансовая деятельность"
        
        # Фильтруем категории для текущего типа
        type_incoming = incoming[incoming.index.get_level_values(0) == flow_type] if not incoming.empty else pd.DataFrame()
        type_outgoing = outgoing[outgoing.index.get_level_values(0) == flow_type] if not outgoing.empty else pd.DataFrame()
        
        if not type_incoming.empty or not type_outgoing.empty:
            # Заголовок раздела
            rows.append({
                "Тип статьи": type_label,
                "Статья ДДС 1-го уровня": "",
                "Статья ДДС 2-го уровня": "",
                "Статья ДДС 3-го уровня": "",
                RU_ACCOUNT_MAIN: 0,
                RU_ACCOUNT_TRADES: 0,
            })

            # Категории текущего типа (корректное объединение множеств)
            cats_in = list(type_incoming.index.get_level_values(1)) if not type_incoming.empty else []
            cats_out = list(type_outgoing.index.get_level_values(1)) if not type_outgoing.empty else []
            categories = sorted(set(cats_in) | set(cats_out))

            for cat in categories:
                # Приход
                if not type_incoming.empty and cat in type_incoming.index.get_level_values(1):
                    cat_incoming = type_incoming.xs((flow_type, cat), level=[0, 1])
                    rows.append({
                        "Тип статьи": type_label,
                        "Статья ДДС 1-го уровня": cat,
                        "Статья ДДС 2-го уровня": "",
                        "Статья ДДС 3-го уровня": "",
                        RU_ACCOUNT_MAIN: float(cat_incoming.get(RU_ACCOUNT_MAIN, 0)),
                        RU_ACCOUNT_TRADES: float(cat_incoming.get(RU_ACCOUNT_TRADES, 0)),
                    })

                # Расход (отрицательные значения)
                if not type_outgoing.empty and cat in type_outgoing.index.get_level_values(1):
                    cat_outgoing = type_outgoing.xs((flow_type, cat), level=[0, 1])
                    rows.append({
                        "Тип статьи": type_label,
                        "Статья ДДС 1-го уровня": cat,
                        "Статья ДДС 2-го уровня": "",
                        "Статья ДДС 3-го уровня": "",
                        RU_ACCOUNT_MAIN: -float(cat_outgoing.get(RU_ACCOUNT_MAIN, 0)),
                        RU_ACCOUNT_TRADES: -float(cat_outgoing.get(RU_ACCOUNT_TRADES, 0)),
                    })

    # Конечные остатки
    rows.append({
        "Тип статьи": "Остаток на конец",
        "Статья ДДС 1-го уровня": "",
        "Статья ДДС 2-го уровня": "",
        "Статья ДДС 3-го уровня": "",
        RU_ACCOUNT_MAIN: float(end_bal.get(RU_ACCOUNT_MAIN, 0)),
        RU_ACCOUNT_TRADES: float(end_bal.get(RU_ACCOUNT_TRADES, 0)),
    })

    # Создаем DataFrame с нужными колонками
    result = pd.DataFrame(rows, columns=[
        "Тип статьи",
        "Статья ДДС 1-го уровня",
        "Статья ДДС 2-го уровня",
        "Статья ДДС 3-го уровня",
        RU_ACCOUNT_MAIN,
        RU_ACCOUNT_TRADES,
    ])

    # Add total column
    result["Итого"] = result[[RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]].sum(axis=1)

    return result, day


def build_cashflow_detailed_table_for_day(raw_json_day: Dict[str, Any], raw_json_prev: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    day = pd.json_normalize(raw_json_day.get("data", []))
    prev = pd.json_normalize(raw_json_prev.get("data", []))

    day = _normalize_accounts(day)
    prev = _normalize_accounts(prev)

    for df in (day, prev):
        for col in [
            "Sum.Incoming",
            "Sum.Outgoing",
            "FinalBalance.Money",
            "StartBalance.Money",
            "CashFlowCategory.HierarchyLevel1",
            "CashFlowCategory.HierarchyLevel2",
            "CashFlowCategory.HierarchyLevel3",
            "CashFlowCategory.Type",
        ]:
            if col not in df.columns:
                df[col] = 0

    cat_col = "CashFlowCategory.HierarchyLevel1"
    type_col = "CashFlowCategory.Type"

    prev_balances = prev[prev[cat_col].isna()] if prev[cat_col].isna().any() else prev[prev[cat_col] == None]
    start_bal = (
        prev_balances.groupby("AccountNorm")["FinalBalance.Money"].sum().reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]).fillna(0)
    )

    flows_mask = day[cat_col].notna()
    # В детальном отчёте за день учитываем все типы (OPERATIONAL и FINANCE), чтобы не терялась категория "Займ"
    flows_day = day[flows_mask]

    incoming = (
        flows_day.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Incoming"].sum(min_count=1)
        .unstack(fill_value=0)
    )
    outgoing = (
        flows_day.groupby([type_col, cat_col, "AccountNorm"])  # type: ignore
        ["Sum.Outgoing"].sum(min_count=1)
        .unstack(fill_value=0)
    )

    if not incoming.empty:
        incoming.index = incoming.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        incoming = incoming.groupby(level=[0, 1]).sum()
    if not outgoing.empty:
        outgoing.index = outgoing.index.map(lambda x: (x[0], CATEGORY_RU_MAP.get(x[1], str(x[1]))))
        outgoing = outgoing.groupby(level=[0, 1]).sum()

    for frame in (incoming, outgoing):
        for col in [RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES]:
            if col not in frame.columns:
                frame[col] = 0

    def _get_vals(frame: pd.DataFrame, cat: str) -> Tuple[float, float]:
        if frame.empty or cat not in frame.index:
            return (0.0, 0.0)
        row = frame.loc[cat]
        return float(row.get(RU_ACCOUNT_MAIN, 0) or 0), float(row.get(RU_ACCOUNT_TRADES, 0) or 0)

    rows = []

    def add_row(type_label: str, l1: str = "", l2: str = "", l3: str = "", inout: str = "", main_val: float = 0.0, trade_val: float = 0.0):
        total = (main_val or 0) + (trade_val or 0)
        rows.append({
            "Тип статьи": type_label,
            "Статья ДДС 1-го уровня": l1,
            "Статья ДДС 2-ого уровня": l2,
            "Статья ДДС 3-ого уровня": l3,
            "Приход/Расход": inout,
            RU_ACCOUNT_MAIN: main_val,
            RU_ACCOUNT_TRADES: trade_val,
            "Итого": total,
        })

    # Start of day
    add_row("Остаток на начало", main_val=float(start_bal.get(RU_ACCOUNT_MAIN, 0)), trade_val=float(start_bal.get(RU_ACCOUNT_TRADES, 0)))

    # Движения по типам и категориям за день: включаем обе категории деятельности
    for flow_type in ["OPERATIONAL", "FINANCE"]:
        type_label = "Операционная деятельность" if flow_type == "OPERATIONAL" else "Финансовая деятельность"

        type_incoming = incoming[incoming.index.get_level_values(0) == flow_type] if not incoming.empty else pd.DataFrame()
        type_outgoing = outgoing[outgoing.index.get_level_values(0) == flow_type] if not outgoing.empty else pd.DataFrame()

        if not type_incoming.empty or not type_outgoing.empty:
            # Заголовок/раздел
            add_row(type_label)

            # Категории текущего типа
            cats_in = list(type_incoming.index.get_level_values(1)) if not type_incoming.empty else []
            cats_out = list(type_outgoing.index.get_level_values(1)) if not type_outgoing.empty else []
            categories = sorted(set(cats_in) | set(cats_out))

            for cat in categories:
                # Приход
                if not type_incoming.empty and cat in type_incoming.index.get_level_values(1):
                    cat_incoming = type_incoming.xs((flow_type, cat), level=[0, 1])
                    add_row(type_label, cat, inout="Приход",
                            main_val=float(cat_incoming.get(RU_ACCOUNT_MAIN, 0)),
                            trade_val=float(cat_incoming.get(RU_ACCOUNT_TRADES, 0)))

                # Расход (отображаем отрицательным)
                if not type_outgoing.empty and cat in type_outgoing.index.get_level_values(1):
                    cat_outgoing = type_outgoing.xs((flow_type, cat), level=[0, 1])
                    add_row(type_label, cat, inout="Расход",
                            main_val=-float(cat_outgoing.get(RU_ACCOUNT_MAIN, 0)),
                            trade_val=-float(cat_outgoing.get(RU_ACCOUNT_TRADES, 0)))

    net_by_acc = (
        flows_day.groupby("AccountNorm").apply(lambda x: (x["Sum.Incoming"].fillna(0) - x["Sum.Outgoing"].fillna(0)).sum())
        .reindex([RU_ACCOUNT_MAIN, RU_ACCOUNT_TRADES])
        .fillna(0)
    )
    end_bal = start_bal + net_by_acc
    add_row("Остаток на конец", main_val=float(end_bal.get(RU_ACCOUNT_MAIN, 0)), trade_val=float(end_bal.get(RU_ACCOUNT_TRADES, 0)))

    detailed = pd.DataFrame(rows, columns=[
        "Тип статьи",
        "Статья ДДС 1-го уровня",
        "Статья ДДС 2-ого уровня",
        "Статья ДДС 3-ого уровня",
        "Приход/Расход",
        RU_ACCOUNT_MAIN,
        RU_ACCOUNT_TRADES,
        "Итого",
    ])

    return detailed, day


def export_to_excel(result: pd.DataFrame, path: str = "cashflow_pivot.xlsx") -> str:
    try:
        result.to_excel(path, sheet_name="ДДС", index=False)
        return path
    except PermissionError:
        # Файл может быть открыт в Excel — сохраняем под новым именем с меткой времени
        from datetime import datetime
        root, ext = os.path.splitext(path)
        ext = ext or ".xlsx"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt_path = f"{root}_{ts}{ext}"
        result.to_excel(alt_path, sheet_name="ДДС", index=False)
        return alt_path


def dataframe_to_text_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    display_df = df.copy()
    if len(display_df) > max_rows:
        display_df = display_df.head(max_rows)
    def _fmt(v):
        if isinstance(v, (int, float)):
            # Русский формат: пробел как разделитель тысяч и запятая как десятичный
            return f"{v:,.2f}".replace(",", " ").replace(".", ",")
        return str(v)
    formatted = display_df.map(_fmt)
    return formatted.to_string()