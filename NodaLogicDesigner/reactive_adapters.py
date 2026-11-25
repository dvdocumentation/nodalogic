"""Read-only adapters exposed to Reactive formulas for ERP data providers."""
from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any


def ql_quant(*parts: Any) -> str:
    """Build a QuantLedger selector directly from formula arguments."""
    from quant_ledger.api import quant
    return quant(*parts)


def ql_sum(config_uid: str, space: Any, selector_quant: Any = None,
           resource_index: Any = 0, period_from: Any = None,
           period_to: Any = None, metric: Any = "net") -> float:
    """Sum one QuantLedger resource over a selector and optional period.

    ``metric`` is ``income``, ``expense``, ``net`` or ``closing``. Resource
    indexes are zero-based, matching QuantLedger's Python API.
    """
    from quant_ledger import api as ledger

    index = int(resource_index)
    if index < 0 or index >= ledger.MAX_RESOURCES:
        raise ValueError("QL_SUM resource_index must be between 0 and 15")
    measure = str(metric or "net").strip().lower()
    if measure not in {"income", "expense", "net", "closing", "opening"}:
        raise ValueError("QL_SUM metric must be income, expense, net, closing or opening")
    rows = ledger.statement(
        str(space or ""), selector_quant=selector_quant,
        period_from=period_from, period_to=period_to, scope=str(config_uid or ""),
    )
    total = Decimal(0)
    for row in rows:
        if measure == "net":
            total += row.income[index] - row.expense[index]
        else:
            total += getattr(row, measure)[index]
    return float(total)


def ql_month_sum(config_uid: str, space: Any, selector_quant: Any,
                 resource_index: Any, month: Any, metric: Any = "net") -> float:
    """Sum one QL resource across the calendar month containing ``month``."""
    raw = str(month or "")
    if isinstance(month, (date, datetime)):
        year, month_number = month.year, month.month
    else:
        parsed = date.fromisoformat(raw[:10])
        year, month_number = parsed.year, parsed.month
    first = date(year, month_number, 1).isoformat()
    last = date(year, month_number, monthrange(year, month_number)[1]).isoformat()
    return ql_sum(config_uid, space, selector_quant, resource_index, first, last, metric)


def cg_scope(config_uid: str):
    from cost_graph.store import Scope

    return Scope(config_uid, config_uid, "operational", "actual", "RUB")


def cg_value(config_uid: str, bucket: Any) -> float:
    """Read a published N50 CostGraph bucket value for the current config."""
    config = str(config_uid or "").strip()
    if not config:
        raise ValueError("CG_VALUE requires a configuration")
    from nodes import STORAGE_BASE_PATH
    from cost_graph.store import Store

    directory = Path(STORAGE_BASE_PATH)
    path = directory / f"erp_journal_{config}.sqlite"
    if not path.is_file():
        return 0.0
    with closing(sqlite3.connect(path, timeout=30)) as journal, journal:
        journal.execute("BEGIN IMMEDIATE")
        if journal.execute("SELECT 1 FROM posting WHERE posted=0 LIMIT 1").fetchone():
            raise ValueError("CostGraph has no ready publication")
        snapshot = Store(directory / f"erp_cost_{config}.sqlite").recalculate(cg_scope(config))
    if snapshot is None:
        return 0.0
    if snapshot.get("status") != "ready":
        raise ValueError("CostGraph has no ready publication")
    value = (snapshot.get("values") or {}).get(str(bucket or ""))
    if value is None:
        return 0.0
    return float(Fraction(str(value)))


def cg_month_sum(config_uid: str, department_ref: Any, period: Any) -> float:
    """Read a published CostGraph total for a department and calendar month."""
    department = str(department_ref or "").strip()
    if not department:
        return 0.0
    return cg_value(config_uid, f"{department}|cost-center|{str(period)[:7]}")
