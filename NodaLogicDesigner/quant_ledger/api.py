# -*- coding: utf-8 -*-
"""Optional SQL-backed quant ledger API for NodaLogic server handlers.

The package is intentionally optional.  ``app.py`` imports it only when this
folder is present and calls :func:`ensure_schema`.  Business handlers import the
functions they need explicitly, for example::

    from quant_ledger.api import quant, move, transaction, select_balances

Concepts
--------
``quant``
    Full analytical key.  It is the only key that identifies one balance row.
``selector_quant``
    A second, coarser quant stored in a dedicated indexed SQL column.  It is
    used only to fetch a group of balances quickly.
``details``
    Movement-only JSON attributes such as document, employee or comment.
``resources``
    Up to ``MAX_RESOURCES`` additive numeric values.  SQL stores fixed-point
    integers and applies deltas with ``resourceN = resourceN + delta``.

Every standalone :func:`move` is atomic: movement insert and balance update are
committed together.  Several moves may optionally be wrapped in
:func:`transaction`; then all of them commit or roll back together.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, unquote

from extensions import db

# Fixed storage contract: changing these values after data exists would reinterpret
# SQL columns, so they are intentionally not environment-configurable.
MAX_RESOURCES = 16
RESOURCE_SCALE_DIGITS = 6
RESOURCE_SCALE = 10 ** RESOURCE_SCALE_DIGITS
SQLITE_BUSY_TIMEOUT_MS = max(1000, int(os.getenv("QUANT_LEDGER_BUSY_TIMEOUT_MS", "30000") or 30000))
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

BALANCE_TABLE = "quant_ledger_balance"
MOVEMENT_TABLE = "quant_ledger_movement"
API_VERSION = 2

_ENGINE_OVERRIDE = None


class QuantLedgerError(RuntimeError):
    """Base error for the optional quant ledger."""


class QuantFormatError(QuantLedgerError):
    pass


class ScopeRequiredError(QuantLedgerError):
    pass


class ResourceError(QuantLedgerError):
    pass


class NegativeBalanceError(QuantLedgerError):
    def __init__(self, *, scope: str, space: str, quant_value: str, resource_index: int, attempted: Decimal):
        self.scope = scope
        self.space = space
        self.quant = quant_value
        self.resource_index = resource_index
        self.attempted = attempted
        super().__init__(
            f"Negative balance is not allowed: scope={scope!r}, space={space!r}, "
            f"resource={resource_index}, attempted={attempted}"
        )


class OperationConflictError(QuantLedgerError):
    pass


class SelectorConflictError(QuantLedgerError):
    pass


@dataclass(frozen=True)
class MoveResult:
    scope: str
    space: str
    operation_id: str
    quant: str
    selector_quant: str
    resources: Tuple[Decimal, ...]
    reposted: bool = False
    replaced_movements: int = 0
    # Backward-compatible field for old handlers. Reposting is never a no-op,
    # therefore this value is always False in the new posting model.
    already_applied: bool = False


@dataclass(frozen=True)
class BalanceRow:
    scope: str
    space: str
    quant: str
    selector_quant: str
    resources: Tuple[Decimal, ...]
    version: int = 0
    updated_at: str = ""

    @property
    def parts(self) -> Tuple[Any, ...]:
        return parse_quant(self.quant)

    @property
    def selector_parts(self) -> Tuple[Any, ...]:
        return parse_quant(self.selector_quant) if self.selector_quant else tuple()


@dataclass(frozen=True)
class MovementRow:
    id: int
    scope: str
    space: str
    operation_id: str
    period: str
    quant: str
    selector_quant: str
    details: Dict[str, Any]
    resources: Tuple[Decimal, ...]
    created_at: str = ""

    @property
    def parts(self) -> Tuple[Any, ...]:
        return parse_quant(self.quant)


@dataclass(frozen=True)
class StatementRow:
    scope: str
    space: str
    quant: str
    selector_quant: str
    opening: Tuple[Decimal, ...]
    income: Tuple[Decimal, ...]
    expense: Tuple[Decimal, ...]
    closing: Tuple[Decimal, ...]

    @property
    def parts(self) -> Tuple[Any, ...]:
        return parse_quant(self.quant)


@dataclass(frozen=True)
class VerifyResult:
    scope: str
    space: str
    valid: bool
    checked_quants: int
    errors: Tuple[Dict[str, Any], ...]


def configure_engine(engine: Any) -> None:
    """Testing/embedding hook. Normal NodaLogic code should not call this."""
    global _ENGINE_OVERRIDE
    _ENGINE_OVERRIDE = engine


def _engine():
    return _ENGINE_OVERRIDE or db.engine


def available() -> bool:
    return True


def _resource_columns(prefix: str = "resource") -> List[str]:
    return [f"{prefix}{i}" for i in range(1, MAX_RESOURCES + 1)]


def ensure_schema() -> None:
    """Create ledger tables and indexes if this optional package is present."""
    global _ENGINE_OVERRIDE
    engine = _engine()
    # Cache the already-resolved Flask-SQLAlchemy engine. Server handlers may
    # later run in worker threads where Flask's application context is not
    # propagated, but the Engine itself is safe to reuse.
    if _ENGINE_OVERRIDE is None:
        _ENGINE_OVERRIDE = engine
    if engine.dialect.name != "sqlite":
        raise QuantLedgerError(
            f"quant_ledger currently supports SQLite only; got {engine.dialect.name!r}"
        )

    resource_sql = ",\n".join(
        f"                    resource{i} INTEGER NOT NULL DEFAULT 0"
        for i in range(1, MAX_RESOURCES + 1)
    )
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {MOVEMENT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope VARCHAR(128) NOT NULL,
                operation_id VARCHAR(255) NOT NULL,
                period VARCHAR(40) NOT NULL,
                space VARCHAR(128) NOT NULL,
                quant TEXT NOT NULL,
                quant_hash CHAR(64) NOT NULL,
                selector_quant TEXT NOT NULL DEFAULT '',
                selector_hash CHAR(64) NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{{}}',
                payload_hash CHAR(64) NOT NULL,
{resource_sql},
                created_at VARCHAR(40) NOT NULL,
                UNIQUE(scope, operation_id, space, quant_hash)
            )
            """
        )
        conn.exec_driver_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {BALANCE_TABLE} (
                scope VARCHAR(128) NOT NULL,
                space VARCHAR(128) NOT NULL,
                quant TEXT NOT NULL,
                quant_hash CHAR(64) NOT NULL,
                selector_quant TEXT NOT NULL DEFAULT '',
                selector_hash CHAR(64) NOT NULL,
{resource_sql},
                version INTEGER NOT NULL DEFAULT 0,
                updated_at VARCHAR(40) NOT NULL,
                PRIMARY KEY(scope, space, quant_hash)
            )
            """
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS ix_ql_movement_period ON {MOVEMENT_TABLE}(scope, space, period)"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS ix_ql_movement_quant ON {MOVEMENT_TABLE}(scope, space, quant_hash, period)"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS ix_ql_movement_selector ON {MOVEMENT_TABLE}(scope, space, selector_hash, period)"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS ix_ql_movement_operation ON {MOVEMENT_TABLE}(scope, operation_id)"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS ix_ql_balance_selector ON {BALANCE_TABLE}(scope, space, selector_hash, quant_hash)"
        )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    node_id = getattr(value, "_id", None)
    if node_id is not None:
        return str(node_id)
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def _hash(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _encode_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise QuantFormatError("NaN and Infinity are not allowed in quant keys")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _encode_quant_part(value: Any) -> Tuple[str, str]:
    node_id = getattr(value, "_id", None)
    if node_id is not None:
        value = str(node_id)
    if value is None:
        return "n", ""
    if isinstance(value, bool):
        return "b", "1" if value else "0"
    if isinstance(value, int) and not isinstance(value, bool):
        return "i", str(value)
    if isinstance(value, Decimal):
        return "d", _encode_decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise QuantFormatError("NaN and Infinity are not allowed in quant keys")
        return "d", _encode_decimal(Decimal(str(value)))
    if isinstance(value, datetime):
        return "t", _normalize_period(value)
    if isinstance(value, date):
        return "D", value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        return "j", _canonical_json(value)
    return "s", str(value)


def quant(*keys: Any) -> str:
    """Build a canonical, reversible analytical key string."""
    encoded: List[str] = []
    for value in keys:
        tag, payload = _encode_quant_part(value)
        encoded.append(f"{tag}:{quote(payload, safe='')}")
    return "q1|" + "|".join(encoded)


def parse_quant(value: str) -> Tuple[Any, ...]:
    raw = str(value or "")
    if not raw.startswith("q1|"):
        raise QuantFormatError("Unsupported quant format")
    body = raw[3:]
    if body == "":
        return tuple()
    out: List[Any] = []
    for token in body.split("|"):
        if ":" not in token:
            raise QuantFormatError("Malformed quant token")
        tag, encoded = token.split(":", 1)
        payload = unquote(encoded)
        if tag == "n":
            out.append(None)
        elif tag == "b":
            out.append(payload == "1")
        elif tag == "i":
            out.append(int(payload))
        elif tag == "d":
            out.append(Decimal(payload))
        elif tag in {"t", "D", "s"}:
            out.append(payload)
        elif tag == "j":
            out.append(json.loads(payload))
        else:
            raise QuantFormatError(f"Unknown quant token type: {tag}")
    return tuple(out)


def quant_part(value: str, index: int, default: Any = None) -> Any:
    try:
        return parse_quant(value)[index]
    except (IndexError, QuantFormatError):
        return default


def _normalize_period(value: Any) -> str:
    if value is None or value == "":
        dt = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        parsed = None
        candidates = [text]
        if text.endswith("Z"):
            candidates.append(text[:-1] + "+00:00")
        for candidate in candidates:
            try:
                parsed = datetime.fromisoformat(candidate)
                break
            except Exception:
                pass
        if parsed is None:
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except Exception:
                    pass
        if parsed is None:
            raise QuantLedgerError(f"Unsupported period value: {value!r}")
        dt = parsed
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _normalize_period_bound(value: Any, *, end: bool) -> str:
    """Normalize report bounds; a date-only upper bound includes the whole day."""
    date_only = False
    if isinstance(value, date) and not isinstance(value, datetime):
        date_only = True
    elif isinstance(value, str):
        text = value.strip()
        date_only = bool(
            len(text) == 10
            and (
                (text[4:5] == "-" and text[7:8] == "-")
                or (text[2:3] == "." and text[5:6] == ".")
            )
        )
    normalized = _normalize_period(value)
    if end and date_only:
        return normalized[:10] + "T23:59:59.999999Z"
    return normalized


def _resolve_scope(scope: Optional[str] = None) -> str:
    explicit = str(scope or "").strip()
    if explicit:
        return explicit
    try:
        import nodes  # local import avoids a hard startup dependency cycle

        current = getattr(nodes, "CURRENT_NODE", None)
        uid = str(getattr(current, "_config_uid", "") or "").strip()
        if not uid:
            fn = getattr(nodes, "current_config_uid_from_handlers", None)
            if callable(fn):
                uid = str(fn() or "").strip()
        if not uid:
            ctx = getattr(nodes, "CURRENT_CONFIG_UID", None)
            if ctx is not None and hasattr(ctx, "get"):
                uid = str(ctx.get() or "").strip()
        if uid:
            return uid
    except Exception:
        pass
    raise ScopeRequiredError(
        "quant_ledger scope is unknown. Call it inside a server handler or pass scope= explicitly."
    )


def _validate_space(space: Any) -> str:
    value = str(space or "").strip()
    if not value:
        raise QuantLedgerError("space is required")
    if len(value) > 128:
        raise QuantLedgerError("space is too long")
    return value


def _normalize_quant(value: Any, *, optional: bool = False) -> str:
    if value is None and optional:
        return ""
    raw = str(value or "")
    if optional and raw == "":
        return ""
    parse_quant(raw)
    return raw


def _to_scaled(value: Any) -> int:
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value if value is not None else 0))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ResourceError(f"Invalid resource value: {value!r}") from exc
    if not dec.is_finite():
        raise ResourceError("NaN and Infinity are not allowed in resources")
    scaled_dec = dec * RESOURCE_SCALE
    integral = scaled_dec.to_integral_value(rounding=ROUND_DOWN)
    if scaled_dec != integral:
        raise ResourceError(
            f"Resource precision exceeds {RESOURCE_SCALE_DIGITS} decimal places: {value!r}"
        )
    scaled = int(integral)
    if scaled < INT64_MIN or scaled > INT64_MAX:
        raise ResourceError(f"Resource value is outside signed 64-bit range: {value!r}")
    return scaled


def _from_scaled(value: Any) -> Decimal:
    return Decimal(int(value or 0)) / Decimal(RESOURCE_SCALE)


def _normalize_resources(values: Optional[Sequence[Any]]) -> Tuple[int, ...]:
    if values is None:
        raise ResourceError("resources are required")
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise ResourceError("resources must be a list or tuple")
    if len(values) == 0:
        raise ResourceError("resources must contain at least one value")
    if len(values) > MAX_RESOURCES:
        raise ResourceError(f"At most {MAX_RESOURCES} resources are supported")
    result = [_to_scaled(v) for v in values]
    result.extend([0] * (MAX_RESOURCES - len(result)))
    return tuple(result)


def _public_resources(values: Sequence[Any]) -> Tuple[Decimal, ...]:
    return tuple(_from_scaled(values[i]) for i in range(MAX_RESOURCES))


def _nonnegative_indices(
    *,
    allow_negative: bool,
    nonnegative_resources: Optional[Iterable[int]],
) -> Tuple[int, ...]:
    if nonnegative_resources is None:
        return tuple() if bool(allow_negative) else (0,)
    out: List[int] = []
    for raw in nonnegative_resources:
        idx = int(raw)
        if idx < 0 or idx >= MAX_RESOURCES:
            raise ResourceError(f"Invalid nonnegative resource index: {idx}")
        if idx not in out:
            out.append(idx)
    return tuple(out)


def _row_dict(cursor, row) -> Dict[str, Any]:
    return {cursor.description[i][0]: row[i] for i in range(len(cursor.description))}


def _open_raw_connection():
    engine = _engine()
    if engine.dialect.name != "sqlite":
        raise QuantLedgerError(
            f"quant_ledger currently supports SQLite only; got {engine.dialect.name!r}"
        )
    raw = engine.raw_connection()
    cur = raw.cursor()
    cur.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    cur.execute("PRAGMA foreign_keys=ON")
    return raw


class LedgerTransaction:
    """Optional transaction that can link several otherwise independent moves."""

    def __init__(self, *, scope: Optional[str] = None):
        self.scope = _resolve_scope(scope)
        self._connection = None
        self._closed = False
        self._failed = False
        self._failure = None

    def __enter__(self) -> "LedgerTransaction":
        if self._connection is not None:
            raise QuantLedgerError("Transaction is already open")
        ensure_schema()
        self._connection = _open_raw_connection()
        cursor = self._connection.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._connection is None:
            return False
        rollback_only = exc_type is not None or self._failed
        try:
            if rollback_only:
                self._connection.rollback()
            else:
                self._connection.commit()
        finally:
            self._connection.close()
            self._connection = None
            self._closed = True
        if exc_type is None and self._failed:
            raise QuantLedgerError(
                "A move failed inside the transaction; all linked moves were rolled back"
            ) from self._failure
        return False

    def move(
        self,
        space: str,
        quant: str,
        period: Any,
        operation_id: str,
        details: Optional[Mapping[str, Any]],
        resources: Sequence[Any],
        *,
        selector_quant: Optional[str] = None,
        allow_negative: bool = False,
        nonnegative_resources: Optional[Iterable[int]] = None,
    ) -> MoveResult:
        if self._connection is None:
            raise QuantLedgerError("Transaction is not open")
        if self._failed:
            raise QuantLedgerError("Transaction is marked for rollback after a failed move")
        try:
            return _apply_move(
                self._connection,
                scope=self.scope,
                space=space,
                quant_value=quant,
                selector_quant_value=selector_quant,
                period=period,
                operation_id=operation_id,
                details=details,
                resources=resources,
                allow_negative=allow_negative,
                nonnegative_resources=nonnegative_resources,
            )
        except Exception as exc:
            self._failed = True
            self._failure = exc
            raise


def transaction(*, scope: Optional[str] = None) -> LedgerTransaction:
    return LedgerTransaction(scope=scope)


def move(
    space: str,
    quant: str,
    period: Any,
    operation_id: str,
    details: Optional[Mapping[str, Any]],
    resources: Sequence[Any],
    *,
    selector_quant: Optional[str] = None,
    allow_negative: bool = False,
    nonnegative_resources: Optional[Iterable[int]] = None,
    scope: Optional[str] = None,
    tx: Optional[LedgerTransaction] = None,
) -> MoveResult:
    """Post or repost one logical movement atomically.

    ``operation_id`` identifies exactly one logical movement inside a scope.
    When it already exists, all old rows with this id are removed from their
    balances and replaced by the new movement in the same SQL transaction.
    If validation of the resulting balances fails, the whole repost is rolled
    back and the previous movement remains unchanged.
    """
    if tx is not None:
        resolved = _resolve_scope(scope) if scope else tx.scope
        if resolved != tx.scope:
            raise QuantLedgerError("move scope differs from transaction scope")
        return tx.move(
            space,
            quant,
            period,
            operation_id,
            details,
            resources,
            selector_quant=selector_quant,
            allow_negative=allow_negative,
            nonnegative_resources=nonnegative_resources,
        )
    with transaction(scope=scope) as local_tx:
        return local_tx.move(
            space,
            quant,
            period,
            operation_id,
            details,
            resources,
            selector_quant=selector_quant,
            allow_negative=allow_negative,
            nonnegative_resources=nonnegative_resources,
        )


def _apply_move(
    connection,
    *,
    scope: str,
    space: str,
    quant_value: str,
    selector_quant_value: Optional[str],
    period: Any,
    operation_id: str,
    details: Optional[Mapping[str, Any]],
    resources: Sequence[Any],
    allow_negative: bool,
    nonnegative_resources: Optional[Iterable[int]],
) -> MoveResult:
    space = _validate_space(space)
    quant_value = _normalize_quant(quant_value)
    selector_quant_value = _normalize_quant(selector_quant_value, optional=True)
    operation_id = str(operation_id or "").strip()
    if not operation_id:
        raise QuantLedgerError("operation_id is required")
    if len(operation_id) > 255:
        raise QuantLedgerError("operation_id is too long")

    period_value = _normalize_period(period)
    details_obj = dict(details or {})
    details_json = _canonical_json(details_obj)
    scaled = _normalize_resources(resources)
    checks = _nonnegative_indices(
        allow_negative=allow_negative,
        nonnegative_resources=nonnegative_resources,
    )
    quant_hash = _hash(quant_value)
    selector_hash = _hash(selector_quant_value)
    payload_hash = _hash(
        _canonical_json(
            {
                "period": period_value,
                "space": space,
                "quant": quant_value,
                "selector_quant": selector_quant_value,
                "details": details_obj,
                "resources": list(scaled),
            }
        )
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    cur = connection.cursor()

    resource_names = _resource_columns()
    cur.execute(
        f"""
        SELECT id, space, quant, quant_hash, selector_quant, selector_hash,
               {', '.join(resource_names)}
        FROM {MOVEMENT_TABLE}
        WHERE scope = ? AND operation_id = ?
        ORDER BY id
        """,
        (scope, operation_id),
    )
    old_rows = cur.fetchall()

    # Net deltas are calculated first. This is important for reposting a
    # receipt that has later expenses: the temporary reversal may be negative,
    # while the final balance after applying the new receipt is still valid.
    affected: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    def add_delta(
        item_space: str,
        item_quant: str,
        item_quant_hash: str,
        item_selector: str,
        item_selector_hash: str,
        delta: Sequence[int],
        *,
        requires_existing: bool,
    ) -> None:
        if _hash(item_quant) != item_quant_hash:
            raise OperationConflictError("Quant hash collision detected for existing movement")
        if _hash(item_selector) != item_selector_hash:
            raise OperationConflictError("Selector hash collision detected for existing movement")
        key = (item_space, item_quant_hash, item_quant)
        bucket = affected.get(key)
        if bucket is None:
            bucket = {
                "space": item_space,
                "quant": item_quant,
                "quant_hash": item_quant_hash,
                "selector_quant": item_selector,
                "selector_hash": item_selector_hash,
                "delta": [0] * MAX_RESOURCES,
                "requires_existing": bool(requires_existing),
            }
            affected[key] = bucket
        else:
            if bucket["selector_quant"] != item_selector:
                raise SelectorConflictError(
                    "One full quant cannot have different selector_quant values"
                )
            bucket["requires_existing"] = bool(bucket["requires_existing"] or requires_existing)
        for index, value in enumerate(delta):
            bucket["delta"][index] += int(value)
            if bucket["delta"][index] < INT64_MIN or bucket["delta"][index] > INT64_MAX:
                raise ResourceError(f"Resource {index} overflow while building repost delta")

    for old in old_rows:
        old_space = str(old[1] or "")
        old_quant = str(old[2] or "")
        old_quant_hash = str(old[3] or "")
        old_selector = str(old[4] or "")
        old_selector_hash = str(old[5] or "")
        old_resources = [int(v or 0) for v in old[6:6 + MAX_RESOURCES]]
        add_delta(
            old_space,
            old_quant,
            old_quant_hash,
            old_selector,
            old_selector_hash,
            [-value for value in old_resources],
            requires_existing=True,
        )

    add_delta(
        space,
        quant_value,
        quant_hash,
        selector_quant_value,
        selector_hash,
        scaled,
        requires_existing=False,
    )

    # Read and validate every final balance before changing any row.
    for bucket in affected.values():
        cur.execute(
            f"""
            SELECT quant, selector_quant, {', '.join(resource_names)}
            FROM {BALANCE_TABLE}
            WHERE scope = ? AND space = ? AND quant_hash = ?
            """,
            (scope, bucket["space"], bucket["quant_hash"]),
        )
        balance_row = cur.fetchone()
        if balance_row is None:
            if bucket["requires_existing"]:
                raise QuantLedgerError(
                    "Cannot repost movement because its previous balance row is missing"
                )
            current = [0] * MAX_RESOURCES
            bucket["exists"] = False
        else:
            if str(balance_row[0] or "") != bucket["quant"]:
                raise OperationConflictError("Quant hash collision detected for balance")
            if str(balance_row[1] or "") != bucket["selector_quant"]:
                raise SelectorConflictError(
                    "selector_quant must be immutable for an existing scope + space + quant"
                )
            current = [int(v or 0) for v in balance_row[2:2 + MAX_RESOURCES]]
            bucket["exists"] = True

        final_values: List[int] = []
        for index, (old_value, delta) in enumerate(zip(current, bucket["delta"])):
            final_value = old_value + delta
            if final_value < INT64_MIN or final_value > INT64_MAX:
                raise ResourceError(f"Resource {index} overflow")
            final_values.append(final_value)
        for index in checks:
            if final_values[index] < 0:
                raise NegativeBalanceError(
                    scope=scope,
                    space=bucket["space"],
                    quant_value=bucket["quant"],
                    resource_index=index,
                    attempted=_from_scaled(final_values[index]),
                )
        bucket["final"] = final_values

    balance_columns = [
        "scope", "space", "quant", "quant_hash", "selector_quant", "selector_hash",
        *resource_names, "version", "updated_at",
    ]
    balance_placeholders = ",".join(["?"] * len(balance_columns))
    assignments = ", ".join(
        f"resource{i} = resource{i} + ?" for i in range(1, MAX_RESOURCES + 1)
    )

    for bucket in affected.values():
        if not bucket["exists"]:
            cur.execute(
                f"INSERT INTO {BALANCE_TABLE} ({', '.join(balance_columns)}) "
                f"VALUES ({balance_placeholders})",
                (
                    scope, bucket["space"], bucket["quant"], bucket["quant_hash"],
                    bucket["selector_quant"], bucket["selector_hash"],
                    *([0] * MAX_RESOURCES), 0, now,
                ),
            )
        cur.execute(
            f"""
            UPDATE {BALANCE_TABLE}
            SET {assignments}, version = version + 1, updated_at = ?
            WHERE scope = ? AND space = ? AND quant_hash = ? AND quant = ?
            """,
            (
                *bucket["delta"], now, scope, bucket["space"],
                bucket["quant_hash"], bucket["quant"],
            ),
        )
        if cur.rowcount != 1:
            raise QuantLedgerError("Balance update failed")

    # The previous posting is replaced, not supplemented. Deleting and
    # inserting inside the same transaction keeps movements and balances equal.
    cur.execute(
        f"DELETE FROM {MOVEMENT_TABLE} WHERE scope = ? AND operation_id = ?",
        (scope, operation_id),
    )

    movement_columns = [
        "scope", "operation_id", "period", "space", "quant", "quant_hash",
        "selector_quant", "selector_hash", "details_json", "payload_hash",
        *resource_names, "created_at",
    ]
    movement_placeholders = ",".join(["?"] * len(movement_columns))
    cur.execute(
        f"INSERT INTO {MOVEMENT_TABLE} ({', '.join(movement_columns)}) "
        f"VALUES ({movement_placeholders})",
        (
            scope, operation_id, period_value, space, quant_value, quant_hash,
            selector_quant_value, selector_hash, details_json, payload_hash,
            *scaled, now,
        ),
    )

    return MoveResult(
        scope=scope,
        space=space,
        operation_id=operation_id,
        quant=quant_value,
        selector_quant=selector_quant_value,
        resources=_public_resources(scaled),
        reposted=bool(old_rows),
        replaced_movements=len(old_rows),
        already_applied=False,
    )


def _query(sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
    ensure_schema()
    raw = _open_raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql, tuple(params))
        return [_row_dict(cur, row) for row in cur.fetchall()]
    finally:
        raw.close()


def _balance_from_dict(row: Mapping[str, Any]) -> BalanceRow:
    return BalanceRow(
        scope=str(row.get("scope") or ""),
        space=str(row.get("space") or ""),
        quant=str(row.get("quant") or ""),
        selector_quant=str(row.get("selector_quant") or ""),
        resources=tuple(_from_scaled(row.get(f"resource{i}")) for i in range(1, MAX_RESOURCES + 1)),
        version=int(row.get("version") or 0),
        updated_at=str(row.get("updated_at") or ""),
    )


def get_balance(
    space: str,
    quant: str,
    *,
    scope: Optional[str] = None,
) -> BalanceRow:
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    quant_value = _normalize_quant(quant)
    rows = _query(
        f"SELECT * FROM {BALANCE_TABLE} WHERE scope = ? AND space = ? AND quant_hash = ? AND quant = ?",
        (resolved_scope, space, _hash(quant_value), quant_value),
    )
    if rows:
        return _balance_from_dict(rows[0])
    return BalanceRow(
        scope=resolved_scope,
        space=space,
        quant=quant_value,
        selector_quant="",
        resources=tuple(Decimal(0) for _ in range(MAX_RESOURCES)),
        version=0,
        updated_at="",
    )


def select_balances(
    space: str,
    *,
    selector_quant: Optional[str] = None,
    quants: Optional[Iterable[str]] = None,
    nonzero_resource: Optional[int] = None,
    positive_resource: Optional[int] = None,
    limit: Optional[int] = None,
    scope: Optional[str] = None,
) -> List[BalanceRow]:
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    where = ["scope = ?", "space = ?"]
    params: List[Any] = [resolved_scope, space]

    if selector_quant is not None:
        selector_value = _normalize_quant(selector_quant, optional=True)
        where.extend(["selector_hash = ?", "selector_quant = ?"])
        params.extend([_hash(selector_value), selector_value])

    if quants is not None:
        quant_values = [_normalize_quant(q) for q in quants]
        if not quant_values:
            return []
        hashes = [_hash(q) for q in quant_values]
        where.append(f"quant_hash IN ({','.join(['?'] * len(hashes))})")
        params.extend(hashes)

    if nonzero_resource is not None:
        idx = int(nonzero_resource)
        if idx < 0 or idx >= MAX_RESOURCES:
            raise ResourceError(f"Invalid resource index: {idx}")
        where.append(f"resource{idx + 1} <> 0")

    if positive_resource is not None:
        idx = int(positive_resource)
        if idx < 0 or idx >= MAX_RESOURCES:
            raise ResourceError(f"Invalid resource index: {idx}")
        where.append(f"resource{idx + 1} > 0")

    sql = f"SELECT * FROM {BALANCE_TABLE} WHERE {' AND '.join(where)} ORDER BY quant"
    if limit is not None:
        lim = max(0, int(limit))
        sql += " LIMIT ?"
        params.append(lim)
    rows = [_balance_from_dict(row) for row in _query(sql, params)]
    if quants is not None:
        allowed = set(quant_values)
        rows = [row for row in rows if row.quant in allowed]
    return rows


def _details_where(details: Optional[Mapping[str, Any]], where: List[str], params: List[Any]) -> None:
    for key, value in dict(details or {}).items():
        key_s = str(key or "").strip()
        if not key_s or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for ch in key_s):
            raise QuantLedgerError(f"Unsupported details key: {key!r}")
        where.append(f"json_extract(details_json, '$.{key_s}') = ?")
        if isinstance(value, (dict, list, tuple, set)):
            params.append(_canonical_json(value))
        elif isinstance(value, bool):
            params.append(1 if value else 0)
        elif value is None:
            params.append(None)
        else:
            params.append(value)


def select_movements(
    space: str,
    *,
    quant: Optional[str] = None,
    selector_quant: Optional[str] = None,
    operation_id: Optional[str] = None,
    period_from: Any = None,
    period_to: Any = None,
    details: Optional[Mapping[str, Any]] = None,
    limit: Optional[int] = None,
    scope: Optional[str] = None,
) -> List[MovementRow]:
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    where = ["scope = ?", "space = ?"]
    params: List[Any] = [resolved_scope, space]
    if quant is not None:
        q = _normalize_quant(quant)
        where.extend(["quant_hash = ?", "quant = ?"])
        params.extend([_hash(q), q])
    if selector_quant is not None:
        selector = _normalize_quant(selector_quant, optional=True)
        where.extend(["selector_hash = ?", "selector_quant = ?"])
        params.extend([_hash(selector), selector])
    if operation_id is not None:
        where.append("operation_id = ?")
        params.append(str(operation_id))
    if period_from not in (None, ""):
        where.append("period >= ?")
        params.append(_normalize_period_bound(period_from, end=False))
    if period_to not in (None, ""):
        where.append("period <= ?")
        params.append(_normalize_period_bound(period_to, end=True))
    _details_where(details, where, params)
    sql = f"SELECT * FROM {MOVEMENT_TABLE} WHERE {' AND '.join(where)} ORDER BY period, id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, int(limit)))
    rows = _query(sql, params)
    out: List[MovementRow] = []
    for row in rows:
        try:
            details_obj = json.loads(row.get("details_json") or "{}")
        except Exception:
            details_obj = {}
        out.append(
            MovementRow(
                id=int(row.get("id") or 0),
                scope=str(row.get("scope") or ""),
                space=str(row.get("space") or ""),
                operation_id=str(row.get("operation_id") or ""),
                period=str(row.get("period") or ""),
                quant=str(row.get("quant") or ""),
                selector_quant=str(row.get("selector_quant") or ""),
                details=details_obj if isinstance(details_obj, dict) else {},
                resources=tuple(_from_scaled(row.get(f"resource{i}")) for i in range(1, MAX_RESOURCES + 1)),
                created_at=str(row.get("created_at") or ""),
            )
        )
    return out


def statement(
    space: str,
    *,
    period_from: Any = None,
    period_to: Any = None,
    quant: Optional[str] = None,
    selector_quant: Optional[str] = None,
    scope: Optional[str] = None,
) -> List[StatementRow]:
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    from_value = _normalize_period_bound(period_from, end=False) if period_from not in (None, "") else None
    to_value = _normalize_period_bound(period_to, end=True) if period_to not in (None, "") else None
    where = ["scope = ?", "space = ?"]
    params: List[Any] = [resolved_scope, space]
    if quant is not None:
        q = _normalize_quant(quant)
        where.extend(["quant_hash = ?", "quant = ?"])
        params.extend([_hash(q), q])
    if selector_quant is not None:
        selector = _normalize_quant(selector_quant, optional=True)
        where.extend(["selector_hash = ?", "selector_quant = ?"])
        params.extend([_hash(selector), selector])
    if to_value is not None:
        where.append("period <= ?")
        params.append(to_value)

    expressions: List[str] = []
    expression_params: List[Any] = []
    for i in range(1, MAX_RESOURCES + 1):
        col = f"resource{i}"
        if from_value is None:
            expressions.append(f"0 AS opening{i}")
        else:
            expressions.append(f"SUM(CASE WHEN period < ? THEN {col} ELSE 0 END) AS opening{i}")
            expression_params.append(from_value)
        period_condition = "1=1"
        cond_params: List[Any] = []
        if from_value is not None:
            period_condition += " AND period >= ?"
            cond_params.append(from_value)
        if to_value is not None:
            period_condition += " AND period <= ?"
            cond_params.append(to_value)
        expressions.append(
            f"SUM(CASE WHEN ({period_condition}) AND {col} > 0 THEN {col} ELSE 0 END) AS income{i}"
        )
        expression_params.extend(cond_params)
        expressions.append(
            f"SUM(CASE WHEN ({period_condition}) AND {col} < 0 THEN -{col} ELSE 0 END) AS expense{i}"
        )
        expression_params.extend(cond_params)
        expressions.append(f"SUM({col}) AS closing{i}")

    sql = f"""
        SELECT scope, space, quant, selector_quant, {', '.join(expressions)}
        FROM {MOVEMENT_TABLE}
        WHERE {' AND '.join(where)}
        GROUP BY scope, space, quant, selector_quant
        ORDER BY quant
    """
    # SQL placeholders in SELECT expressions appear before WHERE placeholders.
    rows = _query(sql, [*expression_params, *params])
    out: List[StatementRow] = []
    for row in rows:
        out.append(
            StatementRow(
                scope=str(row.get("scope") or ""),
                space=str(row.get("space") or ""),
                quant=str(row.get("quant") or ""),
                selector_quant=str(row.get("selector_quant") or ""),
                opening=tuple(_from_scaled(row.get(f"opening{i}")) for i in range(1, MAX_RESOURCES + 1)),
                income=tuple(_from_scaled(row.get(f"income{i}")) for i in range(1, MAX_RESOURCES + 1)),
                expense=tuple(_from_scaled(row.get(f"expense{i}")) for i in range(1, MAX_RESOURCES + 1)),
                closing=tuple(_from_scaled(row.get(f"closing{i}")) for i in range(1, MAX_RESOURCES + 1)),
            )
        )
    return out


def verify_space(space: str, *, scope: Optional[str] = None, max_errors: int = 100) -> VerifyResult:
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    balances = {
        row.quant: row
        for row in select_balances(space, scope=resolved_scope)
    }
    movement_rows = _query(
        f"""
        SELECT quant, selector_quant, {', '.join(f'SUM(resource{i}) AS resource{i}' for i in range(1, MAX_RESOURCES + 1))}
        FROM {MOVEMENT_TABLE}
        WHERE scope = ? AND space = ?
        GROUP BY quant, selector_quant
        """,
        (resolved_scope, space),
    )
    movement_map = {str(row.get("quant") or ""): row for row in movement_rows}
    all_quants = sorted(set(balances) | set(movement_map))
    errors: List[Dict[str, Any]] = []
    for q in all_quants:
        bal = balances.get(q)
        mov = movement_map.get(q)
        expected = tuple(_from_scaled(mov.get(f"resource{i}") if mov else 0) for i in range(1, MAX_RESOURCES + 1))
        actual = bal.resources if bal else tuple(Decimal(0) for _ in range(MAX_RESOURCES))
        selector_expected = str(mov.get("selector_quant") or "") if mov else ""
        selector_actual = bal.selector_quant if bal else ""
        if actual != expected or selector_actual != selector_expected:
            errors.append(
                {
                    "quant": q,
                    "selector_balance": selector_actual,
                    "selector_movements": selector_expected,
                    "balance": [str(v) for v in actual],
                    "movements_sum": [str(v) for v in expected],
                }
            )
            if len(errors) >= max(1, int(max_errors)):
                break
    return VerifyResult(
        scope=resolved_scope,
        space=space,
        valid=not errors,
        checked_quants=len(all_quants),
        errors=tuple(errors),
    )


def rebuild_balances(space: str, *, scope: Optional[str] = None) -> VerifyResult:
    """Rebuild one space from its movements inside one write transaction."""
    resolved_scope = _resolve_scope(scope)
    space = _validate_space(space)
    ensure_schema()
    raw = _open_raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            f"""
            SELECT quant, quant_hash, selector_quant, selector_hash,
                   {', '.join(f'SUM(resource{i}) AS resource{i}' for i in range(1, MAX_RESOURCES + 1))}
            FROM {MOVEMENT_TABLE}
            WHERE scope = ? AND space = ?
            GROUP BY quant, quant_hash, selector_quant, selector_hash
            """,
            (resolved_scope, space),
        )
        rows = [_row_dict(cur, row) for row in cur.fetchall()]
        cur.execute(
            f"DELETE FROM {BALANCE_TABLE} WHERE scope = ? AND space = ?",
            (resolved_scope, space),
        )
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        columns = [
            "scope", "space", "quant", "quant_hash", "selector_quant", "selector_hash",
            *_resource_columns(), "version", "updated_at",
        ]
        placeholders = ",".join(["?"] * len(columns))
        for row in rows:
            cur.execute(
                f"INSERT INTO {BALANCE_TABLE} ({', '.join(columns)}) VALUES ({placeholders})",
                (
                    resolved_scope, space, row["quant"], row["quant_hash"],
                    row.get("selector_quant") or "", row.get("selector_hash") or _hash(""),
                    *[int(row.get(f"resource{i}") or 0) for i in range(1, MAX_RESOURCES + 1)],
                    1, now,
                ),
            )
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
    return verify_space(space, scope=resolved_scope)


# Compact aliases for handler code.
balance = get_balance
balances = select_balances
movements = select_movements


__all__ = [
    "API_VERSION",
    "MAX_RESOURCES",
    "RESOURCE_SCALE_DIGITS",
    "QuantLedgerError",
    "QuantFormatError",
    "ScopeRequiredError",
    "ResourceError",
    "NegativeBalanceError",
    "OperationConflictError",
    "SelectorConflictError",
    "MoveResult",
    "BalanceRow",
    "MovementRow",
    "StatementRow",
    "VerifyResult",
    "available",
    "ensure_schema",
    "configure_engine",
    "quant",
    "parse_quant",
    "quant_part",
    "transaction",
    "LedgerTransaction",
    "move",
    "get_balance",
    "balance",
    "select_balances",
    "balances",
    "select_movements",
    "movements",
    "statement",
    "verify_space",
    "rebuild_balances",
]
