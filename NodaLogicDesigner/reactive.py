"""Reactive fields for NodaLogic server nodes.

A Reactive field is logically one field with two server-side facets:
- node._data[field] contains the last calculated value (normal code sees this);
- this module stores the formula/source and dependency graph in a sidecar DB.

Formula syntax:
    @qty * @price
    @qty * $Product$<node-id>@price

The preprocessor rewrites references to private resolver calls before handing the
result to NodaScript.  The user never sees or writes those resolver calls.
"""
from __future__ import annotations

import ast
from functools import lru_cache
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

_DB_DIR = os.environ.get("NODA_NODE_STORAGE_PATH") or "node_storage"
_DB_PATH = os.path.join(_DB_DIR, "reactive.sqlite")
_DB_LOCK = threading.RLock()
_WAVE_LOCK = threading.RLock()
_GRAPH_LOCK = threading.RLock()
_GRAPH_REVISION: int | None = None
_GRAPH_OUTGOING: dict[str, tuple[CellRef, ...]] = {}
_GRAPH_EDGES: tuple[tuple[str, str], ...] = ()
_WORKER_LOCK = threading.RLock()
_WORKER_RUNNING = False
_SCHEMA_READY_PATH: str | None = None
_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, min(4, int(os.environ.get("NODA_REACTIVE_WORKERS") or 2))),
    thread_name_prefix="noda-reactive",
)
@dataclass(frozen=True, slots=True)
class CellRef:
    config_uid: str
    class_name: str
    node_id: str
    field: str

    @property
    def node_uid(self) -> str:
        return f"{self.config_uid}${self.class_name}${self.node_id}"

    @property
    def key(self) -> str:
        return f"{self.node_uid}@{self.field}"

    @classmethod
    def from_node(cls, node: Any, field: str) -> "CellRef":
        config_uid = str(getattr(node, "_config_uid", "") or "")
        class_name = str(getattr(node, "_schema_class_name", "") or node.__class__.__name__)
        raw_node_id = str(getattr(node, "_id", "") or "")

        # Canonicalise the node part of a cell reference.  Web custom_process
        # historically created its singleton under a two-part key
        # ``config$Class``.  Reactive references intentionally use the normal
        # three-part identity ``config$Class$singleton``.  Without normalising
        # here, a write to that legacy singleton would never match a dependency
        # on ``$Class$singleton@field``.
        node_id = raw_node_id
        parts = raw_node_id.split("$") if raw_node_id else []
        if len(parts) >= 3 and parts[0] == config_uid and parts[-2] == class_name:
            node_id = parts[-1]
        elif len(parts) == 2 and parts[0] == config_uid and parts[1] == class_name:
            node_id = "singleton"
        elif len(parts) == 2 and parts[0] == class_name:
            node_id = parts[1]

        return cls(config_uid, class_name, node_id, str(field or ""))


class ReactiveError(RuntimeError):
    pass


class ReactiveCycleError(ReactiveError):
    pass


def _connect() -> sqlite3.Connection:
    global _SCHEMA_READY_PATH
    os.makedirs(_DB_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    schema_path = os.path.abspath(_DB_PATH)
    if _SCHEMA_READY_PATH != schema_path:
        with _DB_LOCK:
            if _SCHEMA_READY_PATH != schema_path:
                conn.execute("PRAGMA journal_mode=WAL")
                _ensure_schema(conn)
                _SCHEMA_READY_PATH = schema_path
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reactive_cells (
            config_uid TEXT NOT NULL,
            class_name TEXT NOT NULL,
            node_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            formula TEXT NOT NULL DEFAULT '',
            async_mode INTEGER NOT NULL DEFAULT 0,
            generation INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'idle',
            error TEXT,
            value_json TEXT,
            updated_at REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (config_uid, class_name, node_id, field_name)
        );
        CREATE TABLE IF NOT EXISTS reactive_dependencies (
            source_config_uid TEXT NOT NULL,
            source_class_name TEXT NOT NULL,
            source_node_id TEXT NOT NULL,
            source_field_name TEXT NOT NULL,
            target_config_uid TEXT NOT NULL,
            target_class_name TEXT NOT NULL,
            target_node_id TEXT NOT NULL,
            target_field_name TEXT NOT NULL,
            PRIMARY KEY (
                source_config_uid, source_class_name, source_node_id, source_field_name,
                target_config_uid, target_class_name, target_node_id, target_field_name
            )
        );
        CREATE INDEX IF NOT EXISTS ix_reactive_dependencies_source
        ON reactive_dependencies (
            source_config_uid, source_class_name, source_node_id, source_field_name
        );
        CREATE INDEX IF NOT EXISTS ix_reactive_dependencies_target
        ON reactive_dependencies (
            target_config_uid, target_class_name, target_node_id, target_field_name
        );
        CREATE TABLE IF NOT EXISTS reactive_queue (
            config_uid TEXT NOT NULL,
            class_name TEXT NOT NULL,
            node_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            generation INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            updated_at REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (config_uid, class_name, node_id, field_name)
        );
        CREATE INDEX IF NOT EXISTS ix_reactive_queue_status
        ON reactive_queue(status, updated_at);
        CREATE TABLE IF NOT EXISTS reactive_meta (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO reactive_meta(key, value) VALUES ('graph_revision', 0);
        CREATE TRIGGER IF NOT EXISTS reactive_dependencies_insert_revision
        AFTER INSERT ON reactive_dependencies BEGIN
            UPDATE reactive_meta SET value=value+1 WHERE key='graph_revision';
        END;
        CREATE TRIGGER IF NOT EXISTS reactive_dependencies_delete_revision
        AFTER DELETE ON reactive_dependencies BEGIN
            UPDATE reactive_meta SET value=value+1 WHERE key='graph_revision';
        END;
        """
    )


def _is_ident_char(ch: str) -> bool:
    return bool(ch) and (ch == "_" or ch == "." or ch.isalnum())


def _read_identifier(text: str, pos: int) -> tuple[str, int]:
    start = pos
    while pos < len(text) and _is_ident_char(text[pos]):
        pos += 1
    return text[start:pos], pos


def _scan_and_compile(formula: str, target: CellRef) -> tuple[str, list[CellRef]]:
    """Compile @/$ references while leaving strings/comments untouched."""
    text = str(formula or "")
    out: list[str] = []
    deps: list[CellRef] = []
    seen: set[CellRef] = set()
    i = 0
    quote: str | None = None
    triple = False

    def add_dep(ref: CellRef) -> None:
        if ref not in seen:
            seen.add(ref)
            deps.append(ref)

    while i < len(text):
        ch = text[i]
        if quote is not None:
            if ch == "\\":
                out.append(ch)
                if i + 1 < len(text):
                    out.append(text[i + 1])
                    i += 2
                else:
                    i += 1
                continue
            if triple:
                if text.startswith(quote * 3, i):
                    out.append(quote * 3)
                    i += 3
                    quote = None
                    triple = False
                    continue
            elif ch == quote:
                out.append(ch)
                i += 1
                quote = None
                continue
            out.append(ch)
            i += 1
            continue

        if ch in ("'", '"'):
            if text.startswith(ch * 3, i):
                out.append(ch * 3)
                i += 3
                quote = ch
                triple = True
            else:
                out.append(ch)
                i += 1
                quote = ch
                triple = False
            continue

        if ch == "#":
            # Python/NodaScript comment: copy verbatim through end of line.
            end = text.find("\n", i)
            if end < 0:
                out.append(text[i:])
                break
            out.append(text[i:end + 1])
            i = end + 1
            continue

        if ch == "@":
            field, end = _read_identifier(text, i + 1)
            if field:
                ref = CellRef(target.config_uid, target.class_name, target.node_id, field)
                add_dep(ref)
                out.append(f"__reactive_ref__({ref.node_uid!r}, {field!r})")
                i = end
                continue

        if ch == "$":
            # External same-configuration reference: $Class$node-id@field
            class_name, p = _read_identifier(text, i + 1)
            if class_name and p < len(text) and text[p] == "$":
                node_start = p + 1
                at = text.find("@", node_start)
                if at > node_start:
                    node_id = text[node_start:at]
                    # Keep the node-id intentionally permissive (UUIDs, numeric ids,
                    # business ids). Whitespace/operators indicate it wasn't a ref.
                    if node_id and not any(c.isspace() or c in "()[]{}+*/%=,!<>" for c in node_id):
                        field, end = _read_identifier(text, at + 1)
                        if field:
                            ref = CellRef(target.config_uid, class_name, node_id, field)
                            add_dep(ref)
                            out.append(f"__reactive_ref__({ref.node_uid!r}, {field!r})")
                            i = end
                            continue

        out.append(ch)
        i += 1

    code = "".join(out)
    # Provider calls are ordinary formula functions, but their data lives
    # outside Nodes. Record a synthetic source dependency so QL and CostGraph
    # publications can invalidate the same Reactive graph.
    try:
        probe = code
        if "__reactive_ref__" in code:
            probe = "\n".join(
                (line[:len(line) - len(line.lstrip())] + "_return = " + line.lstrip()[7:])
                if line.lstrip().startswith("return ") else line
                for line in code.splitlines()
            )
        tree = ast.parse(probe, mode="eval")
        call_nodes = ast.walk(tree)
    except SyntaxError:
        try:
            probe_lines = []
            for line in code.splitlines():
                stripped = line.lstrip()
                indent = line[:len(line) - len(stripped)]
                if stripped.startswith("return "):
                    probe_lines.append(indent + "_return = " + stripped[7:])
                elif stripped == "return":
                    probe_lines.append(indent + "_return = None")
                else:
                    probe_lines.append(line)
            call_nodes = ast.walk(ast.parse("\n".join(probe_lines), mode="exec"))
        except SyntaxError:
            call_nodes = ()
    for item in call_nodes:
        if not isinstance(item, ast.Call) or not isinstance(item.func, ast.Name):
            continue
        name = item.func.id.lower()
        if name in {"ql_sum", "ql_month_sum"}:
            space = "*"
            if item.args and isinstance(item.args[0], ast.Constant) and isinstance(item.args[0].value, str):
                space = item.args[0].value
            add_dep(CellRef(target.config_uid, "__ql__", space, "*"))
        elif name in {"cg_value", "cg_month_sum"}:
            try:
                from reactive_adapters import cg_scope
                scope_key = cg_scope(target.config_uid).key()
            except Exception:
                scope_key = target.config_uid
            add_dep(CellRef(target.config_uid, "__cg__", scope_key, "*"))

    return code, deps


@lru_cache(maxsize=8192)
def _compile_formula_cached(formula: str, target: CellRef) -> tuple[str, str, tuple[CellRef, ...]]:
    code, deps = _scan_and_compile(formula, target)
    mode = "code"
    try:
        ast.parse(code, mode="eval")
        mode = "expression"
    except SyntaxError:
        # Validate as Python/NodaScript statement code too. NodaScript's return
        # rewriting happens later, so a top-level `return` is allowed here.
        probe_lines = []
        for line in str(code or "").splitlines():
            stripped = line.lstrip()
            indent = line[:len(line) - len(stripped)]
            if stripped.startswith("return "):
                probe_lines.append(indent + "_return = " + stripped[7:])
            elif stripped == "return":
                probe_lines.append(indent + "_return = None")
            else:
                probe_lines.append(line)
        ast.parse("\n".join(probe_lines), mode="exec")
    return code, mode, tuple(deps)


def compile_formula(formula: str, target: CellRef) -> dict[str, Any]:
    """Compile a formula once per source text and target cell.

    Formula definitions are stable across value edits, so reparsing on every
    recalculation only adds latency. Return a fresh dependency list to callers
    while retaining the immutable compiled result in the process cache.
    """
    code, mode, dependencies = _compile_formula_cached(str(formula or ""), target)
    return {"code": code, "mode": mode, "dependencies": list(dependencies)}


def _cell_params(ref: CellRef) -> tuple[str, str, str, str]:
    return ref.config_uid, ref.class_name, ref.node_id, ref.field


def _row_to_ref(row: sqlite3.Row, prefix: str = "") -> CellRef:
    return CellRef(
        str(row[prefix + "config_uid"]),
        str(row[prefix + "class_name"]),
        str(row[prefix + "node_id"]),
        str(row[prefix + "field_name"]),
    )


def _target_from_dep_row(row: sqlite3.Row) -> CellRef:
    return CellRef(
        str(row["target_config_uid"]),
        str(row["target_class_name"]),
        str(row["target_node_id"]),
        str(row["target_field_name"]),
    )


def _dependents(ref: CellRef) -> list[CellRef]:
    outgoing, _edges = _dependency_snapshot()
    return list(outgoing.get(ref.key, ()))


def _dependency_snapshot() -> tuple[dict[str, tuple[CellRef, ...]], tuple[tuple[str, str], ...]]:
    """Keep the dependency graph in memory until a formula changes.

    SQLite remains the durable source of truth, while the revision counter lets
    every process notice edits made by another worker without rereading all
    dependency rows for each value change.
    """
    global _GRAPH_REVISION, _GRAPH_OUTGOING, _GRAPH_EDGES
    with _GRAPH_LOCK:
        conn = _connect()
        try:
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT value FROM reactive_meta WHERE key='graph_revision'"
            ).fetchone()
            revision = int(row[0] or 0) if row else 0
            if revision == _GRAPH_REVISION:
                conn.commit()
                return _GRAPH_OUTGOING, _GRAPH_EDGES
            rows = conn.execute("""
                SELECT source_config_uid,source_class_name,source_node_id,source_field_name,
                       target_config_uid,target_class_name,target_node_id,target_field_name
                FROM reactive_dependencies
            """).fetchall()
            outgoing: dict[str, list[CellRef]] = {}
            edges: list[tuple[str, str]] = []
            for item in rows:
                source = CellRef(str(item[0]), str(item[1]), str(item[2]), str(item[3]))
                target = CellRef(str(item[4]), str(item[5]), str(item[6]), str(item[7]))
                outgoing.setdefault(source.key, []).append(target)
                edges.append((source.key, target.key))
            conn.commit()
            _GRAPH_REVISION = revision
            _GRAPH_OUTGOING = {key: tuple(value) for key, value in outgoing.items()}
            _GRAPH_EDGES = tuple(edges)
            return _GRAPH_OUTGOING, _GRAPH_EDGES
        finally:
            conn.close()


def _path_exists(start: CellRef, goal: CellRef) -> bool:
    if start == goal:
        return True
    todo = [start]
    visited = {start}
    with _connect() as conn:
        while todo:
            cur = todo.pop()
            rows = conn.execute(
                """
                SELECT target_config_uid, target_class_name, target_node_id, target_field_name
                FROM reactive_dependencies
                WHERE source_config_uid=? AND source_class_name=? AND source_node_id=? AND source_field_name=?
                """,
                _cell_params(cur),
            ).fetchall()
            for row in rows:
                nxt = _target_from_dep_row(row)
                if nxt == goal:
                    return True
                if nxt not in visited:
                    visited.add(nxt)
                    todo.append(nxt)
    return False


def _json_dump(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def _json_load(value: str | None) -> Any:
    if value in (None, ""):
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def get_formula(node_or_ref: Any, field: str | None = None) -> str | None:
    ref = node_or_ref if isinstance(node_or_ref, CellRef) else CellRef.from_node(node_or_ref, str(field or ""))
    with _connect() as conn:
        row = conn.execute(
            "SELECT formula FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
    return None if row is None else str(row["formula"] or "")


def get_state(node_or_ref: Any, field: str | None = None) -> dict[str, Any]:
    ref = node_or_ref if isinstance(node_or_ref, CellRef) else CellRef.from_node(node_or_ref, str(field or ""))
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
    if row is None:
        return {"exists": False, "ref": ref.key}
    return {
        "exists": True,
        "ref": ref.key,
        "formula": str(row["formula"] or ""),
        "async": bool(row["async_mode"]),
        "generation": int(row["generation"] or 0),
        "status": str(row["status"] or "idle"),
        "error": row["error"],
        "value": _json_load(row["value_json"]),
        "updated_at": row["updated_at"],
    }


def list_dependencies(node_or_ref: Any, field: str | None = None) -> list[str]:
    ref = node_or_ref if isinstance(node_or_ref, CellRef) else CellRef.from_node(node_or_ref, str(field or ""))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT source_config_uid, source_class_name, source_node_id, source_field_name
            FROM reactive_dependencies
            WHERE target_config_uid=? AND target_class_name=? AND target_node_id=? AND target_field_name=?
            ORDER BY source_class_name, source_node_id, source_field_name
            """,
            _cell_params(ref),
        ).fetchall()
    return [
        CellRef(str(r[0]), str(r[1]), str(r[2]), str(r[3])).key
        for r in rows
    ]


def set_formula(node: Any, field: str, formula: str, async_mode: bool = False) -> dict[str, Any]:
    ref = CellRef.from_node(node, field)
    if not all((ref.config_uid, ref.class_name, ref.node_id, ref.field)):
        raise ReactiveError("Reactive field requires config, class, node id and field")
    compiled = compile_formula(str(formula or ""), ref)
    deps: list[CellRef] = compiled["dependencies"]

    for dep in deps:
        if dep == ref or _path_exists(ref, dep):
            raise ReactiveCycleError(f"Circular reactive dependency: {dep.key} -> {ref.key}")

    with _WAVE_LOCK, _DB_LOCK, _connect() as conn:
        old = conn.execute(
            "SELECT generation FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
        generation = int((old["generation"] if old else 0) or 0) + 1
    formula_update = (ref, str(formula or ""), bool(async_mode), generation, deps)
    if async_mode:
        _commit_wave(
            ref.config_uid, {}, {}, [(ref, generation, "dirty", None)],
            [(ref, generation)], formula_update=formula_update, source_ref=ref,
        )
        state = get_state(ref)
        state["affected_values"] = []
        state["pending"] = True
        return state
    try:
        affected_values = _recalculate_wave(
            ref, recalculate_source=True, expected_generation=generation,
            formula_override=(str(formula or ""), False, generation),
            dependency_override=deps, formula_update=formula_update,
        )
    except _WaveCellError as exc:
        raise ReactiveError(str(exc.cause)) from exc.cause
    state = get_state(ref)
    state["affected_values"] = [
        {"config_uid": cell.config_uid, "class_name": cell.class_name,
         "node_id": cell.node_id, "field": cell.field, "value": value}
        for cell, value in affected_values.items()
    ]
    state["pending"] = False
    return state


def remove_formula(node: Any, field: str, *, keep_value: bool = True) -> bool:
    ref = CellRef.from_node(node, field)
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM reactive_dependencies WHERE target_config_uid=? AND target_class_name=? AND target_node_id=? AND target_field_name=?",
            _cell_params(ref),
        )
        conn.execute(
            "DELETE FROM reactive_queue WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        )
        cur = conn.execute(
            "DELETE FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        )
        conn.commit()
    if not keep_value:
        try:
            node._set_reactive_value(field, None)
        except Exception:
            pass
    return bool(cur.rowcount)


def _load_node(ref: CellRef) -> Any | None:
    try:
        import nodes as server_nodes
        cls = server_nodes.Node._resolve_node_class(ref.class_name)
        if cls is None:
            return None

        node = cls.get(ref.node_id, ref.config_uid)
        if node is not None:
            return node

        # Backward compatibility for singleton nodes created by older web code
        # under ``config$Class`` instead of internal id ``singleton``.  Keep the
        # logical Reactive identity canonical while still reading/writing that
        # persisted node until it is naturally saved/migrated.
        if str(ref.node_id) == "singleton" and ref.config_uid and ref.class_name:
            for legacy in (
                f"{ref.config_uid}${ref.class_name}",
                f"{ref.config_uid}${ref.class_name}$singleton",
            ):
                try:
                    node = cls.get(legacy, ref.config_uid)
                except Exception:
                    node = None
                if node is not None:
                    return node
        return None
    except Exception:
        return None


def _read_ref_value(ref: CellRef) -> Any:
    node = _load_node(ref)
    if node is None:
        raise ReactiveError(f"Referenced node is not available: {ref.node_uid}")
    data = node.get_data() if hasattr(node, "get_data") else getattr(node, "_data", {})
    if not isinstance(data, dict):
        raise ReactiveError(f"Referenced node has no data: {ref.node_uid}")
    return data.get(ref.field)


def _node_snapshot(ref: CellRef, cache: dict[tuple[str, str, str], tuple[Any, dict, dict]]):
    key = (ref.config_uid, ref.class_name, ref.node_id)
    snapshot = cache.get(key)
    if snapshot is not None:
        return snapshot
    node = _load_node(CellRef(ref.config_uid, ref.class_name, ref.node_id, ""))
    if node is None:
        raise ReactiveError(f"Referenced node is not available: {ref.node_uid}")
    stored = node._storage[node._id]
    record = dict(stored or {})
    raw_data = record.get("_data")
    data = dict(raw_data) if isinstance(raw_data, dict) else {}
    snapshot = (node, record, data)
    cache[key] = snapshot
    return snapshot


def _evaluate(ref: CellRef, formula: str, *, value_overrides: dict[CellRef, Any] | None = None,
              node_cache: dict[tuple[str, str, str], tuple[Any, dict, dict]] | None = None) -> Any:
    compiled = compile_formula(formula, ref)
    try:
        from client_app.nodascript import NodaScriptEngine
    except Exception:
        from nodascript import NodaScriptEngine  # type: ignore

    engine = NodaScriptEngine()

    source_data = node_cache if node_cache is not None else {}
    overrides = value_overrides or {}

    def resolver(node_uid: str, field_name: str) -> Any:
        parts = str(node_uid or "").split("$")
        if len(parts) < 3:
            raise ReactiveError(f"Invalid reactive node reference: {node_uid}")
        source_ref = CellRef(parts[0], parts[-2], parts[-1], str(field_name or ""))
        if source_ref in overrides:
            return overrides[source_ref]
        _node, _record, data = _node_snapshot(source_ref, source_data)
        return data.get(source_ref.field)

    engine.register("__reactive_ref__", resolver)
    from reactive_adapters import cg_month_sum, cg_value, ql_month_sum, ql_quant, ql_sum
    engine.register("QL_SUM", lambda *args: ql_sum(ref.config_uid, *args))
    engine.register("QL_MONTH_SUM", lambda *args: ql_month_sum(ref.config_uid, *args))
    engine.register("QL_QUANT", ql_quant)
    engine.register("CG_VALUE", lambda *args: cg_value(ref.config_uid, *args))
    engine.register("CG_MONTH_SUM", lambda *args: cg_month_sum(ref.config_uid, *args))
    code = str(compiled["code"] or "")

    # Async workers do not inherit request ContextVars. Preserve the target
    # configuration explicitly so ordinary NodaScript helpers (FindByIndex,
    # create helpers, etc.) keep the same configuration semantics as sync runs.
    token = None
    try:
        import nodes as server_nodes
        token = server_nodes.CURRENT_CONFIG_UID.set(ref.config_uid or None)
    except Exception:
        server_nodes = None
    try:
        if compiled["mode"] == "expression":
            return engine.get("return " + code, {})
        return engine.get(code, {})
    finally:
        if token is not None and server_nodes is not None:
            try:
                server_nodes.CURRENT_CONFIG_UID.reset(token)
            except Exception:
                pass


def _set_cell_error(ref: CellRef, generation: int, exc: BaseException) -> None:
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            """
            UPDATE reactive_cells SET status='error', error=?, updated_at=?
            WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=? AND generation=?
            """,
            (str(exc), time.time(), *_cell_params(ref), int(generation)),
        )


class _WaveCellError(ReactiveError):
    def __init__(self, ref: CellRef, generation: int, cause: BaseException):
        super().__init__(str(cause))
        self.ref = ref
        self.generation = generation
        self.cause = cause


def _commit_wave(config_uid: str, node_cache, value_changes, state_updates, queued,
                 formula_update=None, source_ref: CellRef | None = None):
    """Commit one already-calculated Reactive wave in a single SQL transaction."""
    import nodes as server_nodes

    now = time.time()
    node_rows = {}
    grouped_changes = {}
    for cell, value in value_changes.items():
        node, record, data = _node_snapshot(cell, node_cache)
        key = (cell.config_uid, cell.class_name, cell.node_id)
        grouped_changes.setdefault(key, {
            "node": node, "record": record, "data": data, "values": {},
        })["values"][cell.field] = value

    for key, group in grouped_changes.items():
        node = group["node"]
        old_data = dict(group["data"])
        new_data = dict(old_data)
        new_data.update(group["values"])
        guard = getattr(node, "onReactiveBeforeWrite", None)
        if callable(guard):
            for field, value in group["values"].items():
                decision = guard(field, value)
                if decision is False or (isinstance(decision, tuple) and decision and decision[0] is False):
                    detail = decision[1] if isinstance(decision, tuple) and len(decision) > 1 else "Reactive write rejected"
                    if isinstance(detail, dict):
                        detail = detail.get("error") or detail.get("message") or detail
                    raise PermissionError(str(detail))
        audited = server_nodes._apply_node_audit(new_data, old_data)
        row = dict(group["record"])
        row["_data"] = audited
        row["_updated_at"] = datetime.now(timezone.utc).isoformat()
        storage = node._storage
        node_rows[key] = {
            "node": node, "record": row, "old_data": old_data,
            "new_data": audited, "storage": storage,
            "storage_path": os.path.abspath(storage.filename),
            "table": storage.tablename,
            "storage_key": storage.encode_key(node._id),
            "encoded": storage.encode(row),
        }

    conn = sqlite3.connect(_DB_PATH, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=30000")
    aliases = {}
    try:
        for row in node_rows.values():
            path = row["storage_path"]
            if path not in aliases:
                alias = f"reactive_node_{len(aliases)}"
                conn.execute(f'ATTACH DATABASE ? AS "{alias}"', (path,))
                aliases[path] = alias
        conn.execute("BEGIN IMMEDIATE")
        for row in node_rows.values():
            alias = aliases[row["storage_path"]]
            table = str(row["table"]).replace('"', '""')
            cursor = conn.execute(
                f'UPDATE "{alias}"."{table}" SET value=? WHERE key=?',
                (row["encoded"], row["storage_key"]),
            )
            if cursor.rowcount != 1:
                raise ReactiveError("Reactive target node disappeared before commit")
        if formula_update is not None:
            source, formula, async_mode, generation, dependencies = formula_update
            current = conn.execute(
                "SELECT generation FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
                _cell_params(source),
            ).fetchone()
            actual_generation = int(current[0] or 0) if current is not None else 0
            if actual_generation != int(generation) - 1:
                raise ReactiveError("Reactive formula changed concurrently; retry the edit")
            conn.execute(
                """INSERT INTO reactive_cells
                   (config_uid,class_name,node_id,field_name,formula,async_mode,generation,status,error,updated_at)
                   VALUES (?,?,?,?,?,?,?,'dirty',NULL,?)
                   ON CONFLICT(config_uid,class_name,node_id,field_name) DO UPDATE SET
                   formula=excluded.formula,async_mode=excluded.async_mode,
                   generation=excluded.generation,status='dirty',error=NULL,updated_at=excluded.updated_at""",
                (*_cell_params(source), formula, 1 if async_mode else 0, generation, now),
            )
            conn.execute(
                """DELETE FROM reactive_dependencies WHERE target_config_uid=? AND target_class_name=?
                   AND target_node_id=? AND target_field_name=?""",
                _cell_params(source),
            )
            for dep in dependencies:
                conn.execute(
                    """INSERT OR IGNORE INTO reactive_dependencies
                       (source_config_uid,source_class_name,source_node_id,source_field_name,
                        target_config_uid,target_class_name,target_node_id,target_field_name)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (*_cell_params(dep), *_cell_params(source)),
                )
        for cell, generation, status, value in state_updates:
            expected_generation = generation
            if cell != source_ref:
                expected_generation = generation - 1
            if status == "dirty":
                cursor = conn.execute(
                    """UPDATE reactive_cells SET generation=?,status=?,error=NULL,updated_at=?
                       WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=? AND generation=?""",
                    (generation, status, now, *_cell_params(cell), expected_generation),
                )
            else:
                cursor = conn.execute(
                    """UPDATE reactive_cells SET generation=?,status=?,error=NULL,
                       value_json=?,updated_at=?
                       WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=? AND generation=?""",
                    (generation, status, _json_dump(value), now, *_cell_params(cell), expected_generation),
                )
            if cursor.rowcount != 1:
                raise ReactiveError("Reactive formula changed before commit")
        for cell, generation in queued:
            conn.execute(
                """INSERT INTO reactive_queue(config_uid,class_name,node_id,field_name,generation,status,updated_at)
                   VALUES (?,?,?,?,?,'queued',?)
                   ON CONFLICT(config_uid,class_name,node_id,field_name) DO UPDATE SET
                   generation=excluded.generation,status='queued',updated_at=excluded.updated_at""",
                (*_cell_params(cell), generation, now),
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

    # Refresh process-local node snapshots and secondary indexes after the
    # authoritative multi-node commit has succeeded.
    for row in node_rows.values():
        node = row["node"]
        node._data_cache = dict(row["new_data"])
        try:
            old_dk = server_nodes.normalize_date_key(
                row["old_data"].get("_date_key") or row["old_data"].get("_date")
            )
            new_dk = server_nodes.normalize_date_key(
                row["new_data"].get("_date_key") or row["new_data"].get("_date")
            )
            if old_dk != new_dk:
                index = node.__class__._get_date_index_storage(node._config_uid)
                if old_dk:
                    index.pop(node.__class__._date_index_key(old_dk, node._id), None)
                if new_dk:
                    index[node.__class__._date_index_key(new_dk, node._id)] = 1
        except Exception:
            pass
        try:
            node.__class__._update_defined_indexes(
                node._config_uid, node._id, row["old_data"], row["new_data"]
            )
        except Exception:
            pass
        try:
            node.__class__._update_global_indexes(
                node._data.get("_id") or node._id, row["old_data"], row["new_data"]
            )
        except Exception:
            pass
        try:
            server_nodes._call_bridge(
                "update_node_rls_index_global", node._config_uid,
                node.__class__.__name__, node._id, row["new_data"]
            )
        except Exception:
            pass
    if queued:
        _kick_worker()
    if formula_update is not None:
        global _GRAPH_REVISION
        with _GRAPH_LOCK:
            _GRAPH_REVISION = None


def _recalculate_wave(source: CellRef, *, recalculate_source: bool = False,
                      expected_generation: int | None = None, formula_override=None,
                      dependency_override=None, formula_update=None) -> bool:
    """Evaluate a dirty dependency wave in memory, then persist it atomically."""
    with _WAVE_LOCK:
        with _connect() as conn:
            rows = conn.execute(
                """SELECT class_name,node_id,field_name,formula,async_mode,generation
                   FROM reactive_cells WHERE config_uid=?""",
                (source.config_uid,),
            ).fetchall()
        formulas = {
            CellRef(source.config_uid, str(row[0]), str(row[1]), str(row[2])):
                (str(row[3] or ""), bool(row[4]), int(row[5] or 0))
            for row in rows
        }
        if formula_override is not None:
            formula, async_mode, generation = formula_override
            formulas[source] = (str(formula or ""), bool(async_mode), int(generation))
        source_info = formulas.get(source)
        if recalculate_source and source_info is None:
            return False

        outgoing, _edges = _dependency_snapshot()
        if formula_update is not None:
            outgoing = {key: list(targets) for key, targets in outgoing.items()}
            for key, targets in list(outgoing.items()):
                outgoing[key] = [target for target in targets if target != source]
            for dep in dependency_override or ():
                outgoing.setdefault(dep.key, []).append(source)
        reachable = {source}
        todo = [source]
        while todo:
            current = todo.pop()
            for target in outgoing.get(current.key, ()):
                if target.config_uid == source.config_uid and target not in reachable:
                    reachable.add(target)
                    todo.append(target)

        indegree = {cell: 0 for cell in reachable}
        parents = {cell: [] for cell in reachable}
        for parent in reachable:
            for target in outgoing.get(parent.key, ()):
                if target in reachable:
                    indegree[target] += 1
                    parents[target].append(parent)
        ready = [source]
        order = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for target in outgoing.get(current.key, ()):
                if target not in indegree:
                    continue
                indegree[target] -= 1
                if indegree[target] == 0:
                    ready.append(target)
        if len(order) != len(reachable):
            raise ReactiveCycleError("Reactive dependency graph contains a cycle")

        node_cache: dict[tuple[str, str, str], tuple[Any, dict, dict]] = {}
        overrides: dict[CellRef, Any] = {}
        value_changes: dict[CellRef, Any] = {}
        state_updates = []
        queued = []
        changed_refs = set()
        if recalculate_source:
            formula, async_mode, generation = source_info
            if expected_generation is not None and generation != expected_generation:
                return False
            if async_mode:
                return False
            try:
                value = _evaluate(source, formula, value_overrides=overrides, node_cache=node_cache)
            except Exception as exc:
                raise _WaveCellError(source, generation, exc) from exc
            _node, _record, data = _node_snapshot(source, node_cache)
            old_value = data.get(source.field)
            overrides[source] = value
            state_updates.append((source, generation, "ok", value))
            if old_value != value:
                value_changes[source] = value
                changed_refs.add(source)
        else:
            changed_refs.add(source)

        for target in order[1:]:
            if not any(parent in changed_refs for parent in parents[target]):
                continue
            formula_info = formulas.get(target)
            if formula_info is None:
                continue
            formula, async_mode, old_generation = formula_info
            generation = old_generation + 1
            if async_mode:
                state_updates.append((target, generation, "dirty", None))
                queued.append((target, generation))
                continue
            try:
                value = _evaluate(target, formula, value_overrides=overrides, node_cache=node_cache)
            except Exception as exc:
                raise _WaveCellError(target, generation, exc) from exc
            _node, _record, data = _node_snapshot(target, node_cache)
            old_value = data.get(target.field)
            overrides[target] = value
            state_updates.append((target, generation, "ok", value))
            if old_value != value:
                value_changes[target] = value
                changed_refs.add(target)

        if not state_updates and not value_changes:
            return {}
        affected_values = {
            cell: value for cell, _generation, status, value in state_updates
            if status == "ok"
        }
        _commit_wave(source.config_uid, node_cache, value_changes, state_updates, queued,
                     formula_update=formula_update,
                     source_ref=source if (recalculate_source or formula_update is not None) else None)
        return affected_values


def _calculate_ref(ref: CellRef, expected_generation: int | None = None, *, propagate: bool = True) -> bool:
    if propagate:
        try:
            return bool(_recalculate_wave(ref, recalculate_source=True,
                                          expected_generation=expected_generation))
        except _WaveCellError as exc:
            _set_cell_error(exc.ref, exc.generation, exc.cause)
            if exc.ref != ref:
                _set_cell_error(ref, expected_generation or 0, exc.cause)
            return False
        except Exception as exc:
            generation = expected_generation
            if generation is None:
                state = get_state(ref)
                generation = int(state.get("generation") or 0)
            _set_cell_error(ref, int(generation or 0), exc)
            return False
    with _connect() as conn:
        row = conn.execute(
            "SELECT formula,generation FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
    if row is None:
        return False
    generation = int(row["generation"] or 0)
    if expected_generation is not None and generation != int(expected_generation):
        return False
    formula = str(row["formula"] or "")
    try:
        value = _evaluate(ref, formula)
        # Re-check after potentially long computation.
        with _connect() as conn:
            fresh = conn.execute(
                "SELECT generation FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
                _cell_params(ref),
            ).fetchone()
        if fresh is None or int(fresh["generation"] or 0) != generation:
            return False

        node = _load_node(ref)
        if node is None:
            raise ReactiveError(f"Reactive target node is not available: {ref.node_uid}")
        changed = bool(node._set_reactive_value(ref.field, value))
        with _DB_LOCK, _connect() as conn:
            conn.execute(
                """
                UPDATE reactive_cells
                SET status='ok', error=NULL, value_json=?, updated_at=?
                WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=? AND generation=?
                """,
                (_json_dump(value), time.time(), *_cell_params(ref), generation),
            )
        if changed and propagate:
            _source_changed(ref)
        return changed
    except Exception as exc:
        _set_cell_error(ref, generation, exc)
        return False


def _mark_dirty(ref: CellRef) -> tuple[int, bool] | None:
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT generation,async_mode FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
        if row is None:
            conn.rollback()
            return None
        generation = int(row["generation"] or 0) + 1
        async_mode = bool(row["async_mode"])
        conn.execute(
            """
            UPDATE reactive_cells SET generation=?, status='dirty', error=NULL, updated_at=?
            WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?
            """,
            (generation, time.time(), *_cell_params(ref)),
        )
        conn.commit()
    return generation, async_mode


def _source_changed_python(source: CellRef) -> None:
    """Recalculate descendants from a value that has already changed."""
    try:
        _recalculate_wave(source, recalculate_source=False)
    except _WaveCellError as exc:
        _set_cell_error(exc.ref, exc.generation, exc.cause)
    except Exception:
        # A user node write has already committed. Preserve the original write
        # and let the formula state surface errors through its normal pathway.
        pass


def _source_changed_rust(source: CellRef) -> None:
    """Use the same staged in-memory wave with the native graph backend."""
    _source_changed_python(source)


def _source_changed(source: CellRef) -> None:
    from compute_engines import get_backend

    backend = get_backend("reactive", config_uid=source.config_uid)
    if backend == "rust":
        _source_changed_rust(source)
    else:
        _source_changed_python(source)


def _notify_external_change_now(provider: str, config_uid: str, key: str) -> None:
    provider_key = str(provider or "").strip().lower()
    class_name = {"ql": "__ql__", "cg": "__cg__"}.get(provider_key)
    if not class_name:
        return
    config = str(config_uid or "").strip()
    if not config:
        return
    source = CellRef(config, class_name, str(key or "*"), "*")
    _source_changed(source)
    if source.node_id != "*":
        _source_changed(CellRef(config, class_name, "*", "*"))


def notify_external_change(provider: str, config_uid: str, key: str = "*") -> None:
    """Schedule invalidation of formulas that read an external data provider.

    Providers often publish while their business transaction still owns a
    journal lock. Recalculating on that same call stack could deadlock a formula
    that reads back from the provider, so the graph wakeup runs on the Reactive
    executor after the transaction has returned.
    """
    try:
        _EXECUTOR.submit(_notify_external_change_now, str(provider or ""),
                         str(config_uid or ""), str(key or "*"))
    except RuntimeError:
        pass


def notify_node_write(node: Any, old_data: dict | None, new_data: dict | None) -> None:
    old = old_data if isinstance(old_data, dict) else {}
    new = new_data if isinstance(new_data, dict) else {}
    keys = set(old) | set(new)
    for field in keys:
        if str(field).startswith("_"):
            continue
        if old.get(field) != new.get(field):
            _source_changed(CellRef.from_node(node, str(field)))
    _kick_worker()


def notify_node_deleted(node: Any) -> None:
    """Remove Reactive definitions owned by a deleted node.

    Dependencies *from* the deleted node to other Reactive cells are kept: the
    formulas still reference that logical source and should become valid again
    if a node with the same id is recreated. Dependents are recalculated now so
    they move to an explicit error state instead of silently keeping stale data.
    """
    ref0 = CellRef.from_node(node, "")
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT target_config_uid,target_class_name,target_node_id,target_field_name
            FROM reactive_dependencies
            WHERE source_config_uid=? AND source_class_name=? AND source_node_id=?
            """,
            (ref0.config_uid, ref0.class_name, ref0.node_id),
        ).fetchall()
    dependents = [
        CellRef(str(r[0]), str(r[1]), str(r[2]), str(r[3]))
        for r in rows
    ]
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM reactive_dependencies WHERE target_config_uid=? AND target_class_name=? AND target_node_id=?",
            (ref0.config_uid, ref0.class_name, ref0.node_id),
        )
        conn.execute(
            "DELETE FROM reactive_queue WHERE config_uid=? AND class_name=? AND node_id=?",
            (ref0.config_uid, ref0.class_name, ref0.node_id),
        )
        conn.execute(
            "DELETE FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=?",
            (ref0.config_uid, ref0.class_name, ref0.node_id),
        )
        conn.commit()
    for target in dependents:
        dirty = _mark_dirty(target)
        if dirty is None:
            continue
        generation, async_mode = dirty
        if async_mode:
            _enqueue(target, generation)
        else:
            _calculate_ref(target, expected_generation=generation)


def notify_nodes_deleted(config_uid: str, class_name: str, node_ids: Iterable[str]) -> None:
    # Lightweight adapter for bulk_delete. Use a tiny stand-in with the same
    # identity attributes; no Node construction is needed.
    class DeletedNode:
        pass
    for node_id in node_ids or []:
        n = DeletedNode()
        n._config_uid = str(config_uid or "")
        n._schema_class_name = str(class_name or "")
        n._id = str(node_id or "")
        notify_node_deleted(n)


def _enqueue(ref: CellRef, generation: int) -> None:
    with _DB_LOCK, _connect() as conn:
        conn.execute(
            """
            INSERT INTO reactive_queue(config_uid,class_name,node_id,field_name,generation,status,updated_at)
            VALUES (?,?,?,?,?,'queued',?)
            ON CONFLICT(config_uid,class_name,node_id,field_name) DO UPDATE SET
                generation=excluded.generation,
                status='queued',
                updated_at=excluded.updated_at
            """,
            (*_cell_params(ref), int(generation), time.time()),
        )
    _kick_worker()


def _claim_one() -> tuple[CellRef, int] | None:
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT config_uid,class_name,node_id,field_name,generation
            FROM reactive_queue WHERE status='queued'
            ORDER BY updated_at ASC LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.rollback()
            return None
        ref = CellRef(str(row[0]), str(row[1]), str(row[2]), str(row[3]))
        generation = int(row[4] or 0)
        conn.execute(
            """
            UPDATE reactive_queue SET status='running', updated_at=?
            WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=? AND generation=?
            """,
            (time.time(), *_cell_params(ref), generation),
        )
        conn.commit()
        return ref, generation


def _finish_queue_item(ref: CellRef, generation: int, *, unresolved: bool = False) -> None:
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT generation,status FROM reactive_queue WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
        if row is None:
            conn.rollback()
            return
        current_gen = int(row["generation"] or 0)
        if current_gen != int(generation):
            # A newer invalidation arrived while this generation was running.
            conn.execute(
                "UPDATE reactive_queue SET status='queued', updated_at=? WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
                (time.time(), *_cell_params(ref)),
            )
        elif unresolved:
            conn.execute(
                "UPDATE reactive_queue SET status='queued', updated_at=? WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
                (time.time(), *_cell_params(ref)),
            )
        else:
            conn.execute(
                "DELETE FROM reactive_queue WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
                _cell_params(ref),
            )
        conn.commit()


def _drain_worker() -> None:
    global _WORKER_RUNNING
    try:
        while True:
            item = _claim_one()
            if item is None:
                return
            ref, generation = item
            if _load_node(ref) is None:
                _finish_queue_item(ref, generation, unresolved=True)
                # Avoid a hot loop until the generated class becomes available.
                return
            _calculate_ref(ref, expected_generation=generation)
            _finish_queue_item(ref, generation)
    finally:
        with _WORKER_LOCK:
            _WORKER_RUNNING = False
        # Race-safe: a producer may have enqueued between our last claim and the
        # flag reset. Kick once more; unresolved items simply stop immediately.
        with _connect() as conn:
            row = conn.execute("SELECT 1 FROM reactive_queue WHERE status='queued' LIMIT 1").fetchone()
        if row is not None:
            # Only restart when at least one queued target class is currently known.
            try:
                import nodes as server_nodes
                candidates = conn = None
                with _connect() as db:
                    candidates = db.execute(
                        "SELECT DISTINCT class_name FROM reactive_queue WHERE status='queued'"
                    ).fetchall()
                if any(server_nodes.Node._resolve_node_class(str(r[0])) is not None for r in (candidates or [])):
                    _kick_worker()
            except Exception:
                pass


def _kick_worker() -> None:
    global _WORKER_RUNNING
    with _WORKER_LOCK:
        if _WORKER_RUNNING:
            return
        _WORKER_RUNNING = True
    _EXECUTOR.submit(_drain_worker)


def notify_class_available(_class_name: str | None = None) -> None:
    """Called when generated Node subclasses are loaded after server restart."""
    _kick_worker()


def resume_pending() -> None:
    # Jobs left in running state belong to a process that no longer exists.
    with _DB_LOCK, _connect() as conn:
        conn.execute("UPDATE reactive_queue SET status='queued' WHERE status='running'")
    _kick_worker()

# Recover durable asynchronous work after a process restart. If the generated
# target class has not been loaded yet, the worker leaves the item queued and
# Node.__init_subclass__ will kick it again when that class becomes available.
try:
    resume_pending()
except Exception:
    pass
