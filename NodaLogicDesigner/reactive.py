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
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Iterable

_DB_DIR = os.environ.get("NODA_NODE_STORAGE_PATH") or "node_storage"
_DB_PATH = os.path.join(_DB_DIR, "reactive.sqlite")
_DB_LOCK = threading.RLock()
_WORKER_LOCK = threading.RLock()
_WORKER_RUNNING = False
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
    os.makedirs(_DB_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
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

    return "".join(out), deps


def compile_formula(formula: str, target: CellRef) -> dict[str, Any]:
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
    return {"code": code, "mode": mode, "dependencies": deps}


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
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT target_config_uid, target_class_name, target_node_id, target_field_name
            FROM reactive_dependencies
            WHERE source_config_uid=? AND source_class_name=? AND source_node_id=? AND source_field_name=?
            """,
            _cell_params(ref),
        ).fetchall()
    return [_target_from_dep_row(r) for r in rows]


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

    now = time.time()
    with _DB_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute(
            "SELECT generation FROM reactive_cells WHERE config_uid=? AND class_name=? AND node_id=? AND field_name=?",
            _cell_params(ref),
        ).fetchone()
        generation = int((old["generation"] if old else 0) or 0) + 1
        conn.execute(
            """
            INSERT INTO reactive_cells
                (config_uid,class_name,node_id,field_name,formula,async_mode,generation,status,error,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(config_uid,class_name,node_id,field_name) DO UPDATE SET
                formula=excluded.formula,
                async_mode=excluded.async_mode,
                generation=excluded.generation,
                status=excluded.status,
                error=NULL,
                updated_at=excluded.updated_at
            """,
            (*_cell_params(ref), str(formula or ""), 1 if async_mode else 0, generation, "dirty", None, now),
        )
        conn.execute(
            "DELETE FROM reactive_dependencies WHERE target_config_uid=? AND target_class_name=? AND target_node_id=? AND target_field_name=?",
            _cell_params(ref),
        )
        for dep in deps:
            conn.execute(
                """
                INSERT OR IGNORE INTO reactive_dependencies
                (source_config_uid,source_class_name,source_node_id,source_field_name,
                 target_config_uid,target_class_name,target_node_id,target_field_name)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (*_cell_params(dep), *_cell_params(ref)),
            )
        conn.commit()

    if async_mode:
        _enqueue(ref, generation)
        return get_state(ref)
    _calculate_ref(ref, expected_generation=generation)
    return get_state(ref)


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


def _evaluate(ref: CellRef, formula: str) -> Any:
    compiled = compile_formula(formula, ref)
    try:
        from client_app.nodascript import NodaScriptEngine
    except Exception:
        from nodascript import NodaScriptEngine  # type: ignore

    engine = NodaScriptEngine()

    def resolver(node_uid: str, field_name: str) -> Any:
        parts = str(node_uid or "").split("$")
        if len(parts) < 3:
            raise ReactiveError(f"Invalid reactive node reference: {node_uid}")
        return _read_ref_value(CellRef(parts[0], parts[-2], parts[-1], str(field_name or "")))

    engine.register("__reactive_ref__", resolver)
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


def _calculate_ref(ref: CellRef, expected_generation: int | None = None) -> bool:
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
        if changed:
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


def _source_changed(source: CellRef) -> None:
    """Propagate one logical value change through the dependency graph."""
    # Breadth-first, but recalculation itself only propagates further when the
    # calculated value actually changed. A seen set avoids duplicate work when
    # several paths converge in one synchronous wave.
    queue = list(_dependents(source))
    seen: set[CellRef] = set()
    while queue:
        target = queue.pop(0)
        if target in seen:
            continue
        seen.add(target)
        dirty = _mark_dirty(target)
        if dirty is None:
            continue
        generation, async_mode = dirty
        if async_mode:
            _enqueue(target, generation)
            continue
        changed = _calculate_ref(target, expected_generation=generation)
        # _calculate_ref propagates via _source_changed itself. Do not enqueue
        # descendants here as that would calculate them twice.
        if changed:
            continue


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
