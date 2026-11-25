"""Per-configuration selector for optional native compute kernels.

The checked-in Python implementations remain the compatibility default. A
configuration may opt individual mechanisms into the compiled extension.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any


MECHANISMS = ("reactive", "cost_graph", "operation_planning")
BACKENDS = ("python", "rust")
DEFAULTS = {name: "python" for name in MECHANISMS}
_LOCK = threading.RLock()
_CACHE: dict[str, tuple[str, dict[str, str]]] = {}
_NATIVE = None


class ComputeEngineError(RuntimeError):
    pass


def normalize_settings(value: Any) -> dict[str, str]:
    result = dict(DEFAULTS)
    if not isinstance(value, dict):
        return result
    for name in MECHANISMS:
        backend = str(value.get(name, result[name]) or result[name]).strip().lower()
        if backend in {"native", "compiled", "rust_native"}:
            backend = "rust"
        if backend not in BACKENDS:
            raise ValueError(f"Unknown {name} compute engine: {backend}")
        result[name] = backend
    return result


def _settings_path() -> str:
    base = str(os.environ.get("NODA_NODE_STORAGE_PATH") or "node_storage")
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "compute_engine_settings.sqlite")


def _connect() -> sqlite3.Connection:
    db = sqlite3.connect(_settings_path(), timeout=15)
    db.execute("PRAGMA busy_timeout=15000")
    db.execute("""CREATE TABLE IF NOT EXISTS compute_engine_config (
        config_uid TEXT PRIMARY KEY,
        settings_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    return db


def sync_configuration(config_uid: str, settings: Any) -> dict[str, str]:
    """Cache imported/editor settings for async workers lacking request context."""
    uid = str(config_uid or "").strip()
    normalized = normalize_settings(settings)
    if not uid:
        return normalized
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    with _LOCK:
        cached = _CACHE.get(uid)
        if cached and cached[0] == encoded:
            return dict(cached[1])
        with _connect() as db:
            db.execute("""INSERT INTO compute_engine_config(config_uid, settings_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(config_uid) DO UPDATE SET
                    settings_json=excluded.settings_json, updated_at=CURRENT_TIMESTAMP""", (uid, encoded))
        _CACHE[uid] = (encoded, normalized)
    return dict(normalized)


def _context_settings(config_uid: str) -> dict[str, str] | None:
    try:
        import nodes
        parsed = nodes.CURRENT_PARSED_CONFIG.get()
        current_uid = str(nodes.CURRENT_CONFIG_UID.get() or "").strip()
        if isinstance(parsed, dict) and (not config_uid or not current_uid or config_uid == current_uid):
            settings = parsed.get("compute_engines")
            if isinstance(settings, dict):
                return sync_configuration(config_uid or current_uid, settings)
    except Exception:
        pass
    return None


def settings_for(config_uid: str | None = None) -> dict[str, str]:
    uid = str(config_uid or "").strip()
    contextual = _context_settings(uid)
    if contextual is not None:
        return contextual
    if not uid:
        try:
            import nodes
            uid = str(nodes.CURRENT_CONFIG_UID.get() or "").strip()
        except Exception:
            uid = ""
    if not uid:
        return dict(DEFAULTS)
    with _LOCK:
        try:
            with _connect() as db:
                row = db.execute("SELECT settings_json FROM compute_engine_config WHERE config_uid=?", (uid,)).fetchone()
        except Exception:
            row = None
        if not row:
            return dict(DEFAULTS)
        try:
            normalized = normalize_settings(json.loads(row[0]))
        except Exception:
            normalized = dict(DEFAULTS)
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        _CACHE[uid] = (encoded, normalized)
        return dict(normalized)


def get_backend(mechanism: str, *, config_uid: str | None = None, explicit: str | None = None) -> str:
    if mechanism not in MECHANISMS:
        raise ValueError(f"Unknown compute mechanism: {mechanism}")
    if explicit is not None:
        return normalize_settings({mechanism: explicit})[mechanism]
    return settings_for(config_uid)[mechanism]


def native_module():
    global _NATIVE
    if _NATIVE is not None:
        return _NATIVE
    try:
        import nodalogic_native
    except ImportError as exc:
        raise ComputeEngineError(
            "The Rust compute engine is selected, but nodalogic_native is not installed "
            "for this Python/platform. Build and install the native_core wheel, or select Python."
        ) from exc
    _NATIVE = nodalogic_native
    return _NATIVE

