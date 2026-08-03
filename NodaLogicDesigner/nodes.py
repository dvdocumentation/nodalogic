import uuid
from datetime import datetime, timezone, timedelta
from html import escape as html_escape
from sqlitedict import SqliteDict
import os
import threading
import hashlib
import json
import hashlib
import copy
import inspect
import base64
import binascii
import re
import sys
import urllib.request
import urllib.error
import sqlite3
import struct
import time
import math
import random
import pickle
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import background_jobs as _background_jobs
except Exception:
    _background_jobs = None

# Optional backend-only SQL storage/projection for backend transactions.
# If balance_sql.py is absent or cannot be imported, the legacy JSON storage
# inside node._data["_transactions"] / node._data["_state_transactions"] is used exactly as before.
try:
    import balance_sql as _BALANCE_SQL
except Exception:
    _BALANCE_SQL = None


def _userfiles_root_dir() -> str:
    """Absolute path to the UserFiles root folder."""
    root_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root_dir, "UserFiles")


def userfiles_dir(config_uid: str | None = None) -> str:
    """Return absolute path to UserFiles/<uid> for the current config.

    If config_uid is None, tries to resolve it from current handler execution
    context (Handlers/<uid>/handlers.py). Falls back to CURRENT_CONFIG_UID.
    """
    uid = (config_uid or "").strip()
    if not uid:
        uid = (current_config_uid_from_handlers() or "").strip()
    if not uid:
        uid = (CURRENT_CONFIG_UID.get() or "").strip()
    base = _userfiles_root_dir()
    return os.path.join(base, uid) if uid else base

def _config_uid_from_node_or_uid(node_or_uid):
    raw = node_or_uid if isinstance(node_or_uid, str) else getattr(node_or_uid, "_id", None)
    raw = str(raw or "").strip()
    if not raw:
        return ""
    parts = raw.split("$")
    if len(parts) >= 3:
        return parts[0].strip()
    return ""

_DATA_URL_RE = re.compile(r"^data:(?P<mime>[^;]+);base64,(?P<data>.+)$", re.IGNORECASE | re.DOTALL)


def _ext_from_mime(mime: str) -> str:
    mime = (mime or "").lower().strip()
    return {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/webp": "webp",
        "image/gif": "gif",
        "image/bmp": "bmp",
        "image/svg+xml": "svg",
        "video/mp4": "mp4",
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "application/pdf": "pdf",
    }.get(mime, "bin")


def _guess_ext_from_bytes(data: bytes) -> str:
    if not data:
        return "bin"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    if len(data) >= 12 and data[0:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if data.startswith(b"BM"):
        return "bmp"
    if data.startswith(b"%PDF"):
        return "pdf"
    return "bin"


def _decode_base64_payload(base64_string: str) -> tuple[bytes, str]:
    """Decode base64 string.

    Returns: (bytes, ext)
    Accepts both raw base64 and data URLs.
    """
    s = (base64_string or "").strip()
    if not s:
        return b"", "bin"

    mime = ""
    m = _DATA_URL_RE.match(s)
    if m:
        mime = (m.group("mime") or "").strip()
        s = (m.group("data") or "").strip()

    s = re.sub(r"\s+", "", s)
    pad = (-len(s)) % 4
    if pad:
        s += "=" * pad
    try:
        raw = base64.b64decode(s, validate=False)
    except (binascii.Error, ValueError):
        raw = base64.b64decode(s + "===")

    ext = _ext_from_mime(mime) if mime else _guess_ext_from_bytes(raw)
    return raw, ext


def getBase64FromImageFile(path_to_image: str) -> str:
    """Read file and return raw base64 string (no data: prefix)."""
    if not path_to_image:
        return ""
    with open(path_to_image, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode("utf-8")


def convertImageFilesToBase64Array(paths_to_images_array: list[str]) -> list[str]:
    """Convert list of file paths to list of raw base64 strings."""
    out: list[str] = []
    for p in (paths_to_images_array or []):
        try:
            out.append(getBase64FromImageFile(p))
        except Exception:
            out.append("")
    return out


def saveBase64ToFile(base64_string: str) -> str:
    """Save base64 to UserFiles/<uid> and return *filename only*.

    Filename includes extension. The caller/UI should resolve the absolute
    path by joining it with UserFiles/<uid>.
    """
    data, ext = _decode_base64_payload(base64_string)
    uid_dir = userfiles_dir()
    os.makedirs(uid_dir, exist_ok=True)

    filename = f"file_{uuid.uuid4().hex}.{ext or 'bin'}"
    abs_path = os.path.join(uid_dir, filename)
    with open(abs_path, "wb") as f:
        f.write(data)
    return filename


def convertBase64ArrayToFilePaths(base64_array: list[str]) -> list[str]:
    """Save array of base64 strings to UserFiles/<uid>.

    Returns list of *filenames* (no folders), suitable to store in node._data.
    """
    out: list[str] = []
    for s in (base64_array or []):
        try:
            out.append(saveBase64ToFile(s))
        except Exception:
            out.append("")
    return out

from contextvars import ContextVar

CURRENT_CONFIG_UID = ContextVar("CURRENT_CONFIG_UID", default=None)
CURRENT_PARSED_CONFIG = ContextVar("CURRENT_PARSED_CONFIG", default=None)
CURRENT_SYSTEM_USER = ContextVar("CURRENT_SYSTEM_USER", default=None)

# Per-request runtime messages (server-side). Web clients can display them if
# the API endpoint includes them in the JSON response.
RUNTIME_MESSAGES = ContextVar("RUNTIME_MESSAGES", default=None)

# Guard to avoid running onAcceptServer multiple times for the same node
# during a single logical operation (e.g. setter -> update_data -> _save).
ACCEPT_GUARD = ContextVar("ACCEPT_GUARD", default=None)

# Guard to avoid running onAfterAcceptServer multiple times for the same node
# during a single logical operation (e.g. setter -> update_data -> _save).
AFTER_ACCEPT_GUARD = ContextVar("AFTER_ACCEPT_GUARD", default=None)

# per-runtime/request cache
DATASET_VIEW_CACHE = ContextVar("DATASET_VIEW_CACHE", default=None)   # (cfg_uid, ds_name, item_id) -> str(view)
DATASET_OBJ_CACHE  = ContextVar("DATASET_OBJ_CACHE", default=None)    # (cfg_uid, ds_name, item_id) -> dict(obj)
DATASET_ID_CACHE   = ContextVar("DATASET_ID_CACHE", default=None)     # (cfg_uid, ds_name) -> int(dataset_id)


def debug_compare_index_values(class_or_name, node_or_id, index_names, config_uid=None):
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")

    node = node_or_id if not isinstance(node_or_id, str) else cls.get(node_or_id, str(config_uid or "").strip())
    if node is None:
        raise ValueError(f"Node not found: {node_or_id}")

    data = dict(getattr(node, "_data", {}) or {})

    cfg_uid = str(config_uid or "").strip()
    if not cfg_uid:
        cfg_uid = _config_uid_from_node_or_uid(node)

    defs = cls._get_defined_indexes(cfg_uid) or []

    result = {
        "class": cls.__name__,
        "config_uid": cfg_uid,
        "node_id": getattr(node, "_id", None),
        "raw_data": data,
        "defined_indexes": defs,
        "indexes": {}
    }

    wanted = set(index_names or [])
    for idx_def in defs:
        if not isinstance(idx_def, dict):
            continue
        name = str(idx_def.get("name") or "").strip()
        if name not in wanted:
            continue

        keys_spec = idx_def.get("keys")
        try:
            extracted = cls._extract_index_values(data, keys_spec)
        except Exception as e:
            extracted = f"ERROR: {e!r}"

        result["indexes"][name] = {
            "index_def": idx_def,
            "keys_spec": keys_spec,
            "raw_value": data.get(keys_spec) if isinstance(keys_spec, str) else None,
            "extracted_index_values": extracted,
        }

    return result

def debug_defined_index(class_or_name, index_name: str, value=None, config_uid=None, limit=50):
    import os

    cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")

    index_name = str(index_name or "").strip()
    if not index_name:
        raise ValueError("index_name is empty")

    storage_key = f"{cls.__name__}_{cfg_uid}__idx__{index_name}" if cfg_uid else f"{cls.__name__}__idx__{index_name}"
    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
    store = cls._defined_index_storage(index_name, cfg_uid)

    def _lookup_variants(one):
        raw = "" if one is None else str(one).strip()
        if raw == "":
            return []
        variants = []
        seen = set()

        def add(v):
            s = str(v or "").strip()
            if not s or s in seen:
                return
            seen.add(s)
            variants.append(s)

        add(raw)
        try:
            uid_cfg, uid_cls, internal_id = parse_uid_any(raw)
        except Exception:
            uid_cfg, uid_cls, internal_id = None, None, None

        if internal_id:
            add(internal_id)
            if uid_cls:
                add(f"{uid_cls}${internal_id}")
                if cfg_uid:
                    add(f"{cfg_uid}${uid_cls}${internal_id}")
        return variants

    def _normalized_node_ref(one):
        raw = "" if one is None else str(one).strip()
        if not raw:
            return None
        try:
            _cfg, ref_cls, ref_id = parse_uid_any(raw)
        except Exception:
            ref_cls, ref_id = None, None
        if ref_cls and ref_id:
            return (str(ref_cls).strip(), str(ref_id).strip())
        return None

    result = {
        "class": cls.__name__,
        "config_uid": cfg_uid,
        "index_name": index_name,
        "db_path": db_path,
        "value": value,
        "variants": [],
        "exact_hits": {},
        "normalized_value": _normalized_node_ref(value),
        "normalized_hits": {},
        "sample_keys": [],
        "sample_size": 0,
    }

    variants = _lookup_variants(value)
    result["variants"] = variants

    for v in variants:
        try:
            bucket = list(store.get(v, []) or [])
        except Exception as e:
            bucket = [f"ERROR: {e}"]
        result["exact_hits"][v] = bucket

    try:
        keys = list(store.keys())
    except Exception as e:
        result["sample_keys"] = [f"ERROR reading keys: {e}"]
        return result

    result["sample_size"] = len(keys)
    result["sample_keys"] = [str(k) for k in keys[:limit]]

    wanted = _normalized_node_ref(value)
    if wanted:
        for k in keys:
            sk = str(k or "").strip()
            nref = _normalized_node_ref(sk)
            if nref == wanted:
                try:
                    bucket = list(store.get(k, []) or [])
                except Exception as e:
                    bucket = [f"ERROR: {e}"]
                result["normalized_hits"][sk] = bucket

    return result

def debug_class_indexes(class_or_name, config_uid=None):
    cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")
    return {
        "class": cls.__name__,
        "config_uid": cfg_uid,
        "defined_indexes": cls._get_defined_indexes(cfg_uid),
    }

def dump_defined_index_keys(class_or_name, index_name: str, config_uid=None, limit=200):
    cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")

    store = cls._defined_index_storage(index_name, cfg_uid)

    out = {}
    count = 0
    for k in store.keys():
        sk = str(k)
        out[sk] = list(store.get(k, []) or [])
        count += 1
        if count >= limit:
            break
    return out

def debug_node_index_value(class_or_name, node_or_id, index_name: str, config_uid=None):
    cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")

    if isinstance(node_or_id, str):
        node = cls.get(node_or_id, cfg_uid)
        if node is None:
            raise ValueError(f"Node not found: {node_or_id}")
    else:
        node = node_or_id

    defs = cls._get_defined_indexes(cfg_uid)
    idx_def = None
    for x in defs:
        if isinstance(x, dict) and str(x.get("name") or "").strip() == str(index_name).strip():
            idx_def = x
            break

    if not idx_def:
        raise ValueError(f"Index not found: {index_name}")

    data = dict(getattr(node, "_data", {}) or {})
    keys_spec = idx_def.get("keys")
    extracted = cls._extract_index_values(data, keys_spec)

    return {
        "class": cls.__name__,
        "config_uid": cfg_uid,
        "node_id": getattr(node, "_id", None),
        "index_name": index_name,
        "index_def": idx_def,
        "keys_spec": keys_spec,
        "node_value_by_keys": {k.strip(): data.get(k.strip()) for k in str(keys_spec or "").split("|") if k.strip()},
        "extracted_index_values": extracted,
    }

def current_handlers_dir() -> str:
    # ищем в стеке фрейм, который выполняется из Handlers/<uid>/handlers.py
    for fi in inspect.stack():
        try:
            fp = fi.frame.f_globals.get("__file__", "") or ""
        except Exception:
            fp = ""
        if fp and (os.sep + "Handlers" + os.sep) in fp and fp.endswith(os.sep + "handlers.py"):
            return os.path.dirname(fp)
    return ""

def current_config_uid_from_handlers() -> str:
    d = current_handlers_dir()
    if d:
        return os.path.basename(d)
    try:
        return (CURRENT_CONFIG_UID.get() or "").strip()
    except Exception:
        return ""

class AcceptRejected(Exception):
    """Expected business rejection from a node handler.

    ``status_code`` is optional and defaults to 200 for backward compatibility
    with existing web/mobile handlers. Integration handlers may request an
    HTTP error explicitly, for example ``status_code=500``.
    """
    def __init__(self, payload=None, status_code=200, error_code="ACCEPT_REJECTED"):
        self.payload = payload or {}
        try:
            code = int(status_code)
        except Exception:
            code = 200
        self.status_code = code if 100 <= code <= 599 else 200
        self.error_code = str(error_code or "ACCEPT_REJECTED")
        super().__init__(self.payload.get("error") or self.payload.get("message") or "Rejected")


class UiBreak(Exception):
    """Stop current web handler and ask the web client to show a node-list modal."""
    def __init__(self, payload=None):
        self.payload = payload or {}
        super().__init__(self.payload.get("title") or "UI break")

def set_runtime_context(config_uid: str | None, parsed_config: dict | None, system_user: dict | None = None):
    t1 = CURRENT_CONFIG_UID.set(config_uid)
    t2 = CURRENT_PARSED_CONFIG.set(parsed_config)
    t3 = CURRENT_SYSTEM_USER.set(system_user)
    # Reset per-request helpers
    RUNTIME_MESSAGES.set([])
    ACCEPT_GUARD.set(set())
    AFTER_ACCEPT_GUARD.set(set())
    
    DATASET_VIEW_CACHE.set({})
    DATASET_OBJ_CACHE.set({})
    DATASET_ID_CACHE.set({})
    
    return (t1, t2, t3)

def reset_runtime_context(tokens):
    try:
        t1, t2, t3 = tokens
    except Exception:
        t1, t2 = tokens
        t3 = None
    CURRENT_CONFIG_UID.reset(t1)
    CURRENT_PARSED_CONFIG.reset(t2)
    if t3 is not None:
        CURRENT_SYSTEM_USER.reset(t3)
    # Clear per-request helpers
    RUNTIME_MESSAGES.set(None)
    ACCEPT_GUARD.set(None)
    AFTER_ACCEPT_GUARD.set(None)


class SystemUserNode:
    """Small runtime wrapper exposed as _system_user inside Python/NodaScript."""
    def __init__(self, payload=None):
        payload = payload or {}
        data = payload.get('_data') if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            data = dict(payload) if isinstance(payload, dict) else {}
        self._data = data
        self._id = str((payload or {}).get('_id') or data.get('_id') or '')
        self._class = str((payload or {}).get('_class') or '_User')

    def get(self, key, default=None):
        return self._data.get(key, default)

    def to_dict(self):
        return {'_id': self._id, '_class': self._class, '_data': dict(self._data)}

    def __getattr__(self, name):
        try:
            return self._data.get(name)
        except Exception:
            return None


def current_system_user_data():
    payload = CURRENT_SYSTEM_USER.get()
    if payload is None:
        try:
            payload = _call_bridge('current_system_user_payload_global')
        except Exception:
            payload = None
    if isinstance(payload, dict) and payload.get('ok') is False:
        payload = None
    return payload or {'_id': '', '_class': '_User', '_data': {}}


def system_user_node():
    return SystemUserNode(current_system_user_data())


def _audit_user_id() -> str:
    """Stable user identifier for node audit fields.

    Runtime handlers receive the authenticated system user through
    ``CURRENT_SYSTEM_USER``.  Prefer its full node UID, then fall back to
    familiar identity fields.  Background jobs without a user are explicit.
    """
    payload = current_system_user_data()
    data = payload.get('_data') if isinstance(payload, dict) and isinstance(payload.get('_data'), dict) else {}
    for value in (
        payload.get('_id') if isinstance(payload, dict) else None,
        data.get('_id'), data.get('login'), data.get('email'), data.get('name'), data.get('user_id'),
    ):
        if value not in (None, ''):
            return str(value)
    return '_system'


def _apply_node_audit(data, saved_state=None):
    """Add immutable creation and current modification audit metadata."""
    if not isinstance(data, dict):
        return data
    old = saved_state if isinstance(saved_state, dict) else {}
    now = datetime.now(timezone.utc).isoformat()
    user_id = _audit_user_id()

    created_user = data.get('_created_user') or old.get('_created_user')
    created_date = data.get('_created_date') or old.get('_created_date')
    data['_created_user'] = str(created_user or user_id)
    data['_created_date'] = str(created_date or now)
    data['_last_change_user'] = user_id
    data['_last_change_date'] = now
    return data


def _rls_storage_key(config_uid: str, class_name: str) -> str:
    return f"rls_{str(config_uid or '').strip()}_{str(class_name or '').strip()}"


def _rls_storage(config_uid: str, class_name: str):
    os.makedirs(STORAGE_BASE_PATH, exist_ok=True)
    return SqliteDict(os.path.join(STORAGE_BASE_PATH, f"{_rls_storage_key(config_uid, class_name)}.sqlite"), autocommit=True)


def _rls_key(node_id: str, profile_uid: str) -> str:
    return f"{str(node_id or '').strip()}|{str(profile_uid or '').strip()}"


def set_rls_decision(config_uid: str, class_name: str, node_id: str, profile_uid: str, allowed: bool):
    with _rls_storage(config_uid, class_name) as st:
        st[_rls_key(node_id, profile_uid)] = bool(allowed)
    return True


def get_rls_decision(config_uid: str, class_name: str, node_id: str, profile_uid: str):
    with _rls_storage(config_uid, class_name) as st:
        key = _rls_key(node_id, profile_uid)
        if key not in st:
            return None
        return bool(st[key])


def clear_rls_decision(config_uid: str, class_name: str, node_id: str, profile_uid: str | None = None):
    with _rls_storage(config_uid, class_name) as st:
        if profile_uid is not None:
            st.pop(_rls_key(node_id, profile_uid), None)
            return True
        prefix = f"{str(node_id or '').strip()}|"
        for key in list(st.keys()):
            if str(key).startswith(prefix):
                st.pop(key, None)
    return True


def push_message(text: str, level: str = "info"):
    """Add a runtime message to be returned by API endpoints."""
    try:
        msg = {"text": str(text), "level": str(level or "info")}
        lst = RUNTIME_MESSAGES.get()
        if lst is None:
            lst = []
            RUNTIME_MESSAGES.set(lst)
        lst.append(msg)
    except Exception:
        pass



def _bridge_func(name: str):
    """Find a function exported by the Flask app/main module.

    Under flask run / gunicorn the app is often not __main__, so we scan loaded
    modules instead of depending only on import __main__.
    """
    name = str(name or '').strip()
    if not name:
        return None

    # Fast path: common module names first.
    for module_name in ('__main__', 'app', 'main', 'application', 'wsgi'):
        mod = sys.modules.get(module_name)
        fn = getattr(mod, name, None) if mod is not None else None
        if callable(fn):
            return fn

    # Slow but robust fallback: inspect all loaded modules.
    try:
        modules = list(sys.modules.values())
    except Exception:
        modules = []
    for mod in modules:
        try:
            fn = getattr(mod, name, None)
        except Exception:
            continue
        if callable(fn):
            return fn
    return None


def _call_bridge(name: str, *args, **kwargs):
    fn = _bridge_func(name)
    if not callable(fn):
        return {'ok': False, 'error': f'{name} is unavailable'}
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _current_config_uid() -> str:
    return str(current_config_uid_from_handlers() or CURRENT_CONFIG_UID.get() or '').strip()



def register_in_config_rooms_many(nodes, config_uid=None) -> dict:
    """Publish nodes to every room alias attached to their configuration.

    Payloads are grouped by class and sent in one room object per class, avoiding
    one database row / one FCM push per operation. Missing room aliases are a
    normal no-op, which lets configurations use contracts and rooms in parallel.
    The helper establishes its own Flask application context because it is
    normally called from a server ``runasync`` worker.
    """
    values = list(nodes or []) if isinstance(nodes, (list, tuple, set)) else ([nodes] if nodes is not None else [])
    if not values:
        return {'ok': True, 'rooms': 0, 'objects': 0, 'messages': 0}
    try:
        room_handler = _bridge_func('handle_room_objects')
        bridge_module = sys.modules.get(getattr(room_handler, '__module__', '')) if callable(room_handler) else None
        Configuration = getattr(bridge_module, 'Configuration', None) if bridge_module is not None else None
        RoomAlias = getattr(bridge_module, 'RoomAlias', None) if bridge_module is not None else None
        db = getattr(bridge_module, 'db', None) if bridge_module is not None else None
        app_obj = getattr(bridge_module, 'app', None) if bridge_module is not None else None
        if not callable(room_handler) or Configuration is None or RoomAlias is None or db is None:
            return {'ok': False, 'error': 'Room models are unavailable', 'rooms': 0, 'objects': 0, 'messages': 0}

        def publish():
            grouped = {}
            resolved_cfg_uid = str(config_uid or '').strip()
            for node in values:
                if node is None:
                    continue
                cfg_uid = str(resolved_cfg_uid or getattr(node, '_config_uid', '') or '').strip()
                class_name = str(getattr(node, '_schema_class_name', '') or node.__class__.__name__ or '').strip()
                try:
                    payload = node.to_dict() if hasattr(node, 'to_dict') else dict(node)
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                if not cfg_uid:
                    data = payload.get('_data') if isinstance(payload.get('_data'), dict) else payload
                    cfg_uid = str(payload.get('_config_uid') or data.get('_config_uid') or '').strip()
                if not class_name or class_name == 'dict':
                    data = payload.get('_data') if isinstance(payload.get('_data'), dict) else payload
                    class_ref = str(payload.get('_class') or data.get('_class') or '').strip()
                    class_name = class_ref.split('$')[-1] if class_ref else ''
                if cfg_uid and class_name:
                    grouped.setdefault((cfg_uid, class_name), []).append(payload)

            total_rooms = set()
            messages = 0
            objects = 0
            errors = []
            for (cfg_uid, class_name), payloads in grouped.items():
                cfg_obj = db.session.query(Configuration).filter(Configuration.uid == cfg_uid).first()
                if not cfg_obj:
                    continue
                aliases = db.session.query(RoomAlias).filter(RoomAlias.config_id == cfg_obj.id).all()
                room_uids = sorted({str(x.room_uid or '').strip() for x in aliases if str(x.room_uid or '').strip()})
                for room_uid in room_uids:
                    try:
                        room_handler(cfg_uid, class_name, room_uid, payloads)
                        total_rooms.add(room_uid)
                        messages += 1
                        objects += len(payloads)
                    except Exception as exc:
                        errors.append(f'{room_uid}:{class_name}: {exc}')
            return {
                'ok': not errors,
                'rooms': len(total_rooms),
                'objects': objects,
                'messages': messages,
                'errors': errors,
            }

        if app_obj is not None and hasattr(app_obj, 'app_context'):
            with app_obj.app_context():
                return publish()
        return publish()
    except Exception as exc:
        return {'ok': False, 'error': str(exc), 'rooms': 0, 'objects': 0, 'messages': 0}


def register_in_config_rooms(node, config_uid=None) -> dict:
    return register_in_config_rooms_many([node], config_uid=config_uid)

def message_user(user_key: str, payload=None, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
    result = _call_bridge('send_message_to_user_global', str(user_key or '').strip(), title, body, payload, sender_user=sender_user)
    if isinstance(result, dict) and result.get('error') == 'send_message_to_user_global is unavailable':
        return result
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def message_device(device_uid: str, payload=None, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
    result = _call_bridge('send_message_to_device_global', str(device_uid or '').strip(), title, body, payload, sender_user=sender_user)
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def sendTextMessage(target: str, text: str) -> dict:
    """Send a p2p/group text message from server-side PythonScript.

    target: user key for p2p or "group:<group_id>" for group chats.
    sender_user is fixed to "server" by the app bridge.
    """
    result = _call_bridge('_noda_send_text_message', str(target or '').strip(), str(text or ''))
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def sendImageMessage(target: str, text: str, filename: str) -> dict:
    """Send a p2p/group image message from server-side PythonScript."""
    result = _call_bridge(
        '_noda_send_image_message',
        str(target or '').strip(),
        str(text or ''),
        str(filename or ''),
        config_uid=_current_config_uid(),
    )
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def sendTextToNodeDiscussion(node, text: str) -> dict:
    """Send server text into all known discussions for this node."""
    result = _call_bridge('_noda_send_text_to_node_discussion', node, str(text or ''))
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def sendImageToNodeDiscussion(node, text: str, filename: str) -> dict:
    """Send server image into all known discussions for this node."""
    result = _call_bridge(
        '_noda_send_image_to_node_discussion',
        node,
        str(text or ''),
        str(filename or ''),
        config_uid=_current_config_uid(),
    )
    return result if isinstance(result, dict) else {'ok': True, 'result': result}


def _load_python_script_code(script_ref: str) -> str:
    """Load PythonScript source from cache-aware app bridge; fallback to URL/inline."""
    script_ref = str(script_ref or '').strip()
    if not script_ref:
        return ''

    fn = _bridge_func('_noda_load_python_script_code')
    if callable(fn):
        return str(fn(script_ref) or '')

    # Fallback if nodes.py is used outside the Flask app.
    if script_ref.startswith(('http://', 'https://')):
        with urllib.request.urlopen(script_ref, timeout=20) as resp:
            return resp.read().decode('utf-8')
    return script_ref


def downloadJsonCached(download_url: str, force_refresh: bool = False):
    """Load JSON object/class by download_url through the app download cache."""
    fn = _bridge_func('_noda_download_json_cached')
    if callable(fn):
        return fn(str(download_url or '').strip(), force_refresh=force_refresh)
    with urllib.request.urlopen(str(download_url or '').strip(), timeout=20) as resp:
        return json.loads(resp.read().decode('utf-8'))


def downloadNodeCached(download_url: str):
    """Alias for readability when the downloaded JSON is a node payload."""
    return downloadJsonCached(download_url)


def _accept_guard_key(node) -> str:
    return f"{getattr(node, '_config_uid', '')}:{getattr(node, '_schema_class_name', None) or node.__class__.__name__}:{getattr(node, '_id', '')}"

def _find_class_event_actions(parsed: dict, class_name: str, event_name: str, listener: str = "") -> list[dict]:
    cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
    actions: list[dict] = []
    for ev in (cls_cfg.get("events") or []):
        if (ev.get("event") or "") != event_name:
            continue
        ev_listener = str(ev.get("listener") or "").strip()

        # listener matching: как у api_node_event_web
        if listener:
            if ev_listener and ev_listener != listener:
                continue
        else:
            if ev_listener:
                continue

        actions.extend(ev.get("actions") or [])
    return actions

import inspect

def _coerce_handler_result(result) -> tuple[bool, dict]:
    if isinstance(result, tuple) and len(result) >= 1:
        ok = bool(result[0])
        data = result[1] if len(result) > 1 and isinstance(result[1], dict) else {}
        return ok, data
    if isinstance(result, dict):
        if 'status' in result:
            return bool(result.get('status')), result
        if 'ok' in result and result.get('ok') is False:
            return False, result
        return True, result
    if result is False:
        return False, {}
    return True, {}


def _swarm_default_workers(total_count: int, explicit_workers=None) -> int:
    """Return a safe default worker count for backend-only swarm calls."""
    try:
        total = int(total_count or 0)
    except Exception:
        total = 0

    if explicit_workers is not None:
        try:
            n = int(explicit_workers)
            if n > 0:
                return max(1, min(n, total or n))
        except Exception:
            pass

    try:
        env = int(os.environ.get("NODA_SWARM_WORKERS") or "0")
        if env > 0:
            return max(1, min(env, total or env))
    except Exception:
        pass

    cpu = os.cpu_count() or 1
    # These calls are usually storage/business-method I/O, not pure Python CPU.
    # Keep the cap conservative so 10k nodes do not create 10k active threads.
    return max(1, min(total or 1, max(4, min(64, cpu * 4))))


def _swarm_iter_items(value):
    """Normalize target containers for CallSwarm without treating node dicts as mappings."""
    if value is None:
        return []
    if isinstance(value, dict):
        if any(k in value for k in ("uid", "_uid", "_id", "id", "_class", "class")):
            return [value]
        return list(value.values())
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _swarm_node_uid(node, fallback_config_uid: str = "") -> str:
    try:
        cls_name = getattr(node, "_schema_class_name", None) or node.__class__.__name__
        cfg = getattr(node, "_config_uid", None) or fallback_config_uid or current_config_uid_from_handlers()
        raw = getattr(node, "_id", None) or ""
        if cfg:
            return normalize_own_uid(cfg or "", cls_name, raw)
        return str(raw or "")
    except Exception:
        return str(getattr(node, "_id", "") or "")


def _swarm_resolve_class(class_or_name):
    try:
        if isinstance(class_or_name, type) and issubclass(class_or_name, Node):
            return class_or_name
    except Exception:
        pass
    if isinstance(class_or_name, str):
        name = str(class_or_name or "").strip()
        if name and "$" not in name:
            try:
                return Node._resolve_node_class(name)
            except Exception:
                return None
    return None


def _swarm_resolve_node(item, default_config_uid: str = ""):
    """Resolve a CallSwarm item to (node, key, error)."""
    if item is None:
        return None, "", "empty target"

    try:
        if isinstance(item, Node):
            return item, _swarm_node_uid(item, default_config_uid), ""
    except Exception:
        pass

    if isinstance(item, dict):
        raw_id = item.get("_id") or item.get("uid") or item.get("_uid") or item.get("id")
        raw_cls = item.get("_class") or item.get("class") or item.get("_class_name") or item.get("_schema_class_name")
        raw_cfg = item.get("_config_uid") or item.get("config_uid") or default_config_uid
        if not raw_id:
            return None, str(item), "target dict has no _id/id"
        uid_cfg, uid_cls, internal_id = parse_uid_any(raw_id)
        cls_name = raw_cls or uid_cls
        cls = _swarm_resolve_class(cls_name) if cls_name else None
        if cls is None:
            return None, str(raw_id), f"node class not found: {cls_name or ''}".strip()
        cfg = str(raw_cfg or uid_cfg or default_config_uid or "").strip()
        node = cls.get(raw_id, cfg)
        if node is None:
            return None, str(raw_id), "node not found"
        return node, _swarm_node_uid(node, cfg), ""

    if isinstance(item, str):
        raw = str(item or "").strip()
        if not raw:
            return None, "", "empty target id"
        uid_cfg, uid_cls, internal_id = parse_uid_any(raw)
        cls = _swarm_resolve_class(uid_cls) if uid_cls else None
        if cls is None and "$" not in raw:
            # A bare string may be a class name, but bare node ids are ambiguous.
            possible_cls = _swarm_resolve_class(raw)
            if possible_cls is not None:
                return possible_cls, raw, ""
        if cls is None:
            return None, raw, f"node class not found in id: {raw}"
        cfg = str(uid_cfg or default_config_uid or current_config_uid_from_handlers() or "").strip()
        node = cls.get(raw, cfg)
        if node is None:
            return None, raw, "node not found"
        return node, _swarm_node_uid(node, cfg), ""

    return None, str(item), f"unsupported target type: {type(item).__name__}"


def _swarm_expand_targets(targets, config_uid: str = ""):
    """Expand classes, ids, live nodes, get_all dicts into unique live node jobs."""
    cfg = str(config_uid or current_config_uid_from_handlers() or "").strip()
    nodes_out = []
    errors = {}
    seen = set()

    for item in _swarm_iter_items(targets):
        cls = _swarm_resolve_class(item)
        if cls is not None:
            try:
                all_nodes = cls.get_all(cfg)
                for node in _swarm_iter_items(all_nodes):
                    resolved, key, err = _swarm_resolve_node(node, cfg)
                    if err:
                        errors[key or str(node)] = {"ok": False, "data": {"error": err}}
                        continue
                    if key and key not in seen:
                        seen.add(key)
                        nodes_out.append((key, resolved))
            except Exception as e:
                name = getattr(cls, "__name__", str(item))
                errors[f"class:{name}"] = {"ok": False, "data": {"error": str(e)}}
            continue

        resolved, key, err = _swarm_resolve_node(item, cfg)
        # _swarm_resolve_node can return a class for a bare string class name.
        if isinstance(resolved, type):
            try:
                all_nodes = resolved.get_all(cfg)
                for node in _swarm_iter_items(all_nodes):
                    rn, rkey, rerr = _swarm_resolve_node(node, cfg)
                    if rerr:
                        errors[rkey or str(node)] = {"ok": False, "data": {"error": rerr}}
                        continue
                    if rkey and rkey not in seen:
                        seen.add(rkey)
                        nodes_out.append((rkey, rn))
            except Exception as e:
                name = getattr(resolved, "__name__", str(item))
                errors[f"class:{name}"] = {"ok": False, "data": {"error": str(e)}}
            continue

        if err:
            errors[key or str(item)] = {"ok": False, "data": {"error": err}}
            continue
        if key and key not in seen:
            seen.add(key)
            nodes_out.append((key, resolved))

    return nodes_out, errors


def _swarm_call_one(key, node, method_name: str, input_data=None, extra_args=(), config_uid: str = "", parsed_config=None):
    """Worker body for CallSwarm. It returns only serializable values."""
    t_cfg = None
    t_parsed = None
    try:
        if config_uid:
            t_cfg = CURRENT_CONFIG_UID.set(config_uid)
        if parsed_config is not None:
            t_parsed = CURRENT_PARSED_CONFIG.set(parsed_config)

        fn = getattr(node, str(method_name or "").strip(), None)
        if not callable(fn):
            return key, False, {"error": f"method not found: {method_name}"}

        if extra_args:
            raw = fn(input_data, *extra_args)
        else:
            # NodaLogic server handlers normally use method(self, input_data=None).
            # If the target method is legacy/no-arg, support it without hiding
            # TypeError raised inside methods that do accept input_data.
            try:
                sig = inspect.signature(fn)
                positional = [
                    p for p in sig.parameters.values()
                    if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                ]
                has_varargs = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
                if not has_varargs and len(positional) == 0:
                    raw = fn()
                else:
                    raw = fn(input_data)
            except (TypeError, ValueError):
                raw = fn(input_data)

        ok, data = _coerce_handler_result(raw)
        return key, ok, data
    except AcceptRejected as e:
        return key, False, dict(getattr(e, "payload", None) or {})
    except Exception as e:
        return key, False, {"error": str(e)}
    finally:
        try:
            if t_parsed is not None:
                CURRENT_PARSED_CONFIG.reset(t_parsed)
        except Exception:
            pass
        try:
            if t_cfg is not None:
                CURRENT_CONFIG_UID.reset(t_cfg)
        except Exception:
            pass


def CallSwarm(targets, method_name: str, input_data=None, *extra_args, max_workers=None, config_uid: str | None = None):
    """Backend-only synchronous parallel fan-out call over many nodes.

    Targets can be:
      - Node subclasses or class-name strings: CallSwarm([Cell, OuterCell], "method", {...})
      - node uid strings: ["cfg$Cell$1", "cfg$OuterCell$2"]
      - live Node objects
      - mappings returned by ClassName.get_all()

    Returns:
      (all_ok, {node_uid: {"ok": bool, "data": dict}})

    The function is intentionally server/backend oriented. Do not use it in
    Android handlers. Use max_workers=... or env NODA_SWARM_WORKERS to tune the
    pool size for large storages.
    """
    method = str(method_name or "").strip()
    if not method:
        return False, {"_swarm": {"ok": False, "data": {"error": "method_name is empty"}}}

    cfg = str(config_uid or current_config_uid_from_handlers() or CURRENT_CONFIG_UID.get() or "").strip()
    parsed = None
    try:
        parsed = CURRENT_PARSED_CONFIG.get()
    except Exception:
        parsed = None

    jobs, results = _swarm_expand_targets(targets, cfg)
    if not jobs:
        all_ok = all(v.get("ok") for v in results.values()) if results else True
        return bool(all_ok), results

    workers = _swarm_default_workers(len(jobs), max_workers)
    if workers <= 1 or len(jobs) <= 1:
        for key, node in jobs:
            k, ok, data = _swarm_call_one(key, node, method, input_data, extra_args, cfg, parsed)
            results[k] = {"ok": bool(ok), "data": data if isinstance(data, dict) else {}}
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="noda-swarm") as pool:
            futures = [
                pool.submit(_swarm_call_one, key, node, method, input_data, extra_args, cfg, parsed)
                for key, node in jobs
            ]
            for fut in as_completed(futures):
                try:
                    k, ok, data = fut.result()
                except Exception as e:
                    k, ok, data = f"_swarm_error_{len(results)+1}", False, {"error": str(e)}
                results[k] = {"ok": bool(ok), "data": data if isinstance(data, dict) else {}}

    all_ok = all(bool(v.get("ok")) for v in results.values()) if results else True
    return bool(all_ok), results


def _call_script_callable(fn, node, input_data):
    try:
        return fn(node, input_data)
    except TypeError:
        try:
            return fn(input_data)
        except TypeError:
            return fn()


def _run_python_script_action(node, action: dict, input_data: dict, *, text_key: str = 'methodText') -> tuple[bool, dict]:
    alt_text_key = 'post_execute_text' if text_key == 'postExecuteMethodText' else 'method_text'
    script_ref = str((action or {}).get(text_key) or (action or {}).get(alt_text_key) or '').strip()
    if not script_ref:
        return False, {'error': f'PythonScript has empty {text_key}'}

    code = _load_python_script_code(script_ref)
    if not str(code or '').strip():
        return False, {'error': 'PythonScript source is empty'}

    ns = {
        '__name__': '__noda_python_script__',
        'node': node,
        'self': node,
        'input_data': input_data if isinstance(input_data, dict) else {},
        'CURRENT_NODE': node,
        'CURRENT_CONFIG_UID': _current_config_uid(),
        'sendTextMessage': sendTextMessage,
        'sendImageMessage': sendImageMessage,
        'sendTextToNodeDiscussion': sendTextToNodeDiscussion,
        'sendImageToNodeDiscussion': sendImageToNodeDiscussion,
        'message_user': message_user,
        'message_device': message_device,
        'push_message': push_message,
        'json': json,
        'datetime': datetime,
        'timezone': timezone,
    }

    try:
        exec(compile(code, f'<PythonScript:{hashlib.sha1(script_ref.encode("utf-8")).hexdigest()[:12]}>', 'exec'), ns, ns)

        result = ns.get('result', None)
        if result is None:
            # Optional convention for scripts stored as function libraries.
            for fn_name in ('handler', 'main'):
                fn = ns.get(fn_name)
                if callable(fn):
                    result = _call_script_callable(fn, node, input_data if isinstance(input_data, dict) else {})
                    break

        return _coerce_handler_result(result)
    except AcceptRejected as e:
        return False, e.payload
    except Exception as e:
        return False, {'error': str(e), 'script': script_ref}


def _execute_event_action(node, action: dict, input_data: dict, method_key: str = 'method') -> tuple[bool, dict]:
    m = str((action or {}).get(method_key) or '').strip()
    if not m:
        return True, {}

    if m == 'PythonScript':
        text_key = 'postExecuteMethodText' if method_key == 'postExecuteMethod' else 'methodText'
        return _run_python_script_action(node, action, input_data, text_key=text_key)

    # NodaScript remains a client/runtime concern here.  We do not fail server
    # saves just because an old event contains a NodaScript action.
    if m == 'NodaScript':
        return True, {}

    fn = getattr(node, m, None)
    if not callable(fn):
        return False, {'error': f"Handler method '{m}' not found"}

    try:
        return _coerce_handler_result(fn(input_data))
    except AcceptRejected as e:
        return False, e.payload
    except Exception as e:
        return False, {'error': str(e), 'method': m}


def _submit_server_event_action(node, action: dict, input_data: dict, event_name: str = "") -> bool:
    """Queue one server ClassEvent action without delaying the current save/API call.

    Client-side ``runasync`` already used the background job pool, but server
    lifecycle events (onAcceptServer/onAfterAcceptServer) historically ignored
    the action mode and executed every action inline.  This helper preserves the
    current configuration, parsed config and system user context, then reloads
    the node in the worker before calling the action.  The original request does
    not wait for the result.
    """
    if _background_jobs is None:
        return False

    try:
        action_copy = copy.deepcopy(action or {})
    except Exception:
        action_copy = dict(action or {})
    try:
        input_copy = copy.deepcopy(input_data or {})
    except Exception:
        input_copy = dict(input_data or {})

    node_class = node.__class__
    node_id = str(getattr(node, "_id", "") or "")
    config_uid = str(
        getattr(node, "_config_uid", "")
        or _config_uid_from_node_or_uid(getattr(node, "_data", {}).get("_id") if isinstance(getattr(node, "_data", None), dict) else "")
        or CURRENT_CONFIG_UID.get()
        or current_config_uid_from_handlers()
        or ""
    ).strip()
    try:
        parsed_copy = copy.deepcopy(CURRENT_PARSED_CONFIG.get())
    except Exception:
        parsed_copy = CURRENT_PARSED_CONFIG.get()
    try:
        system_user_copy = copy.deepcopy(CURRENT_SYSTEM_USER.get())
    except Exception:
        system_user_copy = CURRENT_SYSTEM_USER.get()

    method_name = str(action_copy.get("method") or "").strip()
    post_method_name = str(action_copy.get("postExecuteMethod") or "").strip()

    def work():
        tokens = []
        try:
            tokens.append((CURRENT_CONFIG_UID, CURRENT_CONFIG_UID.set(config_uid or None)))
            tokens.append((CURRENT_PARSED_CONFIG, CURRENT_PARSED_CONFIG.set(parsed_copy)))
            tokens.append((CURRENT_SYSTEM_USER, CURRENT_SYSTEM_USER.set(system_user_copy)))
            tokens.append((RUNTIME_MESSAGES, RUNTIME_MESSAGES.set([])))
            tokens.append((ACCEPT_GUARD, ACCEPT_GUARD.set(set())))
            tokens.append((AFTER_ACCEPT_GUARD, AFTER_ACCEPT_GUARD.set(set())))
            tokens.append((DATASET_VIEW_CACHE, DATASET_VIEW_CACHE.set({})))
            tokens.append((DATASET_OBJ_CACHE, DATASET_OBJ_CACHE.set({})))
            tokens.append((DATASET_ID_CACHE, DATASET_ID_CACHE.set({})))

            fresh = None
            try:
                fresh = node_class.get(node_id, config_uid) if node_id else None
            except Exception:
                fresh = None
            target = fresh or node

            previous_current = globals().get("CURRENT_NODE")
            globals()["CURRENT_NODE"] = target
            try:
                ok, data = _execute_event_action(target, action_copy, input_copy, "method")
                if not ok:
                    raise RuntimeError(str((data or {}).get("error") or data or f"Async handler {method_name} failed"))

                if post_method_name:
                    post_input = dict(input_copy)
                    if data:
                        post_input["_previous_result"] = data
                    ok, post_data = _execute_event_action(target, action_copy, post_input, "postExecuteMethod")
                    if not ok:
                        raise RuntimeError(str((post_data or {}).get("error") or post_data or f"Async post handler {post_method_name} failed"))
                return {"ok": True, "method": method_name, "post_method": post_method_name}
            finally:
                globals()["CURRENT_NODE"] = previous_current
        finally:
            for var, token in reversed(tokens):
                try:
                    var.reset(token)
                except Exception:
                    pass

    _background_jobs.submit(
        work,
        owner="server:" + config_uid,
        mode="runasync",
        title=(str(event_name or "Server event") + (": " + method_name if method_name else "")),
    )
    return True


def dispatch_node_class_event(node, event_name: str, input_data: dict) -> tuple[bool, dict]:
    parsed = CURRENT_PARSED_CONFIG.get()
    if not isinstance(parsed, dict):
        return True, {}

    cls_name = getattr(node, "_schema_class_name", None) or node.__class__.__name__

    listener = ""
    if isinstance(input_data, dict):
        listener = str(input_data.get("listener") or input_data.get("id") or "").strip()

    actions = _find_class_event_actions(parsed, cls_name, event_name, listener)
    if not actions:
        return True, {}

    prev_current = globals().get("CURRENT_NODE")
    globals()["CURRENT_NODE"] = node
    try:
        for action in actions:
            action_mode = str((action or {}).get("action") or "run").strip().lower()
            if str(event_name or "").strip() == "onAfterAcceptServer" and action_mode in {"runasync", "runprogress"}:
                # Only post-save actions may detach safely. onAcceptServer is a
                # validation/normalization gate and must remain synchronous so
                # it can reject the save.
                if _submit_server_event_action(node, action, input_data, event_name):
                    continue
                # Safe fallback when the optional job module is unavailable.

            ok, data = _execute_event_action(node, action, input_data, 'method')
            if not ok:
                return False, data or {}

            post_method = str((action or {}).get('postExecuteMethod') or '').strip()
            if post_method:
                post_input = dict(input_data or {}) if isinstance(input_data, dict) else {}
                if data:
                    post_input['_previous_result'] = data
                ok, post_data = _execute_event_action(node, action, post_input, 'postExecuteMethod')
                if not ok:
                    return False, post_data or {}

        return True, {}
    finally:
        globals()["CURRENT_NODE"] = prev_current


def _json_class_name(class_obj) -> str:
    if isinstance(class_obj, str):
        return class_obj.strip()
    if isinstance(class_obj, dict):
        for key in ('name', 'code', 'uid', 'id', 'full_name'):
            value = class_obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ''


def _json_node_events(node_payload: dict) -> tuple[str, list[dict]]:
    if not isinstance(node_payload, dict):
        return '', []
    class_obj = node_payload.get('_class') or node_payload.get('class')
    class_name = _json_class_name(class_obj)
    events = []
    if isinstance(class_obj, dict):
        events = class_obj.get('events') or []
    if not events:
        events = node_payload.get('events') or []
    return class_name, events if isinstance(events, list) else []


def dispatch_json_node_event(node_payload: dict, event_name: str, input_data: dict | None = None) -> tuple[bool, dict]:
    """Dispatch event for a JSON node whose class is embedded as JSON.

    This covers the external/raw-node case: node lives by download_url, its
    `_class` can be a JSON object with events, and PythonScript action source is
    still loaded via the same URL cache.
    """
    class_name, events = _json_node_events(node_payload)
    if not events:
        return True, {}

    listener = ''
    if isinstance(input_data, dict):
        listener = str(input_data.get('listener') or input_data.get('id') or '').strip()

    actions = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        if str(ev.get('event') or '').strip() != str(event_name or '').strip():
            continue
        ev_listener = str(ev.get('listener') or '').strip()
        if listener:
            if ev_listener and ev_listener != listener:
                continue
        else:
            if ev_listener:
                continue
        for action in (ev.get('actions') or []):
            if isinstance(action, dict):
                actions.append(action)

    if not actions:
        return True, {}

    prev_current = globals().get('CURRENT_NODE')
    globals()['CURRENT_NODE'] = node_payload
    try:
        for action in actions:
            ok, data = _execute_event_action(node_payload, action, input_data or {}, 'method')
            if not ok:
                return False, data or {}
            post_method = str(action.get('postExecuteMethod') or '').strip()
            if post_method:
                post_input = dict(input_data or {})
                if data:
                    post_input['_previous_result'] = data
                ok, post_data = _execute_event_action(node_payload, action, post_input, 'postExecuteMethod')
                if not ok:
                    return False, post_data or {}
        return True, {}
    finally:
        globals()['CURRENT_NODE'] = prev_current


def dispatch_downloaded_node_event(download_url: str, event_name: str, input_data: dict | None = None, *, force_refresh: bool = False) -> tuple[bool, dict]:
    node_payload = downloadJsonCached(download_url, force_refresh=force_refresh)
    return dispatch_json_node_event(node_payload, event_name, input_data or {})

def run_on_accept_server_once(node, saved_state: dict, input_data: dict | None = None) -> None:
    """Run config ClassEvent 'onAcceptServer' at most once per request per node.

    Raises AcceptRejected if rejected.
    """
    
    if "_skip_accept_handler" in node._data:
        del node._data["_skip_accept_handler"]
        node._save()
        return

    guard = ACCEPT_GUARD.get()
    if guard is None:
        guard = set()
        ACCEPT_GUARD.set(guard)

    key = _accept_guard_key(node)
    if key in guard:
        return
    guard.add(key)

    payload = dict(input_data or {})
    payload["_saved_state"] = dict(saved_state or {})
    ok, out = dispatch_node_class_event(node, "onAcceptServer", payload)
    if not ok:
        # Also attach runtime messages if any
        # If handler used nodes.message() (Node.Message), attach it as message payload too
        try:
            ui_msgs = getattr(node, "_ui_message", None)
            if isinstance(ui_msgs, list) and ui_msgs:
                out = dict(out or {})
                out.setdefault("messages", ui_msgs)
                out.setdefault("message", ui_msgs[-1])
                # one-shot: keep consistent with other UI hints
                try:
                    delattr(node, "_ui_message")
                except Exception:
                    pass
        except Exception:
            pass
        raise AcceptRejected(out)


def run_on_after_accept_server_once(node, saved_state: dict, input_data: dict | None = None) -> None:
    """Run config ClassEvent 'onAfterAcceptServer' at most once per request per node.

    This hook runs AFTER the node state has been persisted.

    Note: unlike onAcceptServer, this hook is not used to reject the operation.
    """
    guard = AFTER_ACCEPT_GUARD.get()
    if guard is None:
        guard = set()
        AFTER_ACCEPT_GUARD.set(guard)

    key = _accept_guard_key(node)
    if key in guard:
        return
    guard.add(key)

    payload: dict = dict(input_data or {})
    payload["_saved_state"] = dict(saved_state or {})
    started = time.perf_counter()
    try:
        ok, out = dispatch_node_class_event(node, "onAfterAcceptServer", payload)
        if not ok:
            print(
                "onAfterAcceptServer failed",
                getattr(node, "_schema_class_name", None) or node.__class__.__name__,
                getattr(node, "_id", ""),
                out,
            )
    except Exception as exc:
        # Post-save hook must never break the main flow, but silently swallowing
        # the error made failed WMS planning indistinguishable from success.
        print(
            "onAfterAcceptServer exception",
            getattr(node, "_schema_class_name", None) or node.__class__.__name__,
            getattr(node, "_id", ""),
            repr(exc),
        )
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if elapsed_ms >= 1000.0:
            print(
                "onAfterAcceptServer slow",
                getattr(node, "_schema_class_name", None) or node.__class__.__name__,
                getattr(node, "_id", ""),
                round(elapsed_ms, 3),
                "ms",
            )


STORAGE_BASE_PATH = 'node_storage'
os.makedirs(STORAGE_BASE_PATH, exist_ok=True)
SCHEMES_DB_PATH = os.path.join(STORAGE_BASE_PATH, "node_schemes.sqlite")
try:
    _SCHEMES_STORAGE = SqliteDict(SCHEMES_DB_PATH, autocommit=True)
except Exception:
    
    _SCHEMES_STORAGE = {}

_NODE_CLASS_REGISTRY = {}

class Node:
    
    _schemes = {}
    
    _class_storages = {}
    _storage_locks = {}  
    _instance_locks = {}  
    
    _date_index_storages = {}
    _date_index_locks = {}
    _defined_index_storages = {}
    _defined_index_locks = {}
    _semantic_index_locks = {}
    _semantic_model_cache = {}
    # Model loading used to be guarded by one global lock.  A background
    # warm-up/download of any stale or unavailable model then blocked searches
    # for every other semantic index as well.  Keep one lock per concrete
    # model/cache pair instead: loading USER2 must never freeze an already
    # cached multilingual-e5-small search.
    _semantic_model_locks = {}
    _semantic_model_locks_guard = threading.RLock()
    _global_index_storages = {}
    _global_index_locks = {}

    # Optional per-index background writes.  The executor is deliberately
    # shared and single-threaded so updates submitted by one server process are
    # applied in save order.  Index definitions remain synchronous by default.
    _async_index_executor = None
    _async_index_executor_lock = threading.RLock()
    _text_sidecar_rebuild_pending = set()
    _text_sidecar_rebuild_pending_lock = threading.RLock()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        
        
        if cls.__name__ != "Node":
            _NODE_CLASS_REGISTRY[cls.__name__] = cls
    @classmethod
    def _resolve_node_class(cls, class_or_name):
        
        if isinstance(class_or_name, type):
            return class_or_name
        if isinstance(class_or_name, str):
            name = class_or_name.strip()
            if not name:
                return None
            return _NODE_CLASS_REGISTRY.get(name)
        return class_or_name
    
    @classmethod
    def _resolve_class(cls, class_or_name):
        
        if isinstance(class_or_name, str):
            name = class_or_name.strip()
            if not name:
                raise ValueError("Empty class name")

            
            g = getattr(cls, "__dict__", {})
            mod_globals = getattr(__import__(cls.__module__), "__dict__", {})

            
            if name in mod_globals and isinstance(mod_globals[name], type):
                return mod_globals[name]

            raise ValueError(f"Unknown node class: {name}")

        
        if isinstance(class_or_name, type):
            return class_or_name

        raise TypeError(f"Invalid class spec: {class_or_name!r}")

    @staticmethod
    def _resolve_room_uid(alias_or_uid: str, config_uid: str) -> str:
        alias = str(alias_or_uid or "").strip()
        if not alias:
            return ""

        # uid passed directly
        if len(alias) >= 32 and ("-" in alias):
            return alias

        try:
            import __main__ as main
            Configuration = getattr(main, "Configuration", None)
            RoomAlias = getattr(main, "RoomAlias", None)
            db = getattr(main, "db", None)
            if Configuration is None or RoomAlias is None or db is None:
                return ""

            cfg_obj = db.session.query(Configuration).filter(Configuration.uid == config_uid).first()
            if not cfg_obj:
                return ""

            ra = (
                db.session.query(RoomAlias)
                .filter(RoomAlias.config_id == cfg_obj.id, RoomAlias.alias == alias)
                .first()
            )
            return str(ra.room_uid or "").strip() if ra else ""
        except Exception:
            return ""
    
    @classmethod
    def Register(cls, uids: list, room_alias: str, config_uid: str = None) -> dict:
        """
        Bulk register nodes of THIS class into a room.
        Call like: ReceiptPosition.Register([uid1, uid2, ...], "kitchen")

        Returns: {"ok": bool, "room_uid": str, "count": int, "errors": [..]}
        """
        # 1) determine config uid
        cfg_uid = str(config_uid or "").strip()
        if not cfg_uid:
            # try to derive from first uid
            try:
                first = (uids or [None])[0]
                uid_cfg, _, _ = parse_uid_any(first)
                cfg_uid = str(uid_cfg or "").strip()
            except Exception:
                cfg_uid = ""

        if not cfg_uid:
            return {"ok": False, "room_uid": "", "count": 0, "errors": ["config_uid is empty"]}

        # 2) resolve room uid ONCE (alias -> room_uid via DB)
        room_uid = cls._resolve_room_uid(room_alias, cfg_uid) if hasattr(cls, "_resolve_room_uid") else ""
        if not room_uid:
            # if you kept resolver as Node._resolve_room_uid, call it explicitly
            try:
                room_uid = Node._resolve_room_uid(room_alias, cfg_uid)
            except Exception:
                room_uid = ""

        if not room_uid:
            return {"ok": False, "room_uid": "", "count": 0, "errors": [f"room alias not found: {room_alias}"]}

        # 3) build objects (use cls.get(uid) so it understands composite IDs)
        objs = []
        errors = []
        for raw_uid in (uids or []):
            try:
                n = cls.get(raw_uid, None)  # ✅ let get() parse any uid format
                if not n:
                    errors.append(f"not found: {raw_uid}")
                    continue

                try:
                    d = n.to_dict() if hasattr(n, "to_dict") else {}
                except Exception:
                    d = {}
                if not isinstance(d, dict):
                    d = {}
                d.setdefault("_id", getattr(n, "_id", None) or str(raw_uid))
                objs.append(d)

            except Exception as e:
                errors.append(f"{raw_uid}: {e}")

        if not objs:
            return {"ok": False, "room_uid": room_uid, "count": 0, "errors": (errors or ["no nodes"])}

        # 4) one write + one send
        try:
            import __main__ as main
            class_name = str(getattr(cls, "_schema_class_name", "") or cls.__name__)
            main.handle_room_objects(cfg_uid, class_name, room_uid, objs)

            return {"ok": True, "room_uid": room_uid, "count": len(objs), "errors": errors}
        except Exception as e:
            errors.append(str(e))
            return {"ok": False, "room_uid": room_uid, "count": 0, "errors": errors}



    def __init__(self, node_id=None, config_uid=None):
        self._id = node_id or str(uuid.uuid4())
        self._config_uid = config_uid
        
        self._schema_class_name = getattr(self, "_schema_class_name", None) or self.__class__.__name__
        self._storage = None
        self._data_cache = None  
        
        
        if self._id not in Node._instance_locks:
            Node._instance_locks[self._id] = threading.RLock()
        
        self._lock = Node._instance_locks[self._id]
        
        with self._lock:
            self._init_storage()
            
            if self._id not in self._storage:
                
                initial_data = _apply_node_audit({
                    '_id': self._id,
                    '_class': self.__class__.__name__
                })
                self._storage[self._id] = {
                    '_id': self._id,
                    '_class': self.__class__.__name__,
                    '_config_uid': config_uid,
                    '_data': initial_data,
                    '_created_at': datetime.now(timezone.utc).isoformat(),
                    '_updated_at': datetime.now(timezone.utc).isoformat()
                }
            else:
                # Constructing/loading an existing node is a read operation.
                # Do not rewrite storage or touch _updated_at here: contract
                # delivery hashes use persisted node content, and a read-side
                # timestamp update made every acknowledged object look changed.
                # get_data(), _data and to_dict() normalize _id/_class in memory.
                pass
    
    @property
    def _data(self):
        
        with self._lock:
            if self._data_cache is None:
                if self._id in self._storage:
                    stored = self._storage[self._id]
                    
                    data = dict(stored.get('_data', {}) or {})

                    
                    data.setdefault('_class', self.__class__.__name__)

                    
                    data['_id'] = normalize_own_uid(
                        self._config_uid,
                        self.__class__.__name__,
                        data.get('_id') or self._id
                    )

                    self._data_cache = data
                else:
                    self._data_cache = {}
            return self._data_cache

    
    @_data.setter
    def _data(self, value):
        
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                old = node_data.get('_data')
                saved_state = dict(old) if isinstance(old, dict) else {}

                # make new value visible to handler via cache
                self._data_cache = dict(value) if isinstance(value, dict) else value

                # run accept hook BEFORE persisting
                run_on_accept_server_once(self, saved_state)

                # persist what handler left in cache (it may have modified _data)
                to_write = self._data_cache
                if isinstance(to_write, dict):
                    audited = _apply_node_audit(dict(to_write), saved_state)
                    self._data_cache = audited
                    node_data['_data'] = audited
                else:
                    node_data['_data'] = to_write
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
    
    def _save(self):
        
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]

                # snapshot state currently stored in DB (before modifications)
                old = node_data.get("_data")
                saved_state = dict(old) if isinstance(old, dict) else {}

                stored = node_data.get("_data")
                if not isinstance(stored, dict):
                    stored = {}

                
                if isinstance(getattr(self, "_data", None), dict):
                    stored = dict(self._data)
                
                stored['_class'] = self.__class__.__name__
                stored['_id'] = normalize_own_uid(self._config_uid, self.__class__.__name__, stored.get('_id') or self._id)    

                
                # expose new state to handler
                self._data_cache = dict(stored)

                # run accept hook BEFORE persisting (only once per request)
                run_on_accept_server_once(self, saved_state)

                # persist what handler left in cache (it may have modified _data)
                to_write = self._data_cache
                if isinstance(to_write, dict):
                    audited = _apply_node_audit(dict(to_write), saved_state)
                    self._data_cache = audited
                    node_data["_data"] = audited
                else:
                    node_data["_data"] = _apply_node_audit(dict(stored), saved_state)

                node_data["_updated_at"] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data

                # --- update date index (best-effort; never break save) ---
                try:
                    old_date = None
                    if isinstance(saved_state, dict):
                        old_date = saved_state.get("_date_key") or saved_state.get("_date")

                    new_state = node_data.get("_data") or {}
                    new_date = None
                    if isinstance(new_state, dict):
                        new_date = new_state.get("_date_key") or new_state.get("_date")

                    old_dk = normalize_date_key(old_date)
                    new_dk = normalize_date_key(new_date)

                    # persist normalized key for consistency
                    if new_dk and isinstance(new_state, dict):
                        if new_state.get("_date_key") != new_dk:
                            new_state["_date_key"] = new_dk
                            node_data["_data"] = new_state
                            self._storage[self._id] = node_data

                    idx = self.__class__._get_date_index_storage(self._config_uid)

                    if old_dk and old_dk != new_dk:
                        old_k = self.__class__._date_index_key(old_dk, self._id)
                        try:
                            if old_k in idx:
                                del idx[old_k]
                        except Exception:
                            pass

                    if new_dk:
                        new_k = self.__class__._date_index_key(new_dk, self._id)
                        if old_dk != new_dk or new_k not in idx:
                            idx[new_k] = 1
                except Exception:
                    pass

                try:
                    self.__class__._update_defined_indexes(self._config_uid, self._id, saved_state, node_data.get("_data") or {})
                except Exception:
                    pass

                try:
                    self.__class__._update_global_indexes(
                        normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                        saved_state,
                        node_data.get("_data") or {}
                    )
                except Exception:
                    pass

                try:
                    _call_bridge(
                        'update_node_rls_index_global',
                        self._config_uid,
                        self.__class__.__name__,
                        self._id,
                        node_data.get("_data") or {}
                    )
                except Exception:
                    pass

                # run post-save hook AFTER persisting (only once per request)
                run_on_after_accept_server_once(self, saved_state, {})
                return True
            return False
        

    def message_user(self, user_key: str, payload=None, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
        return message_user(user_key, payload, title=title, body=body, sender_user=sender_user)

    def message_device(self, device_uid: str, payload=None, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
        return message_device(device_uid, payload, title=title, body=body, sender_user=sender_user)

    def send_to_user(self, user_key: str, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
        return message_user(user_key, self, title=title, body=body, sender_user=sender_user)

    def send_to_device(self, device_uid: str, title: str = "Direct message", body: str = "New message", sender_user: str | None = None) -> dict:
        return message_device(device_uid, self, title=title, body=body, sender_user=sender_user)

    def sendTextMessage(self, target: str, text: str) -> dict:
        return sendTextMessage(target, text)

    def sendImageMessage(self, target: str, text: str, filename: str) -> dict:
        return sendImageMessage(target, text, filename)

    def sendTextToNodeDiscussion(self, text: str) -> dict:
        return sendTextToNodeDiscussion(self, text)

    def sendImageToNodeDiscussion(self, text: str, filename: str) -> dict:
        return sendImageToNodeDiscussion(self, text, filename)

    def _register(self, room_alias: str) -> bool:
        """
        Register this node into a room by alias.
        Alias -> room_uid is stored in DB (RoomAlias), not in parsed_config.
        """
        alias = str(room_alias or "").strip()
        if not alias:
            try: self.Message("Room alias is empty", "warning")
            except Exception: pass
            try: push_message("Room alias is empty", "warning")
            except Exception: pass
            return False

        cfg_uid = str(getattr(self, "_config_uid", "") or "").strip()
        class_name = str(getattr(self, "_schema_class_name", "") or self.__class__.__name__).strip()

        # 1) If user passed room_uid directly (36 chars uuid) — accept
        room_uid = ""
        if len(alias) >= 32 and ("-" in alias):
            room_uid = alias

        # 2) Resolve alias via DB RoomAlias for this Configuration.uid
        if not room_uid:
            try:
                import __main__ as main

                Configuration = getattr(main, "Configuration", None)
                RoomAlias = getattr(main, "RoomAlias", None)
                db = getattr(main, "db", None)

                if Configuration is None or RoomAlias is None or db is None:
                    raise RuntimeError("DB models not available in __main__")

                cfg_obj = db.session.query(Configuration).filter(Configuration.uid == cfg_uid).first()
                if cfg_obj:
                    ra = (
                        db.session.query(RoomAlias)
                        .filter(RoomAlias.config_id == cfg_obj.id, RoomAlias.alias == alias)
                        .first()
                    )
                    if ra:
                        room_uid = str(ra.room_uid or "").strip()
            except Exception as e:
                try: self.Message(f"Room alias resolve failed: {e}", "danger")
                except Exception: pass
                try: push_message(f"Room alias resolve failed: {e}", "danger")
                except Exception: pass
                return False

        if not room_uid:
            msg = f"Room alias not found in DB: {alias}"
            try: self.Message(msg, "warning")
            except Exception: pass
            try: push_message(msg, "warning")
            except Exception: pass
            return False

        # 3) Prepare object payload like standard registration
        try:
            d = self.to_dict() if hasattr(self, "to_dict") else {}
        except Exception:
            d = {}
        if not isinstance(d, dict):
            d = {}
        d.setdefault("_id", self._id)

        # 4) Queue into room via the same server helper
        try:
            import __main__ as main
            rv = main.handle_room_objects(cfg_uid, class_name, room_uid, [d])

            response_obj = rv[0] if isinstance(rv, tuple) and rv else rv
            result = {}
            try:
                if hasattr(response_obj, "get_json"):
                    result = response_obj.get_json(silent=True) or {}
                elif isinstance(response_obj, dict):
                    result = response_obj
            except Exception:
                result = {}

            push = result.get("push") if isinstance(result, dict) else None
            transport = str((result or {}).get("transport") or "").strip().lower()
            if transport == "fcm" and isinstance(push, dict) and not push.get("ok"):
                err = str(push.get("error") or (result or {}).get("delivery_error") or "FCM push failed")
                msg = f"Queued in room, but FCM delivery failed: {err}"
                try: self.Message(msg, "danger")
                except Exception: pass
                try: push_message(msg, "danger")
                except Exception: pass
                return False

            msg = f"Registered in room: {room_uid}"
            try: self.Message(msg, "success")
            except Exception: pass
            try: push_message(msg, "success")
            except Exception: pass
            return True
        except Exception as e:
            msg = f"Register failed: {e}"
            try: self.Message(msg, "danger")
            except Exception: pass
            try: push_message(msg, "danger")
            except Exception: pass
            return False

    def _open(self, *, new_tab: bool = True):
        
        try:
            #import nodes as _nodes_mod
            #host = getattr(_nodes_mod, "CURRENT_NODE", None)
            #if host is None:
            import nodes as _nodes_mod
            host = getattr(_nodes_mod, "CURRENT_NODE", None) or self

            host._ui_open = {
                "config_uid": str(getattr(self, "_config_uid", "") or ""),
                # class name used by web-client routes (from config)
                "class_name": str(getattr(self, "_schema_class_name", None) or self.__class__.__name__),
                "node_id": str(getattr(self, "_id", "") or ""),
                "new_tab": bool(new_tab),
            }
        except Exception as e:
            pass

    def CloseNode(self):
        
        try:
            import nodes as _nodes_mod
            host = getattr(_nodes_mod, "CURRENT_NODE", None) or self
            host._ui_close = True
        except Exception:
            pass    

    
    @classmethod
    def create(self, node_id=None, initial_data=None):
        """
        Creates a new node of the same class and current runtime configuration.

        Args:
        node_id: ID of the new node (automatically generated if not specified).
                 If a dict is passed here, it is treated as initial_data.
        initial_data: Initial data for the new node

        Returns:
        Node: New node instance
        """
        # Support short handler syntax:
        #   ContainerLine.create({"parent_doc": self._data["_id"], "qty": 1})
        if isinstance(node_id, dict) and initial_data is None:
            initial_data = node_id
            node_id = None

        config_uid = ""

        # In server handlers CURRENT_NODE is the node whose method is running.
        # Prefer it because this is exactly the configuration the user expects.
        try:
            current_node = globals().get("CURRENT_NODE")
            config_uid = str(getattr(current_node, "_config_uid", "") or "").strip()
        except Exception:
            config_uid = ""

        # Fallback to runtime ContextVar / Handlers/<uid>/handlers.py detection.
        if not config_uid:
            try:
                config_uid = str(_current_config_uid() or "").strip()
            except Exception:
                config_uid = ""

        if not config_uid:
            class_name = getattr(self, "__name__", str(self))
            raise RuntimeError(
                f"Cannot create node {class_name}: current config uid is empty. "
                "Run create() inside a server/runtime handler."
            )

        new_node = self(node_id, config_uid)

        if initial_data:
            with new_node._lock:
                if new_node._id in new_node._storage:
                    node_data = new_node._storage[new_node._id]
                    if '_data' not in node_data or not isinstance(node_data.get('_data'), dict):
                        node_data['_data'] = {}

                    saved_state = dict(node_data.get('_data') or {})
                    new_state = dict(saved_state)

                    protected_keys = {'_id', '_class'}
                    for key, value in (initial_data or {}).items():
                        if key not in protected_keys:
                            new_state[key] = value

                    new_state['_id'] = normalize_own_uid(new_node._config_uid, new_node.__class__.__name__, new_node._id)
                    new_state['_class'] = new_node.__class__.__name__

                    new_node._data_cache = dict(new_state)

                    # run accept hook BEFORE persisting (only once per request)
                    run_on_accept_server_once(new_node, saved_state, dict(initial_data or {}))

                    # persist what handler left in cache (it may have modified _data)
                    to_write = new_node._data_cache
                    if isinstance(to_write, dict):
                        audited = _apply_node_audit(dict(to_write), saved_state)
                        new_node._data_cache = audited
                        node_data['_data'] = audited
                    else:
                        node_data['_data'] = _apply_node_audit(dict(new_state), saved_state)
                    node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                    new_node._storage[new_node._id] = node_data

                    # run post-save hook AFTER persisting (only once per request)
                    run_on_after_accept_server_once(new_node, saved_state)
                    new_node._data_cache = None

        return new_node

    def _init_storage(self):
        class_name = self.__class__.__name__
        storage_key = f"{class_name}_{self._config_uid}" if self._config_uid else class_name
        
        if storage_key not in Node._class_storages:
            # Lock for creating a new repository
            if storage_key not in Node._storage_locks:
                Node._storage_locks[storage_key] = threading.RLock()
            
            with Node._storage_locks[storage_key]:
                # Double check after getting blocked
                if storage_key not in Node._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    Node._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        
        self._storage = Node._class_storages[storage_key]
    
    def get_data(self):
        with self._lock:
            if self._id in self._storage:
                data = self._storage[self._id].get('_data', {})
                # Ensure that _id and _class are always present
                #if '_id' not in data:
                #    data['_id'] = self._id
                data['_id'] = normalize_own_uid(self._config_uid, self.__class__.__name__, data.get('_id') or self._id)    
                if '_class' not in data:
                    data['_class'] = self.__class__.__name__
                return data
            return {}
    
    def get_view(self, default=""):
        """Return the class ``record_view`` for this node.

        This is the same platform-level representation used by NodeLink.
        """
        return node_view(self, default=default)

    def record_view(self, default=""):
        """Compatibility alias for :meth:`get_view`."""
        return node_view(self, default=default)

    def set_data(self, key, value):
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                if '_data' not in node_data or not isinstance(node_data.get('_data'), dict):
                    node_data['_data'] = {}
                saved_state = dict(node_data.get('_data') or {})

                base_state = self._data_cache if isinstance(self._data_cache, dict) else saved_state
                new_state = dict(base_state)
                new_state[key] = value
                new_state['_class'] = self.__class__.__name__
                new_state['_id'] = normalize_own_uid(self._config_uid, self.__class__.__name__, self._id)

                self._data_cache = dict(new_state)

                # run accept hook BEFORE persisting (only once per request)
                run_on_accept_server_once(self, saved_state, {key: value})

                # persist what handler left in cache (it may have modified _data)
                to_write = self._data_cache
                if isinstance(to_write, dict):
                    audited = _apply_node_audit(dict(to_write), saved_state)
                    self._data_cache = audited
                    node_data['_data'] = audited
                else:
                    node_data['_data'] = _apply_node_audit(dict(new_state), saved_state)
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data

                # --- update date index (best-effort; never break save) ---
                try:
                    old_date = None
                    if isinstance(saved_state, dict):
                        old_date = saved_state.get("_date_key") or saved_state.get("_date")

                    new_state = node_data.get("_data") or {}
                    new_date = None
                    if isinstance(new_state, dict):
                        new_date = new_state.get("_date_key") or new_state.get("_date")

                    old_dk = normalize_date_key(old_date)
                    new_dk = normalize_date_key(new_date)

                    # persist normalized key for consistency
                    if new_dk and isinstance(new_state, dict):
                        if new_state.get("_date_key") != new_dk:
                            new_state["_date_key"] = new_dk
                            node_data["_data"] = new_state
                            self._storage[self._id] = node_data

                    idx = self.__class__._get_date_index_storage(self._config_uid)

                    if old_dk and old_dk != new_dk:
                        old_k = self.__class__._date_index_key(old_dk, self._id)
                        try:
                            if old_k in idx:
                                del idx[old_k]
                        except Exception:
                            pass

                    if new_dk:
                        new_k = self.__class__._date_index_key(new_dk, self._id)
                        if old_dk != new_dk or new_k not in idx:
                            idx[new_k] = 1
                except Exception:
                    pass

                try:
                    self.__class__._update_defined_indexes(self._config_uid, self._id, saved_state, node_data.get("_data") or {})
                except Exception:
                    pass

                try:
                    self.__class__._update_global_indexes(
                        normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                        saved_state,
                        node_data.get("_data") or {}
                    )
                except Exception:
                    pass

                # run post-save hook AFTER persisting (only once per request)
                run_on_after_accept_server_once(self, saved_state, {key: value})

    def update_data(self, data_dict):
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                if '_data' not in node_data or not isinstance(node_data.get('_data'), dict):
                    node_data['_data'] = {}
                saved_state = dict(node_data.get('_data') or {})

                base_state = self._data_cache if isinstance(self._data_cache, dict) else saved_state
                new_state = dict(base_state)
                protected_keys = {'_id', '_class'}
                transient_keys = {'_user_modification'}
                for key, value in (data_dict or {}).items():
                    if key not in protected_keys and key not in transient_keys:
                        new_state[key] = value

                new_state['_class'] = self.__class__.__name__
                new_state['_id'] = normalize_own_uid(self._config_uid, self.__class__.__name__, self._id)

                self._data_cache = dict(new_state)

                # run accept hook BEFORE persisting (only once per request)
                run_on_accept_server_once(self, saved_state, dict(data_dict or {}))

                # persist what handler left in cache (it may have modified _data)
                to_write = self._data_cache
                if isinstance(to_write, dict):
                    audited = _apply_node_audit(dict(to_write), saved_state)
                    self._data_cache = audited
                    node_data['_data'] = audited
                else:
                    node_data['_data'] = _apply_node_audit(dict(new_state), saved_state)
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data

                # --- update date index (best-effort; never break save) ---
                try:
                    old_date = None
                    if isinstance(saved_state, dict):
                        old_date = saved_state.get("_date_key") or saved_state.get("_date")

                    new_state = node_data.get("_data") or {}
                    new_date = None
                    if isinstance(new_state, dict):
                        new_date = new_state.get("_date_key") or new_state.get("_date")

                    old_dk = normalize_date_key(old_date)
                    new_dk = normalize_date_key(new_date)

                    # persist normalized key for consistency
                    if new_dk and isinstance(new_state, dict):
                        if new_state.get("_date_key") != new_dk:
                            new_state["_date_key"] = new_dk
                            node_data["_data"] = new_state
                            self._storage[self._id] = node_data

                    idx = self.__class__._get_date_index_storage(self._config_uid)

                    if old_dk and old_dk != new_dk:
                        old_k = self.__class__._date_index_key(old_dk, self._id)
                        try:
                            if old_k in idx:
                                del idx[old_k]
                        except Exception:
                            pass

                    if new_dk:
                        new_k = self.__class__._date_index_key(new_dk, self._id)
                        if old_dk != new_dk or new_k not in idx:
                            idx[new_k] = 1
                except Exception:
                    pass

                try:
                    self.__class__._update_defined_indexes(self._config_uid, self._id, saved_state, node_data.get("_data") or {})
                except Exception:
                    pass

                try:
                    self.__class__._update_global_indexes(
                        normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                        saved_state,
                        node_data.get("_data") or {}
                    )
                except Exception:
                    pass

                # run post-save hook AFTER persisting (only once per request)
                run_on_after_accept_server_once(self, saved_state, dict(data_dict or {}))

    @staticmethod
    def _atomic_sqlite_table(conn):
        """Find the SqliteDict key/value table in a storage file."""
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for row in rows:
            name = str((row or [""])[0] or "")
            if not name:
                continue
            qname = '"' + name.replace('"', '""') + '"'
            try:
                cols = {str(x[1] or "").lower() for x in conn.execute(f"PRAGMA table_info({qname})").fetchall()}
            except Exception:
                continue
            if "key" in cols and "value" in cols:
                return name
        raise RuntimeError("SqliteDict key/value table was not found")

    @staticmethod
    def _atomic_expected_matches(current, expected):
        """Exact expected-value matching used by compare-and-set."""
        mismatches = {}
        for key, wanted in (expected or {}).items():
            actual = current.get(key) if isinstance(current, dict) else None
            if actual != wanted:
                mismatches[str(key)] = {"expected": wanted, "actual": actual}
        return mismatches

    @classmethod
    def _bulk_storage_path(cls, config_uid=None) -> str:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = f"{cls.__name__}_{cfg_uid}" if cfg_uid else cls.__name__
        return os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")

    @classmethod
    def _bulk_ensure_storage(cls, config_uid=None):
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = f"{cls.__name__}_{cfg_uid}" if cfg_uid else cls.__name__
        if storage_key not in cls._class_storages:
            lock = cls._storage_locks.setdefault(storage_key, threading.RLock())
            with lock:
                if storage_key not in cls._class_storages:
                    os.makedirs(STORAGE_BASE_PATH, exist_ok=True)
                    cls._class_storages[storage_key] = SqliteDict(
                        cls._bulk_storage_path(cfg_uid), autocommit=True
                    )
        return cls._class_storages[storage_key]

    @classmethod
    def _bulk_read_records(cls, config_uid, node_ids):
        ids = [str(x) for x in (node_ids or []) if str(x)]
        if not ids:
            return {}
        cls._bulk_ensure_storage(config_uid)
        db_path = cls._bulk_storage_path(config_uid)
        conn = sqlite3.connect(db_path, timeout=60.0)
        try:
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            out = {}
            for start in range(0, len(ids), 800):
                part = ids[start:start + 800]
                marks = ','.join('?' for _ in part)
                for key, raw in conn.execute(
                    f"SELECT key, value FROM {qtable} WHERE key IN ({marks})", part
                ).fetchall():
                    if isinstance(raw, memoryview):
                        raw = raw.tobytes()
                    try:
                        value = pickle.loads(raw)
                    except Exception:
                        continue
                    if isinstance(value, dict):
                        out[str(key)] = value
            return out
        finally:
            conn.close()

    @classmethod
    def _bulk_write_records(cls, config_uid, records):
        if not records:
            return 0
        cls._bulk_ensure_storage(config_uid)
        db_path = cls._bulk_storage_path(config_uid)
        conn = sqlite3.connect(db_path, timeout=120.0, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=120000")
            conn.execute("BEGIN IMMEDIATE")
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            rows = [
                (
                    str(node_id),
                    sqlite3.Binary(pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL)),
                )
                for node_id, record in records.items()
            ]
            conn.executemany(
                f"INSERT OR REPLACE INTO {qtable} (key, value) VALUES (?, ?)", rows
            )
            conn.commit()
            return len(rows)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    @classmethod
    def _bulk_replace_plain_index(cls, index_name, config_uid, buckets):
        store = cls._defined_index_storage(index_name, config_uid)
        db_path = os.path.join(
            STORAGE_BASE_PATH,
            f"{cls.__name__}_{str(config_uid or '').strip()}__idx__{index_name}.sqlite"
            if str(config_uid or '').strip()
            else f"{cls.__name__}__idx__{index_name}.sqlite",
        )
        # Ensure SqliteDict has created its table before opening a direct writer.
        try:
            _ = store.tablename
        except Exception:
            pass
        conn = sqlite3.connect(db_path, timeout=120.0, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=120000")
            conn.execute("BEGIN IMMEDIATE")
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            conn.execute(f"DELETE FROM {qtable}")
            if buckets:
                rows = [
                    (
                        str(key),
                        sqlite3.Binary(pickle.dumps(list(values), protocol=pickle.HIGHEST_PROTOCOL)),
                    )
                    for key, values in buckets.items()
                    if str(key) and values
                ]
                conn.executemany(
                    f"INSERT OR REPLACE INTO {qtable} (key, value) VALUES (?, ?)", rows
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

        idx_def = cls._defined_index_def(index_name, config_uid)
        idx_kind = cls._defined_index_kind(idx_def)
        if idx_kind in {"text_index", "text_index_full"}:
            cls._rebuild_text_search_sidecar(index_name, config_uid)
        elif idx_kind == "hash_index":
            cls._clear_hash_index_delta(index_name, config_uid)

    @classmethod
    def rebuild_plain_indexes_fast(cls, config_uid=None, index_names=None, progress_text="") -> dict:
        """Rebuild non-semantic indexes in linear time.

        Buckets are assembled in memory while the node table is scanned once,
        then every bucket is written once in a single SQLite transaction.  This
        avoids the old read/append/rewrite cycle for low-cardinality indexes.
        """
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        wanted = None
        if index_names:
            raw = index_names if isinstance(index_names, (list, tuple, set)) else [index_names]
            wanted = {str(x or "").strip() for x in raw if str(x or "").strip()}
        definitions = []
        semantic = []
        for idx in cls._get_defined_indexes(cfg_uid) or []:
            if not isinstance(idx, dict):
                continue
            name = str(idx.get("name") or "").strip()
            if not name or (wanted is not None and name not in wanted):
                continue
            if cls._is_semantic_index_kind(cls._defined_index_kind(idx)):
                semantic.append(name)
            else:
                definitions.append(idx)
        result = {
            "ok": True,
            "class": cls.__name__,
            "config_uid": cfg_uid,
            "nodes": 0,
            "indexes": len(definitions),
            "semantic_skipped": semantic,
            "index_nodes": {str(x.get("name") or ""): 0 for x in definitions},
            "errors": [],
        }
        if not definitions:
            return result

        cls._bulk_ensure_storage(cfg_uid)
        db_path = cls._bulk_storage_path(cfg_uid)
        conn = sqlite3.connect(db_path, timeout=120.0)
        buckets = {str(idx.get("name")): {} for idx in definitions}
        try:
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            cursor = conn.execute(f"SELECT key, value FROM {qtable}")
            for row_no, (node_id, raw) in enumerate(cursor, 1):
                if isinstance(raw, memoryview):
                    raw = raw.tobytes()
                try:
                    record = pickle.loads(raw)
                    data = (record or {}).get("_data") or {}
                except Exception:
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                for idx in definitions:
                    name = str(idx.get("name") or "")
                    keys_spec = idx.get("keys") or idx.get("field") or idx.get("key") or idx.get("fields") or ""
                    values = cls._extract_index_values(data, keys_spec)
                    for value in values:
                        if value:
                            buckets[name].setdefault(str(value), []).append(str(node_id))
                            result["index_nodes"][name] += 1
                result["nodes"] = row_no
                if row_no % 5000 == 0 and _background_jobs is not None:
                    try:
                        _background_jobs.update_progress(
                            text=(progress_text or f"Rebuilding {cls.__name__} indexes") + f": {row_no}",
                        )
                        _background_jobs.raise_if_cancelled()
                    except Exception as exc:
                        if exc.__class__.__name__ == "JobCancelled":
                            raise
        finally:
            conn.close()

        for idx in definitions:
            name = str(idx.get("name") or "")
            try:
                cls._bulk_replace_plain_index(name, cfg_uid, buckets.get(name) or {})
            except Exception as exc:
                result["ok"] = False
                result["errors"].append(f"{name}: {exc}")
        return result

    @classmethod
    def rebuild_date_index_fast(cls, config_uid=None) -> dict:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        cls._bulk_ensure_storage(cfg_uid)
        date_store = cls._get_date_index_storage(cfg_uid)
        storage_key = f"{cls.__name__}_{cfg_uid}" if cfg_uid else cls.__name__
        db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}__date_index.sqlite")
        try:
            db_path = str(date_store.filename)
        except Exception:
            pass
        main_conn = sqlite3.connect(cls._bulk_storage_path(cfg_uid), timeout=120.0)
        rows = []
        try:
            table = cls._atomic_sqlite_table(main_conn)
            qtable = '"' + table.replace('"', '""') + '"'
            for node_id, raw in main_conn.execute(f"SELECT key, value FROM {qtable}"):
                if isinstance(raw, memoryview):
                    raw = raw.tobytes()
                try:
                    record = pickle.loads(raw)
                    data = (record or {}).get("_data") or {}
                except Exception:
                    data = {}
                date_key = normalize_date_key((data or {}).get("_date_key") or (data or {}).get("_date"))
                if date_key:
                    rows.append((cls._date_index_key(date_key, str(node_id)), 1))
        finally:
            main_conn.close()
        conn = sqlite3.connect(db_path, timeout=120.0, isolation_level=None)
        try:
            conn.execute("BEGIN IMMEDIATE")
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            conn.execute(f"DELETE FROM {qtable}")
            if rows:
                conn.executemany(
                    f"INSERT OR REPLACE INTO {qtable} (key, value) VALUES (?, ?)",
                    [(k, sqlite3.Binary(pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL))) for k, v in rows],
                )
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "rows": len(rows)}

    @classmethod
    def bulk_upsert(cls, items, config_uid=None, chunk_size=1000, rebuild_indexes=True, run_handlers=True) -> dict:
        """Insert/update many nodes with one SQLite transaction per chunk.

        onAcceptServer is executed against the in-memory candidate before the
        chunk is persisted. onAfterAcceptServer runs after persistence. Derived
        ordinary indexes are rebuilt once, linearly, after the import.
        """
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        values = list(items or [])
        chunk_size = max(50, min(5000, int(chunk_size or 1000)))
        total = len(values)
        written = 0
        started = time.perf_counter()
        after_nodes = []
        for start in range(0, total, chunk_size):
            if _background_jobs is not None:
                _background_jobs.raise_if_cancelled()
            # Guards are only needed inside one save graph. Bulk items are
            # independent, so do not retain 100k keys for the whole job.
            try:
                ACCEPT_GUARD.set(set())
                AFTER_ACCEPT_GUARD.set(set())
            except Exception:
                pass
            chunk = values[start:start + chunk_size]
            ids = []
            normalized = []
            for item in chunk:
                if not isinstance(item, dict):
                    raise ValueError("Bulk item must be an object")
                raw_id = item.get("_id")
                try:
                    _cfg, _cls, internal_id = parse_uid_any(raw_id)
                except Exception:
                    internal_id = None
                node_id = str(internal_id or raw_id or uuid.uuid4()).strip()
                ids.append(node_id)
                normalized.append((node_id, dict(item)))
            existing = cls._bulk_read_records(cfg_uid, ids)
            records = {}
            after_nodes = []
            # onAccept guard is request/job local; unique ids remain independent.
            for node_id, user_data in normalized:
                old_record = existing.get(node_id) or {}
                saved_state = dict((old_record or {}).get("_data") or {})
                new_state = dict(saved_state)
                for key, value in user_data.items():
                    if key not in {"_id", "_class", "_user_modification"}:
                        new_state[key] = value
                new_state["_id"] = normalize_own_uid(cfg_uid, cls.__name__, node_id)
                new_state["_class"] = cls.__name__

                node = object.__new__(cls)
                node._id = node_id
                node._config_uid = cfg_uid
                node._schema_class_name = cls.__name__
                node._storage = cls._bulk_ensure_storage(cfg_uid)
                node._lock = threading.RLock()
                node._data_cache = dict(new_state)
                if run_handlers:
                    run_on_accept_server_once(node, saved_state, user_data)
                candidate = node._data_cache if isinstance(node._data_cache, dict) else new_state
                audited = _apply_node_audit(dict(candidate), saved_state)
                now = datetime.now(timezone.utc).isoformat()
                record = dict(old_record) if isinstance(old_record, dict) else {}
                record.update({
                    "_id": node_id,
                    "_class": cls.__name__,
                    "_config_uid": cfg_uid,
                    "_data": audited,
                    "_created_at": record.get("_created_at") or now,
                    "_updated_at": now,
                })
                records[node_id] = record
                node._data_cache = dict(audited)
                after_nodes.append((node, saved_state, user_data))
            cls._bulk_write_records(cfg_uid, records)
            written += len(records)
            if run_handlers:
                for node, saved_state, user_data in after_nodes:
                    run_on_after_accept_server_once(node, saved_state, user_data)
            try:
                _call_bridge(
                    "update_nodes_rls_index_bulk_global",
                    cfg_uid,
                    cls.__name__,
                    [
                        {"node_id": node_id, "data": (record or {}).get("_data") or {}}
                        for node_id, record in records.items()
                    ],
                )
            except Exception:
                pass
            if _background_jobs is not None:
                _background_jobs.update_progress(
                    current=written,
                    total=total,
                    text=f"{cls.__name__}: written {written} of {total}",
                )

        index_result = None
        date_result = None
        if rebuild_indexes:
            index_result = cls.rebuild_plain_indexes_fast(
                cfg_uid, progress_text=f"{cls.__name__}: rebuilding indexes"
            )
            semantic_names = list((index_result or {}).get("semantic_skipped") or [])
            if semantic_names:
                semantic_result = cls.rebuild_defined_indexes(cfg_uid, index_names=semantic_names)
                index_result["semantic"] = semantic_result
                if not semantic_result.get("ok", True):
                    index_result["ok"] = False
                    index_result.setdefault("errors", []).extend(semantic_result.get("errors") or [])
            try:
                date_result = cls.rebuild_date_index_fast(cfg_uid)
            except Exception:
                date_result = {"ok": False}

        return {
            "ok": True,
            "class": cls.__name__,
            "count": written,
            "seconds": time.perf_counter() - started,
            "indexes": index_result,
            "date_index": date_result,
        }

    @classmethod
    def bulk_delete(cls, node_ids, config_uid=None, rebuild_indexes=True) -> dict:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        ids = [str(x) for x in (node_ids or []) if str(x)]
        if not ids:
            return {"ok": True, "count": 0}
        cls._bulk_ensure_storage(cfg_uid)
        conn = sqlite3.connect(cls._bulk_storage_path(cfg_uid), timeout=120.0, isolation_level=None)
        deleted = 0
        try:
            conn.execute("PRAGMA busy_timeout=120000")
            conn.execute("BEGIN IMMEDIATE")
            table = cls._atomic_sqlite_table(conn)
            qtable = '"' + table.replace('"', '""') + '"'
            for start in range(0, len(ids), 800):
                part = ids[start:start + 800]
                marks = ','.join('?' for _ in part)
                cur = conn.execute(f"DELETE FROM {qtable} WHERE key IN ({marks})", part)
                deleted += max(0, int(cur.rowcount or 0))
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
        for node_id in ids:
            cls._instance_locks.pop(node_id, None)
        if rebuild_indexes:
            index_result = cls.rebuild_plain_indexes_fast(cfg_uid)
            semantic_names = list((index_result or {}).get("semantic_skipped") or [])
            if semantic_names:
                cls.rebuild_defined_indexes(cfg_uid, index_names=semantic_names)
            try:
                cls.rebuild_date_index_fast(cfg_uid)
            except Exception:
                pass
        return {"ok": True, "count": deleted}

    def atomic_update(
        self,
        expected=None,
        values=None,
        increments=None,
        reject_payload=None,
        reject_status_code=409,
        timeout=30.0,
    ):
        """Atomically compare and update this node across Python processes.

        The comparison and write are performed in one SQLite ``BEGIN
        IMMEDIATE`` transaction, so only one worker can successfully claim the
        same state. This is intended for short critical state transitions such
        as taking a WMS task from ``available`` to ``claimed``.

        Parameters:
            expected: exact field values required in the currently persisted
                node data, e.g. ``{"status": "available"}``.
            values: fields to assign when the comparison succeeds.
            increments: numeric fields to increment from the persisted value.
            reject_payload: when supplied, a failed comparison raises
                :class:`AcceptRejected` with this payload.
            reject_status_code: HTTP code attached to that rejection.
            timeout: SQLite busy timeout in seconds.

        The low-level operation deliberately does not run ``onAcceptServer`` or
        ``onAfterAcceptServer``. The calling node method is the business
        boundary and should perform any side effects only after ``ok=True``.
        Defined/date/global indexes are refreshed after the committed write.
        """
        expected = dict(expected or {})
        values = dict(values or {})
        increments = dict(increments or {})

        protected_keys = {"_id", "_class"}
        transient_keys = {"_user_modification"}
        for key in list(values.keys()):
            if key in protected_keys or key in transient_keys:
                values.pop(key, None)
        for key in list(increments.keys()):
            if key in protected_keys or key in transient_keys:
                increments.pop(key, None)

        storage_key = f"{self.__class__.__name__}_{self._config_uid}" if self._config_uid else self.__class__.__name__
        db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        conn = None
        saved_state = None
        new_state = None
        mismatch_result = None
        try:
            conn = sqlite3.connect(db_path, timeout=float(timeout or 30.0), isolation_level=None)
            try:
                conn.execute(f"PRAGMA busy_timeout={max(1, int(float(timeout or 30.0) * 1000))}")
            except Exception:
                pass
            conn.execute("BEGIN IMMEDIATE")

            table_name = self._atomic_sqlite_table(conn)
            qtable = '"' + table_name.replace('"', '""') + '"'
            row = conn.execute(f"SELECT value FROM {qtable} WHERE key = ?", (str(self._id),)).fetchone()
            if row is None:
                mismatch_result = {
                    "ok": False,
                    "matched": False,
                    "reason": "node_not_found",
                    "node_id": normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                }
                conn.rollback()
            else:
                raw_value = row[0]
                if isinstance(raw_value, memoryview):
                    raw_value = raw_value.tobytes()
                record = pickle.loads(raw_value)
                if not isinstance(record, dict):
                    raise RuntimeError("Invalid node record in SqliteDict storage")

                saved_state = dict(record.get("_data") or {})
                mismatches = self._atomic_expected_matches(saved_state, expected)
                if mismatches:
                    mismatch_result = {
                        "ok": False,
                        "matched": False,
                        "reason": "condition_failed",
                        "mismatches": mismatches,
                        "current": saved_state,
                    }
                    conn.rollback()
                else:
                    new_state = dict(saved_state)
                    new_state.update(values)
                    for key, delta in increments.items():
                        current_value = new_state.get(key, 0)
                        if current_value in (None, ""):
                            current_value = 0
                        try:
                            new_state[key] = current_value + delta
                        except Exception as exc:
                            raise ValueError(f"Cannot increment field {key}: {exc}") from exc

                    new_state["_class"] = self.__class__.__name__
                    new_state["_id"] = normalize_own_uid(
                        self._config_uid,
                        self.__class__.__name__,
                        self._id,
                    )
                    new_state = _apply_node_audit(new_state, saved_state)

                    record["_data"] = new_state
                    record["_updated_at"] = datetime.now(timezone.utc).isoformat()
                    encoded = sqlite3.Binary(pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL))
                    cur = conn.execute(
                        f"UPDATE {qtable} SET value = ? WHERE key = ?",
                        (encoded, str(self._id)),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("Atomic node update did not modify exactly one row")
                    conn.commit()
        except AcceptRejected:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        self._data_cache = None

        if mismatch_result is not None:
            if reject_payload is not None:
                payload = dict(reject_payload or {})
                payload.setdefault("reason", mismatch_result.get("reason"))
                if mismatch_result.get("mismatches"):
                    payload.setdefault("mismatches", mismatch_result.get("mismatches"))
                raise AcceptRejected(payload, status_code=reject_status_code)
            return mismatch_result

        # Index refresh is intentionally after commit: index files are separate
        # SQLite databases and must not be touched while the node DB write lock
        # is held.
        try:
            old_date = saved_state.get("_date_key") or saved_state.get("_date")
            new_date = new_state.get("_date_key") or new_state.get("_date")
            old_dk = normalize_date_key(old_date)
            new_dk = normalize_date_key(new_date)
            idx = self.__class__._get_date_index_storage(self._config_uid)
            if old_dk and old_dk != new_dk:
                old_k = self.__class__._date_index_key(old_dk, self._id)
                if old_k in idx:
                    del idx[old_k]
            if new_dk:
                new_k = self.__class__._date_index_key(new_dk, self._id)
                if old_dk != new_dk or new_k not in idx:
                    idx[new_k] = 1
        except Exception:
            pass

        try:
            self.__class__._update_defined_indexes(
                self._config_uid,
                self._id,
                saved_state or {},
                new_state or {},
            )
        except Exception:
            pass

        try:
            self.__class__._update_global_indexes(
                normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                saved_state or {},
                new_state or {},
            )
        except Exception:
            pass

        return {
            "ok": True,
            "matched": True,
            "before": saved_state or {},
            "after": new_state or {},
        }

    def compare_and_set(self, expected=None, values=None, increments=None, **kwargs):
        """Readable alias for :meth:`atomic_update`."""
        return self.atomic_update(
            expected=expected,
            values=values,
            increments=increments,
            **kwargs,
        )

    def delete(self):
        """Recursively delete a node and all its descendants"""
        with self._lock:
            # Сначала получаем всех детей (поддерживаем оба формата)
            children_nodes = self.GetChildren()
            
            # Рекурсивно удаляем всех потомков
            for child in children_nodes:
                child.delete()
            
            # Затем удаляем узел сам
            if self._id in self._storage:
                try:
                    old_state = dict((self._storage.get(self._id) or {}).get('_data') or {})
                except Exception:
                    old_state = {}
                try:
                    self.__class__._update_defined_indexes(self._config_uid, self._id, old_state, {})
                except Exception:
                    pass
                try:
                    self.__class__._update_global_indexes(
                        normalize_own_uid(self._config_uid, self.__class__.__name__, self._id),
                        old_state,
                        {}
                    )
                except Exception:
                    pass
                del self._storage[self._id]
                if self._id in Node._instance_locks:
                    del Node._instance_locks[self._id]
            
            # Удаляем связь с родителем, если она есть
            parent_uid = self._data.get("_parent")  # "cfg$ParentClass$42"
            if parent_uid:
                try:
                    cfg_uid, parent_class, parent_id = parent_uid.split("$", 2)
                    parent_cls = self._resolve_node_class(parent_class)
            
                    parent_node = parent_cls.get(parent_uid, cfg_uid)  # или get(parent_id, cfg_uid) — как у вас принято
                    parent_node.RemoveChild(self._data.get("_id") or self._id)
                except Exception as e:
                    print(f"Error removing from parent: {e}")

                
    
    @classmethod
    def get(cls, node_id, config_uid=None):
        # ✅ accept composite ids: "cfg$Class$Id" | "Class$Id" | "Id"
        uid_cfg, uid_cls, internal_id = parse_uid_any(node_id)
        effective_config_uid = config_uid or uid_cfg

        # If uid contains another class name and we can resolve it -> delegate
        # (useful when someone calls Node.get("cfg$Warehouse$123") or wrong class)
        try:
            if uid_cls and uid_cls != cls.__name__:
                parsed = CURRENT_PARSED_CONFIG.get()
                if isinstance(parsed, dict):
                    real_cls = _resolve_node_class(parsed, uid_cls)
                    if real_cls and isinstance(real_cls, type) and issubclass(real_cls, Node):
                        return real_cls.get(internal_id, effective_config_uid)
        except Exception:
            pass

        # For normal calls (called on correct class), just use internal id
        storage_key = f"{cls.__name__}_{effective_config_uid}" if effective_config_uid else cls.__name__

        # Make sure the storage is initialized
        if storage_key not in cls._class_storages:
            if storage_key not in cls._storage_locks:
                cls._storage_locks[storage_key] = threading.RLock()

            with cls._storage_locks[storage_key]:
                if storage_key not in cls._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    if not os.path.exists(db_path):
                        return None
                    try:
                        cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
                    except Exception:
                        return None

        storage = cls._class_storages[storage_key]

        # ✅ lookup by internal id (preferred), but be tolerant to legacy keys
        candidates = []
        if internal_id is not None:
            candidates.append(str(internal_id))
        # legacy: full raw key may have been used as internal id
        try:
            raw_s = str(node_id)
            candidates.append(raw_s)
            # also try normalized "cfg$Class$singleton" and "cfg$Class"
            if effective_config_uid and uid_cls:
                candidates.append(f"{effective_config_uid}${uid_cls}$singleton")
                candidates.append(f"{effective_config_uid}${uid_cls}")
        except Exception:
            pass

        for cand in candidates:
            if cand in storage:
                return cls(cand, effective_config_uid)
        return None

    @classmethod
    def get_many_data(cls, node_ids, config_uid=None):
        """Read many nodes as plain data dictionaries without constructing Node objects.

        This is intended for reports and planning algorithms that need to enrich a
        large QuantLedger slice.  ``Node.get()`` performs several storage lookups
        per object (existence check, constructor check, then data read); doing that
        for every report row creates a classic N+1 bottleneck.  This method performs
        one storage read per *unique* id and never writes or runs handlers.

        The result contains entries under the exact supplied reference and, when
        unambiguous, under its internal id as well. Missing nodes are omitted.
        """
        values = list(node_ids or [])
        if not values:
            return {}

        default_config_uid = config_uid
        if not default_config_uid:
            default_config_uid = current_config_uid_from_handlers()
        if not default_config_uid:
            try:
                default_config_uid = CURRENT_CONFIG_UID.get()
            except Exception:
                default_config_uid = None

        grouped = {}
        requested = []
        for raw_value in values:
            raw = str(raw_value or "").strip()
            if not raw:
                continue
            try:
                uid_cfg, uid_cls, internal_id = parse_uid_any(raw)
            except Exception:
                uid_cfg, uid_cls, internal_id = None, None, raw
            if uid_cls and str(uid_cls) != str(cls.__name__):
                continue
            effective_uid = str(config_uid or uid_cfg or default_config_uid or "").strip()
            internal = str(internal_id or raw).strip()
            if not internal:
                continue
            grouped.setdefault(effective_uid, {})[internal] = raw
            requested.append((raw, effective_uid, internal))

        loaded = {}
        by_internal = {}
        for effective_uid, ids in grouped.items():
            storage_key = f"{cls.__name__}_{effective_uid}" if effective_uid else cls.__name__
            if storage_key not in cls._class_storages:
                if storage_key not in cls._storage_locks:
                    cls._storage_locks[storage_key] = threading.RLock()
                with cls._storage_locks[storage_key]:
                    if storage_key not in cls._class_storages:
                        db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                        if not os.path.exists(db_path):
                            continue
                        try:
                            cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
                        except Exception:
                            continue
            storage = cls._class_storages.get(storage_key)
            if storage is None:
                continue

            for internal, original_raw in ids.items():
                raw_record = None
                try:
                    raw_record = storage.get(internal)
                except Exception:
                    raw_record = None
                if raw_record is None and original_raw != internal:
                    try:
                        raw_record = storage.get(original_raw)
                    except Exception:
                        raw_record = None
                if not isinstance(raw_record, dict):
                    continue
                raw_data = raw_record.get("_data")
                if not isinstance(raw_data, dict):
                    continue
                data = dict(raw_data)
                data.setdefault("_class", cls.__name__)
                data["_id"] = normalize_own_uid(
                    effective_uid,
                    cls.__name__,
                    data.get("_id") or internal,
                )
                loaded[(effective_uid, internal)] = data
                by_internal.setdefault(internal, data)

        result = {}
        for raw, effective_uid, internal in requested:
            data = loaded.get((effective_uid, internal))
            if data is None:
                continue
            result[raw] = data
            result.setdefault(internal, data)
            full_uid = normalize_own_uid(effective_uid, cls.__name__, internal)
            result.setdefault(full_uid, data)
        return result
    
    @classmethod
    def get_all_data(cls, config_uid=None, limit=None):
        """Read a class as plain dictionaries without constructing Node objects.

        Use this for reports that genuinely need a complete small/medium class
        scan and cannot narrow the candidate set with an index. Unlike
        ``get_all()``, this method does not instantiate handlers, run events or
        populate Node caches. For large classes prefer ``find_ids_by_index()``
        followed by ``get_many_data()``.
        """
        if not config_uid:
            config_uid = current_config_uid_from_handlers()
        if not config_uid:
            try:
                config_uid = CURRENT_CONFIG_UID.get()
            except Exception:
                config_uid = None

        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        if storage_key not in cls._class_storages:
            if storage_key not in cls._storage_locks:
                cls._storage_locks[storage_key] = threading.RLock()
            with cls._storage_locks[storage_key]:
                if storage_key not in cls._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    if not os.path.exists(db_path):
                        return {}
                    cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)

        storage = cls._class_storages[storage_key]
        max_rows = None if limit is None else max(0, int(limit))
        result = {}
        count = 0
        for internal_id, raw_record in storage.items():
            if max_rows is not None and count >= max_rows:
                break
            if not isinstance(raw_record, dict):
                continue
            raw_data = raw_record.get("_data")
            if not isinstance(raw_data, dict):
                continue
            data = dict(raw_data)
            data.setdefault("_class", cls.__name__)
            full_uid = normalize_own_uid(
                config_uid,
                cls.__name__,
                data.get("_id") or internal_id,
            )
            data["_id"] = full_uid
            result[full_uid] = data
            count += 1
        return result

    @classmethod
    def get_all(cls, config_uid=None):
        if not config_uid:
            config_uid = current_config_uid_from_handlers()
        if not config_uid:
            try:
                config_uid = CURRENT_CONFIG_UID.get()
            except Exception:
                config_uid = None
        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        
        if storage_key not in cls._class_storages:
            # Lock for loading storage
            if storage_key not in cls._storage_locks:
                cls._storage_locks[storage_key] = threading.RLock()
            
            with cls._storage_locks[storage_key]:
                # Double check after getting blocked
                if storage_key not in cls._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    if not os.path.exists(db_path):
                        return {}
                    cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        
        storage = cls._class_storages[storage_key]
        return {node_id: cls(node_id, config_uid) for node_id in storage.keys()}

    @staticmethod
    def _normalize_global_index_name(index_name: str) -> str:
        name = str(index_name or "").strip()
        if name.startswith("__"):
            name = name[2:]
        return name.strip()

    @classmethod
    def _global_index_storage(cls, index_name: str):
        name = cls._normalize_global_index_name(index_name)
        if not name:
            return None
        storage_key = f"__global_idx__{name}"
        if storage_key not in cls._global_index_storages:
            if storage_key not in cls._global_index_locks:
                cls._global_index_locks[storage_key] = threading.RLock()
            with cls._global_index_locks[storage_key]:
                if storage_key not in cls._global_index_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    cls._global_index_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        return cls._global_index_storages[storage_key]

    @classmethod
    def _extract_global_index_values(cls, data: dict) -> dict[str, list[str]]:
        out = {}
        if not isinstance(data, dict):
            return out
        for raw_key, raw_val in data.items():
            key = str(raw_key or "").strip()
            if not key.startswith("__"):
                continue
            name = cls._normalize_global_index_name(key)
            if not name:
                continue
            vals = []
            if isinstance(raw_val, dict):
                vv = raw_val.get("_id") or raw_val.get("id") or raw_val.get("uid") or ""
                if vv:
                    vals.append(str(vv))
            elif isinstance(raw_val, (list, tuple, set)):
                for item in raw_val:
                    if isinstance(item, dict):
                        vv = item.get("_id") or item.get("id") or item.get("uid") or ""
                    else:
                        vv = item
                    vv = str(vv or "").strip()
                    if vv:
                        vals.append(vv)
            elif raw_val is not None:
                vv = str(raw_val).strip()
                if vv:
                    vals.append(vv)
            if vals:
                out[name] = vals
        return out

    @classmethod
    def _update_global_indexes(cls, node_uid: str, old_state, new_state):
        old_map = cls._extract_global_index_values(old_state or {})
        new_map = cls._extract_global_index_values(new_state or {})
        names = sorted(set(old_map.keys()) | set(new_map.keys()))
        for name in names:
            store = cls._global_index_storage(name)
            if store is None:
                continue
            old_vals = list(old_map.get(name, []) or [])
            new_vals = list(new_map.get(name, []) or [])
            for val in old_vals:
                if val not in new_vals:
                    try:
                        bucket = list(store.get(str(val), []) or [])
                        bucket = [x for x in bucket if str(x) != str(node_uid)]
                        if bucket:
                            store[str(val)] = bucket
                        elif str(val) in store:
                            del store[str(val)]
                    except Exception:
                        pass
            for val in new_vals:
                try:
                    bucket = list(store.get(str(val), []) or [])
                    sid = str(node_uid)
                    if sid not in bucket:
                        bucket.append(sid)
                    store[str(val)] = bucket
                except Exception:
                    pass

    @staticmethod
    def _normalize_defined_index_def(idx_def):
        """Normalize current and legacy class-index JSON shapes.

        Current configurations use ``kind`` + ``keys``. Older/exported client
        configurations may still use ``type`` + ``field`` (or ``fields``).
        Index maintenance must understand both shapes; otherwise the first
        modern index (often barcode) is updated while a following semantic
        index silently receives no source value.
        """
        if not isinstance(idx_def, dict):
            return None

        item = dict(idx_def)
        name = str(item.get("name") or item.get("index") or item.get("id") or "").strip()
        if not name:
            return None

        kind = str(item.get("kind") or item.get("type") or "hash_index").strip().lower() or "hash_index"
        keys = item.get("keys")
        if keys in (None, "", []):
            keys = item.get("field")
        if keys in (None, "", []):
            keys = item.get("key")
        if keys in (None, "", []):
            keys = item.get("fields")
        if isinstance(keys, (list, tuple, set)):
            keys = "|".join(str(x or "").strip() for x in keys if str(x or "").strip())
        else:
            keys = str(keys or "").strip()

        item["name"] = name
        item["kind"] = kind
        item["keys"] = keys
        # Keep compatibility fields coherent for code that still reads them.
        item.setdefault("type", kind)
        if keys and not item.get("field") and "|" not in keys:
            item["field"] = keys
        return item

    @classmethod
    def _normalize_defined_indexes(cls, raw_indexes):
        if isinstance(raw_indexes, str):
            try:
                raw_indexes = json.loads(raw_indexes)
            except Exception:
                raw_indexes = []
        if not isinstance(raw_indexes, list):
            return []
        out = []
        seen = set()
        for raw in raw_indexes:
            item = cls._normalize_defined_index_def(raw)
            if not item:
                continue
            name = item["name"]
            if name in seen:
                # Last declaration wins, but retain its original position.
                for pos, old_item in enumerate(out):
                    if old_item.get("name") == name:
                        out[pos] = item
                        break
                continue
            seen.add(name)
            out.append(item)
        return out

    @classmethod
    def _get_defined_indexes(cls, config_uid=None):
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        if not cfg_uid:
            return []

        # During web-client/runtime requests CURRENT_PARSED_CONFIG is the actual
        # installed repository configuration. It may be newer than the Designer
        # DB or may not exist in that DB at all (remote/public repository).
        try:
            parsed = CURRENT_PARSED_CONFIG.get()
            classes = parsed.get("classes") if isinstance(parsed, dict) else None
            if isinstance(classes, dict) and cls.__name__ in classes:
                class_cfg = classes.get(cls.__name__) or {}
                if isinstance(class_cfg, dict):
                    if "indexes" in class_cfg or "indexes_json" in class_cfg or "indexesJson" in class_cfg:
                        raw = class_cfg.get("indexes")
                        if raw is None:
                            raw = class_cfg.get("indexes_json")
                        if raw is None:
                            raw = class_cfg.get("indexesJson")
                        return cls._normalize_defined_indexes(raw)
        except Exception:
            pass

        # Designer/editor fallback. Merge duplicate historical ConfigClass rows
        # by index name instead of taking an arbitrary .first() row.
        try:
            Configuration = ConfigClass = db = None
            for module_name in ("__main__", "app", "main", "application", "wsgi"):
                mod = sys.modules.get(module_name)
                if mod is None:
                    continue
                Configuration = Configuration or getattr(mod, "Configuration", None)
                ConfigClass = ConfigClass or getattr(mod, "ConfigClass", None)
                db = db or getattr(mod, "db", None)
            if Configuration is None or ConfigClass is None or db is None:
                return []
            cfg = db.session.query(Configuration).filter(Configuration.uid == cfg_uid).first()
            if not cfg:
                return []
            rows = (
                db.session.query(ConfigClass)
                .filter(ConfigClass.config_id == cfg.id, ConfigClass.name == cls.__name__)
                .order_by(ConfigClass.id.asc())
                .all()
            )
            merged = []
            positions = {}
            for row in rows:
                for item in cls._normalize_defined_indexes(getattr(row, "indexes_json", None) or []):
                    name = item.get("name")
                    if name in positions:
                        merged[positions[name]] = item
                    else:
                        positions[name] = len(merged)
                        merged.append(item)
            return merged
        except Exception:
            return []

    @classmethod
    def _defined_index_storage(cls, index_name: str, config_uid=None):
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = f"{cls.__name__}_{cfg_uid}__idx__{index_name}" if cfg_uid else f"{cls.__name__}__idx__{index_name}"
        if storage_key not in cls._defined_index_storages:
            if storage_key not in cls._defined_index_locks:
                cls._defined_index_locks[storage_key] = threading.RLock()
            with cls._defined_index_locks[storage_key]:
                if storage_key not in cls._defined_index_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    cls._defined_index_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        return cls._defined_index_storages[storage_key]

    @classmethod
    def _text_search_db_path(cls, index_name: str, config_uid=None) -> str:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = f"{cls.__name__}_{cfg_uid}__idx__{index_name}" if cfg_uid else f"{cls.__name__}__idx__{index_name}"
        return os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")

    @staticmethod
    def _text_search_table_names():
        return "noda_text_keys", "noda_text_fts", "noda_text_meta"

    @classmethod
    def _ensure_text_search_sidecar(
        cls, conn, source_table: str, force_rebuild: bool = False, allow_rebuild: bool = True
    ) -> bool:
        """Prepare a Unicode substring search sidecar for text indexes.

        The old limited Section search called a Python ``casefold`` UDF for
        every one of 100k index keys.  That preserves Unicode correctness but
        crosses the Python/SQLite boundary 100k times per key press.  The
        sidecar stores casefolded keys once and, when SQLite supports it, uses
        FTS5's trigram tokenizer for indexed substring lookup.
        """
        key_table, fts_table, meta_table = cls._text_search_table_names()
        qsource = '"' + str(source_table).replace('"', '""') + '"'
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{key_table}" ('
            'key TEXT PRIMARY KEY, norm_key TEXT NOT NULL)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{key_table}_norm" ON "{key_table}" (norm_key)'
        )
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{meta_table}" ('
            'id INTEGER PRIMARY KEY CHECK(id=1), source_rows INTEGER NOT NULL DEFAULT 0, version INTEGER NOT NULL DEFAULT 1)'
        )

        fts_available = True
        try:
            conn.execute(
                f'CREATE VIRTUAL TABLE IF NOT EXISTS "{fts_table}" '
                "USING fts5(key UNINDEXED, norm_key, tokenize='trigram')"
            )
        except Exception:
            fts_available = False

        # Search requests and normal incremental writes only need the schema.
        # Rebuilding 100k normalized keys here made one stale async update turn
        # the next Section search into a multi-second foreground operation.
        if not allow_rebuild and not force_rebuild:
            return fts_available

        source_rows = int(conn.execute(f"SELECT COUNT(*) FROM {qsource}").fetchone()[0] or 0)
        side_rows = int(conn.execute(f'SELECT COUNT(*) FROM "{key_table}"').fetchone()[0] or 0)
        meta = conn.execute(
            f'SELECT source_rows, version FROM "{meta_table}" WHERE id=1'
        ).fetchone()
        ready = bool(meta and int(meta[0] or 0) == source_rows and int(meta[1] or 0) == 1 and side_rows == source_rows)
        if force_rebuild or not ready:
            conn.execute(f'DELETE FROM "{key_table}"')
            rows = []
            for (raw_key,) in conn.execute(f"SELECT CAST(key AS TEXT) FROM {qsource}"):
                key = str(raw_key or "")
                if key:
                    rows.append((key, key.casefold()))
                    if len(rows) >= 5000:
                        conn.executemany(
                            f'INSERT OR REPLACE INTO "{key_table}" (key, norm_key) VALUES (?, ?)',
                            rows,
                        )
                        rows = []
            if rows:
                conn.executemany(
                    f'INSERT OR REPLACE INTO "{key_table}" (key, norm_key) VALUES (?, ?)',
                    rows,
                )
            if fts_available:
                conn.execute(f'DELETE FROM "{fts_table}"')
                conn.execute(
                    f'INSERT INTO "{fts_table}" (key, norm_key) '
                    f'SELECT key, norm_key FROM "{key_table}"'
                )
            conn.execute(
                f'INSERT OR REPLACE INTO "{meta_table}" (id, source_rows, version) VALUES (1, ?, 1)',
                (source_rows,),
            )
        elif fts_available:
            # An older sidecar may have the normalized table but no populated
            # FTS table.  Building it once is still far cheaper than scanning
            # through Python on every search.
            fts_rows = int(conn.execute(f'SELECT COUNT(*) FROM "{fts_table}"').fetchone()[0] or 0)
            if fts_rows != source_rows:
                conn.execute(f'DELETE FROM "{fts_table}"')
                conn.execute(
                    f'INSERT INTO "{fts_table}" (key, norm_key) '
                    f'SELECT key, norm_key FROM "{key_table}"'
                )
        return fts_available

    @classmethod
    def _queue_text_search_sidecar_rebuild(cls, index_name: str, config_uid=None) -> None:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        token = (cls.__name__, cfg_uid, str(index_name or "").strip())
        with Node._text_sidecar_rebuild_pending_lock:
            if token in Node._text_sidecar_rebuild_pending:
                return
            Node._text_sidecar_rebuild_pending.add(token)

        def work():
            try:
                cls._rebuild_text_search_sidecar(index_name, cfg_uid)
            except Exception as exc:
                print("text search sidecar rebuild error", token, repr(exc))
            finally:
                with Node._text_sidecar_rebuild_pending_lock:
                    Node._text_sidecar_rebuild_pending.discard(token)

        cls._get_async_index_executor().submit(work)

    @classmethod
    def _sync_text_search_sidecar(cls, index_name: str, config_uid, removed, added) -> None:
        db_path = cls._text_search_db_path(index_name, config_uid)
        if not os.path.exists(db_path):
            return
        conn = sqlite3.connect(db_path, timeout=5.0, isolation_level=None)
        needs_rebuild = False
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            conn.execute("BEGIN IMMEDIATE")
            source_table = cls._atomic_sqlite_table(conn)
            qsource = '"' + source_table.replace('"', '""') + '"'
            fts_available = cls._ensure_text_search_sidecar(
                conn, source_table, allow_rebuild=False
            )
            key_table, fts_table, meta_table = cls._text_search_table_names()
            meta = conn.execute(
                f'SELECT source_rows, version FROM "{meta_table}" WHERE id=1'
            ).fetchone()

            # An old installation can have the source index but no normalized
            # sidecar yet. Do not rebuild it inside a save/index worker while it
            # owns this transaction; enqueue one ordered full build instead.
            if not meta:
                source_has_rows = conn.execute(
                    f"SELECT 1 FROM {qsource} LIMIT 1"
                ).fetchone() is not None
                side_has_rows = conn.execute(
                    f'SELECT 1 FROM "{key_table}" LIMIT 1'
                ).fetchone() is not None
                if source_has_rows and not side_has_rows:
                    needs_rebuild = True
                    conn.commit()
                    return

            for raw in removed or ():
                key = str(raw or "")
                if not key:
                    continue
                still_exists = conn.execute(
                    f"SELECT 1 FROM {qsource} WHERE key=? LIMIT 1", (key,)
                ).fetchone()
                if still_exists:
                    continue
                conn.execute(f'DELETE FROM "{key_table}" WHERE key=?', (key,))
                if fts_available:
                    # Deletion is uncommon. Keep compatibility with the existing
                    # FTS layout; additions below avoid this full FTS key scan.
                    conn.execute(f'DELETE FROM "{fts_table}" WHERE key=?', (key,))

            for raw in added or ():
                key = str(raw or "")
                if not key:
                    continue
                already_present = conn.execute(
                    f'SELECT 1 FROM "{key_table}" WHERE key=? LIMIT 1', (key,)
                ).fetchone()
                if already_present:
                    # The bucket changed, but the searchable text did not.
                    continue
                folded = key.casefold()
                conn.execute(
                    f'INSERT OR REPLACE INTO "{key_table}" (key, norm_key) VALUES (?, ?)',
                    (key, folded),
                )
                if fts_available:
                    conn.execute(
                        f'INSERT INTO "{fts_table}" (key, norm_key) VALUES (?, ?)',
                        (key, folded),
                    )

            # Meta is advisory now; foreground search deliberately does not
            # compare full-table counts. Keep it consistent at background cost.
            side_rows = int(
                conn.execute(f'SELECT COUNT(*) FROM "{key_table}"').fetchone()[0] or 0
            )
            conn.execute(
                f'INSERT OR REPLACE INTO "{meta_table}" (id, source_rows, version) VALUES (1, ?, 1)',
                (side_rows,),
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            # Search has a compatibility fallback, so a sidecar failure must
            # never make the business save fail. Rebuild later, once.
            needs_rebuild = True
        finally:
            conn.close()
            if needs_rebuild:
                cls._queue_text_search_sidecar_rebuild(index_name, config_uid)

    @classmethod
    def _rebuild_text_search_sidecar(cls, index_name: str, config_uid=None) -> None:
        db_path = cls._text_search_db_path(index_name, config_uid)
        conn = sqlite3.connect(db_path, timeout=120.0, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=120000")
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            conn.execute("BEGIN IMMEDIATE")
            source_table = cls._atomic_sqlite_table(conn)
            cls._ensure_text_search_sidecar(conn, source_table, force_rebuild=True)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    @classmethod
    def _search_text_sidecar(cls, index_name: str, config_uid, needles, bidirectional: bool, result_limit: int):
        db_path = cls._text_search_db_path(index_name, config_uid)
        conn = sqlite3.connect(db_path, timeout=1.5, isolation_level=None)
        keys = []
        seen = set()
        needs_rebuild = False
        try:
            conn.execute("PRAGMA busy_timeout=1500")
            conn.execute("BEGIN")
            source_table = cls._atomic_sqlite_table(conn)
            fts_available = cls._ensure_text_search_sidecar(
                conn, source_table, allow_rebuild=False
            )
            key_table, fts_table, meta_table = cls._text_search_table_names()
            meta = conn.execute(
                f'SELECT source_rows, version FROM "{meta_table}" WHERE id=1'
            ).fetchone()
            if not meta:
                needs_rebuild = True
                return None
            cap = max(20, int(result_limit or 20) * 4)

            for _original, normalized in needles:
                folded = str(normalized or "").casefold()
                if not folded:
                    continue
                rows = []
                if fts_available and len(folded) >= 3:
                    # A quoted trigram query keeps the original substring
                    # semantics, including spaces and punctuation.
                    phrase = '"' + folded.replace('"', '""') + '"'
                    try:
                        rows = conn.execute(
                            f'SELECT key FROM "{fts_table}" WHERE norm_key MATCH ? LIMIT ?',
                            (phrase, cap),
                        ).fetchall()
                    except Exception:
                        rows = []
                if not rows:
                    sql = f'SELECT key FROM "{key_table}" WHERE INSTR(norm_key, ?) > 0'
                    params = [folded]
                    if bidirectional:
                        sql += ' OR INSTR(?, norm_key) > 0'
                        params.append(folded)
                    sql += ' LIMIT ?'
                    params.append(cap)
                    rows = conn.execute(sql, params).fetchall()
                elif bidirectional and len(keys) < cap:
                    # text_index_full also accepts a stored key contained in
                    # the query.  Keep that legacy direction as a limited C
                    # scan over pre-normalized text.
                    extra = conn.execute(
                        f'SELECT key FROM "{key_table}" WHERE INSTR(?, norm_key) > 0 LIMIT ?',
                        (folded, cap),
                    ).fetchall()
                    rows.extend(extra)

                for row in rows:
                    key = str((row or [""])[0] or "")
                    if key and key not in seen:
                        seen.add(key)
                        keys.append(key)
                        if len(keys) >= cap:
                            break
                if len(keys) >= cap:
                    break
            conn.commit()
            return keys
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            conn.close()
            if needs_rebuild:
                cls._queue_text_search_sidecar_rebuild(index_name, config_uid)

    @staticmethod
    def _extract_index_values(data: dict, keys_spec: str) -> list[str]:
        if not isinstance(data, dict):
            return []
        raw_keys = [str(x or "").strip() for x in str(keys_spec or "").split("|") if str(x or "").strip()]
        if not raw_keys:
            return []
        parts = []
        for rk in raw_keys:
            v = data.get(rk)
            if isinstance(v, dict):
                v = v.get("_id") or v.get("id") or v.get("uid") or ""
            elif isinstance(v, (list, tuple, set)):
                vals = []
                for item in v:
                    if isinstance(item, dict):
                        vals.append(str(item.get("_id") or item.get("id") or item.get("uid") or ""))
                    else:
                        vals.append(str(item or ""))
                v = "|".join([x for x in vals if x])
            elif v is None:
                v = ""
            else:
                v = str(v)
            parts.append(v.strip())
        if not any(parts):
            return []
        return ["|".join(parts)]

    @staticmethod
    def _is_semantic_index_kind(kind: str) -> bool:
        kind = str(kind or "").strip().lower()
        return kind in {"semantic", "semantic_index", "semanic_index"}

    @staticmethod
    def _semantic_default_model() -> str:
        return "intfloat/multilingual-e5-small"

    @staticmethod
    def _semantic_is_multilingual_e5_small(model_name: str) -> bool:
        """Return True only for the model with query/passage input contract."""
        value = str(model_name or "").strip().lower().rstrip("/")
        return value == "intfloat/multilingual-e5-small"

    @classmethod
    def _semantic_prepare_model_text(cls, text_value: str, model_name: str, role: str) -> str:
        """Prepare model input without changing the stored/displayed text.

        intfloat/multilingual-e5-small was trained for asymmetric retrieval with
        ``query: `` on search requests and ``passage: `` on indexed documents.
        No other model is rewritten here. Existing prefixes are replaced rather
        than duplicated so a caller may safely pass either raw or prefixed text.
        """
        text = str(text_value or "").strip()
        if not text or not cls._semantic_is_multilingual_e5_small(model_name):
            return text

        lowered = text.lower()
        for known_prefix in ("query:", "passage:"):
            if lowered.startswith(known_prefix):
                text = text[len(known_prefix):].lstrip()
                break

        prefix = "query: " if str(role or "").strip().lower() == "query" else "passage: "
        return prefix + text

    @classmethod
    def _semantic_default_threshold(cls, model_name: str) -> float:
        # E5 cosine scores are intentionally concentrated in a high range, so
        # 0.5 is not a useful default for this model. This is only a visible
        # default: an explicitly configured threshold is always respected.
        if cls._semantic_is_multilingual_e5_small(model_name):
            return 0.8
        return 0.5

    @staticmethod
    def _semantic_index_model(idx_def) -> str:
        if isinstance(idx_def, dict):
            value = idx_def.get("model") or idx_def.get("model_name") or idx_def.get("embedding_model")
            value = str(value or "").strip()
            if value:
                return value
        return Node._semantic_default_model()

    @staticmethod
    def _semantic_parse_float01(raw, default: float = 0.5) -> float:
        if raw is None:
            raw = default
        if isinstance(raw, str):
            raw = raw.strip().replace(",", ".")
        try:
            value = float(raw)
        except Exception:
            value = default
        return max(0.0, min(1.0, value))

    @classmethod
    def _semantic_index_threshold(cls, idx_def) -> float:
        model_name = cls._semantic_index_model(idx_def)
        default = cls._semantic_default_threshold(model_name)
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get("threshold", idx_def.get("min_score", idx_def.get("min_similarity", default)))
        return cls._semantic_parse_float01(raw, default)

    @classmethod
    def _semantic_index_limit(cls, idx_def) -> int:
        # Retrieval models return a ranked top-k list.  multilingual-e5-small
        # keeps many unrelated short product names above a useful cosine
        # threshold, so returning all threshold matches (the old default was
        # 50) hides the meaningful head of the ranking.  Match the normal E5
        # retrieval/test setup and return the best 10 unless the index explicitly
        # configures another limit.
        model_name = cls._semantic_index_model(idx_def)
        default = 10 if cls._semantic_is_multilingual_e5_small(model_name) else 50
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get("limit", idx_def.get("candidate_limit", idx_def.get("max_candidates", default)))
        try:
            value = int(raw if raw is not None else default)
        except Exception:
            value = default
        return max(1, min(value, 1000))

    @classmethod
    def _semantic_index_embedding_weight(cls, idx_def) -> float:
        model_name = cls._semantic_index_model(idx_def)
        default = 1.0 if cls._semantic_is_multilingual_e5_small(model_name) else 0.5
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get("embedding_weight", idx_def.get("semantic_weight", idx_def.get("vector_weight", default)))
        return cls._semantic_parse_float01(raw, default)

    @classmethod
    def _semantic_index_token_weight(cls, idx_def) -> float:
        model_name = cls._semantic_index_model(idx_def)
        default = 0.0 if cls._semantic_is_multilingual_e5_small(model_name) else 0.5
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get(
                "technical_token_weight",
                idx_def.get("token_weight", idx_def.get("technical_weight", default)),
            )
        return cls._semantic_parse_float01(raw, default)

    @staticmethod
    def _semantic_index_scan_all_limit(idx_def) -> int:
        """Explicit full-rescoring limit for semantic_index."""
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get("scan_all_limit", idx_def.get("fallback_scan_limit", None))
        if raw is None:
            raw = os.environ.get("NODE_SEMANTIC_SCAN_ALL_LIMIT") or 0
        try:
            value = int(str(raw).strip())
        except Exception:
            value = 0
        return max(0, min(value, 200000))

    @staticmethod
    def _semantic_index_exact_scan_max_items(idx_def) -> int:
        """Use exact full rescoring automatically for small semantic indexes.

        Approximate LSH is useful for large collections, but on a catalog with
        tens or hundreds of rows it can only reduce recall without providing a
        meaningful speed benefit.  Up to this size every embedding is rescored.
        """
        raw = None
        if isinstance(idx_def, dict):
            raw = idx_def.get(
                "exact_scan_max_items",
                idx_def.get("full_scan_max_items", idx_def.get("small_index_scan_limit", None)),
            )
        if raw is None:
            raw = os.environ.get("NODE_SEMANTIC_EXACT_SCAN_MAX_ITEMS") or 5000
        try:
            value = int(str(raw).strip())
        except Exception:
            value = 5000
        return max(0, min(value, 200000))

    @staticmethod
    def _semantic_literal_tokens(text_value: str) -> set[str]:
        """Extract literal product/technical tokens for optional token scoring.

        This tokenizer does not add synonyms, stems, prefixes, or other search
        heuristics.  It is used only when the configured token weight is above
        zero.  With semantic_weight=1 and token_weight=0 it has no effect on
        candidates, acceptance, ranking, or the query embedding.
        """
        s = str(text_value or "").lower().replace("ё", "е").replace("х", "x").replace("×", "x")
        if not s.strip():
            return set()
        tokens = set()
        try:
            for m in re.finditer(r"[0-9a-zа-я]+(?:[x*./\\_-][0-9a-zа-я]+)*", s, flags=re.I):
                token = m.group(0).strip(" .,/\\_-*")
                if not token:
                    continue
                tokens.add(token)
                compact = re.sub(r"[ .,/\\_-]+", "", token)
                if compact and compact != token:
                    tokens.add(compact)
                for part in re.findall(r"\d+(?:[.,]\d+)?|[a-zа-я]+", token, flags=re.I):
                    if part:
                        tokens.add(part.replace(",", "."))
        except Exception:
            pass
        return {x for x in tokens if x}

    @staticmethod
    def _semantic_technical_tokens(text_value: str) -> set[str]:
        """Technical tokens without aliases or semantic query rewriting."""
        return Node._semantic_literal_tokens(text_value)

    @staticmethod
    def _semantic_token_score(query: str, text_value: str) -> float:
        qt = Node._semantic_technical_tokens(query)
        tt = Node._semantic_technical_tokens(text_value)
        if not qt or not tt:
            return 0.0
        inter = len(qt & tt)
        if inter <= 0:
            return 0.0
        recall = inter / max(1, len(qt))
        precision = inter / max(1, len(tt))
        if recall + precision <= 0:
            return 0.0
        return max(0.0, min(1.0, (2.0 * recall * precision) / (recall + precision)))

    @staticmethod
    def _semantic_mix_score(embedding_score: float, token_score: float, idx_def) -> float:
        """Mix only the two explicitly configured scores.

        No lexical bonuses, synonym boosts, prefix matching, hidden floors, or
        query variants are applied.  Therefore semantic_weight=1 and
        token_weight=0 returns the raw cosine similarity from the model.
        """
        ew = Node._semantic_index_embedding_weight(idx_def)
        tw = Node._semantic_index_token_weight(idx_def)
        total = ew + tw
        try:
            es = max(-1.0, min(1.0, float(embedding_score)))
        except Exception:
            es = 0.0
        try:
            ts = max(0.0, min(1.0, float(token_score)))
        except Exception:
            ts = 0.0
        if total <= 0.0:
            return es
        return max(-1.0, min(1.0, (ew * es + tw * ts) / total))

    @classmethod
    def _semantic_index_storage_key(cls, index_name: str, config_uid=None) -> str:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(index_name or "").strip()) or "semantic"
        return f"{cls.__name__}_{cfg_uid}__semidx__{safe}" if cfg_uid else f"{cls.__name__}__semidx__{safe}"

    @classmethod
    def _semantic_index_db_path(cls, index_name: str, config_uid=None) -> str:
        return os.path.join(STORAGE_BASE_PATH, f"{cls._semantic_index_storage_key(index_name, config_uid)}.sqlite")

    @classmethod
    def _semantic_index_conn(cls, index_name: str, config_uid=None):
        db_path = cls._semantic_index_db_path(index_name, config_uid)
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        conn.execute(
            "CREATE TABLE IF NOT EXISTS items ("
            "node_id TEXT PRIMARY KEY, "
            "text_hash TEXT NOT NULL, "
            "model TEXT NOT NULL, "
            "dim INTEGER NOT NULL, "
            "text_value TEXT, "
            "embedding BLOB NOT NULL, "
            "updated_at REAL NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bands ("
            "band INTEGER NOT NULL, "
            "bucket TEXT NOT NULL, "
            "node_id TEXT NOT NULL, "
            "PRIMARY KEY (band, bucket, node_id))"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS ix_semantic_bands_lookup ON bands (band, bucket)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_semantic_bands_node ON bands (node_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_semantic_items_model ON items (model)")
        return conn

    @classmethod
    def _defined_index_has_rows(cls, index_name: str, config_uid=None) -> bool:
        idx_def = cls._defined_index_def(index_name, config_uid)
        kind = cls._defined_index_kind(idx_def)
        if cls._is_semantic_index_kind(kind):
            try:
                conn = cls._semantic_index_conn(index_name, config_uid)
                try:
                    row = conn.execute("SELECT 1 FROM items LIMIT 1").fetchone()
                    return row is not None
                finally:
                    conn.close()
            except Exception:
                return False
        try:
            store = cls._defined_index_storage(index_name, config_uid)
            try:
                db_path = str(store.filename)
            except Exception:
                cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
                storage_key = f"{cls.__name__}_{cfg_uid}__idx__{index_name}" if cfg_uid else f"{cls.__name__}__idx__{index_name}"
                db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
            conn = sqlite3.connect(db_path, timeout=10.0)
            try:
                table = cls._atomic_sqlite_table(conn)
                qtable = '"' + table.replace('"', '""') + '"'
                return conn.execute(f"SELECT 1 FROM {qtable} LIMIT 1").fetchone() is not None
            finally:
                conn.close()
        except Exception:
            return False

    @staticmethod
    def _semantic_hash_text(model_name: str, text_value: str) -> str:
        return hashlib.sha256((str(model_name or "") + "\0" + str(text_value or "")).encode("utf-8", "ignore")).hexdigest()

    @staticmethod
    def _semantic_model_cache_dir() -> str:
        """Shared on-disk cache for embedding models.

        The model is not user/configuration specific. Keep it in node_storage so
        a server restart, another configuration, or another user reuses the same
        downloaded files instead of downloading the Hugging Face model again.
        Can be overridden for deployments with NODE_SEMANTIC_MODEL_CACHE_DIR.
        """
        base = (os.environ.get("NODE_SEMANTIC_MODEL_CACHE_DIR") or "").strip()
        if not base:
            base = os.path.join(STORAGE_BASE_PATH, "semantic_models")
        try:
            base = os.path.abspath(base)
            os.makedirs(base, exist_ok=True)
        except Exception:
            # Last-resort fallback; still deterministic inside the project.
            base = os.path.abspath(os.path.join(STORAGE_BASE_PATH, "semantic_models"))
            os.makedirs(base, exist_ok=True)
        return base

    @classmethod
    def _semantic_embedding_model(cls, model_name: str):
        model_name = str(model_name or cls._semantic_default_model()).strip() or cls._semantic_default_model()
        cache_dir = cls._semantic_model_cache_dir()
        cache_key = f"{model_name}@@{cache_dir}"
        cached = cls._semantic_model_cache.get(cache_key)
        if cached is not None:
            return cached

        with cls._semantic_model_locks_guard:
            model_lock = cls._semantic_model_locks.get(cache_key)
            if model_lock is None:
                model_lock = threading.RLock()
                cls._semantic_model_locks[cache_key] = model_lock

        with model_lock:
            cached = cls._semantic_model_cache.get(cache_key)
            if cached is not None:
                return cached
            # Make the cache location explicit for sentence-transformers,
            # transformers and huggingface_hub. setdefault respects deployment-level
            # overrides while preventing accidental per-user/temp caches.
            os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", cache_dir)
            os.environ.setdefault("HF_HOME", cache_dir)
            os.environ.setdefault("TRANSFORMERS_CACHE", os.path.join(cache_dir, "transformers"))
            # HF_TOKEN is optional.  When present in root credentials.json it
            # removes unauthenticated Hub rate-limit warnings and speeds model
            # downloads; an already cached public model works without it.
            try:
                from llm_credentials import load_credentials as _load_shared_llm_credentials
                _hf_token = str(_load_shared_llm_credentials().get("hf_token") or "").strip()
                if _hf_token:
                    os.environ.setdefault("HF_TOKEN", _hf_token)
                    os.environ.setdefault("HUGGING_FACE_HUB_TOKEN", _hf_token)
            except Exception:
                pass
            try:
                from sentence_transformers import SentenceTransformer
            except Exception as e:
                raise RuntimeError("sentence-transformers is required for semantic_index") from e
            # Prefer the already downloaded model without touching the Hub.
            # This is the normal path after an index rebuild and prevents a
            # temporary network/Hugging Face outage from freezing every first
            # search after a server restart.  Older sentence-transformers
            # versions do not expose local_files_only, so fall back cleanly.
            try:
                model = SentenceTransformer(
                    model_name,
                    cache_folder=cache_dir,
                    local_files_only=True,
                )
            except TypeError:
                try:
                    model = SentenceTransformer(model_name, cache_folder=cache_dir)
                except TypeError:
                    model = SentenceTransformer(model_name)
            except Exception:
                try:
                    model = SentenceTransformer(model_name, cache_folder=cache_dir)
                except TypeError:
                    model = SentenceTransformer(model_name)
            cls._semantic_model_cache[cache_key] = model
            return model

    @classmethod
    def _semantic_embed(cls, text_value: str, model_name: str, role: str = "passage"):
        text_value = cls._semantic_prepare_model_text(text_value, model_name, role)
        if not text_value:
            return []
        model = cls._semantic_embedding_model(model_name)
        try:
            vec = model.encode([text_value], normalize_embeddings=True, show_progress_bar=False)[0]
        except TypeError:
            vec = model.encode([text_value], normalize_embeddings=True)[0]
        try:
            # numpy ndarray fast path
            import numpy as np
            return np.asarray(vec, dtype="float32").tolist()
        except Exception:
            return [float(x) for x in vec]

    @staticmethod
    def _semantic_vec_to_blob(vec) -> bytes:
        try:
            import numpy as np
            return np.asarray(vec, dtype="float32").tobytes()
        except Exception:
            return struct.pack("<%sf" % len(vec), *[float(x) for x in vec])

    @staticmethod
    def _semantic_blob_to_vec(blob: bytes):
        try:
            import numpy as np
            return np.frombuffer(blob, dtype="float32")
        except Exception:
            if not blob:
                return []
            n = len(blob) // 4
            return struct.unpack("<%sf" % n, blob)

    @staticmethod
    def _semantic_dot(query_vec, stored_vec) -> float:
        try:
            return float(stored_vec.dot(query_vec))
        except Exception:
            try:
                return float(sum(float(a) * float(b) for a, b in zip(query_vec, stored_vec)))
            except Exception:
                return 0.0

    @staticmethod
    def _semantic_lsh_seed(model_name: str, index_name: str, dim: int) -> int:
        raw = f"{model_name}\0{index_name}\0{dim}"
        return int(hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:16], 16) & 0x7FFFFFFF

    @classmethod
    def _semantic_lsh_buckets(cls, vec, model_name: str, index_name: str):
        dim = len(vec)
        if dim <= 0:
            return []
        bits_total = 64
        band_bits = 8
        bands = bits_total // band_bits
        seed = cls._semantic_lsh_seed(model_name, index_name, dim)
        try:
            import numpy as np
            v = np.asarray(vec, dtype="float32")
            rng = np.random.default_rng(seed)
            planes = rng.standard_normal((bits_total, dim)).astype("float32")
            signs = (planes @ v) >= 0
            buckets = []
            for band in range(bands):
                val = 0
                for bit in range(band_bits):
                    if bool(signs[band * band_bits + bit]):
                        val |= (1 << bit)
                buckets.append((band, format(val, "02x")))
            return buckets
        except Exception:
            rng = random.Random(seed)
            buckets = []
            for band in range(bands):
                val = 0
                for bit in range(band_bits):
                    dot = 0.0
                    for x in vec:
                        dot += float(x) * rng.gauss(0.0, 1.0)
                    if dot >= 0:
                        val |= (1 << bit)
                buckets.append((band, format(val, "02x")))
            return buckets

    @classmethod
    def _semantic_delete_node(cls, index_name: str, config_uid, node_id):
        lock_key = cls._semantic_index_storage_key(index_name, config_uid)
        lock = cls._semantic_index_locks.setdefault(lock_key, threading.RLock())
        with lock:
            conn = cls._semantic_index_conn(index_name, config_uid)
            try:
                sid = str(node_id)
                conn.execute("DELETE FROM bands WHERE node_id=?", (sid,))
                conn.execute("DELETE FROM items WHERE node_id=?", (sid,))
                conn.commit()
            finally:
                conn.close()

    @classmethod
    def _semantic_clear_index(cls, index_name: str, config_uid=None):
        lock_key = cls._semantic_index_storage_key(index_name, config_uid)
        lock = cls._semantic_index_locks.setdefault(lock_key, threading.RLock())
        with lock:
            conn = cls._semantic_index_conn(index_name, config_uid)
            try:
                conn.execute("DELETE FROM bands")
                conn.execute("DELETE FROM items")
                conn.commit()
            finally:
                conn.close()

    @classmethod
    def _semantic_upsert_node(cls, index_name: str, config_uid, node_id, text_value: str, idx_def):
        text_value = str(text_value or "").strip()
        if not text_value:
            cls._semantic_delete_node(index_name, config_uid, node_id)
            return
        model_name = cls._semantic_index_model(idx_def)
        embed_text = cls._semantic_prepare_model_text(text_value, model_name, "passage")
        text_hash = cls._semantic_hash_text(model_name, embed_text)
        lock_key = cls._semantic_index_storage_key(index_name, config_uid)
        lock = cls._semantic_index_locks.setdefault(lock_key, threading.RLock())
        with lock:
            conn = cls._semantic_index_conn(index_name, config_uid)
            try:
                sid = str(node_id)
                row = conn.execute("SELECT text_hash, model FROM items WHERE node_id=?", (sid,)).fetchone()
                if row and str(row[0] or "") == text_hash and str(row[1] or "") == model_name:
                    return
                vec = cls._semantic_embed(text_value, model_name, role="passage")
                if not vec:
                    cls._semantic_delete_node(index_name, config_uid, node_id)
                    return
                blob = cls._semantic_vec_to_blob(vec)
                conn.execute("DELETE FROM bands WHERE node_id=?", (sid,))
                conn.execute(
                    "INSERT OR REPLACE INTO items (node_id, text_hash, model, dim, text_value, embedding, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (sid, text_hash, model_name, len(vec), text_value, sqlite3.Binary(blob), time.time()),
                )
                for band, bucket in cls._semantic_lsh_buckets(vec, model_name, index_name):
                    conn.execute(
                        "INSERT OR IGNORE INTO bands (band, bucket, node_id) VALUES (?, ?, ?)",
                        (int(band), str(bucket), sid),
                    )
                conn.commit()
            finally:
                conn.close()

    @classmethod
    def _semantic_find_ids(cls, index_name: str, value, config_uid=None) -> list[str]:
        """Search a semantic index using only configured model and score weights.

        Pure mode (semantic_weight=1, token_weight=0):
            original query text -> one model embedding -> cosine similarity ->
            configured threshold -> descending cosine order.

        The optional token score is calculated only when token_weight > 0.  It
        never changes candidate generation and there are no built-in aliases,
        prefix matches, lexical acceptance overrides, stop-word removal, query
        variants, or hidden thresholds.
        """
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        idx_def = cls._defined_index_def(index_name, cfg_uid)
        model_name = cls._semantic_index_model(idx_def)
        threshold = cls._semantic_index_threshold(idx_def)
        limit = cls._semantic_index_limit(idx_def)
        semantic_weight = cls._semantic_index_embedding_weight(idx_def)
        token_weight = cls._semantic_index_token_weight(idx_def)
        query = "" if value is None else str(value).strip()
        if not query:
            return []

        try:
            query_vec = cls._semantic_embed(query, model_name, role="query")
        except Exception as e:
            print("semantic_index embed/search error", index_name, e)
            return []
        if not query_vec:
            return []

        conn = cls._semantic_index_conn(index_name, cfg_uid)
        try:
            candidates = {}

            def add_rows(rows):
                for row in rows or []:
                    try:
                        node_id, blob, text_value = row
                    except Exception:
                        continue
                    sid = str(node_id)
                    if sid not in candidates:
                        candidates[sid] = (blob, text_value or "")

            # Exact cosine scan is the default for small/medium catalogs.  This
            # is not a lexical heuristic: every stored model vector is compared
            # with the single model vector of the original query.
            scan_limit = cls._semantic_index_scan_all_limit(idx_def)
            if scan_limit <= 0:
                exact_scan_max = cls._semantic_index_exact_scan_max_items(idx_def)
                if exact_scan_max > 0:
                    try:
                        row = conn.execute(
                            "SELECT COUNT(*) FROM items WHERE model=?",
                            (model_name,),
                        ).fetchone()
                        item_count = int((row or [0])[0] or 0)
                    except Exception:
                        item_count = 0
                    if 0 < item_count <= exact_scan_max:
                        scan_limit = item_count

            if scan_limit > 0:
                try:
                    rows = conn.execute(
                        "SELECT node_id, embedding, text_value FROM items "
                        "WHERE model=? ORDER BY node_id LIMIT ?",
                        (model_name, int(scan_limit)),
                    ).fetchall()
                except Exception:
                    rows = []
                add_rows(rows)
            else:
                # Large indexes may use LSH only to choose model-vector
                # candidates.  Ranking and filtering remain pure cosine/token
                # scoring with no lexical additions.
                try:
                    buckets = cls._semantic_lsh_buckets(query_vec, model_name, index_name)
                except Exception:
                    buckets = []
                for band, bucket in buckets:
                    try:
                        rows = conn.execute(
                            "SELECT i.node_id, i.embedding, i.text_value FROM bands b "
                            "JOIN items i ON i.node_id=b.node_id "
                            "WHERE b.band=? AND b.bucket=? AND i.model=?",
                            (int(band), str(bucket), model_name),
                        ).fetchall()
                    except Exception:
                        rows = []
                    add_rows(rows)

            scored = []
            debug_rows = []
            debug_enabled = str(os.environ.get("NODE_SEMANTIC_DEBUG") or "").strip().lower() in {
                "1", "true", "yes", "on"
            }

            for node_id, payload in candidates.items():
                try:
                    blob, text_value = payload
                except Exception:
                    blob, text_value = payload, ""
                stored_vec = cls._semantic_blob_to_vec(blob)
                embedding_score = cls._semantic_dot(query_vec, stored_vec)
                token_score = (
                    cls._semantic_token_score(query, text_value)
                    if token_weight > 0.0
                    else 0.0
                )
                score = cls._semantic_mix_score(embedding_score, token_score, idx_def)
                accepted = score >= threshold
                if accepted:
                    scored.append((score, embedding_score, token_score, node_id))
                if debug_enabled:
                    debug_rows.append(
                        (score, embedding_score, token_score, str(text_value), str(node_id), accepted)
                    )

            scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
            if debug_rows:
                debug_rows.sort(key=lambda x: (x[0], x[1]), reverse=True)
                print(
                    "SEMANTIC DEBUG",
                    {
                        "index": index_name,
                        "query": query,
                        "model_input": cls._semantic_prepare_model_text(query, model_name, "query"),
                        "model": model_name,
                        "threshold": threshold,
                        "semantic_weight": semantic_weight,
                        "token_weight": token_weight,
                        "candidate_count": len(candidates),
                        "top": debug_rows[:20],
                    },
                )
            return [node_id for _score, _emb, _tok, node_id in scored[:limit]]
        finally:
            conn.close()

    @classmethod
    def rebuild_defined_indexes(cls, config_uid=None, index_names=None) -> dict:
        """Rebuild configured class indexes from existing stored nodes.

        This is intended for editor/admin actions after adding a new index,
        changing semantic model/threshold, or importing old data. It does not
        call _save() and does not run business event handlers; it only rewrites
        derived index storage. For semantic indexes, it also warms/downloads the
        embedding model before the loop so the UI action clearly represents the
        model preparation step.
        """
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        wanted = None
        if index_names:
            wanted = {str(x or "").strip() for x in (index_names if isinstance(index_names, (list, tuple, set)) else [index_names])}
            wanted.discard("")
        defs = []
        for idx in cls._get_defined_indexes(cfg_uid) or []:
            if not isinstance(idx, dict):
                continue
            name = str(idx.get("name") or "").strip()
            if not name:
                continue
            if wanted is not None and name not in wanted:
                continue
            defs.append(idx)
        result = {
            "ok": True,
            "class": cls.__name__,
            "config_uid": cfg_uid,
            "indexes": len(defs),
            "nodes": 0,
            "index_nodes": {str(idx.get("name") or ""): 0 for idx in defs if str(idx.get("name") or "").strip()},
            "semantic_models": [],
            "errors": [],
        }
        if not defs:
            return result

        # Clear selected index storages first; rebuild must reflect current data exactly.
        for idx in defs:
            name = str(idx.get("name") or "").strip()
            kind = cls._defined_index_kind(idx)
            try:
                if cls._is_semantic_index_kind(kind):
                    model_name = cls._semantic_index_model(idx)
                    try:
                        cls._semantic_embedding_model(model_name)
                        if model_name not in result["semantic_models"]:
                            result["semantic_models"].append(model_name)
                    except Exception as e:
                        result["errors"].append(f"{name}: semantic model load failed: {e}")
                        result["ok"] = False
                        # Keep going; upsert will skip/fail safely too.
                    cls._semantic_clear_index(name, cfg_uid)
                else:
                    store = cls._defined_index_storage(name, cfg_uid)
                    try:
                        store.clear()
                        try:
                            store.commit()
                        except Exception:
                            pass
                    except Exception:
                        # Fallback for SqliteDict-like storages where clear may fail.
                        for k in list(store.keys()):
                            try:
                                del store[k]
                            except Exception:
                                pass
            except Exception as e:
                result["errors"].append(f"{name}: clear failed: {e}")
                result["ok"] = False

        try:
            all_nodes = cls.get_all(cfg_uid)
        except Exception as e:
            result["ok"] = False
            result["errors"].append(f"get_all failed: {e}")
            return result
        if isinstance(all_nodes, dict):
            iterable = list(all_nodes.items())
        else:
            iterable = [(getattr(n, "_id", ""), n) for n in (all_nodes or [])]

        for node_id, node in iterable:
            try:
                data = getattr(node, "_data", {}) or {}
                if not isinstance(data, dict):
                    data = {}
                update_result = cls._update_defined_indexes(
                    cfg_uid,
                    getattr(node, "_id", node_id),
                    {},
                    data,
                    strict=True,
                )
                for written_name in (update_result or {}).get("written", []):
                    if written_name in result["index_nodes"]:
                        result["index_nodes"][written_name] += 1
                result["nodes"] += 1
            except Exception as e:
                result["errors"].append(f"{node_id}: {e}")
                result["ok"] = False
        return result

    @staticmethod
    def _index_debug_enabled() -> bool:
        return str(os.environ.get("NODE_INDEX_DEBUG") or "").strip().lower() in {
            "1", "true", "yes", "on"
        }

    @classmethod
    def _index_debug(cls, *parts) -> None:
        if cls._index_debug_enabled():
            try:
                print(*parts)
            except Exception:
                pass

    @staticmethod
    def _defined_index_async_write(idx_def) -> bool:
        if not isinstance(idx_def, dict):
            return False
        raw = idx_def.get("async_write", idx_def.get("asynchronous_write", False))
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return bool(raw)

    @classmethod
    def _get_async_index_executor(cls):
        # Store on Node itself rather than on generated subclasses so every
        # class shares one ordered queue in this server process.
        with Node._async_index_executor_lock:
            if Node._async_index_executor is None:
                Node._async_index_executor = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="noda-index"
                )
            return Node._async_index_executor

    @classmethod
    def _defined_index_delta_path(cls, index_name: str, config_uid=None) -> str:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = (
            f"{cls.__name__}_{cfg_uid}__idx__{index_name}__delta"
            if cfg_uid else f"{cls.__name__}__idx__{index_name}__delta"
        )
        return os.path.join(STORAGE_BASE_PATH, storage_key + ".sqlite")

    @classmethod
    def _apply_hash_index_delta(cls, index_name, config_uid, node_id, removed, added) -> None:
        """Overlay exact-index changes without rewriting a potentially huge bucket.

        The historical index format stores ``value -> [node ids]``.  Rewriting a
        100k-id low-cardinality bucket for one new item is quadratic and can take
        minutes.  Keep that base format for compatibility, but store subsequent
        membership changes as tiny rows in a sidecar.  Reads merge base + overlay;
        a normal fast rebuild folds the overlay back into the base and clears it.
        """
        removed = [str(x) for x in (removed or []) if str(x)]
        added = [str(x) for x in (added or []) if str(x)]
        if not removed and not added:
            return
        path = cls._defined_index_delta_path(index_name, config_uid)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        ready = getattr(Node, "_hash_index_delta_ready", None)
        if ready is None:
            ready = set()
            Node._hash_index_delta_ready = ready
        ready_lock = getattr(Node, "_hash_index_delta_ready_lock", None)
        if ready_lock is None:
            ready_lock = threading.RLock()
            Node._hash_index_delta_ready_lock = ready_lock

        conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            if path not in ready:
                with ready_lock:
                    if path not in ready:
                        conn.execute(
                            "CREATE TABLE IF NOT EXISTS index_delta ("
                            "value TEXT NOT NULL, node_id TEXT NOT NULL, present INTEGER NOT NULL,"
                            "PRIMARY KEY(value,node_id))"
                        )
                        conn.execute(
                            "CREATE INDEX IF NOT EXISTS idx_index_delta_value "
                            "ON index_delta(value,present,node_id)"
                        )
                        ready.add(path)
            conn.execute("BEGIN IMMEDIATE")
            sid = str(node_id)
            rows = [(value, sid, 0) for value in removed] + [(value, sid, 1) for value in added]
            conn.executemany(
                "INSERT INTO index_delta(value,node_id,present) VALUES(?,?,?) "
                "ON CONFLICT(value,node_id) DO UPDATE SET present=excluded.present",
                rows,
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

    @classmethod
    def _hash_index_delta_for_value(cls, index_name, config_uid, value):
        path = cls._defined_index_delta_path(index_name, config_uid)
        if not os.path.isfile(path):
            return []
        conn = sqlite3.connect(path, timeout=10.0)
        try:
            return [
                (str(node_id), bool(present))
                for node_id, present in conn.execute(
                    "SELECT node_id,present FROM index_delta WHERE value=?",
                    (str(value),),
                ).fetchall()
            ]
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()

    @classmethod
    def _hash_index_delta_values(cls, index_name, config_uid):
        path = cls._defined_index_delta_path(index_name, config_uid)
        if not os.path.isfile(path):
            return []
        conn = sqlite3.connect(path, timeout=10.0)
        try:
            return [str(row[0]) for row in conn.execute("SELECT DISTINCT value FROM index_delta").fetchall()]
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()

    @classmethod
    def _hash_index_effective_bucket(cls, index_name, config_uid, store, value):
        try:
            base = [str(x) for x in (store.get(str(value), []) or [])]
        except Exception:
            base = []
        out = list(base)
        positions = {node_id: idx for idx, node_id in enumerate(out)}
        removed = set()
        added = []
        for node_id, present in cls._hash_index_delta_for_value(index_name, config_uid, value):
            if present:
                removed.discard(node_id)
                if node_id not in positions and node_id not in added:
                    added.append(node_id)
            else:
                removed.add(node_id)
                if node_id in added:
                    added.remove(node_id)
        if removed:
            out = [node_id for node_id in out if node_id not in removed]
        out.extend(added)
        return out

    @classmethod
    def _clear_hash_index_delta(cls, index_name, config_uid) -> None:
        path = cls._defined_index_delta_path(index_name, config_uid)
        if not os.path.isfile(path):
            return
        conn = sqlite3.connect(path, timeout=30.0)
        try:
            conn.execute("DELETE FROM index_delta")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()

    @classmethod
    def _write_one_defined_index(cls, config_uid, node_id, old_state, new_state, idx, strict=False) -> bool:
        """Synchronously apply one index delta. Return whether storage changed."""
        if not isinstance(idx, dict):
            return False
        name = str(idx.get("name") or "").strip()
        if not name:
            return False
        idx_kind = cls._defined_index_kind(idx)
        keys_spec = idx.get("keys") or idx.get("field") or idx.get("key") or idx.get("fields") or ""
        old_vals = cls._extract_index_values(old_state or {}, keys_spec)
        new_vals = cls._extract_index_values(new_state or {}, keys_spec)

        # The previous implementation rewrote every bucket on every _save(),
        # even when indexed fields had not changed.  Besides needless fsyncs it
        # made harmless parent/child saves dominate long setup operations.
        if old_vals == new_vals:
            return False

        if cls._is_semantic_index_kind(idx_kind):
            try:
                new_text = (new_vals[0] if new_vals else "")
                if new_text:
                    cls._semantic_upsert_node(name, config_uid, node_id, new_text, idx)
                    cls._index_debug("IDX WRITE", name, config_uid, new_text, [str(node_id)])
                else:
                    cls._semantic_delete_node(name, config_uid, node_id)
                    cls._index_debug("IDX DELETE", name, config_uid, str(node_id))
                return True
            except Exception as e:
                print("semantic_index update error", name, config_uid, node_id, e)
                if strict:
                    raise RuntimeError(f"{name}: semantic index update failed: {e}") from e
                return False

        store = cls._defined_index_storage(name, config_uid)
        old_set = set(old_vals)
        new_set = set(new_vals)
        removed = old_set - new_set
        added = new_set - old_set

        # Exact/hash indexes use an O(1) membership overlay.  This is critical
        # for low-cardinality fields such as placement_zone where one bucket can
        # contain 100 000 item ids.  The previous read/append/rewrite path made a
        # single web save take minutes.
        if idx_kind == "hash_index":
            try:
                cls._apply_hash_index_delta(name, config_uid, node_id, removed, added)
                for val in removed:
                    cls._index_debug("IDX DELTA DELETE", name, config_uid, val, str(node_id))
                for val in added:
                    cls._index_debug("IDX DELTA WRITE", name, config_uid, val, str(node_id))
                return bool(removed or added)
            except Exception as e:
                if strict:
                    raise RuntimeError(f"{name}: hash index delta failed: {e}") from e
                print("hash_index delta update error", name, config_uid, node_id, e)
                return False

        # Text/fuzzy indexes keep the historical bucket path. Their keys are
        # normally high-cardinality and the search sidecar depends on them.
        # Protect the read-modify-write update from concurrent writers.
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        storage_key = f"{cls.__name__}_{cfg_uid}__idx__{name}" if cfg_uid else f"{cls.__name__}__idx__{name}"
        index_lock = cls._defined_index_locks.setdefault(storage_key, threading.RLock())
        with index_lock:
            for val in removed:
                try:
                    bucket = list(store.get(val, []) or [])
                    bucket = [x for x in bucket if str(x) != str(node_id)]
                    if bucket:
                        store[val] = bucket
                    elif val in store:
                        del store[val]
                    cls._index_debug("IDX DELETE", name, config_uid, val, str(node_id))
                except Exception as e:
                    if strict:
                        raise RuntimeError(f"{name}: index delete failed: {e}") from e

            for val in added:
                try:
                    bucket = list(store.get(val, []) or [])
                    sid = str(node_id)
                    if sid not in bucket:
                        bucket.append(sid)
                        store[val] = bucket
                    cls._index_debug("IDX WRITE", name, config_uid, val, bucket)
                except Exception as e:
                    if strict:
                        raise RuntimeError(f"{name}: index update failed: {e}") from e
        if idx_kind in {"text_index", "text_index_full"} and (removed or added):
            cls._sync_text_search_sidecar(name, config_uid, removed, added)
        return bool(removed or added)

    @classmethod
    def _queue_defined_index_update(cls, config_uid, node_id, old_state, new_state, idx) -> None:
        idx_copy = dict(idx or {})
        old_copy = dict(old_state or {})
        new_copy = dict(new_state or {})

        def work():
            try:
                cls._write_one_defined_index(
                    config_uid, node_id, old_copy, new_copy, idx_copy, strict=False
                )
            except Exception as e:
                # Background mode is optional and must never terminate the
                # request worker. Keep the failure visible in the server log.
                print(
                    "async index update error",
                    str(idx_copy.get("name") or ""),
                    config_uid,
                    node_id,
                    e,
                )

        cls._get_async_index_executor().submit(work)

    @classmethod
    def _update_defined_indexes(cls, config_uid, node_id, old_state, new_state, strict=False):
        defs = cls._get_defined_indexes(config_uid)
        if not defs:
            return {"written": [], "queued": []}
        written = []
        queued = []
        for idx in defs:
            if not isinstance(idx, dict):
                continue
            name = str(idx.get("name") or "").strip()
            if not name:
                continue
            keys_spec = idx.get("keys") or idx.get("field") or idx.get("key") or idx.get("fields") or ""
            old_vals = cls._extract_index_values(old_state or {}, keys_spec)
            new_vals = cls._extract_index_values(new_state or {}, keys_spec)
            if old_vals == new_vals:
                continue

            if not strict and cls._defined_index_async_write(idx):
                cls._queue_defined_index_update(config_uid, node_id, old_state, new_state, idx)
                queued.append(name)
                if name not in written:
                    written.append(name)
                continue

            changed = cls._write_one_defined_index(
                config_uid, node_id, old_state, new_state, idx, strict=strict
            )
            if changed and name not in written:
                written.append(name)
        return {"written": written, "queued": queued}

    @classmethod
    def _defined_index_def(cls, index_name: str, config_uid=None):
        name = str(index_name or "").strip()
        if not name:
            return None
        for idx in cls._get_defined_indexes(config_uid):
            if not isinstance(idx, dict):
                continue
            if str(idx.get("name") or "").strip() == name:
                return idx
        return None

    @staticmethod
    def _defined_index_kind(idx_def) -> str:
        kind = ""
        if isinstance(idx_def, dict):
            kind = str(idx_def.get("kind") or idx_def.get("type") or "").strip().lower()
        return kind or "hash_index"

    @classmethod
    def find_ids_by_index(cls, index_name: str, value, config_uid=None, limit=None) -> list[str]:
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        name = str(index_name or "").strip()
        if not name:
            return []

        idx_def = cls._defined_index_def(name, cfg_uid)
        idx_kind = cls._defined_index_kind(idx_def)
        if cls._is_semantic_index_kind(idx_kind):
            result = cls._semantic_find_ids(name, value, cfg_uid)
            try:
                return result[:max(0, int(limit))] if limit is not None else result
            except Exception:
                return result
        store = cls._defined_index_storage(name, cfg_uid)

        def _lookup_variants(one):
            raw = "" if one is None else str(one).strip()
            if raw == "":
                return []
            variants = []
            seen_variants = set()

            def add_variant(v):
                s = str(v or "").strip()
                if not s or s in seen_variants:
                    return
                seen_variants.add(s)
                variants.append(s)

            add_variant(raw)
            try:
                uid_cfg, uid_cls, internal_id = parse_uid_any(raw)
            except Exception:
                uid_cfg, uid_cls, internal_id = None, None, None
            if internal_id:
                add_variant(internal_id)
                if uid_cls:
                    add_variant(f"{uid_cls}${internal_id}")
                    if cfg_uid:
                        add_variant(f"{cfg_uid}${uid_cls}${internal_id}")
                elif cfg_uid and cls.__name__:
                    add_variant(f"{cls.__name__}${internal_id}")
                    add_variant(f"{cfg_uid}${cls.__name__}${internal_id}")
            return variants

        def _normalized_node_ref(one):
            raw = "" if one is None else str(one).strip()
            if not raw:
                return None
            try:
                _cfg, ref_cls, ref_id = parse_uid_any(raw)
            except Exception:
                ref_cls, ref_id = None, None
            if ref_cls and ref_id:
                return (str(ref_cls).strip(), str(ref_id).strip())
            return None

        values = value
        if isinstance(values, str):
            raw = values.strip()
            if raw.startswith('[') and raw.endswith(']'):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        values = parsed
                except Exception:
                    values = raw

        try:
            result_limit = max(0, int(limit)) if limit is not None else None
        except Exception:
            result_limit = None

        def _append_bucket(out, seen, bucket):
            for x in (bucket or []):
                sx = str(x)
                if sx not in seen:
                    seen.add(sx)
                    out.append(sx)
                    if result_limit is not None and len(out) >= result_limit:
                        break

        def _collect_hash_many(seq):
            out = []
            seen = set()
            unresolved_refs = []
            unresolved_seen = set()

            def add_effective(value):
                bucket = cls._hash_index_effective_bucket(name, cfg_uid, store, value)
                before = len(out)
                _append_bucket(out, seen, bucket)
                return len(out) > before

            for one in seq:
                found = False
                for variant in _lookup_variants(one):
                    if add_effective(variant):
                        found = True
                    if result_limit is not None and len(out) >= result_limit:
                        return out
                nref = _normalized_node_ref(one)
                # The old implementation scanned every index key even when the
                # direct full-UID lookup had already succeeded.  Only use the
                # compatibility scan for unresolved legacy representations.
                if not found and nref and nref not in unresolved_seen:
                    unresolved_seen.add(nref)
                    unresolved_refs.append(nref)

            if not unresolved_refs:
                return out

            try:
                store_keys = list(store.keys())
            except Exception:
                store_keys = []
            delta_keys = cls._hash_index_delta_values(name, cfg_uid)
            candidate_keys = []
            candidate_seen = set()
            for store_key in list(store_keys) + list(delta_keys):
                skey = str(store_key or "").strip()
                if not skey or skey in candidate_seen:
                    continue
                candidate_seen.add(skey)
                snref = _normalized_node_ref(skey)
                if snref and snref in unresolved_seen:
                    candidate_keys.append(skey)

            for store_key in candidate_keys:
                add_effective(store_key)
                if result_limit is not None and len(out) >= result_limit:
                    break
            return out

        def _collect_text_many(seq, bidirectional: bool = False):
            needles = []
            seen_needles = set()
            for one in seq:
                raw_needle = "" if one is None else str(one).strip()
                normalized = raw_needle.lower()
                if not normalized or normalized in seen_needles:
                    continue
                seen_needles.add(normalized)
                needles.append((raw_needle, normalized))
            if not needles:
                return []

            out = []
            seen = set()

            # Section/list search only needs the first page.  Use the
            # persistent Unicode/trigram sidecar instead of invoking a Python
            # casefold callback for every key in the SQLite table.
            if result_limit is not None:
                sidecar_keys = cls._search_text_sidecar(
                    name,
                    cfg_uid,
                    needles,
                    bidirectional,
                    result_limit,
                )
                if sidecar_keys is not None:
                    for store_key in sidecar_keys:
                        try:
                            bucket = store.get(store_key, []) or []
                        except Exception:
                            bucket = []
                        _append_bucket(out, seen, bucket)
                        if len(out) >= result_limit:
                            break
                    return out

                # Compatibility fallback for SQLite builds without FTS5 or a
                # damaged sidecar.  It is deliberately kept out of the normal
                # hot path.
                cfg_part = str(cfg_uid or "").strip()
                storage_key = (
                    f"{cls.__name__}_{cfg_part}__idx__{name}"
                    if cfg_part else f"{cls.__name__}__idx__{name}"
                )
                db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                conn = sqlite3.connect(db_path, timeout=30.0)
                try:
                    conn.create_function(
                        "NODA_CASEFOLD",
                        1,
                        lambda value: str(value or "").casefold(),
                        deterministic=True,
                    )
                    table = cls._atomic_sqlite_table(conn)
                    qtable = '"' + table.replace('"', '""') + '"'
                    clauses = []
                    params = []
                    for _original, normalized in needles:
                        folded = str(normalized or "").casefold()
                        clauses.append("INSTR(NODA_CASEFOLD(CAST(key AS TEXT)), ?) > 0")
                        params.append(folded)
                        if bidirectional:
                            clauses.append("INSTR(?, NODA_CASEFOLD(CAST(key AS TEXT))) > 0")
                            params.append(folded)
                    sql = f"SELECT value FROM {qtable} WHERE " + " OR ".join(clauses) + " LIMIT ?"
                    params.append(max(20, result_limit * 4))
                    for (raw_bucket,) in conn.execute(sql, params).fetchall():
                        if isinstance(raw_bucket, memoryview):
                            raw_bucket = raw_bucket.tobytes()
                        try:
                            bucket = pickle.loads(raw_bucket)
                        except Exception:
                            bucket = []
                        _append_bucket(out, seen, bucket)
                        if len(out) >= result_limit:
                            break
                    return out
                finally:
                    conn.close()

            try:
                store_keys = list(store.keys())
            except Exception:
                store_keys = []
            for store_key in store_keys:
                skey = str(store_key or "").strip()
                if not skey:
                    continue
                skey_l = skey.lower()
                if bidirectional:
                    matched = any((n in skey_l) or (skey_l in n) for _o, n in needles)
                else:
                    matched = any(n in skey_l for _o, n in needles)
                if not matched:
                    continue
                try:
                    res = store.get(store_key, []) or []
                except Exception:
                    res = []
                _append_bucket(out, seen, res)
            return out

        def _trigram_normalize(one):
            s = "" if one is None else str(one).strip().lower()
            return re.sub(r"\s+", " ", s)

        def _trigrams(one):
            s = _trigram_normalize(one)
            if not s:
                return set()
            if len(s) < 3:
                return {s}
            padded = "  " + s + "  "
            return {padded[i:i + 3] for i in range(max(0, len(padded) - 2))}

        def _trigram_similarity(a, b):
            ta = _trigrams(a)
            tb = _trigrams(b)
            if not ta or not tb:
                return 0.0
            return len(ta & tb) / float(max(len(ta), len(tb), 1))

        def _collect_trigram_many(seq):
            needles = []
            seen_needles = set()
            for one in seq:
                s = _trigram_normalize(one)
                if not s or s in seen_needles:
                    continue
                seen_needles.add(s)
                needles.append(s)
            if not needles:
                return []
            try:
                threshold = float((idx_def or {}).get("threshold", (idx_def or {}).get("min_similarity", (idx_def or {}).get("similarity", 0.28))))
            except Exception:
                threshold = 0.28
            threshold = max(0.05, min(1.0, threshold))
            scored = []
            try:
                store_keys = list(store.keys())
            except Exception:
                store_keys = []
            for store_key in store_keys:
                skey = str(store_key or "").strip()
                skey_l = _trigram_normalize(skey)
                if not skey_l:
                    continue
                best = 0.0
                for n in needles:
                    score = _trigram_similarity(n, skey_l)
                    # exact substring is still a very strong fuzzy hit, but not
                    # the only condition as it was before.
                    if n in skey_l or skey_l in n:
                        score = max(score, 0.95)
                    best = max(best, score)
                if best < threshold:
                    continue
                try:
                    res = store.get(store_key, []) or []
                except Exception:
                    res = []
                if res:
                    scored.append((best, store_key, res))
            scored.sort(key=lambda x: x[0], reverse=True)
            out = []
            seen = set()
            for _score, _store_key, res in scored:
                _append_bucket(out, seen, res)
            return out

        seq = values if isinstance(values, (list, tuple, set)) else [values]
        if idx_kind == "text_index":
            result = _collect_text_many(seq)
        elif idx_kind == "trigram_index":
            result = _collect_trigram_many(seq)
        elif idx_kind == "text_index_full":
            result = _collect_text_many(seq, bidirectional=True)
        else:
            result = _collect_hash_many(seq)
        return result[:result_limit] if result_limit is not None else result

    @classmethod
    def find_by_index(cls, index_name: str, value, config_uid=None):
        cfg_uid = str(config_uid or current_config_uid_from_handlers() or "").strip()
        ids = cls.find_ids_by_index(index_name, value, cfg_uid)
        out = {}
        for nid in ids:
            try:
                obj = cls.get(nid, cfg_uid)
                if obj:
                    out[str(nid)] = obj
            except Exception:
                pass
        return out

    @classmethod
    def get_by_index(cls, index_name: str, value, config_uid=None):
        found = cls.find_by_index(index_name, value, config_uid)
        for _nid, obj in found.items():
            return obj
        return None

    # --- date index (fast queries by _data._date_key) ---

    @classmethod
    def _get_date_index_storage(cls, config_uid=None):
        if not config_uid:
            config_uid = current_config_uid_from_handlers()
        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        idx_key = f"{storage_key}__date_index"

        if idx_key not in cls._date_index_storages:
            if idx_key not in cls._date_index_locks:
                cls._date_index_locks[idx_key] = threading.RLock()
            with cls._date_index_locks[idx_key]:
                if idx_key not in cls._date_index_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{idx_key}.sqlite")
                    cls._date_index_storages[idx_key] = SqliteDict(db_path, autocommit=True)

        return cls._date_index_storages[idx_key]

    @classmethod
    def _date_index_key(cls, date_key: str, node_id: str) -> str:
        return f"{date_key}|{node_id}"

    @classmethod
    def page_at_date(cls, *, date=None, config_uid=None, offset=0, limit=50):
        """Fast paged nodes list up to date (inclusive) using date index.
        date: 'YYYY-MM-DD'|'YYYYMMDD'|ISO datetime; if None -> falls back to key paging.
        Returns: {total, offset, limit, items}
        """
        if not config_uid:
            config_uid = current_config_uid_from_handlers()

        import sqlite3, pickle

        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        main_db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
        if not os.path.exists(main_db_path):
            return {"total": 0, "offset": int(offset), "limit": int(limit), "items": []}

        table = "unnamed"

        def unpack(blob):
            try:
                return pickle.loads(blob)
            except Exception:
                return None

        dk = normalize_date_key(date)

        # no date -> fast key paging like nodes_api_page
        if dk is None:
            conn = sqlite3.connect(main_db_path)
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(1) FROM {table}")
                total = int(cur.fetchone()[0] or 0)
                cur.execute(
                    f"SELECT value FROM {table} ORDER BY key LIMIT ? OFFSET ?",
                    (int(limit), int(offset)),
                )
                rows = cur.fetchall()
                items = []
                for (val_blob,) in rows:
                    obj = unpack(val_blob)
                    if obj is not None:
                        items.append(obj)
                return {"total": total, "offset": int(offset), "limit": int(limit), "items": items}
            finally:
                conn.close()

        # date provided -> use date index to get node ids, then fetch docs from main storage
        idx_db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}__date_index.sqlite")
        if not os.path.exists(idx_db_path):
            # index missing -> fallback to scan (slow) but correct
            conn = sqlite3.connect(main_db_path)
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT value FROM {table}")
                rows = cur.fetchall()
                items_all = []
                for (val_blob,) in rows:
                    obj = unpack(val_blob)
                    if obj is None:
                        continue
                    data = (obj or {}).get("_data") or {}
                    k = normalize_date_key(data.get("_date_key") or data.get("_date"))
                    if k and k <= dk:
                        items_all.append(obj)
                total = len(items_all)
                sliced = items_all[int(offset): int(offset) + int(limit)]
                return {"total": total, "offset": int(offset), "limit": int(limit), "items": sliced}
            finally:
                conn.close()

        upper = f"{dk}|~"

        conn = sqlite3.connect(idx_db_path)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(1) FROM {table} WHERE key <= ?", (upper,))
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT key FROM {table} WHERE key <= ? ORDER BY key LIMIT ? OFFSET ?",
                (upper, int(limit), int(offset)),
            )
            idx_rows = cur.fetchall()
            idx_keys = [r[0] for r in idx_rows]
        finally:
            conn.close()

        node_ids = []
        for k in idx_keys:
            try:
                _, node_id = k.split("|", 1)
                node_ids.append(node_id)
            except Exception:
                pass

        if not node_ids:
            return {"total": total, "offset": int(offset), "limit": int(limit), "items": []}

        # fetch docs for these node_ids from main db (per-id lookup is cheap for page-sized list)
        conn = sqlite3.connect(main_db_path)
        try:
            cur = conn.cursor()
            items = []
            for node_id in node_ids:
                cur.execute(f"SELECT value FROM {table} WHERE key = ?", (node_id,))
                row = cur.fetchone()
                if not row:
                    continue
                obj = unpack(row[0])
                if obj is not None:
                    items.append(obj)
            return {"total": total, "offset": int(offset), "limit": int(limit), "items": items}
        finally:
            conn.close()
    
    @classmethod
    def find(cls, condition_func, config_uid=None):
        if not config_uid:
            config_uid = current_config_uid_from_handlers()
        results = {}
        for node_id, node in cls.get_all(config_uid).items():
            if condition_func(node):
                results[node_id] = node
        return results
    
    def to_dict(self):
        with self._lock:
            if self._id in self._storage:
                # Deep-copy before normalizing metadata so to_dict() stays a
                # pure read even when a storage backend returns mutable objects.
                result = copy.deepcopy(self._storage[self._id])
                # We ensure that _data contains the current _id and _class
                if '_data' in result:
                    result['_data']['_class'] = self.__class__.__name__
                    result['_data']['_id'] = normalize_own_uid(self._config_uid, self.__class__.__name__, result['_data'].get('_id') or self._id)
                return result
            return {}
    
    # --- class schema managers (persistent via SqliteDict) ---
    @classmethod
    def _load_schemes_for_class(cls):
        
        
        if hasattr(cls, "_schemes") and cls._schemes is not None:
            return cls._schemes
        
        try:
            stored = _SCHEMES_STORAGE.get(cls.__name__, None)
        except Exception:
            stored = None
        cls._schemes = stored or {}
        return cls._schemes

    @classmethod
    def _save_schemes_for_class(cls):
        
        try:
            _SCHEMES_STORAGE[cls.__name__] = cls._schemes or {}
           
            try:
                _SCHEMES_STORAGE.commit()
            except Exception:
                pass
        except Exception:
            pass

    @classmethod
    def _add_scheme(cls, name, key_types, value_types):
        schemes = cls._load_schemes_for_class()
        schemes[name] = {"keys": key_types, "values": value_types}
        cls._schemes = schemes
        cls._save_schemes_for_class()

    @classmethod
    def _remove_scheme(cls, name):
        schemes = cls._load_schemes_for_class()
        if name in schemes:
            del schemes[name]
            cls._schemes = schemes
            cls._save_schemes_for_class()

    @classmethod
    def _get_schemes(cls):
        return cls._load_schemes_for_class()

    def _sql_balance_enabled(self) -> bool:
        """Return True when optional backend SQL balance projection is installed."""
        try:
            return bool(_BALANCE_SQL is not None and getattr(_BALANCE_SQL, "AVAILABLE", True))
        except Exception:
            return False

    def _sql_balance_owner_id(self) -> str:
        """Stable normalized owner uid used by the SQL balance projection."""
        try:
            return normalize_own_uid(self._config_uid or "", self.__class__.__name__, self._id)
        except Exception:
            return str(self._id or "")

    def _sql_balance_config_uid(self) -> str:
        try:
            return str(self._config_uid or CURRENT_CONFIG_UID.get() or current_config_uid_from_handlers() or "")
        except Exception:
            return str(self._config_uid or "")

    def _sql_transaction_import_legacy(self, scheme_name: str, tx_kind: str = "sum") -> None:
        """One-time lazy import of legacy JSON transactions for this owner/scheme.

        This protects existing installations: when balance_sql.py is first added,
        old node._data transaction rows are copied into SQL before the first SQL
        read/write. New SQL-mode transactions are not duplicated back into JSON.
        Removing balance_sql.py later therefore returns to the old JSON
        implementation, but JSON will not contain transactions created while SQL
        mode was active.
        """
        if not self._sql_balance_enabled():
            return
        root_key = "_state_transactions" if tx_kind == "state" else "_transactions"
        try:
            txs = list(self._data.get(root_key, {}).get(scheme_name, []) or [])
        except Exception:
            txs = []
        if not txs:
            return
        _BALANCE_SQL.import_json_transactions(
            owner_id=self._sql_balance_owner_id(),
            config_uid=self._sql_balance_config_uid(),
            class_name=self.__class__.__name__,
            scheme_name=str(scheme_name or ""),
            transactions=txs,
            tx_kind="state" if tx_kind == "state" else "sum",
        )

    def _sql_balance_import_legacy(self, scheme_name: str) -> None:
        # Backward-compatible internal alias used by older patches.
        return self._sql_transaction_import_legacy(scheme_name, "sum")

    def _rebuild_sum_transactions(self, scheme_name: str):
        """
        Полный пересчёт цепочки _transactions[scheme_name]:
        - balances пересчитываются заново
        - parent/child/prev_hash/hash пересчитываются заново
        Индекс _tx_index тоже пересобирается.
        """
        if self._sql_balance_enabled():
            self._sql_balance_import_legacy(scheme_name)
            return _BALANCE_SQL.rebuild_sum_transactions(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                class_name=self.__class__.__name__,
                scheme_name=str(scheme_name or ""),
            )

        txs = list(self._data.get("_transactions", {}).get(scheme_name, []) or [])
        # ensure stable ordering by period_key then uid
        try:
            txs.sort(key=lambda t: ((t.get("period_key") or normalize_date_key(t.get("period")) or ""), str(t.get("uid") or "")))
        except Exception:
            pass
        if not txs:
            # почистим индекс
            idx_root = self._data.setdefault("_tx_index", {})
            idx_root[scheme_name] = {}
            self._save()
            return True

        idx = {}

        prev = None
        balances = {}

        for i, tx in enumerate(txs):
            # parent/child
            tx["parent"] = prev["uid"] if prev else None
            if prev:
                prev["child"] = tx["uid"]
            tx["child"] = None  # выставим после, когда будет следующий

            # normalize period key for fast queries
            tx["period_key"] = normalize_date_key(tx.get("period")) or tx.get("period_key")

            # пересчёт balances
            keys = tx.get("keys") or []
            values = tx.get("values") or []
            key_str = "::".join(str(k) for k in keys)

            if key_str not in balances:
                balances[key_str] = [0] * len(values)

            # защитимся от несовпадения длин
            min_len = min(len(balances[key_str]), len(values))
            new_vec = list(balances[key_str])
            for j in range(min_len):
                new_vec[j] = new_vec[j] + values[j]
            # если values длиннее — “дорастим”
            if len(values) > len(new_vec):
                new_vec.extend(values[len(new_vec):])
            balances[key_str] = new_vec

            tx["balances"] = copy.deepcopy(balances)

            # prev_hash/hash
            tx["prev_hash"] = prev["hash"] if prev else None
            tx["hash"] = hashlib.sha256(
                f"{tx['uid']}{tx['parent']}{tx['balances']}{tx.get('period')}".encode()
            ).hexdigest()

            # индекс по dedup_key (если есть) или вычислим из meta/полей
            meta = tx.get("meta") or {}
            dk = meta.get("dedup_key")
            if not dk:
                dk = self._tx_dedup_key(
                    scheme_name,
                    str(tx.get("period") or ""),
                    keys,
                    source_uid=meta.get("source_uid") or (self._data.get("_id") or self._id),
                )
                meta["dedup_key"] = dk
                tx["meta"] = meta
            idx[dk] = tx["uid"]

            prev = tx

        # закрыть child у последней
        if txs:
            txs[-1]["child"] = None

        self._data.setdefault("_transactions", {})[scheme_name] = txs
        self._data.setdefault("_tx_index", {})[scheme_name] = idx
        self._save()
        return True

    def _remove_sum_transaction_unique(self, scheme_name: str, *, unique_key: str) -> bool:
        if self._sql_balance_enabled():
            self._sql_balance_import_legacy(scheme_name)
            return _BALANCE_SQL.remove_sum_transaction_unique(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                scheme_name=str(scheme_name or ""),
                unique_key=str(unique_key or ""),
            )

        txs = list(self._data.get("_transactions", {}).get(scheme_name, []) or [])
        # ensure stable ordering by period_key then uid
        try:
            txs.sort(key=lambda t: ((t.get("period_key") or normalize_date_key(t.get("period")) or ""), str(t.get("uid") or "")))
        except Exception:
            pass
        if not txs:
            return False

        new_txs = [t for t in txs if t.get("uk") != unique_key]
        if len(new_txs) == len(txs):
            return False  # ничего не удалили

        self._data.setdefault("_transactions", {})[scheme_name] = new_txs
        self._rebuild_sum_transactions(scheme_name)  # пересчёт parent/child/balances/hash
        self._save()
        return True
    
    def _sum_transaction_unique(
        self,
        scheme_name: str,
        *,
        unique_key: str,
        period: str,
        keys: list,
        values: list,
        meta: dict | None = None,
    ) -> str | None:
        """
        Добавляет транзакцию только если unique_key ещё не встречался.
        Возвращает uid существующей/новой транзакции.
        """

        if not unique_key:
            raise ValueError("unique_key is required")

        if self._sql_balance_enabled():
            self._sql_transaction_import_legacy(scheme_name, "sum")
            return _BALANCE_SQL.sum_transaction_unique(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                class_name=self.__class__.__name__,
                scheme_name=str(scheme_name or ""),
                unique_key=str(unique_key or ""),
                period=period,
                keys=list(keys or []),
                values=list(values or []),
                meta=dict(meta or {}),
            )

        txs = self._data.setdefault("_transactions", {}).setdefault(scheme_name, [])

        # 1) dedup check
        existing = next((t for t in txs if t.get("uk") == unique_key), None)
        if existing:
            return existing["uid"]

        # 2) обычное добавление (как в твоём _sum_transaction)
        last_tx = txs[-1] if txs else None
        parent_id = last_tx["uid"] if last_tx else None

        balances = last_tx["balances"].copy() if last_tx else {}

        key_str = "::".join(str(k) for k in (keys or []))
        if key_str not in balances:
            balances[key_str] = [0] * len(values)
        balances[key_str] = [old + delta for old, delta in zip(balances[key_str], values)]

        uid = str(uuid.uuid4())
        prev_hash = last_tx["hash"] if last_tx else None
        tx_hash = hashlib.sha256(f"{uid}{parent_id}{balances}{period}".encode()).hexdigest()

        tx = {
            "uid": uid,
            "uk": unique_key,     # <-- ВОТ ОН, тех. уникальный ключ
            "parent": parent_id,
            "child": None,
            "period": period,
            "period_key": normalize_date_key(period),
            "keys": keys,
            "values": values,
            "balances": balances,
            "hash": tx_hash,
            "prev_hash": prev_hash,
            "meta": dict(meta or {}),  # meta остаётся описанием (накладная и т.п.)
        }

        if last_tx:
            last_tx["child"] = uid

        txs.append(tx)
        self._data["_transactions"][scheme_name] = txs
        self._save()
        return uid
    def _sum_transaction(self, scheme_name, period=None, keys=None, values=None, meta=None):
        
        #schemes = self.__class__._get_schemes()
        #if scheme_name not in schemes:
        #    raise ValueError(f"Схема '{scheme_name}' не найдена для {self.__class__.__name__}. "
        #                     f"Зарегистрируй через {self.__class__.__name__}._add_scheme(...)")

        if keys is None:
            keys = []
        if values is None:
            values = []

        
        if period is None:
            period = datetime.now().strftime("%Y-%m-%d")

        if self._sql_balance_enabled():
            self._sql_transaction_import_legacy(scheme_name, "sum")
            return _BALANCE_SQL.sum_transaction(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                class_name=self.__class__.__name__,
                scheme_name=str(scheme_name or ""),
                period=period,
                keys=list(keys or []),
                values=list(values or []),
                meta=dict(meta or {}),
            )

        txs = self._data.setdefault("_transactions", {}).setdefault(scheme_name, [])
        last_tx = txs[-1] if txs else None
        parent_id = last_tx["uid"] if last_tx else None

        # We take past balances
        balances = last_tx["balances"].copy() if last_tx else {}

        # Generating an analytics key
        key_str = "::".join(str(k) for k in keys)
        if key_str not in balances:
            balances[key_str] = [0] * len(values)

        # Updating the balance
        balances[key_str] = [old + delta for old, delta in zip(balances[key_str], values)]

        # uid и hash
        uid = str(uuid.uuid4())
        prev_hash = last_tx["hash"] if last_tx else None
        tx_hash = hashlib.sha256(
            f"{uid}{parent_id}{balances}{period}".encode()
        ).hexdigest()

        tx = {
            "uid": uid,
            "parent": parent_id,
            "child": None,
            "period": period,
            "period_key": normalize_date_key(period),
            "keys": keys,
            "values": values,
            "balances": balances,
            "hash": tx_hash,
            "prev_hash": prev_hash,
            "meta": meta or {}
        }

        # Close the child of the previous one
        if last_tx:
            last_tx["child"] = uid

        txs.append(tx)
        self._data["_transactions"][scheme_name] = txs
        self._save()
        return uid

    def _get_balance(self, scheme_name, date=None):
            """Returns balances for scheme at a specific date (inclusive).
            If date is None -> last balances.
            Date can be 'YYYY-MM-DD', 'YYYYMMDD' or ISO datetime.
            """
            if self._sql_balance_enabled():
                self._sql_transaction_import_legacy(scheme_name, "sum")
                return _BALANCE_SQL.get_balance(
                    owner_id=self._sql_balance_owner_id(),
                    config_uid=self._sql_balance_config_uid(),
                    scheme_name=str(scheme_name or ""),
                    date=date,
                )

            txs = self._data.get("_transactions", {}).get(scheme_name, [])
            if not txs:
                return {}
            if date is None:
                return txs[-1].get("balances") or {}

            dk = normalize_date_key(date)
            if not dk:
                # fallback to string compare on period
                target = str(date)
                lo, hi, idx = 0, len(txs) - 1, -1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    p = str(txs[mid].get("period") or "")
                    if p <= target:
                        idx = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1
                return (txs[idx].get("balances") or {}) if idx >= 0 else {}

            lo, hi, idx = 0, len(txs) - 1, -1
            while lo <= hi:
                mid = (lo + hi) // 2
                pkey = txs[mid].get("period_key") or normalize_date_key(txs[mid].get("period"))
                if not pkey:
                    pkey = "00000000"
                if pkey <= dk:
                    idx = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            return (txs[idx].get("balances") or {}) if idx >= 0 else {}

    def _get_sum_transactions(self, scheme_name):
        """Returns the full chain of transactions according to the scheme"""
        if self._sql_balance_enabled():
            self._sql_transaction_import_legacy(scheme_name, "sum")
            return _BALANCE_SQL.get_sum_transactions(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                scheme_name=str(scheme_name or ""),
            )
        return self._data.get("_transactions", {}).get(scheme_name, [])
    
    def _state_transaction(self, scheme_name, period=None, keys=None, values=None, meta=None):
        """Adds a state transaction to the specified schema (does not sum, but sets values)"""
        if keys is None:
            keys = []
        if values is None:
            values = []

        
        if period is None:
            period = datetime.now().strftime("%Y-%m-%d")

        if self._sql_balance_enabled():
            self._sql_transaction_import_legacy(scheme_name, "state")
            return _BALANCE_SQL.state_transaction(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                class_name=self.__class__.__name__,
                scheme_name=str(scheme_name or ""),
                period=period,
                keys=list(keys or []),
                values=list(values or []),
                meta=dict(meta or {}),
            )

        txs = self._data.setdefault("_state_transactions", {}).setdefault(scheme_name, [])
        last_tx = txs[-1] if txs else None
        parent_id = last_tx["uid"] if last_tx else None

        
        key_str = "::".join(str(k) for k in keys)
        
        
        current_state = {key_str: values.copy()}

        # uid и hash
        uid = str(uuid.uuid4())
        prev_hash = last_tx["hash"] if last_tx else None
        tx_hash = hashlib.sha256(
            f"{uid}{parent_id}{current_state}{period}".encode()
        ).hexdigest()

        tx = {
            "uid": uid,
            "parent": parent_id,
            "child": None,
            "period": period,
            "period_key": normalize_date_key(period),
            "keys": keys,
            "values": values,
            "state": current_state,  
            "hash": tx_hash,
            "prev_hash": prev_hash,
            "meta": meta or {}
        }

        
        if last_tx:
            last_tx["child"] = uid

        txs.append(tx)
        self._data["_state_transactions"][scheme_name] = txs
        self._save()
        return uid

    def _get_state_balance(self, scheme_name, date=None):
            """Returns state snapshot for scheme at a specific date (inclusive).
            If date is None -> last state.
            Date can be 'YYYY-MM-DD', 'YYYYMMDD' or ISO datetime.
            """
            if self._sql_balance_enabled():
                self._sql_transaction_import_legacy(scheme_name, "state")
                return _BALANCE_SQL.get_state_balance(
                    owner_id=self._sql_balance_owner_id(),
                    config_uid=self._sql_balance_config_uid(),
                    scheme_name=str(scheme_name or ""),
                    date=date,
                )

            txs = self._data.get("_state_transactions", {}).get(scheme_name, [])
            if not txs:
                return {}
            if date is None:
                return txs[-1].get("state") or {}

            dk = normalize_date_key(date)
            if not dk:
                target = str(date)
                lo, hi, idx = 0, len(txs) - 1, -1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    p = str(txs[mid].get("period") or "")
                    if p <= target:
                        idx = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1
                return (txs[idx].get("state") or {}) if idx >= 0 else {}

            lo, hi, idx = 0, len(txs) - 1, -1
            while lo <= hi:
                mid = (lo + hi) // 2
                pkey = txs[mid].get("period_key") or normalize_date_key(txs[mid].get("period"))
                if not pkey:
                    pkey = "00000000"
                if pkey <= dk:
                    idx = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            return (txs[idx].get("state") or {}) if idx >= 0 else {}

    def _get_state_transactions(self, scheme_name):
        """Returns the complete chain of transactions for the state of the schema"""
        if self._sql_balance_enabled():
            self._sql_transaction_import_legacy(scheme_name, "state")
            return _BALANCE_SQL.get_state_transactions(
                owner_id=self._sql_balance_owner_id(),
                config_uid=self._sql_balance_config_uid(),
                scheme_name=str(scheme_name or ""),
            )
        return self._data.get("_state_transactions", {}).get(scheme_name, [])
        
    def __str__(self):
        return f"{self.__class__.__name__}(id={self._id})"
    
    def __repr__(self):
        return self.__str__()
    
    def AddChild(self, child_class, child_id=None, child_data=None):
        """
        Add a child node.
        child_class: Node class OR string (logical class name from config)
        """
        with self._lock:
            child_cls = self._resolve_node_class(child_class)
            if child_cls is None:
                raise ValueError(
                    f"Unknown child class: {child_class!r}. Known: {sorted(_NODE_CLASS_REGISTRY.keys())}"
                )
            
            child_node = child_cls(child_id, self._config_uid)
            
            schema_name = None
            if isinstance(child_class, str) and child_class.strip():
                schema_name = child_class.strip()
                child_node._schema_class_name = schema_name
            
            # Получаем текущий список детей в нужном формате
            children_data = self._data.setdefault("_children", {})
            
            # Если children_data - список (старый формат), конвертируем в новый формат
            if isinstance(children_data, list):
                # Конвертируем старый формат в новый
                new_children = {}
                for child in children_data:
                    if isinstance(child, dict):
                        child_class_name = child.get("class", child.get("_class", ""))
                        child_id_value = child.get("id", child.get("_id", ""))
                        if child_class_name and child_id_value:
                            key = f"{child_class_name}${child_id_value}"
                            # Значение - полный uid в новом формате
                            value = normalize_own_uid(self._config_uid, child_class_name, child_id_value)
                            new_children[key] = value
                children_data = new_children
                self._data["_children"] = children_data
            
            # Добавляем нового ребенка в новом формате
            key = f"{child_cls.__name__}${child_node._id}"
            value = normalize_own_uid(child_node._config_uid, child_cls.__name__, child_node._id)
            children_data[key] = value
            
            # Устанавливаем родителя в данных ребенка
            if "_id" in self._data:
                child_node._data["_parent"] = self._data.get("_id")
            else:    
                child_node._data["_parent"] = self._id
            
            if child_data:
                child_node.update_data(child_data)
            
            
            self._save()
            child_node._save()
            return child_node




    def RemoveChild(self, child_id):
        with self._lock:
            children_data = self._data.get("_children", [])
            
            # Новый формат (dict)
            if isinstance(children_data, dict):
                # Ищем ключи, которые заканчиваются на указанный child_id
                keys_to_remove = []
                internal = extract_internal_id(child_id)
                for key in children_data.keys():
                    if key.endswith(f"${internal}"):
                        keys_to_remove.append(key)
                
                # Удаляем найденные ключи
                for key in keys_to_remove:
                    del children_data[key]
                
                self._data["_children"] = children_data
            
            # Старый формат (list)
            elif isinstance(children_data, list):
                # Фильтруем список, оставляя только тех детей, у которых id не совпадает
                children_data = [
                    child for child in children_data 
                    if isinstance(child, dict) and 
                    child.get("id") != child_id and 
                    child.get("_id") != child_id
                ]
                self._data["_children"] = children_data
            
            self._save()

    def GetChildren(self, level=None):
        with self._lock:
            children_data = self._data.get("_children", []) or []
            children_nodes = []
            
            # Обработка нового формата (dict)
            if isinstance(children_data, dict):
                for key, value in children_data.items():
                    # key: "ClassName$nodeId"
                    # value: "config_uid$ClassName$nodeId"
                    
                    # Разбираем ключ или значение
                    parts = key.split("$")
                    if len(parts) == 2:
                        child_class_name = parts[0]
                        child_id = parts[1]
                    elif len(parts) == 3:
                        child_class_name = parts[1]
                        child_id = parts[2]
                    else:
                        # Пробуем разобрать значение
                        value_parts = value.split("$")
                        if len(value_parts) >= 3:
                            child_class_name = value_parts[-2]
                            child_id = value_parts[-1]
                        else:
                            continue
                    
                    child_cls = self._resolve_node_class(child_class_name)
                    if child_cls is None:
                        continue
                    

                    child_node = child_cls.get(child_id, self._config_uid)
                    if child_node is not None:
                        children_nodes.append(child_node)
            
            # Обработка старого формата (list)
            elif isinstance(children_data, list):
                for child_info in children_data:
                    if not isinstance(child_info, dict):
                        continue
                        
                    child_id = child_info.get("id") or child_info.get("_id")
                    child_class_name = child_info.get("class") or child_info.get("_class")
                    
                    if not child_id or not child_class_name:
                        continue
                    
                    child_cls = self._resolve_node_class(child_class_name)
                    if child_cls is None:
                        continue
                    
                    child_node = child_cls.get(child_id, self._config_uid)
                    if child_node is not None:
                        children_nodes.append(child_node)
            
            return children_nodes
                

    
    def PlugIn(self, plugins):
        """Request client-side plugins (e.g. BarcodeScanner).

        Example:
            self.PlugIn([{"type":"BarcodeScanner","id":"barcode_scan"}])

        Notes:
          - Stored as one-shot UI hint in `_ui_plugins`
          - Web client can use it to route scanner events to onInputWeb with listener=id
        """
        try:
            if not isinstance(plugins, list):
                return False
            norm = []
            for it in plugins:
                if isinstance(it, dict):
                    t = str(it.get("type") or "").strip()
                    pid = str(it.get("id") or it.get("listener") or "").strip()
                    if t:
                        d = dict(it)
                        if pid:
                            d["id"] = pid
                        d["type"] = t
                        norm.append(d)
            self._ui_plugins = norm
            return True
        except Exception:
            return False

    def Show(self, layout):
            """
            Server-side Show() for web client:
            handlers can call self.Show(layout), and web client will render it via nodalayout.py
            """
            self._ui_layout = layout
            return True

    def Progress(self, current=None, total=None, text: str = "", progress=None):
        """Update progress of the current runprogress/runasync background job.

        It is a no-op when the method is executed synchronously, so handlers may
        call it without checking their execution mode.
        """
        if _background_jobs is not None:
            try:
                _background_jobs.update_progress(current=current, total=total, text=text, progress=progress)
            except Exception:
                pass
        return True

    def CancelRequested(self) -> bool:
        """Return True when the user requested cancellation of this job."""
        if _background_jobs is None:
            return False
        try:
            return bool(_background_jobs.cancel_requested())
        except Exception:
            return False

    def RaiseIfCancelled(self):
        if _background_jobs is not None:
            _background_jobs.raise_if_cancelled()
        return True

    def Message(self, text: str, level: str = "info"):
        """Request a top message popup in the web client."""
        try:
            msgs = getattr(self, "_ui_message", None)
            if not isinstance(msgs, list):
                msgs = []
            msgs.append({"text": str(text), "level": str(level or "info")})
            self._ui_message = msgs
        except Exception:
            pass

    def Dialog(self, dialog_id: str, title: str = "", *, positive: str = "OK", negative: str = "Cancel", layout=None, html: str = ""):
        """Request a dialog in the web client.

        dialog_id is used to generate listeners:
          <dialog_id>_positive / <dialog_id>_negative
        """
        self._ui_dialog = {
            "id": str(dialog_id or "dialog"),
            "title": str(title or ""),
            "positive": str(positive or "OK"),
            "negative": str(negative or "Cancel"),
            "layout": layout,
            "html": html,
        }


    def RunProjection(self):
        """Ask the web client to run the current projection again."""
        self._ui_run_projection = True
        return True

    def BreakNodes(self, nodes_or_ids, title: str = "Debug nodes"):
        """Show nodes/ids in a modal node-list and stop current web handler.

        This is a web-client debugging helper. It cannot resume the Python call
        stack after the user closes the modal, so it intentionally raises
        UiBreak and terminates the current handler response.
        """
        raise UiBreak({"title": str(title or "Debug nodes"), "items": to_uid(nodes_or_ids)})

    def DebugBreakNodes(self, nodes_or_ids, title: str = "Debug nodes"):
        return self.BreakNodes(nodes_or_ids, title)




# --- Compatibility helpers (LLM.txt style) ---
# Some generated handlers call message(...) / Dialog(...) as free functions.
CURRENT_NODE = None


def message(text: str, level: str = "info"):
    n = globals().get("CURRENT_NODE")
    if n is not None and hasattr(n, "Message"):
        try:
            # Store one-shot UI hint on node
            r = n.Message(text, level)
            # Also store in runtime messages so API/save responses can surface it
            try:
                push_message(text, level)
            except Exception:
                pass
            return r
        except Exception:
            return None
    return None


def Dialog(dialog_id: str, title: str = "", positive: str = "OK", negative: str = "Cancel", layout=None, html: str = ""):
    n = globals().get("CURRENT_NODE")
    if n is not None and hasattr(n, "Dialog"):
        try:
            return n.Dialog(dialog_id, title, positive=positive, negative=negative, layout=layout, html=html)
        except Exception:
            return None
    return None


def RunProjection():
    n = globals().get("CURRENT_NODE")
    if n is not None and hasattr(n, "RunProjection"):
        try:
            return n.RunProjection()
        except Exception:
            return None
    return None

def BreakNodes(nodes_or_ids, title: str = "Debug nodes"):
    raise UiBreak({"title": str(title or "Debug nodes"), "items": to_uid(nodes_or_ids)})

def DebugBreakNodes(nodes_or_ids, title: str = "Debug nodes"):
    return BreakNodes(nodes_or_ids, title)

def Break(nodes_or_ids, title: str = "Debug nodes"):
    return BreakNodes(nodes_or_ids, title)

def CloseNode():
    n = globals().get("CURRENT_NODE")
    if n is not None and hasattr(n, "CloseNode"):
        try:
            return n.CloseNode()
        except Exception:
            return None
    return None    

def to_uid(nodes_list):
    """Return normalized node UIDs for projection/contract lists.

    Accepts:
      - a list/tuple/set of Node objects;
      - a dict returned by Node.get_all(): {node_id: Node(...)};
      - a single Node object;
      - strings with already prepared UIDs;
      - dict records like {"_id": ..., "_class": ..., "_config_uid": ...}.

    This helper is intentionally permissive because generated business handlers
    often pass get_all() directly, and get_all() returns a mapping, not a list.
    """
    def _items(value):
        if value is None:
            return []
        if isinstance(value, dict):
            # A single serialized node dict has an id-like field. Otherwise it is
            # most likely the {id: node} mapping returned by get_all().
            if any(k in value for k in ("uid", "_uid", "_id", "id")):
                return [value]
            return list(value.values())
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return [value]

    def _add(uid):
        uid = str(uid or "").strip()
        if uid and uid not in seen:
            seen.add(uid)
            out.append(uid)

    out = []
    seen = set()
    for n in _items(nodes_list):
        if n is None:
            continue

        # Already normalized / plain uid strings.
        if isinstance(n, str):
            _add(n)
            continue

        # Serialized node-like dict.
        if isinstance(n, dict):
            raw_id = n.get("uid") or n.get("_uid") or n.get("_id") or n.get("id")
            if not raw_id:
                continue
            try:
                uid_cfg, uid_cls, uid_id = parse_uid_any(raw_id)
            except Exception:
                uid_cfg, uid_cls, uid_id = None, None, raw_id
            cls = (
                n.get("_schema_class_name")
                or n.get("_class_name")
                or n.get("_class")
                or n.get("class")
                or uid_cls
            )
            cfg = n.get("_config_uid") or n.get("config_uid") or uid_cfg or current_config_uid_from_handlers()
            if cls:
                _add(normalize_own_uid(cfg or "", cls, uid_id or raw_id))
            else:
                _add(raw_id)
            continue

        # Live Node instance or node-like object.
        raw_id = getattr(n, "_id", None) or getattr(n, "id", None)
        if not raw_id:
            continue
        cls = getattr(n, "_schema_class_name", None) or getattr(n, "_class_name", None) or n.__class__.__name__
        cfg = getattr(n, "_config_uid", None) or getattr(n, "config_uid", None) or current_config_uid_from_handlers()
        _add(normalize_own_uid(cfg or "", cls, raw_id))
    return out

def parse_uid(uid: str):
    s = str(uid or "")
    if "$" in s:
        parts = str(uid).split("$")
        if len(parts) >= 3:
            # cfg$Class$Id
            return parts[-2], parts[-1]
        if len(parts) == 2:
            # Class$Id
            return parts[0], parts[1]
        # Id only
        return None, parts[0]
        return cls.strip(), nid.strip()
    return "", s.strip()



# def extract_internal_id(raw_id: str) -> str:
#     """
#     Accepts: "100" | "Class$100" | "cfg$Class$100"
#     Returns internal storage id: "100"
#     """
#     if raw_id is None:
#         return None
#     s = str(raw_id)
#     parts = s.split("$")
#     if len(parts) >= 3:
#         return parts[-1]
#     if len(parts) == 2:
#         return parts[1]
#     return s


def normalize_own_uid(config_uid: str, class_name: str, raw_id: str) -> str:
    """
    Returns normalized uid: "cfg$Class$100"
    """
    internal = extract_internal_id(raw_id)
    if internal is None:
        return None
    return f"{config_uid}${class_name}${internal}"

def normalize_date_key(date_str):
    """Normalize date-like string into 'YYYYMMDD' (8 chars) or return None.
    Accepts:
      - 'YYYY-MM-DD'
      - 'YYYYMMDD'
      - ISO datetime 'YYYY-MM-DDTHH:MM:SS...'
    """
    if date_str is None:
        return None
    s = str(date_str).strip()
    if not s:
        return None
    if "T" in s:
        s = s.split("T", 1)[0].strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        y, m_, d = s[0:4], s[5:7], s[8:10]
        if y.isdigit() and m_.isdigit() and d.isdigit():
            return y + m_ + d
        return None
    if len(s) == 8 and s.isdigit():
        return s
    return None




def parse_uid_any(uid):
    """
    Returns tuple: (uid_config, class_name, internal_id)

    Accepts:
      - "cfg$Class$Id"
      - "Class$Id"
      - "Id"
      - dict forms: {"_id": "...", "_class": "..."} or {"id": "...", "class": "..."}
    """
    if uid is None:
        return None, None, None

    # dict support
    if isinstance(uid, dict):
        raw_id = uid.get("_id") or uid.get("id")
        raw_class = uid.get("_class") or uid.get("class")
        # If raw_id itself can be composite, parse it too, but class from dict wins if present.
        c_uid, c_cls, c_id = parse_uid_any(raw_id)
        return c_uid, (raw_class or c_cls), c_id

    s = str(uid)
    parts = s.split("$")
    if len(parts) >= 3:
        # cfg$Class$Id  (if there are more, we still take last two as class/id)
        return parts[0], parts[-2], parts[-1]
    if len(parts) == 2:
        # Two-part form can be either "Class$Id" OR shorthand "cfg$Class" for singleton.
        # Heuristic: if the first part looks like a UUID -> treat as config uid.
        if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", parts[0]):
            return parts[0], parts[1], "singleton"
        # Otherwise it's the classic "Class$Id"
        return None, parts[0], parts[1]
    # Id only
    return None, None, parts[0]


def extract_internal_id(raw_id: str) -> str:
    """
    "100" | "Class$100" | "cfg$Class$100" -> "100"
    """
    if raw_id is None:
        return None
    return parse_uid_any(raw_id)[2]


def _resolve_node_class(config_info, class_name):
    """
    Tries to extract the Python Node class for `class_name` from config_info.

    Supports a few common shapes:
      - config_info["classes"][class_name] is the class itself
      - config_info["classes"][class_name] is dict with keys: "class", "node_class", "cls"
      - config_info["classes"][class_name] is object with attrs: class_/node_class/cls
    """
    if not config_info:
        return None

    classes = None
    if isinstance(config_info, dict):
        classes = config_info.get("classes") or config_info.get("Classes") or config_info.get("node_classes")
    else:
        classes = getattr(config_info, "classes", None) or getattr(config_info, "node_classes", None)

    if not classes:
        return None

    entry = classes.get(class_name) if isinstance(classes, dict) else None
    if entry is None:
        return None

    # entry may already be the class
    if isinstance(entry, type):
        return entry

    # entry may be dict
    if isinstance(entry, dict):
        return entry.get("node_class") or entry.get("class") or entry.get("cls")

    # entry may be object
    return getattr(entry, "node_class", None) or getattr(entry, "class", None) or getattr(entry, "cls", None)


def from_uid(uid, config_uid, config_info):
    """
    Resolve uid to a Node instance.

    - uid can be: "cfg$Class$Id", "Class$Id", "Id", or dict with _id/_class.
    - config_uid argument has priority. If config_uid is None, uid's own config part is used.
    - If class is missing, tries to find the node by scanning all classes in config_info.
    """
    uid_cfg, cls_name, internal_id = parse_uid_any(uid)
    if internal_id is None:
        return None

    effective_config_uid = config_uid or uid_cfg

    # 1) If we have class name -> resolve class and get node
    if cls_name:
        node_class = _resolve_node_class(config_info, cls_name)
        if node_class is None:
            raise KeyError(f"Unknown class '{cls_name}' in uid '{uid}'")
        return node_class.get(internal_id, effective_config_uid)

    # 2) No class -> scan all known classes and find first where node exists
    #    This keeps backward compatibility with "Id only"
    classes = None
    if isinstance(config_info, dict):
        classes = config_info.get("classes") or config_info.get("Classes") or config_info.get("node_classes") or {}
    else:
        classes = getattr(config_info, "classes", None) or getattr(config_info, "node_classes", None) or {}

    if isinstance(classes, dict):
        class_names = list(classes.keys())
    else:
        # if somehow it's not dict, we can't scan
        class_names = []

    for cn in class_names:
        node_class = _resolve_node_class(config_info, cn)
        if not node_class:
            continue
        try:
            node = node_class.get(internal_id, effective_config_uid)
            if node is not None:
                return node
        except Exception:
            # some get() implementations may raise if not found; ignore and continue
            continue

    # Not found
    return None



import os, inspect
from contextvars import ContextVar
from typing import Any, Dict, Optional, Tuple

DATASET_VIEW_CACHE = ContextVar("DATASET_VIEW_CACHE", default=None)  # (cfg_uid, ds_name, item_id)->str
DATASET_OBJ_CACHE  = ContextVar("DATASET_OBJ_CACHE", default=None)   # (cfg_uid, ds_name, item_id)->dict|None


def current_config_uid_from_handlers() -> str:
    try:
        for fi in inspect.stack():
            fp = ""
            try:
                fp = fi.frame.f_globals.get("__file__", "") or ""
            except Exception:
                fp = ""
            if fp and (os.sep + "Handlers" + os.sep) in fp and fp.endswith(os.sep + "handlers.py"):
                return os.path.basename(os.path.dirname(fp))
    except Exception:
        pass
    try:
        return (CURRENT_CONFIG_UID.get() or "").strip()
    except Exception:
        return ""


class DataSets:
    class Dataset:
        def __init__(self, name: str):
            self.name = str(name or "").strip()

        def get(self, item_id: str) -> Optional[Dict[str, Any]]:
            """goods.get('123') -> dataset item object or None"""
            item_id = str(item_id or "").strip()
            if not self.name or not item_id:
                return None
            return DataSets.getObject(f"{self.name}${item_id}")

        def view(self, item_id: str) -> str:
            """goods.view('123') -> view string"""
            item_id = str(item_id or "").strip()
            if not self.name or not item_id:
                return item_id
            return DataSets.getView(f"{self.name}${item_id}")

    @staticmethod
    def GetDataSet(name: str) -> "DataSets.Dataset":
        return DataSets.Dataset(name)

    @staticmethod
    def getView(uid: str) -> str:
        obj = DataSets.getObject(uid)
        if obj and isinstance(obj, dict):
            return str(obj.get("_view") or obj.get("_id") or uid)
        return str(uid or "")

    @staticmethod
    def getObject(uid: str) -> Optional[Dict[str, Any]]:
        """
        uid: 'DatasetName$item_id'
        Returns {"_id","_view","_data","_dataset"} or None.
        No HTTP. Uses config_uid from handlers path.
        """
        uid = str(uid or "").strip()
        if "$" not in uid:
            return None

        ds_name, item_id = uid.split("$", 1)
        ds_name, item_id = ds_name.strip(), item_id.strip()
        if not ds_name or not item_id:
            return None

        cfg_uid = current_config_uid_from_handlers()
        if not cfg_uid:
            return None

        oc = DATASET_OBJ_CACHE.get()
        if oc is None:
            oc = {}
            DATASET_OBJ_CACHE.set(oc)

        ck = (cfg_uid, ds_name, item_id)
        if ck in oc:
            return oc[ck]

        try:
            import __main__ as main
            Configuration = main.Configuration
            Dataset = main.Dataset
            DatasetItem = main.DatasetItem

            cfg = Configuration.query.filter_by(uid=cfg_uid).first()
            if not cfg:
                oc[ck] = None
                return None

            ds = Dataset.query.filter_by(config_id=cfg.id, name=ds_name).first()
            if not ds:
                oc[ck] = None
                return None

            item = DatasetItem.query.filter_by(dataset_id=ds.id, item_id=item_id).first()
            if not item:
                oc[ck] = None
                return None

            data = item.data or {}
            if not isinstance(data, dict):
                data = {}

            # build view from template
            view = ""
            tpl = (ds.view_template or "").strip()
            if tpl:
                import re
                pattern = r"{([A-Za-z0-9_]+)}"

                def repl(m):
                    k = m.group(1)
                    v = data.get(k, "")
                    return "" if v is None else str(v)

                view = re.sub(pattern, repl, tpl).strip()

            if not view:
                view = str(data.get("title") or data.get("name") or item_id)

            obj = {"_id": item_id, "_view": view, "_data": data, "_dataset": ds_name}

            # warm view cache too
            vc = DATASET_VIEW_CACHE.get()
            if vc is None:
                vc = {}
                DATASET_VIEW_CACHE.set(vc)
            vc[ck] = view

            oc[ck] = obj
            return obj

        except Exception:
            oc[ck] = None
            return None




def find_node_ids_by_index(class_or_name, index_name: str, value, config_uid=None):
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")
    return cls.find_ids_by_index(index_name, value, config_uid)


def findByIndex(class_or_name, index_name: str, value, config_uid=None):
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")
    return cls.find_by_index(index_name, value, config_uid)


def getByIndex(class_or_name, index_name: str, value, config_uid=None):
    cls = Node._resolve_node_class(class_or_name)
    if cls is None:
        raise ValueError(f"Unknown node class: {class_or_name}")
    return cls.get_by_index(index_name, value, config_uid)


def _resolve_node_by_global_uid(global_uid: str):
    cfg_uid, class_name, internal_id = parse_uid_any(global_uid)
    if not cfg_uid or not class_name or not internal_id:
        return None
    try:
        import __main__ as main
        Configuration = getattr(main, "Configuration", None)
        db = getattr(main, "db", None)
        loader = getattr(main, "_load_server_handlers_ns", None)
        if Configuration is None or db is None or loader is None:
            return None
        cfg = db.session.query(Configuration).filter(Configuration.uid == cfg_uid).first()
        if not cfg:
            return None
        isolated_globals = loader(cfg_uid, cfg) or {}
        node_class = isolated_globals.get(class_name)
        if node_class is None:
            return None
        return node_class.get(internal_id, cfg_uid)
    except Exception:
        return None


def findByGlobalIndex(index: str, value):
    store = Node._global_index_storage(index)
    if store is None:
        return {}
    try:
        ids = list(store.get(str(value), []) or [])
    except Exception:
        ids = []
    out = {}
    for uid in ids:
        obj = _resolve_node_by_global_uid(uid)
        if obj is not None:
            out[str(uid)] = obj
    return out


def getByGlobalIndex(index: str, value):
    found = findByGlobalIndex(index, value)
    for _uid, obj in found.items():
        return obj
    return None


find_by_global_index = findByGlobalIndex
get_by_global_index = getByGlobalIndex
find_by_index = findByIndex
get_by_index = getByIndex


_NODE_VIEW_TOKEN_RE = re.compile(r"\{([\w.]+)\}", re.UNICODE)


def _node_view_value(data, path):
    cur = data if isinstance(data, dict) else {}
    for part in str(path or "").split("."):
        if not isinstance(cur, dict) or part not in cur:
            return ""
        cur = cur.get(part)
    return "" if cur is None else str(cur)


def _node_view_class_config(config_uid, class_name):
    """Return class metadata for record_view without depending on the web client."""
    parsed = CURRENT_PARSED_CONFIG.get() or {}
    try:
        cfg = ((parsed.get("classes") or {}).get(class_name) or {}) if isinstance(parsed, dict) else {}
        if cfg:
            return cfg
    except Exception:
        pass

    # Cross-configuration/server fallback.  This keeps node_view useful in
    # ordinary handlers as well as in generated HTML projections.
    try:
        Configuration = None
        db_obj = None
        try:
            import __main__ as main
            Configuration = getattr(main, "Configuration", None)
            db_obj = getattr(main, "db", None)
        except Exception:
            pass
        # When Flask is started through an importable app module, __main__ does
        # not contain the SQLAlchemy models. Fall back to their real modules.
        if Configuration is None:
            from models import Configuration as ConfigurationModel
            Configuration = ConfigurationModel
        if db_obj is None:
            from extensions import db as db_obj
        config = db_obj.session.query(Configuration).filter(Configuration.uid == str(config_uid or "")).first()
        if not config:
            return {}
        for cls in (getattr(config, "classes", None) or []):
            if str(getattr(cls, "name", "") or "") != str(class_name or ""):
                continue
            return {
                "name": getattr(cls, "name", "") or "",
                "display_name": getattr(cls, "display_name", "") or "",
                "record_view": getattr(cls, "record_view", "") or "",
            }
    except Exception:
        pass
    return {}


def node_view(value, default=""):
    """Resolve a Node/NodeLink value to its normal class ``record_view``.

    Accepted values: Node, ``config_uid$Class$Id``, ``Class$Id`` or a mapping
    containing ``_id``/``id``/``uid``.  This helper is deliberately placed in
    ``nodes.py`` so server handlers, saved Projections and nGenie reports use the
    same representation instead of inventing report-local resolver functions.
    """
    explicit_data = None
    node = None
    raw = value
    if isinstance(value, dict):
        explicit = str(value.get("_view") or value.get("view") or "").strip()
        if explicit:
            return explicit
        explicit_data = value.get("_data") if isinstance(value.get("_data"), dict) else value
        raw = value.get("_id") or value.get("id") or value.get("uid") or ""
    elif hasattr(value, "get_data"):
        node = value
        raw = getattr(value, "_id", "") or ""

    raw = str(raw or "").strip()
    cfg_uid = ""
    class_name = ""
    internal_id = ""
    if node is not None:
        cfg_uid = str(getattr(node, "_config_uid", "") or "").strip()
        class_name = str(getattr(node, "_schema_class_name", "") or node.__class__.__name__ or "").strip()
        try:
            _uc, _cl, _iid = parse_uid_any(raw)
            internal_id = str(_iid or raw or "").strip()
            if _uc:
                cfg_uid = str(_uc)
            if _cl:
                class_name = str(_cl)
        except Exception:
            internal_id = raw
    else:
        try:
            uid_cfg, uid_cls, uid_id = parse_uid_any(raw)
            cfg_uid = str(uid_cfg or "").strip()
            class_name = str(uid_cls or "").strip()
            internal_id = str(uid_id or "").strip()
        except Exception:
            return str(default or raw)

    if not cfg_uid:
        cfg_uid = str(CURRENT_CONFIG_UID.get() or current_config_uid_from_handlers() or "").strip()
    if not class_name or not internal_id:
        return str(default or internal_id or raw)

    # _User nodes live in the owner's hidden _System configuration and are not
    # ordinary business classes/repositories. Resolve them through the Flask
    # platform bridge so NodeLink/NodeInput shows a user name instead of a raw UID.
    if class_name == "_User":
        try:
            resolved_user_view = _call_bridge('resolve_system_user_view_global', raw, default=default)
            if isinstance(resolved_user_view, str) and resolved_user_view.strip():
                return resolved_user_view.strip()
        except Exception:
            pass

    if node is None:
        try:
            cls = Node._resolve_node_class(class_name)
            if cls is not None:
                node = cls.get(internal_id, cfg_uid or None)
        except Exception:
            node = None
        if node is None and cfg_uid:
            try:
                node = _resolve_node_by_global_uid(normalize_own_uid(cfg_uid, class_name, internal_id))
            except Exception:
                node = None

    data = explicit_data if isinstance(explicit_data, dict) else {}
    if node is not None:
        try:
            loaded = node.get_data() or {}
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            try:
                loaded = getattr(node, "_data", None)
                if isinstance(loaded, dict):
                    data = loaded
            except Exception:
                pass

    explicit = str((data or {}).get("_view") or "").strip()
    if explicit:
        return explicit

    cls_cfg = _node_view_class_config(cfg_uid, class_name)
    template = str((cls_cfg or {}).get("record_view") or "").strip()
    if template:
        # The documented format is ``{field}``.  Support the simple ``@field``
        # spelling too because older Projection classes used it for their title.
        if template.startswith("@") and re.fullmatch(r"@[\w.]+", template):
            rendered = _node_view_value(data, template[1:]).strip()
        else:
            rendered = _NODE_VIEW_TOKEN_RE.sub(lambda m: _node_view_value(data, m.group(1)), template).strip()
        if rendered:
            return rendered

    # Friendly fallback for classes that have no explicit record_view.
    for key in ("name", "title", "caption", "number", "num", "code", "article"):
        text = str((data or {}).get(key) or "").strip()
        if text:
            return text
    return str(default or internal_id or raw)


def ngenie_nodes(value):
    """Normalize get_all()/lists/single nodes for report code."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            return ngenie_nodes(json.loads(text))
        except Exception:
            return []
    if isinstance(value, dict):
        if "_data" in value or "_id" in value or "_class" in value:
            return [value]
        return list(value.values())
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _hydrate_node_link_views(data):
    if not isinstance(data, dict):
        return {}
    out = dict(data)
    for key, value in list(data.items()):
        skey = str(key or "")
        if not skey or skey.startswith("_") or skey.endswith("_view"):
            continue
        raw = value
        if isinstance(value, dict):
            raw = value.get("_id") or value.get("id") or value.get("uid") or ""
        if not isinstance(raw, str) or "$" not in raw:
            continue
        try:
            _cfg, ref_class, ref_id = parse_uid_any(raw)
        except Exception:
            continue
        if not ref_class or not ref_id:
            continue
        view_key = skey + "_view"
        if not str(out.get(view_key) or "").strip():
            out[view_key] = node_view(raw, default=raw)
    return out


def ngenie_data(value):
    """Return a hydrated data dictionary from a Node/storage row/JSON string."""
    if value is None:
        return {}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            return ngenie_data(json.loads(text))
        except Exception:
            return {}
    if isinstance(value, dict):
        nested = value.get("_data")
        return _hydrate_node_link_views(nested if isinstance(nested, dict) else value)
    try:
        data = value.get_data()
        if isinstance(data, dict):
            return _hydrate_node_link_views(data)
    except Exception:
        pass
    try:
        data = getattr(value, "_data", None)
        if isinstance(data, dict):
            return _hydrate_node_link_views(data)
    except Exception:
        pass
    return {}


def ngenie_rows(value):
    """Normalize inline/Node-backed table values into hydrated row dicts."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except Exception:
            return []
    if isinstance(value, dict):
        if "_data" in value or "_id" in value or "_class" in value:
            values = [value]
        elif value and all(isinstance(v, (dict, str)) or hasattr(v, "get_data") for v in value.values()):
            values = list(value.values())
        else:
            values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    rows = []
    for item in values:
        row = ngenie_data(item)
        if row:
            rows.append(row)
    return rows


# Backward compatibility for reports generated by earlier nGenie builds.
ngenie_ref_view = node_view
get_node_view = node_view
NodeView = node_view

# --- Ephemeral dashboard banners and direct nGenie calls ---
_NGENIE_RUNNER = None

def set_ngenie_runner(fn):
    global _NGENIE_RUNNER
    _NGENIE_RUNNER = fn

def _current_config_uid():
    """Resolve the configuration which is currently executing the handler.

    CURRENT_NODE is not sufficient for CommonEvents/Timers because those use a
    lightweight UI host.  The runtime ContextVar and the handler module path are
    authoritative in those contexts.
    """
    try:
        uid = str(current_config_uid_from_handlers() or "").strip()
        if uid:
            return uid
    except Exception:
        pass
    try:
        uid = str(CURRENT_CONFIG_UID.get() or "").strip()
        if uid:
            return uid
    except Exception:
        pass
    n = globals().get("CURRENT_NODE")
    return str(getattr(n, "_config_uid", "") or getattr(n, "config_uid", "") or "").strip()

def banner(banner_id, value, size=0.25, background=None):
    # Keep the public command as NodaLayout, but give its Text element the
    # banner defaults on both runtimes: centered, bold and slightly larger.
    layout = [[{
        "type": "Text",
        "value": str(value),
        "width": -1,
        "height": -2,
        "gravity": "center",
        "bold": True,
        "size": 20,
        "padding": 8,
    }]]
    return banner_layout(banner_id, layout, size=size, background=background)

def banner_html(banner_id, value, size=0.25, background=None):
    from client_app.ephemeral_banners import put
    return put(_current_config_uid(), banner_id, "html", str(value), size, background)

def banner_layout(banner_id, value, size=0.25, background=None):
    from client_app.ephemeral_banners import put
    return put(_current_config_uid(), banner_id, "layout", value, size, background)

def close_banner(banner_id):
    from client_app.ephemeral_banners import close
    return close(_current_config_uid(), banner_id)

def ngenie(prompt, file_path=None):
    if not callable(_NGENIE_RUNNER):
        raise RuntimeError("nGenie runtime is not initialized")
    return _NGENIE_RUNNER(str(prompt or ""), file_path, _current_config_uid())
