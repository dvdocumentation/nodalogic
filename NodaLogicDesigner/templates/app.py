from flask import Flask, url_for, request, jsonify, abort, after_this_request, send_from_directory, g
from flask_login import login_required, current_user

from werkzeug.utils import secure_filename

from werkzeug.security import generate_password_hash, check_password_hash
import uuid
from sqlalchemy import select, text
import sqlalchemy as sa
import base64
import requests
import urllib.request
import urllib.error
from urllib.parse import urlparse, unquote
from datetime import datetime, timezone
import json
from flask_sockets import Sockets
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket import WebSocketError
from gevent.pywsgi import WSGIServer
from sqlitedict import SqliteDict
from collections import defaultdict
import pytz
import ast
import inspect
from pathlib import Path
import secrets
import socket
import threading
import hashlib
import boto3
from botocore.client import Config

print("APP LOAD:", __name__, __file__, id(object()))

try:
    import firebase_admin
    from firebase_admin import credentials as firebase_credentials, messaging as firebase_messaging
except Exception:
    firebase_admin = None
    firebase_credentials = None
    firebase_messaging = None

# ==================================================================
# Handlers loading (server): file-first, DB blob fallback
# Used to replace direct base64+exec blocks without changing endpoint logic.
# ==================================================================
def _handlers_file_path(config_uid: str) -> str:
    root_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root_dir, "Handlers", str(config_uid), "handlers.py")


def _load_server_handlers_ns(config_uid, config):
    """Return an isolated namespace with server node handlers.

    Priority:
      1) Handlers/<config_uid>/handlers.py (same as client approach)
      2) config.nodes_server_handlers (base64 blob) as fallback
    Returns an empty dict if nothing is available.
    """
    isolated_globals = {}

    # Make server messaging helpers available even in older handler files that
    # do not import them explicitly from nodes.py.  The actual implementation
    # stays in nodes.py and calls back into this app through bridge helpers.
    try:
        mod = globals().get('_nodes_mod')
        for _helper_name in (
            'sendTextMessage',
            'sendImageMessage',
            'sendTextToNodeDiscussion',
            'sendImageToNodeDiscussion',
            'downloadJsonCached',
            'downloadNodeCached',
            'dispatch_json_node_event',
            'dispatch_downloaded_node_event',
        ):
            _helper = getattr(mod, _helper_name, None) if mod is not None else None
            if callable(_helper):
                isolated_globals[_helper_name] = _helper
    except Exception:
        pass

    fp = _handlers_file_path(config_uid)
    try:
        if os.path.isfile(fp):
            with open(fp, "r", encoding="utf-8") as f:
                code = f.read()
            compiled = compile(code, fp, "exec")
            exec(compiled, isolated_globals)
            return isolated_globals
    except Exception:
        # Keep old behavior: endpoints will fall back to DB blob (or 404)
        pass

    try:
        if getattr(config, "nodes_server_handlers", None):
            code = base64.b64decode(config.nodes_server_handlers).decode("utf-8")
            compiled = compile(code, f"<db_handlers:{config_uid}>", "exec")
            exec(compiled, isolated_globals)
            return isolated_globals
    except Exception:
        # Keep old behavior: endpoints will handle errors as they did before
        pass

    return isolated_globals

import base64
from flask.json.provider import DefaultJSONProvider
import os
import time
import traceback
from functools import wraps
from urllib.parse import parse_qs
import logging
import re
from nodes import extract_internal_id 
import nodes as _nodes_mod

from extensions import db, login_manager
from models import (
    RawNode,
    NodeDiscussionMessage,
    Dataset,
    DatasetItem,
    Room,
    RoomDevice,
    RoomAlias,
    User,
    UserConfigAccess,
    UserDevice,
    ConfigEvent,
    ConfigEventAction,
    Configuration,
    ConfigSection,
    ConfigClass,
    ClassMethod,
    ClassEvent,
    EventAction,
    Contract,
    ContractObject,
    ContractAck,
    RoomObjects,
    OutgoingMessageLog,
    OutgoingMessageDeviceAck,
    MessageGroup,
    MessageGroupMember,
    Server,
    ApiToken,
    ensure_node_discussion_message_table_runtime,
)


logging.getLogger("geventwebsocket.handler").setLevel(logging.ERROR)
import ast
import inspect

#******************************************************************
#CHANGE IT WITH YOUR VALUES
DEEPSEEK_API_KEY = 'sk-85b64349ae5a4c48ba7a877463a01eec'
ADMIN_LOGIN = 'dv1555@hotmail.com'
FLASK_SECRET= 'ferret-26'

S3_ENDPOINT = "https://s3.ru1.storage.beget.cloud"
S3_BUCKET = "bf871c2d93ee-s3noda"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="6IQFBHS4BOEVXO5W5K6C",
    aws_secret_access_key="HXiBRr2qc1X8f34bEjZlfJGf4gAvWZNsfA52Abod",
    config=Config(signature_version="s3v4"),
    region_name="ru1",
)

SCRIPT_TEXT_METHODS = {"NodaScript", "PythonScript"}
HTTP_REQUEST_METHOD = "HTTP Request"

def _is_script_text_method(value):
    return (value or "") in SCRIPT_TEXT_METHODS

def _is_http_request_method(value):
    return (value or "") == HTTP_REQUEST_METHOD

def _s3_key_from_public_url(file_url: str):
    file_url = (file_url or "").strip()
    if not file_url:
        return ""
    parsed = urlparse(file_url)
    if not parsed.scheme and not parsed.netloc:
        return file_url.lstrip("/")
    endpoint = urlparse(S3_ENDPOINT)
    if parsed.netloc != endpoint.netloc:
        return ""
    path = unquote(parsed.path or "").lstrip("/")
    bucket_prefix = S3_BUCKET.strip("/") + "/"
    if path.startswith(bucket_prefix):
        return path[len(bucket_prefix):]
    return ""


# ------------------------------------------------------------------
# Runtime download cache for PythonScript source and JSON nodes/classes.
#
# PythonScript actions store an S3/public URL in methodText/postExecuteText.
# Raw JSON nodes also arrive by download_url.  Both are immutable enough in
# this flow (new uploads normally get a new URL), so we deliberately cache by
# URL and do not re-download unless force_refresh=True is passed explicitly.
# The cache is process-local + disk-backed to survive worker reloads.
# ------------------------------------------------------------------
_RUNTIME_DOWNLOAD_CACHE: dict[str, bytes] = {}
_RUNTIME_DOWNLOAD_CACHE_LOCK = threading.RLock()
_RUNTIME_DOWNLOAD_CACHE_DIR = os.environ.get(
    "NODA_RUNTIME_DOWNLOAD_CACHE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "runtime_cache", "downloads"),
)


def _runtime_cache_key(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _runtime_cache_path(value: str, suffix: str = ".bin") -> str:
    safe_suffix = suffix if str(suffix or "").startswith(".") else ".bin"
    return os.path.join(_RUNTIME_DOWNLOAD_CACHE_DIR, _runtime_cache_key(value) + safe_suffix)


def _runtime_cache_invalidate(value: str) -> None:
    """Drop one cached runtime-download entry from memory and disk."""
    value = str(value or "").strip()
    if not value:
        return
    cache_path = _runtime_cache_path(value)
    with _RUNTIME_DOWNLOAD_CACHE_LOCK:
        _RUNTIME_DOWNLOAD_CACHE.pop(value, None)
        for path in (cache_path, cache_path + ".tmp"):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


def _runtime_download_bytes_cached(url: str, *, timeout: int = 20, force_refresh: bool = False) -> bytes:
    """Download URL/S3 object once and then serve bytes from memory/disk cache."""
    url = str(url or "").strip()
    if not url:
        raise ValueError("download url is empty")

    cache_path = _runtime_cache_path(url)
    with _RUNTIME_DOWNLOAD_CACHE_LOCK:
        if not force_refresh and url in _RUNTIME_DOWNLOAD_CACHE:
            return _RUNTIME_DOWNLOAD_CACHE[url]
        if not force_refresh and os.path.isfile(cache_path):
            with open(cache_path, "rb") as f:
                data = f.read()
            _RUNTIME_DOWNLOAD_CACHE[url] = data
            return data

    key = _s3_key_from_public_url(url)
    if key:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
    else:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.content

    with _RUNTIME_DOWNLOAD_CACHE_LOCK:
        os.makedirs(_RUNTIME_DOWNLOAD_CACHE_DIR, exist_ok=True)
        tmp_path = cache_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, cache_path)
        _RUNTIME_DOWNLOAD_CACHE[url] = data
    return data


def _runtime_download_text_cached(url: str, *, timeout: int = 20, force_refresh: bool = False, encoding: str = "utf-8") -> str:
    return _runtime_download_bytes_cached(url, timeout=timeout, force_refresh=force_refresh).decode(encoding)


def _runtime_download_json_cached(url: str, *, timeout: int = 20, force_refresh: bool = False):
    return json.loads(_runtime_download_text_cached(url, timeout=timeout, force_refresh=force_refresh))


def _noda_load_python_script_code(script_ref: str, *, force_refresh: bool = False) -> str:
    """Bridge used by nodes.py: PythonScript ref may be URL/S3 URL or inline code."""
    ref = str(script_ref or "").strip()
    if not ref:
        return ""
    parsed = urlparse(ref)
    # Only explicit remote refs are downloaded. Do NOT call
    # _s3_key_from_public_url(ref) for arbitrary inline code because that helper
    # intentionally treats scheme-less strings as raw keys.
    if parsed.scheme in ("http", "https") or ref.startswith("uploads/python_scripts/"):
        return _runtime_download_text_cached(ref, force_refresh=force_refresh)
    return ref


def _raw_node_id_from_download_url(download_url: str) -> str:
    """Extract local raw-node id from absolute or relative /api/raw-node/<id> URL."""
    raw = str(download_url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        path = parsed.path or raw
    except Exception:
        path = raw
    for marker in ("/api/raw-node/", "api/raw-node/", "/raw-node/", "raw-node/"):
        if marker in path:
            return unquote(path.split(marker, 1)[-1].split("?", 1)[0].split("#", 1)[0]).strip()
    return ""


def _raw_node_payload_from_local_db(download_url: str):
    """Load local raw-node payload directly from DB instead of HTTP-calling this Flask app."""
    node_id = _raw_node_id_from_download_url(download_url)
    if not node_id:
        return None, False
    try:
        model = globals().get("RawNode")
        if model is None:
            return None, False
        obj = db.session.execute(select(model).where(model.node_id == str(node_id))).scalar_one_or_none()
        if obj is None:
            return None, True
        return (obj.payload_json or {}), True
    except Exception:
        return None, False


def _noda_download_json_cached(download_url: str, *, force_refresh: bool = False):
    """Bridge used by nodes.py for JSON nodes/classes loaded by download_url.

    Local raw-node URLs are read directly from RawNode, not through requests.get().
    This avoids auth/self-request issues and always sees the current DB payload
    after /api/raw-node/<id> POST or _save_raw_node_local().
    """
    url = str(download_url or "").strip()
    if force_refresh:
        _runtime_cache_invalidate(url)

    payload, handled = _raw_node_payload_from_local_db(url)
    if handled:
        if payload is None:
            raise FileNotFoundError(f"raw node not found: {_raw_node_id_from_download_url(url)}")
        return payload

    return _runtime_download_json_cached(url, force_refresh=force_refresh)

#******************************************************************




NL_FORMAT = "1.1"

DEFAULT_PUSH_GATEWAY_TOKEN = "I2YixHv7-5e5s2s45SWiQ2GPufGWkdz9Zn05DFY7Ip2wxRpI"
NMAKER_SERVER_URL = os.environ.get("NMAKER_SERVER_URL", "https://nmaker.pw").rstrip("/")
PUSH_GATEWAY_URL = os.environ.get("PUSH_GATEWAY_URL", "").rstrip("/")
PUSH_GATEWAY_TOKEN = os.environ.get("PUSH_GATEWAY_TOKEN", DEFAULT_PUSH_GATEWAY_TOKEN)
PUBLIC_API_BASE_URL = os.environ.get("PUBLIC_API_BASE_URL", "").rstrip("/")





pending_responses = {}

pending_remote_requests = defaultdict(dict)


def api_auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        user = None
        if auth:
            user = check_api_auth(auth.username, auth.password)
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401

        if not bool(getattr(user, 'can_api', False)):
            return jsonify({'error': 'Forbidden'}), 403

        cfg_uid = kwargs.get('config_uid') or kwargs.get('uid')
        if cfg_uid and not user_can_access_config(user, str(cfg_uid)):
            return jsonify({'error': 'Forbidden'}), 403

        g.api_user = user
        return f(*args, **kwargs)
    return decorated_function

def check_api_auth(username, password):

    user = db.session.execute(
        select(User).where(User.email == username)
    ).scalar_one_or_none()
    
    if user and check_password_hash(user.password, password):
        return user
    return None


def user_can_access_config(user: 'User', config_uid: str) -> bool:
    """Config is accessible if user owns it or it is explicitly shared to them."""
    if not user or not config_uid:
        return False
    cfg = db.session.execute(
        select(Configuration).where(Configuration.uid == str(config_uid))
    ).scalar_one_or_none()
    if not cfg:
        return False
    if cfg.user_id == user.id:
        return True
    return bool(
        db.session.execute(
            select(UserConfigAccess).where(
                UserConfigAccess.user_id == user.id,
                UserConfigAccess.config_id == cfg.id,
            )
        ).scalar_one_or_none()
    )


#Server functions
from sqlitedict import SqliteDict

STORAGE_BASE_PATH = 'node_storage'


os.makedirs(STORAGE_BASE_PATH, exist_ok=True)



app = Flask(__name__)
print("FLASK APP ID:", id(app))
print("DB ID:", id(db))
from editor_routes import init_editor_ui, get_default_server_handlers
babel = init_editor_ui(app)


sockets = Sockets(app)













class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault('ensure_ascii', False)
        kwargs.setdefault('indent', 4)
        return json.dumps(obj, **kwargs)

    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        return super().default(o)
app.json = CustomJSONProvider(app)

active_connections = defaultdict(dict)

#Node client

# Node browser WebSocket connections (separate channel from Rooms)
# Each item: {"ws": ws, "config_uid": str, "classes": set[str] | None}
node_ws_connections = []

def _cleanup_node_ws():
    """Remove closed/broken node WS connections."""
    global node_ws_connections
    alive = []
    for c in node_ws_connections:
        ws = c.get("ws")
        try:
            if ws is not None and not ws.closed:
                alive.append(c)
        except Exception:
            pass
    node_ws_connections = alive

def broadcast_node_change(config_uid: str, class_name: str, node_id: str | None = None, event: str = "changed"):
    """Broadcast a lightweight invalidation event to all subscribed node browser clients."""
    _cleanup_node_ws()
    payload = {
        "type": f"node.{event}",   # node.created / node.updated / node.deleted
        "config_uid": config_uid,
        "class": class_name,
        "id": node_id,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    dead = []
    for c in node_ws_connections:
        ws = c.get("ws")
        if ws is None:
            dead.append(c)
            continue

        # subscription filter
        sub_cfg = c.get("config_uid")
        sub_classes = c.get("classes")  # None -> all
        if sub_cfg and sub_cfg != config_uid:
            continue
        if sub_classes is not None and class_name not in sub_classes:
            continue

        try:
            ws.send(json.dumps(payload))
        except Exception:
            dead.append(c)

    if dead:
        node_ws_connections[:] = [c for c in node_ws_connections if c not in dead]

def handle_nodes_websocket(ws):
    """
    WebSocket channel for node browser.
    Client sends:
      {"type":"subscribe","config_uid":"...","classes":["A","B"]}  (classes optional)
    Server sends:
      {"type":"node.updated|node.created|node.deleted", ...}
    """
    subscription = {"ws": ws, "config_uid": None, "classes": None}
    node_ws_connections.append(subscription)

    while not ws.closed:
        try:
            msg = ws.receive()
            if msg is None:
                break
            data = json.loads(msg) if isinstance(msg, str) else msg
            mtype = data.get("type")

            if mtype == "subscribe":
                subscription["config_uid"] = data.get("config_uid")
                classes = data.get("classes")
                if classes is None:
                    subscription["classes"] = None
                else:
                    subscription["classes"] = set(classes)

                ws.send(json.dumps({
                    "type": "subscribed",
                    "config_uid": subscription["config_uid"],
                    "classes": list(subscription["classes"]) if subscription["classes"] is not None else None
                }))

            elif mtype == "ping":
                ws.send(json.dumps({"type": "pong"}))
        except Exception:
            break

    _cleanup_node_ws()

# Node discussion WebSocket connections (used by the web client Chat tab).
# Each item: {"ws": ws, "node_ids": set[str] | None}
discussion_ws_connections = []

def _cleanup_discussion_ws():
    """Remove closed/broken node-discussion WS connections."""
    global discussion_ws_connections
    alive = []
    for c in discussion_ws_connections:
        ws = c.get("ws")
        try:
            if ws is not None and not ws.closed:
                alive.append(c)
        except Exception:
            pass
    discussion_ws_connections = alive

def broadcast_node_discussion_change(node_id: str, message=None, event: str = "message"):
    """Broadcast a lightweight node-discussion event to subscribed browser clients."""
    node_id = str(node_id or '').strip()
    if not node_id:
        return

    _cleanup_discussion_ws()
    # Do not push message contents over this unauthenticated lightweight WS.
    # Browsers receive only a change signal and then reload through the
    # session-protected /client/api/node-discussion endpoint, where messages
    # are filtered by current_user.
    payload = {
        "type": "node_discussion.changed",
        "node_id": node_id,
        "message": None,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    dead = []
    for c in discussion_ws_connections:
        ws = c.get("ws")
        if ws is None:
            dead.append(c)
            continue

        sub_node_ids = c.get("node_ids")
        if sub_node_ids is not None and node_id not in sub_node_ids:
            continue

        try:
            ws.send(json.dumps(payload, ensure_ascii=False))
        except Exception:
            dead.append(c)

    if dead:
        discussion_ws_connections[:] = [c for c in discussion_ws_connections if c not in dead]

def handle_discussion_websocket(ws):
    """
    WebSocket channel for node discussions.
    Client sends:
      {"type":"subscribe","node_id":"..."}
      {"type":"subscribe","node_ids":["...", "..."]}
    Server sends:
      {"type":"node_discussion.message", "node_id":"...", "message": {...}}
    """
    subscription = {"ws": ws, "node_ids": None}
    discussion_ws_connections.append(subscription)

    while not ws.closed:
        try:
            msg = ws.receive()
            if msg is None:
                break
            data = json.loads(msg) if isinstance(msg, str) else msg
            if not isinstance(data, dict):
                continue
            mtype = data.get("type")

            if mtype == "subscribe":
                raw_ids = data.get("node_ids")
                if raw_ids is None and data.get("node_id") is not None:
                    raw_ids = [data.get("node_id")]
                if raw_ids is None:
                    subscription["node_ids"] = None
                else:
                    node_ids = set()
                    for value in raw_ids:
                        value = str(value or '').strip()
                        if value:
                            node_ids.add(value)
                    subscription["node_ids"] = node_ids

                ws.send(json.dumps({
                    "type": "subscribed",
                    "channel": "discussion",
                    "node_ids": list(subscription["node_ids"]) if subscription["node_ids"] is not None else None
                }, ensure_ascii=False))

            elif mtype == "ping":
                ws.send(json.dumps({"type": "pong"}))
        except Exception:
            break

    _cleanup_discussion_ws()


app.config['SECRET_KEY'] = FLASK_SECRET
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'

app.config['SQLALCHEMY_BINDS'] = {
    # stored near db.sqlite by default
    'client': 'sqlite:///client.sqlite',
}

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024
app.config['USER_TIMEZONE'] = 'Europe/Moscow'
app.config['JSON_AS_ASCII'] = False  


TASKS_DB_PATH = 'tasks.db'

db.init_app(app)
login_manager.init_app(app)
login_manager.login_view = 'index'

try:
    with app.app_context():
        ensure_node_discussion_message_table_runtime()
except Exception as _e:
    print('Node discussion runtime schema ensure skipped:', _e)


# ---------------------------------------------------------------
# Lightweight SQLite schema migration
#
# The project doesn't use Alembic. When we add new SQLAlchemy columns,
# existing sqlite DB files won't have them and the app can crash even on
# simple SELECTs (because SQLAlchemy selects all mapped columns).
#
# To keep upgrades zero-touch, we add missing columns with ALTER TABLE
# at startup, before any queries happen.

def _ensure_sqlite_schema():
    """
    Lightweight SQLite schema migration without Alembic.

    IMPORTANT:
    - db.create_all() does NOT add missing columns on SQLite.
    - SQLAlchemy selects all mapped columns; if a column is missing -> crash on SELECT.
    - This function must run BEFORE any queries.
    

    """

    try:
        if not _table_exists('raw_node'):
            with db.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE raw_node (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id VARCHAR(255) NOT NULL UNIQUE,
                        payload_json JSON NOT NULL,
                        content_type VARCHAR(64) NOT NULL DEFAULT 'node',
                        owner_user_id INTEGER NULL,
                        created_at DATETIME NULL,
                        updated_at DATETIME NULL
                    )
                """))
                conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_raw_node_node_id ON raw_node(node_id)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_raw_node_owner_user_id ON raw_node(owner_user_id)"))
    except Exception as e:
        print("Could not ensure raw_node table:", e)

    try:
        inspector = sa.inspect(db.engine)
    except Exception as e:
        print("Could not create inspector:", e)
        return

    # 1) Ensure base tables exist (creates missing tables only)
    try:
        db.create_all()
    except Exception as e:
        print("Could not create_all:", e)

    # Client bind tables (optional)
    try:
        db.create_all(bind_key="client")
    except Exception:
        pass

    def _table_exists(name: str) -> bool:
        try:
            return name in inspector.get_table_names()
        except Exception:
            return False

    def _get_cols(table: str) -> set[str]:
        try:
            return {c["name"] for c in inspector.get_columns(table)}
        except Exception:
            return set()

    def _add_col(table: str, col_sql: str, col_name: str):
        # refresh cols lazily
        cols = _get_cols(table)
        if col_name in cols:
            return
        try:
            with db.engine.begin() as conn:
                conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN {col_sql}"))
            print(f"Migration: {table} add column {col_name}")
        except Exception as e:
            print(f"Could not add column {table}.{col_name}:", e)

    def _create_index(sql: str, label: str):
        try:
            with db.engine.begin() as conn:
                conn.execute(sa.text(sql))
            print(f"Migration: {label}")
        except Exception as e:
            # indexes may already exist; keep silent-ish
            print(f"Could not create index ({label}):", e)


    # ------------------------------------------------------------
    # node_discussion_message migrations
    # Permanent history for node-discussion messages only.
    # ------------------------------------------------------------
    try:
        if not _table_exists('node_discussion_message'):
            with db.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE node_discussion_message (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id VARCHAR(255) NOT NULL,
                        client_message_id VARCHAR(128) NOT NULL UNIQUE,
                        sender_user VARCHAR(255),
                        sender_display_name VARCHAR(255) DEFAULT '',
                        target_type VARCHAR(32) NOT NULL DEFAULT 'user',
                        target_id VARCHAR(255) NOT NULL,
                        text TEXT DEFAULT '',
                        image TEXT,
                        image_url TEXT,
                        payload_json JSON,
                        delivery_status VARCHAR(32) DEFAULT 'accepted',
                        created_at DATETIME
                    )
                """))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_node_id ON node_discussion_message(node_id)"))
                conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_node_discussion_message_client_message_id ON node_discussion_message(client_message_id)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_created_at ON node_discussion_message(created_at)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_target ON node_discussion_message(target_type, target_id)"))
    except Exception as e:
        print("Could not ensure node_discussion_message table:", e)

    # ------------------------------------------------------------
    # user table migrations
    # ------------------------------------------------------------
    if _table_exists("user"):
        ucols = _get_cols("user")
        if "config_display_name" not in ucols:
            _add_col("user", 'config_display_name VARCHAR(100) DEFAULT ""', "config_display_name")

        # Backward compatible defaults: existing users keep access
        if "can_designer" not in ucols:
            _add_col("user", "can_designer BOOLEAN DEFAULT TRUE", "can_designer")
        if "can_client" not in ucols:
            _add_col("user", "can_client BOOLEAN DEFAULT TRUE", "can_client")
        if "can_api" not in ucols:
            _add_col("user", "can_api BOOLEAN DEFAULT TRUE", "can_api")
        if "parent_user_id" not in ucols:
            _add_col("user", "parent_user_id INTEGER", "parent_user_id")

    # ------------------------------------------------------------
    # config_section migrations
    # ------------------------------------------------------------
    if _table_exists("config_section"):
        scols = _get_cols("config_section")
        if "commands" not in scols:
            _add_col("config_section", "commands TEXT", "commands")

    # ------------------------------------------------------------
    # dataset migrations
    # ------------------------------------------------------------
    if _table_exists("dataset"):
        dcols = _get_cols("dataset")
        if "view_template" not in dcols:
            _add_col("dataset", "view_template TEXT", "view_template")
        if "autoload" not in dcols:
            _add_col("dataset", "autoload BOOLEAN DEFAULT FALSE", "autoload")

    # ------------------------------------------------------------
    # configuration migrations
    # ------------------------------------------------------------
    if _table_exists("configuration"):
        ccols = _get_cols("configuration")

        if "content_uid" not in ccols:
            _add_col("configuration", "content_uid VARCHAR(100)", "content_uid")
        if "vendor" not in ccols:
            _add_col("configuration", "vendor TEXT", "vendor")

        # common_layouts JSON
        if "common_layouts" not in ccols:
            _add_col("configuration", "common_layouts JSON", "common_layouts")

        if "user_id" not in ccols:
            _add_col("configuration", "user_id INTEGER", "user_id")
            # best-effort fill for old rows
            try:
                first_user = db.session.execute(select(User)).scalar()
                if first_user:
                    with db.engine.begin() as conn:
                        conn.execute(
                            sa.text("UPDATE configuration SET user_id = :uid WHERE user_id IS NULL"),
                            {"uid": first_user.id},
                        )
                _create_index(
                    "CREATE INDEX IF NOT EXISTS ix_configuration_user_id ON configuration (user_id)",
                    "configuration.user_id index",
                )
            except Exception as e:
                print("Could not backfill configuration.user_id:", e)

        if "server_name" not in ccols:
            _add_col("configuration", 'server_name VARCHAR(100) DEFAULT ""', "server_name")

        if "nodes_handlers" not in ccols:
            _add_col("configuration", "nodes_handlers TEXT", "nodes_handlers")
        if "nodes_handlers_meta" not in ccols:
            _add_col("configuration", "nodes_handlers_meta JSON", "nodes_handlers_meta")

        if "nodes_server_handlers" not in ccols:
            _add_col("configuration", "nodes_server_handlers TEXT", "nodes_server_handlers")
        if "nodes_server_handlers_meta" not in ccols:
            _add_col("configuration", "nodes_server_handlers_meta JSON", "nodes_server_handlers_meta")

        if "version" not in ccols:
            _add_col("configuration", 'version VARCHAR(20) DEFAULT "00.00.01"', "version")

        if "last_modified" not in ccols:
            _add_col("configuration", "last_modified DATETIME", "last_modified")
            # fill nulls
            try:
                with db.engine.begin() as conn:
                    conn.execute(sa.text(
                        "UPDATE configuration SET last_modified = CURRENT_TIMESTAMP "
                        "WHERE last_modified IS NULL"
                    ))
            except Exception as e:
                print("Could not backfill configuration.last_modified:", e)

        # best-effort normalize existing rows (content_uid/vendor)
        try:
            for cfg in Configuration.query.all():
                if not getattr(cfg, "content_uid", None):
                    cfg.content_uid = str(uuid.uuid4())
                if not getattr(cfg, "vendor", None):
                    # keep existing behavior
                    cfg.vendor = (cfg.user.config_display_name or cfg.user.email) if cfg.user else ""
            db.session.commit()
        except Exception as e:
            print("Could not normalize configuration rows:", e)
            db.session.rollback()

    # ------------------------------------------------------------
    # config_class migrations (this is where your crash came from)
    # ------------------------------------------------------------
    if _table_exists("config_class"):
        cols = _get_cols("config_class")

        # legacy / structural fields
        if "has_storage" not in cols:
            _add_col("config_class", "has_storage BOOLEAN DEFAULT FALSE", "has_storage")
        if "class_type" not in cols:
            _add_col("config_class", "class_type VARCHAR(50)", "class_type")
        if "hidden" not in cols:
            _add_col("config_class", "hidden BOOLEAN DEFAULT FALSE", "hidden")

        if "section" not in cols:
            _add_col("config_class", "section VARCHAR(100)", "section")
        if "section_code" not in cols:
            _add_col("config_class", "section_code VARCHAR(100)", "section_code")

        if "display_name" not in cols:
            _add_col("config_class", "display_name VARCHAR(100)", "display_name")
        if "record_view" not in cols:
            _add_col("config_class", 'record_view TEXT DEFAULT ""', "record_view")
        if "cover_image" not in cols:
            _add_col("config_class", "cover_image TEXT", "cover_image")

        # JSON/events column
        if "events" not in cols:
            _add_col("config_class", "events TEXT", "events")

        # display/layout fields
        if "display_image_web" not in cols:
            _add_col("config_class", 'display_image_web TEXT DEFAULT ""', "display_image_web")
        if "display_image_table" not in cols:
            _add_col("config_class", 'display_image_table TEXT DEFAULT ""', "display_image_table")
        if "init_screen_layout" not in cols:
            _add_col("config_class", 'init_screen_layout TEXT DEFAULT ""', "init_screen_layout")
        if "init_screen_layout_web" not in cols:
            _add_col("config_class", 'init_screen_layout_web TEXT DEFAULT ""', "init_screen_layout_web")
        if "plug_in" not in cols:
            _add_col("config_class", 'plug_in TEXT DEFAULT ""', "plug_in")
        if "plug_in_web" not in cols:
            _add_col("config_class", 'plug_in_web TEXT DEFAULT ""', "plug_in_web")

        # commands UI fields
        if "commands" not in cols:
            _add_col("config_class", 'commands TEXT DEFAULT ""', "commands")
        if "use_standard_commands" not in cols:
            _add_col("config_class", "use_standard_commands BOOLEAN DEFAULT TRUE", "use_standard_commands")
        if "svg_commands" not in cols:
            _add_col("config_class", 'svg_commands TEXT DEFAULT ""', "svg_commands")

        # Migration tab fields
        if "migration_register_command" not in cols:
            _add_col("config_class", "migration_register_command BOOLEAN DEFAULT 0", "migration_register_command")
        if "migration_register_on_save" not in cols:
            _add_col("config_class", "migration_register_on_save BOOLEAN DEFAULT 0", "migration_register_on_save")
        if "migration_default_room_uid" not in cols:
            _add_col("config_class", 'migration_default_room_uid VARCHAR(36) DEFAULT ""', "migration_default_room_uid")
        if "migration_default_room_alias" not in cols:
            _add_col("config_class", 'migration_default_room_alias VARCHAR(100) DEFAULT ""', "migration_default_room_alias")
        if "link_share_mode" not in cols:
            _add_col("config_class", 'link_share_mode VARCHAR(30) DEFAULT ""', "link_share_mode")
        if "indexes_json" not in cols:
            _add_col("config_class", 'indexes_json JSON', "indexes_json")

    # ------------------------------------------------------------
    # class_method migrations
    # ------------------------------------------------------------
    if _table_exists("class_method"):
        mcols = _get_cols("class_method")
        if "source" not in mcols:
            _add_col("class_method", 'source VARCHAR(100) DEFAULT "internal"', "source")
        if "server" not in mcols:
            _add_col("class_method", 'server VARCHAR(255) DEFAULT "internal"', "server")

    # ------------------------------------------------------------
    # room_objects migrations
    # ------------------------------------------------------------
    if _table_exists("room"):
        room_cols = _get_cols("room")
        if "transport" not in room_cols:
            _add_col("room", 'transport VARCHAR(30) DEFAULT "websocket"', "transport")

    try:
        if not _table_exists("room_device"):
            db.create_all()
    except Exception as e:
        print("Could not ensure room_device table:", e)

    if _table_exists("user_device"):
        udcols = _get_cols("user_device")
        if "device_uid" not in udcols:
            _add_col("user_device", 'device_uid VARCHAR(120) DEFAULT ""', "device_uid")
        if "extra_json" not in udcols:
            _add_col("user_device", 'extra_json JSON', "extra_json")
    else:
        try:
            db.create_all()
        except Exception as e:
            print("Could not ensure user_device table:", e)

    if _table_exists("room_objects"):
        rocols = _get_cols("room_objects")
        if "acknowledged_by" not in rocols:
            _add_col("room_objects", 'acknowledged_by JSON DEFAULT "[]"', "acknowledged_by")

    # ------------------------------------------------------------
    # config_event / config_event_action migrations (tables might be missing on old DB)
    # ------------------------------------------------------------
    # ensure tables exist
    try:
        if not _table_exists("config_event") or not _table_exists("config_event_action"):
            db.create_all()
    except Exception as e:
        print("Could not ensure config_event tables:", e)

    if _table_exists("config_event"):
        ecol = _get_cols("config_event")
        if "config_id" not in ecol:
            _add_col("config_event", "config_id INTEGER", "config_id")
            _create_index(
                "CREATE INDEX IF NOT EXISTS ix_config_event_config_id ON config_event (config_id)",
                "config_event.config_id index",
            )

    if _table_exists("config_event_action"):
        eacols = _get_cols("config_event_action")
        if "event_id" not in eacols:
            _add_col("config_event_action", "event_id INTEGER", "event_id")
            _create_index(
                "CREATE INDEX IF NOT EXISTS ix_config_event_action_event_id ON config_event_action (event_id)",
                "config_event_action.event_id index",
            )

        # NodaScript support
        if "method_text" not in eacols:
            _add_col("config_event_action", 'method_text TEXT DEFAULT ""', "method_text")
        if "post_execute_text" not in eacols:
            _add_col("config_event_action", 'post_execute_text TEXT DEFAULT ""', "post_execute_text")
        if "http_function_name" not in eacols:
            _add_col("config_event_action", 'http_function_name VARCHAR(255) DEFAULT ""', "http_function_name")
        if "post_http_function_name" not in eacols:
            _add_col("config_event_action", 'post_http_function_name VARCHAR(255) DEFAULT ""', "post_http_function_name")

    # ------------------------------------------------------------
    # outgoing_message_log migrations
    # ------------------------------------------------------------
    if _table_exists("outgoing_message_log"):
        mcols = _get_cols("outgoing_message_log")
        if "client_message_id" not in mcols:
            _add_col("outgoing_message_log", 'client_message_id VARCHAR(128)', "client_message_id")
        if "sender_user" not in mcols:
            _add_col("outgoing_message_log", 'sender_user VARCHAR(255)', "sender_user")
        if "target_type" not in mcols:
            _add_col("outgoing_message_log", 'target_type VARCHAR(32) DEFAULT "user"', "target_type")
        if "target_id" not in mcols:
            _add_col("outgoing_message_log", 'target_id VARCHAR(255)', "target_id")
        if "title" not in mcols:
            _add_col("outgoing_message_log", 'title VARCHAR(255)', "title")
        if "body" not in mcols:
            _add_col("outgoing_message_log", 'body TEXT', "body")
        if "payload_json" not in mcols:
            _add_col("outgoing_message_log", 'payload_json JSON', "payload_json")
        if "status" not in mcols:
            _add_col("outgoing_message_log", 'status VARCHAR(32) DEFAULT "queued"', "status")
        if "created_at" not in mcols:
            _add_col("outgoing_message_log", 'created_at DATETIME', "created_at")
        if "accepted_at" not in mcols:
            _add_col("outgoing_message_log", 'accepted_at DATETIME', "accepted_at")
        if "pushed_at" not in mcols:
            _add_col("outgoing_message_log", 'pushed_at DATETIME', "pushed_at")
        if "ack_at" not in mcols:
            _add_col("outgoing_message_log", 'ack_at DATETIME', "ack_at")
        if "ack_by" not in mcols:
            _add_col("outgoing_message_log", 'ack_by VARCHAR(255)', "ack_by")
        if "ack_payload" not in mcols:
            _add_col("outgoing_message_log", 'ack_payload JSON', "ack_payload")
        if "last_error" not in mcols:
            _add_col("outgoing_message_log", 'last_error TEXT', "last_error")

        _create_index('CREATE UNIQUE INDEX IF NOT EXISTS ux_outgoing_message_client_message_id ON outgoing_message_log (client_message_id)', 'ux_outgoing_message_client_message_id')
        _create_index('CREATE INDEX IF NOT EXISTS ix_outgoing_message_target ON outgoing_message_log (target_type, target_id)', 'ix_outgoing_message_target')
        _create_index('CREATE INDEX IF NOT EXISTS ix_outgoing_message_status ON outgoing_message_log (status)', 'ix_outgoing_message_status')


    # ------------------------------------------------------------
    # outgoing_message_device_ack migrations
    # ------------------------------------------------------------
    try:
        if not _table_exists('outgoing_message_device_ack'):
            with db.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE outgoing_message_device_ack (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id INTEGER NOT NULL,
                        client_message_id VARCHAR(128),
                        user_key VARCHAR(255),
                        device_uid VARCHAR(120) NOT NULL,
                        ack_at DATETIME,
                        ack_by VARCHAR(255),
                        ack_payload JSON,
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                """))
        if _table_exists('outgoing_message_device_ack'):
            dacols = _get_cols('outgoing_message_device_ack')
            if 'message_id' not in dacols:
                _add_col('outgoing_message_device_ack', 'message_id INTEGER', 'message_id')
            if 'client_message_id' not in dacols:
                _add_col('outgoing_message_device_ack', 'client_message_id VARCHAR(128)', 'client_message_id')
            if 'user_key' not in dacols:
                _add_col('outgoing_message_device_ack', 'user_key VARCHAR(255)', 'user_key')
            if 'device_uid' not in dacols:
                _add_col('outgoing_message_device_ack', 'device_uid VARCHAR(120)', 'device_uid')
            if 'ack_at' not in dacols:
                _add_col('outgoing_message_device_ack', 'ack_at DATETIME', 'ack_at')
            if 'ack_by' not in dacols:
                _add_col('outgoing_message_device_ack', 'ack_by VARCHAR(255)', 'ack_by')
            if 'ack_payload' not in dacols:
                _add_col('outgoing_message_device_ack', 'ack_payload JSON', 'ack_payload')
            if 'created_at' not in dacols:
                _add_col('outgoing_message_device_ack', 'created_at DATETIME', 'created_at')
            if 'updated_at' not in dacols:
                _add_col('outgoing_message_device_ack', 'updated_at DATETIME', 'updated_at')
            _create_index('CREATE UNIQUE INDEX IF NOT EXISTS ux_outgoing_message_device_ack_msg_device ON outgoing_message_device_ack (message_id, device_uid)', 'ux_outgoing_message_device_ack_msg_device')
            _create_index('CREATE INDEX IF NOT EXISTS ix_outgoing_message_device_ack_user_device ON outgoing_message_device_ack (user_key, device_uid)', 'ix_outgoing_message_device_ack_user_device')
            _create_index('CREATE INDEX IF NOT EXISTS ix_outgoing_message_device_ack_client_message_id ON outgoing_message_device_ack (client_message_id)', 'ix_outgoing_message_device_ack_client_message_id')
    except Exception as e:
        print('Could not ensure outgoing_message_device_ack table:', e)

    # ------------------------------------------------------------
    # message_group / message_group_member migrations
    # ------------------------------------------------------------
    try:
        if not _table_exists('message_group'):
            with db.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE message_group (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id VARCHAR(64) NOT NULL UNIQUE,
                        title VARCHAR(255) NOT NULL,
                        created_by VARCHAR(255),
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                """))
            _create_index('CREATE UNIQUE INDEX IF NOT EXISTS ux_message_group_group_id ON message_group (group_id)', 'ux_message_group_group_id')
            _create_index('CREATE INDEX IF NOT EXISTS ix_message_group_created_by ON message_group (created_by)', 'ix_message_group_created_by')
        if _table_exists('message_group'):
            gcols = _get_cols('message_group')
            if 'group_id' not in gcols:
                _add_col('message_group', 'group_id VARCHAR(64)', 'group_id')
            if 'title' not in gcols:
                _add_col('message_group', 'title VARCHAR(255)', 'title')
            if 'created_by' not in gcols:
                _add_col('message_group', 'created_by VARCHAR(255)', 'created_by')
            if 'created_at' not in gcols:
                _add_col('message_group', 'created_at DATETIME', 'created_at')
            if 'updated_at' not in gcols:
                _add_col('message_group', 'updated_at DATETIME', 'updated_at')
            _create_index('CREATE UNIQUE INDEX IF NOT EXISTS ux_message_group_group_id ON message_group (group_id)', 'ux_message_group_group_id')
            _create_index('CREATE INDEX IF NOT EXISTS ix_message_group_created_by ON message_group (created_by)', 'ix_message_group_created_by')
    except Exception as e:
        print('Could not ensure message_group table:', e)

    try:
        if not _table_exists('message_group_member'):
            with db.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE message_group_member (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id VARCHAR(64) NOT NULL,
                        user_key VARCHAR(255) NOT NULL,
                        created_at DATETIME
                    )
                """))
            _create_index('CREATE UNIQUE INDEX IF NOT EXISTS uq_message_group_member_group_user ON message_group_member (group_id, user_key)', 'uq_message_group_member_group_user')
            _create_index('CREATE INDEX IF NOT EXISTS ix_message_group_member_user_group ON message_group_member (user_key, group_id)', 'ix_message_group_member_user_group')
        if _table_exists('message_group_member'):
            gmcols = _get_cols('message_group_member')
            if 'group_id' not in gmcols:
                _add_col('message_group_member', 'group_id VARCHAR(64)', 'group_id')
            if 'user_key' not in gmcols:
                _add_col('message_group_member', 'user_key VARCHAR(255)', 'user_key')
            if 'created_at' not in gmcols:
                _add_col('message_group_member', 'created_at DATETIME', 'created_at')
            _create_index('CREATE UNIQUE INDEX IF NOT EXISTS uq_message_group_member_group_user ON message_group_member (group_id, user_key)', 'uq_message_group_member_group_user')
            _create_index('CREATE INDEX IF NOT EXISTS ix_message_group_member_user_group ON message_group_member (user_key, group_id)', 'ix_message_group_member_user_group')
    except Exception as e:
        print('Could not ensure message_group_member table:', e)

    # ------------------------------------------------------------
    # event_action migrations (ClassEvent actions)
    # ------------------------------------------------------------
    if _table_exists("event_action"):
        acols = _get_cols("event_action")
        # NodaScript support
        if "method_text" not in acols:
            _add_col("event_action", 'method_text TEXT DEFAULT ""', "method_text")
        if "post_execute_text" not in acols:
            _add_col("event_action", 'post_execute_text TEXT DEFAULT ""', "post_execute_text")
        if "http_function_name" not in acols:
            _add_col("event_action", 'http_function_name VARCHAR(255) DEFAULT ""', "http_function_name")
        if "post_http_function_name" not in acols:
            _add_col("event_action", 'post_http_function_name VARCHAR(255) DEFAULT ""', "post_http_function_name")

# Run schema check immediately on import (works for `flask run` too)
try:
    with app.app_context():
        _ensure_sqlite_schema()
except Exception as _e:
    print('SQLite schema ensure skipped:', _e)

try:
    from client_app.routes import client_bp
    app.register_blueprint(client_bp)
except Exception as _e:
    print('Client blueprint not loaded:', _e)


# NOTE: Models are defined throughout this large single-file app.
# Run schema ensure once more near the end of the module so newly added
# columns are present before any runtime SELECTs on updated models.
try:
    with app.app_context():
        _ensure_sqlite_schema()
except Exception as _e:
    print('SQLite schema ensure (late) skipped:', _e)




















    

    























      

# Authorization
@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

@app.route('/api/get-token', methods=['GET'])
@login_required
def get_token_by_android_id():
    android_id = request.args.get('android_id')
    if not android_id:
        return jsonify({'error': 'android_id is required'}), 400

    device = UserDevice.query.filter_by(user_id=current_user.id, android_id=android_id).first()
    if not device:
        return jsonify({'error': 'device not found'}), 404

    return jsonify({'token': device.token or ''})





def issue_api_token(user) -> str:
    # достаточно длинный, URL-safe
    token = secrets.token_urlsafe(48)
    db.session.add(ApiToken(user_id=user.id, token=token))
    db.session.commit()
    return token


def check_api_token(token: str):
    if not token:
        return None
    tok = ApiToken.query.filter_by(token=token, revoked_at=None).first()
    return tok.user if tok else None

@app.route("/api/auth/register", methods=["POST"])
def api_auth_register():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    name = (data.get("name") or email).strip().lower()

    if not email or not password:
        return jsonify({"error": "email and password are required"}), 400

    existing = User.query.filter_by(email=email).first()
    if existing:
        return jsonify({"error": "user already exists"}), 409

    user = User(
        email=email,
        password=generate_password_hash(password),
        config_display_name=name,
        can_api=True,
    )
    db.session.add(user)
    db.session.flush()

    device_uid = (data.get('device_uid') or data.get('uid') or data.get('android_id') or '').strip()
    android_id = (data.get('android_id') or device_uid).strip()
    device_model = (data.get('device_model') or '').strip()
    fcm_token = (data.get('fcm_token') or data.get('token') or '').strip()

    device_info = None
    if device_uid or android_id or fcm_token:
        effective_device_uid = device_uid or android_id
        if not effective_device_uid:
            db.session.rollback()
            return jsonify({'error': 'device_uid or android_id is required when registering a device'}), 400

        user_device = UserDevice.query.filter_by(user_id=user.id, android_id=android_id or effective_device_uid).first()
        if not user_device:
            user_device = UserDevice(
                user_id=user.id,
                device_uid=effective_device_uid,
                android_id=android_id or effective_device_uid,
            )
            db.session.add(user_device)

        user_device.device_uid = effective_device_uid
        user_device.android_id = android_id or effective_device_uid
        user_device.device_model = device_model
        user_device.token = fcm_token
        user_device.extra_json = data
        user_device.last_connected = datetime.now(timezone.utc)

        device_info = {
            'device_uid': user_device.device_uid,
            'android_id': user_device.android_id,
        }

    db.session.commit()

    token_value = issue_api_token(user)

    response = {
        'user': {'id': user.id, 'email': user.email,'name':user.config_display_name},
        'access_token': token_value,
    }
    if device_info:
        response['device'] = device_info
    return jsonify(response), 201    

@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"error": "email and password are required"}), 400

    user = User.query.filter_by(email=email).first()

    # у вас пароль хранится в user.password (hash)
    if not user or not check_password_hash(user.password, password):
        return jsonify({"error": "Invalid credentials"}), 401

    if not bool(getattr(user, "can_api", False)):
        return jsonify({"error": "Forbidden"}), 403

    # reuse последний активный токен, чтобы не плодить
    tok = ApiToken.query.filter_by(user_id=user.id, revoked_at=None).order_by(ApiToken.id.desc()).first()
    token_value = tok.token if tok else issue_api_token(user)

    return jsonify({
        "user": {"id": user.id, "email": user.email, "name":user.config_display_name},
        "access_token": token_value
    }), 200

@app.route('/api/auth/me', methods=['GET'])
def api_auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = None

        # --- 1) Token auth: Bearer / X-API-Token ---
        bearer = request.headers.get("Authorization", "")
        x_token = request.headers.get("X-API-Token", "")

        token = None
        if bearer.lower().startswith("bearer "):
            token = bearer.split(" ", 1)[1].strip()
        elif x_token:
            token = x_token.strip()

        if token:
            user = check_api_token(token)
            if not user:
                return jsonify({"error": "Unauthorized"}), 401


        if not user:
            auth = request.authorization
            if auth:
                user = check_api_auth(auth.username, auth.password)

            if not user:
                return jsonify({"error": "Unauthorized"}), 401


        if not bool(getattr(user, "can_api", False)):
            return jsonify({"error": "Forbidden"}), 403


        cfg_uid = kwargs.get("config_uid") or kwargs.get("uid")
        if cfg_uid and not user_can_access_config(user, str(cfg_uid)):
            return jsonify({"error": "Forbidden"}), 403

        g.api_user = user
        return f(*args, **kwargs)

    return decorated_function












@app.route("/api/s3/upload-url", methods=["POST"])
def get_upload_url():
    # Web client uses Flask-Login cookies; mobile/API clients may use the
    # existing API token/basic auth. Do not issue public upload URLs to
    # anonymous callers.
    upload_user = None
    try:
        if getattr(current_user, 'is_authenticated', False):
            upload_user = current_user
    except Exception:
        upload_user = None

    if upload_user is None:
        bearer = request.headers.get("Authorization", "")
        x_token = request.headers.get("X-API-Token", "")
        token = None
        if bearer.lower().startswith("bearer "):
            token = bearer.split(" ", 1)[1].strip()
        elif x_token:
            token = x_token.strip()
        if token:
            upload_user = check_api_token(token)
        if upload_user is None and request.authorization:
            upload_user = check_api_auth(request.authorization.username, request.authorization.password)
        if upload_user is None:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        if not bool(getattr(upload_user, "can_api", False)):
            return jsonify({"ok": False, "error": "Forbidden"}), 403
        g.api_user = upload_user

    data = request.get_json(silent=True) or {}

    original_name = data.get("filename", "file.bin")
    content_type = data.get("content_type", "application/octet-stream")

    ext = ""
    if "." in original_name:
        ext = "." + original_name.rsplit(".", 1)[1].lower()

    upload_user_id = str(getattr(upload_user, 'id', '') or 'user').strip() or 'user'
    object_key = f"uploads/chat/{upload_user_id}/{uuid.uuid4().hex}{ext}"

    url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": object_key,
            "ContentType": content_type,
        },
        ExpiresIn=600,  # 10 минут
    )

    public_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{object_key}"

    return jsonify({
        "upload_url": url,
        "object_key": object_key,
        "method": "PUT",
        "expires_in": 600,
        "headers": {
            "Content-Type": content_type
        },
        "file_url": public_url
    })


#Row nodes
@app.route('/api/raw-node/<node_id>', methods=['POST'])
@api_auth_required
def api_raw_node_post(node_id):
    data = request.get_json(silent=True) or {}

    payload = data.get('payload', data)
    content_type = str(data.get('content_type') or 'node').strip() or 'node'

    obj = db.session.execute(
        select(RawNode).where(RawNode.node_id == str(node_id))
    ).scalar_one_or_none()

    now = datetime.now(timezone.utc)
    api_user = getattr(g, 'api_user', None)

    if obj is None:
        obj = RawNode(
            node_id=str(node_id),
            payload_json=payload,
            content_type=content_type,
            owner_user_id=getattr(api_user, 'id', None),
            created_at=now,
            updated_at=now,
        )
        db.session.add(obj)
    else:
        obj.payload_json = payload
        obj.content_type = content_type
        obj.updated_at = now
        # Preserve existing owner, but bind previously anonymous raw-node rows
        # to the authenticated uploader.  This is only metadata for web-client
        # Received Nodes; GET /api/raw-node/<id> remains link-addressable after
        # Basic/API auth.
        if getattr(obj, 'owner_user_id', None) is None and getattr(api_user, 'id', None) is not None:
            obj.owner_user_id = getattr(api_user, 'id', None)

    db.session.commit()

    # Raw-node URLs may be reused for updated JSON payloads; drop any old
    # cached variants so event dispatch sees fresh class/events data.
    try:
        for _u in (
            f"{request.url_root.rstrip('/')}/api/raw-node/{obj.node_id}",
            f"/api/raw-node/{obj.node_id}",
            f"raw-node/{obj.node_id}",
        ):
            _runtime_cache_invalidate(_u)
    except Exception:
        pass

    return jsonify({
        'ok': True,
        'node_id': obj.node_id,
        'content_type': obj.content_type,
        'created_at': obj.created_at.isoformat() if obj.created_at else None,
        'updated_at': obj.updated_at.isoformat() if obj.updated_at else None,
        'url': f"{request.url_root.rstrip('/')}/api/raw-node/{obj.node_id}"
    })


@app.route('/api/raw-node/<node_id>', methods=['GET'])
@api_auth_required
def api_raw_node_get(node_id):
    obj = db.session.execute(
        select(RawNode).where(RawNode.node_id == str(node_id))
    ).scalar_one_or_none()

    if obj is None:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    # Keep this endpoint link-addressable for mobile clients and raw-node
    # download URLs. Authentication is still required by @api_auth_required,
    # but possession of the raw-node URL is enough after Basic/API auth.
    # User-scoping is applied only in web-client listings/opening routes
    # such as Received Nodes and /client/raw-node/<id>.
    return jsonify(obj.payload_json or {})


def _normalized_base_url(value: str) -> str:
    return str(value or '').strip().rstrip('/')


def _current_public_base_url() -> str:
    base = _normalized_base_url(PUBLIC_API_BASE_URL)
    if base:
        return base

    # Behind nginx / reverse proxy Flask often sees http://localhost, while
    # mobile clients work with the public https:// host. Use forwarded headers
    # first so generated raw-node thread_ref matches the client-side thread_ref.
    try:
        forwarded_proto = str(
            request.headers.get('X-Forwarded-Proto')
            or request.headers.get('X-Forwarded-Scheme')
            or request.headers.get('X-Scheme')
            or ''
        ).split(',', 1)[0].strip()
        forwarded_host = str(
            request.headers.get('X-Forwarded-Host')
            or request.headers.get('Host')
            or ''
        ).split(',', 1)[0].strip()
        if forwarded_host:
            if not forwarded_proto:
                forwarded_proto = 'https' if (request.is_secure or str(request.headers.get('X-Forwarded-Ssl') or '').lower() == 'on') else ''
            if forwarded_proto in ('http', 'https'):
                return _normalized_base_url(f'{forwarded_proto}://{forwarded_host}')
    except Exception:
        pass

    try:
        return _normalized_base_url(request.url_root)
    except Exception:
        return ''


def _hostname_candidates(host: str) -> set[str]:
    host = str(host or '').strip().lower()
    result = {host} if host else set()
    if not host:
        return result
    try:
        canon, aliases, addrs = socket.gethostbyname_ex(host)
        if canon:
            result.add(canon.lower())
        for item in aliases or []:
            if item:
                result.add(str(item).lower())
        for item in addrs or []:
            if item:
                result.add(str(item).lower())
    except Exception:
        pass
    return {x for x in result if x}


def _local_host_candidates() -> set[str]:
    result = {
        'localhost',
        '127.0.0.1',
        '::1',
    }

    for value in [
        request.host.split(':', 1)[0] if getattr(request, 'host', None) else '',
        urlparse(_current_public_base_url()).hostname or '',
        os.environ.get('SERVER_NAME', ''),
        socket.gethostname(),
        socket.getfqdn(),
    ]:
        result.update(_hostname_candidates(value))

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            addr = info[4][0]
            if addr:
                result.add(str(addr).lower())
    except Exception:
        pass

    extra = os.environ.get('SELF_BASE_URL_ALIASES', '')
    for item in extra.split(','):
        item = item.strip()
        if not item:
            continue
        parsed = urlparse(item if '://' in item else f'http://{item}')
        result.update(_hostname_candidates(parsed.hostname or item))

    return {x for x in result if x}


def _same_server_base_url(a: str, b: str) -> bool:
    a = _normalized_base_url(a)
    b = _normalized_base_url(b)
    if not a or not b:
        return False

    pa = urlparse(a)
    pb = urlparse(b)

    def _normalized_port(p):
        if p.port is not None:
            return p.port
        scheme = (p.scheme or '').lower()
        return 443 if scheme == 'https' else 80

    host_a = (pa.hostname or '').lower()
    host_b = (pb.hostname or '').lower()

    if not host_a or not host_b:
        return False

    candidates_a = _hostname_candidates(host_a)
    candidates_b = _hostname_candidates(host_b)
    local_candidates = _local_host_candidates()

    same_host = bool(candidates_a & candidates_b)
    if not same_host:
        same_host = bool((candidates_a & local_candidates) and (candidates_b & local_candidates))

    if not same_host:
        return False

    port_a = _normalized_port(pa)
    port_b = _normalized_port(pb)
    if port_a == port_b:
        return True
    # Reverse proxy / internal bind case: treat as same server when both hosts resolve to local machine.
    return bool((candidates_a & local_candidates) and (candidates_b & local_candidates))


def _extract_node_class_name(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ('full_name', 'code', 'uid', 'id', 'name'):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
    return str(value or '').strip()


def _raw_node_public_url(base_url: str, node_id: str) -> str:
    return f"{_normalized_base_url(base_url)}/api/raw-node/{str(node_id or '').strip()}"


def _node_discussion_existing_thread_ref(node_id: str) -> str:
    """Return the thread_ref already used by clients for this raw node, if known."""
    node_id = str(node_id or '').strip()
    if not node_id:
        return ''

    def _pick(payload):
        if not isinstance(payload, dict):
            return ''
        for key in ('thread_ref', 'download_url', 'raw_node_url', 'node_url'):
            value = str(payload.get(key) or '').strip()
            if '/api/raw-node/' in value:
                return value
        return ''

    try:
        rows = NodeDiscussionMessage.query.filter_by(node_id=node_id).order_by(
            NodeDiscussionMessage.created_at.desc(),
            NodeDiscussionMessage.id.desc(),
        ).limit(50).all()
        for msg in rows:
            ref = _pick(msg.payload_json if isinstance(msg.payload_json, dict) else {})
            if ref:
                return ref
    except Exception:
        pass

    try:
        rows = OutgoingMessageLog.query.filter(
            OutgoingMessageLog.target_type.in_(('user', 'group'))
        ).order_by(OutgoingMessageLog.created_at.desc(), OutgoingMessageLog.id.desc()).limit(500).all()
        for msg in rows:
            payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
            if not (
                _extract_node_discussion_node_id(payload) == node_id
                or _node_discussion_payload_matches_node(payload, node_id)
                or _node_delivery_payload_matches_node(payload, node_id)
            ):
                continue
            ref = _pick(payload)
            if ref:
                return ref
    except Exception:
        pass

    return ''


def _node_discussion_thread_ref(node_id: str, preferred: str = '') -> str:
    """Build a client-compatible raw-node thread_ref.

    Prefer an existing thread_ref from discussion history to avoid http/https
    mismatches on Android clients that key chats by exact thread_ref string.
    """
    preferred = str(preferred or '').strip()
    if '/api/raw-node/' in preferred:
        return preferred
    existing = _node_discussion_existing_thread_ref(node_id)
    if existing:
        return existing
    return _raw_node_public_url(_current_public_base_url(), node_id)


def _save_raw_node_local(node_id: str, payload: dict, owner_user_id=None, content_type='node'):
    obj = db.session.execute(
        select(RawNode).where(RawNode.node_id == str(node_id))
    ).scalar_one_or_none()

    now = datetime.now(timezone.utc)

    if obj is None:
        obj = RawNode(
            node_id=str(node_id),
            payload_json=payload,
            content_type=content_type,
            owner_user_id=owner_user_id,
            created_at=now,
            updated_at=now,
        )
        db.session.add(obj)
    else:
        obj.payload_json = payload
        obj.content_type = content_type
        obj.updated_at = now
        if owner_user_id is not None:
            obj.owner_user_id = owner_user_id

    db.session.commit()
    try:
        for _u in (
            _raw_node_public_url(_current_public_base_url(), node_id),
            f"/api/raw-node/{node_id}",
            f"raw-node/{node_id}",
        ):
            _runtime_cache_invalidate(_u)
    except Exception:
        pass
    return obj

def _node_discussion_group_id(class_name, node_id):
    class_name = str(class_name or '').strip()
    node_id = str(node_id or '').strip()
    return f"node:{class_name}:{node_id}"


def _ensure_node_discussion_group(class_name, node_id, title=None, members=None):
    group_id = _node_discussion_group_id(class_name, node_id)
    group = MessageGroup.query.filter_by(group_id=group_id).first()

    if group is None:
        group = MessageGroup(
            group_id=group_id,
            title=title or f"{class_name}:{node_id}",
            created_by=_normalize_user_key(getattr(getattr(g, 'api_user', None), 'email', None)) or None,
        )
        db.session.add(group)
        db.session.flush()

    current_user_key = _normalize_user_key(getattr(getattr(g, 'api_user', None), 'email', None))
    all_members = _normalize_member_user_keys(members, include_user_key=current_user_key)

    existing = {
        m.user_key.lower()
        for m in MessageGroupMember.query.filter_by(group_id=group_id).all()
    }

    for user_key in all_members:
        if user_key.lower() not in existing:
            db.session.add(MessageGroupMember(group_id=group_id, user_key=user_key))

    group.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return group

@app.route('/api/node-discussion/<class_name>/<node_id>/messages', methods=['GET'])
@api_auth_required
def api_node_discussion_messages(class_name, node_id):
    group_id = _node_discussion_group_id(class_name, node_id)

    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        return jsonify({
            'ok': True,
            'group_id': group_id,
            'class_name': class_name,
            'node_id': node_id,
            'count': 0,
            'messages': [],
        }), 200

    limit = request.args.get('limit', 1000)
    before = request.args.get('before')

    payload, status = _get_group_messages_history_impl(group_id, limit=limit, before=before)

    if status == 200 and payload.get('ok'):
        messages = []
        for m in payload.get('messages', []):
            data = m.get('data') if isinstance(m.get('data'), dict) else {}

            messages.append({
                'client_message_id': m.get('client_message_id'),
                'sender_user': m.get('sender_user'),
                'sender_display_name': m.get('sender_display_name'),
                'created_at': m.get('created_at'),
                'text': data.get('text') if data.get('text') is not None else m.get('text'),
                'image': data.get('image'),
                'image_url': data.get('image_url'),
                'data': data,
            })

        payload.update({
            'class_name': class_name,
            'node_id': node_id,
            'messages': messages,
            'count': len(messages),
        })

    return jsonify(payload), status


@app.route('/api/node-discussion/<class_name>/<node_id>/messages', methods=['POST'])
@api_auth_required
def api_post_node_discussion_message(class_name, node_id):
    data = request.get_json(silent=True) or {}

    group = _ensure_node_discussion_group(
        class_name,
        node_id,
        title=data.get('title') or f"{class_name}:{node_id}",
        members=data.get('members'),
    )

    # Sender identity is always taken from the authenticated API user/session.
    # Do not trust sender_user from JSON; otherwise one client can spoof another.
    sender_user = _get_sender_user(None)
    sender_display_name = _get_sender_display_name(sender_user)

    text = data.get('text')
    image = data.get('image')
    image_url = data.get('image_url')

    payload = {
        'type': 'node_discussion_message',
        'class_name': class_name,
        'node_id': node_id,
        'text': text or '',
    }

    if image is not None:
        payload['image'] = image
    if image_url is not None:
        payload['image_url'] = image_url
    if sender_display_name:
        payload['sender_display_name'] = sender_display_name

    extra = data.get('data')
    if isinstance(extra, dict):
        payload.update(extra)
        payload['type'] = 'node_discussion_message'
        payload['class_name'] = class_name
        payload['node_id'] = node_id
        payload['text'] = text or payload.get('text') or ''
    if sender_user:
        payload['sender_user'] = sender_user
    if sender_display_name:
        payload['sender_display_name'] = sender_display_name

    body = text or data.get('body') or data.get('message') or 'New message'
    title = data.get('title') or sender_display_name or group.title or 'Node discussion'

    result = send_message_to_group_global(
        group.group_id,
        title,
        body,
        payload,
        sender_user=sender_user,
    )

    return jsonify({
        'ok': bool(result.get('ok')),
        'group_id': group.group_id,
        'class_name': class_name,
        'node_id': node_id,
        'result': result,
    }), (200 if result.get('ok') else 400)


def _node_discussion_ref_candidates(node_id):
    node_id = str(node_id or '').strip()
    if not node_id:
        return set()
    candidates = {node_id}
    try:
        candidates.add(_raw_node_public_url(_current_public_base_url(), node_id))
    except Exception:
        pass
    candidates.add(f'/api/raw-node/{node_id}')
    candidates.add(f'raw-node/{node_id}')
    return {str(x).strip() for x in candidates if str(x or '').strip()}


def _node_discussion_value_matches_node(value, node_id):
    node_id = str(node_id or '').strip()
    value = str(value or '').strip()
    if not node_id or not value:
        return False
    if value == node_id:
        return True
    if value.rstrip('/').endswith('/' + node_id):
        return True
    if f'/api/raw-node/{node_id}' in value or f'raw-node/{node_id}' in value:
        return True
    return False


def _node_discussion_payload_matches_node(payload, node_id):
    if not isinstance(payload, dict):
        return False

    for key in ('node_id', 'node_uid', '_id', 'discussion_node_id', 'discussion_node_uid'):
        if _node_discussion_value_matches_node(payload.get(key), node_id):
            return True

    for key in ('thread_ref', 'download_url', 'node_url', 'raw_node_url'):
        if _node_discussion_value_matches_node(payload.get(key), node_id):
            return True

    # Some clients put nested data into a stringified JSON field.
    for key in ('payload', 'data'):
        nested = payload.get(key)
        if isinstance(nested, dict) and _node_discussion_payload_matches_node(nested, node_id):
            return True

    return False


# -----------------------------------------------------------------------------
# Node discussion diagnostics / API datetime formatting
# -----------------------------------------------------------------------------
def _node_discussion_debug(event, **fields):
    """Small stdout logger for node-discussion delivery/pending diagnostics.

    Enable on VPS with:
        export NODA_NODE_DISCUSSION_DEBUG=1
    """
    try:
        enabled = str(os.environ.get('NODA_NODE_DISCUSSION_DEBUG') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
        try:
            enabled = enabled or bool(app.config.get('NODA_NODE_DISCUSSION_DEBUG'))
        except Exception:
            pass
        if not enabled:
            return

        safe = []
        for key, value in fields.items():
            key_s = str(key)
            if 'token' in key_s.lower():
                value = '<redacted>'
            elif isinstance(value, (dict, list, tuple)):
                try:
                    value = json.dumps(value, ensure_ascii=False, default=str)
                except Exception:
                    value = str(value)
            safe.append(f"{key_s}={value}")
        print('[node-discussion-debug] ' + str(event) + (' | ' + ' | '.join(safe) if safe else ''), flush=True)
    except Exception as e:
        try:
            print('[node-discussion-debug] logger_failed:', e, flush=True)
        except Exception:
            pass


def _node_discussion_response_timezone_name():
    """Timezone used only by GET /api/node-discussion/by-node/.../messages."""
    try:
        tz_arg = request.args.get('tz') or request.args.get('timezone')
        if tz_arg:
            return str(tz_arg).strip()
    except Exception:
        pass

    try:
        api_user = getattr(g, 'api_user', None)
        tz_name = getattr(api_user, 'timezone', None)
        if tz_name:
            return str(tz_name).strip()
    except Exception:
        pass

    try:
        g_user = getattr(g, 'user', None)
        tz_name = getattr(g_user, 'timezone', None)
        if tz_name:
            return str(tz_name).strip()
    except Exception:
        pass

    try:
        return str(app.config.get('USER_TIMEZONE') or 'Europe/Moscow').strip()
    except Exception:
        return 'Europe/Moscow'


def _node_discussion_parse_dt(value):
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except Exception:
            return None
    if dt.tzinfo is None:
        # SQLite often returns timezone-aware columns as naive datetimes.
        # In this app those values are stored as UTC, so treat naive as UTC.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _node_discussion_format_dt(value, tz_name=None):
    dt = _node_discussion_parse_dt(value)
    if dt is None:
        return value if value in (None, '') else str(value)
    tz_name = str(tz_name or _node_discussion_response_timezone_name() or 'Europe/Moscow').strip()
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone('Europe/Moscow')
    return dt.astimezone(tz).isoformat()


def _node_discussion_format_dt_utc(value):
    dt = _node_discussion_parse_dt(value)
    if dt is None:
        return value if value in (None, '') else str(value)
    return dt.isoformat().replace('+00:00', 'Z')


def _localize_node_discussion_message_times(message, tz_name=None):
    """Return a copy with local timestamps for by-node GET only.

    Backward compatible: existing fields such as created_at become local-time
    strings; additional *_utc fields preserve the original UTC value.
    """
    if not isinstance(message, dict):
        return message
    result = dict(message)
    for field in ('created_at', 'accepted_at', 'pushed_at', 'ack_at'):
        value = result.get(field)
        if value in (None, ''):
            continue
        result[field + '_utc'] = _node_discussion_format_dt_utc(value)
        result[field] = _node_discussion_format_dt(value, tz_name=tz_name)
    return result

def _node_discussion_group_matches_node(group_id, node_id):
    group_id = str(group_id or '').strip()
    node_id = str(node_id or '').strip()
    return bool(group_id and node_id and group_id.startswith('node:') and group_id.rstrip('/').endswith(':' + node_id))


def _node_discussion_text_from_msg(msg, payload=None):
    payload = payload if isinstance(payload, dict) else (msg.payload_json if isinstance(msg.payload_json, dict) else {})
    for key in ('text', 'message', 'body', 'caption'):
        value = payload.get(key)
        if value not in (None, ''):
            return value
    return msg.body or ''


def _serialize_node_discussion_message(msg):
    payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
    data = dict(payload)
    text_value = _node_discussion_text_from_msg(msg, payload)
    return {
        'id': msg.id,
        'client_message_id': msg.client_message_id,
        'target_type': msg.target_type,
        'target_id': msg.target_id,
        'sender_user': msg.sender_user,
        'sender_display_name': _resolve_sender_display_name(msg.sender_user, payload),
        'title': msg.title or '',
        'body': msg.body or '',
        'text': text_value,
        'image': payload.get('image'),
        'image_url': payload.get('image_url'),
        'thread_type': payload.get('thread_type'),
        'thread_ref': payload.get('thread_ref') or payload.get('download_url'),
        'node_id': payload.get('node_id') or payload.get('node_uid') or payload.get('_id'),
        'type': payload.get('type'),
        'data': data,
        'status': msg.status,
        'created_at': msg.created_at.isoformat() if msg.created_at else None,
        'accepted_at': msg.accepted_at.isoformat() if msg.accepted_at else None,
        'pushed_at': msg.pushed_at.isoformat() if msg.pushed_at else None,
        'ack_at': msg.ack_at.isoformat() if msg.ack_at else None,
        'last_error': msg.last_error,
    }


def _extract_node_discussion_node_id(payload):
    if not isinstance(payload, dict):
        return ''

    thread_type = str(payload.get('thread_type') or '').strip()
    msg_type = str(payload.get('type') or '').strip()

    # Strict enough not to capture ordinary type='node' payloads.
    if thread_type != 'node_discussion' and msg_type not in ('node_discussion_message', 'node_discussion'):
        return ''

    node_id = (
        payload.get('node_id')
        or payload.get('node_uid')
        or payload.get('_id')
        or payload.get('raw_node_id')
    )

    if not node_id:
        for key in ('thread_ref', 'raw_node_url', 'download_url', 'node_url'):
            value = str(payload.get(key) or '')
            if '/api/raw-node/' in value:
                node_id = value.rsplit('/api/raw-node/', 1)[-1].split('?', 1)[0].split('#', 1)[0]
                break

    return str(node_id or '').strip()


def _save_node_discussion_history_message(
    node_id,
    client_message_id,
    sender_user=None,
    sender_display_name='',
    target_type='user',
    target_id='',
    text='',
    image=None,
    image_url=None,
    payload=None,
    delivery_status='accepted',
):
    node_id = str(node_id or '').strip()
    client_message_id = str(client_message_id or '').strip()
    if not node_id or not client_message_id:
        return None

    try:
        msg = NodeDiscussionMessage.query.filter_by(client_message_id=client_message_id).first()
        if msg is None:
            msg = NodeDiscussionMessage(
                node_id=node_id,
                client_message_id=client_message_id,
                created_at=datetime.now(timezone.utc),
            )
            db.session.add(msg)

        msg.node_id = node_id
        msg.sender_user = str(sender_user or '').strip() or None
        msg.sender_display_name = str(sender_display_name or '')
        msg.target_type = str(target_type or 'user').strip() or 'user'
        msg.target_id = str(target_id or '').strip()
        msg.text = str(text or '')
        msg.image = image
        msg.image_url = image_url
        msg.payload_json = payload if isinstance(payload, dict) else {}
        msg.delivery_status = str(delivery_status or 'accepted')
        db.session.commit()
        try:
            broadcast_node_discussion_change(node_id, message=_serialize_node_discussion_history_message(msg), event='message')
        except Exception:
            pass
        return msg
    except Exception as e:
        db.session.rollback()
        print('Could not save node discussion history:', e)
        return None


def _update_node_discussion_delivery_status(client_message_id, delivery_status):
    client_message_id = str(client_message_id or '').strip()
    if not client_message_id:
        return None

    try:
        msg = NodeDiscussionMessage.query.filter_by(client_message_id=client_message_id).first()
        if not msg:
            return None
        msg.delivery_status = str(delivery_status or msg.delivery_status or 'accepted')
        db.session.commit()
        try:
            broadcast_node_discussion_change(msg.node_id, message=_serialize_node_discussion_history_message(msg), event='message')
        except Exception:
            pass
        return msg
    except Exception as e:
        db.session.rollback()
        print('Could not update node discussion delivery status:', e)
        return None


def _serialize_node_discussion_history_message(msg):
    payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
    text_value = msg.text or payload.get('text') or payload.get('message') or payload.get('body') or ''
    return {
        'id': msg.id,
        'client_message_id': msg.client_message_id,
        'target_type': msg.target_type,
        'target_id': msg.target_id,
        'sender_user': msg.sender_user,
        'sender_display_name': msg.sender_display_name or _resolve_sender_display_name(msg.sender_user, payload),
        'title': payload.get('title') or '',
        'body': payload.get('body') or '',
        'text': text_value,
        'image': msg.image or payload.get('image'),
        'image_url': msg.image_url or payload.get('image_url'),
        'thread_type': 'node_discussion',
        'thread_ref': payload.get('thread_ref') or payload.get('download_url') or payload.get('raw_node_url'),
        'node_id': msg.node_id,
        'type': payload.get('type') or 'node_discussion_message',
        'data': dict(payload),
        'status': msg.delivery_status,
        'delivery_status': msg.delivery_status,
        'created_at': msg.created_at.isoformat() if msg.created_at else None,
    }


def _maybe_save_node_discussion_history_from_payload(
    *,
    target_type,
    target_id,
    title='',
    body='',
    payload=None,
    sender_user=None,
    delivery_status='accepted',
):
    if not isinstance(payload, dict):
        return None

    node_id = _extract_node_discussion_node_id(payload)
    if not node_id:
        return None

    client_message_id = str(payload.get('_client_message_id') or payload.get('client_message_id') or '').strip()
    if not client_message_id:
        return None

    text = payload.get('text') or payload.get('message') or payload.get('body') or body or ''
    sender_display_name = str(payload.get('sender_display_name') or '').strip()

    return _save_node_discussion_history_message(
        node_id=node_id,
        client_message_id=client_message_id,
        sender_user=sender_user or payload.get('sender_user'),
        sender_display_name=sender_display_name,
        target_type=target_type,
        target_id=target_id,
        text=text,
        image=payload.get('image'),
        image_url=payload.get('image_url'),
        payload=payload,
        delivery_status=delivery_status,
    )


def _get_node_discussion_messages_by_node_id(node_id, viewer_user=None):
    node_id = str(node_id or '').strip()
    if not node_id:
        return []

    viewer_keys = _node_discussion_user_keys_for_user(viewer_user) if viewer_user is not None else []
    group_ids = _node_discussion_group_ids_for_user_keys(viewer_keys) if viewer_keys else set()

    def visible(row):
        if not viewer_keys:
            # Backward compatible internal use: no viewer means trusted server-side read.
            return True
        return _node_discussion_message_visible_to_user_keys(row, viewer_keys, group_ids=group_ids)

    messages = []
    seen_client_ids = set()

    try:
        history_rows = NodeDiscussionMessage.query.filter_by(node_id=node_id).order_by(
            NodeDiscussionMessage.created_at.asc(),
            NodeDiscussionMessage.id.asc(),
        ).all()
        for msg in history_rows:
            if msg.client_message_id:
                seen_client_ids.add(msg.client_message_id)
            if not visible(msg):
                continue
            messages.append(_serialize_node_discussion_history_message(msg))
    except Exception as e:
        print('Could not read node discussion history:', e)

    # Legacy fallback for messages that were written before node_discussion_message existed.
    # Query only rows related to the viewer by indexed columns. The previous
    # implementation scanned the whole outgoing_message_log table and inspected
    # every JSON payload, which could peg CPU on busy servers.
    try:
        if viewer_keys:
            rows = _query_outgoing_rows_for_user_keys(viewer_keys, group_ids=group_ids, limit=1000, ascending=True)
        else:
            rows = OutgoingMessageLog.query.filter(
                OutgoingMessageLog.target_type.in_(('user', 'group'))
            ).order_by(OutgoingMessageLog.created_at.asc(), OutgoingMessageLog.id.asc()).limit(1000).all()

        for msg in rows:
            if msg.client_message_id in seen_client_ids:
                continue
            payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
            if (
                _extract_node_discussion_node_id(payload) == node_id
                or _node_discussion_payload_matches_node(payload, node_id)
                or _node_discussion_group_matches_node(msg.target_id, node_id)
            ):
                if not visible(msg):
                    continue
                messages.append(_serialize_node_discussion_message(msg))
                if msg.client_message_id:
                    seen_client_ids.add(msg.client_message_id)
    except Exception as e:
        print('Could not read legacy node discussion history:', e)

    return messages


def _add_node_discussion_target(targets, target_type, target_id, group_id=None, user_key=None):
    target_type = str(target_type or '').strip()
    target_id = str(target_id or '').strip()
    if not target_type or not target_id:
        return
    key = (target_type, target_id)
    if key not in targets:
        targets[key] = {
            'target_type': target_type,
            'target_id': target_id,
            'group_id': group_id if group_id is not None else (target_id if target_type == 'group' else None),
            'user_key': user_key if user_key is not None else (target_id if target_type == 'user' else None),
        }


def _node_delivery_payload_matches_node(payload, node_id):
    """Match a previously delivered node payload to a raw node id.

    Initial node delivery has type='node', not thread_type='node_discussion'.
    This is used only to discover the first discussion target.
    """
    node_id = str(node_id or '').strip()
    if not node_id:
        return False
    if isinstance(payload, dict):
        if _node_discussion_payload_matches_node(payload, node_id):
            return True
        for key in ('placed', 'node', 'payload', 'data', '_data'):
            nested = payload.get(key)
            if isinstance(nested, dict) and _node_delivery_payload_matches_node(nested, node_id):
                return True
        try:
            return node_id in json.dumps(payload, ensure_ascii=False)
        except Exception:
            return False
    if isinstance(payload, str):
        return node_id in payload or _node_discussion_value_matches_node(payload, node_id)
    return False


def _remember_node_delivery_target(node_id, target_type, target_id, sender_user=None):
    """Remember who received a node so a later empty by-node discussion can start.

    This does not alter delivery/FCM. It only stores a small target hint in the
    RawNode payload for future /api/node-discussion/by-node/<node_id>/messages POSTs.
    """
    node_id = str(node_id or '').strip()
    target_type = str(target_type or '').strip()
    target_id = str(target_id or '').strip()
    if not node_id or target_type not in ('user', 'group') or not target_id:
        return False
    try:
        obj = db.session.execute(select(RawNode).where(RawNode.node_id == node_id)).scalar_one_or_none()
        if obj is None:
            return False
        payload = obj.payload_json if isinstance(obj.payload_json, dict) else {}
        hints = payload.get('_node_message_targets')
        if not isinstance(hints, list):
            hints = []
        hint = {
            'target_type': target_type,
            'target_id': target_id,
            'sender_user': str(sender_user or '').strip(),
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        exists = False
        for old in hints:
            if isinstance(old, dict) and str(old.get('target_type') or '') == target_type and str(old.get('target_id') or '') == target_id:
                old.update(hint)
                exists = True
                break
        if not exists:
            hints.append(hint)
        payload['_node_message_targets'] = hints[-20:]
        obj.payload_json = payload
        db.session.add(obj)
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        print('Could not remember node delivery target:', e)
        return False


def _add_targets_from_node_delivery_hints(targets, hints, sender_user=None):
    sender_lower = str(sender_user or '').strip().lower()
    if not isinstance(hints, list):
        return
    for hint in hints:
        if not isinstance(hint, dict):
            continue
        target_type = str(hint.get('target_type') or '').strip()
        target_id = str(hint.get('target_id') or hint.get('user_key') or hint.get('group_id') or '').strip()
        if target_type == 'user':
            user_key = _normalize_user_key(target_id)
            if user_key and user_key.lower() != sender_lower:
                _add_node_discussion_target(targets, 'user', user_key)
        elif target_type == 'group' and target_id:
            _add_node_discussion_target(targets, 'group', target_id)

def _find_node_discussion_targets(node_id, sender_user=None):
    node_id = str(node_id or '').strip()
    sender_user = str(sender_user or '').strip()
    sender_lower = sender_user.lower()
    targets = {}

    if not node_id:
        return []

    # First use the permanent node-discussion history.
    try:
        rows = NodeDiscussionMessage.query.filter_by(node_id=node_id).order_by(
            NodeDiscussionMessage.created_at.asc(),
            NodeDiscussionMessage.id.asc(),
        ).all()

        for msg in rows:
            target_type = str(msg.target_type or '').strip()
            target_id = str(msg.target_id or '').strip()
            payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}

            if target_type == 'group':
                _add_node_discussion_target(targets, 'group', target_id)
                continue

            if target_type == 'user':
                original_sender = str(msg.sender_user or payload.get('sender_user') or '').strip()
                reply_target = target_id

                # If the current API user was the previous recipient, answer back to the previous sender.
                if sender_lower and target_id.lower() == sender_lower and original_sender and original_sender.lower() != sender_lower:
                    reply_target = original_sender

                if reply_target:
                    _add_node_discussion_target(targets, 'user', reply_target)
    except Exception as e:
        print('Could not find node discussion targets from history:', e)

    if targets:
        return list(targets.values())

    # Legacy fallback: old messages kept only in OutgoingMessageLog.
    try:
        rows = OutgoingMessageLog.query.filter(
            OutgoingMessageLog.target_type.in_(('user', 'group'))
        ).order_by(OutgoingMessageLog.created_at.asc(), OutgoingMessageLog.id.asc()).all()

        for msg in rows:
            payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
            if not (
                _extract_node_discussion_node_id(payload) == node_id
                or _node_discussion_payload_matches_node(payload, node_id)
                or _node_delivery_payload_matches_node(payload, node_id)
                or _node_discussion_group_matches_node(msg.target_id, node_id)
            ):
                continue

            target_type = str(msg.target_type or '').strip()
            target_id = str(msg.target_id or '').strip()
            if target_type == 'group':
                _add_node_discussion_target(targets, 'group', target_id)
                continue

            if target_type == 'user':
                original_sender = str(msg.sender_user or payload.get('sender_user') or '').strip()
                reply_target = target_id
                if sender_lower and target_id.lower() == sender_lower and original_sender and original_sender.lower() != sender_lower:
                    reply_target = original_sender
                if reply_target:
                    _add_node_discussion_target(targets, 'user', reply_target)
    except Exception as e:
        print('Could not find node discussion targets from legacy log:', e)

    # Existing legacy synthetic groups: node:<class>:<node_id>
    try:
        for group in MessageGroup.query.filter(MessageGroup.group_id.like('node:%')).all():
            if _node_discussion_group_matches_node(group.group_id, node_id):
                _add_node_discussion_target(targets, 'group', group.group_id)
    except Exception:
        pass

    return list(targets.values())


def _find_node_discussion_targets_from_raw_node(node_id, sender_user=None):
    """Best-effort initial target discovery for a brand-new node discussion.

    Used only when there is no discussion history yet and the by-node POST did
    not explicitly provide target_user/user_key/members/group_id. It keeps the
    general /api/user/... flow unchanged.
    """
    targets = {}
    node_id = str(node_id or '').strip()
    sender_lower = str(sender_user or '').strip().lower()
    if not node_id:
        return []

    try:
        obj = db.session.execute(
            select(RawNode).where(RawNode.node_id == node_id)
        ).scalar_one_or_none()
    except Exception:
        obj = None

    if obj is None:
        return []

    # Common case: an external by-node client starts a discussion on a raw node
    # owned by another API user/client. Use that owner as the first p2p target.
    try:
        owner_id = getattr(obj, 'owner_user_id', None)
        if owner_id:
            owner = db.session.execute(select(User).where(User.id == owner_id)).scalar_one_or_none()
            owner_key = _normalize_user_key(getattr(owner, 'email', None))
            if owner_key and owner_key.lower() != sender_lower:
                _add_node_discussion_target(targets, 'user', owner_key)
    except Exception:
        pass

    # Also scan the stored raw-node payload for explicit recipient hints.
    payload = obj.payload_json if isinstance(getattr(obj, 'payload_json', None), dict) else {}
    sources = [payload]
    for key in ('data', '_data', 'payload', 'node'):
        if isinstance(payload.get(key), dict):
            sources.append(payload.get(key))

    # Target hints written when the node itself was sent through /nodes-message.
    _add_targets_from_node_delivery_hints(targets, payload.get('_node_message_targets'), sender_user=sender_user)

    for source in sources:
        if not isinstance(source, dict):
            continue

        group_id = str(source.get('group_id') or source.get('discussion_group_id') or '').strip()
        target_key = str(source.get('target_key') or '').strip()
        if target_key.startswith('group:') and not group_id:
            group_id = target_key[len('group:'):].strip()
        if group_id:
            _add_node_discussion_target(targets, 'group', group_id)

        for key in ('target_user', 'user_key', 'target_key', 'target_id', 'recipient',
                    'recipient_user', 'to', 'to_user', 'peer', 'peer_user', 'receiver'):
            value = source.get(key)
            if key == 'target_key' and str(value or '').startswith('group:'):
                continue
            user_key = _normalize_user_key(value)
            if user_key and user_key.lower() != sender_lower:
                _add_node_discussion_target(targets, 'user', user_key)

    return list(targets.values())


def _create_node_discussion_targets_from_request(data, node_id, sender_user):
    targets = {}
    data = data if isinstance(data, dict) else {}
    nested = data.get('data') if isinstance(data.get('data'), dict) else {}
    sources = (data, nested)

    def _first_value(*keys):
        for source in sources:
            for key in keys:
                value = source.get(key)
                if value not in (None, ''):
                    return value
        return None

    group_id = str(_first_value('group_id', 'discussion_group_id') or '').strip()
    members = _first_value('members', 'participants')

    raw_target_key = str(_first_value('target_key') or '').strip()
    if raw_target_key.startswith('group:') and not group_id:
        group_id = raw_target_key[len('group:'):].strip()

    if group_id:
        group = MessageGroup.query.filter_by(group_id=group_id).first()
        if group is None:
            group = MessageGroup(
                group_id=group_id,
                title=data.get('title') or nested.get('title') or f'Node discussion: {node_id}',
                created_by=sender_user or None,
            )
            db.session.add(group)
            db.session.flush()

        for member_key in _normalize_member_user_keys(members, include_user_key=sender_user):
            exists = MessageGroupMember.query.filter_by(group_id=group.group_id, user_key=member_key).first()
            if not exists:
                db.session.add(MessageGroupMember(group_id=group.group_id, user_key=member_key))

        group.updated_at = datetime.now(timezone.utc)
        db.session.commit()
        _add_node_discussion_target(targets, 'group', group.group_id)

    elif isinstance(members, (list, tuple, set)) and members:
        group = MessageGroup(
            group_id=_make_group_id(),
            title=data.get('title') or nested.get('title') or f'Node discussion: {node_id}',
            created_by=sender_user or None,
        )
        db.session.add(group)
        db.session.flush()

        for member_key in _normalize_member_user_keys(members, include_user_key=sender_user):
            db.session.add(MessageGroupMember(group_id=group.group_id, user_key=member_key))

        db.session.commit()
        _add_node_discussion_target(targets, 'group', group.group_id)

    for source in sources:
        for key in ('target_user', 'user_key', 'target_key', 'target_id', 'recipient',
                    'recipient_user', 'to', 'to_user', 'peer', 'peer_user', 'receiver'):
            value = source.get(key)
            if key == 'target_key' and str(value or '').startswith('group:'):
                continue
            user_key = _normalize_user_key(value)
            if user_key:
                _add_node_discussion_target(targets, 'user', user_key)

    return list(targets.values())

@app.route('/api/node-discussion/by-node/<path:node_id>/messages', methods=['GET'])
@api_auth_required
def api_node_discussion_messages_by_node_id(node_id):
    viewer_user = getattr(g, 'api_user', None) or (_get_sender_user(None) or None)
    messages = _get_node_discussion_messages_by_node_id(node_id, viewer_user=viewer_user)
    tz_name = _node_discussion_response_timezone_name()
    messages = [_localize_node_discussion_message_times(m, tz_name=tz_name) for m in messages]
    _node_discussion_debug('route_get_by_node.result', node_id=str(node_id or '').strip(), count=len(messages), timezone=tz_name)
    return jsonify(messages), 200


@app.route('/api/node-discussion/by-node/<path:node_id>/messages', methods=['POST'])
@api_auth_required
def api_post_node_discussion_message_by_node_id(node_id):
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}

    # Allow the external by-node client to pass first-discussion targets either
    # in JSON body or in query string. Existing clients are unaffected.
    for _key in (
        'target_user', 'user_key', 'target_key', 'target_id', 'recipient',
        'recipient_user', 'to', 'to_user', 'peer', 'peer_user', 'receiver',
        'group_id', 'discussion_group_id'
    ):
        if data.get(_key) in (None, '') and request.args.get(_key) not in (None, ''):
            data[_key] = request.args.get(_key)

    # Sender identity is always taken from the authenticated API user/session.
    # Do not trust sender_user from JSON; otherwise one client can spoof another.
    sender_user = _get_sender_user(None)
    sender_display_name = _get_sender_display_name(sender_user)

    text = data.get('text')
    if text is None:
        text = data.get('message')
    if text is None:
        text = data.get('body')
    image = data.get('image')
    image_url = data.get('image_url')

    if text in (None, '') and image in (None, '') and image_url in (None, ''):
        return jsonify({'ok': False, 'error': 'text_or_image_required', 'node_id': node_id}), 400

    api_user = getattr(g, 'api_user', None)
    if not _user_can_access_node_discussion_id(node_id, user_obj=api_user):
        return jsonify({'ok': False, 'error': 'forbidden', 'node_id': str(node_id or '').strip(), 'results': []}), 403

    targets = _find_node_discussion_targets(node_id, sender_user=sender_user)
    if not targets:
        targets = _create_node_discussion_targets_from_request(data, node_id, sender_user)
    if not targets:
        targets = _find_node_discussion_targets_from_raw_node(node_id, sender_user=sender_user)

    if not targets:
        return jsonify({
            'ok': False,
            'error': 'node_discussion_target_required',
            'details': 'No existing discussion target was found. For the first by-node message provide target_user/user_key/target_key/recipient/to/peer, or members/group_id.',
            'node_id': str(node_id or '').strip(),
            'results': [],
        }), 400

    thread_ref = _node_discussion_thread_ref(node_id, data.get('thread_ref'))
    message_type = 'image' if image not in (None, '') or image_url not in (None, '') else 'text'
    # Important: Android/iOS chat clients already handle node discussion replies
    # as ordinary chat messages with thread_type='node_discussion'. Do not send
    # type='node_discussion_message' to FCM clients: they may ignore that custom
    # type. Keep node_id/node_uid for server-side history, and thread_ref for
    # older clients that identify the discussion by raw-node URL.
    payload = {
        'type': message_type,
        'thread_type': 'node_discussion',
        'thread_ref': thread_ref,
        'node_id': str(node_id or '').strip(),
        'node_uid': str(node_id or '').strip(),
        'text': text or '',
    }
    if image is not None:
        payload['image'] = image
    if image_url is not None:
        payload['image_url'] = image_url
    if sender_user:
        payload['sender_user'] = sender_user
    if sender_display_name:
        payload['sender_display_name'] = sender_display_name

    extra = data.get('data')
    if isinstance(extra, dict):
        payload.update(extra)
        # Preserve the externally visible chat type expected by clients.
        # Extra data is allowed, but not allowed to change the discussion marker
        # or node identity/sender.
        payload['type'] = message_type
        payload.pop('message_type', None)
        payload['thread_type'] = 'node_discussion'
        payload['thread_ref'] = thread_ref
        payload['node_id'] = str(node_id or '').strip()
        payload['node_uid'] = str(node_id or '').strip()
    if sender_user:
        payload['sender_user'] = sender_user
    if sender_display_name:
        payload['sender_display_name'] = sender_display_name

    title = data.get('title') or sender_display_name or 'Обсуждение узла'
    body = text or data.get('body') or data.get('message') or 'New message'

    results = []
    delivery_ok_count = 0
    accepted_count = 0
    saved_messages = []

    for target in targets:
        target_type = target.get('target_type')
        target_id = target.get('target_id')
        item_payload = dict(payload)

        if target_type == 'group':
            item_payload['group_id'] = target_id
            result = send_message_to_group_global(
                target_id,
                title,
                body,
                item_payload,
                sender_user=sender_user,
            )
        elif target_type == 'user':
            item_payload['user_key'] = target_id
            result = send_message_to_user_global(
                target_id,
                title,
                body,
                item_payload,
                sender_user=sender_user,
            )
        else:
            result = {'ok': False, 'error': 'unsupported_target_type'}

        client_message_id = result.get('client_message_id') if isinstance(result, dict) else None
        history_msg = None
        if client_message_id:
            try:
                history_msg = NodeDiscussionMessage.query.filter_by(client_message_id=client_message_id).first()
            except Exception:
                history_msg = None

        if history_msg:
            accepted_count += 1
            saved_messages.append(_serialize_node_discussion_history_message(history_msg))

        if bool((result or {}).get('ok')):
            delivery_ok_count += 1

        results.append({
            'target_type': target_type,
            'target_id': target_id,
            'client_message_id': client_message_id,
            # by-node success is about history acceptance; delivery result is separate.
            'ok': bool(history_msg),
            'delivery_ok': bool((result or {}).get('ok')),
            'history_saved': bool(history_msg),
            'result': result,
        })

    return jsonify({
        'ok': accepted_count > 0,
        'node_id': node_id,
        'thread_ref': thread_ref,
        'count': len(saved_messages),
        'accepted_count': accepted_count,
        'delivery_ok_count': delivery_ok_count,
        'ok_count': delivery_ok_count,
        'messages': saved_messages,
        'results': results,
    }), (200 if accepted_count > 0 else 400)

def _is_group_target(target_key):
    return str(target_key or '').startswith('group:')


def _extract_group_id(target_key):
    return str(target_key or '')[len('group:'):].strip()


def _build_node_message_payload(
    *,
    class_name,
    node_id,
    download_url,
    sender_user=None,
    sender_display_name=None,
    group_id=None,
    thread_ref=None,
    text='Node',
    client_message_id=None,
):
    payload = {
        'type': 'node',
        'text': text or 'Node',
        'class_name': class_name,
        'node_id': node_id,
        'node_uid': node_id,
        'download_url': download_url,
        '_client_message_id': client_message_id or uuid.uuid4().hex,
    }

    if sender_user:
        payload['sender_user'] = sender_user
    if sender_display_name:
        payload['sender_display_name'] = sender_display_name
    if group_id:
        payload['group_id'] = group_id
    # ВАЖНО: отправка самого узла через /nodes-message не является
    # комментарием/discussion-сообщением. Не проставляем thread_type=
    # node_discussion здесь, иначе мобильный клиент открывает обычный
    # node-message как комментарий к узлу. thread_ref/thread_type
    # выставляются только в роуте комментариев по узлу.
    if thread_ref:
        payload['thread_ref'] = thread_ref

    return payload


def _place_uploaded_node(upload_url: str, node: dict, auth=None, api_user=None):
    if not isinstance(node, dict):
        raise ValueError('request body must be a node object')

    raw_class = node.get('_class')
    class_name = _extract_node_class_name(raw_class)
    node_id = str(node.get('_id') or '').strip()
    node_data = node.get('_data', {})
    attached_nodes = node.get('_attached_nodes', [])

    if raw_class is None or (isinstance(raw_class, str) and not raw_class.strip()):
        raise ValueError('node._class is required')
    if not class_name:
        raise ValueError('node._class must contain a valid class identifier')
    if not node_id:
        raise ValueError('node._id is required')
    if not isinstance(node_data, dict):
        raise ValueError('node._data must be an object')
    if not isinstance(attached_nodes, list):
        raise ValueError('node._attached_nodes must be an array')

    current_base = _current_public_base_url()
    target_base = _normalized_base_url(upload_url) or current_base
    same_server = _same_server_base_url(target_base, current_base)
    raw_node_url = _raw_node_public_url(target_base, node_id)

    # Пишем ссылку скачивания внутрь данных самого узла до сохранения/выгрузки,
    # чтобы получатель скачивал уже самодостаточный payload с _data._download_url.
    node_data = dict(node_data)
    node_data['_download_url'] = raw_node_url

    # _class сохраняем как есть: строка или JSON object
    payload = {
        '_class': raw_class,
        '_id': node_id,
        '_data': node_data,
        '_attached_nodes': attached_nodes,
    }

    if same_server:
        _save_raw_node_local(
            node_id=node_id,
            payload=payload,
            owner_user_id=getattr(api_user, 'id', None),
            content_type='node',
        )
    else:
        resp = requests.post(
            _raw_node_public_url(target_base, node_id),
            json={
                'payload': payload,
                'content_type': 'node',
            },
            auth=auth,
            timeout=20,
        )
        if resp.status_code >= 300:
            raise RuntimeError(f'remote upload failed: {resp.status_code} {resp.text}')

    return {
        '_class': raw_class,
        '_class_name': class_name,
        '_id': node_id,
        'raw_node_url': raw_node_url,
    }


@app.route('/api/user/<target_key>/nodes-message', methods=['POST'])
@api_auth_required
def push_user_nodes_message(target_key):
    data = request.get_json(silent=True) or {}

    upload_url = _normalized_base_url(data.get('upload_url')) or _current_public_base_url()

    auth = None
    req_auth = request.authorization
    if req_auth:
        auth = (req_auth.username, req_auth.password)

    api_user = getattr(g, 'api_user', None)

    try:
        placed = _place_uploaded_node(upload_url, data, auth=auth, api_user=api_user)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

    # A raw-node message belongs to the authenticated API user, not to a
    # user-provided sender_user field in the JSON body.
    sender_user = _get_sender_user(None)
    sender_display_name = _get_sender_display_name(sender_user)

    class_name = placed['_class_name']
    node_id = placed['_id']
    download_url = placed['raw_node_url']

    group_id = None
    if _is_group_target(target_key):
        group_id = _extract_group_id(target_key)
        if not group_id:
            return jsonify({'ok': False, 'error': 'group id is empty'}), 400

    message_data = _build_node_message_payload(
        class_name=class_name,
        node_id=node_id,
        download_url=download_url,
        sender_user=sender_user,
        sender_display_name=sender_display_name,
        group_id=group_id,
        thread_ref=data.get('thread_ref'),
        text=data.get('text') or data.get('body') or 'Node',
    )

    title = data.get('title') or class_name or node_id or 'Node'
    body = data.get('body') or data.get('text') or 'Node'

    # Remember the recipient of the node itself. If a by-node discussion is
    # started later on an otherwise empty thread, this is the first target.
    try:
        if group_id:
            _remember_node_delivery_target(node_id, 'group', group_id, sender_user=sender_user)
        else:
            _remember_node_delivery_target(node_id, 'user', target_key, sender_user=sender_user)
    except Exception as e:
        print('Could not remember nodes-message target:', e)

    if group_id:
        result = send_message_to_group_global(
            group_id,
            title,
            body,
            message_data,
            sender_user=sender_user,
        )
    else:
        result = send_message_to_user_global(
            target_key,
            title,
            body,
            message_data,
            sender_user=sender_user,
        )

    return jsonify({
        'ok': bool(result.get('ok')),
        'target_key': target_key,
        'user_key': None if group_id else target_key,
        'group_id': group_id,
        'upload_url': upload_url,
        'placed': {
            '_class': placed['_class'],
            '_id': node_id,
            'raw_node_url': download_url,
        },
        'message': message_data,
        'result': result,
    }), (200 if result.get('ok') else 400)









#Rooms






def get_connected_users(room_uid):
    """Returns a list of all connected users in the room."""
    if room_uid not in active_connections:
        return []
    
    users = []
    for username, ws in active_connections[room_uid].items():
        users.append({
            'user': username,
            'email': username,  
            'connection_time': datetime.now(timezone.utc).isoformat(),
            'status': 'connected'
        })
    return users

# Websocket handlers
def handle_websocket(ws, room_uid):
    with app.app_context():
        room = Room.query.filter_by(uid=room_uid).first()
    if room and (room.transport or 'websocket') != 'websocket':
        try:
            ws.send(json.dumps({'error': 'This room uses FCM transport'}))
        except Exception:
            pass
        try:
            ws.close()
        except Exception:
            pass
        return

    print(f"New connection for room {room_uid}")
    user = None
    
    try:

        auth_header = ws.environ.get('HTTP_AUTHORIZATION')
        active_connections[room_uid][user] = ws
        user_connected_message = {
            'type': 'user_connected',
            'user': user,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        for conn_user, conn_ws in list(active_connections[room_uid].items()):
            try:
                if not conn_ws.closed:
                    conn_ws.send(json.dumps(user_connected_message))
            except WebSocketError:
                active_connections[room_uid].pop(conn_user, None)

        auth_success=False
        if auth_header and auth_header.startswith('Basic '):
             try:
                 credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
                 username, password = credentials.split(':', 1)
                
                
                 with app.app_context():
                     user_obj = db.session.execute(
                         select(User).where(User.email == username)
                     ).scalar_one_or_none()
                    
                     if not user_obj or not check_password_hash(user_obj.password, password):
                         ws.close(code=4001)
                         return
                       
                     
                     user = user_obj.email
                     auth_success=True

                     #android_id = ws.environ.get('HTTP_ANDROID_ID')
                     #device_model = ws.environ.get('HTTP_DEVICE_MODEL')

                     query = parse_qs(ws.environ.get('QUERY_STRING', ''))
                     android_id = query.get('android_id', [None])[0]
                     device_model = query.get('device_model', [None])[0]

                     if android_id:
                        with app.app_context():
                            device = UserDevice.query.filter_by(user_id=user_obj.id, android_id=android_id).first()
                            if not device:
                                device = UserDevice(
                                    user_id=user_obj.id,
                                    android_id=android_id,
                                    device_model=device_model or "Unknown"
                                )
                                db.session.add(device)
                            else:
                                device.device_model = device_model or device.device_model
                                device.last_connected = datetime.now(timezone.utc)
                            db.session.commit()   

                     print(f"Authenticated user: {user}")
                   
             except Exception as e:
                 print(f"Auth error: {str(e)}")
                 ws.close(code=4001)
                 return
        else:
             
             #ws.close(code=4001)
             #return    
             pass

        
        init_message = ws.receive()
        if not init_message:
            return
            
        try:
            data = json.loads(init_message)
            if data.get('type') != 'connection':
                raise ValueError("First message must be connection type")
                
            user = data.get('user')
            if not user:
                raise ValueError("User not specified")
                
            
            active_connections[room_uid][user] = ws
            print(f"User {user} connected to room {room_uid}")
            
            
            is_debug_room = False
            room_name = ""
            with app.app_context():
                room = Room.query.filter_by(uid=room_uid).first()
                if room:
                    is_debug_room = ('debug' in room.name.lower() or room.name == 'Debug room')
                    room_name = room.name
            
            
            room_info = {
                'type': 'room_info',
                'is_debug_room': is_debug_room,
                'room_name': room_name,
                'room_uid': room_uid,
                'message': f'Connection to the room  "{room_name}" has been established'
            }
            ws.send(json.dumps(room_info))
            
            
            if is_debug_room:
                debug_message = {
                    'type': 'debug_connected',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'user': user,
                    'message': f'The user {user}  has connected to the debug room.'
                }
                
                
                for conn_user, conn_ws in list(active_connections[room_uid].items()):
                    try:
                        if not conn_ws.closed:
                            conn_ws.send(json.dumps(debug_message))
                    except WebSocketError:
                        active_connections[room_uid].pop(conn_user, None)
            
            
            if not is_debug_room and auth_success:
                send_tasks_update(room_uid)
                send_nodes_update(room_uid, user)
            
            
            while True:
                message = ws.receive()
                if message is None:
                    break
                    
                try:
                    data = json.loads(message)
                    handle_ws_command(room_uid, user, data, auth_success)
                except json.JSONDecodeError:
                    print(f"Invalid JSON from {user}")
                    ws.send(json.dumps({'error': 'Invalid JSON format'}))
                
                #time.sleep(0.1)    

        except (ValueError, json.JSONDecodeError) as e:
            print(f"Connection error: {str(e)}")
            ws.send(json.dumps({'error': str(e)}))
            
    except WebSocketError as e:
        print(f"WebSocket error: {str(e)}")
    finally:
        
        if user and room_uid in active_connections:
            active_connections[room_uid].pop(user, None)
            
            user_disconnected_message = {
                'type': 'user_disconnected',
                'user': user,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            for conn_user, conn_ws in list(active_connections[room_uid].items()):
                try:
                    if not conn_ws.closed:
                        conn_ws.send(json.dumps(user_disconnected_message))
                except WebSocketError:
                    active_connections[room_uid].pop(conn_user, None)

        if not ws.closed:
            ws.close()
        print(f"Connection closed for {user} in room {room_uid}")

def send_nodes_update(room_uid, user_id=None):
    """Sends objects to room clients, excluding those already confirmed"""
    with app.app_context():
        query = RoomObjects.query.filter_by(room_uid=room_uid)
        
        objects = query.order_by(RoomObjects.created_at.desc()).all()
        
        if room_uid in active_connections:
            for user, ws in list(active_connections[room_uid].items()):
                
                if not user:
                    continue
                
                if user_id and user != user_id:
                    continue
                    
                objects_data = []
                for obj in objects:
                    
                    if user in (obj.acknowledged_by or []):
                        continue
                        
                    objects_data.append({
                        'object_id': obj.id,  
                        'config_uid': obj.config_uid,
                        'class_name': obj.class_name,
                        'objects': obj.objects_data,
                        'created_at': obj.created_at.isoformat()
                    })
                
                if objects_data:  
                    try:
                        if not ws.closed:
                            ws.send(json.dumps({
                                'type': 'nodes_update',
                                'objects': objects_data
                            }))
                    except WebSocketError:
                        active_connections[room_uid].pop(user, None)
                        print(f"Removed dead connection for {user}")       

def send_tasks_update(room_uid):
    """Sends a task update to all clients in the room"""
    with app.app_context():  
        with SqliteDict(TASKS_DB_PATH) as tasks_db:
            tasks = tasks_db.get(room_uid, [])
            active_tasks = [t for t in tasks if not t.get('_done') and not t.get('_blocked')]
            
            if room_uid in active_connections:
                for user, ws in list(active_connections[room_uid].items()):
                    try:
                        if not ws.closed: 
                            ws.send(json.dumps({
                                'type': 'tasks_update',
                                'data': active_tasks
                            }))
                    except WebSocketError:
                        
                        active_connections[room_uid].pop(user, None)
                        print(f"Removed dead connection for {user}")


# API for working with tasks
@app.route('/api/room/<room_uid>/tasks', methods=['POST'])
@api_auth_required
def add_tasks(room_uid):
    if not request.is_json:
        abort(400, description="Request must be JSON")
    
    tasks = request.json
    if not isinstance(tasks, list):
        abort(400, description="Tasks should be an array")
    
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
       
        for task in tasks:
            if not isinstance(task, dict):
                continue
                
            if 'uid' not in task:
                task['uid'] = str(uuid.uuid4())
            task['_created'] = datetime.now(timezone.utc).isoformat()
            room_tasks.append(task)
        
        tasks_db[room_uid] = room_tasks
        tasks_db.commit()
        
        # Sending an update via websocket
        active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
        send_tasks_update(room_uid)
    
    return jsonify({"status": "success", "count": len(tasks)})

@app.route('/api/room/<room_uid>/tasks/available', methods=['GET'])
@api_auth_required
def get_available_task(room_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        # Find the first available task
        for i, task in enumerate(room_tasks):
            if not task.get('_done') and not task.get('_blocked'):
                # Mark as blocked
                room_tasks[i]['_blocked'] = True
                room_tasks[i]['_blocked_at'] = datetime.now(timezone.utc).isoformat()
                tasks_db[room_uid] = room_tasks
                tasks_db.commit()
                
                # Sending an update via websocket
                active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
                send_tasks_update(room_uid)
                
                return jsonify(task)
    
    return jsonify({"status": "no_tasks_available"}), 404

@app.route('/api/room/<room_uid>/tasks/<task_uid>/complete', methods=['POST'])
@api_auth_required
def complete_task(room_uid, task_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        for i, task in enumerate(room_tasks):
            if task.get('uid') == task_uid:
                room_tasks[i]['_done'] = True
                room_tasks[i]['_completed_at'] = datetime.now(timezone.utc).isoformat()
                tasks_db[room_uid] = room_tasks
                tasks_db.commit()
                
                # Sending an update via websocket
                active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
                send_tasks_update(room_uid)
                
                return jsonify({"status": "success"})
    
    return jsonify({"status": "task_not_found"}), 404

@app.route('/api/room/<room_uid>/tasks/completed', methods=['DELETE'])
@api_auth_required
def clear_completed_tasks(room_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        # We leave only unfinished tasks
        updated_tasks = [t for t in room_tasks if not t.get('_done')]
        tasks_db[room_uid] = updated_tasks
        tasks_db.commit()
        
        # Sending an update via websocket
        active_tasks = [t for t in updated_tasks if not t.get('_done') and not t.get('_blocked')]
        send_tasks_update(room_uid)
    
    return jsonify({"status": "success", "remaining": len(updated_tasks)})


#Personal account


def _contract_total_object_count(contract: Contract):
    try:
        live_items = _load_live_contract_snapshot(contract)[1] or {}
    except Exception:
        live_items = {}
    try:
        pushed_items = _load_pushed_contract_snapshot(contract) or {}
    except Exception:
        pushed_items = {}
    merged = dict(live_items)
    merged.update(pushed_items)
    return len(merged)












''

#API
@app.route('/api/config/<uid>')
def get_config(uid):
    #import json
    # Access control:
    # - if basic auth provided: require can_api and config access
    # - else if user logged in: require (can_client OR can_designer) and config access
    #auth = request.authorization
    #if auth:
    #    user = check_api_auth(auth.username, auth.password)
    #    if not user or not bool(getattr(user, 'can_api', False)) or not user_can_access_config(user, uid):
    #        return jsonify({'error': 'Forbidden'}), 403
    #else:
    #    if not getattr(current_user, 'is_authenticated', False):
    #        return jsonify({'error': 'Unauthorized'}), 401
    #    if not (bool(getattr(current_user, 'can_client', False)) or bool(getattr(current_user, 'can_designer', False))):
    #        return jsonify({'error': 'Forbidden'}), 403
    #    if not user_can_access_config(current_user, uid) and not db.session.execute(select(Configuration).where(Configuration.uid==uid, Configuration.user_id==current_user.id)).scalar_one_or_none():
    #        return jsonify({'error': 'Forbidden'}), 403

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()

    if not config:
        abort(404)
    
    provider = (config.user.config_display_name 
               if config.user and hasattr(config.user, 'config_display_name') 
               else (config.user.email if config.user else 'Unknown'))
    
    local_time = config.last_modified.astimezone(g.user_timezone)

    base_url = url_for('get_config', uid=config.uid, _external=True)
    
    return json.dumps({
        'name': config.name,
        'server_name': config.server_name,
        'uid': config.uid,
        'url':base_url,
        "content_uid": config.content_uid,
        'nodes_handlers': config.nodes_handlers,
        'version': getattr(config, 'version', '00.00.01'),
        'last_modified': local_time.isoformat(),
        "NodaLogicFormat": NL_FORMAT,
        "NodaLogicType": "ANDROID_SERVER",
        'provider': config.vendor,
        'classes': [
            {
                'name': c.name,
                'section': c.section,
                'section_code': c.section_code,
                'has_storage': c.has_storage,
                'display_name': c.display_name,
                'record_view': getattr(c, 'record_view', '') or '',
                'cover_image': c.cover_image,
                'display_image_web': getattr(c, 'display_image_web', '') or '',
                'display_image_table': getattr(c, 'display_image_table', '') or '',
                'init_screen_layout': getattr(c, 'init_screen_layout', '') or '',
                'init_screen_layout_web': getattr(c, 'init_screen_layout_web', '') or '',
                'plug_in': getattr(c, 'plug_in', '') or '',
                'plug_in_web': getattr(c, 'plug_in_web', '') or '',
                'init_screen_layout_web': getattr(c, 'init_screen_layout_web', '') or '',
                'plug_in': getattr(c, 'plug_in', '') or '',
                'plug_in_web': getattr(c, 'plug_in_web', '') or '',

                'commands': getattr(c, 'commands', '') or '',
                'use_standard_commands': bool(getattr(c, 'use_standard_commands', True)),
                'svg_commands': getattr(c, 'svg_commands', '') or '',
                # Migration tab
                'migration_register_command': bool(getattr(c, 'migration_register_command', False)),
                'migration_register_on_save': bool(getattr(c, 'migration_register_on_save', False)),
                'migration_default_room_uid': getattr(c, 'migration_default_room_uid', '') or '',
                'migration_default_room_alias': getattr(c, 'migration_default_room_alias', '') or '',
                'link_share_mode': getattr(c, 'link_share_mode', '') or '',
                'indexes': getattr(c, 'indexes_json', None) or [],
                'class_type': c.class_type,
                'hidden': c.hidden,
                'methods': [{
                    'name': m.name,
                    'source': m.source,
                    'engine': m.engine,
                    'code': m.code
                } for m in c.methods],
                'events': [
                    {
                        'event': e.event,
                        'listener': e.listener,
                        'actions': [
                            {
                                'action': a.action,
                                'source': a.source,
                                'server': a.server,
                                'method': a.method,
                                'postExecuteMethod': a.post_execute_method,
                                # NodaScript texts (plain JSON-escaped strings)
                                **({"methodText": a.method_text} if _is_script_text_method(a.method) else {}),
                                **({"postExecuteMethodText": a.post_execute_text} if _is_script_text_method(a.post_execute_method) else {}),
                                **({"httpFunctionName": a.http_function_name} if _is_http_request_method(a.method) else {}),
                                **({"postHttpFunctionName": a.post_http_function_name} if _is_http_request_method(a.post_execute_method) else {}),
                            }
                            for a in e.actions
                        ]
                    }
                    for e in c.event_objs
                ]
            } for c in config.classes
        ],
        'datasets': [
            {
                'name': d.name,
                'hash_indexes': d.hash_indexes.split(',') if d.hash_indexes else [],
                'text_indexes': d.text_indexes.split(',') if d.text_indexes else [],
                'view_template': d.view_template,
                'autoload': d.autoload,
                'created_at': d.created_at.isoformat(),
                'updated_at': d.updated_at.isoformat(),
                'api_url':f"{base_url}/dataset/{d.name}/items",
                'item_count': len(d.items)
            } for d in config.datasets
        ],
        'sections': [
            {
                'name': d.name,
                'code': d.code,
                'commands': d.commands
            } for d in config.sections
        ],
        "servers": [
            {"alias": s.alias, "url": s.url, "is_default": s.is_default}
            for s in config.servers
        ],
        "rooms": [
            {"alias": ra.alias, "room_id": ra.room_uid}
            for ra in (getattr(config, 'room_aliases', None) or [])
        ],
        'CommonEvents': [
            {
                'event': e.event,
                'listener': e.listener,
                'actions': [
                    {
                        'action': a.action,
                        'source': a.source,
                        'server': a.server,
                        'method': a.method,
                        'postExecuteMethod': a.post_execute_method,
                        # NodaScript texts (plain JSON-escaped strings)
                        **({"methodText": a.method_text} if _is_script_text_method(a.method) else {}),
                        **({"postExecuteMethodText": a.post_execute_text} if _is_script_text_method(a.post_execute_method) else {}),
                        **({"httpFunctionName": a.http_function_name} if _is_http_request_method(a.method) else {}),
                        **({"postHttpFunctionName": a.post_http_function_name} if _is_http_request_method(a.post_execute_method) else {}),
                    }
                    for a in e.actions
                ]
            }
            for e in config.config_events
        ]
    }, ensure_ascii=False, indent=4)


import re


def _build_runtime_parsed_config(config: Configuration) -> dict:
    """Build minimal parsed config dict needed for class events dispatch."""
    classes = {}
    try:
        for c in (config.classes or []):
            events = []
            event_objs = getattr(c, "event_objs", None) or getattr(c, "events", None) or []
            for e in (event_objs or []):
                actions = []
                for a in (getattr(e, "actions", None) or []):
                    actions.append({
                        "action": getattr(a, "action", ""),
                        "source": getattr(a, "source", ""),
                        "server": getattr(a, "server", None),
                        "method": getattr(a, "method", ""),
                        "postExecuteMethod": getattr(a, "post_execute_method", "") or getattr(a, "postExecuteMethod", ""),
                        "methodText": getattr(a, "method_text", "") or getattr(a, "methodText", ""),
                        "postExecuteMethodText": getattr(a, "post_execute_text", "") or getattr(a, "postExecuteMethodText", ""),
                        "httpFunctionName": getattr(a, "http_function_name", "") or getattr(a, "httpFunctionName", ""),
                        "postHttpFunctionName": getattr(a, "post_http_function_name", "") or getattr(a, "postHttpFunctionName", ""),
                    })
                events.append({
                    "event": getattr(e, "event", ""),
                    "listener": getattr(e, "listener", "") or "",
                    "actions": actions,
                })
            classes[getattr(c, "name", "")] = {"events": events}
    except Exception:
        pass
    return {"classes": classes}


def _compact_clean(value):
    if isinstance(value, dict):
        cleaned = {}
        for k, v in value.items():
            vv = _compact_clean(v)
            if vv in (None, "", [], {}):
                continue
            cleaned[k] = vv
        return cleaned
    if isinstance(value, list):
        cleaned = [_compact_clean(v) for v in value]
        return [v for v in cleaned if v not in (None, "", [], {})]
    return value


def _export_class_json(class_obj: ConfigClass) -> dict:
    data = {
        'name': class_obj.name,
    }

    if getattr(class_obj, 'section', None):
        data['section'] = class_obj.section
    if getattr(class_obj, 'section_code', None):
        data['section_code'] = class_obj.section_code
    if bool(getattr(class_obj, 'has_storage', False)):
        data['has_storage'] = True
    if (getattr(class_obj, 'display_name', '') or '').strip() and (class_obj.display_name or '').strip() != (class_obj.name or '').strip():
        data['display_name'] = class_obj.display_name
    if (getattr(class_obj, 'record_view', '') or '').strip():
        data['record_view'] = class_obj.record_view
    if (getattr(class_obj, 'cover_image', '') or '').strip():
        data['cover_image'] = class_obj.cover_image
    if (getattr(class_obj, 'display_image_web', '') or '').strip():
        data['display_image_web'] = class_obj.display_image_web
    if (getattr(class_obj, 'display_image_table', '') or '').strip():
        data['display_image_table'] = class_obj.display_image_table
    if (getattr(class_obj, 'init_screen_layout', '') or '').strip():
        data['init_screen_layout'] = class_obj.init_screen_layout
    if (getattr(class_obj, 'init_screen_layout_web', '') or '').strip():
        data['init_screen_layout_web'] = class_obj.init_screen_layout_web
    if (getattr(class_obj, 'plug_in', '') or '').strip():
        data['plug_in'] = class_obj.plug_in
    if (getattr(class_obj, 'plug_in_web', '') or '').strip():
        data['plug_in_web'] = class_obj.plug_in_web
    if (getattr(class_obj, 'commands', '') or '').strip():
        data['commands'] = class_obj.commands
    if bool(getattr(class_obj, 'use_standard_commands', True)) is False:
        data['use_standard_commands'] = False
    if (getattr(class_obj, 'svg_commands', '') or '').strip():
        data['svg_commands'] = class_obj.svg_commands
    if bool(getattr(class_obj, 'migration_register_command', False)):
        data['migration_register_command'] = True
    if bool(getattr(class_obj, 'migration_register_on_save', False)):
        data['migration_register_on_save'] = True
    if (getattr(class_obj, 'migration_default_room_uid', '') or '').strip():
        data['migration_default_room_uid'] = class_obj.migration_default_room_uid
    if (getattr(class_obj, 'migration_default_room_alias', '') or '').strip():
        data['migration_default_room_alias'] = class_obj.migration_default_room_alias
    if (getattr(class_obj, 'link_share_mode', '') or '').strip():
        data['link_share_mode'] = class_obj.link_share_mode
    if getattr(class_obj, 'indexes_json', None):
        data['indexes'] = class_obj.indexes_json
    if (getattr(class_obj, 'class_type', '') or '').strip():
        data['class_type'] = class_obj.class_type
    if bool(getattr(class_obj, 'hidden', False)):
        data['hidden'] = True

    methods = []
    for m in (getattr(class_obj, 'methods', None) or []):
        md = {
            'name': m.name,
            'code': m.code,
        }
        if (getattr(m, 'source', '') or '').strip() and (m.source or '').strip() != 'internal':
            md['source'] = m.source
        if (getattr(m, 'engine', '') or '').strip():
            md['engine'] = m.engine
        if (getattr(m, 'server', '') or '').strip() and (m.server or '').strip() != 'internal':
            md['server'] = m.server
        methods.append(_compact_clean(md))
    if methods:
        data['methods'] = methods

    events = []
    for e in (getattr(class_obj, 'event_objs', None) or []):
        ed = {
            'event': getattr(e, 'event', ''),
        }
        if (getattr(e, 'listener', '') or '').strip():
            ed['listener'] = e.listener
        actions = []
        for a in (getattr(e, 'actions', None) or []):
            actions.append(_compact_clean({
                'action': getattr(a, 'action', ''),
                'source': getattr(a, 'source', ''),
                'server': getattr(a, 'server', ''),
                'method': getattr(a, 'method', ''),
                'postExecuteMethod': getattr(a, 'post_execute_method', ''),
                'methodText': getattr(a, 'method_text', ''),
                'postExecuteMethodText': getattr(a, 'post_execute_text', ''),
                'httpFunctionName': getattr(a, 'http_function_name', ''),
                'postHttpFunctionName': getattr(a, 'post_http_function_name', ''),
            }))
        if actions:
            ed['actions'] = actions
        events.append(_compact_clean(ed))
    if events:
        data['events'] = events

    return _compact_clean(data)


def _normalize_contract_source_type(raw_value: str) -> str:
    val = str(raw_value or '').strip().lower()
    if val in {'class', 'global_index', 'external_only'}:
        return val
    if val in {'external', 'post', 'push', 'post_only'}:
        return 'external_only'
    return 'class'


def _object_id_from_payload(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ''
    for key in ('_id', 'id', 'uid'):
        v = payload.get(key)
        if v:
            return str(v).strip()
    data = payload.get('_data')
    if isinstance(data, dict):
        for key in ('_id', 'id', 'uid'):
            v = data.get(key)
            if v:
                return str(v).strip()
    return ''


def _object_version_from_payload(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ''
    for key in ('_updated_at', '_version', 'updated_at', 'version'):
        v = payload.get(key)
        if v:
            return str(v).strip()
    data = payload.get('_data')
    if isinstance(data, dict):
        for key in ('_updated_at', '_version', 'updated_at', 'version'):
            v = data.get(key)
            if v:
                return str(v).strip()
    return datetime.now(timezone.utc).isoformat()


def _contract_public_url(contract: Contract) -> str:
    return url_for('contract_download', contract_uid=contract.uid, _external=True)


def _contract_ack_url(contract: Contract) -> str:
    return url_for('contract_ack', contract_uid=contract.uid, _external=True)


def _contract_add_url(contract: Contract) -> str:
    return url_for('contract_add_info', contract_uid=contract.uid, _external=True)


def _contract_add_payload(contract: Contract) -> dict:
    return {
        'name': contract.name or '',
        'display_name': contract.display_name or '',
        'download_url': _contract_public_url(contract),
        'ack_url': _contract_ack_url(contract),
    }


def _contract_accessible_configs(user):
    if not user:
        return []
    shared_cfg_ids = select(UserConfigAccess.config_id).where(UserConfigAccess.user_id == user.id)
    stmt = (
        select(Configuration)
        .where(sa.or_(Configuration.user_id == user.id, Configuration.id.in_(shared_cfg_ids)))
        .order_by(Configuration.name)
    )
    return db.session.execute(stmt).scalars().all()


def _contract_update_from_data(contract: Contract, data: dict, actor) -> Contract:
    name = str((data or {}).get('name') or '').strip()
    if not name:
        raise ValueError('Name is required')

    source_type = _normalize_contract_source_type((data or {}).get('source_type'))
    source_config_uid = str((data or {}).get('source_config_uid') or (data or {}).get('config_uid') or '').strip()
    if source_config_uid and actor and not user_can_access_config(actor, source_config_uid):
        own_cfg = db.session.execute(
            select(Configuration).where(Configuration.uid == source_config_uid, Configuration.user_id == actor.id)
        ).scalar_one_or_none()
        if own_cfg is None:
            raise PermissionError('Forbidden')

    contract.name = name
    contract.display_name = str((data or {}).get('display_name') or '').strip()
    contract.source_type = source_type
    contract.source_config_uid = source_config_uid
    contract.class_name = str((data or {}).get('class_name') or '').strip()
    contract.global_index_name = str((data or {}).get('global_index_name') or (data or {}).get('index_name') or '').strip()
    contract.global_index_value = str((data or {}).get('global_index_value') or (data or {}).get('index_value') or '').strip()
    if 'external_class_json' in (data or {}):
        contract.external_class_json = (data or {}).get('external_class_json') if isinstance((data or {}).get('external_class_json'), dict) else None
    contract.updated_at = datetime.now(timezone.utc)
    return contract


def _request_actor_for_contract_write():
    auth = request.authorization
    if auth:
        user = check_api_auth(auth.username, auth.password)
        if user and bool(getattr(user, 'can_api', False)):
            return user
    if getattr(current_user, 'is_authenticated', False):
        return current_user
    return None


def _get_owned_contract_or_404(contract_uid: str, actor=None) -> Contract:
    actor = actor or _request_actor_for_contract_write()
    if actor is None:
        abort(401)
    contract = db.session.execute(
        select(Contract).where(Contract.uid == str(contract_uid).strip())
    ).scalar_one_or_none()
    if not contract:
        abort(404)
    if int(contract.user_id) != int(actor.id):
        abort(403)
    return contract


def _load_live_contract_snapshot(contract: Contract):
    class_json = None
    items = {}

    cfg_uid = str(getattr(contract, 'source_config_uid', '') or '').strip()
    class_name = str(getattr(contract, 'class_name', '') or '').strip()
    source_type = _normalize_contract_source_type(getattr(contract, 'source_type', 'class'))
    if not cfg_uid or not class_name:
        return class_json, items

    config = db.session.execute(select(Configuration).where(Configuration.uid == cfg_uid)).scalar_one_or_none()
    if not config:
        return class_json, items

    class_obj = next((c for c in (config.classes or []) if str(c.name or '') == class_name), None)
    if class_obj is not None:
        class_json = _export_class_json(class_obj)

    runtime_parsed = _build_runtime_parsed_config(config)
    ctx_tokens = _nodes_mod.set_runtime_context(cfg_uid, runtime_parsed)
    try:
        isolated_globals = _load_server_handlers_ns(cfg_uid, config) or {}
        node_class = isolated_globals.get(class_name)
        if node_class is None:
            return class_json, items

        if source_type == 'global_index' and (str(getattr(contract, 'global_index_name', '') or '').strip()):
            idx_name = str(contract.global_index_name or '').strip()
            idx_value = str(contract.global_index_value or '').strip()
            global_finder = getattr(_nodes_mod, 'findByGlobalIndex', None) or getattr(_nodes_mod, 'find_by_global_index', None)
            global_getter = getattr(_nodes_mod, 'getByGlobalIndex', None) or getattr(_nodes_mod, 'get_by_global_index', None)
            if callable(global_finder):
                raw_nodes = global_finder(idx_name, idx_value)
                if isinstance(raw_nodes, dict):
                    iterable = list((raw_nodes or {}).values())
                elif isinstance(raw_nodes, (list, tuple, set)):
                    iterable = list(raw_nodes)
                elif raw_nodes is None:
                    iterable = []
                else:
                    iterable = [raw_nodes]
            elif callable(global_getter):
                one_node = global_getter(idx_name, idx_value)
                iterable = [one_node] if one_node is not None else []
            else:
                iterable = []
        else:
            raw_nodes = node_class.get_all(cfg_uid) or {}
            iterable = list(raw_nodes.values())

        for node in iterable:
            try:
                payload = node.to_dict()
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue

            payload_cfg = str(payload.get('_config_uid') or getattr(node, '_config_uid', '') or '').strip()
            payload_class = str(payload.get('_class') or payload.get('_data', {}).get('_class') or node.__class__.__name__ or '').strip()
            if payload_cfg and payload_cfg != cfg_uid:
                continue
            if payload_class and payload_class != class_name:
                continue

            object_id = _object_id_from_payload(payload)
            if not object_id:
                continue
            items[object_id] = {
                'payload': payload,
                'version': _object_version_from_payload(payload),
                'source': 'live',
            }
    finally:
        _nodes_mod.reset_runtime_context(ctx_tokens)

    return class_json, items


def _load_pushed_contract_snapshot(contract: Contract):
    items = {}
    for row in (getattr(contract, 'pushed_objects', None) or []):
        payload = row.payload_json if isinstance(row.payload_json, dict) else {}
        object_id = str(row.object_id or '').strip() or _object_id_from_payload(payload)
        if not object_id:
            continue
        items[object_id] = {
            'payload': payload,
            'version': str(row.object_version or (row.updated_at.isoformat() if row.updated_at else '')),
            'source': 'push',
        }
    return items


def _build_contract_delivery(contract: Contract, device_id: str = ''):
    live_class_json, live_items = _load_live_contract_snapshot(contract)
    pushed_items = _load_pushed_contract_snapshot(contract)

    items = dict(live_items or {})
    items.update(pushed_items or {})

    class_json = contract.external_class_json or live_class_json

    ack_map = {}
    if device_id:
        ack_rows = db.session.execute(
            select(ContractAck).where(ContractAck.contract_id == contract.id, ContractAck.device_id == device_id)
        ).scalars().all()
        ack_map = {str(a.object_id): str(a.object_version or '') for a in ack_rows}

    out_objects = []
    for object_id, item in sorted(items.items(), key=lambda kv: str(kv[0])):
        version = str(item.get('version') or '')
        if device_id and ack_map.get(str(object_id)) == version:
            continue
        payload = item.get('payload') if isinstance(item.get('payload'), dict) else {}
        if isinstance(payload, dict):
            out_objects.append(payload)

    return {
        '_class': class_json or {},
        '_data_objects': out_objects,
    }


def _upsert_contract_pushed_objects(contract: Contract, payload, external_class_json=None):
    if isinstance(payload, dict) and '_data_objects' in payload:
        objects = payload.get('_data_objects') or []
        external_class_json = payload.get('_class') if '_class' in payload else external_class_json
    elif isinstance(payload, dict):
        objects = [payload]
    elif isinstance(payload, list):
        objects = payload
    else:
        objects = []

    if external_class_json is not None:
        contract.external_class_json = external_class_json if isinstance(external_class_json, dict) else None

    upserted = []
    now_version = datetime.now(timezone.utc).isoformat()

    for raw in (objects or []):
        if not isinstance(raw, dict):
            continue
        object_id = _object_id_from_payload(raw)
        if not object_id:
            continue
        object_version = _object_version_from_payload(raw) or now_version
        row = db.session.execute(
            select(ContractObject).where(ContractObject.contract_id == contract.id, ContractObject.object_id == object_id)
        ).scalar_one_or_none()
        if row is None:
            row = ContractObject(
                contract_id=contract.id,
                object_id=object_id,
                payload_json=raw,
                object_version=object_version,
            )
            db.session.add(row)
        else:
            row.payload_json = raw
            row.object_version = object_version
            row.updated_at = datetime.now(timezone.utc)
        upserted.append(object_id)

    contract.updated_at = datetime.now(timezone.utc)
    return upserted


def _contract_to_dict(contract: Contract) -> dict:
    return {
        'uid': contract.uid,
        'name': contract.name,
        'display_name': contract.display_name or '',
        'source_type': contract.source_type,
        'source_config_uid': contract.source_config_uid or '',
        'class_name': contract.class_name or '',
        'global_index_name': contract.global_index_name or '',
        'global_index_value': contract.global_index_value or '',
        'download_url': _contract_public_url(contract),
        'ack_url': _contract_ack_url(contract),
        'add_url': _contract_add_url(contract),
        'created_at': contract.created_at.isoformat() if contract.created_at else None,
        'updated_at': contract.updated_at.isoformat() if contract.updated_at else None,
    }


@app.route('/api/contracts', methods=['POST'])
def create_contract_api():
    actor = _request_actor_for_contract_write()
    if actor is None:
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json(silent=True) or {}
    contract = Contract(user_id=actor.id)
    try:
        _contract_update_from_data(contract, data, actor)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except PermissionError:
        return jsonify({'error': 'Forbidden'}), 403

    db.session.add(contract)
    db.session.commit()
    return jsonify({'ok': True, 'contract': _contract_to_dict(contract)}), 201


@app.route('/api/contracts/<contract_uid>', methods=['DELETE'])
def delete_contract_api(contract_uid):
    contract = _get_owned_contract_or_404(contract_uid)
    db.session.delete(contract)
    db.session.commit()
    return jsonify({'ok': True, 'uid': contract_uid})


@app.route('/api/contracts/<contract_uid>', methods=['PUT', 'PATCH'])
def update_contract_api(contract_uid):
    actor = _request_actor_for_contract_write()
    contract = _get_owned_contract_or_404(contract_uid, actor=actor)
    data = request.get_json(silent=True) or {}
    try:
        _contract_update_from_data(contract, data, actor)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except PermissionError:
        return jsonify({'error': 'Forbidden'}), 403

    db.session.commit()
    return jsonify({'ok': True, 'contract': _contract_to_dict(contract)})


@app.route('/api/contracts/<contract_uid>', methods=['GET'])
def contract_download(contract_uid):
    contract = db.session.execute(
        select(Contract).where(Contract.uid == str(contract_uid).strip())
    ).scalar_one_or_none()
    if not contract:
        return jsonify({'error': 'Not found'}), 404

    device_id = str(request.args.get('device_id') or '').strip()
    payload = _build_contract_delivery(contract, device_id=device_id)
    return jsonify(payload)


@app.route('/api/contracts/<contract_uid>/add', methods=['GET'])
def contract_add_info(contract_uid):
    contract = db.session.execute(
        select(Contract).where(Contract.uid == str(contract_uid).strip())
    ).scalar_one_or_none()
    if not contract:
        return jsonify({'error': 'Not found'}), 404

    return jsonify(_contract_add_payload(contract))


@app.route('/api/contracts/<contract_uid>/ack', methods=['POST'])
def contract_ack(contract_uid):
    contract = db.session.execute(
        select(Contract).where(Contract.uid == str(contract_uid).strip())
    ).scalar_one_or_none()
    if not contract:
        return jsonify({'error': 'Not found'}), 404

    data = request.get_json(silent=True) or {}
    device_id = str(data.get('device_id') or request.args.get('device_id') or '').strip()
    if not device_id:
        return jsonify({'error': 'device_id is required'}), 400

    raw_ids = data.get('_ids', data.get('ids', []))
    if isinstance(raw_ids, str):
        object_ids = [raw_ids]
    else:
        object_ids = [str(x).strip() for x in (raw_ids or []) if str(x).strip()]

    live_class_json, live_items = _load_live_contract_snapshot(contract)
    pushed_items = _load_pushed_contract_snapshot(contract)
    merged = dict(live_items or {})
    merged.update(pushed_items or {})
    version_map = {str(oid): str(item.get('version') or '') for oid, item in merged.items()}

    acked = []
    for object_id in object_ids:
        row = db.session.execute(
            select(ContractAck).where(
                ContractAck.contract_id == contract.id,
                ContractAck.device_id == device_id,
                ContractAck.object_id == object_id,
            )
        ).scalar_one_or_none()
        if row is None:
            row = ContractAck(contract_id=contract.id, device_id=device_id, object_id=object_id)
            db.session.add(row)
        row.object_version = version_map.get(object_id, '')
        row.acked_at = datetime.now(timezone.utc)
        acked.append(object_id)

    db.session.commit()
    return jsonify({'ok': True, 'device_id': device_id, 'acked_ids': acked})


@app.route('/api/contracts/<contract_uid>/push', methods=['POST'])
def contract_push(contract_uid):
    contract = db.session.execute(
        select(Contract).where(Contract.uid == str(contract_uid).strip())
    ).scalar_one_or_none()
    if not contract:
        return jsonify({'error': 'Not found'}), 404

    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({'error': 'JSON body is required'}), 400

    upserted = _upsert_contract_pushed_objects(contract, payload)
    db.session.commit()
    return jsonify({'ok': True, 'updated_ids': upserted, 'count': len(upserted)})


# Best-effort creation of newly added tables when the app is imported via WSGI/flask run.
try:
    with app.app_context():
        db.create_all()
except Exception as _e:
    print('Late db.create_all skipped:', _e)

@app.route('/api/config/<config_uid>/node/<class_name>/<node_id>/<method_name>', methods=['POST'])
@api_auth_required
def execute_node_method(config_uid, class_name, node_id, method_name):
    """API for node execution"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    
    try:
        if os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers:
            isolated_globals = _load_server_handlers_ns(config_uid, config)
            
            # Check that the class exists and is a subclass of Node
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # Get the node
                node = node_class.get(node_id, config_uid)
                if not node:
                    abort(404, description=f"Node {node_id} not found")
                
                # Check the existence of the method
                if not hasattr(node, method_name):
                    abort(404, description=f"Method {method_name} not found in class {class_name}")
                
                # Getting input data
                request_data = request.get_json() or {}
                
                # Determine the method type
                custom_methods = ['_sum_transaction', '_get_sum_balance', '_get_balance', '_get_sum_transactions',
            '_state_transaction', '_get_state_balance', '_get_state_transactions',
            '_add_scheme', '_remove_scheme']
                
                if method_name in custom_methods:
                    # Handling arbitrary methods
                    args = request_data.get('args', [])
                    kwargs = request_data.get('kwargs', {})
                    
                    try:
                        result = getattr(node, method_name)(*args, **kwargs)
                        return jsonify({
                            'status': True,
                            'result': result
                        })
                    except _nodes_mod.AcceptRejected as e:

                        return jsonify({'status': False, 'data': e.payload}), 200

                    except Exception as e:
                        return jsonify({
                            'status': False,
                            'error': str(e)
                        }), 500
                
                else:
                    # Processing standard methods
                    input_data = request_data
                    
                    try:
                        if method_name == "_save":
                            if input_data:
                                node._data_cache = input_data
                            result = node._save()

                            return jsonify({
                                'status': result,
                                'node': node.to_dict()
                            })
                        else:
                            result = getattr(node, method_name)(input_data)
                            if isinstance(result, tuple) and len(result) == 2:

                                success, data = result
                                if hasattr(node, "_ui_layout") and node._ui_layout is not None:
                                    data["_ui_layout"] = node._ui_layout

                                return jsonify({'status': success, 'data': data})
                            else:
                                return jsonify(result)
                    except Exception as e:
                        return jsonify({
                            'status': False,
                            'error': str(e),
                            'node': node.to_dict()
                        }), 500
        
        abort(404, description=f"Class {class_name} not found")
        
    except Exception as e:
        return jsonify({'status': False, "error": str(e)}), 500

#API for calling remote nodes
@app.route('/api/<room_uid>/<target_user>/<config_uid>/remote_node/<class_name>/<node_id>/<method_name>', methods=['POST'])
def execute_remote_method(room_uid, target_user, config_uid, class_name, node_id, method_name):
    """Executing a method on a remote device via WebSocket"""
    
    # Check if the target user is active in the room
    if (room_uid not in active_connections or 
        target_user not in active_connections[room_uid]):
        return jsonify({
            'success': False,
            'error': f'A user {target_user} not in the room {room_uid}'
        }), 404
    
    # Get the target user's WebSocket connection
    target_ws = active_connections[room_uid][target_user]
    if target_ws.closed:
        return jsonify({
            'success': False,
            'error': f'Connection with {target_user} was closed'
        }), 404
    
    # Generate a unique request ID
    request_id = str(uuid.uuid4())
    
    # Getting input data
    input_data = request.get_json() or {}
    
    # Create a message to send
    message = {
        'type': 'remote_method',
        'request_id': request_id,
        'config_uid': config_uid,
        'class_name': class_name,
        'node_id': node_id,
        'method_name': method_name,
        'input_data': input_data,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Initialize the query in the waiting dictionary
    if room_uid not in pending_remote_requests:
        pending_remote_requests[room_uid] = {}
    
    pending_remote_requests[room_uid][request_id] = {
        'result': None,
        'error': None,
        'completed': False
    }
    
    
        
    request_id = str(uuid.uuid4())
    
    # Create a record about waiting for a response
    pending_responses[request_id] = {
        'room_uid': room_uid,
        'completed': False,
        'result': None,
        'error': None,
        'created_at': time.time()
    }
    
    # Add request_id to the message
    message['request_id'] = request_id
    
    try:
        target_ws.send(json.dumps(message))
        
        
        return jsonify({
            'success': True,
            'request_id': request_id,
            'status': 'pending',
            'message': 'The request has been sent. Use /api/check-response to check the status.'
        }), 202
        
    except WebSocketError as e:
        # Remove from pending on error
        if request_id in pending_responses:
            del pending_responses[request_id]
        return jsonify({
            'success': False,
            'error': f'WebSocket Error: {str(e)}'
        }), 500
        


@app.route('/api/check-response/<request_id>')
@api_auth_required
def check_response(request_id):
    """Checking the status of a remote request"""
    if request_id not in pending_responses:
        return jsonify({
            'status': 'not_found',
            'message': 'Request not found or expired'
        }), 404
    
    response_data = pending_responses[request_id]
    
    if response_data['completed']:
        # The request is complete, we return the result and clean up
        result = response_data
        del pending_responses[request_id]
        
        if result['error']:
            return jsonify({
                'status': 'error',
                'error': result['error']
            }), 500
        else:
            return jsonify({
                'status': 'completed',
                'data': result['result']
            })
    else:
        # The request is still in process
        elapsed = time.time() - response_data['created_at']
        return jsonify({
            'status': 'pending',
            'elapsed_seconds': round(elapsed, 1),
            'message': 'The request is still being processed.'
        })

@app.route('/api/config/<config_uid>/node/<class_name>/<node_id>', methods=['GET', 'PUT', 'DELETE'])
@api_auth_required
def node_api(config_uid, class_name, node_id):
    """API for working with a specific node"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp


    internal_id = extract_internal_id(node_id)    
    
    try:
        if os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers:
            isolated_globals = _load_server_handlers_ns(config_uid, config)
            
            # We check that the class exists and is a subclass of Node from this space
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                if request.method == 'GET':
                    node = node_class.get(internal_id , config_uid)
                    if node:
                        return jsonify(node.to_dict())
                    abort(404)
                
                elif request.method == 'PUT':
                    data = request.get_json()
                    node = node_class(internal_id , config_uid)
                    if data:
                        node.update_data(data)

    
                    return jsonify(node.to_dict())
                
                elif request.method == 'DELETE':
                    node = node_class.get(internal_id , config_uid)
                    if node:
                        node.delete()

                        return jsonify({"status": "deleted"})
                    abort(404)
        
        abort(404)
        
    except _nodes_mod.AcceptRejected as e:

        
        return jsonify({'status': False, 'data': e.payload}), 200

        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<config_uid>/node/<class_name>/register/<room_uid>', methods=['POST'])
@api_auth_required
def register_nodes(config_uid, class_name, room_uid):
    """Registers nodes of the specified class in the download room"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        # Safely retrieve JSON from the request body
        node_ids = []
        if request.data:  # Check if there is a request body
            try:
                #request_data = request.get_json() or {}
                node_ids = request.get_json() or []
            except Exception:
                # If the JSON is invalid, we assume that the body is empty.
                node_ids = []
        
        if os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers:
            isolated_globals = _load_server_handlers_ns(config_uid, config)
            
            # Checking that the class exists
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # We receive nodes
                if node_ids:
                    # We register only selected nodes
                    nodes_data = []
                    for node_id in node_ids:
                        node = node_class.get(node_id, config_uid)
                        if node:
                            node_dict = node.to_dict()
                            #node_dict['_id'] = node_id
                            node_dict = node.to_dict()
                            node_dict['_id'] = node_dict.get('_data', {}).get('_id') or node_id
                            nodes_data.append(node_dict)
                    
                    message = f"Registered {len(nodes_data)} selected nodes"
                else:
                    # Register all class nodes
                    nodes = node_class.get_all(config_uid)
                    nodes_data = []
                    for node_id, node in nodes.items():
                        node_dict = node.to_dict()
                        #node_dict['_id'] = node_id
                        node_dict = node.to_dict()
                        node_dict['_id'] = node_dict.get('_data', {}).get('_id') or node_id
                        nodes_data.append(node_dict)
                    
                    message = f"Registered all {len(nodes_data)} nodes"
                
                # We register in the room
                return  handle_room_objects(config_uid, class_name, room_uid, nodes_data)
                
                
              
                
                
        
        abort(404, description=f"Class {class_name} not found")
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<config_uid>/class_method/<class_name>/<method_name>', methods=['POST'])
@api_auth_required
def execute_class_method(config_uid, class_name, method_name):

    data = request.get_json() or {}
    date = data.get("date")

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()

    if not config:
        abort(404)

    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    try:

        isolated_globals = _load_server_handlers_ns(config_uid, config)

        if class_name not in isolated_globals:
            return jsonify({"status": False, "error": "class not found"}), 404

        node_class = isolated_globals[class_name]

        if not hasattr(node_class, method_name):
            return jsonify({"status": False, "error": "method not found"}), 404

        fn = getattr(node_class, method_name)

        if date is not None:
            result = fn(date)
        else:
            result = fn()

        return jsonify({
            "status": True,
            "result": result
        })

    except Exception as e:

        return jsonify({
            "status": False,
            "error": str(e)
        }), 500


@app.route("/api/config/<config_uid>/date_range", methods=["GET"])
@api_auth_required
def config_date_range(config_uid):
    """
    Returns min/max dates across ALL node classes for given config_uid.

    Prefers date index DBs:
      node_storage/<Class>_<config_uid>__date_index.sqlite
    Fallback: scans main storage DB:
      node_storage/<Class>_<config_uid>.sqlite

    Output date format:
      min_date_key/max_date_key: 'YYYYMMDD'
      min_date/max_date: 'YYYY-MM-DD'
    """
    import os, glob, sqlite3, pickle
    import nodes as _nodes_mod

    # ensure config exists (optional, but good)
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    if not config:
        abort(404)

    base_dir = "node_storage"
    table = "unnamed"

    def key_to_iso(dk: str | None) -> str | None:
        if not dk or len(dk) != 8:
            return None
        return f"{dk[0:4]}-{dk[4:6]}-{dk[6:8]}"

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    # discover classes by scanning storage files
    pattern = os.path.join(base_dir, f"*_{config_uid}.sqlite")
    main_files = [
        p for p in glob.glob(pattern)
        if not p.endswith("__date_index.sqlite")
    ]

    per_class = []
    global_min = None
    global_max = None

    for main_path in sorted(main_files):
        fname = os.path.basename(main_path)
        # "<Class>_<config_uid>.sqlite"  -> class_name
        suffix = f"_{config_uid}.sqlite"
        if not fname.endswith(suffix):
            continue
        class_name = fname[:-len(suffix)]

        idx_path = os.path.join(base_dir, f"{class_name}_{config_uid}__date_index.sqlite")

        cls_min = None
        cls_max = None
        used = None

        # 1) Fast path: read min/max from index
        if os.path.exists(idx_path):
            conn = sqlite3.connect(idx_path)
            try:
                cur = conn.cursor()
                # keys are "YYYYMMDD|node_id" so MIN/MAX works lexicographically
                cur.execute(f"SELECT MIN(key), MAX(key) FROM {table}")
                row = cur.fetchone()
                if row:
                    kmin, kmax = row
                    if kmin:
                        cls_min = str(kmin)[0:8]
                    if kmax:
                        cls_max = str(kmax)[0:8]
                used = "date_index"
            finally:
                conn.close()

        # 2) Fallback: scan main DB values for _data._date_key / _data._date
        if (cls_min is None and cls_max is None) and os.path.exists(main_path):
            conn = sqlite3.connect(main_path)
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT value FROM {table}")
                for (blob,) in cur.fetchall():
                    obj = unpack(blob)
                    if not isinstance(obj, dict):
                        continue
                    data = obj.get("_data") or {}
                    if not isinstance(data, dict):
                        continue
                    dk = data.get("_date_key") or _nodes_mod.normalize_date_key(data.get("_date"))
                    if not dk:
                        continue
                    # update min/max
                    if cls_min is None or dk < cls_min:
                        cls_min = dk
                    if cls_max is None or dk > cls_max:
                        cls_max = dk
                used = "scan_main_db"
            finally:
                conn.close()

        # update global min/max
        if cls_min:
            if global_min is None or cls_min < global_min:
                global_min = cls_min
        if cls_max:
            if global_max is None or cls_max > global_max:
                global_max = cls_max

        per_class.append({
            "class": class_name,
            "min_date_key": cls_min,
            "max_date_key": cls_max,
            "min_date": key_to_iso(cls_min),
            "max_date": key_to_iso(cls_max),
            "source": used,
        })

    return jsonify({
        "status": True,
        "config_uid": config_uid,
        "min_date_key": global_min,
        "max_date_key": global_max,
        "min_date": key_to_iso(global_min),
        "max_date": key_to_iso(global_max),
        "classes": per_class,
    })

@app.route("/api/config/<config_uid>/node/<class_name>/page_at_date", methods=["GET"])
@api_auth_required
def nodes_api_page_at_date(config_uid, class_name):
    """
    Fast paged nodes list up to date (inclusive) using date index.
    Query:
      date (YYYY-MM-DD|YYYYMMDD), offset (int), limit (int)
    """
    # validate config exists
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    if not config:
        abort(404)

    import os, sqlite3, pickle
    import nodes as _nodes_mod

    date = (request.args.get("date") or "").strip()
    offset = int(request.args.get("offset", 0) or 0)
    limit = int(request.args.get("limit", 50) or 50)

    # normalize date to YYYYMMDD
    dk = _nodes_mod.normalize_date_key(date)
    if not dk:
        return jsonify({"total": 0, "offset": offset, "limit": limit, "items": [], "error": "bad date format"}), 400

    storage_key = f"{class_name}_{config_uid}"
    main_db_path = os.path.join("node_storage", f"{storage_key}.sqlite")
    idx_db_path = os.path.join("node_storage", f"{storage_key}__date_index.sqlite")

    if not os.path.exists(main_db_path):
        return jsonify({"total": 0, "offset": offset, "limit": limit, "items": []})

    if not os.path.exists(idx_db_path):
        # index not built yet
        return jsonify({"total": 0, "offset": offset, "limit": limit, "items": [], "warning": "date index missing"}), 200

    table = "unnamed"

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    upper = f"{dk}|~"

    # 1) get page of node_ids from index (fast)
    conn = sqlite3.connect(idx_db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM {table} WHERE key <= ?", (upper,))
        total = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"SELECT key FROM {table} WHERE key <= ? ORDER BY key LIMIT ? OFFSET ?",
            (upper, limit, offset),
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
        return jsonify({"total": total, "offset": offset, "limit": limit, "items": []})

    # 2) fetch docs from main storage by ids (page sized -> OK)
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

        return jsonify({"total": total, "offset": offset, "limit": limit, "items": items})
    finally:
        conn.close()        

@app.route('/api/config/<config_uid>/node/<class_name>', methods=['GET', 'POST'])
@api_auth_required
def nodes_api(config_uid, class_name):
    """API for working with all class nodes"""
    import nodes as _nodes_mod

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()

    if not config:
        abort(404)

    # --- runtime context for onAcceptServer ---
    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    room_uid = request.args.get('room')

    try:
        # ============================================================
        # ROOM MODE (special create path)
        # ============================================================
        if room_uid and request.method == 'POST':
            data = request.get_json() or {}

            if not (os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers):
                abort(404)

            isolated_globals = _load_server_handlers_ns(config_uid, config)

            if (
                class_name not in isolated_globals or
                not hasattr(isolated_globals[class_name], '__bases__') or
                not any(base.__name__ == 'Node'
                        for base in isolated_globals[class_name].__bases__)
            ):
                abort(404)

            node_class = isolated_globals[class_name]

            objects_data = data if isinstance(data, list) else [data]

            for item_data in objects_data:
                raw_id = item_data.get('_id')
                node_id = extract_internal_id(raw_id) if raw_id else str(uuid.uuid4())

                user_data = dict(item_data)

                node = node_class(node_id, config_uid)
                if user_data:
                    node.update_data(user_data)   # <-- AcceptRejected here

            return handle_room_objects(config_uid, class_name, room_uid, data)

        # ============================================================
        # NORMAL MODE
        # ============================================================
        if not (os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers):
            abort(404)

        isolated_globals = _load_server_handlers_ns(config_uid, config)

        if (
            class_name not in isolated_globals or
            not hasattr(isolated_globals[class_name], '__bases__') or
            not any(base.__name__ == 'Node'
                    for base in isolated_globals[class_name].__bases__)
        ):
            abort(404)

        node_class = isolated_globals[class_name]

        # ---------------- GET ----------------
        if request.method == 'GET':
            nodes = node_class.get_all(config_uid)
            result = {node_id: node.to_dict() for node_id, node in nodes.items()}
            return jsonify(result)

        # ---------------- POST ----------------
        if request.method == 'POST':
            data = request.get_json() or {}

            # ----- array -----
            if isinstance(data, list):
                created_nodes = []

                for item_data in data:
                    raw_id = item_data.get('_id')
                    node_id = extract_internal_id(raw_id) if raw_id else str(uuid.uuid4())

                    user_data = dict(item_data)

                    node = node_class(node_id, config_uid)
                    if user_data:
                        node.update_data(user_data)   # <-- AcceptRejected here

                    created_nodes.append(node.to_dict())

                return jsonify(created_nodes), 201

            # ----- single -----
            raw_id = data.get('_id')
            node_id = extract_internal_id(raw_id) if raw_id else str(uuid.uuid4())

            user_data = dict(data)

            node = node_class(node_id, config_uid)
            if user_data:
                node.update_data(user_data)   # <-- AcceptRejected here

            return jsonify(node.to_dict()), 201

        abort(404)

    # ============================================================
    # ACCEPT REJECT (EXPECTED BUSINESS ERROR)
    # ============================================================
    except _nodes_mod.AcceptRejected as e:
        return jsonify({
            'status': False,
            'data': e.payload
        }), 200

    # ============================================================
    # REAL ERROR
    # ============================================================
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500


@app.route("/api/config/<config_uid>/node/batch_get", methods=["POST"])
@api_auth_required
def node_batch_get(config_uid):

    data = request.get_json() or {}

    class_name = data.get("class")
    ids = data.get("ids", [])
    date = data.get("date")

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()

    if not config:
        abort(404)

    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    isolated_globals = _load_server_handlers_ns(config_uid, config)

    if class_name not in isolated_globals:
        return jsonify({"status":True,"items":[]})

    node_class = isolated_globals[class_name]

    result = []

    for nid in ids:

        node = node_class.get(nid, config_uid)

        if not node:
            continue

        result.append(node._data)

    return jsonify({
        "status":True,
        "items":result
    })

@app.route("/api/config/<config_uid>/node/batch_summary", methods=["POST"])
@api_auth_required
def node_batch_summary(config_uid):

    data = request.get_json() or {}

    class_name = data.get("class")
    ids = data.get("ids",[])
    date = data.get("date")

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()

    if not config:
        abort(404)

    runtime_parsed = _build_runtime_parsed_config(config)
    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    isolated_globals = _load_server_handlers_ns(config_uid, config)

    if class_name not in isolated_globals:
        return jsonify({"status":True,"items":[]})

    node_class = isolated_globals[class_name]

    result = []

    for nid in ids:

        node = node_class.get(nid, config_uid)

        if not node:
            continue

        fn = getattr(node, "_summary", None)

        if not callable(fn):
            continue

        try:

            r = fn(date)

            result.append({
                "id":nid,
                "summary":r
            })

        except Exception:
            pass

    return jsonify({
        "status":True,
        "items":result
    })


@app.route('/api/config/<config_uid>/node/<class_name>/page', methods=['GET'])
@api_auth_required
def nodes_api_page(config_uid, class_name):
    """
    Fast paged nodes list from sqlitedict storage (no exec, no Node instantiation).

    Query:
      offset (int), limit (int), q (str)
    Sorting:
      prefers _data._sort_string_desc, then _data._sort_string, else _id
    Search:
      if q -> substring search in _data values (stringified)
    """
    # validate config exists
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    if not config:
        abort(404)

    import os, sqlite3, pickle

    offset = int(request.args.get("offset", 0) or 0)
    limit = int(request.args.get("limit", 50) or 50)
    q = (request.args.get("q") or "").strip().lower()
    index_name = (request.args.get("index_name") or "").strip()
    index_value = request.args.get("index_value")

    storage_key = f"{class_name}_{config_uid}"
    db_path = os.path.join("node_storage", f"{storage_key}.sqlite")
    if not os.path.exists(db_path):
        return jsonify({"total": 0, "offset": offset, "limit": limit, "items": []})

    # sqlitedict default table name is "unnamed" unless specified
    table = "unnamed"

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    if index_name and index_value not in (None, ""):
        try:
            isolated_globals = _load_server_handlers_ns(config_uid, config)
            node_class = isolated_globals.get(class_name)
            if node_class is None:
                return jsonify({"total": 0, "offset": offset, "limit": limit, "items": []})
            node_ids = node_class.find_ids_by_index(index_name, index_value, config_uid)
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                items = []
                for nid in node_ids[offset: offset + limit]:
                    cur.execute(f"SELECT value FROM {table} WHERE key = ?", (str(nid),))
                    row = cur.fetchone()
                    if not row:
                        continue
                    obj = unpack(row[0])
                    if obj is not None:
                        items.append(obj)
                return jsonify({"total": len(node_ids), "offset": offset, "limit": limit, "items": items})
            finally:
                conn.close()
        except Exception:
            pass

    # FAST PATH: no search -> return page ordered by key, without scanning whole DB
    # (Sorting by _sort_string would require unpickling everything anyway.)
    if not q:
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            # total count
            cur.execute(f"SELECT COUNT(1) FROM {table}")
            total = int(cur.fetchone()[0] or 0)

            # page
            cur.execute(
                f"SELECT value FROM {table} ORDER BY key LIMIT ? OFFSET ?",
                (limit, offset),
            )
            rows = cur.fetchall()

            items = []
            for (val_blob,) in rows:
                obj = unpack(val_blob)
                if obj is not None:
                    items.append(obj)

            return jsonify({"total": total, "offset": offset, "limit": limit, "items": items})
        finally:
            conn.close()

    # SLOW PATH: q present -> scan + filter + sort (pickle prevents SQL filtering)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM {table}")
        total_all = int(cur.fetchone()[0] or 0)

        cur.execute(f"SELECT value FROM {table}")
        rows = cur.fetchall()

        all_items = []
        for (val_blob,) in rows:
            obj = unpack(val_blob)
            if obj is None:
                continue
            all_items.append(obj)

        # filter by q
        def match(item: dict) -> bool:
            data = (item or {}).get("_data") or {}
            for v in data.values():
                try:
                    if q in str(v).lower():
                        return True
                except Exception:
                    pass
            return False

        filtered = [it for it in all_items if match(it)]

        # sort
        def sort_key(item: dict):
            data = (item or {}).get("_data") or {}
            if "_sort_string_desc" in data:
                return str(data.get("_sort_string_desc") or "")
            if "_sort_string" in data:
                return str(data.get("_sort_string") or "")
            return str((item or {}).get("_id") or "")

        # if any item has _sort_string_desc -> sort descending
        has_desc = any("_sort_string_desc" in ((it or {}).get("_data") or {}) for it in filtered)
        filtered.sort(key=sort_key, reverse=bool(has_desc))

        total = len(filtered)
        sliced = filtered[offset: offset + limit]

        return jsonify({"total": total, "offset": offset, "limit": limit, "items": sliced, "total_all": total_all})
    finally:
        conn.close()


def handle_room_objects(config_uid, class_name, room_uid,data):
    """Processing objects across the room"""

    if not isinstance(data, list):
        data = [data]

    room_objects = RoomObjects(
        room_uid=room_uid,
        config_uid=config_uid,
        class_name=class_name,
        objects_data=data,
        expires_at=datetime.now(timezone.utc),
        acknowledged_by=[]
    )
    db.session.add(room_objects)
    db.session.commit()

    room = Room.query.filter_by(uid=room_uid).first()
    transport = (getattr(room, 'transport', 'websocket') or 'websocket').strip().lower()

    push_result = None
    if transport == 'fcm':
        push_result = notify_room_transport(room_uid, config_uid=config_uid, class_name=class_name, object_id=room_objects.id)
    else:
        send_nodes_update(room_uid)

    return jsonify({
        "status": "objects_queued",
        "count": len(data),
        "room_uid": room_uid,
        "object_id": room_objects.id,
        "transport": transport,
        "push": push_result,
        "message": "Objects sent to room for client processing"
    }), 202

def send_objects_update(room_uid, config_uid, class_name, objects_data):
    """Sends an object update to all clients of the room"""
    if room_uid in active_connections:
        message = {
            'type': 'objects_create',
            'config_uid': config_uid,
            'class_name': class_name,
            'objects': objects_data,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        for user, ws in list(active_connections[room_uid].items()):
            try:
                if not ws.closed:
                    ws.send(json.dumps(message))
            except WebSocketError:
                active_connections[room_uid].pop(user, None)
                print(f"Removed dead connection for {user}")

def _get_firebase_app():
    if firebase_admin is None or firebase_credentials is None:
        return None, 'firebase_admin is not installed'
    try:
        if firebase_admin._apps:
            return firebase_admin.get_app(), None
    except Exception:
        pass

    service_account_path = (
        os.environ.get('FIREBASE_SERVICE_ACCOUNT')
        or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'firebase-service-account.json')
    )
    if not service_account_path or not os.path.isfile(service_account_path):
        return None, 'Firebase service account file is not configured'
    try:
        cred = firebase_credentials.Certificate(service_account_path)
        app_obj = firebase_admin.initialize_app(cred)
        return app_obj, None
    except Exception as e:
        return None, str(e)


def _gateway_headers():
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {PUSH_GATEWAY_TOKEN}',
    }


def _gateway_send_fcm(tokens, title, body, data_payload=None):
    gateway_url = (PUSH_GATEWAY_URL or '').strip().rstrip('/')
    if not gateway_url:
        return {'ok': False, 'error': 'gateway url is not configured', 'tokens': len(tokens or [])}
    payload = {
        'tokens': [str(t).strip() for t in (tokens or []) if str(t).strip()],
        'title': str(title or ''),
        'body': str(body or ''),
        'data': data_payload or {},
    }
    req = urllib.request.Request(
        gateway_url + '/api/push/fcm/send',
        data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers=_gateway_headers(),
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode('utf-8')
            try:
                data = json.loads(raw)
            except Exception:
                data = {'ok': False, 'error': raw or 'invalid gateway response'}
            data.setdefault('via', 'gateway')
            return data
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', errors='ignore')
        return {'ok': False, 'error': f'gateway http {e.code}', 'details': raw, 'tokens': len(payload['tokens'])}
    except Exception as e:
        return {'ok': False, 'error': f'gateway request failed: {e}', 'tokens': len(payload['tokens'])}


def _gateway_send_user(user_key, title, body, data_payload=None):
    gateway_url = (NMAKER_SERVER_URL or PUSH_GATEWAY_URL or '').strip().rstrip('/')
    if not gateway_url:
        return {'ok': False, 'error': 'gateway url is not configured', 'user_key': str(user_key or '').strip()}
    payload = {
        'user_key': str(user_key or '').strip(),
        'title': str(title or ''),
        'body': str(body or ''),
        'data': data_payload or {},
    }
    req = urllib.request.Request(
        gateway_url + '/api/push/user/send',
        data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers=_gateway_headers(),
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode('utf-8')
            try:
                data = json.loads(raw)
            except Exception:
                data = {'ok': False, 'error': raw or 'invalid gateway response'}
            data.setdefault('via', 'gateway-user')
            return data
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', errors='ignore')
        return {'ok': False, 'error': f'gateway http {e.code}', 'details': raw, 'user_key': str(user_key or '').strip()}
    except Exception as e:
        return {'ok': False, 'error': f'gateway request failed: {e}', 'user_key': str(user_key or '').strip()}


def _gateway_send_device(device_uid, title, body, data_payload=None):
    gateway_url = (NMAKER_SERVER_URL or PUSH_GATEWAY_URL or '').strip().rstrip('/')
    if not gateway_url:
        return {'ok': False, 'error': 'gateway url is not configured', 'device_uid': str(device_uid or '').strip()}
    payload = {
        'device_uid': str(device_uid or '').strip(),
        'title': str(title or ''),
        'body': str(body or ''),
        'data': data_payload or {},
    }
    req = urllib.request.Request(
        gateway_url + '/api/push/device/send',
        data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers=_gateway_headers(),
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode('utf-8')
            try:
                data = json.loads(raw)
            except Exception:
                data = {'ok': False, 'error': raw or 'invalid gateway response'}
            data.setdefault('via', 'gateway-device')
            return data
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', errors='ignore')
        return {'ok': False, 'error': f'gateway http {e.code}', 'details': raw, 'device_uid': str(device_uid or '').strip()}
    except Exception as e:
        return {'ok': False, 'error': f'gateway request failed: {e}', 'device_uid': str(device_uid or '').strip()}


def _public_api_base_url():
    base = (PUBLIC_API_BASE_URL or '').strip().rstrip('/')
    if base:
        return base
    try:
        return request.url_root.rstrip('/')
    except Exception:
        return ''


def _node_download_url(config_uid, class_name, node_id):
    config_uid = str(config_uid or '').strip()
    class_name = str(class_name or '').strip()
    node_id = _nodes_mod.extract_internal_id(node_id) or str(node_id or '').strip()
    base = _public_api_base_url()
    if not (base and config_uid and class_name and node_id):
        return ''
    return f"{base}/api/config/{config_uid}/node/{class_name}/{node_id}"


def _get_sender_user(explicit_sender=None):
    sender = str(explicit_sender or '').strip()
    if sender:
        return sender
    try:
        api_user = getattr(g, 'api_user', None)
        if api_user and getattr(api_user, 'email', None):
            return str(api_user.email).strip()
    except Exception:
        pass
    try:
        if getattr(current_user, 'is_authenticated', False) and getattr(current_user, 'email', None):
            return str(current_user.email).strip()
    except Exception:
        pass
    return ''


def _get_sender_display_name(explicit_sender=None):
    sender_user = _get_sender_user(explicit_sender)
    if not sender_user:
        return ''

    try:
        api_user = getattr(g, 'api_user', None)
        if api_user and getattr(api_user, 'email', None):
            api_email = str(api_user.email).strip()
            if api_email.lower() == sender_user.lower():
                return str(getattr(api_user, 'config_display_name', '') or api_email).strip()
    except Exception:
        pass

    try:
        if getattr(current_user, 'is_authenticated', False) and getattr(current_user, 'email', None):
            current_email = str(current_user.email).strip()
            if current_email.lower() == sender_user.lower():
                return str(getattr(current_user, 'config_display_name', '') or current_email).strip()
    except Exception:
        pass

    try:
        user = User.query.filter_by(email=sender_user).first()
        if user:
            return str(getattr(user, 'config_display_name', '') or getattr(user, 'email', '') or sender_user).strip()
    except Exception:
        pass

    return sender_user


def _node_discussion_user_keys_for_user(user_obj=None):
    """Return all known message aliases for the authenticated user.

    Raw-node download links are delivered through the message layer, where the
    recipient is stored as ``target_id``/``user_key``.  Android clients may use
    an application-level user_key that is not exactly the Flask login email.
    Therefore raw-node authorization must compare delivery history with every
    alias we know belongs to the authenticated account, not only email/id.
    """
    keys = []

    if user_obj is None:
        try:
            user_obj = getattr(g, 'api_user', None)
        except Exception:
            user_obj = None
        try:
            if user_obj is None and getattr(current_user, 'is_authenticated', False):
                user_obj = current_user
        except Exception:
            pass

    def add(value):
        value = str(value or '').strip()
        if value:
            keys.append(value)

    # If only the email/string key was passed, keep it first.
    if isinstance(user_obj, str) and user_obj.strip():
        add(user_obj.strip())

    for attr in ('email', 'id', 'config_display_name', 'user_key', 'username', 'name'):
        try:
            add(getattr(user_obj, attr, ''))
        except Exception:
            pass

    user_id = None
    try:
        user_id = getattr(user_obj, 'id', None)
    except Exception:
        user_id = None
    if user_id is None:
        try:
            api_user = getattr(g, 'api_user', None)
            user_id = getattr(api_user, 'id', None) if api_user is not None else None
        except Exception:
            user_id = None
    if user_id is None:
        try:
            if getattr(current_user, 'is_authenticated', False):
                user_id = getattr(current_user, 'id', None)
        except Exception:
            user_id = None

    # Room/device registration binds Android user_key aliases to the Basic-auth
    # user.  This is the critical link that lets a recipient download a raw-node
    # URL from a message addressed to that Android user_key.
    device_uids = set()
    if user_id is not None:
        try:
            for rd in RoomDevice.query.filter_by(user_id=user_id).all():
                add(getattr(rd, 'user_key', ''))
                du = str(getattr(rd, 'device_uid', '') or '').strip()
                if du:
                    device_uids.add(du)
                    add(du)
                extra = getattr(rd, 'extra_json', None)
                if isinstance(extra, dict):
                    for k in ('user_key', 'target_user', 'recipient', 'to_user'):
                        add(extra.get(k))
        except Exception:
            pass
        try:
            for ud in UserDevice.query.filter_by(user_id=user_id).all():
                du = str(getattr(ud, 'device_uid', '') or '').strip()
                if du:
                    device_uids.add(du)
                    add(du)
                add(getattr(ud, 'android_id', ''))
                extra = getattr(ud, 'extra_json', None)
                if isinstance(extra, dict):
                    for k in ('user_key', 'target_user', 'recipient', 'to_user'):
                        add(extra.get(k))
        except Exception:
            pass

    # If this account/device already acknowledged messages, keep those stored
    # ack aliases too.  This helps with old rows created before raw-node target
    # hints were written into RawNode.payload_json.
    if device_uids:
        try:
            for ack in OutgoingMessageDeviceAck.query.filter(OutgoingMessageDeviceAck.device_uid.in_(list(device_uids))).order_by(OutgoingMessageDeviceAck.id.desc()).limit(200).all():
                add(getattr(ack, 'user_key', ''))
                add(getattr(ack, 'ack_by', ''))
                ack_payload = getattr(ack, 'ack_payload', None)
                if isinstance(ack_payload, dict):
                    for k in ('user_key', 'ack_user', 'target_user', 'recipient', 'to_user'):
                        add(ack_payload.get(k))
        except Exception:
            pass

    seen = set()
    out = []
    for key in keys:
        low = str(key or '').strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        out.append(str(key).strip())
    return out


def _node_discussion_group_ids_for_user_keys(user_keys):
    lows = {str(k or '').strip().lower() for k in (user_keys or []) if str(k or '').strip()}
    if not lows:
        return set()
    try:
        rows = MessageGroupMember.query.filter(sa.func.lower(MessageGroupMember.user_key).in_(lows)).all()
        return {str(row.group_id or '').strip() for row in rows if str(row.group_id or '').strip()}
    except Exception:
        return set()


def _node_discussion_message_visible_to_user_keys(msg, user_keys, group_ids=None):
    """Check visibility of NodeDiscussionMessage or OutgoingMessageLog row."""
    lows = {str(k or '').strip().lower() for k in (user_keys or []) if str(k or '').strip()}
    if not lows:
        return False
    if group_ids is None:
        group_ids = _node_discussion_group_ids_for_user_keys(user_keys)

    try:
        payload = msg.payload_json if isinstance(getattr(msg, 'payload_json', None), dict) else {}
    except Exception:
        payload = {}

    sender = str(getattr(msg, 'sender_user', '') or payload.get('sender_user') or '').strip().lower()
    if sender and sender in lows:
        return True

    target_type = str(getattr(msg, 'target_type', '') or '').strip().lower()
    target_id = str(getattr(msg, 'target_id', '') or '').strip()

    if target_type in ('user', 'device') and target_id.lower() in lows:
        return True
    if target_type == 'group' and target_id in (group_ids or set()):
        return True

    # Some older payloads kept user/group only inside JSON.
    for key in ('user_key', 'target_user', 'recipient', 'recipient_user', 'to', 'to_user', 'receiver'):
        value = str(payload.get(key) or '').strip().lower()
        if value and value in lows:
            return True

    group_id = str(payload.get('group_id') or payload.get('discussion_group_id') or '').strip()
    if group_id and group_id in (group_ids or set()):
        return True

    return False


def _dt_sort_value(value):
    if value is None:
        return 0.0
    try:
        if getattr(value, 'tzinfo', None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return float(value.timestamp())
    except Exception:
        try:
            return float(datetime.fromisoformat(str(value)).timestamp())
        except Exception:
            return 0.0


def _query_outgoing_rows_for_user_keys(user_keys, group_ids=None, limit=1000, ascending=False):
    """Fetch message rows for a user via indexed columns, not a full JSON scan."""
    lows = {str(k or '').strip().lower() for k in (user_keys or []) if str(k or '').strip()}
    keys = [str(k or '').strip() for k in (user_keys or []) if str(k or '').strip()]
    groups = set(group_ids or set())
    rows_by_id = {}

    clauses = []
    if keys:
        clauses.append(OutgoingMessageLog.sender_user.in_(keys))
        clauses.append(sa.and_(OutgoingMessageLog.target_type.in_(('user', 'device')), OutgoingMessageLog.target_id.in_(keys)))
    if groups:
        clauses.append(sa.and_(OutgoingMessageLog.target_type == 'group', OutgoingMessageLog.target_id.in_(list(groups))))

    order = (OutgoingMessageLog.created_at.asc(), OutgoingMessageLog.id.asc()) if ascending else (OutgoingMessageLog.created_at.desc(), OutgoingMessageLog.id.desc())

    try:
        if clauses:
            for row in OutgoingMessageLog.query.filter(sa.or_(*clauses)).order_by(*order).limit(limit).all():
                rows_by_id[getattr(row, 'id', id(row))] = row
    except Exception:
        pass

    # Compatibility fallback for old direct-device rows where the indexed
    # target is only a device id and the user_key lives inside payload_json.
    # This is deliberately tiny and shallow to avoid the CPU spike from the old
    # full-table scan.
    try:
        fallback_limit = min(200, max(50, int(limit // 5)))
        for row in OutgoingMessageLog.query.order_by(OutgoingMessageLog.created_at.desc(), OutgoingMessageLog.id.desc()).limit(fallback_limit).all():
            if _node_discussion_message_visible_to_user_keys(row, user_keys, group_ids=groups):
                rows_by_id[getattr(row, 'id', id(row))] = row
    except Exception:
        pass

    rows = list(rows_by_id.values())
    rows.sort(key=lambda r: (_dt_sort_value(getattr(r, 'created_at', None)), getattr(r, 'id', 0) or 0), reverse=not ascending)
    return rows[:limit]


def _raw_node_payload_user_has_hint(payload, user_keys, group_ids=None, include_sender=True):
    payload = payload if isinstance(payload, dict) else {}
    lows = {str(k or '').strip().lower() for k in (user_keys or []) if str(k or '').strip()}
    if not lows:
        return False
    if group_ids is None:
        group_ids = _node_discussion_group_ids_for_user_keys(user_keys)

    hints = payload.get('_node_message_targets')
    if isinstance(hints, list):
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            target_type = str(hint.get('target_type') or '').strip().lower()
            target_id = str(hint.get('target_id') or hint.get('user_key') or hint.get('group_id') or '').strip()
            if target_type == 'user' and target_id.lower() in lows:
                return True
            if target_type == 'group' and target_id in (group_ids or set()):
                return True
            if include_sender and str(hint.get('sender_user') or '').strip().lower() in lows:
                return True

    if include_sender and str(payload.get('sender_user') or '').strip().lower() in lows:
        return True

    for key in ('target_user', 'user_key', 'target_key', 'target_id', 'recipient', 'recipient_user', 'to', 'to_user', 'peer', 'peer_user', 'receiver'):
        value = str(payload.get(key) or '').strip()
        if value and value.lower() in lows:
            return True
    group_id = str(payload.get('group_id') or payload.get('discussion_group_id') or '').strip()
    if group_id and group_id in (group_ids or set()):
        return True

    for key in ('data', '_data', 'payload', 'node'):
        nested = payload.get(key)
        if isinstance(nested, dict) and _raw_node_payload_user_has_hint(nested, user_keys, group_ids=group_ids, include_sender=include_sender):
            return True

    return False


def _user_can_access_raw_node_id(node_id, user_obj=None, include_sender=True):
    node_id = str(node_id or '').strip()
    if not node_id:
        return False
    user_keys = _node_discussion_user_keys_for_user(user_obj)
    if not user_keys:
        return False
    group_ids = _node_discussion_group_ids_for_user_keys(user_keys)

    try:
        obj = db.session.execute(select(RawNode).where(RawNode.node_id == node_id)).scalar_one_or_none()
    except Exception:
        obj = None

    if obj is not None:
        try:
            user_id = getattr(user_obj, 'id', None)
            if user_id is None:
                api_user = getattr(g, 'api_user', None)
                if api_user is not None:
                    user_id = getattr(api_user, 'id', None)
            if user_id is None and getattr(current_user, 'is_authenticated', False):
                user_id = getattr(current_user, 'id', None)
            if user_id is not None and getattr(obj, 'owner_user_id', None) == user_id:
                return True
        except Exception:
            pass

        payload = obj.payload_json if isinstance(getattr(obj, 'payload_json', None), dict) else {}
        if _raw_node_payload_user_has_hint(payload, user_keys, group_ids=group_ids, include_sender=include_sender):
            return True

    # Delivery history is the source of truth for received raw nodes, but it
    # must be queried through indexed user/group columns, not by scanning every
    # outgoing message JSON payload.
    try:
        rows = _query_outgoing_rows_for_user_keys(user_keys, group_ids=group_ids, limit=1000, ascending=False)
        for row in rows:
            payload = row.payload_json if isinstance(row.payload_json, dict) else {}
            if not _node_delivery_payload_matches_node(payload, node_id):
                continue
            return True
    except Exception:
        pass

    return False


def _user_can_access_node_discussion_id(node_id, user_obj=None):
    node_id = str(node_id or '').strip()
    if not node_id:
        return False
    user_keys = _node_discussion_user_keys_for_user(user_obj)
    if not user_keys:
        return False
    group_ids = _node_discussion_group_ids_for_user_keys(user_keys)

    if _user_can_access_raw_node_id(node_id, user_obj=user_obj):
        return True

    try:
        rows = NodeDiscussionMessage.query.filter_by(node_id=node_id).limit(2000).all()
        for row in rows:
            if _node_discussion_message_visible_to_user_keys(row, user_keys, group_ids=group_ids):
                return True
    except Exception:
        pass

    try:
        rows = _query_outgoing_rows_for_user_keys(user_keys, group_ids=group_ids, limit=1000, ascending=False)
        for row in rows:
            payload = row.payload_json if isinstance(row.payload_json, dict) else {}
            if (
                _extract_node_discussion_node_id(payload) == node_id
                or _node_discussion_payload_matches_node(payload, node_id)
                or _node_discussion_group_matches_node(row.target_id, node_id)
            ):
                return True
    except Exception:
        pass

    return False



def _normalize_message_payload(payload, sender_user=None):
    sender_user = _get_sender_user(sender_user)
    sender_display_name = _get_sender_display_name(sender_user)

    if isinstance(payload, dict):
        out = {str(k): v for k, v in payload.items()}

        if sender_user:
            out.setdefault('sender_user', sender_user)
        if sender_display_name:
            out.setdefault('sender_display_name', sender_display_name)

        is_node_download = bool(out.get('download_url') or out.get('node_uid'))
        is_node_download_list = str(out.get('type') or '').strip() == 'node_download_list'

        if sender_user and (is_node_download or is_node_download_list):
            out['sender_user'] = sender_user
        if sender_display_name and (is_node_download or is_node_download_list):
            out['sender_display_name'] = sender_display_name

        if sender_user and is_node_download_list:
            items = None
            if isinstance(out.get('items'), list):
                items = out.get('items')
            elif isinstance(out.get('items_json'), str):
                try:
                    parsed = json.loads(out.get('items_json') or '[]')
                    if isinstance(parsed, list):
                        items = parsed
                except Exception:
                    items = None

            if isinstance(items, list):
                normalized_items = []
                for item in items:
                    if isinstance(item, dict):
                        item_out = {str(k): v for k, v in item.items()}
                        if item_out.get('download_url') or item_out.get('node_uid'):
                            item_out['sender_user'] = sender_user
                            if sender_display_name:
                                item_out['sender_display_name'] = sender_display_name
                        normalized_items.append(item_out)
                    else:
                        normalized_items.append(item)
                out['items_json'] = json.dumps(normalized_items, ensure_ascii=False)
                if 'items' in out:
                    out['items'] = normalized_items

        return out, {'kind': 'json', 'sender_user': sender_user, 'sender_display_name': sender_display_name}

    is_node_like = hasattr(payload, '_config_uid') and hasattr(payload, '_id')
    if is_node_like:
        config_uid = str(getattr(payload, '_config_uid', '') or '').strip()
        class_name = str(getattr(payload, '_schema_class_name', None) or getattr(payload.__class__, '__name__', '') or '').strip()
        node_id = str(getattr(payload, '_id', '') or '').strip()
        download_url = _node_download_url(config_uid, class_name, node_id)
        if not download_url:
            return None, {'kind': 'node', 'error': 'cannot build node download_url'}
        out = {
            'type': 'node_download',
            'config_uid': config_uid,
            'class_name': class_name,
            'node_id': _nodes_mod.extract_internal_id(node_id) or node_id,
            'node_uid': _nodes_mod.normalize_own_uid(config_uid, class_name, node_id),
            'download_url': download_url,
        }
        if sender_user:
            out['sender_user'] = sender_user
        if sender_display_name:
            out['sender_display_name'] = sender_display_name
        return out, {'kind': 'node', 'sender_user': sender_user, 'sender_display_name': sender_display_name}

    if isinstance(payload, str):
        raw = payload.strip()
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(raw)
        if cfg_uid and cls_name and internal_id:
            download_url = _node_download_url(cfg_uid, cls_name, internal_id)
            if not download_url:
                return None, {'kind': 'node', 'error': 'cannot build node download_url'}
            out = {
                'type': 'node_download',
                'config_uid': cfg_uid,
                'class_name': cls_name,
                'node_id': internal_id,
                'node_uid': _nodes_mod.normalize_own_uid(cfg_uid, cls_name, internal_id),
                'download_url': download_url,
            }
            if sender_user:
                out['sender_user'] = sender_user
            if sender_display_name:
                out['sender_display_name'] = sender_display_name
            return out, {'kind': 'node', 'sender_user': sender_user, 'sender_display_name': sender_display_name}
        out = {'value': raw}
        if sender_user:
            out.setdefault('sender_user', sender_user)
        if sender_display_name:
            out.setdefault('sender_display_name', sender_display_name)
        return out, {'kind': 'json', 'sender_user': sender_user, 'sender_display_name': sender_display_name}

    if payload is None:
        out = {}
        if sender_user:
            out.setdefault('sender_user', sender_user)
        if sender_display_name:
            out.setdefault('sender_display_name', sender_display_name)
        return out, {'kind': 'json', 'sender_user': sender_user, 'sender_display_name': sender_display_name}

    try:
        out = {'value': json.dumps(payload, ensure_ascii=False, default=str)}
    except Exception:
        out = {'value': str(payload)}
    if sender_user:
        out.setdefault('sender_user', sender_user)
    if sender_display_name:
        out.setdefault('sender_display_name', sender_display_name)
    return out, {'kind': 'json', 'sender_user': sender_user, 'sender_display_name': sender_display_name}


def _ensure_payload_client_message_id(payload):
    if not isinstance(payload, dict):
        payload = {}
    client_message_id = str(payload.get('_client_message_id') or payload.get('client_message_id') or '').strip()
    if not client_message_id:
        client_message_id = uuid.uuid4().hex
    payload['_client_message_id'] = client_message_id
    payload.setdefault('client_message_id', client_message_id)
    return payload, client_message_id


def _upsert_outgoing_message_log(client_message_id, target_type, target_id, title, body, payload, sender_user=None):
    client_message_id = str(client_message_id or '').strip()
    if not client_message_id:
        return None

    msg = OutgoingMessageLog.query.filter_by(client_message_id=client_message_id).first()
    now = datetime.now(timezone.utc)
    if not msg:
        msg = OutgoingMessageLog(
            client_message_id=client_message_id,
            created_at=now,
        )
        db.session.add(msg)

    msg.sender_user = str(sender_user or '').strip() or None
    msg.target_type = str(target_type or '').strip() or 'user'
    msg.target_id = str(target_id or '').strip()
    msg.title = str(title or '')
    msg.body = str(body or '')
    msg.payload_json = payload if isinstance(payload, dict) else {'value': str(payload or '')}
    msg.status = 'accepted'
    msg.accepted_at = now
    if msg.last_error:
        msg.last_error = None
    db.session.commit()
    return msg


def _mark_outgoing_message_push_result(client_message_id, result):
    client_message_id = str(client_message_id or '').strip()
    if not client_message_id:
        return None
    msg = OutgoingMessageLog.query.filter_by(client_message_id=client_message_id).first()
    if not msg:
        return None

    now = datetime.now(timezone.utc)
    if bool((result or {}).get('ok')):
        msg.status = 'pushed'
        msg.pushed_at = now
        msg.last_error = None
    else:
        msg.status = 'error'
        msg.last_error = str((result or {}).get('error') or (result or {}).get('details') or '')
    db.session.commit()
    return msg


def _is_same_gateway_host() -> bool:
    try:
        current = (request.host_url or '').strip().rstrip('/')
        gateway = (NMAKER_SERVER_URL or PUSH_GATEWAY_URL or '').strip().rstrip('/')
        if not current or not gateway:
            return False
        return urlparse(current).netloc.lower() == urlparse(gateway).netloc.lower()
    except Exception:
        return False
    
def send_message_to_user_global(user_key, title, body, payload=None, sender_user=None):
    user_key = str(user_key or '').strip()
    if not user_key:
        return {'ok': False, 'error': 'user_key is required'}

    normalized_payload, meta = _normalize_message_payload(payload, sender_user=sender_user)
    if normalized_payload is None:
        return {'ok': False, 'error': meta.get('error') or 'payload normalization failed', 'user_key': user_key}

    if isinstance(normalized_payload, dict):
        normalized_payload.setdefault('user_key', user_key)
    normalized_payload, client_message_id = _ensure_payload_client_message_id(normalized_payload)

    # Safe isolated hook: only node_discussion payloads are copied to permanent history.
    # This does not change delivery, FCM, pending or the return semantics for ordinary messages.
    try:
        _maybe_save_node_discussion_history_from_payload(
            target_type='user',
            target_id=user_key,
            title=title,
            body=body,
            payload=normalized_payload,
            sender_user=sender_user,
            delivery_status='accepted',
        )
    except Exception as e:
        print('Node discussion history hook failed:', e)

    try:
        _maybe_trigger_node_discussion_input_from_payload(
            target_type='user',
            target_id=user_key,
            title=title,
            body=body,
            payload=normalized_payload,
            sender_user=sender_user,
        )
    except Exception as e:
        print('Node discussion input hook failed:', e)

    _upsert_outgoing_message_log(
        client_message_id=client_message_id,
        target_type='user',
        target_id=user_key,
        title=title,
        body=body,
        payload=normalized_payload,
        sender_user=sender_user,
    )

    try:
        if isinstance(normalized_payload, dict) and str(normalized_payload.get('type') or '') == 'node':
            node_target_hint_id = normalized_payload.get('node_id') or normalized_payload.get('node_uid') or normalized_payload.get('_id')
            _remember_node_delivery_target(node_target_hint_id, 'user', user_key, sender_user=sender_user)
    except Exception as e:
        print('Could not remember user node delivery target:', e)

    if _is_same_gateway_host():
        result = send_message_to_user_internal(user_key, title, body, normalized_payload)
        result.setdefault('via', 'internal-user')
    else:
        result = _gateway_send_user(user_key, title, body, normalized_payload)

    _mark_outgoing_message_push_result(client_message_id, result)

    try:
        if _extract_node_discussion_node_id(normalized_payload):
            _update_node_discussion_delivery_status(
                client_message_id,
                'pushed' if bool((result or {}).get('ok')) else 'accepted',
            )
    except Exception as e:
        print('Node discussion status update failed:', e)

    if isinstance(result, dict):
        result.setdefault('client_message_id', client_message_id)
    return result

def send_message_to_device_global(device_uid, title, body, payload=None, sender_user=None):
    device_uid = str(device_uid or '').strip()
    if not device_uid:
        return {'ok': False, 'error': 'device_uid is required'}

    normalized_payload, meta = _normalize_message_payload(payload, sender_user=sender_user)
    if normalized_payload is None:
        return {'ok': False, 'error': meta.get('error') or 'payload normalization failed', 'device_uid': device_uid}

    if isinstance(normalized_payload, dict):
        normalized_payload.setdefault('device_uid', device_uid)
    normalized_payload, client_message_id = _ensure_payload_client_message_id(normalized_payload)

    _upsert_outgoing_message_log(
        client_message_id=client_message_id,
        target_type='device',
        target_id=device_uid,
        title=title,
        body=body,
        payload=normalized_payload,
        sender_user=sender_user,
    )

    if _is_same_gateway_host():
        result = send_message_to_device_internal(device_uid, title, body, normalized_payload)
        result.setdefault('via', 'internal-device')
    else:
        result = _gateway_send_device(device_uid, title, body, normalized_payload)

    _mark_outgoing_message_push_result(client_message_id, result)
    if isinstance(result, dict):
        result.setdefault('client_message_id', client_message_id)
    return result


def _normalize_group_id(value):
    return str(value or '').strip()


def _make_group_id():
    return f"g_{uuid.uuid4().hex[:12]}"


def _normalize_member_user_keys(values, include_user_key=None):
    raw_items = []
    if isinstance(values, (list, tuple, set)):
        raw_items.extend(list(values))
    elif values not in (None, ''):
        raw_items.append(values)
    if include_user_key not in (None, ''):
        raw_items.append(include_user_key)

    normalized = []
    seen = set()
    for item in raw_items:
        user_key = _normalize_user_key(item)
        if not user_key:
            continue
        key_lower = user_key.lower()
        if key_lower in seen:
            continue
        seen.add(key_lower)
        normalized.append(user_key)
    return normalized


def _get_group_member_keys(group_id):
    group_id = _normalize_group_id(group_id)
    if not group_id:
        return []
    rows = MessageGroupMember.query.filter_by(group_id=group_id).order_by(MessageGroupMember.user_key.asc()).all()
    return [str(row.user_key or '').strip() for row in rows if str(row.user_key or '').strip()]


def _user_can_access_group(user_key, group_id):
    user_key = _normalize_user_key(user_key)
    group_id = _normalize_group_id(group_id)
    if not user_key or not group_id:
        return False
    return MessageGroupMember.query.filter(
        MessageGroupMember.group_id == group_id,
        sa.func.lower(MessageGroupMember.user_key) == user_key.lower(),
    ).first() is not None


def _serialize_group(group, include_members=False):
    if not group:
        return None
    data = {
        'group_id': group.group_id,
        'title': group.title or '',
        'created_by': group.created_by or None,
        'created_at': group.created_at.isoformat() if getattr(group, 'created_at', None) else None,
        'updated_at': group.updated_at.isoformat() if getattr(group, 'updated_at', None) else None,
    }
    if include_members:
        members = _get_group_member_keys(group.group_id)
        data['members'] = members
        data['member_count'] = len(members)
    return data


def _collect_user_tokens(user_key):
    user_key = _normalize_user_key(user_key)
    if not user_key:
        return []

    tokens = []
    room_devices = RoomDevice.query.filter_by(user_key=user_key).all()
    tokens.extend([(d.fcm_token or '').strip() for d in room_devices if (d.fcm_token or '').strip()])

    user_obj = User.query.filter_by(email=user_key).first()
    if user_obj:
        user_devices = UserDevice.query.filter_by(user_id=user_obj.id).all()
        tokens.extend([(d.token or '').strip() for d in user_devices if (d.token or '').strip()])

    return list(dict.fromkeys([token for token in tokens if token]))


def send_message_to_group_internal(group_id, title, body, payload=None):
    group_id = _normalize_group_id(group_id)
    if not group_id:
        return {'ok': False, 'error': 'group_id is required'}

    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        return {'ok': False, 'error': 'group_not_found', 'group_id': group_id}

    payload = payload if isinstance(payload, dict) else {}
    member_keys = _get_group_member_keys(group_id)
    if not member_keys:
        return {'ok': False, 'error': 'group_has_no_members', 'group_id': group_id}

    tokens = []
    for member_key in member_keys:
        tokens.extend(_collect_user_tokens(member_key))

    dedup_tokens = list(dict.fromkeys([token for token in tokens if token]))
    if not dedup_tokens:
        return {'ok': False, 'error': 'no FCM tokens for group', 'group_id': group_id, 'member_count': len(member_keys)}

    result = _send_fcm_to_tokens(dedup_tokens, title, body, payload)
    if isinstance(result, dict):
        result.setdefault('group_id', group_id)
        result.setdefault('group_title', group.title or '')
        result.setdefault('member_count', len(member_keys))
    return result


def _gateway_send_group(group_id, title, body, data_payload=None):
    group_id = _normalize_group_id(group_id)
    if not group_id:
        return {'ok': False, 'error': 'group_id is required'}

    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        return {'ok': False, 'error': 'group_not_found', 'group_id': group_id}

    member_keys = _get_group_member_keys(group_id)
    if not member_keys:
        return {'ok': False, 'error': 'group_has_no_members', 'group_id': group_id}

    success = 0
    failures = []
    for member_key in member_keys:
        result = _gateway_send_user(member_key, title, body, data_payload or {})
        if bool((result or {}).get('ok')):
            success += 1
        else:
            failures.append({
                'user_key': member_key,
                'error': str((result or {}).get('error') or (result or {}).get('details') or 'delivery_failed'),
            })

    return {
        'ok': success > 0 and not failures,
        'success': success,
        'failures': failures,
        'member_count': len(member_keys),
        'group_id': group_id,
        'group_title': group.title or '',
        'via': 'gateway-group',
    }


def send_message_to_group_global(group_id, title, body, payload=None, sender_user=None):
    group_id = _normalize_group_id(group_id)
    if not group_id:
        return {'ok': False, 'error': 'group_id is required'}

    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        return {'ok': False, 'error': 'group_not_found', 'group_id': group_id}

    normalized_payload, meta = _normalize_message_payload(payload, sender_user=sender_user)
    if normalized_payload is None:
        return {'ok': False, 'error': meta.get('error') or 'payload normalization failed', 'group_id': group_id}

    if isinstance(normalized_payload, dict):
        normalized_payload['group_id'] = group.group_id
        normalized_payload['group_title'] = group.title or ''
    normalized_payload, client_message_id = _ensure_payload_client_message_id(normalized_payload)

    # Safe isolated hook: only node_discussion payloads are copied to permanent history.
    # This does not change delivery, FCM, pending or the return semantics for ordinary messages.
    try:
        _maybe_save_node_discussion_history_from_payload(
            target_type='group',
            target_id=group.group_id,
            title=title,
            body=body,
            payload=normalized_payload,
            sender_user=sender_user,
            delivery_status='accepted',
        )
    except Exception as e:
        print('Node discussion history hook failed:', e)

    try:
        _maybe_trigger_node_discussion_input_from_payload(
            target_type='group',
            target_id=group.group_id,
            title=title,
            body=body,
            payload=normalized_payload,
            sender_user=sender_user,
        )
    except Exception as e:
        print('Node discussion input hook failed:', e)

    _upsert_outgoing_message_log(
        client_message_id=client_message_id,
        target_type='group',
        target_id=group.group_id,
        title=title,
        body=body,
        payload=normalized_payload,
        sender_user=sender_user,
    )

    try:
        if isinstance(normalized_payload, dict) and str(normalized_payload.get('type') or '') == 'node':
            node_target_hint_id = normalized_payload.get('node_id') or normalized_payload.get('node_uid') or normalized_payload.get('_id')
            _remember_node_delivery_target(node_target_hint_id, 'group', group.group_id, sender_user=sender_user)
    except Exception as e:
        print('Could not remember group node delivery target:', e)

    if _is_same_gateway_host():
        result = send_message_to_group_internal(group.group_id, title, body, normalized_payload)
        result.setdefault('via', 'internal-group')
    else:
        result = _gateway_send_group(group.group_id, title, body, normalized_payload)

    _mark_outgoing_message_push_result(client_message_id, result)

    try:
        if _extract_node_discussion_node_id(normalized_payload):
            _update_node_discussion_delivery_status(
                client_message_id,
                'pushed' if bool((result or {}).get('ok')) else 'accepted',
            )
    except Exception as e:
        print('Node discussion status update failed:', e)

    if isinstance(result, dict):
        result.setdefault('client_message_id', client_message_id)
    return result


# ------------------------------------------------------------------
# Bridge functions for PythonScript handlers (called from nodes.py).
# sender_user is intentionally fixed to "server" for these helpers.
# ------------------------------------------------------------------
SCRIPT_HANDLER_SENDER = "server"


def _noda_target_is_group(target: str) -> bool:
    return str(target or '').strip().startswith('group:')


def _noda_target_group_id(target: str) -> str:
    value = str(target or '').strip()
    return value[len('group:'):].strip() if value.startswith('group:') else value


def _noda_userfile_url(config_uid: str, filename: str) -> str:
    config_uid = str(config_uid or '').strip()
    filename = secure_filename(str(filename or '').strip())
    if not (config_uid and filename):
        return ''
    base = _current_public_base_url() or _public_api_base_url()
    if not base:
        return ''
    return f"{base}/api/userfiles/{config_uid}/raw/{filename}"


def _noda_prepare_image_payload(text: str, filename: str, *, config_uid: str = '') -> dict:
    text = str(text or '')
    filename = str(filename or '').strip()
    payload = {
        'type': 'image',
        'text': text,
        'message': text,
        'body': text,
        'filename': filename,
        'sender_user': SCRIPT_HANDLER_SENDER,
    }
    if not filename:
        return payload

    parsed = urlparse(filename)
    if parsed.scheme in ('http', 'https'):
        payload['image_url'] = filename
        return payload

    # If the handler passes only a filename saved in UserFiles/<config_uid>,
    # expose it through the existing raw userfiles route.  Keep the filename in
    # `image` for older clients that display local names.
    payload['image'] = os.path.basename(filename)
    candidate_url = _noda_userfile_url(config_uid, os.path.basename(filename))
    if candidate_url:
        payload['image_url'] = candidate_url
    return payload


def _noda_send_text_message(target: str, text: str) -> dict:
    target = str(target or '').strip()
    text = str(text or '')
    if not target:
        return {'ok': False, 'error': 'target is required'}
    payload = {
        'type': 'text',
        'text': text,
        'message': text,
        'body': text,
        'sender_user': SCRIPT_HANDLER_SENDER,
    }
    title = SCRIPT_HANDLER_SENDER
    if _noda_target_is_group(target):
        return send_message_to_group_global(_noda_target_group_id(target), title, text or 'New message', payload, sender_user=SCRIPT_HANDLER_SENDER)
    return send_message_to_user_global(target, title, text or 'New message', payload, sender_user=SCRIPT_HANDLER_SENDER)


def _noda_send_image_message(target: str, text: str, filename: str, *, config_uid: str = '') -> dict:
    target = str(target or '').strip()
    if not target:
        return {'ok': False, 'error': 'target is required'}
    payload = _noda_prepare_image_payload(text, filename, config_uid=config_uid)
    title = SCRIPT_HANDLER_SENDER
    body = str(text or '') or os.path.basename(str(filename or '').strip()) or 'Image'
    if _noda_target_is_group(target):
        return send_message_to_group_global(_noda_target_group_id(target), title, body, payload, sender_user=SCRIPT_HANDLER_SENDER)
    return send_message_to_user_global(target, title, body, payload, sender_user=SCRIPT_HANDLER_SENDER)


def _noda_add_node_id_candidate(candidates, value):
    """Append possible discussion node ids without losing raw-node identity.

    node-discussion history is keyed by the raw-node id from thread_ref, e.g.
    <config_uid>$<class_name>$<internal_id>.  Do not collapse that to only
    <internal_id>, otherwise replies cannot find history/targets.
    """
    raw = str(value or '').strip()
    if not raw:
        return

    # A URL like https://host/api/raw-node/<raw_id> is the strongest identity.
    url_id = ''
    try:
        url_id = _raw_node_id_from_download_url(raw)
    except Exception:
        url_id = ''
    for item in (url_id, raw):
        item = str(item or '').strip()
        if item and item not in candidates:
            candidates.append(item)

    # Keep full composite ids first, but still add the internal id as a fallback
    # for older local discussions keyed only by internal node id.
    try:
        internal_id = _nodes_mod.extract_internal_id(raw)
    except Exception:
        internal_id = ''
    if internal_id and internal_id not in candidates:
        candidates.append(internal_id)


def _noda_node_id_candidates_from_ref(node_or_id) -> list[str]:
    candidates = []

    def add_from_mapping(mapping):
        if not isinstance(mapping, dict):
            return
        # URLs first: they point to the raw-node row used by Android thread_ref.
        for key in ('thread_ref', 'download_url', 'raw_node_url', 'node_url', '_download_url'):
            _noda_add_node_id_candidate(candidates, mapping.get(key))
        for key in ('_id', 'node_id', 'node_uid', 'raw_node_id'):
            _noda_add_node_id_candidate(candidates, mapping.get(key))
        data = mapping.get('_data') if isinstance(mapping.get('_data'), dict) else {}
        for key in ('_id', 'node_id', 'node_uid', 'raw_node_id'):
            _noda_add_node_id_candidate(candidates, data.get(key))

    if isinstance(node_or_id, dict):
        add_from_mapping(node_or_id)
    else:
        # RemoteJsonNode has _download_url.  Prefer it over _id because _id may
        # later be normalized by generic helpers.
        for attr in ('_download_url', 'download_url', 'raw_node_url', 'node_url', 'thread_ref'):
            _noda_add_node_id_candidate(candidates, getattr(node_or_id, attr, None))
        for attr in ('_id', 'node_id', 'node_uid', 'raw_node_id'):
            _noda_add_node_id_candidate(candidates, getattr(node_or_id, attr, None))
        try:
            add_from_mapping(getattr(node_or_id, '_raw', None))
        except Exception:
            pass
        try:
            add_from_mapping(getattr(node_or_id, '_data', None))
        except Exception:
            pass
        if isinstance(node_or_id, str):
            _noda_add_node_id_candidate(candidates, node_or_id)

    return candidates


def _noda_node_id_from_ref(node_or_id) -> str:
    candidates = _noda_node_id_candidates_from_ref(node_or_id)
    return candidates[0] if candidates else ''


def _noda_send_node_discussion_message(node_or_id, text: str, *, filename: str = '', config_uid: str = '') -> dict:
    node_id_candidates = _noda_node_id_candidates_from_ref(node_or_id)
    node_id = node_id_candidates[0] if node_id_candidates else ''
    text = str(text or '')
    filename = str(filename or '').strip()
    if not node_id:
        _node_discussion_debug('script_send.no_node_id', ref_type=type(node_or_id).__name__, ref=str(node_or_id)[:300])
        return {'ok': False, 'error': 'node_id is required', 'node_id_candidates': node_id_candidates}
    if not text and not filename:
        return {'ok': False, 'error': 'text_or_image_required', 'node_id': node_id, 'node_id_candidates': node_id_candidates}

    _node_discussion_debug('script_send.start', node_id=node_id, candidates=node_id_candidates, has_text=bool(text), filename=filename)

    targets = []
    target_probe = []
    for candidate in node_id_candidates:
        found = _find_node_discussion_targets(candidate, sender_user=SCRIPT_HANDLER_SENDER)
        source = 'history'
        if not found:
            found = _find_node_discussion_targets_from_raw_node(candidate, sender_user=SCRIPT_HANDLER_SENDER)
            source = 'raw_node'
        target_probe.append({'node_id': candidate, 'source': source, 'count': len(found or [])})
        if found:
            node_id = candidate
            targets = found
            break

    _node_discussion_debug('script_send.targets', selected_node_id=node_id, targets=targets, probe=target_probe)

    if not targets:
        return {
            'ok': False,
            'error': 'node_discussion_target_required',
            'node_id': node_id,
            'node_id_candidates': node_id_candidates,
            'target_probe': target_probe,
            'results': [],
        }

    thread_ref = _node_discussion_thread_ref(node_id)
    message_type = 'image' if filename else 'text'
    if filename:
        payload = _noda_prepare_image_payload(text, filename, config_uid=config_uid)
        payload['type'] = message_type
    else:
        payload = {'type': message_type, 'text': text}

    payload.update({
        'thread_type': 'node_discussion',
        'thread_ref': thread_ref,
        'node_id': node_id,
        'node_uid': node_id,
        'sender_user': SCRIPT_HANDLER_SENDER,
        'sender_display_name': SCRIPT_HANDLER_SENDER,
    })
    # Keep FCM data close to the mobile client contract for node discussions.
    # Notification body is still passed as the FCM notification body below;
    # duplicated data fields can make Android route the message as a generic DM.
    payload.pop('message', None)
    payload.pop('body', None)

    title = SCRIPT_HANDLER_SENDER
    body = text or os.path.basename(filename) or 'New message'
    results = []
    accepted_count = 0
    delivery_ok_count = 0

    for target in targets:
        target_type = target.get('target_type')
        target_id = target.get('target_id')
        item_payload = dict(payload)
        if target_type == 'group':
            item_payload['group_id'] = target_id
            result = send_message_to_group_global(target_id, title, body, item_payload, sender_user=SCRIPT_HANDLER_SENDER)
        elif target_type == 'user':
            item_payload['user_key'] = target_id
            result = send_message_to_user_global(target_id, title, body, item_payload, sender_user=SCRIPT_HANDLER_SENDER)
        else:
            result = {'ok': False, 'error': 'unsupported_target_type'}

        client_message_id = result.get('client_message_id') if isinstance(result, dict) else None
        history_msg = None
        if client_message_id:
            try:
                history_msg = NodeDiscussionMessage.query.filter_by(client_message_id=client_message_id).first()
            except Exception:
                history_msg = None
        if history_msg:
            accepted_count += 1
        if bool((result or {}).get('ok')):
            delivery_ok_count += 1
        results.append({
            'target_type': target_type,
            'target_id': target_id,
            'client_message_id': client_message_id,
            'ok': bool(history_msg),
            'delivery_ok': bool((result or {}).get('ok')),
            'history_saved': bool(history_msg),
            'result': result,
        })

    return {
        'ok': accepted_count > 0,
        'node_id': node_id,
        'thread_ref': thread_ref,
        'accepted_count': accepted_count,
        'delivery_ok_count': delivery_ok_count,
        'results': results,
    }


def _noda_send_text_to_node_discussion(node_or_id, text: str) -> dict:
    return _noda_send_node_discussion_message(node_or_id, text)


def _noda_send_image_to_node_discussion(node_or_id, text: str, filename: str, *, config_uid: str = '') -> dict:
    return _noda_send_node_discussion_message(node_or_id, text, filename=filename, config_uid=config_uid)

def _group_history_before_dt(before):
    before_value = str(before or '').strip()
    if not before_value:
        return None, None
    try:
        return datetime.fromisoformat(before_value.replace('Z', '+00:00')), None
    except Exception:
        pass
    msg = OutgoingMessageLog.query.filter_by(client_message_id=before_value).first()
    if msg and msg.created_at:
        return msg.created_at, None
    return None, {'ok': False, 'error': 'invalid_before', 'details': 'Use ISO datetime or existing client_message_id'}


def _get_group_messages_history_impl(group_id, limit=100, before=None):
    group_id = _normalize_group_id(group_id)
    if not group_id:
        return {'ok': False, 'error': 'group_id is required'}, 400

    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        return {'ok': False, 'error': 'group_not_found', 'group_id': group_id}, 404

    try:
        limit = int(limit or 100)
    except Exception:
        limit = 100
    if limit <= 0:
        limit = 100
    if limit > 1000:
        limit = 1000

    before_dt, before_error = _group_history_before_dt(before)
    if before_error:
        return before_error, 400

    query = OutgoingMessageLog.query.filter_by(target_type='group', target_id=group_id)
    if before_dt is not None:
        query = query.filter(OutgoingMessageLog.created_at < before_dt)

    messages = query.order_by(OutgoingMessageLog.created_at.desc()).limit(limit).all()
    items = [_serialize_outgoing_message(msg) for msg in messages]

    return {
        'ok': True,
        'group_id': group_id,
        'title': group.title or '',
        'count': len(items),
        'messages': items,
    }, 200


def _gateway_token_ok():
    auth_header = request.headers.get('Authorization', '')
    token = ''
    if auth_header.lower().startswith('bearer '):
        token = auth_header.split(' ', 1)[1].strip()
    elif request.headers.get('X-API-Token'):
        token = request.headers.get('X-API-Token', '').strip()
    expected = str(PUSH_GATEWAY_TOKEN or '').strip()
    if not expected:
        return False, 'gateway token is not configured'
    if not token:
        return False, 'missing gateway token'
    if token != expected:
        return False, 'invalid gateway token'
    return True, ''


def send_message_to_user_internal(user_key, title, body, payload=None):
    user_key = str(user_key or '').strip()
    if not user_key:
        return {'ok': False, 'error': 'user_key is required'}
    payload = payload if isinstance(payload, dict) else {}
    tokens = []
    room_devices = RoomDevice.query.filter_by(user_key=user_key).all()
    tokens.extend([(d.fcm_token or '').strip() for d in room_devices if (d.fcm_token or '').strip()])
    user_obj = User.query.filter_by(email=user_key).first()
    if user_obj:
        user_devices = UserDevice.query.filter_by(user_id=user_obj.id).all()
        tokens.extend([(d.token or '').strip() for d in user_devices if (d.token or '').strip()])
    dedup_tokens = list(dict.fromkeys([t for t in tokens if t]))
    if not dedup_tokens:
        return {'ok': False, 'error': 'no FCM tokens for user', 'user_key': user_key}
    return _send_fcm_to_tokens(dedup_tokens, title, body, payload)


def send_message_to_device_internal(device_uid, title, body, payload=None):
    device_uid = str(device_uid or '').strip()
    if not device_uid:
        return {'ok': False, 'error': 'device_uid is required'}
    payload = payload if isinstance(payload, dict) else {}
    tokens = []
    room_devices = RoomDevice.query.filter_by(device_uid=device_uid).all()
    tokens.extend([(d.fcm_token or '').strip() for d in room_devices if (d.fcm_token or '').strip()])
    user_devices = UserDevice.query.filter((UserDevice.device_uid == device_uid) | (UserDevice.android_id == device_uid)).all()
    tokens.extend([(d.token or '').strip() for d in user_devices if (d.token or '').strip()])
    dedup_tokens = list(dict.fromkeys([t for t in tokens if t]))
    if not dedup_tokens:
        return {'ok': False, 'error': 'device has no FCM token', 'device_uid': device_uid}
    return _send_fcm_to_tokens(dedup_tokens, title, body, payload)


def _sanitize_fcm_data_payload(data_payload):
    """Return a Firebase-safe data payload.

    Firebase data payload keys cannot use reserved names such as
    message_type/from/gcm/google*/collapse_key. Keep the original payload in
    OutgoingMessageLog; this sanitized copy is only for FCM delivery.
    """
    source = data_payload if isinstance(data_payload, dict) else {}
    sanitized = {}
    skipped = []
    aliases = {
        'message_type': 'noda_message_type',
        'from': 'noda_from',
        'collapse_key': 'noda_collapse_key',
        'gcm': 'noda_gcm',
    }

    for raw_key, raw_value in source.items():
        key = str(raw_key or '').strip()
        if not key:
            continue

        lower_key = key.lower()
        safe_key = aliases.get(lower_key)
        if safe_key is None:
            if lower_key.startswith('google') or lower_key.startswith('gcm.'):
                safe_key = 'noda_' + re.sub(r'[^A-Za-z0-9_]', '_', key)
            else:
                safe_key = key

        if safe_key != key:
            skipped.append(key)

        if raw_value is None:
            value = ''
        elif isinstance(raw_value, str):
            value = raw_value
        else:
            try:
                value = json.dumps(raw_value, ensure_ascii=False)
            except Exception:
                value = str(raw_value)
        sanitized[str(safe_key)] = value

    if skipped:
        try:
            #print('[node-discussion-debug] fcm.send.sanitized_payload | skipped_keys=' + json.dumps(skipped, ensure_ascii=False), flush=True)
            pass
        except Exception:
            pass
    return sanitized

def _send_fcm_to_tokens(tokens, title, body, data_payload=None):
    data_payload = _sanitize_fcm_data_payload(data_payload)

    bad_tokens = {"", "null", "none", "undefined", "nan"}

    tokens = [
        str(t).strip()
        for t in (tokens or [])
        if str(t).strip().lower() not in bad_tokens
    ]
    tokens = list(dict.fromkeys(tokens))

    if not tokens:
        return {'ok': False, 'error': 'no tokens'}

    app_obj, err = _get_firebase_app()
    if app_obj is None:
        if (PUSH_GATEWAY_URL or '').strip():
            return _gateway_send_fcm(tokens, title, body, data_payload)
        return {'ok': False, 'error': err or 'firebase unavailable', 'tokens': len(tokens)}

    success = 0
    failures = []

    invalid_token_errors = (
        'invalid registration token',
        'registration token is not a valid',
        'requested entity was not found',
        'unregistered',
        'UNREGISTERED',
    )

    for token in tokens:
        try:
            msg = firebase_messaging.Message(
                token=token,
                notification=firebase_messaging.Notification(
                    title=str(title or ''),
                    body=str(body or '')
                ),
                data=data_payload,
            )
            firebase_messaging.send(msg, app=app_obj)
            success += 1
        except Exception as e:
            err_text = str(e)
            failures.append({'token': token, 'error': err_text})

            # Do not delete FCM tokens automatically from a send error.
            # FCM INVALID_ARGUMENT can be caused by a bad data payload key, not by
            # the token itself. Deleting here made valid device tokens disappear.
            # Token refresh/replace is handled by register-device endpoints.
            if False and any(x.lower() in err_text.lower() for x in invalid_token_errors):
                pass

    _node_discussion_debug(
        'fcm.send.finish',
        success=success,
        failures_count=len(failures),
        tokens=len(tokens),
        failure_errors=[f.get('error') for f in failures[:3]],
    )

    return {
        'ok': success > 0,
        'success': success,
        'failures': failures,
        'tokens': len(tokens),
        'via': 'local',
    }


def notify_room_transport(room_uid, title='Receiving nodes', body='New nodes is available', data_payload=None, config_uid=None, class_name=None, object_id=None):
    room = Room.query.filter_by(uid=room_uid).first()
    if not room:
        return {'ok': False, 'error': 'room not found'}
    transport = (room.transport or 'websocket').strip().lower()
    if transport != 'fcm':
        send_nodes_update(room_uid)
        return {'ok': True, 'transport': 'websocket'}

    devices = RoomDevice.query.filter_by(room_uid=room_uid, push_channel='fcm').all()
    tokens = [d.fcm_token for d in devices if (d.fcm_token or '').strip()]

    payload = dict(data_payload or {})
    payload.setdefault('type', 'room_objects_available')
    payload.setdefault('room_uid', room_uid)
    if config_uid:
        payload.setdefault('config_uid', config_uid)
    if class_name:
        payload.setdefault('class_name', class_name)
    if object_id is not None:
        payload.setdefault('object_id', str(object_id))
    api_base = (request.url_root.rstrip('/') if request else '')
    if api_base:
        download_url = f"{api_base}/api/room/{room_uid}/objects"
        params = []
        if config_uid:
            params.append(f"config_uid={config_uid}")
        if class_name:
            params.append(f"class_name={class_name}")
        if object_id is not None:
            params.append(f"object_id={object_id}")
        if params:
            download_url += '?' + '&'.join(params)
        payload.setdefault('download_url', download_url)

    return _send_fcm_to_tokens(tokens, title, body, payload)


@app.route('/api/push/fcm/send', methods=['POST'])
def gateway_push_fcm_send():
    ok, err = _gateway_token_ok()
    if not ok:
        return jsonify({'ok': False, 'error': err}), 401
    data = request.get_json(silent=True) or {}
    tokens = data.get('tokens') if isinstance(data.get('tokens'), list) else []
    title = data.get('title') or 'Message'
    body = data.get('body') or ''
    payload = data.get('data') if isinstance(data.get('data'), dict) else {}
    app_obj, fb_err = _get_firebase_app()
    if app_obj is None:
        return jsonify({'ok': False, 'error': fb_err or 'firebase unavailable'}), 503
    result = _send_fcm_to_tokens(tokens, title, body, payload)
    result['gateway'] = True
    return jsonify(result), (200 if result.get('ok') else 400)


@app.route('/api/push/user/send', methods=['POST'])
def gateway_push_user_send():
    ok, err = _gateway_token_ok()
    if not ok:
        return jsonify({'ok': False, 'error': err}), 401
    data = request.get_json(silent=True) or {}
    user_key = (data.get('user_key') or '').strip()
    if not user_key:
        return jsonify({'ok': False, 'error': 'user_key is required'}), 400
    title = data.get('title') or 'Direct message'
    body = data.get('body') or ''
    payload = data.get('data') if isinstance(data.get('data'), dict) else {}
    payload.setdefault('user_key', user_key)
    result = send_message_to_user_internal(user_key, title, body, payload)
    result['gateway'] = True
    return jsonify(result), (200 if result.get('ok') else 400)


@app.route('/api/push/device/send', methods=['POST'])
def gateway_push_device_send():
    ok, err = _gateway_token_ok()
    if not ok:
        return jsonify({'ok': False, 'error': err}), 401
    data = request.get_json(silent=True) or {}
    device_uid = (data.get('device_uid') or '').strip()
    if not device_uid:
        return jsonify({'ok': False, 'error': 'device_uid is required'}), 400
    title = data.get('title') or 'Direct message'
    body = data.get('body') or ''
    payload = data.get('data') if isinstance(data.get('data'), dict) else {}
    payload.setdefault('device_uid', device_uid)
    result = send_message_to_device_internal(device_uid, title, body, payload)
    result['gateway'] = True
    return jsonify(result), (200 if result.get('ok') else 400)


@app.route('/api/me/register-device', methods=['POST'])
@api_auth_required
def register_my_device():
    api_user = getattr(g, 'api_user', None)
    data = request.get_json(silent=True) or {}
    device_uid = (data.get('device_uid') or data.get('uid') or data.get('android_id') or '').strip()
    if not device_uid:
        return jsonify({'error': 'device_uid is required'}), 400
    android_id = (data.get('android_id') or device_uid).strip()
    device_model = (data.get('device_model') or '').strip()
    fcm_token = (data.get('fcm_token') or data.get('token') or '').strip()
    ud = UserDevice.query.filter_by(user_id=api_user.id, android_id=android_id).first()
    if not ud:
        ud = UserDevice(user_id=api_user.id, android_id=android_id)
        db.session.add(ud)
    ud.device_uid = device_uid
    ud.device_model = device_model
    if fcm_token:
        ud.token = fcm_token
    ud.extra_json = data
    ud.last_connected = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({'ok': True, 'user_id': api_user.id, 'device_uid': device_uid, 'android_id': android_id})


@app.route('/api/room/<room_uid>/register-device', methods=['POST'])
@api_auth_required
def register_room_device(room_uid):
    room = Room.query.filter_by(uid=room_uid).first_or_404()
    data = request.get_json(silent=True) or {}
    api_user = getattr(g, 'api_user', None)

    device_uid = (data.get('device_uid') or data.get('uid') or data.get('android_id') or '').strip()
    if not device_uid:
        return jsonify({'error': 'device_uid is required'}), 400

    push_channel = (data.get('push_channel') or room.transport or 'websocket').strip().lower()
    if push_channel not in ('websocket', 'fcm'):
        push_channel = room.transport or 'websocket'

    fcm_token = (data.get('fcm_token') or data.get('token') or '').strip()
    android_id = (data.get('android_id') or device_uid).strip()
    device_model = (data.get('device_model') or '').strip()
    user_key = (data.get('user_key') or (api_user.email if api_user else '') or '').strip()

    room_device = RoomDevice.query.filter_by(room_uid=room_uid, device_uid=device_uid).first()
    if not room_device:
        room_device = RoomDevice(room_uid=room_uid, device_uid=device_uid)
        db.session.add(room_device)

    room_device.user_id = api_user.id if api_user else None
    room_device.user_key = user_key
    room_device.push_channel = push_channel
    if fcm_token:
        room_device.fcm_token = fcm_token
    room_device.android_id = android_id
    room_device.device_model = device_model
    room_device.extra_json = data
    room_device.last_seen = datetime.now(timezone.utc)

    if api_user:
        user_device = UserDevice.query.filter_by(user_id=api_user.id, android_id=android_id).first()
        if not user_device:
            user_device = UserDevice(
                user_id=api_user.id,
                device_uid=device_uid,
                android_id=android_id,
                device_model=device_model or 'Unknown',
                token=fcm_token,
                extra_json=data,
                last_connected=datetime.now(timezone.utc)
            )
            db.session.add(user_device)
        else:
            user_device.device_uid = device_uid
            user_device.device_model = device_model or user_device.device_model
            user_device.last_connected = datetime.now(timezone.utc)
            user_device.extra_json = data
            if fcm_token:
                user_device.token = fcm_token

    db.session.commit()

    return jsonify({
        'ok': True,
        'room_uid': room_uid,
        'device_uid': device_uid,
        'transport': room.transport or 'websocket',
        'push_channel': push_channel,
    })


@app.route('/api/room/<room_uid>/messages', methods=['POST'])
@api_auth_required
def push_room_message(room_uid):
    room = Room.query.filter_by(uid=room_uid).first_or_404()
    data = request.get_json(silent=True) or {}
    title = data.get('title') or 'Room message'
    body = data.get('body') or data.get('message') or 'New message'
    payload = data.get('data') if isinstance(data.get('data'), dict) else {}
    result = notify_room_transport(room_uid, title=title, body=body, data_payload=payload)
    return jsonify({'ok': True, 'room_uid': room_uid, 'transport': room.transport or 'websocket', 'result': result})


@app.route('/api/user/<user_key>/messages', methods=['POST'])
@api_auth_required
def push_user_message(user_key):
    data = request.get_json(silent=True) or {}
    if user_key == "_server":
        return handle_server_user_message(data)

    if not isinstance(data, dict):
        data = {}

    explicit_sender = data.get('sender_user')
    sender_display_name = _get_sender_display_name(explicit_sender)
    title = data.get('title') or sender_display_name or 'Direct message'
    body = data.get('body') or data.get('message') or data.get('text') or 'New message'

    raw_payload = data.get('data')
    payload = dict(raw_payload) if isinstance(raw_payload, dict) else raw_payload

    # Keep ordinary /api/user/.../messages unchanged.
    # Only when the caller explicitly marks node_discussion do we copy top-level
    # node fields into payload so the history hook can record the discussion.
    top_thread_type = str(data.get('thread_type') or '').strip()
    payload_thread_type = str(payload.get('thread_type') or '').strip() if isinstance(payload, dict) else ''
    top_type = str(data.get('type') or '').strip()
    payload_type = str(payload.get('type') or '').strip() if isinstance(payload, dict) else ''

    is_node_discussion = (
        top_thread_type == 'node_discussion'
        or payload_thread_type == 'node_discussion'
        or top_type == 'node_discussion_message'
        or payload_type == 'node_discussion_message'
    )

    if is_node_discussion:
        if not isinstance(payload, dict):
            payload = {}
        payload['thread_type'] = 'node_discussion'
        payload.setdefault('type', 'node_discussion_message')

        for key in ('node_id', 'node_uid', '_id', 'raw_node_id', 'thread_ref', 'raw_node_url', 'download_url'):
            if data.get(key) not in (None, '') and payload.get(key) in (None, ''):
                payload[key] = data.get(key)

        if payload.get('node_id') in (None, ''):
            payload['node_id'] = payload.get('node_uid') or payload.get('_id') or payload.get('raw_node_id')
        if payload.get('node_uid') in (None, ''):
            payload['node_uid'] = payload.get('node_id')
        if payload.get('text') in (None, ''):
            payload['text'] = data.get('text') or data.get('message') or data.get('body') or ''

        if explicit_sender and payload.get('sender_user') in (None, ''):
            payload['sender_user'] = explicit_sender
        if sender_display_name and payload.get('sender_display_name') in (None, ''):
            payload['sender_display_name'] = sender_display_name

    sender_user = _get_sender_user(explicit_sender)

    result = send_message_to_user_global(user_key, title, body, payload, sender_user=sender_user)
    if isinstance(result, dict):
        if sender_user:
            result.setdefault('sender_user', sender_user)
        if sender_display_name:
            result.setdefault('sender_display_name', sender_display_name)

    return jsonify({'ok': bool(result.get('ok')), 'user_key': user_key, 'result': result}), (200 if result.get('ok') else 400)

@app.route('/api/groups', methods=['POST'])
@api_auth_required
def api_create_group():
    data = request.get_json(silent=True) or {}
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))

    title = str(data.get('title') or '').strip()
    members = _normalize_member_user_keys(data.get('members'), include_user_key=current_user_key)

    if not title:
        return jsonify({'ok': False, 'error': 'title is required'}), 400
    if not members:
        return jsonify({'ok': False, 'error': 'members must contain at least one user'}), 400

    group = MessageGroup(
        group_id=_make_group_id(),
        title=title,
        created_by=current_user_key or None,
    )
    db.session.add(group)
    db.session.flush()

    for member_key in members:
        db.session.add(MessageGroupMember(group_id=group.group_id, user_key=member_key))

    db.session.commit()
    return jsonify({'group_id': group.group_id, 'title': group.title or ''}), 201


@app.route('/api/groups', methods=['GET'])
@api_auth_required
def api_list_groups():
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    groups = MessageGroup.query.join(
        MessageGroupMember,
        MessageGroupMember.group_id == MessageGroup.group_id,
    ).filter(
        sa.func.lower(MessageGroupMember.user_key) == current_user_key.lower()
    ).order_by(MessageGroup.updated_at.desc(), MessageGroup.created_at.desc()).all()

    items = [_serialize_group(group) for group in groups]
    return jsonify({'ok': True, 'count': len(items), 'groups': items})


@app.route('/api/groups/<group_id>', methods=['GET'])
@api_auth_required
def api_get_group(group_id):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    group = MessageGroup.query.filter_by(group_id=_normalize_group_id(group_id)).first()
    if not group:
        return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': _normalize_group_id(group_id)}), 404
    if not _user_can_access_group(current_user_key, group.group_id):
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': group.group_id}), 403
    return jsonify({'ok': True, 'group': _serialize_group(group, include_members=True)})


@app.route('/api/groups/<group_id>/members', methods=['GET'])
@api_auth_required
def api_get_group_members(group_id):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    normalized_group_id = _normalize_group_id(group_id)
    group = MessageGroup.query.filter_by(group_id=normalized_group_id).first()
    if not group:
        return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': normalized_group_id}), 404
    if not _user_can_access_group(current_user_key, normalized_group_id):
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': normalized_group_id}), 403

    members = _get_group_member_keys(normalized_group_id)
    return jsonify({'ok': True, 'group_id': normalized_group_id, 'title': group.title or '', 'count': len(members), 'members': members})


@app.route('/api/groups/<group_id>/members', methods=['POST'])
@api_auth_required
def api_add_group_members(group_id):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    normalized_group_id = _normalize_group_id(group_id)
    group = MessageGroup.query.filter_by(group_id=normalized_group_id).first()
    if not group:
        return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': normalized_group_id}), 404
    if not _user_can_access_group(current_user_key, normalized_group_id):
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': normalized_group_id}), 403

    data = request.get_json(silent=True) or {}
    members = _normalize_member_user_keys(data.get('members'))
    if not members:
        return jsonify({'ok': False, 'error': 'members must be a non-empty list', 'group_id': normalized_group_id}), 400

    existing = {user_key.lower() for user_key in _get_group_member_keys(normalized_group_id)}
    added = []
    for member_key in members:
        if member_key.lower() in existing:
            continue
        db.session.add(MessageGroupMember(group_id=normalized_group_id, user_key=member_key))
        existing.add(member_key.lower())
        added.append(member_key)

    group.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({
        'ok': True,
        'group_id': normalized_group_id,
        'added_members': added,
        'members': _get_group_member_keys(normalized_group_id),
    })


@app.route('/api/groups/<group_id>/members/<user_key>', methods=['DELETE'])
@api_auth_required
def api_remove_group_member(group_id, user_key):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    normalized_group_id = _normalize_group_id(group_id)
    normalized_user_key = _normalize_user_key(user_key)

    group = MessageGroup.query.filter_by(group_id=normalized_group_id).first()
    if not group:
        return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': normalized_group_id}), 404
    if not _user_can_access_group(current_user_key, normalized_group_id):
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': normalized_group_id}), 403

    members_before = _get_group_member_keys(normalized_group_id)
    if len(members_before) <= 1 and any(m.lower() == normalized_user_key.lower() for m in members_before):
        return jsonify({'ok': False, 'error': 'group_must_have_at_least_one_member', 'group_id': normalized_group_id}), 400

    membership = MessageGroupMember.query.filter(
        MessageGroupMember.group_id == normalized_group_id,
        sa.func.lower(MessageGroupMember.user_key) == normalized_user_key.lower(),
    ).first()
    if not membership:
        return jsonify({'ok': False, 'error': 'member_not_found', 'group_id': normalized_group_id, 'user_key': normalized_user_key}), 404

    db.session.delete(membership)
    group.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({
        'ok': True,
        'group_id': normalized_group_id,
        'removed_user': normalized_user_key,
        'members': _get_group_member_keys(normalized_group_id),
    })


@app.route('/api/groups/<group_id>/messages', methods=['POST'])
@api_auth_required
def api_push_group_message(group_id):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    normalized_group_id = _normalize_group_id(group_id)

    group = MessageGroup.query.filter_by(group_id=normalized_group_id).first()
    if not group:
        return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': normalized_group_id}), 404
    if not _user_can_access_group(current_user_key, normalized_group_id):
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': normalized_group_id}), 403

    data = request.get_json(silent=True) or {}
    explicit_sender = data.get('sender_user')
    sender_display_name = _get_sender_display_name(explicit_sender)
    title = data.get('title') or sender_display_name or group.title or 'Group message'
    body = data.get('body') or data.get('message') or 'New message'
    payload = data.get('data') if isinstance(data.get('data'), dict) else {}
    sender_user = _get_sender_user(explicit_sender)

    result = send_message_to_group_global(normalized_group_id, title, body, payload, sender_user=sender_user)
    if isinstance(result, dict):
        result.setdefault('group_id', normalized_group_id)
        result.setdefault('group_title', group.title or '')
        if sender_user:
            result.setdefault('sender_user', sender_user)
        if sender_display_name:
            result.setdefault('sender_display_name', sender_display_name)

    return jsonify({'ok': bool(result.get('ok')), 'group_id': normalized_group_id, 'title': group.title or '', 'result': result}), (200 if result.get('ok') else 400)


@app.route('/api/groups/<group_id>/messages', methods=['GET'])
@api_auth_required
def api_group_messages_history(group_id):
    api_user = getattr(g, 'api_user', None)
    current_user_key = _normalize_user_key(getattr(api_user, 'email', None))
    normalized_group_id = _normalize_group_id(group_id)
    if not _user_can_access_group(current_user_key, normalized_group_id):
        group_exists = MessageGroup.query.filter_by(group_id=normalized_group_id).first() is not None
        if not group_exists:
            return jsonify({'ok': False, 'error': 'group_not_found', 'group_id': normalized_group_id}), 404
        return jsonify({'ok': False, 'error': 'forbidden', 'group_id': normalized_group_id}), 403

    limit = request.args.get('limit', 100)
    before = request.args.get('before')
    payload, status = _get_group_messages_history_impl(normalized_group_id, limit=limit, before=before)
    return jsonify(payload), status


@app.route('/api/device/<device_uid>/messages', methods=['POST'])
@api_auth_required
def push_device_message(device_uid):
    data = request.get_json(silent=True) or {}
    title = data.get('title') or 'Direct message'
    body = data.get('body') or data.get('message') or 'New message'
    payload = data.get('data')
    sender_user = _get_sender_user()
    result = send_message_to_device_global(device_uid, title, body, payload, sender_user=sender_user)
    return jsonify({'ok': bool(result.get('ok')), 'device_uid': device_uid, 'result': result}), (200 if result.get('ok') else 400)


@app.route('/webapi/messages/user/<user_key>', methods=['POST'])
@login_required
def web_push_user_message(user_key):
    data = request.get_json(silent=True) or {}
    explicit_sender = data.get('sender_user')
    sender_display_name = _get_sender_display_name(explicit_sender)
    title = data.get('title') or sender_display_name or 'Direct message'
    body = data.get('body') or data.get('message') or 'New message'
    payload = data.get('data')
    sender_user = _get_sender_user(explicit_sender)
    
    result = send_message_to_user_global(user_key, title, body, payload, sender_user=sender_user)
    if isinstance(result, dict):
        if sender_user:
            result.setdefault('sender_user', sender_user)
        if sender_display_name:
            result.setdefault('sender_display_name', sender_display_name)
    return jsonify({'ok': bool(result.get('ok')), 'user_key': user_key, 'result': result}), (200 if result.get('ok') else 400)


# ------------------------------------------------------------------
# Server-side node input events for chat/data messages.
#
# This block preserves the existing message routes and only adds the
# server-event interception path:
#   - /api/user/_server/messages  -> onInputServer / onDataMessage
#   - node_discussion messages    -> onInputServer / onDiscussionMessage
# It supports both local config nodes and JSON nodes/classes by download_url.
# ------------------------------------------------------------------
SERVER_INPUT_EVENT = "onInputServer"
SERVER_INPUT_LISTENER = "onDataMessage"
DISCUSSION_INPUT_EVENT = "onInputServer"
DISCUSSION_INPUT_LISTENER = "onDiscussionMessage"


def normalize_server_message(raw_data):
    """Accept both direct node input and push envelope formats."""
    raw_data = raw_data if isinstance(raw_data, dict) else {}
    inner = raw_data.get("data") if isinstance(raw_data.get("data"), dict) else None

    if inner and (
        "node_id" in inner
        or "download_url" in inner
        or str(inner.get("type") or "").strip().lower() in {"node_input", "node-input", "nodeinput"}
    ):
        msg = dict(inner)
        msg["_envelope"] = {
            "title": raw_data.get("title"),
            "body": raw_data.get("body"),
            "sender_user": raw_data.get("sender_user"),
            "sender_display_name": raw_data.get("sender_display_name"),
            "_client_message_id": raw_data.get("_client_message_id"),
        }
        for key in ("sender_user", "sender_display_name", "_client_message_id"):
            if msg.get(key) in (None, "") and raw_data.get(key) not in (None, ""):
                msg[key] = raw_data.get(key)
        return msg

    return dict(raw_data)


def _server_message_type_is_node_input(value):
    return str(value or "").strip().lower() in {"node_input", "node-input", "nodeinput"}


def handle_server_user_message(data):
    """Handle POST /api/user/_server/messages without forwarding to a user/device."""
    msg = normalize_server_message(data)

    if not _server_message_type_is_node_input(msg.get("type")):
        return jsonify({
            "status": False,
            "ok": False,
            "error": "Unsupported server message type",
            "type": msg.get("type"),
        }), 400

    node_id = str(msg.get("node_id") or msg.get("node_uid") or msg.get("_id") or "").strip()
    if not node_id:
        return jsonify({
            "status": False,
            "ok": False,
            "error": "node_id is required",
        }), 400
    msg.setdefault("node_id", node_id)

    try:
        ctx = resolve_server_node_context(msg)
        if not ctx:
            return jsonify({
                "status": False,
                "ok": False,
                "error": "Node not found",
                "node_id": node_id,
                "download_url": msg.get("download_url"),
            }), 404

        result = execute_server_node_event(
            ctx=ctx,
            event_name=SERVER_INPUT_EVENT,
            listener=SERVER_INPUT_LISTENER,
            message_data=msg,
        )

        return jsonify({
            "status": True,
            "ok": True,
            "handled": True,
            "node_id": node_id,
            "mode": ctx.get("mode"),
            "class_name": ctx.get("class_name"),
            "config_uid": ctx.get("config_uid"),
            "result": result,
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": False,
            "ok": False,
            "error": str(e),
            "error_type": e.__class__.__name__,
        }), 500


def parse_node_ref(node_id):
    """Parse id like <config_uid>$<class_name>$<internal_id>."""
    node_id = str(node_id or "").strip()
    parts = node_id.split("$")

    if len(parts) >= 2:
        config_uid = parts[0].strip()
        class_name = parts[1].strip()
        candidates = [node_id]
        if len(parts) >= 3:
            candidates.append("$".join(parts[2:]))
        candidates.append(f"{config_uid}${class_name}")

        seen = set()
        candidates = [x for x in candidates if x and not (x in seen or seen.add(x))]
        return config_uid, class_name, candidates

    return None, None, [node_id]


def resolve_server_node_context(message_data):
    node_id = str(message_data.get("node_id") or message_data.get("node_uid") or message_data.get("_id") or "").strip()
    download_url = str(message_data.get("download_url") or message_data.get("raw_node_url") or message_data.get("thread_ref") or "").strip()

    config_uid, class_name, node_id_candidates = parse_node_ref(node_id)

    # 1) Try local server configuration storage first.
    if config_uid and class_name:
        try:
            ctx = resolve_config_node_context(
                config_uid=config_uid,
                class_name=class_name,
                node_id_candidates=node_id_candidates,
            )
            if ctx:
                return ctx
        except Exception:
            # Do not block download_url mode because config lookup failed.
            pass

    # 2) If not found locally, try raw/download JSON.
    if download_url:
        return resolve_download_url_node_context(
            node_id=node_id,
            fallback_class_name=class_name,
            download_url=download_url,
        )

    return None


def resolve_config_node_context(config_uid, class_name, node_id_candidates):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == str(config_uid))
    ).scalar_one_or_none()
    if not config:
        return None

    class_obj = db.session.execute(
        select(ConfigClass).where(ConfigClass.config_id == config.id, ConfigClass.name == str(class_name))
    ).scalar_one_or_none()
    if not class_obj:
        return None

    ctx_tokens = None
    try:
        try:
            runtime_parsed = _build_runtime_parsed_config(config)
            ctx_tokens = _nodes_mod.set_runtime_context(str(config_uid), runtime_parsed)
        except Exception:
            ctx_tokens = None

        isolated_globals = _load_server_handlers_ns(config_uid, config) or {}
        node_class = isolated_globals.get(class_name)
        if not node_class:
            return None

        node = None
        for candidate in node_id_candidates or []:
            if not candidate:
                continue
            try:
                node = node_class.get(candidate, config_uid)
            except Exception:
                node = None
            if node is not None:
                break

        if node is None:
            return None

        # The execution function owns resetting this context.
        tokens_for_exec = ctx_tokens
        ctx_tokens = None
        return {
            "mode": "config",
            "config": config,
            "config_uid": str(config_uid),
            "class_name": str(class_name),
            "class_obj": class_obj,
            "node": node,
            "node_id": getattr(node, "_id", None),
            "_runtime_context_tokens": tokens_for_exec,
        }
    finally:
        if ctx_tokens is not None:
            try:
                _nodes_mod.reset_runtime_context(ctx_tokens)
            except Exception:
                pass


def _load_raw_node_payload_for_event(node_id=None, download_url=""):
    # Prefer direct local DB access for /api/raw-node/<id> URLs.
    for candidate in (download_url, node_id):
        try:
            payload, handled = _raw_node_payload_from_local_db(candidate)
            if handled:
                return payload, True
        except Exception:
            pass

    # Fallback: direct RawNode lookup by node_id.
    try:
        raw_id = _raw_node_id_from_download_url(download_url) or str(node_id or "").strip()
        if raw_id:
            obj = db.session.execute(select(RawNode).where(RawNode.node_id == raw_id)).scalar_one_or_none()
            if obj is not None:
                return (obj.payload_json or {}), True
    except Exception:
        pass

    return None, False


def _extract_class_json_from_node_json(node_json):
    node_json = node_json if isinstance(node_json, dict) else {}
    raw_class = node_json.get("_class") or node_json.get("class") or node_json.get("class_json")
    if isinstance(raw_class, dict):
        return raw_class
    for key in ("class_json", "_class_json", "schema", "node_class"):
        value = node_json.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_class_name_from_class_json(raw_class, fallback_class_name="", node_json=None):
    if isinstance(raw_class, dict):
        for key in ("name", "code", "class_name", "_name"):
            value = str(raw_class.get(key) or "").strip()
            if value:
                return value
    elif isinstance(raw_class, str):
        value = raw_class.strip()
        if value:
            try:
                return value.split("$")[-2] if value.count("$") >= 2 else value.split("$")[-1]
            except Exception:
                return value

    node_json = node_json if isinstance(node_json, dict) else {}
    for key in ("class_name", "_class_name"):
        value = str(node_json.get(key) or "").strip()
        if value:
            return value
    return str(fallback_class_name or "").strip()


class RemoteJsonNode:
    """Small NodeEx-like wrapper for JSON nodes loaded by download_url."""

    def __init__(self, node_id, node_json, download_url="", raw_node_obj=None):
        self._raw = node_json if isinstance(node_json, dict) else {}
        self._id = str(node_id or self._raw.get("_id") or self._raw.get("node_id") or self._raw.get("node_uid") or "").strip()
        self._download_url = str(download_url or "").strip()
        self._raw_node_obj = raw_node_obj
        self._config_uid, self._schema_class_name, _internal_id = parse_node_ref(self._id)
        self._data = self._normalize_data()

    def _normalize_data(self):
        data = self._raw.get("_data")
        if not isinstance(data, dict):
            data = {k: v for k, v in self._raw.items() if k not in {"_data"}}
        data = dict(data or {})
        data.setdefault("_id", self._id)
        class_info = self._raw.get("_class") or self._raw.get("class")
        if isinstance(class_info, dict):
            data.setdefault("_class", class_info.get("name") or self._raw.get("class_name") or "")
        elif isinstance(class_info, str):
            data.setdefault("_class", class_info)
        return data

    def get_data(self):
        return self._data

    def update_data(self, data_dict):
        if isinstance(data_dict, dict):
            for k, v in data_dict.items():
                if k not in {"_id", "_class"}:
                    self._data[k] = v
            self._save()

    def set_data(self, key, value):
        if key not in {"_id", "_class"}:
            self._data[key] = value
            self._save()

    def _save(self):
        self._raw["_data"] = self._data
        if self._raw_node_obj is not None:
            try:
                self._raw_node_obj.payload_json = self._raw
                self._raw_node_obj.updated_at = datetime.now(timezone.utc)
                db.session.add(self._raw_node_obj)
                db.session.commit()
                try:
                    _runtime_cache_invalidate(self._download_url)
                except Exception:
                    pass
            except Exception:
                db.session.rollback()
                raise
        return True

    def to_dict(self):
        return {
            "_id": self._id,
            "_class": self._data.get("_class"),
            "_data": self._data,
            "_download_url": self._download_url,
        }


def resolve_download_url_node_context(node_id, fallback_class_name, download_url):
    raw_node_obj = None
    node_json, loaded_local = _load_raw_node_payload_for_event(node_id=node_id, download_url=download_url)
    loaded_from = "local_raw_node" if loaded_local and node_json is not None else "http"

    if node_json is None:
        if _raw_node_id_from_download_url(download_url):
            raise RuntimeError(f"RawNode not found locally for {download_url}")
        node_json = _noda_download_json_cached(download_url)

    if not isinstance(node_json, dict):
        raise RuntimeError("download_url returned non-object JSON")

    try:
        raw_id = _raw_node_id_from_download_url(download_url) or str(node_id or "").strip()
        if raw_id:
            raw_node_obj = db.session.execute(select(RawNode).where(RawNode.node_id == raw_id)).scalar_one_or_none()
    except Exception:
        raw_node_obj = None

    class_json = _extract_class_json_from_node_json(node_json)
    class_name = _extract_class_name_from_class_json(
        node_json.get("_class") or node_json.get("class"),
        fallback_class_name=fallback_class_name,
        node_json=node_json,
    )

    node = RemoteJsonNode(
        node_id=node_id or node_json.get("_id") or node_json.get("node_id") or node_json.get("node_uid"),
        node_json=node_json,
        download_url=download_url,
        raw_node_obj=raw_node_obj,
    )
    if class_name:
        node._schema_class_name = class_name

    return {
        "mode": "download_url",
        "config": None,
        "config_uid": getattr(node, "_config_uid", None),
        "class_name": class_name,
        "class_json": class_json,
        "node": node,
        "node_id": getattr(node, "_id", None),
        "download_url": download_url,
        "loaded_from": loaded_from,
    }


def execute_server_node_event(ctx, event_name, listener, message_data):
    message_data = message_data if isinstance(message_data, dict) else {}
    semantic_payload = _extract_server_message_payload(message_data)
    input_data = {
        "event": event_name,
        "listener": listener,
        "message": message_data,
        "payload": semantic_payload,
        "sender_user": message_data.get("sender_user"),
        "sender_display_name": message_data.get("sender_display_name"),
        "node_id": message_data.get("node_id") or message_data.get("node_uid") or message_data.get("_id"),
        "download_url": message_data.get("download_url") or message_data.get("raw_node_url") or message_data.get("thread_ref"),
    }

    node = ctx.get("node") if isinstance(ctx, dict) else None
    if node is not None and listener == SERVER_INPUT_LISTENER:
        _update_node_data_message_payload(node, semantic_payload)
    elif node is not None and listener == DISCUSSION_INPUT_LISTENER:
        text = message_data.get("text")
        if text in (None, ""):
            text = _discussion_text(
                title=message_data.get("title") or "",
                body=message_data.get("body") or "",
                payload=semantic_payload if isinstance(semantic_payload, dict) else {},
            )
        image_url = message_data.get("image_url")
        if image_url in (None, "") and isinstance(semantic_payload, dict):
            image_url = semantic_payload.get("image_url") or semantic_payload.get("image") or ""
        _update_node_message_fields(
            node,
            text=text or "",
            image_url=image_url or "",
            sender_user=message_data.get("sender_user") or "",
            sender_display_name=message_data.get("sender_display_name") or "",
        )

    tokens = ctx.get("_runtime_context_tokens")
    try:
        if ctx.get("mode") == "config":
            return execute_config_node_event(ctx, event_name, listener, input_data)

        if ctx.get("mode") == "download_url":
            return execute_download_url_node_event(ctx, event_name, listener, input_data)

        raise RuntimeError(f"Unsupported node context mode: {ctx.get('mode')}")
    finally:
        if tokens is not None:
            try:
                _nodes_mod.reset_runtime_context(tokens)
            except Exception:
                pass


def execute_config_node_event(ctx, event_name, listener, input_data):
    class_obj = ctx["class_obj"]
    node = ctx["node"]

    event_obj = db.session.execute(
        select(ClassEvent).where(
            ClassEvent.class_id == class_obj.id,
            ClassEvent.event == event_name,
            ClassEvent.listener == listener,
        )
    ).scalar_one_or_none()

    if not event_obj:
        return {"event_found": False, "event": event_name, "listener": listener, "actions": []}

    actions = sorted(list(event_obj.actions), key=lambda a: (getattr(a, "order", 0) or 0, getattr(a, "id", 0) or 0))
    results = []
    for action in actions:
        results.append(execute_config_event_action(node, action, input_data))

    return {"event_found": True, "event": event_name, "listener": listener, "actions": results}


def execute_config_event_action(node, action, input_data):
    method_name = (getattr(action, "method", "") or "").strip()
    post_method_name = (getattr(action, "post_execute_method", "") or getattr(action, "postExecuteMethod", "") or "").strip()

    result = {
        "action": getattr(action, "action", "run"),
        "source": getattr(action, "source", "internal"),
        "method": method_name,
        "postExecuteMethod": post_method_name,
        "status": True,
        "data": None,
        "postExecuteData": None,
    }

    if method_name:
        result["data"] = execute_config_method(node, action, method_name, input_data, post=False)

    if post_method_name:
        post_input = {"input": input_data, "result": result["data"]}
        result["postExecuteData"] = execute_config_method(node, action, post_method_name, post_input, post=True)

    return result


def execute_config_method(node, action, method_name, input_data, post=False):
    if method_name == "PythonScript":
        if post:
            script = getattr(action, "post_execute_text", "") or getattr(action, "postExecuteMethodText", "") or ""
        else:
            script = getattr(action, "method_text", "") or getattr(action, "methodText", "") or ""
        if not str(script or "").strip():
            raise RuntimeError("PythonScript action has no methodText")
        return run_inline_python_script(script, node, input_data)

    if not hasattr(node, method_name):
        raise RuntimeError(f"Method {method_name} not found in node class {node.__class__.__name__}")

    out = getattr(node, method_name)(input_data)
    if isinstance(out, tuple) and len(out) == 2:
        success, data = out
        return {"status": bool(success), "data": data}
    return out


def _event_matches(event_obj, event_name, listener):
    if not isinstance(event_obj, dict):
        return False
    return event_obj.get("event") == event_name and (event_obj.get("listener") or "") == listener


def execute_download_url_node_event(ctx, event_name, listener, input_data):
    class_json = ctx.get("class_json") or {}
    node = ctx["node"]
    events = class_json.get("events") or class_json.get("Events") or []

    event_obj = None
    for ev in events:
        if _event_matches(ev, event_name, listener):
            event_obj = ev
            break

    if not event_obj:
        return {"event_found": False, "event": event_name, "listener": listener, "actions": []}

    actions = event_obj.get("actions") or event_obj.get("Actions") or []
    results = []
    for action in actions:
        if isinstance(action, dict):
            results.append(execute_download_url_event_action(node, class_json, action, input_data))

    return {"event_found": True, "event": event_name, "listener": listener, "actions": results}


def execute_download_url_event_action(node, class_json, action, input_data):
    method_name = (action.get("method") or "").strip()
    post_method_name = (action.get("postExecuteMethod") or action.get("post_execute_method") or "").strip()

    result = {
        "action": action.get("action", "run"),
        "source": action.get("source", "internal"),
        "method": method_name,
        "postExecuteMethod": post_method_name,
        "status": True,
        "data": None,
        "postExecuteData": None,
    }

    if method_name:
        result["data"] = execute_download_url_method(
            node=node,
            class_json=class_json,
            method_name=method_name,
            action=action,
            input_data=input_data,
            script_text_keys=("methodText", "method_text", "code", "source"),
        )

    if post_method_name:
        post_input = {"input": input_data, "result": result["data"]}
        result["postExecuteData"] = execute_download_url_method(
            node=node,
            class_json=class_json,
            method_name=post_method_name,
            action=action,
            input_data=post_input,
            script_text_keys=("postExecuteMethodText", "post_execute_text", "postExecuteText", "post_code"),
        )

    return result


def execute_download_url_method(node, class_json, method_name, action, input_data, script_text_keys=("methodText", "method_text", "code", "source")):
    if method_name == "PythonScript":
        script = ""
        for key in script_text_keys:
            value = action.get(key)
            if isinstance(value, str) and value.strip():
                script = value
                break
        if not script.strip():
            raise RuntimeError("PythonScript action has no methodText/method_text/code/source")
        return run_inline_python_script(script, node, input_data)

    method_json = find_method_json(class_json, method_name)
    if method_json:
        engine = (method_json.get("engine") or "").lower()
        code = method_json.get("code") or method_json.get("source") or method_json.get("methodText") or ""
        if engine in {"python", "pythonscript", "server_python", "python_script"} or (isinstance(code, str) and code.strip()):
            return run_inline_python_script(code, node, input_data)

    if hasattr(node, method_name):
        return getattr(node, method_name)(input_data)

    raise RuntimeError(f"Method {method_name} not found in remote class JSON")


def find_method_json(class_json, method_name):
    for m in (class_json.get("methods") or class_json.get("Methods") or []):
        if not isinstance(m, dict):
            continue
        if m.get("name") == method_name or m.get("code") == method_name:
            return m
    return None


def _looks_like_http_url_text(value):
    value = (value or "").strip()
    if not value or "\n" in value or "\r" in value:
        return False
    try:
        parsed = urlparse(value)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def _resolve_inline_python_script_text(script):
    script = "" if script is None else str(script).strip()
    if not script:
        return script
    return _noda_load_python_script_code(script)


def _node_event_config_uid(node):
    try:
        return str(getattr(node, "_config_uid", "") or "").strip()
    except Exception:
        return ""


def run_inline_python_script(script, node, input_data):
    """
    Supports either inline Python code or an URL/S3 ref to a .py file.
    The script may set result, or define run(node, input_data) / main(node, input_data).
    """
    script = _resolve_inline_python_script_text(script)
    if not isinstance(script, str) or not script.strip():
        raise RuntimeError("Empty PythonScript")

    node_cfg_uid = _node_event_config_uid(node)
    ns = {
        "__builtins__": __builtins__,
        "json": json,
        "datetime": datetime,
        "timezone": timezone,
        "requests": requests,
        "Node": getattr(_nodes_mod, "Node", None),
        "nodes": _nodes_mod,
        "node": node,
        "self": node,
        "_data": getattr(node, "_data", {}),
        "input_data": input_data,
        "payload": input_data.get("payload") or {},
        "message": input_data.get("message") or {},
        "result": None,
        "sendTextMessage": _noda_send_text_message,
        "sendImageMessage": lambda target, text, filename: _noda_send_image_message(target, text, filename, config_uid=node_cfg_uid),
        "sendTextToNodeDiscussion": _noda_send_text_to_node_discussion,
        "sendImageToNodeDiscussion": lambda node_arg, text, filename: _noda_send_image_to_node_discussion(
            node_arg, text, filename, config_uid=_node_event_config_uid(node_arg) or node_cfg_uid
        ),
    }

    exec(script, ns, ns)

    if callable(ns.get("run")):
        out = ns["run"](node, input_data)
    elif callable(ns.get("main")):
        out = ns["main"](node, input_data)
    else:
        out = ns.get("result")

    if isinstance(out, tuple) and len(out) == 2:
        success, data = out
        return {"status": bool(success), "data": data}

    return out


def _payload_is_node_discussion(payload):
    if not isinstance(payload, dict):
        return False
    return (
        str(payload.get("thread_type") or "").strip() == "node_discussion"
        or str(payload.get("type") or "").strip() == "node_discussion_message"
    )


def _fallback_extract_node_discussion_node_id(payload):
    if not isinstance(payload, dict):
        return ""
    for key in ("node_id", "node_uid", "raw_node_id", "_id"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    thread_ref = str(payload.get("thread_ref") or "").strip()
    if "/api/raw-node/" in thread_ref:
        return _raw_node_id_from_download_url(thread_ref)
    return ""


def _get_discussion_node_id(payload):
    try:
        value = _extract_node_discussion_node_id(payload)
        if value:
            return str(value).strip()
    except Exception:
        pass
    return _fallback_extract_node_discussion_node_id(payload)


def _discussion_download_url(payload):
    if not isinstance(payload, dict):
        return ""
    value = str(payload.get("download_url") or payload.get("raw_node_url") or "").strip()
    if value:
        return value
    thread_ref = str(payload.get("thread_ref") or "").strip()
    if thread_ref.startswith("http://") or thread_ref.startswith("https://") or "/api/raw-node/" in thread_ref:
        return thread_ref
    return ""


def _discussion_text(title='', body='', payload=None):
    payload = payload if isinstance(payload, dict) else {}
    value = payload.get("text")
    if value in (None, ""):
        value = payload.get("message")
    if value in (None, ""):
        value = payload.get("body")
    if value in (None, ""):
        value = body
    if value in (None, ""):
        value = title
    return value or ""


def _message_payload_json(value):
    """Return the incoming message payload in a JSON-friendly form."""
    if value is None:
        return {}
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return value
    try:
        # Incoming Flask JSON is already serializable, but keep this safe for
        # PythonScript/internal callers that pass datetime or other objects.
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        try:
            return json.loads(json.dumps(value, ensure_ascii=False, default=str))
        except Exception:
            return str(value)


def _extract_server_message_payload(message_data):
    """Extract the semantic payload for onDataMessage/onDiscussionMessage."""
    message_data = message_data if isinstance(message_data, dict) else {}
    if "payload" in message_data:
        return _message_payload_json(message_data.get("payload"))

    data = message_data.get("data")
    if isinstance(data, dict) and "payload" in data:
        return _message_payload_json(data.get("payload"))
    if data is not None:
        return _message_payload_json(data)

    return {}


def _save_node_runtime_fields(node, fields: dict) -> None:
    """Write transient message fields into node._data and persist them."""
    if node is None or not isinstance(fields, dict) or not fields:
        return
    try:
        data = getattr(node, "_data", None)
        if not isinstance(data, dict):
            get_data = getattr(node, "get_data", None)
            data = get_data() if callable(get_data) else None
        if not isinstance(data, dict):
            return

        for key, value in fields.items():
            data[key] = value

        try:
            setattr(node, "_data", data)
        except Exception:
            pass

        save = getattr(node, "_save", None)
        if callable(save):
            save()
            return

        update_data = getattr(node, "update_data", None)
        if callable(update_data):
            update_data(fields)
    except Exception as e:
        print("Node runtime message field update failed:", e, flush=True)


def _update_node_message_fields(node, text='', image_url='', sender_user='', sender_display_name=''):
    fields = {
        # Contract expected by onDiscussionMessage handlers.
        "_message_text": text or "",
        "_message_image_url": image_url or "",
        "_message_sender_user": sender_user or "",
        "_message_sender_user_display_name": sender_display_name or "",

        # Backward-compatible aliases from the previous implementation.
        "_last_input_text": text or "",
        "_last_input_image_url": image_url or "",
        "_last_input_sender_user": sender_user or "",
        "_last_input_sender_display_name": sender_display_name or "",
    }
    _save_node_runtime_fields(node, fields)


def _update_node_data_message_payload(node, payload=None):
    _save_node_runtime_fields(node, {"_message_payload": _message_payload_json(payload)})


def handle_server_discussion_message(*, target_type, target_id, title='', body='', payload=None, sender_user=None):
    payload = payload if isinstance(payload, dict) else {}
    if not _payload_is_node_discussion(payload):
        return None

    resolved_sender_user = _get_sender_user(sender_user or payload.get("sender_user"))
    # Messages produced by PythonScript helpers must not recursively invoke the same handler.
    if str(resolved_sender_user or "").strip().lower() == SCRIPT_HANDLER_SENDER.lower():
        return None

    node_id = _get_discussion_node_id(payload)
    if not node_id:
        return None

    download_url = _discussion_download_url(payload)
    text = _discussion_text(title=title, body=body, payload=payload)
    image_url = payload.get("image_url") or payload.get("image") or ""
    sender_display_name = payload.get("sender_display_name") or _get_sender_display_name(resolved_sender_user)

    msg = {
        "type": "node_input",
        "node_id": node_id,
        "node_uid": payload.get("node_uid") or node_id,
        "download_url": download_url,
        "raw_node_url": payload.get("raw_node_url"),
        "payload": payload,
        "message": payload,
        "text": text,
        "image": payload.get("image"),
        "image_url": payload.get("image_url"),
        "target_type": target_type,
        "target_id": target_id,
        "sender_user": resolved_sender_user,
        "sender_display_name": sender_display_name,
        "_client_message_id": payload.get("_client_message_id") or payload.get("client_message_id"),
    }

    ctx = resolve_server_node_context(msg)
    if not ctx:
        return None

    node = ctx.get("node")
    if node is not None:
        _update_node_message_fields(
            node,
            text=text,
            image_url=image_url,
            sender_user=resolved_sender_user,
            sender_display_name=sender_display_name,
        )

    return execute_server_node_event(
        ctx=ctx,
        event_name=DISCUSSION_INPUT_EVENT,
        listener=DISCUSSION_INPUT_LISTENER,
        message_data=msg,
    )


def _maybe_trigger_node_discussion_input_from_payload(*, target_type, target_id, title='', body='', payload=None, sender_user=None):
    try:
        if not _payload_is_node_discussion(payload):
            return None
        return handle_server_discussion_message(
            target_type=target_type,
            target_id=target_id,
            title=title,
            body=body,
            payload=payload,
            sender_user=sender_user,
        )
    except Exception as e:
        print('Node discussion input hook failed:', e)
        return None

@app.route('/webapi/messages/device/<device_uid>', methods=['POST'])
@login_required
def web_push_device_message(device_uid):
    data = request.get_json(silent=True) or {}
    title = data.get('title') or 'Direct message'
    body = data.get('body') or data.get('message') or 'New message'
    payload = data.get('data')
    explicit_sender = data.get('sender_user')
    sender_user = _get_sender_user(explicit_sender)
    result = send_message_to_device_global(device_uid, title, body, payload, sender_user=sender_user)
    return jsonify({'ok': bool(result.get('ok')), 'device_uid': device_uid, 'result': result}), (200 if result.get('ok') else 400)


def _normalize_device_uid(value):
    return str(value or '').strip()


def _normalize_user_key(value):
    return str(value or '').strip()


def _resolve_sender_display_name(sender_user=None, payload=None):
    if isinstance(payload, dict):
        payload_name = str(payload.get('sender_display_name') or '').strip()
        if payload_name:
            return payload_name
    return _get_sender_display_name(sender_user)


def _serialize_outgoing_message(msg, device_uid=None, group_title_map=None):
    payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
    group_id = None
    group_title = None
    if str(msg.target_type or '').strip() == 'group':
        group_id = _normalize_group_id(msg.target_id or payload.get('group_id')) or None
        payload_title = str(payload.get('group_title') or '').strip()
        map_title = str((group_title_map or {}).get(group_id) or '').strip() if group_id else ''
        group_title = payload_title or map_title or None

    msg_type = str(payload.get('type') or '').strip() or None
    text_value = payload.get('text')
    if text_value is None:
        text_value = msg.body or ''

    return {
        'client_message_id': msg.client_message_id,
        'title': msg.title or '',
        'body': msg.body or '',
        'data': payload,
        'status': msg.status,
        'target_type': msg.target_type,
        'target_id': msg.target_id,
        'sender_user': msg.sender_user,
        'sender_display_name': _resolve_sender_display_name(msg.sender_user, payload),
        'group_id': group_id,
        'group_title': group_title,
        'type': msg_type,
        'text': text_value,
        'created_at': msg.created_at.isoformat() if msg.created_at else None,
        'accepted_at': msg.accepted_at.isoformat() if msg.accepted_at else None,
        'pushed_at': msg.pushed_at.isoformat() if msg.pushed_at else None,
        'ack_at': msg.ack_at.isoformat() if msg.ack_at else None,
        'last_error': msg.last_error,
        'device_uid': device_uid,
        'acked_for_device': False,
    }


def _list_pending_user_messages_impl(user_key, device_uid, limit=200, since=None):
    user_key = _normalize_user_key(user_key)
    device_uid = _normalize_device_uid(device_uid)
    if not user_key:
        return {'ok': False, 'error': 'user_key is required'}, 400
    if not device_uid:
        return {'ok': False, 'error': 'device_uid is required'}, 400

    try:
        limit = int(limit or 200)
    except Exception:
        limit = 200
    if limit <= 0:
        limit = 200
    if limit > 1000:
        limit = 1000

    _node_discussion_debug('pending.start', user_key=user_key, device_uid=device_uid, limit=limit, since=since)

    group_ids = [row.group_id for row in MessageGroupMember.query.filter(
        sa.func.lower(MessageGroupMember.user_key) == user_key.lower()
    ).all() if _normalize_group_id(row.group_id)]

    query = OutgoingMessageLog.query.filter(
        sa.or_(
            sa.and_(OutgoingMessageLog.target_type == 'user', OutgoingMessageLog.target_id == user_key),
            sa.and_(OutgoingMessageLog.target_type == 'group', OutgoingMessageLog.target_id.in_(group_ids)) if group_ids else sa.false(),
        )
    )

    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(str(since).replace('Z', '+00:00'))
        except Exception:
            return {'ok': False, 'error': 'invalid_since', 'details': 'Use ISO datetime'}, 400

    if since_dt is not None:
        query = query.filter(
            sa.or_(
                OutgoingMessageLog.accepted_at >= since_dt,
                OutgoingMessageLog.pushed_at >= since_dt,
                OutgoingMessageLog.created_at >= since_dt,
            )
        )

    ack_exists = sa.exists().where(sa.and_(
        OutgoingMessageDeviceAck.message_id == OutgoingMessageLog.id,
        OutgoingMessageDeviceAck.device_uid == device_uid,
    ))
    query = query.filter(~ack_exists)

    messages = query.order_by(
        OutgoingMessageLog.accepted_at.desc().nullslast(),
        OutgoingMessageLog.created_at.desc()
    ).limit(limit).all()

    _node_discussion_debug(
        'pending.query_result',
        user_key=user_key,
        device_uid=device_uid,
        count=len(messages),
        group_ids=group_ids,
        messages=[
            {
                'client_message_id': m.client_message_id,
                'target_type': m.target_type,
                'target_id': m.target_id,
                'status': m.status,
                'payload_type': (m.payload_json or {}).get('type') if isinstance(m.payload_json, dict) else None,
                'thread_type': (m.payload_json or {}).get('thread_type') if isinstance(m.payload_json, dict) else None,
                'node_id': (m.payload_json or {}).get('node_id') if isinstance(m.payload_json, dict) else None,
            }
            for m in messages[:10]
        ],
    )

    group_title_map = {}
    effective_group_ids = sorted({_normalize_group_id(msg.target_id) for msg in messages if str(msg.target_type or '').strip() == 'group' and _normalize_group_id(msg.target_id)})
    if effective_group_ids:
        groups = MessageGroup.query.filter(MessageGroup.group_id.in_(effective_group_ids)).all()
        group_title_map = {group.group_id: group.title or '' for group in groups}

    items = [_serialize_outgoing_message(msg, device_uid=device_uid, group_title_map=group_title_map) for msg in messages]

    return {
        'ok': True,
        'user_key': user_key,
        'device_uid': device_uid,
        'count': len(items),
        'messages': items,
    }, 200


def _ack_message_impl(client_message_id, ack_by=None, ack_payload=None, device_uid=None, user_key=None):
    client_message_id = str(client_message_id or '').strip()
    if not client_message_id:
        return {'ok': False, 'error': 'client_message_id is required'}, 400

    msg = OutgoingMessageLog.query.filter_by(client_message_id=client_message_id).first()
    if not msg:
        return {'ok': False, 'error': 'message_not_found', 'client_message_id': client_message_id}, 404

    ack_payload = ack_payload if isinstance(ack_payload, dict) else {}
    ack_by = _normalize_user_key(ack_by) or None
    device_uid = _normalize_device_uid(device_uid or ack_payload.get('device_uid'))
    user_key = _normalize_user_key(user_key or ack_payload.get('user_key'))

    if not user_key:
        try:
            api_user = getattr(g, 'api_user', None)
            if api_user and getattr(api_user, 'email', None):
                user_key = _normalize_user_key(api_user.email)
        except Exception:
            pass

    if msg.target_type == 'user':
        expected_user = _normalize_user_key(msg.target_id)
        if not device_uid:
            return {'ok': False, 'error': 'device_uid is required', 'client_message_id': client_message_id}, 400
        if user_key and expected_user and user_key.lower() != expected_user.lower():
            return {'ok': False, 'error': 'forbidden', 'client_message_id': client_message_id}, 403
        if ack_by and expected_user and ack_by.lower() != expected_user.lower():
            return {'ok': False, 'error': 'forbidden', 'client_message_id': client_message_id}, 403

        device_ack = OutgoingMessageDeviceAck.query.filter_by(message_id=msg.id, device_uid=device_uid).first()
        already_acked = bool(device_ack and device_ack.ack_at)
        now = datetime.now(timezone.utc)
        if not device_ack:
            device_ack = OutgoingMessageDeviceAck(
                message_id=msg.id,
                client_message_id=msg.client_message_id,
                user_key=expected_user or user_key or '',
                device_uid=device_uid,
                ack_at=now,
                ack_by=ack_by,
                ack_payload=ack_payload,
            )
            db.session.add(device_ack)
        elif not already_acked:
            device_ack.ack_at = now
            device_ack.ack_by = ack_by
            device_ack.ack_payload = ack_payload

        msg.last_error = None
        db.session.commit()

        sender_user = str(msg.sender_user or '').strip() or None
        original_payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
        original_type = str(original_payload.get('type') or '').strip().lower()
        ack_target_user = expected_user or user_key or None
        should_notify_sender = (
            bool(sender_user)
            and original_type != 'message_ack'
        )
        if should_notify_sender:
            ack_notice_payload = {
                'type': 'message_ack',
                '_client_message_id': client_message_id,
                'device_uid': device_uid,
            }
            if ack_target_user:
                ack_notice_payload['user_key'] = sender_user
                ack_notice_payload['ack_user'] = ack_target_user
                ack_notice_payload['target_user'] = ack_target_user
            if ack_by:
                ack_notice_payload['ack_by'] = ack_by
            if device_ack and device_ack.ack_at:
                ack_notice_payload['ack_at'] = device_ack.ack_at.isoformat()
                ack_notice_payload['received_at'] = int(device_ack.ack_at.timestamp())
            if isinstance(ack_payload, dict) and ack_payload:
                ack_notice_payload['ack_payload_json'] = json.dumps(ack_payload, ensure_ascii=False)

            try:
                send_message_to_user_global(
                    sender_user,
                    'Message acknowledged',
                    '',
                    ack_notice_payload,
                    sender_user=ack_target_user or ack_by,
                )
            except Exception:
                app.logger.exception('Failed to notify sender about ack for %s', client_message_id)

        return {
            'ok': True,
            'client_message_id': client_message_id,
            'status': msg.status,
            'ack_at': device_ack.ack_at.isoformat() if device_ack and device_ack.ack_at else None,
            'received_at': int(device_ack.ack_at.timestamp()) if device_ack and device_ack.ack_at else None,
            'ack_by': device_ack.ack_by if device_ack else ack_by,
            'already_acked': already_acked,
            'device_uid': device_uid,
            'user_key': expected_user or user_key,
            'group_id': None,
        }, 200

    if msg.target_type == 'group':
        expected_group_id = _normalize_group_id(msg.target_id)
        ack_user = _normalize_user_key(ack_by or user_key)
        if not expected_group_id:
            return {'ok': False, 'error': 'group_id is required', 'client_message_id': client_message_id}, 400
        if not device_uid:
            return {'ok': False, 'error': 'device_uid is required', 'client_message_id': client_message_id}, 400
        if not ack_user:
            return {'ok': False, 'error': 'user_key is required', 'client_message_id': client_message_id, 'group_id': expected_group_id}, 400
        if not _user_can_access_group(ack_user, expected_group_id):
            return {'ok': False, 'error': 'forbidden', 'client_message_id': client_message_id, 'group_id': expected_group_id}, 403

        device_ack = OutgoingMessageDeviceAck.query.filter_by(message_id=msg.id, device_uid=device_uid).first()
        already_acked = bool(device_ack and device_ack.ack_at)
        now = datetime.now(timezone.utc)
        if not device_ack:
            device_ack = OutgoingMessageDeviceAck(
                message_id=msg.id,
                client_message_id=msg.client_message_id,
                user_key=ack_user,
                device_uid=device_uid,
                ack_at=now,
                ack_by=ack_by or ack_user,
                ack_payload=ack_payload,
            )
            db.session.add(device_ack)
        elif not already_acked:
            device_ack.user_key = device_ack.user_key or ack_user
            device_ack.ack_at = now
            device_ack.ack_by = ack_by or ack_user
            device_ack.ack_payload = ack_payload

        msg.last_error = None
        msg.ack_at = device_ack.ack_at
        msg.ack_by = device_ack.ack_by
        msg.ack_payload = ack_payload
        db.session.commit()

        sender_user = str(msg.sender_user or '').strip() or None
        original_payload = msg.payload_json if isinstance(msg.payload_json, dict) else {}
        original_type = str(original_payload.get('type') or '').strip().lower()
        group_title = str(original_payload.get('group_title') or '').strip()
        if not group_title:
            group = MessageGroup.query.filter_by(group_id=expected_group_id).first()
            if group:
                group_title = group.title or ''

        should_notify_sender = bool(sender_user) and original_type != 'message_ack'
        if should_notify_sender:
            ack_notice_payload = {
                'type': 'message_ack',
                '_client_message_id': client_message_id,
                'device_uid': device_uid,
                'user_key': sender_user,
                'ack_user': ack_user,
                'target_user': ack_user,
                'group_id': expected_group_id,
            }
            if group_title:
                ack_notice_payload['group_title'] = group_title
            if ack_by or ack_user:
                ack_notice_payload['ack_by'] = ack_by or ack_user
            if device_ack and device_ack.ack_at:
                ack_notice_payload['ack_at'] = device_ack.ack_at.isoformat()
                ack_notice_payload['received_at'] = int(device_ack.ack_at.timestamp())
            if isinstance(ack_payload, dict) and ack_payload:
                ack_notice_payload['ack_payload_json'] = json.dumps(ack_payload, ensure_ascii=False)

            try:
                send_message_to_user_global(
                    sender_user,
                    'Message acknowledged',
                    '',
                    ack_notice_payload,
                    sender_user=ack_user,
                )
            except Exception:
                app.logger.exception('Failed to notify sender about group ack for %s', client_message_id)

        return {
            'ok': True,
            'client_message_id': client_message_id,
            'status': msg.status,
            'ack_at': device_ack.ack_at.isoformat() if device_ack and device_ack.ack_at else None,
            'received_at': int(device_ack.ack_at.timestamp()) if device_ack and device_ack.ack_at else None,
            'ack_by': device_ack.ack_by if device_ack else (ack_by or ack_user),
            'already_acked': already_acked,
            'device_uid': device_uid,
            'user_key': ack_user,
            'group_id': expected_group_id,
        }, 200

    already_acked = bool(msg.ack_at)
    if not already_acked:
        msg.ack_at = datetime.now(timezone.utc)
        msg.ack_by = ack_by
        msg.ack_payload = ack_payload
        msg.status = 'acked'
        msg.last_error = None
        db.session.commit()

    return {
        'ok': True,
        'client_message_id': client_message_id,
        'status': msg.status,
        'ack_at': msg.ack_at.isoformat() if msg.ack_at else None,
        'received_at': int(msg.ack_at.timestamp()) if msg.ack_at else None,
        'ack_by': msg.ack_by,
        'already_acked': already_acked,
        'device_uid': device_uid or None,
        'user_key': user_key or None,
        'group_id': _normalize_group_id(msg.target_id) if str(msg.target_type or '').strip() == 'group' else None,
    }, 200


@app.route('/webapi/messages/ack/<client_message_id>', methods=['POST'])
@login_required
def webapi_message_ack(client_message_id):
    data = request.get_json(silent=True) or {}
    ack_by = getattr(current_user, 'email', None)
    device_uid = data.get('device_uid') or request.args.get('device_uid')
    payload, status = _ack_message_impl(
        client_message_id,
        ack_by=ack_by,
        ack_payload=data,
        device_uid=device_uid,
        user_key=ack_by,
    )
    return jsonify(payload), status


@app.route('/api/messages/ack/<client_message_id>', methods=['POST'])
@api_auth_required
def api_message_ack(client_message_id):
    data = request.get_json(silent=True) or {}
    device_uid = data.get('device_uid') or request.args.get('device_uid')
    user_key = data.get('user_key') or request.args.get('user_key')
    payload, status = _ack_message_impl(
        client_message_id,
        ack_by=None,
        ack_payload=data,
        device_uid=device_uid,
        user_key=user_key,
    )
    return jsonify(payload), status

@app.route('/webapi/messages/pending', methods=['GET'])
@login_required
def webapi_pending_messages():
    limit = request.args.get('limit', 200)
    since = request.args.get('since')
    user_key = request.args.get('user_key') or getattr(current_user, 'email', None)
    device_uid = request.args.get('device_uid')
    payload, status = _list_pending_user_messages_impl(user_key, device_uid, limit=limit, since=since)
    return jsonify(payload), status


@app.route('/api/messages/pending', methods=['GET'])
@api_auth_required
def api_pending_messages():
    limit = request.args.get('limit', 200)
    since = request.args.get('since')
    user_key = request.args.get('user_key')
    device_uid = request.args.get('device_uid')
    payload, status = _list_pending_user_messages_impl(user_key, device_uid, limit=limit, since=since)
    return jsonify(payload), status


@app.route('/api/room/<room_uid>/objects', methods=['GET'])
@api_auth_required
def get_room_objects(room_uid):
    """Get objects for the room"""
    config_uid = request.args.get('config_uid')
    class_name = request.args.get('class_name')
    since = request.args.get('since')
    object_id = request.args.get('object_id')

    query = RoomObjects.query.filter_by(room_uid=room_uid)

    if object_id:
        try:
            query = query.filter(RoomObjects.id == int(object_id))
        except Exception:
            pass
    if config_uid:
        query = query.filter_by(config_uid=config_uid)
    if class_name:
        query = query.filter_by(class_name=class_name)
    if since:
        try:
            since_date = datetime.fromisoformat(since.replace('Z', '+00:00'))
            query = query.filter(RoomObjects.created_at > since_date)
        except ValueError:
            pass

    api_user = getattr(g, 'api_user', None)
    ack_user = api_user.email if api_user else None

    objects = query.order_by(RoomObjects.created_at.desc()).all()

    if ack_user:
        objects = [o for o in objects if ack_user not in (o.acknowledged_by or [])]

    objects_data = []
    for obj in objects:
        objects_data.append({
            'id': obj.id,
            'config_uid': obj.config_uid,
            'class_name': obj.class_name,
            'objects': obj.objects_data,
            'created_at': obj.created_at.isoformat(),
            'expires_at': obj.expires_at.isoformat() if obj.expires_at else None
        })

    return jsonify(objects_data)

@app.route('/api/room/<room_uid>/objects/ack', methods=['POST'])
@api_auth_required
def acknowledge_room_objects(room_uid):
    Room.query.filter_by(uid=room_uid).first_or_404()

    data = request.get_json(silent=True) or {}
    object_ids = data.get('object_ids', [])

    if not isinstance(object_ids, list):
        return jsonify({
            'ok': False,
            'error': 'object_ids must be a list'
        }), 400

    api_user = getattr(g, 'api_user', None)
    if not api_user:
        return jsonify({
            'ok': False,
            'error': 'Unauthorized'
        }), 401

    ack_user = api_user.email

    updated_ids = []
    not_found_ids = []

    for obj_id in object_ids:
        try:
            obj_id_int = int(obj_id)
        except Exception:
            not_found_ids.append(obj_id)
            continue

        room_object = db.session.get(RoomObjects, obj_id_int)
        if not room_object or room_object.room_uid != room_uid:
            not_found_ids.append(obj_id)
            continue

        acknowledged = set(room_object.acknowledged_by or [])
        acknowledged.add(ack_user)
        room_object.acknowledged_by = list(acknowledged)
        updated_ids.append(obj_id_int)

    db.session.commit()

    return jsonify({
        'ok': True,
        'room_uid': room_uid,
        'acknowledged_by': ack_user,
        'updated_object_ids': updated_ids,
        'not_found_object_ids': not_found_ids
    })
  

@app.route('/api/room/<room_uid>/objects', methods=['DELETE'])
@api_auth_required
def cleanup_room_objects(room_uid):
    """Delete old objects in the room"""
    older_than = request.args.get('older_than')
    
    if not older_than:
        return jsonify({"error": "Parameter 'older_than' is required"}), 400
    
    try:
        cutoff_date = datetime.fromisoformat(older_than.replace('Z', '+00:00'))
        
        # Delete objects older than the specified date
        deleted_count = RoomObjects.query.filter(
            RoomObjects.room_uid == room_uid,
            RoomObjects.created_at < cutoff_date
        ).delete()
        
        db.session.commit()
        
        return jsonify({
            "status": "success",
            "deleted_count": deleted_count,
            "cutoff_date": cutoff_date.isoformat()
        })
        
    except ValueError:
        return jsonify({"error": "Invalid date format. Use ISO format"}), 400                

@app.route('/api/config/<config_uid>/node/<class_name>/search', methods=['POST'])
@api_auth_required
def search_nodes(config_uid, class_name):
    """API for searching nodes by condition"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        if os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers:
            isolated_globals = _load_server_handlers_ns(config_uid, config)
            
            # We check that the class exists and is a subclass of Node from this space
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # We get the search condition from the request body
                search_condition = request.get_json() or {}
                
                def condition_func(node):
                    node_data = node.to_dict().get('_data', {})
                    for key, value in search_condition.items():
                        if key not in node_data or str(node_data[key]) != str(value):
                            return False
                    return True
                
                # We perform a search
                results = node_class.find(condition_func, config_uid)
                return jsonify({node_id: node.to_dict() for node_id, node in results.items()})
        
        abort(404)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _api_coerce_number(x):
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        try:
            if "." in x:
                return float(x)
            return int(x)
        except Exception:
            return None
    return None

def _api_like(pattern: str, value: str) -> bool:
    pat = re.escape(pattern).replace(r"\%", ".*")
    return re.fullmatch(pat, value or "", flags=re.IGNORECASE) is not None

def _api_eval_leaf(node_data: dict, leaf: dict) -> bool:
    key = leaf.get("key")
    exp = leaf.get("exp")
    wanted = leaf.get("value")

    actual = (node_data or {}).get(key)

    if exp == "~":
        return _api_like(str(wanted or ""), str(actual or ""))

    a_num = _api_coerce_number(actual)
    w_num = _api_coerce_number(wanted)
    if a_num is not None and w_num is not None and exp in ("<", ">", "=", "!="):
        if exp == "<":
            return a_num < w_num
        if exp == ">":
            return a_num > w_num
        if exp == "=":
            return a_num == w_num
        if exp == "!=":
            return a_num != w_num

    a = str(actual) if actual is not None else ""
    w = str(wanted) if wanted is not None else ""

    if exp == "=":
        return a == w
    if exp == "!=":
        return a != w
    if exp == "<":
        return a < w
    if exp == ">":
        return a > w

    return False

def _api_eval_condition(node_data: dict, cond) -> bool:
    if cond is None:
        return True

    if isinstance(cond, dict):
        if "&&" in cond:
            return all(_api_eval_condition(node_data, c) for c in (cond.get("&&") or []))
        if "||" in cond:
            return any(_api_eval_condition(node_data, c) for c in (cond.get("||") or []))
        if "!" in cond:
            inner = cond.get("!")
            if isinstance(inner, list):
                return not all(_api_eval_condition(node_data, c) for c in inner)
            return not _api_eval_condition(node_data, inner)
        if "key" in cond and "exp" in cond:
            return _api_eval_leaf(node_data, cond)

    return False

@app.route('/api/config/<config_uid>/node/<class_name>/query', methods=['POST'])
@api_auth_required
def nodes_api_query(config_uid, class_name):
    import nodes as _nodes_mod

    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    if not config:
        abort(404)

    runtime_parsed = _build_runtime_parsed_config(config)
    ctx_tokens = _nodes_mod.set_runtime_context(config_uid, runtime_parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(ctx_tokens)
        return resp

    if not (os.path.isfile(_handlers_file_path(config_uid)) or config.nodes_server_handlers):
        abort(404)

    isolated_globals = _load_server_handlers_ns(config_uid, config)

    if (
        class_name not in isolated_globals or
        not hasattr(isolated_globals[class_name], '__bases__') or
        not any(base.__name__ == 'Node'
                for base in isolated_globals[class_name].__bases__)
    ):
        abort(404)

    node_class = isolated_globals[class_name]

    try:
        payload = request.get_json(silent=True)
        nodes = node_class.get_all(config_uid)

        # ["uid1","uid2",...]
        if isinstance(payload, list):
            wanted = set(str(x) for x in payload if x is not None)
            out = {}
            for node_id, node in nodes.items():
                d = node.to_dict()
                public_id = d.get("_data", {}).get("_id") or node_id
                if str(node_id) in wanted or str(public_id) in wanted:
                    out[node_id] = d
            return jsonify(out)

        # condition object
        if isinstance(payload, dict):
            out = {}
            for node_id, node in nodes.items():
                d = node.to_dict()
                data = d.get("_data", {}) or {}
                if _api_eval_condition(data, payload):
                    out[node_id] = d
            return jsonify(out)

        return jsonify({"error": "Body must be array of ids or condition object"}), 400

    except _nodes_mod.AcceptRejected as e:
        return jsonify({"status": False, "data": e.payload}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/catalog', methods=['GET'])
@api_auth_required
def api_catalog():
    user = getattr(g, "api_user", None)
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    owned = db.session.execute(
        select(Configuration).where(Configuration.user_id == user.id)
    ).scalars().all()

    shared = db.session.execute(
        select(Configuration)
        .join(UserConfigAccess, UserConfigAccess.config_id == Configuration.id)
        .where(UserConfigAccess.user_id == user.id)
    ).scalars().all()

    configs = {c.id: c for c in owned + shared}

    result = []
    for cfg in configs.values():
        cfg_uid = cfg.uid

        base_url = url_for('get_config', uid=cfg_uid, _external=True).replace('/api/config/', '/api/config/')


        classes = []
        for c in (cfg.classes or []):
            name = c.name
            classes.append({
                "name": name,
                "display_name": c.display_name or name,
                "urls": {
                    "get": f"{base_url}/node/{name}",
                    "post": f"{base_url}/node/{name}",
                    "query": f"{base_url}/node/{name}/query",
                }
            })

        datasets = []
        for d in (cfg.datasets or []):
            datasets.append({
                "name": d.name,
                "url": f"{base_url}/dataset/{d.name}/items"
            })

        result.append({
            "name": cfg.name,
            "uid": cfg_uid,
            "classes": classes,
            "datasets": datasets
        })

    return jsonify(result)


@app.route('/api/room/<room_uid>/task/<task_uid>', methods=['GET'])
@api_auth_required
def get_task(room_uid, task_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        tasks = tasks_db.get(room_uid, [])
        for i, task in enumerate(tasks):
            if task.get('uid') == task_uid:
                if task.get('_blocked'):
                    return jsonify({
                        'status': 'error',
                        'message': 'Task already blocked'
                    }), 400
                
                
                tasks[i]['_blocked'] = True
                tasks[i]['_blocked_at'] = datetime.now().isoformat()
                tasks_db[room_uid] = tasks
                tasks_db.commit()
                
                
                send_tasks_update(room_uid)
                
                return jsonify({
                    'status': 'success',
                    'task': task
                })
    
    return jsonify({'status': 'error', 'message': 'Task not found'}), 404

def handle_ws_command(room_uid, user, data, auth_success):
    command = data.get('type')
    
    if not (command == "debug" or command == "get_users"):
        if not auth_success==True:
            return


    if command == 'get_task':
        # Task reservation logic
        with app.app_context():  
            with SqliteDict(TASKS_DB_PATH) as tasks_db:
                tasks = tasks_db.get(room_uid, [])
                for task in tasks:
                    if task.get('uid') == data.get('task_uid') and not task.get('_blocked'):
                        task['_blocked'] = True
                        task['_blocked_by'] = user
                        task['_blocked_at'] = datetime.now().isoformat()
                        tasks_db[room_uid] = tasks
                        tasks_db.commit()
                        
                        ws = active_connections[room_uid].get(user)
                        if ws:
                            ws.send(json.dumps({                                'type': 'task_assigned',
                                'task': task
                            }))
                        send_tasks_update(room_uid)
                        return
                        
                # If the task is not found
                ws = active_connections[room_uid].get(user)
                if ws:
                    ws.send(json.dumps({
                        'type': 'error',
                        'message': 'Task not available'
                    }))
    elif command == 'get_users':
        # Send a list of all connected users
        users_list = get_connected_users(room_uid)
        ws = active_connections[room_uid].get(user)
        if ws and not ws.closed:
            ws.send(json.dumps({
                'type': 'users_update',
                'users': users_list
            }))                
    elif command == 'acknowledge_objects':
        # The client confirms receipt of the objects
        object_ids = data.get('object_ids', [])
        
        with app.app_context():
            for obj_id in object_ids:
                room_object = db.session.get(RoomObjects, obj_id)
                if room_object and room_object.room_uid == room_uid:
                    # Add the user to the list of confirmed users
                    acknowledged = set(room_object.acknowledged_by or [])
                    acknowledged.add(user)
                    room_object.acknowledged_by = list(acknowledged)

                   
                                
            db.session.commit()
            
            # Send confirmation to the client
            ws = active_connections[room_uid].get(user)
            if ws:
                ws.send(json.dumps({
                    'type': 'acknowledgment_confirmed',
                    'object_ids': object_ids
                }))
    elif command == 'remote_method_response':
        # Processing the response from the remote method
        request_id = data.get('request_id')
        result_data = data.get('data', {})
        error = data.get('error')
        
        # Save the result for the corresponding query
        if request_id in pending_responses:
            pending_responses[request_id]['completed'] = True
            pending_responses[request_id]['result'] = result_data
            pending_responses[request_id]['error'] = error
    
    elif command == 'get_objects':
       # Client requests objects
        config_uid = data.get('config_uid')
        class_name = data.get('class_name')
        since = data.get('since')
        
        with app.app_context():
            query = RoomObjects.query.filter_by(room_uid=room_uid)
            
            #if config_uid:
            #    query = query.filter_by(config_uid=config_uid)
            #if class_name:
            #    query = query.filter_by(class_name=class_name)
            #if since:
            #    try:
            #        since_date = datetime.fromisoformat(since.replace('Z', '+00:00'))
            #        query = query.filter(RoomObjects.created_at > since_date)
            #    except ValueError:
            #        pass
            
            objects = query.order_by(RoomObjects.created_at.desc()).all()

            # НЕ отдаём клиенту то, что он уже ack-нул
            objects = [o for o in objects if user not in (o.acknowledged_by or [])]
            
            ws = active_connections[room_uid].get(user)
            if ws:
                objects_data = []
                for obj in objects:
                    objects_data.append({
                        'id': obj.id,
                        'config_uid': obj.config_uid,
                        'class_name': obj.class_name,
                        'objects': obj.objects_data,
                        'created_at': obj.created_at.isoformat()
                    })
                
                ws.send(json.dumps({
                    'type': 'objects_response',
                    'objects': objects_data
                }))
    elif command == 'debug':
        description = data.get("description")
        node_id = data.get("node_id")
        node_data = data.get("node_data")
        
        # Send a debug message to all connected clients
        if room_uid in active_connections:
            debug_message = {
                'type': 'debug',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'description': description,
                'node_id': node_id,
                'node_data': node_data,
                'user': user
            }
            
            for conn_user, ws in list(active_connections[room_uid].items()):
                try:
                    if not ws.closed:
                        ws.send(json.dumps(debug_message))
                except WebSocketError:
                    active_connections[room_uid].pop(conn_user, None)






import ast


def remove_example_method_from_class(module_code, class_name):
   
    lines = module_code.split('\n')
    class_start = -1
    class_indent = 0
    
    # Looking for the beginning of the class
    for i, line in enumerate(lines):
        if line.strip().startswith(f'class {class_name}('):
            class_start = i
            class_indent = len(line) - len(line.lstrip())
            break
    
    if class_start == -1:
        return module_code
    
    # Search for example_method
    example_start = -1
    example_end = -1
    in_example = False
    
    for i in range(class_start + 1, len(lines)):
        line = lines[i]
        current_indent = len(line) - len(line.lstrip())
        
        if current_indent <= class_indent and line.strip():
            # End of class
            break
        
        if line.strip().startswith('def example_method('):
            example_start = i
            in_example = True
            continue
        
        if in_example and current_indent == class_indent + 4:
            # Still inside the method
            continue
        
        if in_example and current_indent <= class_indent:
            # End of method
            example_end = i
            break
    
    if example_start != -1:
        if example_end == -1:
            example_end = len(lines)
        
        # Remove example_method
        new_lines = lines[:example_start] + lines[example_end:]
        return '\n'.join(new_lines)
    
    return module_code
    


def _userfiles_root_dir() -> Path:
    
    return Path(os.path.dirname(os.path.abspath(__file__))) / "UserFiles"


def _userfiles_dir_for_config(config_uid: str) -> Path:
    # защита от path traversal
    if "/" in config_uid or "\\" in config_uid:
        abort(400, "invalid config uid")

    return _userfiles_root_dir() / config_uid


@app.post("/api/userfiles/<config_uid>/images")
def upload_images(config_uid):
    target_dir = _userfiles_dir_for_config(config_uid)
    target_dir.mkdir(parents=True, exist_ok=True)

    # можно присылать либо files[], либо file
    files = request.files.getlist("files")
    if not files and "file" in request.files:
        files = [request.files["file"]]

    if not files:
        return jsonify({"ok": False, "error": "No files provided"}), 400

    overwrite = request.form.get("overwrite", "0") == "1"

    saved = []
    errors = []

    for f in files:
        name = secure_filename(f.filename or "")
        if not name:
            errors.append("empty filename")
            continue

        dst = target_dir / name

        if dst.exists() and not overwrite:
            stem = dst.stem
            suffix = dst.suffix
            i = 1
            while True:
                candidate = target_dir / f"{stem}_{i}{suffix}"
                if not candidate.exists():
                    dst = candidate
                    break
                i += 1

        try:
            f.save(dst)
            saved.append(dst.name)
        except Exception as e:
            errors.append(f"{name}: {str(e)}")

    return jsonify({
        "ok": len(saved) > 0,
        "files": saved,
        "errors": errors
    }), (200 if saved else 400)


@app.get("/api/userfiles/<config_uid>/raw/<path:filename>")
def get_userfile(config_uid, filename):
    target_dir = _userfiles_dir_for_config(config_uid)

    safe_name = secure_filename(filename)
    if not safe_name:
        abort(404)

    file_path = target_dir / safe_name
    if not file_path.exists():
        abort(404)

    return send_from_directory(target_dir, safe_name, as_attachment=False)


#Datasets API
@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['GET'])
@api_auth_required
def get_dataset_items(uid, dataset_name):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    items = []
    for item in dataset.items:
        item_data = item.data.copy()  
        item_data['_id'] = item.item_id 
        items.append(item_data)
    return jsonify(items)


@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['DELETE'])
@api_auth_required
def delete_all_dataset_items(uid, dataset_name):
    """Delete all records from the dataset"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    try:
        
        deleted_count = DatasetItem.query.filter_by(dataset_id=dataset.id).delete()
        db.session.commit()
        
        return jsonify({
            "status": "success", 
            "message": f"Deleted {deleted_count} items",
            "deleted_count": deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<uid>/dataset/<dataset_name>/items/<item_id>', methods=['DELETE'])
@api_auth_required
def delete_dataset_item(uid, dataset_name, item_id):
    """Delete a specific record from a dataset by ID"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    
    item = DatasetItem.query.filter_by(dataset_id=dataset.id, item_id=item_id).first()
    if not item:
        return jsonify({"error": "Item not found"}), 404
    
    try:
        db.session.delete(item)
        db.session.commit()
        
        return jsonify({
            "status": "success", 
            "message": f"Item {item_id} deleted"
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['POST'])
@api_auth_required
def add_dataset_items(uid, dataset_name):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    items = request.get_json()
    
    if not isinstance(items, list):
        abort(400, description="Request body must be a JSON array")
    
    for item in items:
        if not isinstance(item, dict):
            continue
            
        item_id = item.get('_id')
        if '_id' not in item:
            item['_id'] = str(uuid.uuid4())
            
        item_id = item['_id']
            
        # Check if item already exists
        existing_item = DatasetItem.query.filter_by(dataset_id=dataset.id, item_id=item_id).first()
        
        if existing_item:
            # Update existing item
            existing_item.data = item
            existing_item.updated_at = datetime.now(timezone.utc)
        else:
            # Create new item
            new_item = DatasetItem(
                dataset_id=dataset.id,
                item_id=item_id,
                data=item
            )
            db.session.add(new_item)
    
    db.session.commit()
    return jsonify({"status": "success", "count": len(items)})

#Datasets - UI
# Add these routes for dataset management in the UI

from sqlalchemy.orm import joinedload

def migrate_events_json_to_tables(dry_run=False, commit=True):

    stats = {'classes_scanned':0, 'events_migrated':0, 'actions_migrated':0, 'skipped_existing':0}
    with app.app_context():
        classes = db.session.query(ConfigClass).options(joinedload(ConfigClass.event_objs)).all()
        for cls in classes:
            stats['classes_scanned'] += 1
            events_json = cls.events or []  
            
            if not isinstance(events_json, list) or len(events_json) == 0:
                continue

           
            if cls.event_objs and len(cls.event_objs) > 0:
                stats['skipped_existing'] += 1
                continue

            for ev in events_json:
                
                
                try:
                    if isinstance(ev, str):
                        ev_obj = {'event': ev, 'listener': '', 'source': 'internal', 'server': '', 'method': ''}
                    elif isinstance(ev, dict):
                        ev_obj = ev.copy()
                    else:
                        continue

                    event_name = ev_obj.get('event') or ev_obj.get('event_name') or ''
                    listener = ev_obj.get('listener', '') or ev_obj.get('listener_name','') or ''

                    
                    ce = ClassEvent(event=event_name, listener=listener, class_id=cls.id)
                    if not dry_run:
                        db.session.add(ce)
                        db.session.flush()  

                    
                    
                    actions_list = []
                    if isinstance(ev_obj.get('actions'), list) and len(ev_obj.get('actions'))>0:
                        actions_list = ev_obj.get('actions')
                    else:
                        
                        actions_list = [{
                            'action': 'run',
                            'source': ev_obj.get('source','internal') or 'internal',
                            'server': ev_obj.get('server','') or '',
                            'method': ev_obj.get('method','') or ev_obj.get('method_name','') or '',
                            'postExecuteMethod': ''
                        }]

                    
                    order = 0
                    for a in actions_list:
                        order += 1
                        act = EventAction(
                            action = a.get('action','run'),
                            source = a.get('source','internal') or 'internal',
                            server = a.get('server','') or '',
                            method = a.get('method','') or a.get('method_name','') or '',
                            post_execute_method = a.get('postExecuteMethod','') or a.get('postExecute','') or '',
                            order = order,
                            event_id = ce.id if not dry_run else None
                        )
                        if not dry_run:
                            db.session.add(act)
                        stats['actions_migrated'] += 1

                    stats['events_migrated'] += 1

                except Exception as e:
                    print("Error migrating event for class", cls.id, e)
                    db.session.rollback()
                    continue

        if not dry_run and commit:
            db.session.commit()
    return stats

def get_ws_scheme():
    # If Flask runs behind HTTPS (for example, via nginx with SSL)
    if request.is_secure or request.headers.get('X-Forwarded-Proto', '').lower() == 'https':
        return 'wss'
    return 'ws'

#NL_graph API
@app.route("/api/config/<uid>/handlers-server/save", methods=["POST"])
@api_auth_required
def api_handlers_server_save(uid):
    data = request.get_json(silent=True) or {}
    code = data.get("code") or ""

    user = getattr(request, "api_user", None)
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    config = db.session.scalars(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == user.id)
    ).first()
    if not config:
        return jsonify({"error": "not_found"}), 404

    if not code.strip():
        return jsonify({"error": "empty_code"}), 400

    is_valid, error = validate_python_syntax(code)
    if not is_valid:
        return jsonify({"error": "python_syntax_error", "details": error}), 400

    encoded = base64.b64encode(code.encode("utf-8")).decode("utf-8")
    config.nodes_server_handlers = encoded
    config.update_last_modified()
    db.session.commit()

    handlers_dir = os.path.join("Handlers", config.uid)
    os.makedirs(handlers_dir, exist_ok=True)
    with open(os.path.join(handlers_dir, "handlers.py"), "w", encoding="utf-8", newline="\n") as f:
        f.write(code)

    sync_classes_from_server_handlers(config)
    sync_methods_from_code(config)

    return jsonify({"ok": True})

@app.route("/api/config/create", methods=["POST"])
@api_auth_required
def api_config_create():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "New configuration").strip() or "New configuration"

    # user берём из api_auth_required (как current_user аналог)
    user = getattr(request, "api_user", None)  # <- как у тебя реализовано в api_auth_required
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    new_config = Configuration(
        name=name,
        user_id=user.id,
        content_uid=str(uuid.uuid4()),
        vendor=getattr(user, "config_display_name", None) or user.email,
        version="00.00.01",
    )
    new_config.uid = str(uuid.uuid4())

    # android handlers можно как раньше (если нужно), но nl_graph просит только server handlers
    default_server_handlers = get_default_server_handlers()
    new_config.nodes_server_handlers = base64.b64encode(default_server_handlers.encode("utf-8")).decode("utf-8")

    db.session.add(new_config)
    db.session.commit()

    handlers_dir = os.path.join("Handlers", new_config.uid)
    os.makedirs(handlers_dir, exist_ok=True)
    with open(os.path.join(handlers_dir, "handlers.py"), "w", encoding="utf-8", newline="\n") as f:
        f.write(default_server_handlers)

    return jsonify({"ok": True, "uid": new_config.uid, "name": new_config.name})
#


# Editor/configuration routes are kept in a separate module; API/runtime routes stay here.
try:
    from editor_routes import register_editor_routes
    register_editor_routes(app, globals())
except Exception as _e:
    print('Editor routes not loaded:', _e)


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        
        print(migrate_events_json_to_tables(dry_run=False))
        try:
            db.create_all(bind_key='client')
        except Exception as e:
            print('Could not init client bind:', e)


        inspector = db.inspect(db.engine)
        columns = inspector.get_columns('config_class')

        # --- lightweight sqlite migration for new ConfigClass fields (Migration tab) ---
        try:
            col_names = [c.get('name') for c in (columns or [])]
            with db.engine.begin() as conn:
                if 'migration_register_command' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN migration_register_command BOOLEAN DEFAULT 0'))
                if 'migration_register_on_save' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN migration_register_on_save BOOLEAN DEFAULT 0'))
                if 'migration_default_room_uid' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN migration_default_room_uid VARCHAR(36) DEFAULT ""'))
                if 'migration_default_room_alias' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN migration_default_room_alias VARCHAR(100) DEFAULT ""'))
                if 'link_share_mode' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN link_share_mode VARCHAR(30) DEFAULT ""'))
                if 'indexes_json' not in col_names:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN indexes_json JSON'))
        except Exception as e:
            print('Could not migrate config_class Migration fields:', e)

         
        if 'config_event' not in inspector.get_table_names():
            db.create_all()
            print("Created config_event table")
        
        if 'config_event_action' not in inspector.get_table_names():
            db.create_all()
            print("Created config_event_action table")
        
        
        if 'config_event' in inspector.get_table_names():
            config_event_columns = [col['name'] for col in inspector.get_columns('config_event')]
            
           
            if 'config_id' not in config_event_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_event ADD COLUMN config_id INTEGER'))
                    conn.execute(text('CREATE INDEX ix_config_event_config_id ON config_event (config_id)'))
                print("Added config_id to config_event table")
        
        if 'config_event_action' in inspector.get_table_names():
            config_event_action_columns = [col['name'] for col in inspector.get_columns('config_event_action')]
            
            if 'event_id' not in config_event_action_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_event_action ADD COLUMN event_id INTEGER'))
                    conn.execute(text('CREATE INDEX ix_config_event_action_event_id ON config_event_action (event_id)'))
                print("Added event_id to config_event_action table")

        if 'class_method' in inspector.get_table_names():
            class_method_columns = [col['name'] for col in inspector.get_columns('class_method')]
            
            if 'source' not in class_method_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE class_method ADD COLUMN source VARCHAR(100) DEFAULT "internal"'))
                print("Added source column to class_method table")   

            if 'server' not in class_method_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE class_method ADD COLUMN server VARCHAR(255) DEFAULT "internal"'))
                print("Added source column to class_method table")         


        if 'room_objects' in inspector.get_table_names():
            room_objects_columns = [col['name'] for col in inspector.get_columns('room_objects')]
        
        if 'acknowledged_by' not in room_objects_columns:
            with db.engine.begin() as conn:
                conn.execute(text('ALTER TABLE room_objects ADD COLUMN acknowledged_by JSON DEFAULT "[]"'))
            print("Added acknowledged_by column to room_objects table")

        if 'config_class' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('config_class')]
            
            if 'has_storage' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN has_storage BOOLEAN DEFAULT FALSE'))
            
            if 'class_type' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN class_type VARCHAR(50)'))
            if 'hidden' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN hidden BOOLEAN DEFAULT FALSE'))        
            
            if 'section' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN section VARCHAR(100)'))
            if 'section_code' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN section_code VARCHAR(100)'))     

            if 'display_name' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN display_name VARCHAR(100)'))
            
            if 'cover_image' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN cover_image TEXT'))  
                    
                             
        
        
        if 'config_section' not in inspector.get_table_names():
            section_columns = [col['name'] for col in inspector.get_columns('config_section')]
        
        if 'config_section' in inspector.get_table_names():
            section_columns = [col['name'] for col in inspector.get_columns('config_section')]
            if 'commands' not in section_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_section ADD COLUMN commands TEXT'))
                
            db.create_all()

        
        if 'user' in inspector.get_table_names():
            user_columns = [col['name'] for col in inspector.get_columns('user')]
            if 'config_display_name' not in user_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE user ADD COLUMN config_display_name VARCHAR(100) DEFAULT ""'))

            # Backward compatible defaults: existing users keep access to everything
            if 'can_designer' not in user_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE user ADD COLUMN can_designer BOOLEAN DEFAULT TRUE'))
            if 'can_client' not in user_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE user ADD COLUMN can_client BOOLEAN DEFAULT TRUE'))
            if 'can_api' not in user_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE user ADD COLUMN can_api BOOLEAN DEFAULT TRUE'))
            if 'parent_user_id' not in user_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE user ADD COLUMN parent_user_id INTEGER'))

            db.create_all()

        if 'dataset' in inspector.get_table_names():
            dataset_columns = [col['name'] for col in inspector.get_columns('dataset')]
            if 'view_template' not in dataset_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE dataset ADD COLUMN view_template TEXT'))
            if 'autoload' not in dataset_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE dataset ADD COLUMN autoload BOOLEAN DEFAULT FALSE'))
            db.create_all()  

       
        if 'configuration' in inspector.get_table_names():
            config_columns = [col['name'] for col in inspector.get_columns('configuration')]

            if 'content_uid' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN content_uid VARCHAR(100)'))
            if 'vendor' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN vendor TEXT'))   

            insp = sa.inspect(db.engine)
            if Configuration.__tablename__ in insp.get_table_names():
                columns = [c["name"] for c in insp.get_columns(Configuration.__tablename__)]
                if "common_layouts" not in columns:
                    print("Migration: add Configuration.common_layouts")
                    with db.engine.begin() as con:
                        con.execute(
                            sa.text(
                                f'ALTER TABLE {Configuration.__tablename__} '
                                'ADD COLUMN common_layouts JSON'
                            )
                        )            

            if 'user_id' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN user_id INTEGER'))
                    
                    first_user = db.session.execute(select(User)).scalar()
                    if first_user:
                        conn.execute(text('UPDATE configuration SET user_id = :user_id WHERE user_id IS NULL'), 
                                   {'user_id': first_user.id})
                    
                    conn.execute(text('CREATE INDEX ix_configuration_user_id ON configuration (user_id)'))

            if 'server_name' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN server_name VARCHAR(100) DEFAULT ""'))
            
            if 'nodes_server_handlers' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN nodes_server_handlers TEXT'))
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN nodes_server_handlers_meta JSON'))        

            
            if 'version' not in config_columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN version VARCHAR(20) DEFAULT "00.00.01"'))
            
            if 'last_modified' not in config_columns:
                with db.engine.begin() as conn:
                    
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN last_modified DATETIME'))
                   
                    conn.execute(text('UPDATE configuration SET last_modified = CURRENT_TIMESTAMP WHERE last_modified IS NULL'))
        
        with app.app_context():
            for cfg in Configuration.query.all():
                if not cfg.content_uid:
                    cfg.content_uid = str(uuid.uuid4())
                if not cfg.vendor:
                    cfg.vendor = cfg.user.config_display_name or cfg.user.email
            db.session.commit()


        if 'config_class' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('config_class')]
            
            if 'events' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN events TEXT'))
            if 'display_image_web' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN display_image_web TEXT DEFAULT ""'))

            if 'display_image_table' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN display_image_table TEXT DEFAULT ""'))

            if 'commands' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN commands TEXT DEFAULT ""'))

            if 'use_standard_commands' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN use_standard_commands BOOLEAN DEFAULT TRUE'))

            if 'svg_commands' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN svg_commands TEXT DEFAULT ""'))

            if 'init_screen_layout' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN init_screen_layout TEXT DEFAULT ""'))

            if 'init_screen_layout_web' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN init_screen_layout_web TEXT DEFAULT ""'))

            if 'plug_in' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN plug_in TEXT DEFAULT ""'))

            if 'plug_in_web' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE config_class ADD COLUMN plug_in_web TEXT DEFAULT ""'))
               

        if 'configuration' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('configuration')]
            if 'nodes_handlers' not in columns:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN nodes_handlers TEXT'))
                    conn.execute(text('ALTER TABLE configuration ADD COLUMN nodes_handlers_meta JSON'))
        
        
        
    

    # Create a custom WSGI server with WebSocket support
    def application(environ, start_response):
        path = environ.get('PATH_INFO', '')
        
        # Intercept WebSocket requests
        if path == '/ws' and 'wsgi.websocket' in environ:
            ws = environ['wsgi.websocket']
            query_string = environ.get('QUERY_STRING', '')
            parsed_params = parse_qs(query_string)

            channel = parsed_params.get('channel', [''])[0]

            # Node browser channel (separate from Rooms channel)
            if channel == 'nodes':
                handle_nodes_websocket(ws)
                return []

            # Node discussion channel for web-client Chat tabs.
            if channel == 'discussion':
                handle_discussion_websocket(ws)
                return []

            room_uid = parsed_params.get('room', [''])[0]
            android_id = parsed_params.get('android_id', [''])[0]
            device_model = parsed_params.get('device_model', [''])[0]
            if room_uid:
                handle_websocket(ws, room_uid)
                return []
        
        # All other requests are processed through Flask
        return app(environ, start_response)
    
    server = WSGIServer(
        ('0.0.0.0', 5000),
        application,
        handler_class=WebSocketHandler
    )
    print("Server running on:")
    print("HTTP: http://0.0.0.0:5000")
    print("WebSocket: ws://0.0.0.0:5000/ws?room=ROOM_UID")

    server.serve_forever()#test
