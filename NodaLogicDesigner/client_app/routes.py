from __future__ import annotations

import json
import os
import pickle
import sqlite3
import base64
import uuid
import re
import socket
import subprocess
import tempfile
import math
import time
import threading
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, url_for, after_this_request, send_from_directory, Response, make_response
from markupsafe import escape
from flask_login import current_user, login_required

from .nodalayout import render_nodalayout_html, DEFAULT_NL_CSS
from . import models
import nodes as _nodes_mod
import hashlib
import inspect
import mimetypes
from io import BytesIO
from sqlalchemy import select, or_, and_, func
from jinja2.sandbox import SandboxedEnvironment
from jinja2 import select_autoescape

try:
    import qrcode
except Exception:  # optional dependency
    qrcode = None


class _PrintAttrDict(dict):
    """Dictionary wrapper for PrintForm templates.

    Dot-access in Jinja should address _data keys, including names that collide
    with dict methods such as `items`, `keys`, or `values`.
    """
    def __getattribute__(self, name):
        if name.startswith('__'):
            return dict.__getattribute__(self, name)
        try:
            return dict.__getitem__(self, name)
        except KeyError:
            return dict.__getattribute__(self, name)


def _print_attr_tree(value: Any) -> Any:
    if isinstance(value, dict):
        return _PrintAttrDict({k: _print_attr_tree(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_print_attr_tree(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_print_attr_tree(v) for v in value)
    return value


class _PrintSandboxedEnvironment(SandboxedEnvironment):
    def is_safe_attribute(self, obj: Any, attr: str, value: Any) -> bool:
        if isinstance(obj, _PrintAttrDict) and attr in obj and not str(attr).startswith('__'):
            return True
        return super().is_safe_attribute(obj, attr, value)



def string_to_color(text: str) -> str:
    """Stable color for a tag string."""
    hash_object = hashlib.md5(str(text or '').encode('utf-8'))
    return f"#{hash_object.hexdigest()[:6]}"


def _tag_text_color(bg: str) -> str:
    bg = str(bg or '').strip()
    if not re.match(r'^#([0-9a-fA-F]{6})$', bg):
        return '#000000'
    r = int(bg[1:3], 16)
    g = int(bg[3:5], 16)
    b = int(bg[5:7], 16)
    # WCAG relative luminance approximation good enough for black/white choice.
    luminance = (0.299 * r + 0.587 * g + 0.114 * b)
    return '#000000' if luminance > 150 else '#FFFFFF'


def _normalize_node_tags(data: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    raw = (data or {}).get('_tags') if isinstance(data, dict) else None
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, str]] = []
    seen = set()
    for item in raw:
        tag_id = ''
        color = ''
        if isinstance(item, str):
            tag_id = item.strip()
            color = string_to_color(tag_id) if tag_id else ''
        elif isinstance(item, dict):
            tag_id = str(item.get('id') or item.get('name') or item.get('tag') or '').strip()
            color = str(item.get('color') or '').strip()
            if tag_id and not re.match(r'^#([0-9a-fA-F]{6})$', color):
                color = string_to_color(tag_id)
        if not tag_id or tag_id in seen:
            continue
        seen.add(tag_id)
        out.append({'id': tag_id, 'color': color, 'text_color': _tag_text_color(color)})
    return out


def _render_tags_html(data: Optional[Dict[str, Any]]) -> str:
    tags = _normalize_node_tags(data)
    if not tags:
        return ''
    parts = []
    for t in tags:
        tid = str(t.get('id') or '')
        bg = str(t.get('color') or string_to_color(tid))
        fg = str(t.get('text_color') or _tag_text_color(bg))
        parts.append(
            f'<span class="nl-tag-badge" data-nl-tag="{escape(tid)}" '
            f'style="display:inline-flex;align-items:center;border-radius:999px;padding:2px 8px;font-size:12px;line-height:1.4;background:{escape(bg)};color:{escape(fg)};">'
            f'{escape(tid)}</span>'
        )
    return '<div class="nl-tag-cloud d-flex gap-1 flex-wrap mt-2">' + ''.join(parts) + '</div>'


def _cover_with_tags(html: str, data: Optional[Dict[str, Any]], enabled: bool = False) -> str:
    # _tags are now rendered whenever they exist.  The enabled argument is kept
    # only for backward compatibility with older call sites / configs.
    base = str(html or '')
    tags_html = _render_tags_html(data)
    if not tags_html:
        return base
    return base + tags_html


def _tag_ids(data: Optional[Dict[str, Any]]) -> List[str]:
    return [str(t.get('id') or '') for t in _normalize_node_tags(data) if str(t.get('id') or '')]

import __main__ as main


# client_bp is registered by server/app.py as url_prefix="/client"
client_bp = Blueprint(
    "client",
    __name__,
    url_prefix="/client",
)

APP_TITLE = "NodaLogic Client"
DEFAULT_LIMIT_PER_CLASS = 50
AUTO_REFRESH_SECONDS = 10
RAW_NODES_SECTION_CODE = "__received_nodes__"
RAW_NODES_SECTION_NAME = "Received Nodes"

PROJECTION_CLASS_TYPE = "projection"
PROJECTION_KANBAN_TYPE = "kanban_projection"
PROJECTION_DIAGRAM_TYPE = "diagram_projection"
PROJECTION_SCHEDULE_TYPE = "schedule_projection"
PROJECTION_GANTT_TYPE = "gantt_projection"
# Projection nodes are reports. They may receive a transient list of object UIDs
# from onRunProjection so the browser can render immediately, but that list must
# not become saved projection state. Object positions/statuses live on the objects
# themselves in _projection_values[projection_uid].
PROJECTION_TRANSIENT_SAVE_FIELDS = {"_projection_objects"}
PRINT_FORM_CLASS_TYPE = "print_form"
PRINT_FORM_TEMPLATE_HTML_JINJA = "html_jinja"
SINGLETON_CLASS_TYPES = {"custom_process", PROJECTION_CLASS_TYPE}

def _class_type_value(cls_or_type: Any) -> str:
    if isinstance(cls_or_type, dict):
        return str(cls_or_type.get("class_type") or "data_node").strip()
    return str(cls_or_type or "data_node").strip()

def _is_singleton_class_type(cls_or_type: Any) -> bool:
    return _class_type_value(cls_or_type) in SINGLETON_CLASS_TYPES

def _is_projection_class_type(cls_or_type: Any) -> bool:
    return _class_type_value(cls_or_type) == PROJECTION_CLASS_TYPE

def _is_print_form_class_type(cls_or_type: Any) -> bool:
    return _class_type_value(cls_or_type) == PRINT_FORM_CLASS_TYPE


def _is_probably_print_template_base64(value: Any) -> bool:
    s = str(value or "").strip()
    if not s or len(s) % 4:
        return False
    try:
        raw = base64.b64decode(s.encode("ascii"), validate=True)
        text = raw.decode("utf-8")
    except Exception:
        return False
    return "\x00" not in text


def _decode_print_html_template(value: Any) -> str:
    s = str(value or "")
    if _is_probably_print_template_base64(s):
        try:
            return base64.b64decode(s.strip().encode("ascii"), validate=True).decode("utf-8")
        except Exception:
            return s
    return s


def _normalize_print_html_templates_in_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Decode PrintForm HTML templates for the web client runtime.

    Public/editor API exports print_html_template as base64 to make JSON import/export
    safe. The client renderer needs the original HTML/Jinja text.
    """
    if not isinstance(cfg, dict):
        return cfg
    for c in (cfg.get("classes") or []):
        if not isinstance(c, dict):
            continue
        if "print_html_template" in c:
            c["print_html_template"] = _decode_print_html_template(c.get("print_html_template") or "")
    return cfg

# Small per-request-ish memoization. These helpers are called many times while
# rendering Received Nodes; without memoization they repeatedly query device/ack
# tables and can burn CPU on large servers. Values are safe to reuse only for
# the current Flask request/user, so the cache key includes the current user id.
_CURRENT_USER_KEYS_CACHE: Dict[Any, List[str]] = {}
_CURRENT_USER_GROUP_IDS_CACHE: Dict[Tuple[Any, Tuple[str, ...]], set] = {}


def _guess_image_mimetype_from_url(value: str) -> str:
    parsed_path = ""
    try:
        parsed_path = urlparse(str(value or "")).path or ""
    except Exception:
        parsed_path = str(value or "")
    mimetype, _ = mimetypes.guess_type(parsed_path)
    if mimetype and str(mimetype).startswith("image/"):
        return mimetype
    return "image/jpeg"


def _is_cacheable_chat_image_url(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https"):
        return False
    s3_key_from_public_url = getattr(main, "_s3_key_from_public_url", None)
    try:
        if callable(s3_key_from_public_url) and s3_key_from_public_url(raw):
            return True
    except Exception:
        pass
    # Keep this permissive enough for older/mobile messages that already stored
    # a public image_url outside the configured S3 endpoint, while still only
    # proxying explicit web URLs.
    return True

def _received_nodes_section() -> Dict[str, str]:
    return {"code": RAW_NODES_SECTION_CODE, "name": RAW_NODES_SECTION_NAME}


def _with_received_nodes_section(sections: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = [_received_nodes_section()]
    for item in (sections or []):
        if (item.get("code") or "") != RAW_NODES_SECTION_CODE:
            out.append(item)
    return out


def _default_section_code(sections: List[Dict[str, str]]) -> str:
    """Return the first regular section; keep Received Nodes pinned but not default."""
    for item in (sections or []):
        code = item.get("code") or ""
        if code != RAW_NODES_SECTION_CODE:
            return code
    return (sections[0].get("code") if sections else "") or ""


def _server_model(name: str):
    return getattr(main, name, None)


def _extract_raw_node_class_name(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("full_name", "code", "uid", "id", "name", "class_name", "_name"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
    return str(value or "").strip()


def _raw_node_payload(obj) -> Dict[str, Any]:
    payload = getattr(obj, "payload_json", None)
    return payload if isinstance(payload, dict) else {}


def _extract_raw_node_class_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return embedded class JSON from a raw-node payload, if present.

    Android/server raw-node messages can carry either:
      * _class: "ClassName"              -> class is resolved from a client repo/config
      * _class: { ... full class json ... } -> class travels with the raw-node

    The web client must not render raw-nodes as plain _data only; layouts and
    events must see the real class JSON, exactly like PythonScript event flow
    does in app.py.
    """
    payload = payload if isinstance(payload, dict) else {}

    # Prefer the server-side helper when it exists, so web-client and runtime
    # event dispatch keep the same accepted aliases (_class/class/class_json...).
    helper = getattr(main, "_extract_class_json_from_node_json", None)
    if callable(helper):
        try:
            obj = helper(payload)
            if isinstance(obj, dict) and obj:
                return obj
        except Exception:
            pass

    for key in ("_class", "class", "class_json", "_class_json", "schema", "node_class"):
        value = payload.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _raw_node_data_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize node data without losing top-level raw-node metadata.

    The form still edits/renders _data, but class/layout/event resolution uses
    the full payload. If an older/raw sender did not wrap fields in _data, use
    the payload fields except structural wrappers.
    """
    payload = payload if isinstance(payload, dict) else {}
    data = payload.get("_data")
    if isinstance(data, dict):
        return dict(data or {})

    skip = {
        "_data", "data", "payload",
        "_class", "class", "class_json", "_class_json", "schema", "node_class",
        "_download_url", "download_url", "raw_node_url", "node_url", "thread_ref",
    }
    return {k: v for k, v in payload.items() if k not in skip}


def _raw_node_download_url(raw_node_id: str) -> str:
    raw_node_id = str(raw_node_id or "").strip()
    explicit = ""
    try:
        explicit = str(request.url_root.rstrip("/")) + f"/api/raw-node/{raw_node_id}"
    except Exception:
        explicit = f"/api/raw-node/{raw_node_id}"
    return explicit


def _raw_node_download_ref(payload: Dict[str, Any], raw_node_id: str) -> str:
    payload = payload if isinstance(payload, dict) else {}
    for key in ("_download_url", "download_url", "raw_node_url", "node_url", "thread_ref"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return _raw_node_download_url(raw_node_id)


def _class_name_from_embedded_class(raw_class: Dict[str, Any], fallback: str = "", payload: Optional[Dict[str, Any]] = None) -> str:
    helper = getattr(main, "_extract_class_name_from_class_json", None)
    if callable(helper):
        try:
            value = helper(raw_class, fallback_class_name=fallback, node_json=payload or {})
            if isinstance(value, str) and value.strip():
                return value.strip()
        except Exception:
            pass
    value = _extract_raw_node_class_name(raw_class)
    return value or str(fallback or "").strip()


def _raw_node_identity(payload: Dict[str, Any], fallback_node_id: str = "") -> Tuple[str, str, Dict[str, Any]]:
    data = _raw_node_data_from_payload(payload)
    embedded_class = _extract_raw_node_class_json(payload)
    class_name = _class_name_from_embedded_class(
        embedded_class,
        fallback=_extract_raw_node_class_name(payload.get("_class") or payload.get("class_name") or data.get("_class")),
        payload=payload,
    )
    node_id = str(payload.get("_id") or payload.get("node_id") or payload.get("node_uid") or data.get("_id") or fallback_node_id or "").strip()
    return class_name, node_id, dict(data or {})


def _merge_raw_class_into_parsed(base_parsed: Optional[Dict[str, Any]], class_name: str, class_obj: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build a parsed-config-like object where embedded raw class wins.

    render_nodalayout_html helpers expect the same parsed structure used by
    normal /section and /node pages. This lets raw-node classes reuse
    CommonLayouts/NodeInput rendering where possible.
    """
    parsed = dict(base_parsed or {})
    cfg = dict((parsed.get("cfg") or {}) if isinstance(parsed.get("cfg"), dict) else {})
    payload = payload if isinstance(payload, dict) else {}

    for key in ("config", "_config", "configuration", "cfg"):
        value = payload.get(key)
        if isinstance(value, dict):
            # Embedded config may contain CommonLayouts or other class-level
            # references; prefer it only for keys that are absent in the repo cfg.
            for k, v in value.items():
                cfg.setdefault(k, v)

    if isinstance(class_obj, dict):
        for key in ("CommonLayouts", "common_layouts"):
            value = class_obj.get(key)
            if isinstance(value, list) and value and "CommonLayouts" not in cfg:
                cfg["CommonLayouts"] = value

    classes = dict((parsed.get("classes") or {}) if isinstance(parsed.get("classes"), dict) else {})
    if class_name and isinstance(class_obj, dict) and class_obj:
        classes[class_name] = class_obj

    parsed["cfg"] = cfg
    parsed["classes"] = classes
    parsed.setdefault("sections", [])
    parsed.setdefault("classes_by_section", {})
    parsed.setdefault("rooms", {})
    return parsed


def _resolve_raw_node_class(payload: Dict[str, Any], class_name: str, preferred_repo=None):
    """Resolve class for a raw-node.

    Returns (repo, parsed, class_obj). If payload carries embedded class JSON,
    that JSON takes precedence; otherwise class_name is resolved from the
    user's configured repositories.
    """
    embedded_class = _extract_raw_node_class_json(payload)
    embedded_name = _class_name_from_embedded_class(embedded_class, fallback=class_name, payload=payload) if embedded_class else ""
    effective_name = embedded_name or str(class_name or "").strip()

    repo = None
    parsed = None

    if embedded_class:
        repo, parsed, _repo_cls = _find_repo_for_raw_node(effective_name, payload)
        if repo is None and preferred_repo is not None:
            repo = preferred_repo
            parsed = get_parsed_config(repo, models.db) or {}
        if repo is None:
            repos = models.Repo.query.filter_by(user_id=current_user.id).order_by(models.Repo.id.asc()).all()
            repo = repos[0] if repos else None
            parsed = get_parsed_config(repo, models.db) if repo else {}
        parsed = _merge_raw_class_into_parsed(parsed or {}, effective_name, embedded_class, payload=payload)
        return repo, parsed, embedded_class

    repo, parsed, cls = _find_repo_for_raw_node(effective_name, payload)
    if repo is None and preferred_repo is not None:
        repo = preferred_repo
        parsed = get_parsed_config(repo, models.db) or {}
        cls = ((parsed or {}).get("classes") or {}).get(effective_name) or cls
    return repo, parsed, cls or {}


def _raw_node_search_text(obj, payload: Dict[str, Any], class_name: str, node_id: str, data: Dict[str, Any]) -> str:
    parts = [
        str(getattr(obj, "node_id", "") or ""),
        str(node_id or ""),
        str(class_name or ""),
        str(getattr(obj, "content_type", "") or ""),
    ]
    try:
        parts.append(json.dumps(data, ensure_ascii=False, default=str))
    except Exception:
        parts.append(str(data))
    try:
        parts.append(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception:
        parts.append(str(payload))
    return "\n".join(parts).lower()


_RAW_NODE_URL_RE = re.compile(r"(?:^|[\s\"'(<])(?:https?://[^\s\"'<>]+)?/(?:api/)?raw-node/([^\s\"'<>?#]+)", re.UNICODE)


def _extract_raw_node_id_from_url(value: str) -> str:
    """Extract the DB RawNode.node_id from absolute/relative raw-node links."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    for marker in ("/api/raw-node/", "api/raw-node/", "/raw-node/", "raw-node/"):
        if marker in raw:
            tail = raw.rsplit(marker, 1)[-1]
            return tail.split("?", 1)[0].split("#", 1)[0].strip().strip('"\'<>')
    return ""


def _extract_raw_node_ids_from_message_payload(value, *, deep: bool = False, _depth: int = 0) -> set:
    """Extract raw-node ids from known node-message shapes.

    Keep the default path intentionally shallow. The previous recursive scanner
    walked arbitrary JSON blobs for every request; on production message tables
    that can pin a CPU core. Received Nodes only needs the fields that the
    server/mobile clients actually use: type=node + node_id/node_uid and the
    raw-node URL fields.
    """
    ids = set()

    def add(item):
        item = str(item or "").strip()
        if item:
            ids.add(item)

    if value is None:
        return ids

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return ids
        add(_extract_raw_node_id_from_url(raw))
        for match in _RAW_NODE_URL_RE.finditer(" " + raw):
            add(match.group(1))
        if deep and _depth < 2 and raw[:1] in ("{", "["):
            try:
                ids.update(_extract_raw_node_ids_from_message_payload(json.loads(raw), deep=False, _depth=_depth + 1))
            except Exception:
                pass
        return {x for x in ids if x}

    if isinstance(value, (list, tuple)):
        for item in list(value)[:50]:
            ids.update(_extract_raw_node_ids_from_message_payload(item, deep=False, _depth=_depth + 1))
        return {x for x in ids if x}

    if not isinstance(value, dict):
        return ids

    obj = value
    for key in ("download_url", "_download_url", "raw_node_url", "node_url", "thread_ref"):
        add(_extract_raw_node_id_from_url(obj.get(key)))

    ptype = str(obj.get("type") or obj.get("message_type") or "").strip().lower()
    has_raw_url = any(_extract_raw_node_id_from_url(obj.get(k)) for k in ("download_url", "_download_url", "raw_node_url", "node_url", "thread_ref"))
    if ptype in {"node", "node_download", "raw_node", "raw-node", "node_message"} or has_raw_url:
        for key in ("node_id", "node_uid", "raw_node_id", "_id"):
            add(obj.get(key))

    # Common one-level wrappers only. Do not recurse over every payload value.
    for key in ("data", "payload", "node"):
        nested = obj.get(key)
        if isinstance(nested, dict):
            ids.update(_extract_raw_node_ids_from_message_payload(nested, deep=False, _depth=_depth + 1))

    # Batch payloads are explicit and bounded.
    for key in ("items", "nodes"):
        nested = obj.get(key)
        if isinstance(nested, list):
            for item in nested[:50]:
                ids.update(_extract_raw_node_ids_from_message_payload(item, deep=False, _depth=_depth + 1))

    for key in ("items_json", "nodes_json", "payload_json", "data_json"):
        raw = obj.get(key)
        if isinstance(raw, str) and raw.strip()[:1] in ("{", "["):
            try:
                ids.update(_extract_raw_node_ids_from_message_payload(json.loads(raw), deep=False, _depth=_depth + 1))
            except Exception:
                pass

    return {x for x in ids if x}


def _dt_sort_value(value) -> float:
    if value is None:
        return 0.0
    try:
        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return float(value.timestamp())
    except Exception:
        try:
            return float(datetime.fromisoformat(str(value)).timestamp())
        except Exception:
            return 0.0


def _current_user_cache_key():
    try:
        return getattr(current_user, "id", None) or getattr(current_user, "email", None) or id(current_user)
    except Exception:
        return None


def _current_user_keys() -> List[str]:
    """Return message aliases known for the logged-in web/API user.

    This is intentionally cached for the duration of rendering because Received
    Nodes calls it from several helpers. It also includes device_uid values so
    direct device-targeted Android deliveries can be matched by indexed
    outgoing_message_log.target_id instead of scanning JSON payloads globally.
    """
    cache_key = _current_user_cache_key()
    if cache_key in _CURRENT_USER_KEYS_CACHE:
        return list(_CURRENT_USER_KEYS_CACHE.get(cache_key) or [])

    keys = []

    def add(value):
        value = str(value or "").strip()
        if value:
            keys.append(value)

    try:
        add(getattr(current_user, "email", ""))
    except Exception:
        pass
    try:
        add(getattr(current_user, "id", ""))
    except Exception:
        pass
    try:
        add(getattr(current_user, "config_display_name", ""))
    except Exception:
        pass

    user_id = None
    try:
        user_id = getattr(current_user, "id", None)
    except Exception:
        user_id = None

    RoomDevice = _server_model("RoomDevice")
    UserDevice = _server_model("UserDevice")
    OutgoingMessageDeviceAck = _server_model("OutgoingMessageDeviceAck")
    device_uids = set()

    if user_id is not None:
        try:
            if RoomDevice is not None:
                for rd in RoomDevice.query.filter_by(user_id=user_id).all():
                    add(getattr(rd, "user_key", ""))
                    du = str(getattr(rd, "device_uid", "") or "").strip()
                    if du:
                        device_uids.add(du)
                        add(du)
                    extra = getattr(rd, "extra_json", None)
                    if isinstance(extra, dict):
                        for k in ("user_key", "target_user", "recipient", "to_user"):
                            add(extra.get(k))
        except Exception:
            pass
        try:
            if UserDevice is not None:
                for ud in UserDevice.query.filter_by(user_id=user_id).all():
                    du = str(getattr(ud, "device_uid", "") or "").strip()
                    if du:
                        device_uids.add(du)
                        add(du)
                    add(getattr(ud, "android_id", ""))
                    extra = getattr(ud, "extra_json", None)
                    if isinstance(extra, dict):
                        for k in ("user_key", "target_user", "recipient", "to_user"):
                            add(extra.get(k))
        except Exception:
            pass

    # Keep this bounded and only once per request/user. It is a compatibility
    # fallback for old Android rows, not the primary lookup path.
    if device_uids and OutgoingMessageDeviceAck is not None:
        try:
            for ack in OutgoingMessageDeviceAck.query.filter(OutgoingMessageDeviceAck.device_uid.in_(list(device_uids))).order_by(OutgoingMessageDeviceAck.id.desc()).limit(200).all():
                add(getattr(ack, "user_key", ""))
                add(getattr(ack, "ack_by", ""))
                ack_payload = getattr(ack, "ack_payload", None)
                if isinstance(ack_payload, dict):
                    for k in ("user_key", "ack_user", "target_user", "recipient", "to_user"):
                        add(ack_payload.get(k))
        except Exception:
            pass

    seen = set()
    out = []
    for key in keys:
        low = str(key or "").strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        out.append(str(key).strip())

    _CURRENT_USER_KEYS_CACHE[cache_key] = list(out)
    return out


def _current_user_group_ids(user_keys: Optional[List[str]] = None) -> set:
    MessageGroupMember = _server_model("MessageGroupMember")
    keys = user_keys if user_keys is not None else _current_user_keys()
    lows_tuple = tuple(sorted({str(k or "").strip().lower() for k in keys if str(k or "").strip()}))
    cache_key = (_current_user_cache_key(), lows_tuple)
    if cache_key in _CURRENT_USER_GROUP_IDS_CACHE:
        return set(_CURRENT_USER_GROUP_IDS_CACHE.get(cache_key) or set())
    if MessageGroupMember is None or not lows_tuple:
        return set()
    try:
        rows = MessageGroupMember.query.filter(func.lower(MessageGroupMember.user_key).in_(list(lows_tuple))).all()
        group_ids = {str(r.group_id or "").strip() for r in rows if str(r.group_id or "").strip()}
    except Exception:
        try:
            rows = MessageGroupMember.query.all()
            lows = set(lows_tuple)
            group_ids = {
                str(r.group_id or "").strip()
                for r in rows
                if str(getattr(r, "user_key", "") or "").strip().lower() in lows and str(r.group_id or "").strip()
            }
        except Exception:
            group_ids = set()
    _CURRENT_USER_GROUP_IDS_CACHE[cache_key] = set(group_ids)
    return group_ids


def _raw_node_payload_has_current_user_hint(payload: Dict[str, Any], include_sender: bool = True, *, user_keys: Optional[List[str]] = None, group_ids: Optional[set] = None) -> bool:
    payload = payload if isinstance(payload, dict) else {}
    keys = user_keys if user_keys is not None else _current_user_keys()
    lows = {k.lower() for k in keys}
    groups = group_ids if group_ids is not None else _current_user_group_ids(keys)
    if not lows:
        return False

    hints = payload.get("_node_message_targets")
    if isinstance(hints, list):
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            target_type = str(hint.get("target_type") or "").strip().lower()
            target_id = str(hint.get("target_id") or hint.get("user_key") or hint.get("group_id") or "").strip()
            if target_type == "user" and target_id.lower() in lows:
                return True
            if target_type == "group" and target_id in groups:
                return True
            if include_sender and str(hint.get("sender_user") or "").strip().lower() in lows:
                return True

    if include_sender and str(payload.get("sender_user") or "").strip().lower() in lows:
        return True

    for key in ("target_user", "user_key", "target_key", "target_id", "recipient", "recipient_user", "to", "to_user", "peer", "peer_user", "receiver"):
        value = str(payload.get(key) or "").strip()
        if value and value.lower() in lows:
            return True

    group_id = str(payload.get("group_id") or payload.get("discussion_group_id") or "").strip()
    if group_id and group_id in groups:
        return True

    # Only known wrappers, not arbitrary recursion.
    for key in ("data", "_data", "payload", "node"):
        nested = payload.get(key)
        if isinstance(nested, dict) and _raw_node_payload_has_current_user_hint(nested, include_sender=include_sender, user_keys=keys, group_ids=groups):
            return True

    return False


def _message_row_visible_to_current_user(row, *, user_keys: Optional[List[str]] = None, group_ids: Optional[set] = None) -> bool:
    keys = user_keys if user_keys is not None else _current_user_keys()
    lows = {k.lower() for k in keys}
    if not lows:
        return False
    groups = group_ids if group_ids is not None else _current_user_group_ids(keys)
    payload = getattr(row, "payload_json", None)
    payload = payload if isinstance(payload, dict) else {}

    sender = str(getattr(row, "sender_user", "") or payload.get("sender_user") or "").strip().lower()
    if sender and sender in lows:
        return True

    target_type = str(getattr(row, "target_type", "") or "").strip().lower()
    target_id = str(getattr(row, "target_id", "") or "").strip()
    if target_type in {"user", "device"} and target_id.lower() in lows:
        return True
    if target_type == "group" and target_id in groups:
        return True

    for key in ("user_key", "target_key", "target_user", "target_id", "recipient", "recipient_user", "to", "to_user", "peer", "peer_user", "receiver"):
        value = str(payload.get(key) or "").strip().lower()
        if value and value in lows:
            return True
    group_id = str(payload.get("group_id") or payload.get("discussion_group_id") or "").strip()
    if group_id and group_id in groups:
        return True
    return False


def _query_current_user_message_rows(limit: int = 1000):
    """Return only rows that are plausibly related to current user, using DB indexes.

    No network calls and no full-table JSON scan. A small recent fallback catches
    older direct-device payloads where only payload.user_key carries the user.
    """
    OutgoingMessageLog = _server_model("OutgoingMessageLog")
    if OutgoingMessageLog is None:
        return []

    keys = _current_user_keys()
    groups = _current_user_group_ids(keys)
    clauses = []
    if keys:
        clauses.append(OutgoingMessageLog.sender_user.in_(keys))
        clauses.append(and_(OutgoingMessageLog.target_type.in_(("user", "device")), OutgoingMessageLog.target_id.in_(keys)))
    if groups:
        clauses.append(and_(OutgoingMessageLog.target_type == "group", OutgoingMessageLog.target_id.in_(list(groups))))

    rows_by_id = {}
    try:
        if clauses:
            rows = OutgoingMessageLog.query.filter(or_(*clauses)).order_by(
                OutgoingMessageLog.id.desc(),
            ).limit(limit).all()
            for row in rows:
                rows_by_id[getattr(row, "id", id(row))] = row
    except Exception:
        pass

    # Bounded compatibility fallback: catches Android rows whose route target
    # is not one of the web user's aliases but payload.user_key/peer_user is.
    # Order by primary key only; created_at is not guaranteed to be indexed.
    try:
        fallback_limit = min(1000, max(100, int(limit)))
        rows = OutgoingMessageLog.query.order_by(
            OutgoingMessageLog.id.desc(),
        ).limit(fallback_limit).all()
        for row in rows:
            if _message_row_visible_to_current_user(row, user_keys=keys, group_ids=groups):
                rows_by_id[getattr(row, "id", id(row))] = row
    except Exception:
        pass

    out = list(rows_by_id.values())
    out.sort(key=lambda r: (_dt_sort_value(getattr(r, "created_at", None)), getattr(r, "id", 0) or 0), reverse=True)
    return out[:limit]


def _raw_node_ids_from_message_history() -> set:
    """Raw node ids delivered to/currently sent by the current user.

    This is based on indexed OutgoingMessageLog columns plus a tiny recent
    fallback. It never downloads /api/raw-node URLs and never loops over RawNode.
    """
    ids = []
    seen = set()
    for row in _query_current_user_message_rows(limit=1000):
        payload = getattr(row, "payload_json", None)
        for raw_id in _extract_raw_node_ids_from_message_payload(payload):
            if raw_id and raw_id not in seen:
                seen.add(raw_id)
                ids.append(raw_id)
    return set(ids)


def _current_user_can_access_raw_node(raw_node_id: str, obj=None, include_sender: bool = True) -> bool:
    raw_node_id = str(raw_node_id or "").strip()
    if not raw_node_id or not getattr(current_user, "is_authenticated", False):
        return False

    RawNode = _server_model("RawNode")
    if obj is None and RawNode is not None:
        try:
            obj = RawNode.query.filter_by(node_id=raw_node_id).first()
        except Exception:
            obj = None

    try:
        if obj is not None and getattr(obj, "owner_user_id", None) == getattr(current_user, "id", None):
            return True
    except Exception:
        pass

    keys = _current_user_keys()
    groups = _current_user_group_ids(keys)
    payload = _raw_node_payload(obj) if obj is not None else {}
    if _raw_node_payload_has_current_user_hint(payload, include_sender=include_sender, user_keys=keys, group_ids=groups):
        return True

    return raw_node_id in _raw_node_ids_from_message_history()


def _message_dict_visible_to_current_user(msg: Dict[str, Any]) -> bool:
    msg = msg if isinstance(msg, dict) else {}
    keys = _current_user_keys()
    lows = {k.lower() for k in keys}
    if not lows:
        return False
    group_ids = _current_user_group_ids(keys)

    data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
    sender = str(msg.get("sender_user") or data.get("sender_user") or "").strip().lower()
    if sender and sender in lows:
        return True
    target_type = str(msg.get("target_type") or data.get("target_type") or "").strip().lower()
    target_id = str(msg.get("target_id") or data.get("target_id") or "").strip()
    if target_type in {"user", "device"} and target_id.lower() in lows:
        return True
    if target_type == "group" and target_id in group_ids:
        return True
    group_id = str(msg.get("group_id") or data.get("group_id") or "").strip()
    if group_id and group_id in group_ids:
        return True
    user_key = str(msg.get("user_key") or data.get("user_key") or "").strip().lower()
    if user_key and user_key in lows:
        return True
    return False


def _current_user_can_access_node_discussion(node_id: str) -> bool:
    node_id = str(node_id or "").strip()
    if not node_id:
        return False
    if _current_user_can_access_raw_node(node_id):
        return True
    NodeDiscussionMessage = _server_model("NodeDiscussionMessage")
    if NodeDiscussionMessage is not None:
        try:
            keys = _current_user_keys()
            groups = _current_user_group_ids(keys)
            rows = NodeDiscussionMessage.query.filter_by(node_id=node_id).order_by(NodeDiscussionMessage.id.desc()).limit(200).all()
            if any(_message_row_visible_to_current_user(r, user_keys=keys, group_ids=groups) for r in rows):
                return True
        except Exception:
            pass
    return False

def _find_repo_for_raw_node(class_name: str, payload: Optional[Dict[str, Any]] = None):
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    if not repos:
        return None, None, None

    payload = payload if isinstance(payload, dict) else {}
    class_obj = payload.get("_class")
    possible_cfg_uids = []
    if isinstance(class_obj, dict):
        for key in ("config_uid", "configuration_uid", "config", "repo_uid"):
            val = str(class_obj.get(key) or "").strip()
            if val:
                possible_cfg_uids.append(val)

    def repo_score(repo):
        score = 0
        if possible_cfg_uids and str(repo.config_uid or "") in possible_cfg_uids:
            score += 10
        return score

    candidates = sorted(repos, key=repo_score, reverse=True)
    first_parsed = None
    first_repo = candidates[0] if candidates else None
    for repo in candidates:
        parsed = get_parsed_config(repo, models.db)
        if first_parsed is None:
            first_parsed = parsed
        cls = ((parsed or {}).get("classes") or {}).get(class_name) if class_name else None
        if cls:
            return repo, parsed, cls
    return first_repo, first_parsed, None


def _build_raw_node_items(q: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build Received Nodes directly from local DB rows.

    Important: do not call /api/raw-node URLs from the web server. Those links
    are hosted by this same Flask app, and the source of truth is RawNode. The
    list is scoped by OutgoingMessageLog/RawNode metadata, then RawNode rows are
    fetched by node_id with indexed queries.
    """
    RawNode = _server_model("RawNode")
    if RawNode is None:
        return [], {"classes_ui": [], "table_headers": ["Created", "Updated"], "filter_indexes": []}

    delivered_ids = _raw_node_ids_from_message_history()
    user_id = getattr(current_user, "id", None)
    rows_by_id = {}

    # Nodes received/sent through messages: fetch by indexed RawNode.node_id.
    if delivered_ids:
        try:
            for obj in RawNode.query.filter(RawNode.node_id.in_(list(delivered_ids))).all():
                raw_id = str(getattr(obj, "node_id", "") or "").strip()
                if raw_id:
                    rows_by_id[raw_id] = obj
        except Exception:
            pass

    # Nodes uploaded by this web/API user: indexed owner_user_id.
    if user_id is not None:
        try:
            q_owner = RawNode.query.filter_by(owner_user_id=user_id)
            try:
                owner_rows = q_owner.order_by(RawNode.updated_at.desc(), RawNode.created_at.desc(), RawNode.id.desc()).limit(500).all()
            except Exception:
                owner_rows = q_owner.order_by(RawNode.id.desc()).limit(500).all()
            for obj in owner_rows:
                raw_id = str(getattr(obj, "node_id", "") or "").strip()
                if raw_id:
                    rows_by_id[raw_id] = obj
        except Exception:
            pass

    rows = list(rows_by_id.values())
    rows.sort(
        key=lambda obj: (
            _dt_sort_value(getattr(obj, "updated_at", None) or getattr(obj, "created_at", None)),
            getattr(obj, "id", 0) or 0,
        ),
        reverse=True,
    )
    rows = rows[:500]

    # Resolve repositories/classes once as much as possible.
    items = []
    q_low = str(q or "").strip().lower()
    repo_cache: Dict[str, Tuple[Any, Any, Any]] = {}

    for obj in rows:
        raw_id = str(getattr(obj, "node_id", "") or "").strip()
        if not raw_id:
            continue

        payload = _raw_node_payload(obj)
        class_name, payload_node_id, data = _raw_node_identity(payload, raw_id)
        node_id = payload_node_id or raw_id
        data.setdefault("_id", node_id)
        if class_name:
            data.setdefault("_class", class_name)
        data.setdefault("_raw_node_id", raw_id)
        data.setdefault("_download_url", _raw_node_download_ref(payload, raw_id))

        if q_low and q_low not in _raw_node_search_text(obj, payload, class_name, node_id, data):
            continue

        embedded_key = "embedded:" + str(id(payload.get("_class"))) if isinstance(payload.get("_class"), dict) else ""
        cache_key = embedded_key or class_name or "raw-node"
        if cache_key in repo_cache:
            repo, parsed, cls = repo_cache[cache_key]
        else:
            repo, parsed, cls = _resolve_raw_node_class(payload, class_name)
            repo_cache[cache_key] = (repo, parsed, cls)

        if isinstance(cls, dict):
            resolved_name = _class_name_from_embedded_class(cls, fallback=class_name, payload=payload)
            if resolved_name:
                class_name = resolved_name
                data.setdefault("_class", class_name)
        repo_id = getattr(repo, "id", 0) or 0
        repo_name = getattr(repo, "display_name", "") or getattr(repo, "name", "") or RAW_NODES_SECTION_NAME
        display_image_html = ""
        tv = {
            "Created": getattr(obj, "created_at", None).isoformat() if getattr(obj, "created_at", None) else "",
            "Updated": getattr(obj, "updated_at", None).isoformat() if getattr(obj, "updated_at", None) else "",
        }
        try:
            if repo and parsed and cls:
                cover_layout = cls.get("cover_image")
                cover_web_layout = cls.get("display_image_web") or ""
                layout_to_use = cover_web_layout if str(cover_web_layout or "").strip() else cover_layout
                if layout_to_use is not None:
                    layout_to_use = resolve_common_layout(parsed, layout_to_use)
                    _fill_nodeinput_views(repo, parsed, layout_to_use, data)
                    display_image_html = _cover_with_tags(_wrap_client_tpl_html(str(render_nodalayout_html(
                        layout_to_use,
                        data,
                        assets_base_dir=_userfiles_dir_for_repo(repo),
                        context=_nl_context(repo, class_name=class_name, node_id=node_id),
                    ) or ""), data), data, bool((cls or {}).get("show_tag_cloud")))
        except Exception:
            display_image_html = ""
        if not display_image_html:
            display_image_html = _render_tags_html(data)

        items.append({
            "repo": repo_name,
            "repo_id": repo_id,
            "class": class_name or "raw-node",
            "id": node_id,
            "raw_node_id": raw_id,
            "data": data,
            "class_obj": cls or {},
            "is_raw_node": True,
            "is_custom_process": True,
            "display_image_html": display_image_html,
            "table_values": tv,
            "use_standard_commands": False,
            "repo_uid": getattr(repo, "config_uid", "") or "",
        })

    return items, {
        "classes_ui": [],
        "table_headers": ["Created", "Updated"],
        "start_menu_cmds_ui": [],
        "filter_indexes": [],
    }


_CLASS_VIEW_RE = re.compile(r"\{([\w.]+)\}", re.UNICODE)

def _render_class_record_view(parsed: Optional[Dict[str, Any]], class_name: str, node_id: str, data: Optional[Dict[str, Any]]) -> str:
    """Render class-level record view template using node data."""
    data = data if isinstance(data, dict) else {}

    if isinstance(data.get("_view"), str) and data.get("_view", "").strip():
        return data.get("_view", "").strip()

    cls_cfg: Dict[str, Any] = {}
    try:
        cls_cfg = ((parsed or {}).get("classes") or {}).get(class_name) or {}
    except Exception:
        cls_cfg = {}

    tpl = str(cls_cfg.get("record_view") or "").strip()
    if tpl:
        def repl(m: re.Match) -> str:
            key = m.group(1)
            val = data.get(key)
            return "" if val is None else str(val)

        rendered = _CLASS_VIEW_RE.sub(repl, tpl).strip()
        if rendered:
            return rendered

    return str(node_id or "")

# in-memory parsed config cache (per repo)
CONFIG_MEM: Dict[int, Dict[str, Any]] = {}
# in-memory cache for exec()'ed server handlers modules (per config_uid)
SERVER_HANDLERS_MEM: Dict[str, Dict[str, Any]] = {}
_SERVER_HANDLERS_NS_MEM: Dict[str, Dict[str, Any]] = {}
_SERVER_NODE_CLASS_MEM: Dict[Tuple[str, str, str], Any] = {}


# -------- client settings (stored in client.sqlite) --------

def _split_dataset_item_uid(ds_name: str, item_uid: str) -> Tuple[str, str]:
    """Return (dataset_name, item_id) from DatasetLink value.

    Dataset links are self-describing: ``Goods$123`` already contains the
    dataset name, so cover layouts do not need an additional ``dataset`` field.
    If ``ds_name`` is explicitly provided, it is kept as fallback/override for
    old layouts that store only ``123``.
    """
    ds = str(ds_name or "").strip()
    uid = str(item_uid or "").strip()
    if "$" in uid:
        left, right = uid.split("$", 1)
        if not ds:
            ds = left.strip()
        return ds, right.strip()
    return ds, uid


def _render_template_fields(template: str, data: Dict[str, Any]) -> str:
    """Small {field} renderer for dataset/record views."""
    template = str(template or "")
    if not template:
        return ""

    def repl(match: re.Match) -> str:
        field_name = match.group(1)
        value = data.get(field_name, "")
        return str(value) if value is not None else ""

    return _CLASS_VIEW_RE.sub(repl, template).strip()


def _get_dataset_item_direct(config_uid: str, ds_name: str, item_id: str) -> Optional[Dict[str, Any]]:
    """Directly get dataset item from database without HTTP."""
    try:
        ds_name, item_id = _split_dataset_item_uid(ds_name, item_id)
        if not ds_name or not item_id:
            return None

        Configuration = main.Configuration
        Dataset = main.Dataset
        DatasetItem = main.DatasetItem

        cfg = Configuration.query.filter_by(uid=config_uid).first() if config_uid else None
        ds = None

        # Сначала ищем в текущей конфигурации репозитория.
        if cfg:
            ds = Dataset.query.filter_by(config_id=cfg.id, name=ds_name).first()

        # В ссылке DatasetLink/DatasetInput хранится только Dataset$Id, без config_uid.
        # Поэтому, если dataset не найден в текущей конфе, пробуем найти такой dataset
        # в конфигурациях репозиториев текущего пользователя. Это особенно важно для
        # node_form и динамических layout-ов, где layout может прийти из другого контекста.
        if not ds:
            try:
                repos = models.Repo.query.filter_by(user_id=current_user.id).all() if current_user.is_authenticated else []
                for r in repos:
                    cfg_uid = str(getattr(r, "config_uid", "") or "").strip()
                    if not cfg_uid:
                        continue
                    c2 = Configuration.query.filter_by(uid=cfg_uid).first()
                    if not c2:
                        continue
                    ds2 = Dataset.query.filter_by(config_id=c2.id, name=ds_name).first()
                    if ds2:
                        cfg = c2
                        ds = ds2
                        break
            except Exception:
                pass

        if not ds:
            return None

        item = DatasetItem.query.filter_by(dataset_id=ds.id, item_id=item_id).first()
        if not item:
            return None

        data = item.data or {}
        if not isinstance(data, dict):
            data = {}

        view = str(data.get("_view") or "").strip()
        if not view:
            view = _render_template_fields(ds.view_template or "", data)
        if not view:
            view = str(data.get("title") or data.get("name") or item_id)

        return {
            "_id": item_id,
            "_view": view,
            "_data": data,
            "_dataset": ds_name,
        }
    except Exception as e:
        print(f"Error getting dataset item: {e}")
        return None

def _get_setting(key: str, default: str = "") -> str:
    """Get per-user client setting."""
    if not current_user.is_authenticated:
        return default
    row = models.ClientSetting.query.filter_by(user_id=current_user.id, key=key).first()
    if not row:
        return default
    return (row.value or "")


def _set_setting(key: str, value: str) -> None:
    if not current_user.is_authenticated:
        return
    row = models.ClientSetting.query.filter_by(user_id=current_user.id, key=key).first()
    if not row:
        row = models.ClientSetting(user_id=current_user.id, key=key, value=value or "")
        models.db.session.add(row)
    else:
        row.value = value or ""
    models.db.session.commit()


def _get_repo_by_config_uid_or_404(config_uid: str) -> models.Repo:
    config_uid = (config_uid or "").strip()
    repo = models.Repo.query.filter_by(config_uid=config_uid, user_id=current_user.id).first()
    if not repo:
        abort(404)
    return repo

def _exec_node_class(config: Any, class_name: str):

    ns = {}
    exec(config.nodes_server_handlers, ns)
    cls = ns.get(class_name)
    if not cls:
        raise RuntimeError(f"Node class '{class_name}' not found in handlers")
    return cls


def _is_local_repo(repo: models.Repo) -> bool:
    if not repo.base_url:
        return True
    return repo.base_url.rstrip("/") == request.host_url.rstrip("/")



# -------- NodaLayout context helpers (NodeChildren / node covers) --------

def _pick_node_title(data: Dict[str, Any]) -> str:
    """Best-effort human title for a node cover."""
    for k in ("title", "name", "caption", "label", "number", "code"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _fetch_node_data_for_repo(repo: models.Repo, class_name: str, node_id: str) -> Dict[str, Any]:
    """Get _data for a node from local storage or remote base_url."""
    cfg_uid = repo.config_uid
    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    if not base_url or base_url == current:
        try:
            return _node_local_get_data(cfg_uid, class_name, node_id) or {}
        except Exception:
            return {}
    try:
        payload = _api_get_remote(repo, f"/api/config/{cfg_uid}/node/{class_name}/{node_id}")
        return (payload or {}).get("_data") or {}
    except Exception:
        return {}

def _wrap_client_tpl_html(html: str, data: dict) -> str:
    html = str(html or "").strip()
    if not html:
        return ""
    if 'data-nl-tpl-' not in html:
        return html
    try:
        payload = escape(json.dumps(data or {}, ensure_ascii=False), quote=True)
        return f'<div class="nl-cover-runtime" data-nl-cover-data="{payload}">{html}</div>'
    except Exception:
        return html

def _node_cover_html(repo: models.Repo, class_name: str, node_id: str, mode: str = "") -> str:

    # NOTE: `mode` is kept for backward/forward compatibility.
    # Some callers (e.g. Table rows) may pass mode="table" to request a more
    # compact look in the future. Currently we render the same cover.
    data = _fetch_node_data_for_repo(repo, class_name, node_id)
    assets_base_dir = _userfiles_dir_for_repo(repo)
    parsed = get_parsed_config(repo, models.db) or {}
    cls_cfg_for_tags = ((parsed.get("classes") or {}).get(class_name) or {}) if isinstance(parsed, dict) else {}
    show_tags = bool(cls_cfg_for_tags.get("show_tag_cloud"))
    nl_context = _nl_context(repo, class_name=class_name, node_id=node_id)

    try:
        cov = data.get("_cover") if isinstance(data, dict) else None
        if cov:
            if isinstance(cov, (dict, list)):
                html = str(render_nodalayout_html(cov, data, assets_base_dir=assets_base_dir, context=nl_context) or "").strip()
                if html:
                    return _cover_with_tags(_wrap_client_tpl_html(html, data), data, show_tags)
            elif isinstance(cov, str):
                s = cov.strip()
                # json layout as string
                if (s.startswith("[") or s.startswith("{")):
                    html = str(render_nodalayout_html(s, data, assets_base_dir=assets_base_dir, context=nl_context) or "").strip()
                    if html:
                        return _cover_with_tags(_wrap_client_tpl_html(html, data), data, show_tags)
                # plain image src
                pic_layout = [[{"type": "Picture", "value": s, "width": -1}]]
                html = str(render_nodalayout_html(pic_layout, data, assets_base_dir=assets_base_dir, context=nl_context) or "").strip()
                if html:
                    return _cover_with_tags(_wrap_client_tpl_html(html, data), data, show_tags)
    except Exception:
        pass

    
    try:
        cls = (parsed.get("classes") or {}).get(class_name) or {}

        cover_web_layout = (cls.get("display_image_web") or "").strip()
        cover_layout = cls.get("cover_image")  # может быть dict layout

        layout_to_use = None
        if cover_web_layout:
            layout_to_use = cover_web_layout
        elif cover_layout:
            layout_to_use = cover_layout

        if layout_to_use:
            _fill_nodeinput_views(repo, parsed, layout_to_use, data)
            html = str(render_nodalayout_html(layout_to_use, data, assets_base_dir=assets_base_dir, context=nl_context) or "").strip()
            if html:
                return _cover_with_tags(_wrap_client_tpl_html(html, data), data, show_tags)
    except Exception:
        pass

    
    title = _pick_node_title(data)
    subtitle = f"{class_name}/{node_id}"

    if title:
        return _cover_with_tags((
            f'<div class="card"><div class="card-body p-2">'
            f'<div class="fw-semibold">{escape(title)}</div>'
            f'<div class="text-muted small">{escape(subtitle)}</div>'
            f'</div></div>'
        ), data, show_tags)
    return _cover_with_tags((
        f'<div class="card"><div class="card-body p-2">'
        f'<div class="fw-semibold">{escape(subtitle)}</div>'
        f'</div></div>'
    ), data, show_tags)


def _node_children_tree(repo: models.Repo, class_name: str, node_id: str) -> List[Dict[str, Any]]:
    """Build recursive tree for NodeChildren renderer. Supports both old and new formats."""
    visited: set[tuple[str, str]] = set()
    
    def _parse_uid(s: str):
        s = str(s or "").strip()
        if "$" in s:
            parts = s.split("$")
            if len(parts) >= 3:
                # cfg$Class$Id
                return parts[-2], parts[-1]
            if len(parts) == 2:
                # Class$Id
                return parts[0], parts[1]
        return "", s
    
    def _get_children_from_node_data(data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract children from node data in both formats"""
        children_data = data.get("_children") or []
        result = []
        
        # New format (dict)
        if isinstance(children_data, dict):
            for key, value in children_data.items():
                # key: "ClassName$nodeId", value: "config_uid$ClassName$nodeId"
                config_uid, child_class, child_id = None, None, None
                
                # Try to parse from key
                key_parts = key.split("$")
                if len(key_parts) == 2:
                    child_class_name, child_id = key_parts[0], key_parts[1]
                elif len(key_parts) == 3:
                    child_class_name, child_id = key_parts[1], key_parts[2]
                
                # If not successful, try from value
                if not child_class or not child_id:
                    value_parts = value.split("$")
                    if len(value_parts) >= 3:
                        child_class, child_id = value_parts[-2], value_parts[-1]
                
                if child_class and child_id:
                    result.append({
                        "class": child_class,
                        "id": child_id,
                        "uid": value
                    })
        
        # Old format (list)
        elif isinstance(children_data, list):
            for child in children_data:
                if isinstance(child, dict):
                    child_class = child.get("class") or child.get("_class")
                    child_id = child.get("id") or child.get("_id")
                    if child_class and child_id:
                        result.append({
                            "class": child_class,
                            "id": child_id,
                            "uid": child.get("uid")
                        })
        
        return result
    
    def build(cn: str, nid: str) -> List[Dict[str, Any]]:
        key = (cn, nid)
        if key in visited:
            return []
        visited.add(key)
        
        data = _fetch_node_data_for_repo(repo, cn, nid)
        children_list = _get_children_from_node_data(data)
        out: List[Dict[str, Any]] = []
        
        for child in children_list:
            cc = str(child.get("class") or "").strip()
            ci = str(child.get("id") or "").strip()
            
            if not cc or not ci:
                # Try to parse from uid
                uid = child.get("uid")
                if uid:
                    cc2, ci2 = _parse_uid(uid)
                    cc, ci = cc2, ci2
            
            if not cc or not ci:
                continue
            
            out.append({
                "class": cc,
                "id": ci,
                "cover_html": _node_cover_html(repo, cc, ci),
                "open_url": url_for("client.node_form_redirect", repo_id=repo.id, class_name=cc, node_id=ci),
                "children": build(cc, ci),
            })
        
        return out
    
    return build(class_name, node_id)

def _walk_layout_find_link_elements(layout_obj):
    """Yield link/input elements from layout (2d/1d/json str)."""
    import json
    if layout_obj is None:
        return
    if isinstance(layout_obj, str):
        try:
            layout_obj = json.loads(layout_obj)
        except Exception:
            return

    def walk(x):
        if isinstance(x, dict):
            t = x.get("type") or x.get("t")
            if t in ("NodeInput", "NodeLink", "DatasetInput", "DatasetField", "DatasetLink", "DataSetLink"):
                yield x
            # walk common nested places
            for k in ("layout", "tabs", "rows", "cols", "items", "children"):
                v = x.get(k)
                if isinstance(v, list):
                    for it in v:
                        yield from walk(it)
                elif isinstance(v, dict):
                    yield from walk(v)
        elif isinstance(x, list):
            for it in x:
                yield from walk(it)

    yield from walk(layout_obj)


def _walk_layout_find_nodeinputs(layout_obj):
    """Backward-compatible iterator name."""
    yield from _walk_layout_find_link_elements(layout_obj)

def _fill_nodeinput_views(repo, parsed, layout, node_data):
    """
    Pre-fill <field>_view for link widgets in node_form.

    Important: DatasetLink/DatasetInput values are self-describing:
      Dataset$ItemId
    NodeLink/NodeInput values are self-describing:
      config_uid$ClassName$Id   or legacy ClassName$Id

    So node_form must not require an extra "dataset" attribute just to show
    the human-readable value.
    """
    import nodes as _nodes_mod

    node_cache = {}      # uid -> view
    dataset_cache = {}   # (dataset, item_id) -> view

    def _raw_ref_for_el(el: dict) -> str:
        raw_val = el.get("value")
        if isinstance(raw_val, str) and raw_val.startswith("@"):
            return str(node_data.get(raw_val[1:], "") or "").strip()
        if isinstance(raw_val, str):
            return raw_val.strip()
        return ""

    for el in _walk_layout_find_link_elements(layout):
        t = str(el.get("type") or el.get("t") or "")
        lid = str(el.get("id") or "").strip()
        raw_ref = _raw_ref_for_el(el)
        if not raw_ref:
            continue

        # For @field values the renderer looks for <field>_view.
        raw_val = el.get("value")
        field_name = raw_val[1:] if isinstance(raw_val, str) and raw_val.startswith("@") else lid
        if not field_name:
            continue
        view_key = f"{field_name}_view"
        if view_key in node_data:
            continue

        # Dataset widgets/links
        if t in ("DatasetInput", "DatasetField", "DatasetLink", "DataSetLink"):
            ds_name, item_id = _split_dataset_item_uid(str(el.get("dataset") or "").strip(), raw_ref)
            if not ds_name or not item_id:
                continue
            ck = (ds_name, item_id)
            if ck in dataset_cache:
                node_data[view_key] = dataset_cache[ck]
                continue
            try:
                item = _get_dataset_item_direct(repo.config_uid, ds_name, item_id)
                if not item:
                    continue
                view = str(item.get("_view") or item_id)
                dataset_cache[ck] = view
                node_data[view_key] = view
            except Exception:
                pass
            continue

        # Node widgets/links
        if t in ("NodeInput", "NodeLink"):
            if "$" not in raw_ref:
                continue
            if raw_ref in node_cache:
                node_data[view_key] = node_cache[raw_ref]
                continue
            try:
                cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(raw_ref)
                if not cls_name or not internal_id:
                    continue
                eff_cfg = cfg_uid or repo.config_uid
                node_cls = _load_server_node_class(eff_cfg, cls_name)
                n = node_cls.get(internal_id, eff_cfg)
                if not n:
                    continue
                try:
                    d = n.get_data() or {}
                except Exception:
                    d = {}
                view = _render_class_record_view(parsed, cls_name, internal_id, d)
                node_cache[raw_ref] = view
                node_data[view_key] = view
            except Exception:
                pass


def _nl_context(repo: models.Repo, *, class_name: str, node_id: str) -> Dict[str, Any]:
    def get_dataset_item_view(ds_name: str, item_uid: str) -> str:
        """Resolve DatasetLink value to display text.

        The normal value format is self-describing: ``DatasetName$ItemId``.
        ``ds_name`` is only a fallback for legacy layouts that store just ItemId.
        """
        raw = str(item_uid or "").strip()
        try:
            parsed_ds, item_id = _split_dataset_item_uid(ds_name, raw)
            if not parsed_ds or not item_id:
                return raw
            item_data = _get_dataset_item_direct(repo.config_uid, parsed_ds, item_id)
            if item_data:
                return str(item_data.get("_view") or item_id)
            return item_id
        except Exception:
            return raw

    def get_node_view(node_uid: str) -> str:
        """Resolve NodeLink value to display text.

        Node links are also self-describing: ``config_uid$ClassName$Id``.
        For old ``ClassName$Id`` links we use the current repo config_uid.
        """
        uid = str(node_uid or "").strip()
        if not uid:
            return ""
        try:
            uid_cfg, cls_name, internal_id = _nodes_mod.parse_uid_any(uid)
            if not internal_id:
                return uid
            eff_cfg = uid_cfg or str(repo.config_uid)
            if not cls_name:
                return internal_id

            node_cls = _load_server_node_class(eff_cfg, cls_name)
            n = node_cls.get(internal_id, eff_cfg)
            if not n:
                return internal_id

            try:
                d = n.get_data() or {}
            except Exception:
                try:
                    d = n._data if isinstance(getattr(n, "_data", None), dict) else {}
                except Exception:
                    d = {}

            return _render_class_record_view(parsed, cls_name, internal_id, d if isinstance(d, dict) else {})
        except Exception:
            return uid

    def uid_resolve(uid: str):
        """Resolve global node uid to (class_name, internal_id) for link/table helpers."""
        raw = str(uid or "").strip()
        try:
            uid_cfg, cls_name, internal_id = _nodes_mod.parse_uid_any(raw)
            if not internal_id:
                return ("", "")
            return (str(cls_name or ""), str(internal_id or ""))
        except Exception:
            return ("", "")


    return {
        "target": {
            "repo_id": int(repo.id),
            "config_uid": str(repo.config_uid),
            "class_name": str(class_name),
            "node_id": str(node_id),
        },
        "node_url": lambda c, i: url_for("client.node_form_redirect", repo_id=repo.id, class_name=c, node_id=i),
        # For Table(nodes_source=True, table=True): allow renderer to fetch per-node data
        # to fill individual cells according to table_header.
        "node_data": lambda c, i: _fetch_node_data_for_repo(repo, c, i),
        "node_cover": lambda c, i: _node_cover_html(repo, c, i),
        "node_cover_table": lambda cls, nid: _node_cover_html(repo, cls, nid, mode="table"),
        "node_children_tree": lambda c, i: _node_children_tree(repo, c, i),
        "get_dataset_item_view": get_dataset_item_view,
        "get_node_view": get_node_view,
        "uid_resolve": uid_resolve,
    }


def parse_config_url(config_url: str) -> Tuple[str, str, str]:
    """Returns (base_url, config_uid, normalized_config_url)."""
    u = (config_url or "").strip()
    if not u:
        raise ValueError("empty url")
    parsed = urlparse(u)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("url must include scheme and host, e.g. https://...")

    path = parsed.path.rstrip("/")
    parts = [p for p in path.split("/") if p]

    uid = None
    for i in range(len(parts) - 2):
        if parts[i] == "api" and parts[i + 1] == "config":
            uid = parts[i + 2]
            break
    if not uid:
        raise ValueError("url path must contain /api/config/<uid>")

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    normalized = base_url + "/api/config/" + uid
    return base_url, uid, normalized


def normalize_sections(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    secs = cfg.get("sections") or []
    out = []
    for s in secs:
        out.append({
            "code": s.get("code") or "",
            "name": s.get("name") or (s.get("code") or "<no code>"),
            "commands": (s.get("commands") or "").strip(),   
        })
    return out



def class_section_code(cls: Dict[str, Any]) -> str:
    return str(cls.get("section_code") or cls.get("section") or "")


def build_parsed_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    sections = normalize_sections(cfg)
    classes = cfg.get("classes") or []
    classes_by_name: Dict[str, Dict[str, Any]] = {}
    classes_by_section: Dict[str, List[Dict[str, Any]]] = {}

    for c in classes:
        name = c.get("name")
        if not name:
            continue
        classes_by_name[name] = c
        sc = class_section_code(c)
        classes_by_section.setdefault(sc, []).append(c)

    # Rooms mapping: alias -> room_uid
    rooms_map: Dict[str, str] = {}
    try:
        for it in (cfg.get("rooms") or []):
            if not isinstance(it, dict):
                continue
            a = str(it.get("alias") or "").strip()
            rid = str(it.get("room_id") or "").strip()
            if a:
                rooms_map[a] = rid
    except Exception:
        rooms_map = {}

    return {
        "cfg": cfg,
        "sections": sections,
        "classes": classes_by_name,
        "classes_by_section": classes_by_section,
        "rooms": rooms_map,
    }


def get_parsed_config(repo: models.Repo, db) -> Optional[Dict[str, Any]]:
    row = db.session.query(models.RepoConfig).filter_by(repo_id=repo.id).first()
    if not row:
        return None

    stamp = row.updated_at.isoformat() if row.updated_at else ""
    mem = CONFIG_MEM.get(repo.id)
    if mem and mem.get("stamp") == stamp:
        return mem

    try:
        cfg = json.loads(row.config_json)
    except Exception:
        return None

    cfg = _normalize_print_html_templates_in_config(cfg)
    parsed = build_parsed_config(cfg)
    parsed["stamp"] = stamp
    CONFIG_MEM[repo.id] = parsed
    return parsed

def resolve_common_layout(parsed: Optional[Dict[str, Any]], layout_spec: Any) -> Any:
    """Resolve '^layout_id' using cfg['CommonLayouts'].

    If not found or spec is not a '^' string, returns layout_spec unchanged.
    If spec is '^...' but not found, returns None (meaning: ignore).
    """
    if not isinstance(layout_spec, str):
        return layout_spec
    s = layout_spec.strip()
    if not s.startswith("^"):
        return layout_spec

    name = s[1:].strip()
    if not name:
        return None

    try:
        cfg = (parsed or {}).get("cfg") if isinstance(parsed, dict) else None
        items = (cfg or {}).get("CommonLayouts") if isinstance(cfg, dict) else None
        if not isinstance(items, list):
            return None
        for it in items:
            if isinstance(it, dict) and str(it.get("id") or "").strip() == name:
                return it.get("layout")
    except Exception:
        return None
    return None

def fetch_config_from_local_db(config_uid: str) -> Dict[str, Any]:
    
    
    Configuration = main.Configuration

    cfg_obj = models.db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()

    if not cfg_obj:
        raise ValueError(f"Configuration {config_uid} not found in DB")

    
    try:
        host_url = (request.host_url or "").rstrip("/")
    except Exception:
        host_url = ""
    url = (host_url + f"/api/config/{cfg_obj.uid}") if host_url else f"/api/config/{cfg_obj.uid}"

    classes = []
    for c in cfg_obj.classes:
        classes.append({
            "name": c.name,
            "section": c.section,
            "section_code": c.section_code,
            "has_storage": c.has_storage,
            "display_name": c.display_name,
            "record_view": getattr(c, "record_view", "") or "",
            "cover_image": c.cover_image,
            
            "display_image_web": getattr(c, "display_image_web", "") or "",
            "display_image_table": getattr(c, "display_image_table", "") or "",
            "data_structure": getattr(c, "data_structure", "") or "",
            "show_tag_cloud": bool(getattr(c, "show_tag_cloud", False)),
            "mobile_print_enabled": bool(getattr(c, "mobile_print_enabled", False)),
            "commands": getattr(c, "commands", "") or "",
            "use_standard_commands": bool(getattr(c, "use_standard_commands", True)),
            "svg_commands": getattr(c, "svg_commands", "") or "",
            
            "migration_register_command": bool(getattr(c, "migration_register_command", False)),
            "migration_register_on_save": bool(getattr(c, "migration_register_on_save", False)),
            "migration_default_room_uid": getattr(c, "migration_default_room_uid", "") or "",
            "migration_default_room_alias": getattr(c, "migration_default_room_alias", "") or "",
            "indexes": getattr(c, "indexes_json", None) or [],
            "class_type": c.class_type,
            "projection_type": getattr(c, "projection_type", "") or "",
            "projection_kanban_columns": getattr(c, "projection_kanban_columns", "") or "",
            "print_template_type": getattr(c, "print_template_type", "") or "html_jinja",
            "print_target_classes": getattr(c, "print_target_classes", None) or [],
            "print_html_template": _decode_print_html_template(getattr(c, "print_html_template", "") or ""),
            "hidden": getattr(c, "hidden", False),
            "init_screen_layout": getattr(c, "init_screen_layout", "") or "",
            "init_screen_layout_web": getattr(c, "init_screen_layout_web", "") or "",
            "methods": [{
                "name": m.name,
                "source": m.source,
                "engine": m.engine,
                "code": m.code,
            } for m in (c.methods or [])],
            "events": [
                {
                    "event": e.event,
                    "listener": e.listener,
                    "actions": [
                        {
                            "action": a.action,
                            "source": a.source,
                            "server": a.server,
                            "method": a.method,
                            "methodText": getattr(a, "method_text", "") or "",
                            "postExecuteMethod": a.post_execute_method,
                            "postExecuteMethodText": getattr(a, "post_execute_method_text", "") or "",
                        }
                        for a in (e.actions or [])
                    ],
                }
                for e in (getattr(c, "event_objs", None) or [])
            ],
        })

    sections = []
    for s in (cfg_obj.sections or []):
        sections.append({
            "name": s.name,
            "code": s.code,
            "commands": s.commands,
        })

    common_events = []
    for e in (getattr(cfg_obj, "config_events", None) or []):
        common_events.append({
            "event": getattr(e, "event", "") or "",
            "listener": getattr(e, "listener", "") or "",
            "actions": e.actions_as_dicts() if hasattr(e, "actions_as_dicts") else [],
        })   

    return {
        "name": cfg_obj.name,
        "server_name": getattr(cfg_obj, "server_name", "") or "",
        "uid": cfg_obj.uid,
        "url": url,
        "content_uid": getattr(cfg_obj, "content_uid", "") or "",
        "nodes_handlers": getattr(cfg_obj, "nodes_handlers", None),
        "nodes_server_handlers": getattr(cfg_obj, "nodes_server_handlers", None),
        "version": getattr(cfg_obj, "version", "00.00.01") or "00.00.01",
        "last_modified": cfg_obj.last_modified.isoformat() if getattr(cfg_obj, "last_modified", None) else "",
        "provider": cfg_obj.vendor or "",
        "vendor": cfg_obj.vendor or "", 
        "display_name": cfg_obj.name or "",
        "classes": classes,
        "sections": sections,
        "rooms": [
            {"alias": a.alias, "room_id": a.room_uid}
            for a in (getattr(cfg_obj, "room_aliases", None) or [])
        ],
        "CommonEvents": common_events,
        "Timers": [t.to_dict() if hasattr(t, "to_dict") else {
            "id": getattr(t, "timer_id", "") or "",
            "timer_id": getattr(t, "timer_id", "") or "",
            "period_seconds": max(900 if (str(getattr(t, "runtime", "") or "server").strip().lower() == "server" or bool(getattr(t, "worker", False))) else 1, getattr(t, "period_seconds", 0) or 0),
            "active": bool(getattr(t, "active", False)),
            "worker": bool(getattr(t, "worker", False)),
            "runtime": str(getattr(t, "runtime", "") or "server").strip().lower(),
            "actions": t.actions_as_dicts() if hasattr(t, "actions_as_dicts") else [],
        } for t in (getattr(cfg_obj, "config_timers", None) or [])],
        "CommonLayouts": getattr(cfg_obj, "common_layouts", None) or getattr(cfg_obj, "CommonLayouts", None) or [],
    }



def _handlers_file_path(config_uid: str) -> str:
    
    base_dir = os.path.dirname(os.path.abspath(__file__))  # client_app/
   
    root = os.path.abspath(os.path.join(base_dir, ".."))
    return os.path.join(root, "Handlers", config_uid, "handlers.py")

def _decode_base64_text_maybe(value: Any) -> str:
    """Decode a base64 text blob; tolerate already-plain text."""
    raw = str(value or "")
    if not raw.strip():
        return ""
    try:
        return base64.b64decode(raw).decode("utf-8", errors="replace")
    except Exception:
        return raw


def _load_server_handlers_ns(config_uid: str, parsed_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load server handlers for config_uid.

    Source priority:
      1) Handlers/<uid>/handlers.py for instant local edits;
      2) Configuration.nodes_server_handlers from the Designer DB;
      3) nodes_server_handlers/nodes_handlers from the cached repository config.

    The __file__ value is always the canonical Handlers/<uid>/handlers.py path,
    even when the code was read from DB/cache. This keeps existing helpers such
    as Node.get_all() able to resolve the current config UID from the handlers
    call stack.
    """
    config_uid = str(config_uid or "").strip()
    fp = _handlers_file_path(config_uid)

    code = None
    if os.path.isfile(fp):
        with open(fp, "r", encoding="utf-8") as f:
            code = f.read()

    if code is None:
        try:
            Configuration = getattr(main, "Configuration", None)
            if Configuration is not None:
                cfg = models.db.session.execute(
                    select(Configuration).where(Configuration.uid == config_uid)
                ).scalar_one_or_none()
                if cfg is not None and getattr(cfg, "nodes_server_handlers", None):
                    code = _decode_base64_text_maybe(cfg.nodes_server_handlers)
        except Exception:
            code = None

    if code is None and isinstance(parsed_config, dict):
        cfg_json = parsed_config.get("cfg") if isinstance(parsed_config.get("cfg"), dict) else parsed_config
        if isinstance(cfg_json, dict):
            # Server handlers are preferred. nodes_handlers is accepted as a
            # backward-compatible fallback for older cached configs.
            code = _decode_base64_text_maybe(
                cfg_json.get("nodes_server_handlers")
                or cfg_json.get("server_handlers")
                or cfg_json.get("nodes_handlers")
                or ""
            ) or None

    if code is None:
        raise ValueError(f"Handlers not found for config: {config_uid}")

    g: Dict[str, Any] = {
        "__name__": f"handlers_{config_uid}",
        "__file__": fp,
    }
    compiled = compile(code, fp, "exec")
    exec(compiled, g, g)

    try:
        for k, v in list(g.items()):
            if isinstance(v, type):
                try:
                    setattr(v, "_handlers_globals", g)
                except Exception:
                    pass
    except Exception:
        pass

    return g

def _handlers_file_path(config_uid: str) -> str:
    """
    Handlers/<uid>/handlers.py relative to project root.
    Adjust root if your folder layout differs.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))  # .../client_app
    root = os.path.abspath(os.path.join(base_dir, ".."))   # project root (usually)
    return os.path.join(root, "Handlers", config_uid, "handlers.py")


def _load_server_node_class(config_uid: str, class_name: str):
    """
    LOCAL-FIRST loader for server-side handlers.

    Source of truth (local):
      Handlers/<uid>/handlers.py

    Fallback (if no file):
      Configuration.nodes_server_handlers (DB blob)

    NO CACHE:
      always reads+execs current code so edits apply immediately.
    """
    config_uid = str(config_uid or "").strip()
    class_name = str(class_name or "").strip()
    if not config_uid or not class_name:
        raise ValueError("config_uid/class_name is empty")

    fp = _handlers_file_path(config_uid)

    # 1) LOCAL FILE FIRST
    code = None
    if os.path.isfile(fp):
        with open(fp, "r", encoding="utf-8") as f:
            code = f.read()

    # 2) FALLBACK TO DB ONLY IF FILE MISSING
    if code is None:
        from sqlalchemy import select, or_, and_, func
        import __main__ as main
        Configuration = main.Configuration

        cfg = models.db.session.execute(
            select(Configuration).where(Configuration.uid == config_uid)
        ).scalar_one_or_none()
        if not cfg or not getattr(cfg, "nodes_server_handlers", None):
            raise ValueError(f"Server handlers not found for config (no file {fp} and no DB blob)")

        code = base64.b64decode(cfg.nodes_server_handlers).decode("utf-8", errors="replace")

    # stable module name
    g: Dict[str, Any] = {
        "__name__": f"handlers_{config_uid}",
        "__file__": fp,
    }
    #exec(code, g, g)
    compiled = compile(code, fp, "exec")
    exec(compiled, g, g)

    
    try:
        for k, v in list(g.items()):
            if isinstance(v, type):
                try:
                    setattr(v, "_handlers_globals", g)
                except Exception:
                    pass
    except Exception:
        pass

    if class_name not in g:
        raise ValueError(f"Class {class_name} not found in server handlers")
    return g[class_name]




def fetch_config(config_url: str) -> Dict[str, Any]:
    resp = requests.get(config_url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def build_global_sections(repos: List[models.Repo], db) -> List[Dict[str, str]]:
    seen: Dict[str, str] = {}
    has_empty = False

    for r in repos:
        parsed = get_parsed_config(r, db)
        if not parsed:
            continue
        cfg = parsed["cfg"]
        classes = [c for c in (cfg.get("classes") or []) if not bool(c.get("hidden")) and not _is_print_form_class_type(c)]
        if any(not class_section_code(c) for c in classes):
            has_empty = True
        for s in normalize_sections(cfg):
            code = s["code"]
            if code not in seen:
                seen[code] = s["name"]

    sections: List[Dict[str, str]] = []
    if has_empty:
        sections.append({"code": "", "name": "<...>"})
    for code in sorted(seen.keys()):
        if code == "":
            continue
        sections.append({"code": code, "name": seen[code]})
    return sections


def _node_id(node: Dict[str, Any]) -> str:
    return str(
        node.get("_id")
        or node.get("_Id")
        or (node.get("_data") or {}).get("_id")
        or (node.get("_data") or {}).get("_Id")
        or ""
    )


def _text_like_index_ids_for_class(config_uid: str, class_name: str, q: str) -> Optional[List[str]]:
    q = (q or "").strip()
    if not q:
        return None
    try:
        node_cls = _load_server_node_class(config_uid, class_name)
        defs = node_cls._get_defined_indexes(config_uid) if hasattr(node_cls, "_get_defined_indexes") else []
    except Exception:
        return None

    idx_names: List[str] = []
    for idx in defs or []:
        if not isinstance(idx, dict):
            continue
        kind = str(idx.get("kind") or "hash_index").strip().lower()
        if kind not in ("text_index", "trigram_index", "text_index_full"):
            continue
        name = str(idx.get("name") or "").strip()
        if name and name not in idx_names:
            idx_names.append(name)

    if not idx_names:
        return None

    out: List[str] = []
    seen = set()
    has_index_rows = False
    for name in idx_names:
        try:
            store = node_cls._defined_index_storage(name, config_uid)
            if list(store.keys()):
                has_index_rows = True
        except Exception:
            pass
        try:
            ids = node_cls.find_ids_by_index(name, q, config_uid)
        except Exception:
            ids = []
        for nid in ids or []:
            sid = str(nid)
            if sid not in seen:
                seen.add(sid)
                out.append(sid)
    return out if has_index_rows else None


def _nodes_storage_page(config_uid: str, class_name: str, *, offset: int, limit: int, q: str = "", index_name: str = "", index_value: str = "") -> List[Dict[str, Any]]:
    """Read nodes directly from the same storage as /api/.../page (no HTTP call)."""
    storage_key = f"{class_name}_{config_uid}"
    db_path = os.path.join("node_storage", f"{storage_key}.sqlite")
    if not os.path.exists(db_path):
        return []

    table = "unnamed"
    q = (q or "").strip().lower()
    index_name = (index_name or "").strip()
    index_value = "" if index_value is None else str(index_value)

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        def fetch_items_by_ids(ids: List[str]) -> List[Dict[str, Any]]:
            items: List[Dict[str, Any]] = []
            for nid in (ids or [])[offset: offset + limit]:
                try:
                    cur.execute(f"SELECT value FROM {table} WHERE key = ?", (str(nid),))
                    row = cur.fetchone()
                    if not row:
                        continue
                    obj = unpack(row[0])
                except Exception:
                    obj = None
                if obj is not None:
                    items.append(obj)
            return items

        if index_name and index_value != "":
            try:
                node_cls = _load_server_node_class(config_uid, class_name)
                ids = node_cls.find_ids_by_index(index_name, index_value, config_uid)
            except Exception:
                ids = []
            return fetch_items_by_ids(ids)

        if q:
            indexed_ids = _text_like_index_ids_for_class(config_uid, class_name, q)
            if indexed_ids is not None:
                return fetch_items_by_ids(indexed_ids)

        if not q:
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
            return items

        # slow path: q scan
        cur.execute(f"SELECT value FROM {table}")
        rows = cur.fetchall()
        all_items = []
        for (val_blob,) in rows:
            obj = unpack(val_blob)
            if obj is not None:
                all_items.append(obj)

        def match(item: dict) -> bool:
            data = (item or {}).get("_data") or {}
            try:
                # prefer precomputed _search_index
                sidx = data.get("_search_index")
                if isinstance(sidx, str) and q in sidx.lower():
                    return True
            except Exception:
                pass
            for v in data.values():
                try:
                    if q in str(v).lower():
                        return True
                except Exception:
                    pass
            return False

        filtered = [it for it in all_items if match(it)]

        # mimic server sorting rule
        def sort_key(item: dict):
            d = (item or {}).get("_data") or {}
            if "_sort_string_desc" in d:
                return str(d.get("_sort_string_desc") or "")
            if "_sort_string" in d:
                return str(d.get("_sort_string") or "")
            return _node_id(item)

        any_desc = any("_sort_string_desc" in ((it or {}).get("_data") or {}) for it in filtered)
        filtered.sort(key=sort_key, reverse=any_desc)
        return filtered[offset: offset + limit]
    finally:
        conn.close()


def _auth_tuple(repo: models.Repo) -> Optional[tuple]:
    if repo.username:
        return (repo.username, repo.password)
    return None


def _api_get_remote(repo: models.Repo, path: str, *, params: Optional[dict] = None, timeout: int = 20) -> Any:
    url = repo.base_url.rstrip("/") + path
    resp = requests.get(url, params=params, auth=_auth_tuple(repo), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _fetch_nodes_for_class(repo: models.Repo, *, config_uid: str, class_name: str, q: str, limit: int, index_name: str = "", index_value: str = "") -> List[Dict[str, Any]]:
    """Fetch nodes either locally (same server) or remotely (repo.base_url override)."""
    # Default: no base_url override or points to this server -> read local storage.
    # If base_url is configured and does not match current host, do remote HTTP.
    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    if not base_url or base_url == current:
        return _nodes_storage_page(config_uid, class_name, offset=0, limit=limit, q=q, index_name=index_name, index_value=index_value)

    # Remote
    try:
        payload = _api_get_remote(
            repo,
            f"/api/config/{config_uid}/node/{class_name}/page",
            params=({"offset": 0, "limit": limit, "q": q} if q else {"offset": 0, "limit": limit}) | ({"index_name": index_name, "index_value": index_value} if index_name and index_value != "" else {}),
        )
        items = payload.get("items", [])
        return items if isinstance(items, list) else []
    except Exception:
        # fallback to full list
        try:
            all_nodes = _api_get_remote(repo, f"/api/config/{config_uid}/node/{class_name}")
        except Exception:
            return []

        if isinstance(all_nodes, dict):
            items = list(all_nodes.values())
        elif isinstance(all_nodes, list):
            items = all_nodes
        else:
            items = []

        ql = (q or "").strip().lower()
        if ql:
            def match(n: Dict[str, Any]) -> bool:
                data = n.get("_data") or {}
                if isinstance(data.get("_search_index"), str):
                    return ql in data["_search_index"].lower()
                try:
                    return ql in json.dumps(data, ensure_ascii=False).lower()
                except Exception:
                    return False
            items = [n for n in items if match(n)]

        return items[:limit]


def _api_get_remote(repo: models.Repo, path: str, *, params: Optional[dict] = None, timeout: int = 20) -> Any:
    url = repo.base_url.rstrip("/") + path
    resp = requests.get(url, params=params, auth=_auth_tuple(repo), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _api_post_remote(repo: models.Repo, path: str, *, json_data: Any = None, timeout: int = 20) -> Any:
    url = repo.base_url.rstrip("/") + path
    resp = requests.post(url, json=json_data, auth=_auth_tuple(repo), timeout=timeout)
    resp.raise_for_status()
    if resp.content and resp.headers.get("content-type", "").lower().startswith("application/json"):
        return resp.json()
    if resp.content:
        try:
            return resp.json()
        except Exception:
            return resp.text
    return None


def _api_delete_remote(repo: models.Repo, path: str, *, json_data: Any = None, timeout: int = 20) -> Any:
    url = repo.base_url.rstrip("/") + path
    resp = requests.delete(url, json=json_data, auth=_auth_tuple(repo), timeout=timeout)
    resp.raise_for_status()
    if resp.content and resp.headers.get("content-type", "").lower().startswith("application/json"):
        return resp.json()
    if resp.content:
        try:
            return resp.json()
        except Exception:
            return resp.text
    return None


# ---------- UI routes ----------

@client_bp.app_context_processor
def _inject_globals():
    # Scanner WS settings are used by base.html to auto-connect.
    # Stored per-user in client.sqlite.
    scanner_ws_url = _get_setting("scanner_ws_url", "").strip()
    scanner_ws_enabled = (_get_setting("scanner_ws_enabled", "1").strip() or "1")
    scanner_ws_enabled = (scanner_ws_enabled not in ("0", "false", "False", "no", "off"))
    return {
        "nl_css": DEFAULT_NL_CSS,
        "client_app_title": APP_TITLE,
        "scanner_ws_url": scanner_ws_url,
        "scanner_ws_enabled": scanner_ws_enabled,
    }


@client_bp.route("/")
@login_required
def home():
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    sections = _with_received_nodes_section(build_global_sections(repos, models.db))
    section_code = request.args.get("section", None)
    if section_code is None:
        section_code = _default_section_code(sections)
    scode_url = section_code if section_code != "" else "__empty__"
    return redirect(url_for("client.section_view", section_code=scode_url))


@client_bp.route("/sections")
@login_required
def sections_home():
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    sections = _with_received_nodes_section(build_global_sections(repos, models.db)) if repos else []
    if not sections:
        return render_template(
            "client/section.html",
            title=f"{APP_TITLE} — Sections",
            repos=repos,
            sections=[],
            section_code="",
            section_name="",
            auto_refresh=AUTO_REFRESH_SECONDS,
            no_repos=(len(repos) == 0),
        )
    first = _default_section_code(sections)
    scode_url = first if first != "" else "__empty__"
    return redirect(url_for("client.section_view", section_code=scode_url))


def _get_repo_or_404(repo_id: int) -> models.Repo:
    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        abort(404)
    return repo


def _get_class_cfg(repo: models.Repo, class_name: str) -> Optional[Dict[str, Any]]:
    parsed = get_parsed_config(repo, models.db)
    if not parsed:
        return None
    return (parsed.get("classes") or {}).get(class_name)




def _node_local_get_data(config_uid: str, class_name: str, node_id: str) -> Dict[str, Any]:
    node_class = _load_server_node_class(config_uid, class_name)
    node = node_class.get(node_id, config_uid)
    if not node:
        return {}
    return node.get_data() or {}


def _node_local_update_data(config_uid: str, class_name: str, node_id: str, data: Dict[str, Any], user_modification: Optional[Dict[str, Any]] = None):
    node_class = _load_server_node_class(config_uid, class_name)
    node = node_class.get(node_id, config_uid)
    if not node:
        raise ValueError("node not found")

    try:
        node._schema_class_name = class_name
    except Exception:
        pass

    merged = dict(data or {})
    merged.setdefault("_class", class_name)

    # Full-form saves should keep replacement semantics, but validation/events
    # must still see the real previous saved_state.  Prime _data_cache instead
    # of saving before update_data(); Node.update_data() will run onAcceptServer,
    # persist, and then run onAfterAcceptServer.
    try:
        node._data_cache = dict(merged)
    except Exception:
        pass

    input_data = dict(merged)
    if isinstance(user_modification, dict) and user_modification:
        input_data["_user_modification"] = user_modification

    node.update_data(input_data)
    return node

def _node_local_delete(config_uid: str, class_name: str, node_id: str) -> None:
    node_class = _load_server_node_class(config_uid, class_name)
    node = node_class.get(node_id, config_uid)
    if not node:
        return

    node.delete()


def _node_local_create(config_uid: str, class_name: str, initial_data: Optional[Dict[str, Any]] = None) -> str:

    node_class = _load_server_node_class(config_uid, class_name)

    data = initial_data or {}
    
    user_data = dict(data or {})

    #node_id = (data.get("_id") or str(uuid.uuid4()))
    #node = node_class(node_id, config_uid)
    raw_id = data.get("_id")
    node_id = _nodes_mod.extract_internal_id(raw_id) if raw_id else str(uuid.uuid4())
    node = node_class(node_id, config_uid)
    if user_data:
        # update_data() already persists the node and runs onAcceptServer/onAfterAcceptServer.
        # Calling _save() again here would duplicate hooks and, for schedule cell creation,
        # could lose the original _user_modification payload.
        node.update_data(user_data)
    return node_id


def _register_nodes_to_room_local(config_uid: str, class_name: str, room_uid: str, node_ids: List[str]) -> int:
    """Register selected nodes in a room locally (without HTTP self-calls).

    Mirrors: /api/config/<config_uid>/node/<class_name>/register/<room_uid>
    but uses direct python calls to avoid deadlocks.
    """
    room_uid = (room_uid or "").strip()
    if not room_uid:
        return 0

    node_class = _load_server_node_class(config_uid, class_name)
    nodes_data: List[Dict[str, Any]] = []

    for nid in (node_ids or []):
        nid = str(nid or "").strip()
        if not nid:
            continue
        try:
            node = node_class.get(nid, config_uid)
        except Exception:
            node = None
        if not node:
            continue
        try:
            d = node.to_dict() if hasattr(node, "to_dict") else {}
        except Exception:
            d = {}
        if not isinstance(d, dict):
            d = {}
        d.setdefault("_id", nid)
        nodes_data.append(d)

    if not nodes_data:
        return 0

    # Delegate to server helper that queues objects in the room
    try:
        main.handle_room_objects(config_uid, class_name, room_uid, nodes_data)
    except Exception:
        # if it fails, still return count=0 to the caller
        return 0
    return len(nodes_data)


def _resolve_class_default_room_uid(parsed: Optional[Dict[str, Any]], cls_cfg: Dict[str, Any]) -> str:
    """Resolve default room uid for Migration registration.

    New style: class stores migration_default_room_alias, mapping stored in cfg['rooms'].
    Backward compatible: falls back to migration_default_room_uid.
    """
    try:
        alias = str(cls_cfg.get("migration_default_room_alias") or "").strip()
        if alias:
            rooms_map = (parsed or {}).get("rooms") if isinstance(parsed, dict) else None
            if isinstance(rooms_map, dict):
                ru = str(rooms_map.get(alias) or "").strip()
                if ru:
                    return ru
        # fallback
        return str(cls_cfg.get("migration_default_room_uid") or "").strip()
    except Exception:
        return str(cls_cfg.get("migration_default_room_uid") or "").strip()


def _normalize_custom_process_uid(config_uid: str, class_name: str, node_id: str) -> str:
    """Ensure custom_process uid is always 3-part: cfg$Class$singleton.

    Backward compatible with older 2-part form: cfg$Class.
    """
    config_uid = str(config_uid or "").strip()
    class_name = str(class_name or "").strip()
    raw = str(node_id or "").strip()
    if not config_uid or not class_name:
        return raw

    parts = raw.split("$") if raw else []

    # Already normalized: cfg$Class$something
    if len(parts) >= 3 and parts[0] == config_uid and parts[1] == class_name:
        return raw

    # Old form: cfg$Class
    if len(parts) == 2 and parts[0] == config_uid and parts[1] == class_name:
        try:
            return _nodes_mod.normalize_own_uid(config_uid, class_name, "singleton")
        except Exception:
            return f"{config_uid}${class_name}$singleton"

    # Fallback: force singleton
    try:
        return _nodes_mod.normalize_own_uid(config_uid, class_name, "singleton")
    except Exception:
        return f"{config_uid}${class_name}$singleton"
    
def _node_local_upsert_custom_process(config_uid: str, class_name: str, node_id: str, data: Dict[str, Any]) -> str:
    """Create (if missing) and save a custom_process singleton node locally."""
    node_uid = _normalize_custom_process_uid(config_uid, class_name, node_id)
    node_class = _load_server_node_class(config_uid, class_name)

    node = None
    try:
        node = node_class.get(node_uid, config_uid)
    except Exception:
        node = None

    if not node:
        try:
            internal_id = _nodes_mod.extract_internal_id(node_uid) or "singleton"
        except Exception:
            internal_id = "singleton"
        node = node_class(internal_id, config_uid)

    merged = dict(data or {})
    merged.setdefault("_class", class_name)
    merged.setdefault("_id", node_uid)

    try:
        node._data = merged
    except Exception:
        node.update_data(merged)

    try:
        node._schema_class_name = class_name
    except Exception:
        pass

    if hasattr(node, "_save") and callable(getattr(node, "_save")):
        node._save()
    return node_uid    


def _parse_projection_kanban_columns(cls_cfg: Dict[str, Any]) -> List[Dict[str, str]]:
    raw = (cls_cfg or {}).get("projection_kanban_columns") or (cls_cfg or {}).get("_kanban_columns") or []
    obj = []
    if isinstance(raw, list):
        obj = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw) if raw.strip() else []
            obj = parsed if isinstance(parsed, list) else []
        except Exception:
            obj = []
    out: List[Dict[str, str]] = []
    seen = set()
    for col in obj:
        if not isinstance(col, dict):
            continue
        cid = str(col.get("id") or col.get("key") or "").strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        out.append({"id": cid, "caption": str(col.get("caption") or col.get("title") or cid)})
    return out


def _apply_projection_defaults_to_data(cls_cfg: Dict[str, Any], data: Dict[str, Any], config_uid: str, class_name: str, node_id: str) -> None:
    if not isinstance(data, dict) or not _is_projection_class_type(cls_cfg):
        return
    projection_type = str((cls_cfg or {}).get("projection_type") or PROJECTION_KANBAN_TYPE).strip() or PROJECTION_KANBAN_TYPE
    data.setdefault("_projection_type", projection_type)
    data.setdefault("_projection_uid", _normalize_custom_process_uid(config_uid, class_name, node_id))
    if projection_type == PROJECTION_KANBAN_TYPE:
        # Columns are a class-level projection setting.  Older projection nodes may
        # already have _kanban_columns stored in their own _data; if we keep that
        # value, editing the class from 3 columns to 5 columns still renders the
        # old 3-column snapshot.  When the class config contains a columns field,
        # always re-sync it from the class; only preserve node/client-supplied
        # columns for legacy/raw projections where the class has no such field.
        class_has_columns_field = ("projection_kanban_columns" in (cls_cfg or {})) or ("_kanban_columns" in (cls_cfg or {}))
        if class_has_columns_field or "_kanban_columns" not in data:
            data["_kanban_columns"] = _parse_projection_kanban_columns(cls_cfg)



def _normalize_print_targets(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x or '').strip()]
    s = str(raw or '').strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x or '').strip()]
    except Exception:
        pass
    return [x.strip() for x in re.split(r"[,;\n]+", s) if x.strip()]


def _print_forms_for_class(parsed: Optional[Dict[str, Any]], class_name: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    classes = (parsed or {}).get("classes") or {}
    for pf in classes.values():
        if not isinstance(pf, dict) or not _is_print_form_class_type(pf):
            continue
        targets = _normalize_print_targets(pf.get("print_target_classes") or pf.get("printTargetClasses"))
        if class_name in targets:
            out.append(pf)
    out.sort(key=lambda x: str(x.get("display_name") or x.get("name") or "").lower())
    return out


def _print_qr_data_url(value: Any) -> str:
    if qrcode is None:
        return ""
    try:
        img = qrcode.make(str(value or ""))
        buf = BytesIO()
        img.save(buf, format="PNG")
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return ""


def _print_image_src(repo: models.Repo, value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("data:") or raw.startswith("blob:") or raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return url_for("client.api_userfiles_raw", repo_id=repo.id, filename=raw)


def _print_table_rows(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return list(value.values())
    return []


def _build_print_jinja_context(repo: models.Repo, data: Dict[str, Any]) -> Dict[str, Any]:
    data = data if isinstance(data, dict) else {}
    wrapped_data = _print_attr_tree(data)
    ctx: Dict[str, Any] = {
        "_data": wrapped_data,
        "data": wrapped_data,
        "qr": _print_qr_data_url,
        "image_src": lambda value: _print_image_src(repo, value),
        "table_rows": _print_table_rows,
    }
    for k, v in data.items():
        key = str(k or "")
        if not key:
            continue
        wrapped_value = _print_attr_tree(v)
        ctx[key] = wrapped_value
        ctx["_" + key.lstrip("_")] = wrapped_value
    return ctx


def _render_print_html(repo: models.Repo, print_cls: Dict[str, Any], data: Dict[str, Any]) -> str:
    template_type = str((print_cls or {}).get("print_template_type") or PRINT_FORM_TEMPLATE_HTML_JINJA).strip()
    html_template = _decode_print_html_template((print_cls or {}).get("print_html_template") or "")
    if template_type != PRINT_FORM_TEMPLATE_HTML_JINJA:
        return f"<div class='alert alert-warning'>Unsupported PrintForm template type: {escape(template_type)}</div>"
    try:
        env = _PrintSandboxedEnvironment(autoescape=select_autoescape(["html", "xml"]))
        env.globals.update(
            qr=_print_qr_data_url,
            image_src=lambda value: _print_image_src(repo, value),
            table_rows=_print_table_rows,
        )
        return env.from_string(html_template).render(**_build_print_jinja_context(repo, data))
    except Exception as e:
        return f"<div class='alert alert-danger'>PrintForm render error: {escape(str(e))}</div>"


def _print_form_node_id(config_uid: str, print_class_name: str, base_class_name: str, base_node_id: str) -> str:
    digest = hashlib.sha1(f"{base_class_name}:{base_node_id}".encode("utf-8", errors="ignore")).hexdigest()[:16]
    return _nodes_mod.normalize_own_uid(config_uid, print_class_name, f"print_{digest}")


def _execute_print_form_start_handler(repo: models.Repo, parsed: Dict[str, Any], print_cls: Dict[str, Any], print_node_id: str, base_class_name: str, base_node_id: str, base_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create an ephemeral PrintForm node, inject _basement_data, run onInputWeb/onStartForm, return _data."""
    print_class_name = str((print_cls or {}).get("name") or "").strip()
    node_data: Dict[str, Any] = {
        "_id": print_node_id,
        "_class": print_class_name,
        "_basement_class": base_class_name,
        "_basement_id": base_node_id,
        "_basement_data": dict(base_data or {}),
    }

    actions: List[Dict[str, Any]] = []
    for ev in (print_cls.get("events") or []):
        if (ev.get("event") or "") not in ("onInputWeb", "onInput"):
            continue
        listener = str(ev.get("listener") or "").strip()
        if listener and listener != "onStartForm":
            continue
        # Prefer the explicit web event, but keep onInput as a fallback for older configs.
        if (ev.get("event") or "") == "onInputWeb" or not actions:
            actions.extend(ev.get("actions") or [])

    if not print_class_name:
        return node_data

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if base_url and base_url != current:
            # Remote PrintForm rendering can still use the template in cached configuration.
            # If handlers are remote, call selected methods and merge returned data when the API supports it.
            payload = {"listener": "onStartForm", "_basement_data": base_data, "base_class_name": base_class_name, "base_node_id": base_node_id}
            for a in actions:
                m = str((a or {}).get("method") or "").strip()
                if not m:
                    continue
                try:
                    r = _api_post_remote(repo, f"/api/config/{repo.config_uid}/node/{print_class_name}/{print_node_id}/{m}", json_data=payload)
                    if isinstance(r, dict) and isinstance(r.get("data"), dict):
                        data = r.get("data") or {}
                        if isinstance(data.get("_data"), dict):
                            node_data.update(data.get("_data") or {})
                        else:
                            node_data.update(data)
                except Exception:
                    continue
            node_data.setdefault("_basement_data", dict(base_data or {}))
            return node_data

        node_class = _load_server_node_class(repo.config_uid, print_class_name)
        node = node_class(_nodes_mod.extract_internal_id(print_node_id) or print_node_id, repo.config_uid)
        try:
            node._schema_class_name = print_class_name
        except Exception:
            pass
        try:
            node._data_cache = dict(node_data)
            node._data = dict(node_data)
        except Exception:
            pass

        prev_current = getattr(_nodes_mod, "CURRENT_NODE", None)
        setattr(_nodes_mod, "CURRENT_NODE", node)
        try:
            payload = {"listener": "onStartForm", "_basement_data": base_data, "base_class_name": base_class_name, "base_node_id": base_node_id}
            for a in actions:
                m = str((a or {}).get("method") or "").strip()
                if m and hasattr(node, m):
                    getattr(node, m)(payload)
        finally:
            setattr(_nodes_mod, "CURRENT_NODE", prev_current)

        try:
            if isinstance(getattr(node, "_data_cache", None), dict):
                node_data.update(node._data_cache or {})
            elif isinstance(getattr(node, "_data", None), dict):
                node_data.update(node._data or {})
        except Exception:
            pass

        # PrintForm is deliberately ephemeral: remove the runtime node row that
        # Node.__init__ may have created so the form does not persist data.
        try:
            if getattr(node, "_storage", None) is not None and getattr(node, "_id", None) in node._storage:
                del node._storage[node._id]
        except Exception:
            pass

        node_data.setdefault("_basement_data", dict(base_data or {}))
        node_data.setdefault("_id", print_node_id)
        node_data.setdefault("_class", print_class_name)
        return node_data
    except Exception as e:
        node_data["_print_error"] = str(e)
        return node_data


def _build_print_form_runtime(repo: models.Repo, parsed: Dict[str, Any], print_class_name: str, base_class_name: str, base_node_id: str) -> Tuple[Dict[str, Any], str, str, Dict[str, Any]]:
    classes = (parsed or {}).get("classes") or {}
    print_cls = classes.get(print_class_name) or {}
    if not print_cls or not _is_print_form_class_type(print_cls):
        abort(404)
    targets = _normalize_print_targets(print_cls.get("print_target_classes"))
    if base_class_name not in targets:
        abort(403)

    base_data = _fetch_node_data_for_repo(repo, base_class_name, base_node_id) or {}
    if isinstance(base_data, dict):
        base_data.setdefault("_id", base_node_id)
        base_data.setdefault("_class", base_class_name)
    print_node_id = _print_form_node_id(repo.config_uid, print_class_name, base_class_name, base_node_id)
    print_data = _execute_print_form_start_handler(repo, parsed, print_cls, print_node_id, base_class_name, base_node_id, base_data)
    html = _render_print_html(repo, print_cls, print_data)
    return print_cls, print_node_id, html, print_data

def _strip_projection_runtime_fields_for_save(cls_cfg: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    """Return projection data without generated/report-only object lists.

    The projection node stores parameters only.  The current set of objects can be
    returned by onRunProjection and kept in the browser for the current render,
    but a normal Save of the projection must not persist those UIDs.
    """
    if not isinstance(data, dict):
        return {}
    if not _is_projection_class_type(cls_cfg):
        return data
    out = dict(data)
    for key in PROJECTION_TRANSIENT_SAVE_FIELDS:
        out.pop(key, None)
    return out


def _collect_runtime_messages_payload(node: Any = None) -> Dict[str, Any]:
    """Collect messages produced while saving a projection object."""
    messages: List[Dict[str, str]] = []
    seen = set()

    def add(msg: Any) -> None:
        if not msg:
            return
        if isinstance(msg, list):
            for one in msg:
                add(one)
            return
        if isinstance(msg, dict):
            text = str(msg.get("text") or msg.get("message") or "").strip()
            level = str(msg.get("level") or "info").strip() or "info"
        else:
            text = str(msg).strip()
            level = "info"
        if not text:
            return
        if level == "error":
            level = "danger"
        key = (text, level)
        if key in seen:
            return
        seen.add(key)
        messages.append({"text": text, "level": level})

    try:
        runtime_messages = getattr(_nodes_mod, "RUNTIME_MESSAGES", None)
        if runtime_messages is not None:
            add(runtime_messages.get())
    except Exception:
        pass
    try:
        add(getattr(node, "_ui_message", None))
    except Exception:
        pass

    if not messages:
        return {}
    return {"messages": messages, "message": messages[-1]}


def _projection_accept_error_payload(e: Exception) -> Dict[str, Any]:
    payload = getattr(e, "payload", None) or {}
    if not isinstance(payload, dict):
        payload = {"error": str(e)}
    msg = payload.get("message")
    if not isinstance(msg, dict):
        msg = {"text": str(payload.get("error") or str(e) or "Save rejected"), "level": "danger"}
    if msg.get("level") == "error":
        msg["level"] = "danger"
    return {"ok": False, "error": payload.get("error") or str(e) or "rejected", "message": msg}


def _projection_move_success_payload(save_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True}
    if isinstance(save_meta, dict):
        out.update(save_meta)
    return out


def _normalize_projection_object_ids(value: Any) -> List[str]:
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            value = parsed
        except Exception:
            value = [x.strip() for x in re.split(r"[\n,;]+", s) if x.strip()]

    if isinstance(value, dict):
        # Accept both a single record {uid/_id/id: ...} and a mapping
        # {id: uid_or_record}. This mirrors nodes.to_uid(get_all()).
        if any(k in value for k in ("uid", "_uid", "_id", "id")):
            value = [value]
        else:
            value = list(value.values())
    elif isinstance(value, (tuple, set)):
        value = list(value)

    if not isinstance(value, list):
        return []
    out: List[str] = []
    seen = set()
    for item in value:
        uid = ""
        if isinstance(item, str):
            uid = item.strip()
        elif isinstance(item, dict):
            uid = str(item.get("uid") or item.get("_uid") or item.get("_id") or item.get("id") or "").strip()
        if uid and uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _repo_for_config_uid(fallback: models.Repo, config_uid: str) -> models.Repo:
    if not config_uid or str(config_uid) == str(fallback.config_uid):
        return fallback
    repo = models.Repo.query.filter_by(config_uid=str(config_uid), user_id=current_user.id).first()
    return repo or fallback


def _get_projection_node_data(repo: models.Repo, cls_cfg: Dict[str, Any], class_name: str, node_id: str) -> Dict[str, Any]:
    cfg_uid = repo.config_uid
    node_uid = _normalize_custom_process_uid(cfg_uid, class_name, node_id)
    defaults = (cls_cfg.get("_data") or {}) if isinstance(cls_cfg, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    data = dict(defaults)
    try:
        node_class = _load_server_node_class(cfg_uid, class_name)
        node = node_class.get(node_uid, cfg_uid)
        if node:
            stored = node.get_data() or {}
            if isinstance(stored, dict):
                data.update(stored)
    except Exception:
        pass
    if _is_projection_class_type(cls_cfg):
        for key in PROJECTION_TRANSIENT_SAVE_FIELDS:
            data.pop(key, None)
    _apply_projection_defaults_to_data(cls_cfg, data, cfg_uid, class_name, node_uid)
    data.setdefault("_id", node_uid)
    data.setdefault("_class", class_name)
    return data


def _projection_key_aliases(projection_uid: str) -> List[str]:
    raw = str(projection_uid or "").strip()
    out: List[str] = []
    if raw:
        out.append(raw)
        parts = raw.split("$")
        # Backward compatibility with older singleton ids: cfg$Class
        if len(parts) >= 3 and parts[0] and parts[1]:
            legacy = f"{parts[0]}${parts[1]}"
            if legacy not in out:
                out.append(legacy)
    return out



def _projection_value_for_data(data: Dict[str, Any], projection_uid: str) -> Any:
    if not isinstance(data, dict):
        return None
    vals = data.get("_projection_values")
    if isinstance(vals, dict):
        for key in _projection_key_aliases(projection_uid):
            if key in vals:
                return vals.get(key)

    # Backward/shortcut compatibility: some handlers use singular
    # _projection_value either as a map by projection uid or as the direct value.
    single = data.get("_projection_value")
    if isinstance(single, dict):
        for key in _projection_key_aliases(projection_uid):
            if key in single:
                return single.get(key)
        direct_markers = {
            "id", "column_id", "resource_id", "doctor_id", "task_id", "parent",
            "start", "end", "period_start", "period_end", "x1", "y1", "x2", "y2",
        }
        if any(k in single for k in direct_markers):
            return single
    elif single is not None:
        return single
    return None


def _normalize_projection_timer(value: Any) -> int:
    try:
        n = int(float(value))
    except Exception:
        return 0
    return n if n > 0 else 0


def _boolish_projection_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on", "да", "истина"}
    return False


def _normalize_diagram_projection_value(value: Any, index: int = 0) -> Dict[str, Any]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value) if value.strip() else {}
            value = parsed if isinstance(parsed, dict) else {}
        except Exception:
            value = {}
    if not isinstance(value, dict):
        value = {}

    def num(name: str, default: float) -> float:
        try:
            return float(value.get(name, default))
        except Exception:
            return float(default)

    x1 = num("x1", num("x", 24 + (index % 10) * 110))
    y1 = num("y1", num("y", 24 + (index // 10) * 74))
    w = num("width", 90)
    h = num("height", 52)
    x2 = num("x2", x1 + w)
    y2 = num("y2", y1 + h)
    if x2 <= x1:
        x2 = x1 + max(20, w)
    if y2 <= y1:
        y2 = y1 + max(20, h)

    figure = str(value.get("figure") or "rectangle").strip().lower()
    if figure not in {"rectangle", "circle", "svg"}:
        figure = "rectangle"
    bg = str(value.get("background") or "#ffffff").strip()
    if not re.match(r"^#[0-9a-fA-F]{3}([0-9a-fA-F]{3})?$", bg):
        bg = "#ffffff"

    return {
        "x1": x1,
        "y1": y1,
        "x2": x2,
        "y2": y2,
        "figure": figure,
        "svg": str(value.get("svg") or ""),
        "background": bg,
        "text": str(value.get("text") or ""),
    }

def _projection_object_payload(repo: models.Repo, projection_uid: str, object_uid: str) -> Optional[Dict[str, Any]]:
    try:
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(object_uid)
    except Exception:
        cfg_uid, cls_name, internal_id = None, None, None
    cls_name = str(cls_name or "").strip()
    internal_id = str(internal_id or "").strip()
    if not cls_name or not internal_id:
        return None
    obj_repo = _repo_for_config_uid(repo, cfg_uid or repo.config_uid)
    data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
    if not isinstance(data, dict):
        data = {}
    if data.get("_hidden"):
        return None
    normalized_uid = _nodes_mod.normalize_own_uid(obj_repo.config_uid, cls_name, internal_id)
    projection_value = _projection_value_for_data(data, projection_uid)
    col = "__empty__"
    if projection_value is not None and not isinstance(projection_value, dict):
        col = str(projection_value or "__empty__")
    try:
        cover_html = _node_cover_html(obj_repo, cls_name, internal_id)
    except Exception:
        cover_html = ""
    parsed = get_parsed_config(obj_repo, models.db) or {}
    view = _render_class_record_view(parsed, cls_name, internal_id, data)
    return {
        "uid": normalized_uid,
        "repo_id": obj_repo.id,
        "repo_uid": obj_repo.config_uid,
        "class": cls_name,
        "id": internal_id,
        "column_id": col or "__empty__",
        "projection_value": projection_value,
        "data": data,
        "view": view,
        "cover_html": cover_html,
        "open_url": url_for("client.node_form", config_uid=obj_repo.config_uid, class_name=cls_name, node_id=internal_id),
    }


def _projection_object_diagram_payload(repo: models.Repo, projection_uid: str, object_uid: str) -> Optional[Dict[str, Any]]:
    """Return a lightweight payload for diagram projections.

    Diagram projections render their own shapes from _projection_values and do not
    need card covers or full class record views. Avoiding _node_cover_html() and
    _render_class_record_view() keeps the Loading... stage fast for large
    diagrams such as warehouse maps.
    """
    try:
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(object_uid)
    except Exception:
        cfg_uid, cls_name, internal_id = None, None, None
    cls_name = str(cls_name or "").strip()
    internal_id = str(internal_id or "").strip()
    if not cls_name or not internal_id:
        return None

    obj_repo = _repo_for_config_uid(repo, cfg_uid or repo.config_uid)
    data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
    if not isinstance(data, dict):
        data = {}
    if data.get("_hidden"):
        return None

    projection_value = _projection_value_for_data(data, projection_uid)
    if projection_value is None:
        return None

    title = str(
        data.get("caption")
        or data.get("title")
        or data.get("name")
        or internal_id
        or ""
    )
    normalized_uid = _nodes_mod.normalize_own_uid(obj_repo.config_uid, cls_name, internal_id)
    return {
        "uid": normalized_uid,
        "repo_id": obj_repo.id,
        "repo_uid": obj_repo.config_uid,
        "class": cls_name,
        "id": internal_id,
        "projection_value": projection_value,
        "view": {"title": title},
        "open_url": url_for("client.node_form", config_uid=obj_repo.config_uid, class_name=cls_name, node_id=internal_id),
    }



def _parse_projection_datetime(value: Any, fallback: Optional[datetime] = None) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        try:
            # Accept Unix seconds or milliseconds.
            n = float(value)
            if n > 10_000_000_000:
                n = n / 1000.0
            return datetime.fromtimestamp(n).replace(tzinfo=None)
        except Exception:
            return fallback
    raw = str(value or "").strip()
    if not raw:
        return fallback
    if raw.endswith("Z"):
        raw = raw[:-1]
    raw = raw.replace(" ", "T")
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw[:len(datetime.now().strftime(fmt))] if "%" in fmt else raw, fmt)
            return dt.replace(tzinfo=None)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=None)
    except Exception:
        return fallback


def _projection_datetime_iso(value: Optional[datetime]) -> str:
    if not value:
        return ""
    return value.replace(microsecond=0).isoformat()


def _projection_day_key(value: Optional[datetime]) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def _projection_read_jsonish(value: Any, fallback: Any) -> Any:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return fallback
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return value if value is not None else fallback


def _normalize_projection_orientation(value: Any) -> str:
    raw = str(value or "vertical").strip().lower()
    if raw in {"horizontal", "h", "row", "rows", "time-horizontal"}:
        return "horizontal"
    return "vertical"


def _projection_float(value: Any, fallback: float) -> float:
    try:
        n = float(value)
        if math.isfinite(n):
            return n
    except Exception:
        pass
    return fallback


def _projection_schedule_create_class(data: Dict[str, Any]) -> str:
    data = data if isinstance(data, dict) else {}
    return str(data.get("_projection_create_class") or "").strip()


def _projection_schedule_default_interval_hours(data: Dict[str, Any]) -> float:
    data = data if isinstance(data, dict) else {}
    n = _projection_float(data.get("_projection_default_interval_hours"), 0.25)
    return min(24.0, max(1.0 / 60.0, n))


def _normalize_schedule_columns(value: Any, period_start: datetime, period_end: datetime, selected_date: str = "") -> Tuple[str, List[Dict[str, Any]], str]:
    raw = _projection_read_jsonish(value, [])
    mode = "resources"
    projection_id = ""
    if isinstance(raw, str) and raw.strip().lower() == "days":
        mode = "days"
    elif isinstance(raw, dict):
        if str(raw.get("mode") or raw.get("type") or "").strip().lower() == "days":
            mode = "days"
        projection_id = str(raw.get("id") or raw.get("projection_id") or "").strip()
        raw = raw.get("columns") or raw.get("items") or []
    elif isinstance(raw, list) and len(raw) == 1 and str(raw[0]).strip().lower() == "days":
        mode = "days"

    if mode == "days":
        base_date = _parse_projection_datetime(selected_date) or period_start or datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_day = period_start.date() if period_start else base_date.date()
        end_day = period_end.date() if period_end and period_end.date() > start_day else (start_day + timedelta(days=6))
        rows = []
        d = start_day
        while d <= end_day and len(rows) < 120:
            key = d.isoformat()
            rows.append({"id": key, "caption": key, "date": key, "areas": []})
            d += timedelta(days=1)
        return mode, rows, projection_id

    cols: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        seen = set()
        for col in raw:
            if not isinstance(col, dict):
                continue
            cid = str(col.get("id") or col.get("key") or col.get("uid") or "").strip()
            if not cid or cid in seen:
                continue
            seen.add(cid)
            areas = col.get("areas") or col.get("availability") or []
            if isinstance(areas, dict):
                areas = [areas]
            norm_areas = []
            if isinstance(areas, list):
                for a in areas:
                    if not isinstance(a, dict):
                        continue
                    a_start = _parse_projection_datetime(a.get("period_start") or a.get("perior_start") or a.get("start"), None)
                    a_end = _parse_projection_datetime(a.get("period_end") or a.get("perior_end") or a.get("end"), None)
                    norm_areas.append({
                        "start": _projection_datetime_iso(a_start) if a_start else str(a.get("period_start") or a.get("perior_start") or a.get("start") or ""),
                        "end": _projection_datetime_iso(a_end) if a_end else str(a.get("period_end") or a.get("perior_end") or a.get("end") or ""),
                        "color": str(a.get("color") or a.get("background") or "#e9ecef"),
                        "available": bool(a.get("available", a.get("availible", True))),
                    })
            cols.append({
                "id": cid,
                "caption": str(col.get("caption") or col.get("title") or cid),
                "areas": norm_areas,
            })
    return mode, cols, projection_id


def _normalize_schedule_projection_value(value: Any, projection_uid: str, mode: str = "resources", index: int = 0) -> Optional[Dict[str, Any]]:
    value = _projection_read_jsonish(value, value)
    if not isinstance(value, dict):
        return None
    row_id = str(value.get("id") or value.get("column_id") or value.get("resource_id") or value.get("doctor_id") or value.get("row_id") or "").strip()
    start = _parse_projection_datetime(value.get("start") or value.get("period_start") or value.get("begin") or value.get("from"), None)
    end = _parse_projection_datetime(value.get("end") or value.get("period_end") or value.get("finish") or value.get("to"), None)
    if not start:
        return None
    if not end or end <= start:
        end = start + timedelta(minutes=30)
    if mode == "days":
        row_id = _projection_day_key(start)
    return {
        "id": row_id,
        "start": _projection_datetime_iso(start),
        "end": _projection_datetime_iso(end),
        "color": str(value.get("color") or value.get("background") or ""),
        "caption": str(value.get("caption") or value.get("title") or ""),
    }


def _projection_object_schedule_payload(repo: models.Repo, projection_uid: str, object_uid: str, mode: str = "resources", index: int = 0) -> Optional[Dict[str, Any]]:
    obj = _projection_object_payload(repo, projection_uid, object_uid)
    if not obj:
        return None
    val = _normalize_schedule_projection_value(obj.get("projection_value"), projection_uid, mode, index)
    if not val:
        return None
    obj["schedule"] = val
    return obj


def _normalize_gantt_tasks(value: Any) -> List[Dict[str, Any]]:
    raw = _projection_read_jsonish(value, [])
    if isinstance(raw, dict):
        raw = raw.get("tasks") or raw.get("items") or []
    out: List[Dict[str, Any]] = []
    seen = set()
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            tid = str(item.get("id") or item.get("task_id") or item.get("key") or "").strip()
            if not tid or tid in seen:
                continue
            seen.add(tid)
            out.append({
                "id": tid,
                "caption": str(item.get("caption") or item.get("title") or item.get("name") or tid),
                "parent": str(item.get("parent") or item.get("parent_id") or "").strip(),
                "color": str(item.get("color") or item.get("background") or ""),
            })
    return out


def _normalize_gantt_projection_value(value: Any, index: int = 0) -> Optional[Dict[str, Any]]:
    value = _projection_read_jsonish(value, value)
    if not isinstance(value, dict):
        return None
    task_id = str(value.get("id") or value.get("task_id") or value.get("row_id") or "").strip()
    start = _parse_projection_datetime(value.get("start") or value.get("period_start") or value.get("begin") or value.get("from"), None)
    end = _parse_projection_datetime(value.get("end") or value.get("period_end") or value.get("finish") or value.get("to"), None)
    if not start:
        return None
    if not end or end <= start:
        end = start + timedelta(days=1)
    return {
        "id": task_id,
        "start": _projection_datetime_iso(start),
        "end": _projection_datetime_iso(end),
        "title": str(value.get("title") or value.get("caption") or ""),
        "parent": str(value.get("parent") or value.get("parent_id") or "").strip(),
        "color": str(value.get("color") or value.get("background") or ""),
    }


def _projection_object_gantt_payload(repo: models.Repo, projection_uid: str, object_uid: str, index: int = 0) -> Optional[Dict[str, Any]]:
    obj = _projection_object_diagram_payload(repo, projection_uid, object_uid)
    if not obj:
        return None
    val = _normalize_gantt_projection_value(obj.get("projection_value"), index)
    if not val:
        return None
    if not val.get("title"):
        val["title"] = str((obj.get("view") or {}).get("title") or obj.get("id") or "")
    obj["gantt"] = val
    return obj


def _save_projection_object_data(obj_repo: models.Repo, cls_name: str, internal_id: str, data: Dict[str, Any], user_modification: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(data or {})
    if isinstance(user_modification, dict) and user_modification:
        payload["_user_modification"] = user_modification
    base_url = (obj_repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")
    if not base_url or base_url == current:
        parsed_ctx = get_parsed_config(obj_repo, models.db) or {}
        tokens = _nodes_mod.set_runtime_context(obj_repo.config_uid, parsed_ctx)
        try:
            node = _node_local_update_data(obj_repo.config_uid, cls_name, internal_id, data, user_modification=user_modification)
            return _collect_runtime_messages_payload(node)
        finally:
            _nodes_mod.reset_runtime_context(tokens)

    url = obj_repo.base_url.rstrip("/") + f"/api/config/{obj_repo.config_uid}/node/{cls_name}/{internal_id}"
    resp = requests.put(url, json=payload, auth=_auth_tuple(obj_repo), timeout=20)
    resp.raise_for_status()
    try:
        remote_payload = resp.json()
    except Exception:
        remote_payload = None
    if isinstance(remote_payload, dict) and remote_payload.get("status") is False:
        err_payload = remote_payload.get("data") if isinstance(remote_payload.get("data"), dict) else remote_payload
        raise _nodes_mod.AcceptRejected(err_payload)
    if isinstance(remote_payload, dict):
        out: Dict[str, Any] = {}
        if isinstance(remote_payload.get("message"), dict):
            out["message"] = remote_payload.get("message")
        if isinstance(remote_payload.get("messages"), list):
            out["messages"] = remote_payload.get("messages")
            if "message" not in out and out["messages"]:
                out["message"] = out["messages"][-1]
        return out
    return {}

def _resolve_projection_object(repo: models.Repo, object_uid: str) -> Tuple[models.Repo, str, str]:
    try:
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(object_uid)
    except Exception:
        cfg_uid, cls_name, internal_id = None, None, None
    cls_name = str(cls_name or "").strip()
    internal_id = str(internal_id or "").strip()
    if not cls_name or not internal_id:
        raise ValueError("bad object uid")
    return _repo_for_config_uid(repo, cfg_uid or repo.config_uid), cls_name, internal_id

@client_bp.route("/api/nodalayout/render", methods=["POST"])
@login_required
def api_nodalayout_render():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "")
    node_id = str(j.get("node_id") or "")
    layout = j.get("layout")
    data = j.get("data")

    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400
    if layout is None:
        return jsonify({"ok": False, "error": "layout required"}), 400
    if data is None or not isinstance(data, dict):
        data = {}

    repo = _get_repo_or_404(repo_id)
    try:
        html = render_nodalayout_html(
            layout,
            data,
            assets_base_dir=_userfiles_dir_for_repo(repo),
            context=_nl_context(repo, class_name=class_name, node_id=node_id),
        )
        return jsonify({"ok": True, "layout_html": html or ""})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@client_bp.route("/api/node/save", methods=["POST"])
@login_required
def api_node_save():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "")
    node_id = str(j.get("node_id") or "")
    data = j.get("data") or {}

    if not repo_id or not class_name or not node_id or not isinstance(data, dict):
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    cfg_uid = repo.config_uid
    parsed_ctx = get_parsed_config(repo, models.db) or {}
    cls_cfg = (parsed_ctx.get("classes") or {}).get(class_name) or {}
    data = _strip_projection_runtime_fields_for_save(cls_cfg, data)
    is_custom_process = _is_singleton_class_type(cls_cfg)
    _ctx_tokens = _nodes_mod.set_runtime_context(cfg_uid, parsed_ctx)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp


    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if not base_url or base_url == current:
            if is_custom_process:
                # custom_process nodes are singletons; create on first save
                node_id = _node_local_upsert_custom_process(cfg_uid, class_name, node_id, data)
            else:
                _node_local_update_data(cfg_uid, class_name, node_id, data)
            # Optional: register in default room after save (Migration tab)
            reg_count = 0
            room_uid = ""
            try:
                parsed = get_parsed_config(repo, models.db) or {}
                cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
                if bool(cls_cfg.get("migration_register_on_save")):
                    room_uid = _resolve_class_default_room_uid(parsed, cls_cfg)
                    if room_uid:
                        reg_count = _register_nodes_to_room_local(cfg_uid, class_name, room_uid, [node_id])
            except Exception:
                reg_count = 0

            out = {"ok": True}
            if reg_count:
                out["registered"] = reg_count
                out["room_uid"] = room_uid
                out["message"] = {"text": f"Registered in Room: {room_uid}", "level": "success"}
            # attach runtime messages (from nodes.push_message)
            try:
                msgs = getattr(_nodes_mod, "RUNTIME_MESSAGES", None)
                msgs = msgs.get() if msgs else []
                if isinstance(msgs, list) and msgs:
                    out.setdefault("messages", msgs)
                    out.setdefault("message", msgs[-1])
            except Exception:
                pass
            return jsonify(out)
        # remote
        if is_custom_process:
            node_id = _normalize_custom_process_uid(cfg_uid, class_name, node_id)
        _api_post_remote(
            repo,
            f"/api/config/{cfg_uid}/node/{class_name}/{node_id}/save",
            {"_data": data},
        )
        msgs = getattr(_nodes_mod, "RUNTIME_MESSAGES", None)
        msgs = msgs.get() if msgs else []
        if isinstance(msgs, list) and msgs:
            last = msgs[-1]
            if isinstance(last, dict) and last.get("level") == "error":
                last["level"] = "danger"
            return jsonify({"ok": True, "message": last if isinstance(last, dict) else {"text": str(last), "level": "info"}, "messages": msgs})
        return jsonify({"ok": True})
    except _nodes_mod.AcceptRejected as e:

        payload = getattr(e, 'payload', None) or {}

        msg = payload.get('message')

        # fallback to runtime messages if handler used nodes.push_message
        if not isinstance(msg, dict):
            try:
                msgs = getattr(_nodes_mod, "RUNTIME_MESSAGES", None)
                msgs = msgs.get() if msgs else []
                if isinstance(msgs, list) and msgs:
                    msg = msgs[-1]
            except Exception:
                msg = None

        if not isinstance(msg, dict):
            msg = {'text': payload.get('error') or 'Save rejected', 'level': 'danger'}
        # normalize level
        if msg.get('level') == 'error':
            msg['level'] = 'danger'

        return jsonify({'ok': False, 'error': payload.get('error') or 'rejected', 'message': msg}), 200

    except Exception as e:
        return jsonify({"ok": False, 
                        "error": str(e),
                        "message": {"text": f"Handler error: {e}", "level": "error"},
                        }), 500


@client_bp.route("/api/projection/kanban/data", methods=["POST"])
@login_required
def api_projection_kanban_data():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "").strip()
    node_id = str(j.get("node_id") or "").strip()

    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
    if not _is_projection_class_type(cls_cfg):
        return jsonify({"ok": False, "error": "class is not a projection"}), 400

    projection_type = str(cls_cfg.get("projection_type") or PROJECTION_KANBAN_TYPE).strip() or PROJECTION_KANBAN_TYPE
    if projection_type != PROJECTION_KANBAN_TYPE:
        return jsonify({"ok": False, "error": "only kanban_projection is implemented"}), 400

    projection_uid = _normalize_custom_process_uid(repo.config_uid, class_name, node_id)
    data = _get_projection_node_data(repo, cls_cfg, class_name, projection_uid)

    # Raw-node projection handlers return updated projection _data to the browser,
    # but the normal projection data endpoint reads from repo storage. Accept the
    # just-returned projection contract fields from the client so Generate works
    # for raw embedded classes as well.
    client_data = j.get("node_data")
    if isinstance(client_data, dict):
        for k in ("_projection_objects", "_projection_timer", "_projection_uid", "_projection_type", "_kanban_columns"):
            if k in client_data:
                data[k] = client_data.get(k)
        _apply_projection_defaults_to_data(cls_cfg, data, repo.config_uid, class_name, projection_uid)

    columns = data.get("_kanban_columns") if isinstance(data.get("_kanban_columns"), list) else _parse_projection_kanban_columns(cls_cfg)
    object_ids = _normalize_projection_object_ids(data.get("_projection_objects"))

    objects = []
    for uid in object_ids:
        obj = _projection_object_payload(repo, projection_uid, uid)
        if obj:
            objects.append(obj)

    return jsonify({
        "ok": True,
        "projection_uid": projection_uid,
        "projection_type": projection_type,
        "columns": columns,
        "objects": objects,
        "node_data": data,
        "timer": _normalize_projection_timer(data.get("_projection_timer")),
        "empty_column": {"id": "__empty__", "caption": "No column"},
    })



@client_bp.route("/api/projection/diagram/data", methods=["POST"])
@login_required
def api_projection_diagram_data():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "").strip()
    node_id = str(j.get("node_id") or "").strip()

    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
    if not _is_projection_class_type(cls_cfg):
        return jsonify({"ok": False, "error": "class is not a projection"}), 400

    projection_type = str(cls_cfg.get("projection_type") or PROJECTION_KANBAN_TYPE).strip() or PROJECTION_KANBAN_TYPE
    if projection_type != PROJECTION_DIAGRAM_TYPE:
        return jsonify({"ok": False, "error": "class is not a diagram_projection"}), 400

    projection_uid = _normalize_custom_process_uid(repo.config_uid, class_name, node_id)
    data = _get_projection_node_data(repo, cls_cfg, class_name, projection_uid)

    # Same raw-node bridge as kanban: use contract fields returned by the event
    # handler before reading objects for visualization.
    client_data = j.get("node_data")
    if isinstance(client_data, dict):
        for k in ("_projection_objects", "_projection_timer", "_projection_uid", "_projection_type", "_projection_header", "_projection_editor"):
            if k in client_data:
                data[k] = client_data.get(k)
        _apply_projection_defaults_to_data(cls_cfg, data, repo.config_uid, class_name, projection_uid)

    object_ids = _normalize_projection_object_ids(data.get("_projection_objects"))

    objects = []
    for idx, uid in enumerate(object_ids):
        obj = _projection_object_diagram_payload(repo, projection_uid, uid)
        if not obj:
            continue
        # Diagram objects are linked only when this projection has a dict-like
        # value in the object's _projection_values. Clearing that value is the
        # non-destructive "unlink" operation.
        obj["diagram"] = _normalize_diagram_projection_value(obj.get("projection_value"), idx)
        objects.append(obj)

    return jsonify({
        "ok": True,
        "projection_uid": projection_uid,
        "projection_type": projection_type,
        "objects": objects,
        "node_data": data,
        "header": str(data.get("_projection_header") or ""),
        "editor": _boolish_projection_value(data.get("_projection_editor")),
        "timer": _normalize_projection_timer(data.get("_projection_timer")),
    })




@client_bp.route("/api/projection/schedule/data", methods=["POST"])
@login_required
def api_projection_schedule_data():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "").strip()
    node_id = str(j.get("node_id") or "").strip()
    selected_date = str(j.get("selected_date") or "").strip()

    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
    if not _is_projection_class_type(cls_cfg):
        return jsonify({"ok": False, "error": "class is not a projection"}), 400

    projection_type = str(cls_cfg.get("projection_type") or PROJECTION_KANBAN_TYPE).strip() or PROJECTION_KANBAN_TYPE
    if projection_type != PROJECTION_SCHEDULE_TYPE:
        return jsonify({"ok": False, "error": "class is not a schedule_projection"}), 400

    projection_uid = _normalize_custom_process_uid(repo.config_uid, class_name, node_id)
    data = _get_projection_node_data(repo, cls_cfg, class_name, projection_uid)

    client_data = j.get("node_data")
    if isinstance(client_data, dict):
        for k in ("_projection_objects", "_projection_timer", "_projection_uid", "_projection_type", "_projection_columns", "_projection_period_start", "_projection_period_end", "_projection_id", "_projection_header", "_projection_orientation", "_projection_create_class", "_projection_default_interval_hours"):
            if k in client_data:
                data[k] = client_data.get(k)
        _apply_projection_defaults_to_data(cls_cfg, data, repo.config_uid, class_name, projection_uid)

    now = datetime.now().replace(second=0, microsecond=0)
    default_start = now.replace(hour=8, minute=0)
    default_end = now.replace(hour=18, minute=0)
    period_start = _parse_projection_datetime(data.get("_projection_period_start"), default_start) or default_start
    period_end = _parse_projection_datetime(data.get("_projection_period_end"), default_end) or default_end
    if period_end <= period_start:
        period_end = period_start + timedelta(hours=10)

    # For resource schedule, selected date switches the day but keeps configured hours.
    sel_dt = _parse_projection_datetime(selected_date, None)
    if sel_dt:
        day = sel_dt.date()
        duration = period_end - period_start
        period_start = datetime.combine(day, period_start.time())
        period_end = period_start + duration

    mode, columns, projection_id = _normalize_schedule_columns(data.get("_projection_columns"), period_start, period_end, selected_date)
    if not projection_id:
        projection_id = str(data.get("_projection_id") or "").strip()

    objects = []
    for idx, uid in enumerate(_normalize_projection_object_ids(data.get("_projection_objects"))):
        obj = _projection_object_schedule_payload(repo, projection_uid, uid, mode, idx)
        if obj:
            objects.append(obj)

    orientation = _normalize_projection_orientation(data.get("_projection_orientation"))
    default_interval_hours = _projection_schedule_default_interval_hours(data)
    create_class = _projection_schedule_create_class(data)

    return jsonify({
        "ok": True,
        "projection_uid": projection_uid,
        "projection_type": projection_type,
        "mode": mode,
        "projection_id": projection_id,
        "columns": columns,
        "objects": objects,
        "node_data": data,
        "header": str(data.get("_projection_header") or ""),
        "projection_orientation": orientation,
        "projection_create_class": create_class,
        "projection_default_interval_hours": default_interval_hours,
        "slot_minutes": max(1, int(round(default_interval_hours * 60))),
        "period_start": _projection_datetime_iso(period_start),
        "period_end": _projection_datetime_iso(period_end),
        "selected_date": _projection_day_key(period_start),
        "timer": _normalize_projection_timer(data.get("_projection_timer")),
    })


@client_bp.route("/api/projection/schedule/move", methods=["POST"])
@login_required
def api_projection_schedule_move():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    projection_uid = str(j.get("projection_uid") or "").strip()
    object_uid = str(j.get("object_uid") or "").strip()
    fields = j.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    if not repo_id or not projection_uid or not object_uid:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    try:
        obj_repo, cls_name, internal_id = _resolve_projection_object(repo, object_uid)
        data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
        if not isinstance(data, dict):
            data = {}
        vals = data.get("_projection_values")
        if not isinstance(vals, dict):
            vals = {}
        existing = None
        for key in _projection_key_aliases(projection_uid):
            if key in vals:
                existing = vals.get(key)
                break
        value = _projection_read_jsonish(existing, {})
        if not isinstance(value, dict):
            value = {}
        if "id" in fields or "row_id" in fields:
            value["id"] = str(fields.get("id") or fields.get("row_id") or "")
        if "start" in fields:
            value["start"] = _projection_datetime_iso(_parse_projection_datetime(fields.get("start"), None)) or str(fields.get("start") or "")
        if "end" in fields:
            value["end"] = _projection_datetime_iso(_parse_projection_datetime(fields.get("end"), None)) or str(fields.get("end") or "")
        vals[projection_uid] = value
        for key in _projection_key_aliases(projection_uid)[1:]:
            vals.pop(key, None)
        data["_projection_values"] = vals
        save_meta = _save_projection_object_data(obj_repo, cls_name, internal_id, data, {
            "source": "projection",
            "projection_type": PROJECTION_SCHEDULE_TYPE,
            "projection_uid": projection_uid,
            "action": "move",
            "object_uid": object_uid,
            "fields": fields,
        })
    except _nodes_mod.AcceptRejected as e:
        return jsonify(_projection_accept_error_payload(e)), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "message": {"text": str(e), "level": "danger"}}), 200
    return jsonify(_projection_move_success_payload(save_meta))


@client_bp.route("/api/projection/gantt/data", methods=["POST"])
@login_required
def api_projection_gantt_data():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "").strip()
    node_id = str(j.get("node_id") or "").strip()
    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
    if not _is_projection_class_type(cls_cfg):
        return jsonify({"ok": False, "error": "class is not a projection"}), 400
    projection_type = str(cls_cfg.get("projection_type") or PROJECTION_KANBAN_TYPE).strip() or PROJECTION_KANBAN_TYPE
    if projection_type != PROJECTION_GANTT_TYPE:
        return jsonify({"ok": False, "error": "class is not a gantt_projection"}), 400

    projection_uid = _normalize_custom_process_uid(repo.config_uid, class_name, node_id)
    data = _get_projection_node_data(repo, cls_cfg, class_name, projection_uid)
    client_data = j.get("node_data")
    if isinstance(client_data, dict):
        for k in ("_projection_objects", "_projection_timer", "_projection_uid", "_projection_type", "_projection_tasks", "_projection_period_start", "_projection_period_end", "_projection_header", "_projection_scale"):
            if k in client_data:
                data[k] = client_data.get(k)
        _apply_projection_defaults_to_data(cls_cfg, data, repo.config_uid, class_name, projection_uid)

    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    period_start = _parse_projection_datetime(data.get("_projection_period_start"), now) or now
    period_end = _parse_projection_datetime(data.get("_projection_period_end"), period_start + timedelta(days=30)) or (period_start + timedelta(days=30))
    if period_end <= period_start:
        period_end = period_start + timedelta(days=30)

    tasks = _normalize_gantt_tasks(data.get("_projection_tasks") or data.get("tasks"))
    objects = []
    generated = {t["id"]: t for t in tasks}
    for idx, uid in enumerate(_normalize_projection_object_ids(data.get("_projection_objects"))):
        obj = _projection_object_gantt_payload(repo, projection_uid, uid, idx)
        if not obj:
            continue
        gv = obj.get("gantt") or {}
        tid = str(gv.get("id") or obj.get("id") or "").strip()
        if not tid:
            tid = str(obj.get("id") or "")
            gv["id"] = tid
        if tid and tid not in generated:
            generated[tid] = {
                "id": tid,
                "caption": str(gv.get("title") or tid),
                "parent": str(gv.get("parent") or ""),
                "color": str(gv.get("color") or ""),
            }
        objects.append(obj)
    tasks = list(generated.values())

    return jsonify({
        "ok": True,
        "projection_uid": projection_uid,
        "projection_type": projection_type,
        "tasks": tasks,
        "objects": objects,
        "node_data": data,
        "header": str(data.get("_projection_header") or ""),
        "period_start": _projection_datetime_iso(period_start),
        "period_end": _projection_datetime_iso(period_end),
        "scale": str(data.get("_projection_scale") or "day"),
        "timer": _normalize_projection_timer(data.get("_projection_timer")),
    })


@client_bp.route("/api/projection/gantt/move", methods=["POST"])
@login_required
def api_projection_gantt_move():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    projection_uid = str(j.get("projection_uid") or "").strip()
    object_uid = str(j.get("object_uid") or "").strip()
    fields = j.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    if not repo_id or not projection_uid or not object_uid:
        return jsonify({"ok": False, "error": "bad args"}), 400
    repo = _get_repo_or_404(repo_id)
    try:
        obj_repo, cls_name, internal_id = _resolve_projection_object(repo, object_uid)
        data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
        if not isinstance(data, dict):
            data = {}
        vals = data.get("_projection_values")
        if not isinstance(vals, dict):
            vals = {}
        existing = None
        for key in _projection_key_aliases(projection_uid):
            if key in vals:
                existing = vals.get(key)
                break
        value = _projection_read_jsonish(existing, {})
        if not isinstance(value, dict):
            value = {}
        if "id" in fields or "task_id" in fields:
            value["id"] = str(fields.get("id") or fields.get("task_id") or "")
        if "parent" in fields:
            value["parent"] = str(fields.get("parent") or "")
        if "start" in fields:
            value["start"] = _projection_datetime_iso(_parse_projection_datetime(fields.get("start"), None)) or str(fields.get("start") or "")
        if "end" in fields:
            value["end"] = _projection_datetime_iso(_parse_projection_datetime(fields.get("end"), None)) or str(fields.get("end") or "")
        vals[projection_uid] = value
        for key in _projection_key_aliases(projection_uid)[1:]:
            vals.pop(key, None)
        data["_projection_values"] = vals
        save_meta = _save_projection_object_data(obj_repo, cls_name, internal_id, data, {
            "source": "projection",
            "projection_type": PROJECTION_GANTT_TYPE,
            "projection_uid": projection_uid,
            "action": "move",
            "object_uid": object_uid,
            "fields": fields,
        })
    except _nodes_mod.AcceptRejected as e:
        return jsonify(_projection_accept_error_payload(e)), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "message": {"text": str(e), "level": "danger"}}), 200
    return jsonify(_projection_move_success_payload(save_meta))

@client_bp.route("/api/projection/kanban/move", methods=["POST"])
@login_required
def api_projection_kanban_move():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    projection_uid = str(j.get("projection_uid") or "").strip()
    object_uid = str(j.get("object_uid") or "").strip()
    column_id = str(j.get("column_id") or "").strip() or "__empty__"

    if not repo_id or not projection_uid or not object_uid:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    try:
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(object_uid)
    except Exception:
        cfg_uid, cls_name, internal_id = None, None, None
    cls_name = str(cls_name or "").strip()
    internal_id = str(internal_id or "").strip()
    if not cls_name or not internal_id:
        return jsonify({"ok": False, "error": "bad object uid"}), 400

    obj_repo = _repo_for_config_uid(repo, cfg_uid or repo.config_uid)
    data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
    if not isinstance(data, dict):
        data = {}
    vals = data.get("_projection_values")
    if not isinstance(vals, dict):
        vals = {}
    if column_id == "__empty__":
        for key in _projection_key_aliases(projection_uid):
            vals.pop(key, None)
    else:
        vals[projection_uid] = column_id
        for key in _projection_key_aliases(projection_uid)[1:]:
            vals.pop(key, None)
    data["_projection_values"] = vals

    try:
        save_meta = _save_projection_object_data(obj_repo, cls_name, internal_id, data, {
            "source": "projection",
            "projection_type": PROJECTION_KANBAN_TYPE,
            "projection_uid": projection_uid,
            "action": "move",
            "object_uid": object_uid,
            "column_id": column_id,
        })
    except _nodes_mod.AcceptRejected as e:
        return jsonify(_projection_accept_error_payload(e)), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "message": {"text": str(e), "level": "danger"}}), 200

    return jsonify(_projection_move_success_payload(save_meta))


@client_bp.route("/api/projection/diagram/move", methods=["POST"])
@login_required
def api_projection_diagram_move():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    projection_uid = str(j.get("projection_uid") or "").strip()
    object_uid = str(j.get("object_uid") or "").strip()
    action = str(j.get("action") or "move").strip().lower()
    coords = j.get("coords") or {}
    fields = j.get("fields") or {}
    if not isinstance(coords, dict):
        coords = {}
    if not isinstance(fields, dict):
        fields = {}

    if not repo_id or not projection_uid or not object_uid:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    try:
        cfg_uid, cls_name, internal_id = _nodes_mod.parse_uid_any(object_uid)
    except Exception:
        cfg_uid, cls_name, internal_id = None, None, None
    cls_name = str(cls_name or "").strip()
    internal_id = str(internal_id or "").strip()
    if not cls_name or not internal_id:
        return jsonify({"ok": False, "error": "bad object uid"}), 400

    obj_repo = _repo_for_config_uid(repo, cfg_uid or repo.config_uid)
    data = _fetch_node_data_for_repo(obj_repo, cls_name, internal_id) or {}
    if not isinstance(data, dict):
        data = {}

    vals = data.get("_projection_values")
    if not isinstance(vals, dict):
        vals = {}

    if action == "unlink":
        for key in _projection_key_aliases(projection_uid):
            vals.pop(key, None)
        data["_projection_values"] = vals
    else:
        existing = None
        for key in _projection_key_aliases(projection_uid):
            if key in vals:
                existing = vals.get(key)
                break
        value = _normalize_diagram_projection_value(existing, 0)

        source = fields if action == "update" else coords
        if action == "move" and not source and fields:
            source = fields
        if not isinstance(source, dict):
            source = {}

        for key in ("x1", "y1", "x2", "y2"):
            if key in source:
                try:
                    value[key] = float(source.get(key))
                except Exception:
                    pass

        if action == "update":
            if "figure" in source:
                figure = str(source.get("figure") or "rectangle").strip().lower()
                if figure not in {"rectangle", "circle", "svg"}:
                    figure = "rectangle"
                value["figure"] = figure
            if "background" in source:
                bg = str(source.get("background") or "#ffffff").strip()
                if not re.match(r"^#[0-9a-fA-F]{3}([0-9a-fA-F]{3})?$", bg):
                    bg = "#ffffff"
                value["background"] = bg
            if "text" in source:
                value["text"] = str(source.get("text") or "")
            if "svg" in source:
                value["svg"] = str(source.get("svg") or "")

        # Keep the shape valid after manual editing.
        try:
            if float(value.get("x2", 0)) <= float(value.get("x1", 0)):
                value["x2"] = float(value.get("x1", 0)) + 20
            if float(value.get("y2", 0)) <= float(value.get("y1", 0)):
                value["y2"] = float(value.get("y1", 0)) + 20
        except Exception:
            pass

        vals[projection_uid] = value
        for key in _projection_key_aliases(projection_uid)[1:]:
            vals.pop(key, None)
        data["_projection_values"] = vals

    try:
        save_meta = _save_projection_object_data(obj_repo, cls_name, internal_id, data, {
            "source": "projection",
            "projection_type": PROJECTION_DIAGRAM_TYPE,
            "projection_uid": projection_uid,
            "action": action,
            "object_uid": object_uid,
            "fields": fields,
            "coords": coords,
        })
    except _nodes_mod.AcceptRejected as e:
        return jsonify(_projection_accept_error_payload(e)), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "message": {"text": str(e), "level": "danger"}}), 200

    return jsonify(_projection_move_success_payload(save_meta))


@client_bp.route("/api/node/register", methods=["POST"])
@login_required
def api_node_register():
    """Register one node in a room defined in class Migration tab."""
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "")
    node_id = str(j.get("node_id") or "")
    # Allow override, but default is from config
    room_uid_req = str(j.get("room_uid") or "").strip()
    room_alias_req = str(j.get("room_alias") or "").strip()

    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)
    cfg_uid = repo.config_uid

    room_uid = room_uid_req
    if not room_uid and room_alias_req:
        try:
            parsed = get_parsed_config(repo, models.db) or {}
            rooms_map = (parsed.get("rooms") or {}) if isinstance(parsed, dict) else {}
            room_uid = str((rooms_map or {}).get(room_alias_req) or "").strip()
        except Exception:
            room_uid = ""

    if not room_uid:
        try:
            parsed = get_parsed_config(repo, models.db) or {}
            cls_cfg = (parsed.get("classes") or {}).get(class_name) or {}
            room_uid = _resolve_class_default_room_uid(parsed, cls_cfg)
        except Exception:
            room_uid = ""

    if not room_uid:
        return jsonify({"ok": False, "error": "Room not specified"}), 400

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        # Local register (preferred: avoids HTTP self-calls)
        if not base_url or base_url == current:
            cnt = _register_nodes_to_room_local(cfg_uid, class_name, room_uid, [node_id])
            if not cnt:
                return jsonify({"ok": False, "error": "node not found or registration failed"}), 404
            return jsonify({
                "ok": True,
                "count": cnt,
                "room_uid": room_uid,
                "message": {"text": f"Registered in the room: {room_uid}", "level": "success"}
            })

        # Remote repo: fall back to remote API (no local deadlocks)
        _api_post_remote(repo, f"/api/config/{cfg_uid}/node/{class_name}/register/{room_uid}", json_data=[node_id])
        return jsonify({
            "ok": True,
            "count": 1,
            "room_uid": room_uid,
            "message": {"text": f"Registered in the room: {room_uid}", "level": "success"}
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "message": {"text": str(e), "level": "danger"}}), 500


# @client_bp.route("/node/<int:repo_id>/<path:class_name>/<path:node_id>")
# @login_required
# def node_view(repo_id: int, class_name: str, node_id: str):
#     repo = _get_repo_or_404(repo_id)
#     class_cfg = _get_class_cfg(repo, class_name) or {}
#     use_std = bool(class_cfg.get("use_standard_commands"))

#     base_url = (repo.base_url or "").strip().rstrip("/")
#     current = (request.host_url or "").rstrip("/")

#     data = {}
#     try:
#         if not base_url or base_url == current:
#             data = _node_local_get_data(repo.config_uid, class_name, node_id)
#         else:
#             payload = _api_get_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}/{node_id}")
#             data = (payload or {}).get("_data") or {}
#     except Exception:
#         data = {}

#     data_json = "{}"
#     try:
#         data_json = json.dumps(data or {}, ensure_ascii=False, indent=2)
#     except Exception:
#         data_json = "{}"    

#     return render_template(
#         "client/node.html",
#         title=f"{APP_TITLE} — {class_name}/{node_id}",
#         repo=repo,
#         repo_id=repo.id,
#         class_name=class_name,
#         node_id=node_id,
#         data=data or {},
#         use_standard_commands=use_std,
#         data_json=data_json,
#     )


@client_bp.route("/section/<path:section_code>")
@login_required
def section_view(section_code: str):
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    if not repos:
        return redirect(url_for("client.sections_home"))

    code = "" if section_code == "__empty__" else section_code
    sections = _with_received_nodes_section(build_global_sections(repos, models.db))
    sec_name = "<...>" if code == "" else next((s["name"] for s in sections if s["code"] == code), code)

    return render_template(
        "client/section.html",
        title=f"{APP_TITLE} — {sec_name}",
        repos=repos,
        sections=sections,
        section_code=code,
        section_name=sec_name,
        auto_refresh=AUTO_REFRESH_SECONDS,
        no_repos=(len(repos) == 0),
    )


@client_bp.route("/api/available_configs")
@login_required
def api_available_configs():
 
    Configuration = main.Configuration
    UserConfigAccess = getattr(main, "UserConfigAccess", None)

    
    own_cfgs = models.db.session.execute(
        select(Configuration).where(Configuration.user_id == current_user.id)
    ).scalars().all()

    
    shared_cfgs = []
    if UserConfigAccess is not None:
        shared_cfgs = models.db.session.execute(
            select(Configuration)
            .join(UserConfigAccess, UserConfigAccess.config_id == Configuration.id)
            .where(UserConfigAccess.user_id == current_user.id)
        ).scalars().all()

    # merge unique by uid
    by_uid = {}
    for c in list(own_cfgs) + list(shared_cfgs):
        try:
            by_uid[c.uid] = c
        except Exception:
            pass

    cfgs = list(by_uid.values())

    out = []
    for c in cfgs:
        out.append({
            "uid": c.uid,
            "name": c.name or "",
            "vendor": c.vendor or "",
            "version": getattr(c, "version", "") or "",
            "server_name": getattr(c, "server_name", "") or "",
            "last_modified": c.last_modified.isoformat() if getattr(c, "last_modified", None) else "",
        })
    
    out.sort(key=lambda x: (x["name"].lower(), x["uid"]))
    return jsonify(out)

# ---------- Repos management ----------

@client_bp.route("/repos")
@login_required
def repos_manage():
    repos = models.Repo.query.filter_by(user_id=current_user.id).order_by(models.Repo.id.desc()).all()
    return render_template("client/repos.html", title=f"{APP_TITLE} — Репозитории", repos=repos)


@client_bp.route("/settings", methods=["GET", "POST"])
@login_required
def client_settings():
    """Client settings page.

    Currently:
      - scanner_ws_url: ws://127.0.0.1:8765
      - scanner_ws_enabled: on/off
    """
    if request.method == "POST":
        ws_url = (request.form.get("scanner_ws_url") or "").strip()
        enabled = "1" if (request.form.get("scanner_ws_enabled") in ("1", "on", "true", "True")) else "0"
        _set_setting("scanner_ws_url", ws_url)
        _set_setting("scanner_ws_enabled", enabled)
        flash("Settings saved", "success")
        return redirect(url_for("client.client_settings"))

    return render_template(
        "client/settings.html",
        title=f"{APP_TITLE} — Settings",
        scanner_ws_url=_get_setting("scanner_ws_url", "ws://127.0.0.1:8765"),
        scanner_ws_enabled=(_get_setting("scanner_ws_enabled", "1") not in ("0", "false", "False", "no", "off")),
    )


@client_bp.route("/repos/add", methods=["POST"])
@login_required
def repos_add():
    config_url = (request.form.get("config_url") or "").strip()

    try:
        base_url, config_uid, normalized_url = parse_config_url(config_url)
    except Exception as e:
        flash(f"Invalid ref: {e}", "error")
        return redirect(url_for("client.repos_manage"))

    try:
        current = (request.host_url or "").rstrip("/")

        if base_url.rstrip("/") == current:
            # Local config on this server: enforce access list
            if hasattr(main, "user_can_access_config") and not main.user_can_access_config(current_user, config_uid):
                flash("You do not have access to this configuration.", "error")
                return redirect(url_for("client.repos_manage"))
            
            cfg = fetch_config_from_local_db(config_uid)
        else:
            cfg = fetch_config(normalized_url)
    except Exception as e:
        flash(f"Failed to read configuration: {e}", "error")
        return redirect(url_for("client.repos_manage"))

    except Exception as e:
        flash(f"Failed to read configuration: {e}", "error")
        return redirect(url_for("client.repos_manage"))

    vendor = cfg.get("vendor") or cfg.get("provider") or ""
    version = cfg.get("version") or ""
    display_name = cfg.get("display_name") or cfg.get("name") or ""
    name = display_name or f"{base_url} · {config_uid[:8]}"

    r = models.Repo(
        user_id=current_user.id,
        name=name,
        base_url="",  # default empty -> use current server
        config_uid=config_uid,
        config_url=normalized_url,
        vendor=vendor,
        version=version,
        display_name=display_name,
        username="",
        password="",
        config_json=json.dumps(cfg, ensure_ascii=False),
        config_cached_at=datetime.now(timezone.utc),
    )

    models.db.session.add(r)
    models.db.session.commit()

    row = models.RepoConfig.query.filter_by(repo_id=r.id).first()
    if not row:
        row = models.RepoConfig(repo_id=r.id, config_json=json.dumps(cfg, ensure_ascii=False))
        models.db.session.add(row)
    else:
        row.config_json = json.dumps(cfg, ensure_ascii=False)
    row.updated_at = datetime.now(timezone.utc)
    models.db.session.commit()

    CONFIG_MEM.pop(r.id, None)
    return redirect(url_for("client.repos_manage"))

@client_bp.route("/repos/add_local", methods=["POST"])
@login_required
def repos_add_local():
    config_uid = (request.form.get("config_uid") or "").strip()
    if not config_uid:
        flash("config_uid not selected", "error")
        return redirect(url_for("client.repos_manage"))

    # enforce access list for local configs
    if hasattr(main, "user_can_access_config") and not main.user_can_access_config(current_user, config_uid):
        flash("You do not have access to this configuration.", "error")
        return redirect(url_for("client.repos_manage"))

    
    exists = models.Repo.query.filter_by(user_id=current_user.id, config_uid=config_uid).first()
    if exists:
        flash("This configuration has already been added to the repository.", "info")
        return redirect(url_for("client.repos_manage"))

    try:
        cfg = fetch_config_from_local_db(config_uid)
    except Exception as e:
        flash(f"Failed to read configuration from database: {e}", "error")
        return redirect(url_for("client.repos_manage"))

    normalized_url = (request.host_url or "").rstrip("/") + f"/api/config/{config_uid}"
    vendor = cfg.get("vendor") or cfg.get("provider") or ""
    version = cfg.get("version") or ""
    display_name = cfg.get("display_name") or cfg.get("name") or ""
    name = display_name or f"local · {config_uid[:8]}"

    r = models.Repo(
        user_id=current_user.id,
        name=name,
        base_url="",  
        config_uid=config_uid,
        config_url=normalized_url,
        vendor=vendor,
        version=version,
        display_name=display_name,
        username="",
        password="",
        config_json=json.dumps(cfg, ensure_ascii=False),
        config_cached_at=datetime.now(timezone.utc),
    )

    models.db.session.add(r)
    models.db.session.commit()

    # cache table
    row = models.RepoConfig.query.filter_by(repo_id=r.id).first()
    if not row:
        row = models.RepoConfig(repo_id=r.id, config_json=json.dumps(cfg, ensure_ascii=False))
        models.db.session.add(row)
    else:
        row.config_json = json.dumps(cfg, ensure_ascii=False)
    row.updated_at = datetime.now(timezone.utc)
    models.db.session.commit()

    CONFIG_MEM.pop(r.id, None)
    flash("Configuration added from the current server", "success")
    return redirect(url_for("client.repos_manage"))

@client_bp.route("/repos/<int:repo_id>/remove", methods=["POST"])
@login_required
def repos_remove(repo_id: int):
    r = models.Repo.query.get_or_404(repo_id)
    if r.user_id != current_user.id:
        abort(403)
    models.db.session.delete(r)
    models.db.session.commit()
    CONFIG_MEM.pop(r.id, None)
    return redirect(url_for("client.repos_manage"))


@client_bp.route("/repos/<int:repo_id>/update_api", methods=["POST"])
@login_required
def repo_update_api(repo_id: int):
    repo = models.Repo.query.get_or_404(repo_id)
    if repo.user_id != current_user.id:
        abort(403)

    repo.base_url = request.form.get("base_url") or ""
    repo.username = request.form.get("username") or ""
    repo.password = request.form.get("password") or ""

    models.db.session.commit()
    flash("API parameters saved", "success")
    return redirect(url_for("client.repos_manage"))


@client_bp.route("/repos/refresh_all", methods=["POST"])
@login_required
def repos_refresh_all():
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    ok, fail = 0, 0

    for r in repos:
        try:
             
            current = (request.host_url or "").rstrip("/")
            cfg_url = (r.config_url or "").strip()
            is_local = cfg_url.startswith(current + "/api/config/") or (not r.base_url)

            if is_local:
                cfg = fetch_config_from_local_db(r.config_uid)
            else:
                cfg = fetch_config(r.config_url)

            row = models.RepoConfig.query.filter_by(repo_id=r.id).first()
            if not row:
                row = models.RepoConfig(repo_id=r.id, config_json=json.dumps(cfg, ensure_ascii=False))
                models.db.session.add(row)
            else:
                row.config_json = json.dumps(cfg, ensure_ascii=False)
            row.updated_at = datetime.now(timezone.utc)

            r.config_json = json.dumps(cfg, ensure_ascii=False)
            r.config_cached_at = datetime.now(timezone.utc)

            models.db.session.commit()
            CONFIG_MEM.pop(r.id, None)
            ok += 1
        except Exception:
            fail += 1

    flash(f"Configurations have been updated: ok={ok}, fail={fail}", "success" if fail == 0 else "info")
    #return redirect(url_for("client.repos_manage"))
    return jsonify({"ok": True, "message": "Configurations have been updated"}), 200


def _parse_display_image_table(spec: str, data: Dict[str, Any]) -> Tuple[List[str], Dict[str, str]]:

    spec = (spec or "").strip()
    if not spec:
        return [], {}

    parts = [p.strip() for p in spec.split(",") if p.strip()]
    headers: List[str] = []
    values: Dict[str, str] = {}


    for p in parts:
        if "|" in p:
            title, expr = p.split("|", 1)
        else:
            
            chunks = p.split(None, 1)
            title = chunks[0]
            expr = chunks[1] if len(chunks) > 1 else ""

        title = (title or "").strip()
        expr = (expr or "").strip()

        if not title:
            continue

        headers.append(title)

        if expr.startswith("@"):
            key = expr[1:]
            v = (data or {}).get(key)
            values[title] = "" if v is None else (v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        else:
            values[title] = expr

    return headers, values

# ---------- API ----------


@client_bp.route("/api/section_data")
@login_required
def api_section_data():
    section_code = request.args.get("section_code", "")
    q = (request.args.get("q") or "").strip().lower()
    index_name = (request.args.get("index_name") or "").strip()
    index_value = request.args.get("index_value")
    tag_filter = (request.args.get("tag") or "").strip()

    if section_code == RAW_NODES_SECTION_CODE:
        items, meta = _build_raw_node_items(q=q)
        return jsonify({
            "ok": True,
            "items": items,
            "count": len(items),
            "nl_css": DEFAULT_NL_CSS,
            "meta": meta,
        })

    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    merged: List[Dict[str, Any]] = []
    any_desc = False

    classes_ui: List[Dict[str, Any]] = []
    std_map: Dict[Tuple[int, str], bool] = {}            # (repo_id, class)->use_standard_commands
    display_name_map: Dict[Tuple[int, str], str] = {}    # (repo_id, class)->display_name
    commands_map: Dict[Tuple[int, str], str] = {}        # (repo_id, class)->commands string

    table_headers: List[str] = []
    table_headers_set: set = set()
    filter_indexes_map: Dict[str, Dict[str, Any]] = {}
    tag_filter_map: Dict[str, Dict[str, str]] = {}

    def remember_tags(data: Dict[str, Any], enabled: bool = True) -> List[str]:
        # _tags are global UI metadata now: collect and render them regardless of
        # the legacy show_tag_cloud class flag.
        tags = _normalize_node_tags(data)
        for tag in tags:
            tid = str(tag.get("id") or "")
            if tid and tid not in tag_filter_map:
                tag_filter_map[tid] = tag
        return [str(tag.get("id") or "") for tag in tags if str(tag.get("id") or "")]

    def parse_table_spec(spec: str) -> List[Tuple[str, str, bool]]:
        """
        "Title|@field,Title2|value" -> [(Title, field, True), (Title2, 'value', False)]
        If no '|': treat whole token as Title, value=''
        """
        out = []
        for raw in (spec or "").split(","):
            raw = (raw or "").strip()
            if not raw:
                continue
            if "|" in raw:
                t, v = raw.split("|", 1)
                t = (t or "").strip()
                v = (v or "").strip()
            else:
                t, v = raw, ""
            is_field = v.startswith("@")
            field = v[1:].strip() if is_field else v
            out.append((t, field, is_field))
        return out

    def build_table_values(spec: str, data: dict) -> Tuple[List[str], Dict[str, str]]:
        specs = parse_table_spec(spec)
        headers = []
        values = {}
        for title, key_or_val, is_field in specs:
            title = title or ""
            headers.append(title)
            if is_field:
                v = data.get(key_or_val)
                if v is None:
                    s = ""
                else:
                    try:
                        s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
                    except Exception:
                        s = str(v)
            else:
                s = key_or_val or ""
            values[title] = s
        return headers, values

    def build_cover_html(cover_web_layout: Any, cover_layout: Any, data: Dict[str, Any], class_name: str, node_id: str, show_tags: bool = False) -> str:
        """Cover renderer with per-node override via _data['_cover'].

        Priority:
        1) _cover in data
        2) display_image_web
        3) cover_image
        """
        assets_base_dir = _userfiles_dir_for_repo(repo)
        nl_context = _nl_context(repo, class_name=class_name, node_id=node_id)

        # 1) _cover override
        try:
            cov = data.get("_cover") if isinstance(data, dict) else None
            if cov:
                if isinstance(cov, (dict, list)):
                    return _cover_with_tags(_wrap_client_tpl_html(str(render_nodalayout_html(cov, data, assets_base_dir=assets_base_dir, context=nl_context) or ""), data), data, show_tags)
                if isinstance(cov, str):
                    s = cov.strip()
                    if s.startswith("[") or s.startswith("{"):
                        return _cover_with_tags(_wrap_client_tpl_html(str(render_nodalayout_html(s, data, assets_base_dir=assets_base_dir, context=nl_context) or ""), data), data, show_tags)
                    pic_layout = [[{"type": "Picture", "value": s, "width": -1}]]
                    return _cover_with_tags(_wrap_client_tpl_html(str(render_nodalayout_html(pic_layout, data, assets_base_dir=assets_base_dir, context=nl_context) or ""), data), data, show_tags)
        except Exception:
            pass

        # 2) class cover layouts (existing)
        try:
            layout_to_use = cover_web_layout if str(cover_web_layout or "").strip() else cover_layout
            if layout_to_use is not None and isinstance(data, dict):
                _fill_nodeinput_views(repo, parsed, layout_to_use, data)
            return _cover_with_tags(_wrap_client_tpl_html(str(render_nodalayout_html(layout_to_use, data, assets_base_dir=assets_base_dir, context=nl_context) or ""), data), data, show_tags)
        except Exception:
            return _render_tags_html(data)

    start_menu_cmds_ui: List[Dict[str, Any]] = []
    timers_ui: List[Dict[str, Any]] = []

    for repo in repos:
        parsed = get_parsed_config(repo, models.db)
        if not parsed:
            continue

        cfg_obj = (parsed.get("cfg") or {}) if isinstance(parsed, dict) else {}
        for timer in (cfg_obj.get("Timers") or cfg_obj.get("timers") or []):
            if not isinstance(timer, dict) or not _timer_bool(timer.get("active"), True):
                continue
            if _timer_runtime(timer) != "client":
                continue
            timer_id = _timer_id(timer)
            period_seconds = _timer_period_seconds(timer)
            worker = _timer_bool(timer.get("worker"), False)
            if timer_id and period_seconds > 0:
                timers_ui.append({
                    "repo_id": repo.id,
                    "repo": repo.name,
                    "timer_id": timer_id,
                    "period_seconds": period_seconds,
                    "worker": worker,
                    "runtime": "client",
                })

        classes_by_section = parsed["classes_by_section"]
        cls_in_section = classes_by_section.get(section_code, []) if section_code != "" else classes_by_section.get("", [])

        cls_in_section = [c for c in (cls_in_section or []) if not bool(c.get("hidden")) and not _is_print_form_class_type(c)]

        sec_cmds = ""
        for s in (parsed.get("sections") or []):
            if (s.get("code") or "") == section_code:
                sec_cmds = (s.get("commands") or "").strip()
                break

        if sec_cmds:
            for raw in sec_cmds.split(","):
                raw = (raw or "").strip()
                if not raw:
                    continue
                if "|" in raw:
                    t, k = raw.split("|", 1)
                else:
                    t, k = raw, raw
                title = (t or "").strip()
                key = (k or "").strip()
                if not title or not key:
                    continue
                start_menu_cmds_ui.append({
                    "repo_id": repo.id,
                    "repo": repo.name,
                    "title": title,
                    "key": key,
                })

        
        for c in cls_in_section:
            cn = (c.get("name") or "").strip()
            if not cn:
                continue
            use_std = bool(c.get("use_standard_commands"))
            disp = (c.get("display_name") or cn).strip()
            cmds = (c.get("commands") or "").strip()

            std_map[(repo.id, cn)] = use_std
            display_name_map[(repo.id, cn)] = disp
            commands_map[(repo.id, cn)] = cmds
            for idx in (c.get("indexes") or []):
                if not isinstance(idx, dict) or not idx.get("filter_enabled"):
                    continue
                iname = str(idx.get("name") or "").strip()
                if not iname:
                    continue
                cur = filter_indexes_map.get(iname)
                item = {
                    "name": iname,
                    "kind": str(idx.get("kind") or "hash_index"),
                    "keys": str(idx.get("keys") or ""),
                    "filter_type": str(idx.get("filter_type") or "string"),
                    "filter_label": str(idx.get("filter_label") or "").strip(),
                    "filter_list_enabled": bool(idx.get("filter_list_enabled")),
                    "classes": [],
                }
                if not cur:
                    filter_indexes_map[iname] = item
                    cur = item
                if cn not in cur["classes"]:
                    cur["classes"].append(cn)

            classes_ui.append({
                "repo": repo.name,
                "repo_id": repo.id,
                "class": cn,
                "display_name": disp,
                "use_standard_commands": use_std,
                "commands": cmds,
                "repo_uid": repo.config_uid,
                "class_type": _class_type_value(c),
                "projection_type": str(c.get("projection_type") or ""),
            })

        # items
        for c in cls_in_section:
            cn = c.get("name")
            if not cn:
                continue

            ctype = c.get("class_type") or "data_node"

            cover_layout = c.get("cover_image")
            cover_web_layout = c.get("display_image_web") or ""
            cover_table_layout = c.get("display_image_table") or ""

            # custom_process
            if _is_singleton_class_type(ctype):
                #node_id = f"{repo.config_uid}${cn}"
                node_id = _nodes_mod.normalize_own_uid(repo.config_uid, cn, "singleton")
                data = (c.get("_data") or {}).copy()
                try:
                    node_class = _load_server_node_class(repo.config_uid, cn)
                    node = node_class.get(node_id, repo.config_uid)
                    if node:
                        saved = node.get_data() or {}
                        if isinstance(saved, dict):
                            data.update(saved)   # сохранённое поверх дефолта
                except Exception:
                    pass

                
                _apply_projection_defaults_to_data(c, data, repo.config_uid, cn, node_id)
                data.setdefault("_id", node_id)

                if isinstance(data, dict) and data.get("_hidden"):
                    continue

                if q:
                    sidx = data.get("_search_index")
                    if isinstance(sidx, str):
                        if q not in sidx.lower():
                            continue
                    else:
                        try:
                            if q not in json.dumps(data, ensure_ascii=False).lower():
                                continue
                        except Exception:
                            continue

                item_tag_ids = remember_tags(data, bool(c.get("show_tag_cloud")))
                if tag_filter and tag_filter not in item_tag_ids:
                    continue

                # cover html for web (priority: display_image_web else cover_image)
                display_image_html = ""
                display_image_html = build_cover_html(cover_web_layout, cover_layout, data, cn, node_id, bool(c.get("show_tag_cloud")))
                # try:
                #     if (cover_web_layout or "").strip():
                #         display_image_html = str(render_nodalayout_html(cover_web_layout, data))
                #     else:
                #         display_image_html = str(render_nodalayout_html(cover_layout, data))
                # except Exception:
                #     display_image_html = ""

                # table values (display_image_table spec)
                tv_headers, tv = build_table_values(cover_table_layout, data)
                for h in tv_headers:
                    if h not in table_headers_set:
                        table_headers_set.add(h)
                        table_headers.append(h)

                merged.append({
                    "repo": repo.name,
                    "repo_id": repo.id,
                    "class": cn,
                    "id": node_id,
                    "data": data,
                    "class_obj": c,
                    "is_custom_process": True,
                    "is_projection": _is_projection_class_type(c),
                    "projection_type": str(c.get("projection_type") or ""),
                    "display_image_html": display_image_html,
                    "tags": _normalize_node_tags(data),
                    "table_values": tv,
                    "use_standard_commands": bool(std_map.get((repo.id, cn), False)),
                    "repo_uid": repo.config_uid,
                })

                if "_sort_string_desc" in data:
                    any_desc = True
                continue

            # data_node
            nodes = _fetch_nodes_for_class(repo, config_uid=repo.config_uid, class_name=cn, q=q, limit=DEFAULT_LIMIT_PER_CLASS, index_name=index_name, index_value=index_value)
            for n in nodes:
                data = n.get("_data") or {}
                node_id = n.get("_id") or data.get("_id") or ""

                if isinstance(data, dict) and data.get("_hidden"):
                    continue

                item_tag_ids = remember_tags(data, bool(c.get("show_tag_cloud")))
                if tag_filter and tag_filter not in item_tag_ids:
                    continue

                # cover html for web (priority: display_image_web else cover_image)
                display_image_html = ""
                display_image_html = build_cover_html(cover_web_layout, cover_layout, data, cn, node_id, bool(c.get("show_tag_cloud")))
                # try:
                #     if (cover_web_layout or "").strip():
                #         display_image_html = str(render_nodalayout_html(cover_web_layout, data))
                #     else:
                #         display_image_html = str(render_nodalayout_html(cover_layout, data))
                # except Exception:
                #     display_image_html = ""

                # table values (display_image_table spec)
                tv_headers, tv = build_table_values(cover_table_layout, data)
                for h in tv_headers:
                    if h not in table_headers_set:
                        table_headers_set.add(h)
                        table_headers.append(h)

                merged.append({
                    "repo": repo.name,
                    "repo_id": repo.id,
                    "class": cn,
                    "id": node_id,
                    "data": data,
                    "class_obj": c,
                    "is_custom_process": False,
                    "is_projection": False,
                    "projection_type": "",
                    "display_image_html": display_image_html,
                    "tags": _normalize_node_tags(data),
                    "table_values": tv,
                    "use_standard_commands": bool(std_map.get((repo.id, cn), False)),
                    "repo_uid": repo.config_uid,
                })

                if "_sort_string_desc" in data:
                    any_desc = True

    def sort_key(it: Dict[str, Any]) -> str:
        d = it.get("data") or {}
        if "_sort_string_desc" in d:
            return str(d.get("_sort_string_desc") or "")
        if "_sort_string" in d:
            return str(d.get("_sort_string") or "")
        return it.get("id") or ""

    merged.sort(key=sort_key, reverse=any_desc)

    return jsonify({
        "ok": True,
        "items": merged,
        "count": len(merged),
        "nl_css": DEFAULT_NL_CSS,
        "meta": {
            "classes_ui": classes_ui,
            "table_headers": table_headers,
            "start_menu_cmds_ui": start_menu_cmds_ui,
            "timers_ui": timers_ui,
            "filter_indexes": list(filter_indexes_map.values()),
            "tag_filter": list(tag_filter_map.values()),
            "selected_tag": tag_filter,
        }
    })



@client_bp.route("/api/node/create", methods=["POST"])
@login_required
def api_node_create():
    payload = request.get_json(force=True) or {}
    repo_id = int(payload.get("repo_id") or 0)
    class_name = (payload.get("class_name") or "").strip()
    initial_data = payload.get("data") or payload.get("initial_data") or {}
    if not isinstance(initial_data, dict):
        initial_data = {}
    if not repo_id or not class_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    # deny bulk delete for custom_process classes (they are virtual, not deletable)
    parsed = get_parsed_config(repo, models.db)
    try:
        cmeta = (parsed or {}).get("classes", {}).get(class_name) if isinstance(parsed, dict) else None
        if isinstance(cmeta, dict) and _is_singleton_class_type(cmeta):
            return jsonify({"ok": False, "error": "singleton process cannot be deleted"}), 400
    except Exception:
        pass

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if not base_url or base_url == current:
            
            node_id = _node_local_create(repo.config_uid, class_name, initial_data=initial_data)
        else:
            j = _api_post_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}", json_data=initial_data)
            node_id = None
            if isinstance(j, dict):
                node_id = (j.get("_id") or (j.get("_data") or {}).get("_id"))
            if not node_id:
                return jsonify({"ok": False, "error": "create: no node_id"}), 500

        return jsonify({"ok": True, "node_id": node_id, "config_uid": repo.config_uid})
    except Exception as e:
        return jsonify({"ok": False, 
                        "error": str(e),
                        "message": {"text": f"Handler error: {e}", "level": "error"},
                        }), 200


@client_bp.route("/api/node_delete", methods=["POST"])
@login_required
def api_node_delete():
    payload = request.get_json(force=True) or {}
    repo_id = int(payload.get("repo_id") or 0)
    class_name = (payload.get("class_name") or "").strip()
    node_id = (payload.get("node_id") or "").strip()
    if not repo_id or not class_name or not node_id:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    # custom_process nodes are virtual and cannot be deleted
    parsed = get_parsed_config(repo, models.db)
    try:
        cmeta = (parsed or {}).get("classes", {}).get(class_name) if isinstance(parsed, dict) else None
        if isinstance(cmeta, dict) and _is_singleton_class_type(cmeta):
            return jsonify({"ok": False, "error": "singleton process cannot be deleted"}), 400
    except Exception:
        pass

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if not base_url or base_url == current:
            
            _node_local_delete(repo.config_uid, class_name, node_id)
        else:
            _api_delete_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}/{node_id}")

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, 
                        "error": str(e),
                        "message": {"text": f"Handler error: {e}", "level": "error"},
                        }), 200


@client_bp.route("/api/node/bulk_delete", methods=["POST"])
@login_required
def api_node_bulk_delete():
    payload = request.get_json(force=True) or {}
    repo_id = int(payload.get("repo_id") or 0)
    class_name = (payload.get("class_name") or "").strip()
    ids = payload.get("ids") or []
    if not repo_id or not class_name or not isinstance(ids, list):
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    cfg_uid = repo.config_uid
    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    deleted = 0
    errors = []

    for node_id in ids:
        node_id = (str(node_id) or "").strip()
        if not node_id:
            continue
        try:
            if not base_url or base_url == current:
                _node_local_delete(cfg_uid, class_name, node_id)
            else:
                _api_delete_remote(repo, f"/api/config/{cfg_uid}/node/{class_name}/{node_id}")
            deleted += 1
        except Exception as e:
            errors.append({"id": node_id, 
                           "error": str(e),
                           "message": {"text": f"Handler error: {e}", "level": "error"},
                           })

    return jsonify({"ok": True, "deleted": deleted, "errors": errors})



def _client_request_actor_for_external_api():
    """Resolve the user for external client API calls.

    Browser sessions can use the normal Flask-Login session. External clients
    (Android, curl, service integrations) can use HTTP Basic auth with the same
    account that has API access enabled.
    """
    try:
        if getattr(current_user, "is_authenticated", False):
            return current_user
    except Exception:
        pass

    auth = request.authorization
    if auth:
        check_api_auth = getattr(main, "check_api_auth", None)
        try:
            user = check_api_auth(auth.username, auth.password) if callable(check_api_auth) else None
        except Exception:
            user = None
        if user and bool(getattr(user, "can_api", False)):
            return user
        if user:
            abort(403)
    abort(401)


def _get_repo_by_config_uid_for_actor(config_uid: str, actor) -> models.Repo:
    config_uid = str(config_uid or "").strip()
    if not config_uid:
        abort(400)
    repo = models.Repo.query.filter_by(config_uid=config_uid, user_id=int(getattr(actor, "id", 0) or 0)).first()
    if not repo:
        abort(404)
    return repo


def _parse_external_print_form_request(j: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    """Parse JSON body for external PrintForm routes.

    Preferred body:
      {"print_form_class": "<config_uid>$<PrintFormClass>", "_data": {...}}

    Also accepts aliases for easier integrations:
      form_class, class_uid, print_form_uid, config_uid + class_name,
      data instead of _data.
    """
    j = j if isinstance(j, dict) else {}
    form_uid = str(
        j.get("print_form_class")
        or j.get("form_class")
        or j.get("class_uid")
        or j.get("print_form_uid")
        or ""
    ).strip()
    config_uid = str(j.get("config_uid") or "").strip()
    class_name = str(j.get("class_name") or j.get("print_class_name") or "").strip()

    if form_uid and "$" in form_uid:
        config_uid, class_name = form_uid.split("$", 1)
        config_uid = config_uid.strip()
        class_name = class_name.strip()
    elif form_uid and not class_name:
        class_name = form_uid

    data = j.get("_data")
    if data is None:
        data = j.get("data")
    if data is None:
        data = {}
    if not isinstance(data, dict):
        abort(400)

    if not config_uid or not class_name:
        abort(400)
    return config_uid, class_name, dict(data)


def _external_print_form_pdf_bytes(config_uid: str, class_name: str, data: Dict[str, Any], actor) -> Tuple[bytes, str, str]:
    repo = _get_repo_by_config_uid_for_actor(config_uid, actor)
    parsed = get_parsed_config(repo, models.db)
    if not parsed:
        abort(404)
    print_cls = ((parsed or {}).get("classes") or {}).get(class_name) or {}
    if not print_cls or not _is_print_form_class_type(print_cls):
        abort(404)

    # External API mode must behave like opening a PrintForm from a normal node:
    # the supplied _data is the original/base document, not the prepared print data.
    # It is injected into _basement_data, then onInputWeb/onInput + listener=onStartForm
    # is executed. The handler fills the final PrintForm _data, and only then the
    # HTML+Jinja template is rendered to PDF.
    base_data = dict(data or {})
    base_class_name = str(
        base_data.get("_class")
        or base_data.get("class_name")
        or base_data.get("_schema_class_name")
        or "ExternalDocument"
    ).strip() or "ExternalDocument"
    base_node_id = str(base_data.get("_id") or base_data.get("id") or f"external_{uuid.uuid4().hex[:12]}").strip()
    base_data.setdefault("_id", base_node_id)
    if base_class_name != "ExternalDocument":
        base_data.setdefault("_class", base_class_name)

    print_node_id = _print_form_node_id(repo.config_uid, class_name, base_class_name, base_node_id)

    _ctx_tokens = _nodes_mod.set_runtime_context(config_uid, parsed)
    try:
        print_data = _execute_print_form_start_handler(
            repo, parsed, print_cls, print_node_id, base_class_name, base_node_id, base_data
        )
        print_html = _render_print_html(repo, print_cls, print_data)
    finally:
        _nodes_mod.reset_runtime_context(_ctx_tokens)

    try:
        from weasyprint import HTML
        pdf_bytes = HTML(string=print_html, base_url=request.host_url).write_pdf()
    except Exception as e:
        raise RuntimeError(f"PDF export error: {e}")

    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{config_uid}_{class_name}_{base_node_id}.pdf")[:180]
    return pdf_bytes, safe_name, print_html


def _send_pdf_to_raw_printer(pdf_bytes: bytes, printer_name: str, printer_port: Optional[int] = None, timeout: int = 20) -> Dict[str, Any]:
    dest = str(printer_name or "").strip()
    if not dest:
        raise ValueError("printer_name is required for raw printing")

    # Direct device path, for example /dev/usb/lp0.
    if dest.startswith("/dev/") or dest.startswith("/tmp/"):
        with open(dest, "wb") as f:
            f.write(pdf_bytes)
        return {"destination": dest, "bytes": len(pdf_bytes), "mode": "device"}

    raw = dest
    if raw.startswith("tcp://"):
        raw = raw[len("tcp://"):]
    if "://" in raw:
        raise ValueError("raw printer_name must be host:port, tcp://host:port, host, or a device path")

    host = raw
    port = int(printer_port or 9100)
    if raw.startswith("[") and "]" in raw:
        # Minimal IPv6 bracket notation: [addr]:9100
        end = raw.find("]")
        host = raw[1:end]
        tail = raw[end + 1:]
        if tail.startswith(":") and tail[1:]:
            port = int(tail[1:])
    elif ":" in raw:
        host, port_s = raw.rsplit(":", 1)
        if port_s.strip():
            port = int(port_s)

    host = host.strip()
    if not host:
        raise ValueError("raw printer host is empty")
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(pdf_bytes)
    return {"destination": f"{host}:{port}", "bytes": len(pdf_bytes), "mode": "socket"}


def _send_pdf_to_cups_printer(pdf_bytes: bytes, printer_name: str, timeout: int = 60) -> Dict[str, Any]:
    printer = str(printer_name or "").strip()
    if not printer:
        raise ValueError("printer_name is required for CUPS printing")
    with tempfile.NamedTemporaryFile(prefix="nodalogic_printform_", suffix=".pdf", delete=True) as tmp:
        tmp.write(pdf_bytes)
        tmp.flush()
        cmd = ["lp", "-d", printer, tmp.name]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "CUPS lp failed").strip()
        raise RuntimeError(err)
    return {"destination": printer, "bytes": len(pdf_bytes), "mode": "cups", "lp_output": (proc.stdout or "").strip()}


@client_bp.route("/api/print-form/pdf", methods=["POST"])
def api_external_print_form_pdf():
    actor = _client_request_actor_for_external_api()
    j = request.get_json(force=True, silent=True) or {}
    config_uid, class_name, data = _parse_external_print_form_request(j)
    try:
        pdf_bytes, safe_name, _ = _external_print_form_pdf_bytes(config_uid, class_name, data, actor)
    except RuntimeError as e:
        return Response(str(e), status=500, mimetype="text/plain")

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = f'attachment; filename="{safe_name}"'
    return resp


@client_bp.route("/api/print-form/print", methods=["POST"])
def api_external_print_form_print():
    actor = _client_request_actor_for_external_api()
    j = request.get_json(force=True, silent=True) or {}
    config_uid, class_name, data = _parse_external_print_form_request(j)

    printer_name = str(j.get("printer_name") or j.get("printer") or "").strip()
    printer_type = str(j.get("printer_type") or "raw").strip().lower()
    if printer_type not in ("raw", "cups"):
        return jsonify({"ok": False, "error": "printer_type must be raw or cups"}), 400
    try:
        printer_port = j.get("printer_port")
        printer_port = int(printer_port) if printer_port not in (None, "") else None
    except Exception:
        return jsonify({"ok": False, "error": "printer_port must be an integer"}), 400

    try:
        pdf_bytes, safe_name, _ = _external_print_form_pdf_bytes(config_uid, class_name, data, actor)
        if printer_type == "cups":
            print_result = _send_pdf_to_cups_printer(pdf_bytes, printer_name)
        else:
            print_result = _send_pdf_to_raw_printer(pdf_bytes, printer_name, printer_port=printer_port)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "filename": safe_name, "printer_type": printer_type, "print": print_result})


@client_bp.route("/print-form/<int:repo_id>/<path:print_class_name>")
@login_required
def print_form_open(repo_id: int, print_class_name: str):
    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db)
    if not parsed:
        abort(404)

    base_class_name = (request.args.get("base_class") or request.args.get("base_class_name") or "").strip()
    base_node_id = (request.args.get("base_node_id") or request.args.get("node_id") or "").strip()
    if not base_class_name or not base_node_id:
        abort(400)

    _ctx_tokens = _nodes_mod.set_runtime_context(repo.config_uid, parsed)
    try:
        print_cls, print_node_id, print_html, print_data = _build_print_form_runtime(
            repo, parsed, print_class_name, base_class_name, base_node_id
        )
    finally:
        _nodes_mod.reset_runtime_context(_ctx_tokens)
    try:
        data_json = json.dumps(print_data if isinstance(print_data, dict) else {}, ensure_ascii=False, indent=2)
    except Exception:
        data_json = "{}"

    pdf_url = url_for(
        "client.print_form_pdf",
        repo_id=repo.id,
        print_class_name=print_class_name,
        base_class=base_class_name,
        base_node_id=base_node_id,
    )

    return render_template(
        "client/node_form.html",
        title=f"{print_class_name} — {base_class_name}/{base_node_id}",
        node_id=print_node_id,
        discussion_node_id=print_node_id,
        class_name=print_class_name,
        repo=repo,
        repo_id=repo.id,
        error="",
        layout_html="",
        print_html=print_html,
        print_pdf_url=pdf_url,
        node_data=print_data,
        data_json=data_json,
        use_standard_commands=False,
        has_onshowweb=False,
        api_event_web=url_for("client.api_node_event_web"),
        api_save_url=url_for("client.api_node_save"),
        api_delete_url=url_for("client.api_node_delete"),
        api_register_url=url_for("client.api_node_register"),
        is_custom_process=False,
        is_projection=False,
        is_print_form=True,
        projection_type="",
        api_projection_kanban_data=url_for("client.api_projection_kanban_data"),
        api_projection_kanban_move=url_for("client.api_projection_kanban_move"),
        api_projection_diagram_data=url_for("client.api_projection_diagram_data"),
        api_projection_diagram_move=url_for("client.api_projection_diagram_move"),
        api_projection_schedule_data=url_for("client.api_projection_schedule_data"),
        api_projection_schedule_move=url_for("client.api_projection_schedule_move"),
        api_projection_gantt_data=url_for("client.api_projection_gantt_data"),
        api_projection_gantt_move=url_for("client.api_projection_gantt_move"),
        show_register_command=False,
        default_room_uid="",
        initial_message=None,
        ui_plugins=[],
        class_obj=print_cls,
        is_raw_node=False,
        print_forms_for_class=[],
    )


@client_bp.route("/print-form/<int:repo_id>/<path:print_class_name>/pdf")
@login_required
def print_form_pdf(repo_id: int, print_class_name: str):
    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db)
    if not parsed:
        abort(404)

    base_class_name = (request.args.get("base_class") or request.args.get("base_class_name") or "").strip()
    base_node_id = (request.args.get("base_node_id") or request.args.get("node_id") or "").strip()
    if not base_class_name or not base_node_id:
        abort(400)

    _ctx_tokens = _nodes_mod.set_runtime_context(repo.config_uid, parsed)
    try:
        _, _, print_html, _ = _build_print_form_runtime(repo, parsed, print_class_name, base_class_name, base_node_id)
    finally:
        _nodes_mod.reset_runtime_context(_ctx_tokens)
    try:
        from weasyprint import HTML
        pdf_bytes = HTML(string=print_html, base_url=request.host_url).write_pdf()
    except Exception as e:
        return Response(f"PDF export error: {escape(str(e))}", status=500, mimetype="text/plain")

    resp = make_response(pdf_bytes)
    resp.headers["Content-Type"] = "application/pdf"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{print_class_name}_{base_class_name}_{base_node_id}.pdf")[:180]
    resp.headers["Content-Disposition"] = f'inline; filename="{safe_name}"'
    return resp


@client_bp.route("/node/<path:config_uid>/<path:class_name>/<path:node_id>")
@login_required
def node_form(config_uid: str, class_name: str, node_id: str):
    repo = _get_repo_by_config_uid_or_404(config_uid)
    if not repo:
        abort(404)

    parsed = get_parsed_config(repo, models.db)
    if not parsed:
        abort(404)

    cls = parsed["classes"].get(class_name)
    if not cls:
        abort(404)

    # class-level PlugIn for Web (stored as JSON text in class.plug_in_web)
    def _parse_plugins(s: str):
        s = (s or "").strip()
        if not s:
            return []
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return obj
            if isinstance(obj, dict):
                return [obj]
        except Exception:
            return []
        return []

    class_plugins_web = _parse_plugins(cls.get("plug_in_web") or "")

    use_std = bool(cls.get("use_standard_commands"))
    is_custom_process = _is_singleton_class_type(cls)
    is_projection = _is_projection_class_type(cls)
    projection_type = str(cls.get("projection_type") or "").strip()
    has_onshowweb = any((ev.get("event") or "") == "onShowWeb" for ev in (cls.get("events") or []))
    print_forms_for_class = _print_forms_for_class(parsed, class_name) if not _is_print_form_class_type(cls) else []

    def actions_for(event_name: str) -> List[Dict[str, Any]]:
        out = []
        for ev in (cls.get("events") or []):
            if ev.get("event") == event_name:
                out.extend(ev.get("actions") or [])
        return out

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    layout = None
    node_data: Dict[str, Any] = {}
    ui_plugins = None

    try:
        # ---------------- REMOTE ----------------
        if base_url and base_url != current:
            for a in actions_for("onShowWeb"):
                m = (a.get("method") or "").strip()
                if not m:
                    continue
                r = _api_post_remote(
                    repo,
                    f"/api/config/{repo.config_uid}/node/{class_name}/{node_id}/{m}",
                    json_data={},
                )
                if isinstance(r, dict) and isinstance(r.get("data"), dict) and ("_ui_layout" in r["data"]):
                    layout = r["data"].get("_ui_layout")

            if is_custom_process:
                # remote custom_process: используем то, что в конфиге
                node_data = (cls.get("_data") or {}).copy()
                _apply_projection_defaults_to_data(cls, node_data, repo.config_uid, class_name, node_id)
                node_data.setdefault("_id", node_id)
                node_data.setdefault("_class", class_name)
            else:
                n = _api_get_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}/{node_id}")
                node_data = (n.get("_data") or {}) if isinstance(n, dict) else {}

            # Remote mode: we can't execute server-side PlugIn(), but we can still
            # expose class-level plug_in_web to the template.
            if class_plugins_web:
                ui_plugins = class_plugins_web

        # ---------------- LOCAL ----------------
        else:
            node_class = _load_server_node_class(repo.config_uid, class_name)

            
            if is_custom_process:
                try:
                    node = node_class.get(node_id, repo.config_uid)
                except Exception:
                    node = None
                if not node:
                    node = node_class(node_id, repo.config_uid)
            else:
                node = node_class.get(node_id, repo.config_uid)
                if not node:
                    abort(404)

            _nodes_mod.CURRENT_NODE = node
            
            stored_data = {}
            try:
                stored_data = node.get_data() or {}
            except Exception:
                stored_data = {}

            if not isinstance(stored_data, dict):
                stored_data = {}

            if is_custom_process:
                
                defaults = (cls.get("_data") or {})
                if not isinstance(defaults, dict):
                    defaults = {}

                merged = stored_data.copy()
                for k, v in defaults.items():
                    merged.setdefault(k, v)

                _apply_projection_defaults_to_data(cls, merged, repo.config_uid, class_name, node_id)
                merged.setdefault("_id", node_id)
                merged.setdefault("_class", class_name)
                node_data = merged

            else:
                node_data = stored_data.copy()

            
            try:
                node._data_cache = node_data.copy()
            except Exception:
                pass

                  
            
            for a in actions_for("onShowWeb"):
                m = (a.get("method") or "").strip()
                if m and hasattr(node, m):
                    getattr(node, m)({})
                    if getattr(node, "_ui_layout", None) is not None:
                        layout = node._ui_layout

            # Class-level PlugIn for web (like calling self.PlugIn(...))
            if class_plugins_web:
                try:
                    if hasattr(node, "PlugIn"):
                        node.PlugIn(class_plugins_web)
                except Exception:
                    pass

            ui_message = getattr(node, "_ui_message", None)
            try:
                if hasattr(node, "_ui_message"):
                    delattr(node, "_ui_message")   # one-shot
            except Exception:
                pass
            
            ui_plugins = getattr(node, "_ui_plugins", None)
            try:
                if hasattr(node, "_ui_plugins"):
                    delattr(node, "_ui_plugins")   # one-shot
            except Exception:
                pass

            
            try:
                if getattr(node, "_data_cache", None) is not None and isinstance(node._data_cache, dict):
                    node_data = node._data_cache.copy()
                else:
                    node_data = node.get_data() or {}
            except Exception:
                pass
            #finally:
            #    _nodes_mod.CURRENT_NODE = None

            
            if getattr(node, "_ui_layout", None) is not None:
                layout = node._ui_layout

    except Exception as e:
        editable_data = node_data if isinstance(node_data, dict) else {}
        try:
            data_json = json.dumps(editable_data, ensure_ascii=False, indent=2)
        except Exception:
            data_json = "{}"
        ui_message = str(e)
        return render_template(
            "client/node_form.html",
            title=f"{class_name}/{node_id}",
            node_id=node_id,
            class_name=class_name,
            repo=repo,
            repo_id=repo.id,
            error=str(e),
            layout_html="",
            node_data=node_data,
            data_json=data_json,
            use_standard_commands=use_std,
            has_onshowweb=has_onshowweb,
            api_event_web=url_for("client.api_node_event_web"),
            api_save_url=url_for("client.api_node_save"),
            api_delete_url=url_for("client.api_node_delete"),
            api_register_url=url_for("client.api_node_register"),
            is_custom_process=is_custom_process,
            is_projection=is_projection,
            is_print_form=False,
            print_html="",
            print_pdf_url="",
            print_forms_for_class=locals().get("print_forms_for_class", []),
            projection_type=projection_type,
            api_projection_kanban_data=url_for("client.api_projection_kanban_data"),
            api_projection_kanban_move=url_for("client.api_projection_kanban_move"),
            api_projection_diagram_data=url_for("client.api_projection_diagram_data"),
            api_projection_diagram_move=url_for("client.api_projection_diagram_move"),
            api_projection_schedule_data=url_for("client.api_projection_schedule_data"),
            api_projection_schedule_move=url_for("client.api_projection_schedule_move"),
            api_projection_gantt_data=url_for("client.api_projection_gantt_data"),
            api_projection_gantt_move=url_for("client.api_projection_gantt_move"),
            show_register_command=bool(cls.get("migration_register_command")) and bool(use_std),
            default_room_uid=_resolve_class_default_room_uid(parsed, cls),
            initial_message=ui_message,
            class_obj=cls,
            
        )

    # --- NEW: default screen layout sources (do NOT break existing Show/onShowWeb) ---
    # Priority: onShowWeb/Show sets `layout` first. If none -> use _data['_layout'], else class.init_screen_layout.
    if layout is None and isinstance(node_data, dict):
        if "_layout" in node_data:
            layout = node_data.get("_layout")
        else:
            layout = (cls.get("init_screen_layout") or "").strip() or None

    # Resolve '^layout_id' via CommonLayouts
    layout = resolve_common_layout(parsed, layout)

    if layout is not None and isinstance(node_data, dict):
        _fill_nodeinput_views(repo, parsed, layout, node_data)


    layout_html = ""
    if layout is not None:
        try:
            layout_html = render_nodalayout_html(
                layout,
                node_data if isinstance(node_data, dict) else {},
                assets_base_dir=_userfiles_dir_for_repo(repo),
                context=_nl_context(repo, class_name=class_name, node_id=node_id),
            )
        except Exception:
            layout_html = ""

    editable_data = node_data if isinstance(node_data, dict) else {}
    try:
        data_json = json.dumps(editable_data, ensure_ascii=False, indent=2)
    except Exception:
        data_json = "{}"

    return render_template(
        "client/node_form.html",
        title=f"{class_name}/{node_id}",
        node_id=node_id,
        class_name=class_name,
        repo=repo,
        repo_id=repo.id,
        error="",
        layout_html=layout_html,
        node_data=node_data,
        data_json=data_json,
        use_standard_commands=use_std,
        has_onshowweb=has_onshowweb,
        api_event_web=url_for("client.api_node_event_web"),
        api_save_url=url_for("client.api_node_save"),
        api_delete_url=url_for("client.api_node_delete"),
        api_register_url=url_for("client.api_node_register"),
        is_custom_process=is_custom_process,
        is_projection=is_projection,
        is_print_form=False,
        print_html="",
        print_pdf_url="",
        print_forms_for_class=locals().get("print_forms_for_class", []),
        projection_type=projection_type,
        api_projection_kanban_data=url_for("client.api_projection_kanban_data"),
        api_projection_kanban_move=url_for("client.api_projection_kanban_move"),
        api_projection_diagram_data=url_for("client.api_projection_diagram_data"),
        api_projection_diagram_move=url_for("client.api_projection_diagram_move"),
            api_projection_schedule_data=url_for("client.api_projection_schedule_data"),
            api_projection_schedule_move=url_for("client.api_projection_schedule_move"),
            api_projection_gantt_data=url_for("client.api_projection_gantt_data"),
            api_projection_gantt_move=url_for("client.api_projection_gantt_move"),
        show_register_command=bool(cls.get("migration_register_command")) and bool(use_std),
        default_room_uid=_resolve_class_default_room_uid(parsed, cls),
        initial_message=ui_message,
        ui_plugins=ui_plugins,
        class_obj=cls,
        is_raw_node=False,
    )


def _parse_plugins_json(s: str):
    s = (s or "").strip()
    if not s:
        return []
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            return [obj]
    except Exception:
        return []
    return []


@client_bp.route("/raw-node/<path:raw_node_id>")
@login_required
def raw_node_form(raw_node_id: str):
    RawNode = _server_model("RawNode")
    if RawNode is None:
        abort(404)

    obj = RawNode.query.filter_by(node_id=str(raw_node_id or "").strip()).first()
    if not obj:
        abort(404)
    if not _current_user_can_access_raw_node(raw_node_id, obj=obj):
        abort(403)

    payload = _raw_node_payload(obj)
    class_name, payload_node_id, node_data = _raw_node_identity(payload, raw_node_id)
    node_id = payload_node_id or str(raw_node_id or "").strip()
    node_data = dict(node_data or {})
    node_data.setdefault("_id", node_id)
    if class_name:
        node_data.setdefault("_class", class_name)
    node_data.setdefault("_raw_node_id", str(raw_node_id or ""))
    download_url = _raw_node_download_ref(payload, raw_node_id)
    event_download_url = _raw_node_download_url(raw_node_id)
    node_data.setdefault("_download_url", download_url)

    repo, parsed, cls = _resolve_raw_node_class(payload, class_name)
    if repo is None:
        repos = models.Repo.query.filter_by(user_id=current_user.id).all()
        repo = repos[0] if repos else None
    if repo is None:
        abort(404)

    cls = cls or {}
    parsed = parsed or get_parsed_config(repo, models.db) or {}
    if isinstance(cls, dict):
        resolved_name = _class_name_from_embedded_class(cls, fallback=class_name, payload=payload)
        if resolved_name:
            class_name = resolved_name
            node_data.setdefault("_class", class_name)
    ui_plugins = _parse_plugins_json(cls.get("plug_in_web") or "") if isinstance(cls, dict) else []
    ui_message = None
    use_std = bool(cls.get("use_standard_commands")) if isinstance(cls, dict) else False
    is_custom_process = _is_singleton_class_type(cls) if isinstance(cls, dict) else False
    is_projection = _is_projection_class_type(cls) if isinstance(cls, dict) else False
    projection_type = str(cls.get("projection_type") or "").strip() if isinstance(cls, dict) else ""
    has_onshowweb = any((ev.get("event") or "") == "onShowWeb" for ev in (cls.get("events") or [])) if isinstance(cls, dict) else False
    layout = None

    # Embedded raw-node classes may contain handlers. Execute onShowWeb through
    # the same download_url/raw-node path used by server-side PythonScript
    # handling, so a raw document behaves like a normal node form instead of a
    # plain JSON viewer.
    if has_onshowweb and _extract_raw_node_class_json(payload):
        try:
            ctx_builder = getattr(main, "resolve_download_url_node_context", None)
            event_runner = getattr(main, "execute_download_url_node_event", None)
            if callable(ctx_builder) and callable(event_runner):
                ctx = ctx_builder(node_id=node_id, fallback_class_name=class_name, download_url=event_download_url)
                event_runner(ctx, "onShowWeb", "", {})
                raw_event_node = ctx.get("node") if isinstance(ctx, dict) else None
                if raw_event_node is not None:
                    try:
                        event_data = raw_event_node.get_data() or {}
                        if isinstance(event_data, dict):
                            node_data = event_data.copy()
                            node_data.setdefault("_id", node_id)
                            node_data.setdefault("_raw_node_id", str(raw_node_id or ""))
                            node_data.setdefault("_download_url", download_url)
                            if class_name:
                                node_data.setdefault("_class", class_name)
                    except Exception:
                        pass
                    if getattr(raw_event_node, "_ui_layout", None) is not None:
                        layout = getattr(raw_event_node, "_ui_layout", None)
                    if getattr(raw_event_node, "_ui_message", None) is not None:
                        ui_message = getattr(raw_event_node, "_ui_message", None)
                    if getattr(raw_event_node, "_ui_plugins", None) is not None:
                        ui_plugins = getattr(raw_event_node, "_ui_plugins", None)
        except Exception as e:
            ui_message = str(e)

    if isinstance(node_data, dict) and "_layout" in node_data:
        layout = layout if layout is not None else node_data.get("_layout")
    elif isinstance(cls, dict):
        layout = layout if layout is not None else ((cls.get("init_screen_layout") or "").strip() or None)

    layout = resolve_common_layout(parsed, layout)
    if layout is not None and isinstance(node_data, dict):
        try:
            _fill_nodeinput_views(repo, parsed, layout, node_data)
        except Exception:
            pass

    layout_html = ""
    if layout is not None:
        try:
            layout_html = render_nodalayout_html(
                layout,
                node_data if isinstance(node_data, dict) else {},
                assets_base_dir=_userfiles_dir_for_repo(repo),
                context=_nl_context(repo, class_name=class_name, node_id=node_id),
            ) or ""
        except Exception:
            layout_html = ""

    try:
        data_json = json.dumps(node_data if isinstance(node_data, dict) else {}, ensure_ascii=False, indent=2)
    except Exception:
        data_json = "{}"

    return render_template(
        "client/node_form.html",
        title=f"{RAW_NODES_SECTION_NAME} — {class_name or 'raw-node'}/{node_id}",
        node_id=node_id,
        class_name=class_name or "raw-node",
        repo=repo,
        repo_id=repo.id,
        error="",
        layout_html=layout_html,
        node_data=node_data,
        data_json=data_json,
        use_standard_commands=use_std,
        has_onshowweb=has_onshowweb,
        api_event_web=url_for("client.api_node_event_web"),
        api_save_url=url_for("client.api_node_save"),
        api_delete_url=url_for("client.api_node_delete"),
        api_register_url=url_for("client.api_node_register"),
        is_custom_process=is_custom_process,
        is_projection=is_projection,
        is_print_form=False,
        print_html="",
        print_pdf_url="",
        print_forms_for_class=locals().get("print_forms_for_class", []),
        projection_type=projection_type,
        api_projection_kanban_data=url_for("client.api_projection_kanban_data"),
        api_projection_kanban_move=url_for("client.api_projection_kanban_move"),
        api_projection_diagram_data=url_for("client.api_projection_diagram_data"),
        api_projection_diagram_move=url_for("client.api_projection_diagram_move"),
            api_projection_schedule_data=url_for("client.api_projection_schedule_data"),
            api_projection_schedule_move=url_for("client.api_projection_schedule_move"),
            api_projection_gantt_data=url_for("client.api_projection_gantt_data"),
            api_projection_gantt_move=url_for("client.api_projection_gantt_move"),
        show_register_command=False,
        default_room_uid="",
        initial_message=ui_message,
        ui_plugins=ui_plugins,
        class_obj=cls,
        is_raw_node=True,
        discussion_node_id=str(raw_node_id or ""),
    )


@client_bp.route("/api/s3/cached-image", methods=["GET"])
@login_required
def api_client_cached_s3_image():
    image_url = str(request.args.get("url") or "").strip()
    if not image_url:
        return jsonify({"ok": False, "error": "image_url_required"}), 400
    if not _is_cacheable_chat_image_url(image_url):
        return jsonify({"ok": False, "error": "unsupported_image_url"}), 400

    downloader = getattr(main, "_runtime_download_bytes_cached", None)
    if not callable(downloader):
        return jsonify({"ok": False, "error": "runtime_cache_unavailable"}), 500

    try:
        data = downloader(image_url, timeout=20)
    except Exception as e:
        return jsonify({"ok": False, "error": "image_cache_failed", "details": str(e)}), 502

    mimetype = _guess_image_mimetype_from_url(image_url)
    resp = Response(data, mimetype=mimetype)
    resp.headers["Cache-Control"] = "private, max-age=86400"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp


@client_bp.route("/api/node-discussion/by-node/<path:node_id>/messages", methods=["GET"])
@login_required
def api_client_node_discussion_messages(node_id: str):
    try:
        getter = getattr(main, "_get_node_discussion_messages_by_node_id", None)
        localize = getattr(main, "_localize_node_discussion_message_times", None)
        tz_name_fn = getattr(main, "_node_discussion_response_timezone_name", None)
        if callable(getter):
            try:
                messages = getter(node_id, viewer_user=current_user)
            except TypeError:
                messages = getter(node_id)
                messages = [m for m in messages if _message_dict_visible_to_current_user(m)]
        else:
            messages = []
        if callable(localize):
            tz_name = tz_name_fn() if callable(tz_name_fn) else None
            messages = [localize(m, tz_name=tz_name) for m in messages]
        return jsonify({"ok": True, "messages": messages, "count": len(messages)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "messages": []}), 500


@client_bp.route("/api/node-discussion/by-node/<path:node_id>/messages", methods=["POST"])
@login_required
def api_client_post_node_discussion_message(node_id: str):
    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        data = {}

    text = data.get("text")
    if text is None:
        text = data.get("message")
    if text is None:
        text = data.get("body")
    image = data.get("image")
    image_url = data.get("image_url")

    if text in (None, "") and image in (None, "") and image_url in (None, ""):
        return jsonify({"ok": False, "error": "text_or_image_required", "node_id": node_id}), 400

    try:
        # The browser is authenticated with Flask-Login; sender_user from JSON
        # is intentionally ignored to prevent spoofing another user.
        sender_user = str(getattr(current_user, "email", "") or "").strip()
        sender_display_name = str(getattr(current_user, "config_display_name", "") or sender_user).strip()

        if not _current_user_can_access_node_discussion(node_id):
            # Allow the first web message only when the raw node itself belongs
            # to/is visible for this user. Existing ordinary-node discussions
            # are allowed by visible history above.
            if not _current_user_can_access_raw_node(node_id):
                return jsonify({"ok": False, "error": "forbidden", "messages": []}), 403

        targets = []
        if hasattr(main, "_find_node_discussion_targets"):
            targets = main._find_node_discussion_targets(node_id, sender_user=sender_user) or []
        if not targets and hasattr(main, "_create_node_discussion_targets_from_request"):
            targets = main._create_node_discussion_targets_from_request(data, node_id, sender_user) or []
        if not targets and hasattr(main, "_find_node_discussion_targets_from_raw_node"):
            targets = main._find_node_discussion_targets_from_raw_node(node_id, sender_user=sender_user) or []

        if not targets:
            return jsonify({
                "ok": False,
                "error": "node_discussion_target_required",
                "details": "No existing discussion target was found. For the first by-node message provide target_user/user_key/target_key/recipient/to/peer, or members/group_id.",
                "node_id": str(node_id or "").strip(),
                "results": [],
                "messages": [],
            }), 400

        if hasattr(main, "_node_discussion_thread_ref"):
            thread_ref = main._node_discussion_thread_ref(node_id, data.get("thread_ref"))
        else:
            thread_ref = str(data.get("thread_ref") or "")

        message_type = "image" if image not in (None, "") or image_url not in (None, "") else "text"
        payload = {
            "type": message_type,
            "thread_type": "node_discussion",
            "thread_ref": thread_ref,
            "node_id": str(node_id or "").strip(),
            "node_uid": str(node_id or "").strip(),
            "text": text or "",
        }
        if image is not None:
            payload["image"] = image
        if image_url is not None:
            payload["image_url"] = image_url
        if sender_user:
            payload["sender_user"] = sender_user
        if sender_display_name:
            payload["sender_display_name"] = sender_display_name

        extra = data.get("data")
        if isinstance(extra, dict):
            payload.update(extra)
            payload["type"] = message_type
            payload.pop("message_type", None)
            payload["thread_type"] = "node_discussion"
            payload["thread_ref"] = thread_ref
            payload["node_id"] = str(node_id or "").strip()
            payload["node_uid"] = str(node_id or "").strip()
        if sender_user:
            payload["sender_user"] = sender_user
        if sender_display_name:
            payload["sender_display_name"] = sender_display_name

        title = data.get("title") or sender_display_name or "Node discussion"
        body = text or data.get("body") or data.get("message") or "New message"

        results = []
        delivery_ok_count = 0
        accepted_count = 0
        saved_messages = []

        for target in targets:
            target_type = target.get("target_type")
            target_id = target.get("target_id")
            item_payload = dict(payload)

            if target_type == "group":
                item_payload["group_id"] = target_id
                result = main.send_message_to_group_global(target_id, title, body, item_payload, sender_user=sender_user)
            elif target_type == "user":
                item_payload["user_key"] = target_id
                result = main.send_message_to_user_global(target_id, title, body, item_payload, sender_user=sender_user)
            else:
                result = {"ok": False, "error": "unsupported_target_type"}

            client_message_id = result.get("client_message_id") if isinstance(result, dict) else None
            history_msg = None
            if client_message_id and hasattr(main, "NodeDiscussionMessage"):
                try:
                    history_msg = main.NodeDiscussionMessage.query.filter_by(client_message_id=client_message_id).first()
                except Exception:
                    history_msg = None

            if not history_msg and client_message_id and hasattr(main, "_save_node_discussion_history_message"):
                try:
                    history_msg = main._save_node_discussion_history_message(
                        node_id=node_id,
                        client_message_id=client_message_id,
                        sender_user=sender_user,
                        sender_display_name=sender_display_name,
                        target_type=target_type,
                        target_id=target_id,
                        text=text or "",
                        image=image,
                        image_url=image_url,
                        payload=item_payload,
                        delivery_status="accepted" if bool((result or {}).get("ok")) else "queued",
                    )
                except Exception:
                    history_msg = None

            if history_msg and hasattr(main, "_serialize_node_discussion_history_message"):
                accepted_count += 1
                saved_messages.append(main._serialize_node_discussion_history_message(history_msg))

            if bool((result or {}).get("ok")):
                delivery_ok_count += 1

            results.append({
                "target_type": target_type,
                "target_id": target_id,
                "client_message_id": client_message_id,
                "ok": bool(history_msg),
                "delivery_ok": bool((result or {}).get("ok")),
                "history_saved": bool(history_msg),
                "result": result,
            })

        return jsonify({
            "ok": accepted_count > 0,
            "node_id": str(node_id or "").strip(),
            "count": len(saved_messages),
            "accepted_count": accepted_count,
            "delivery_ok_count": delivery_ok_count,
            "messages": saved_messages,
            "results": results,
        }), (200 if accepted_count > 0 else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "messages": []}), 500


def _userfiles_root() -> str:
    base = os.path.join(os.path.dirname(__file__), "..", "UserFiles")
    base = os.path.abspath(base)
    try:
        os.makedirs(base, exist_ok=True)
    except Exception:
        pass
    return base


def _userfiles_dir_for_repo(repo) -> str:
    p = os.path.join(_userfiles_root(), str(repo.config_uid))
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass
    return p


def _safe_filename(name: str) -> str:
    name = (name or "").replace("\\", "/")
    name = name.split("/")[-1]
    name = re.sub(r"[^A-Za-z0-9._\- ]+", "_", name)
    return name.strip()[:180]


def _truthy_form_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on", "y", "да")
    return bool(value)


def _public_s3_url_for_key(object_key: str) -> str:
    endpoint = str(getattr(main, "S3_ENDPOINT", "") or "").rstrip("/")
    bucket = str(getattr(main, "S3_BUCKET", "") or "").strip("/")
    if not endpoint or not bucket:
        raise RuntimeError("S3 settings are not configured")
    return f"{endpoint}/{bucket}/{str(object_key).lstrip('/')}"


def _upload_userfile_to_s3(file_storage, *, owner_id: str, filename: str, content_type: str) -> str:
    s3_client = getattr(main, "s3", None)
    bucket = getattr(main, "S3_BUCKET", None)
    if s3_client is None or not bucket:
        raise RuntimeError("S3 client is not configured")
    base_name = _safe_filename(filename) or "file.bin"
    _, ext = os.path.splitext(base_name)
    object_key = f"uploads/client_userfiles/{owner_id or 'user'}/{uuid.uuid4().hex}{ext.lower()}"
    extra = {"ContentType": content_type or "application/octet-stream"}
    file_storage.stream.seek(0)
    s3_client.upload_fileobj(file_storage.stream, bucket, object_key, ExtraArgs=extra)
    try:
        invalidate = getattr(main, "_runtime_cache_invalidate", None)
        if callable(invalidate):
            invalidate(_public_s3_url_for_key(object_key))
    except Exception:
        pass
    return _public_s3_url_for_key(object_key)


@client_bp.route("/api/userfiles/<int:repo_id>/list")
@login_required
def api_userfiles_list(repo_id: int):
    repo = _get_repo_or_404(repo_id)
    d = _userfiles_dir_for_repo(repo)
    try:
        items = [f for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]
        items.sort(key=lambda s: s.lower())
    except Exception:
        items = []
    return jsonify({"ok": True, "files": items})


@client_bp.route("/api/userfiles/<int:repo_id>/upload", methods=["POST"])
@login_required
def api_userfiles_upload(repo_id: int):
    repo = _get_repo_or_404(repo_id)
    d = _userfiles_dir_for_repo(repo)
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "no file"}), 400
    f = request.files["file"]
    if not f or not getattr(f, "filename", ""):
        return jsonify({"ok": False, "error": "empty file"}), 400
    name = _safe_filename(f.filename)
    if not name:
        return jsonify({"ok": False, "error": "bad filename"}), 400

    base, ext = os.path.splitext(name)
    out_name = name
    n = 1
    while os.path.exists(os.path.join(d, out_name)):
        out_name = f"{base}_{n}{ext}"
        n += 1

    upload_s3 = _truthy_form_value(request.form.get("upload_s3"))
    if upload_s3:
        try:
            owner_id = str(getattr(current_user, "id", "") or "user").strip() or "user"
            public_url = _upload_userfile_to_s3(
                f,
                owner_id=owner_id,
                filename=name,
                content_type=getattr(f, "mimetype", None) or "application/octet-stream",
            )
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        return jsonify({"ok": True, "filename": public_url, "url": public_url, "s3": True})

    try:
        f.save(os.path.join(d, out_name))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True, "filename": out_name, "s3": False})


@client_bp.route("/api/userfiles/<int:repo_id>/delete", methods=["POST"])
@login_required
def api_userfiles_delete(repo_id: int):
    repo = _get_repo_or_404(repo_id)
    d = _userfiles_dir_for_repo(repo)
    payload = request.get_json(silent=True) or {}
    raw_filename = str(payload.get("filename") or "").strip()
    s3_key_from_public_url = getattr(main, "_s3_key_from_public_url", None)
    s3_key = ""
    try:
        if callable(s3_key_from_public_url):
            s3_key = s3_key_from_public_url(raw_filename)
    except Exception:
        s3_key = ""
    if s3_key and raw_filename.startswith(("http://", "https://")):
        try:
            s3_client = getattr(main, "s3", None)
            bucket = getattr(main, "S3_BUCKET", None)
            if s3_client is None or not bucket:
                raise RuntimeError("S3 client is not configured")
            s3_client.delete_object(Bucket=bucket, Key=s3_key)
            invalidate = getattr(main, "_runtime_cache_invalidate", None)
            if callable(invalidate):
                invalidate(raw_filename)
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
        return jsonify({"ok": True, "s3": True})

    name = _safe_filename(raw_filename)
    if not name:
        return jsonify({"ok": False, "error": "no filename"}), 400
    p = os.path.join(d, name)
    try:
        if os.path.isfile(p):
            os.remove(p)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True, "s3": False})


@client_bp.route("/api/userfiles/<int:repo_id>/raw/<path:filename>")
@login_required
def api_userfiles_raw(repo_id: int, filename: str):
    repo = _get_repo_or_404(repo_id)
    d = _userfiles_dir_for_repo(repo)
    name = _safe_filename(filename)
    if not name:
        abort(404)
    return send_from_directory(d, name, as_attachment=False)



@client_bp.route("/node_r/<int:repo_id>/<path:class_name>/<path:node_id>")
@login_required
def node_form_redirect(repo_id: int, class_name: str, node_id: str):
    repo = _get_repo_or_404(repo_id)
    return redirect(url_for("client.node_form", config_uid=repo.config_uid, class_name=class_name, node_id=node_id))


from typing import Any, Dict

def _coerce_input_value(payload: Dict[str, Any]) -> Any:

    v = payload.get("value")

    
    t = (payload.get("type") or payload.get("input_type") or payload.get("value_type") or "").lower()
    if t in ("checkbox", "switch", "bool", "boolean"):
        return bool(v)

    
    if isinstance(v, bool):
        return v

    # 2) number
    if t in ("number", "int", "integer", "float", "double"):
        if v is None or v == "":
            return None
        try:
            # если "12.3" -> float, если "12" -> int
            if isinstance(v, (int, float)):
                return v
            s = str(v).strip()
            if "." in s or "," in s:
                return float(s.replace(",", "."))
            return int(s)
        except Exception:
            return v

    
    # (тут ничего не приводим)
    return v

import re

def _parse_path_tokens(path: str):
    #
    tokens = []
    cur = ""
    i = 0
    while i < len(path):
        ch = path[i]
        if ch == ".":
            if cur:
                tokens.append(cur); cur = ""
            i += 1
            continue
        if ch == "[":
            if cur:
                tokens.append(cur); cur = ""
            end = path.find("]", i+1)
            if end == -1:
                tokens.append(path[i:])
                return tokens
            inside = path[i+1:end].strip()
            try:
                tokens.append(int(inside))
            except Exception:
                tokens.append(inside)
            i = end + 1
            continue
        cur += ch
        i += 1
    if cur:
        tokens.append(cur)
    return tokens

def _set_by_path(obj: dict, path: str, value):
    if not isinstance(obj, dict):
        return
    tokens = _parse_path_tokens(path or "")
    if not tokens:
        return
    cur = obj
    for k in tokens[:-1]:
        if isinstance(k, int):
            
            return
        if k not in cur or not isinstance(cur[k], (dict, list)):
            
            nxt = tokens[tokens.index(k)+1]  
            cur[k] = [] if isinstance(nxt, int) else {}
        cur = cur[k]
        if isinstance(cur, list):
            return
    last = tokens[-1]
    if isinstance(last, int):
        return
    cur[last] = value


def _apply_web_payload_to_node_data(node, payload: dict):
    if not isinstance(payload, dict):
        return

    base = None
    if getattr(node, "_data_cache", None) is not None and isinstance(node._data_cache, dict):
        base = node._data_cache
    elif getattr(node, "_data", None) is not None and isinstance(node._data, dict):
        base = node._data
    else:
        base = {}
        try:
            node._data_cache = base
        except Exception:
            pass

    listener = payload.get("listener") or payload.get("id")
    if listener is not None:
        base["listener"] = str(listener)

    el_id = payload.get("id")
    if el_id:
        if "value" in payload:
            base[str(el_id)] = payload.get("value")

        if "date_iso" in payload:
            base["_d" + str(el_id)] = payload.get("date_iso")

    try:
        p = payload.get("path")
        if isinstance(p, str) and p.strip() and ("value" in payload):
            _set_by_path(base, p.strip(), payload.get("value"))
    except Exception:
        pass

    # ВАЖНО: дополнительные значения от UI, например <field>_view
    # DatasetField / DatasetInput могут присылать:
    # "extra": [{"path": "customer_view", "value": "ООО Ромашка"}]
    extra = payload.get("extra")
    if isinstance(extra, list):
        for item in extra:
            if not isinstance(item, dict):
                continue
            try:
                ep = item.get("path")
                if isinstance(ep, str) and ep.strip() and ("value" in item):
                    _set_by_path(base, ep.strip(), item.get("value"))
            except Exception:
                pass

    fd = payload.get("full_data")
    if isinstance(fd, dict):
        try:
            for k, v in fd.items():
                base[k] = v
        except Exception:
            pass

    dv = payload.get("dialog_values")
    if isinstance(dv, dict):
        for p, v in dv.items():
            if isinstance(p, str) and p.strip():
                _set_by_path(base, p.strip(), v)

    passthrough_keys = [
        "row", "col", "row_id", "col_id",
        "selected", "selected_ids",
        "page", "page_size",
        "sort", "filter",
    ]
    for k in passthrough_keys:
        if k in payload:
            base[k] = payload[k]

    try:
        if getattr(node, "_data", None) is None or not isinstance(node._data, dict):
            node._data = {}
        node._data.update(base)
    except Exception:
        pass


def _raw_node_listener_from_payload(payload: Dict[str, Any]) -> str:
    try:
        if isinstance(payload, dict):
            return str(payload.get("listener") or payload.get("id") or "").strip()
    except Exception:
        pass
    return ""


def _raw_node_has_event_action(class_obj: Dict[str, Any], event_name: str, listener: str = "") -> bool:
    if not isinstance(class_obj, dict):
        return False
    for ev in (class_obj.get("events") or class_obj.get("Events") or []):
        if not isinstance(ev, dict):
            continue
        if ev.get("event") != event_name:
            continue
        ev_listener = str(ev.get("listener") or "").strip()
        if listener:
            if ev_listener and ev_listener != listener:
                continue
        else:
            if ev_listener:
                continue
        actions = ev.get("actions") or ev.get("Actions") or []
        return bool(actions)
    return False


def _raw_node_runner_listener(class_obj: Dict[str, Any], event_name: str, listener: str = "") -> str:
    """Match normal web-event listener semantics for app.py's stricter runner."""
    listener = str(listener or "").strip()
    if not isinstance(class_obj, dict):
        return listener
    if listener:
        for ev in (class_obj.get("events") or class_obj.get("Events") or []):
            if not isinstance(ev, dict) or ev.get("event") != event_name:
                continue
            if str(ev.get("listener") or "").strip() == listener and (ev.get("actions") or ev.get("Actions") or []):
                return listener
        for ev in (class_obj.get("events") or class_obj.get("Events") or []):
            if not isinstance(ev, dict) or ev.get("event") != event_name:
                continue
            if not str(ev.get("listener") or "").strip() and (ev.get("actions") or ev.get("Actions") or []):
                return ""
    return listener


def _render_raw_node_layout_response(repo, parsed: Dict[str, Any], class_name: str, node_id: str, layout: Any, node_data: Dict[str, Any]) -> str:
    layout = resolve_common_layout(parsed, layout)
    if layout is None:
        return ""
    try:
        if isinstance(node_data, dict):
            _fill_nodeinput_views(repo, parsed, layout, node_data)
    except Exception:
        pass
    try:
        return render_nodalayout_html(
            layout,
            node_data if isinstance(node_data, dict) else {},
            assets_base_dir=_userfiles_dir_for_repo(repo),
            context=_nl_context(repo, class_name=class_name, node_id=node_id),
        ) or ""
    except Exception:
        return ""


def _api_raw_node_event_web(j: Dict[str, Any]):
    """Handle UI events for raw-nodes that carry embedded _class JSON.

    Returns None when the raw-node only references a normal server class by
    string; in that case the existing normal-node event path remains the best
    match.
    """
    raw_node_id = str(j.get("raw_node_id") or j.get("raw_id") or "").strip()
    if not raw_node_id:
        return None

    RawNode = _server_model("RawNode")
    if RawNode is None:
        return jsonify({"ok": False, "error": "raw_node_model_unavailable"}), 404

    obj = RawNode.query.filter_by(node_id=raw_node_id).first()
    if not obj:
        return jsonify({"ok": False, "error": "raw_node_not_found"}), 404
    if not _current_user_can_access_raw_node(raw_node_id, obj=obj):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    payload_json = _raw_node_payload(obj)
    embedded_class = _extract_raw_node_class_json(payload_json)
    if not embedded_class:
        return None

    event = str(j.get("event") or "").strip()
    payload = j.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    class_name, payload_node_id, node_data = _raw_node_identity(payload_json, raw_node_id)
    class_name = _class_name_from_embedded_class(embedded_class, fallback=class_name, payload=payload_json)
    node_id = payload_node_id or str(j.get("node_id") or raw_node_id).strip()
    download_url = _raw_node_download_ref(payload_json, raw_node_id)
    event_download_url = _raw_node_download_url(raw_node_id)

    repo, parsed, cls = _resolve_raw_node_class(payload_json, class_name)
    if repo is None:
        repos = models.Repo.query.filter_by(user_id=current_user.id).order_by(models.Repo.id.asc()).all()
        repo = repos[0] if repos else None
    if repo is None:
        return jsonify({"ok": False, "error": "repo_not_found"}), 404
    parsed = parsed or get_parsed_config(repo, models.db) or {}
    cls = cls or embedded_class

    listener = _raw_node_listener_from_payload(payload)
    if not _raw_node_has_event_action(cls, event, listener):
        return jsonify({
            "ok": True,
            "noop": True,
            "handled": {"class_name": class_name, "node_id": node_id},
            "node_data": {},
            "patches": [],
        })

    node_data = dict(node_data or {})
    node_data.setdefault("_id", node_id)
    node_data.setdefault("_class", class_name)
    node_data.setdefault("_raw_node_id", raw_node_id)
    node_data.setdefault("_download_url", download_url)

    try:
        ctx_builder = getattr(main, "resolve_download_url_node_context", None)
        event_runner = getattr(main, "execute_download_url_node_event", None)
        if not callable(ctx_builder) or not callable(event_runner):
            return jsonify({"ok": False, "error": "raw_node_event_runtime_unavailable"}), 500

        ctx = ctx_builder(node_id=node_id, fallback_class_name=class_name, download_url=event_download_url)
        raw_event_node = ctx.get("node") if isinstance(ctx, dict) else None
        if raw_event_node is not None:
            try:
                if getattr(raw_event_node, "_data", None) is None or not isinstance(raw_event_node._data, dict):
                    raw_event_node._data = {}
                raw_event_node._data.update(node_data)
            except Exception:
                pass
            _apply_web_payload_to_node_data(raw_event_node, payload)

        event_runner(ctx, event, _raw_node_runner_listener(cls, event, listener), payload)

        if raw_event_node is not None:
            try:
                updated = raw_event_node.get_data() or {}
                if isinstance(updated, dict):
                    node_data = updated.copy()
            except Exception:
                pass

        node_data.setdefault("_id", node_id)
        node_data.setdefault("_class", class_name)
        node_data.setdefault("_raw_node_id", raw_node_id)
        node_data.setdefault("_download_url", download_url)

        new_layout = getattr(raw_event_node, "_ui_layout", None) if raw_event_node is not None else None
        ui_message = getattr(raw_event_node, "_ui_message", None) if raw_event_node is not None else None
        ui_dialog = getattr(raw_event_node, "_ui_dialog", None) if raw_event_node is not None else None
        ui_open = getattr(raw_event_node, "_ui_open", None) if raw_event_node is not None else None
        ui_close = getattr(raw_event_node, "_ui_close", None) if raw_event_node is not None else None
        ui_run_projection = getattr(raw_event_node, "_ui_run_projection", None) if raw_event_node is not None else None
        ui_plugins = getattr(raw_event_node, "_ui_plugins", None) if raw_event_node is not None else None

        if new_layout is not None:
            resp: Dict[str, Any] = {
                "ok": True,
                "layout_html": _render_raw_node_layout_response(repo, parsed, class_name, node_id, new_layout, node_data),
                "node_data": node_data,
            }
        else:
            resp = {"ok": True, "node_data": node_data}

        resp["handled"] = {"class_name": class_name, "node_id": node_id}
        resp["patches"] = []

        if ui_plugins is not None:
            resp["plugins"] = ui_plugins
        if ui_message is not None:
            resp["message"] = ui_message
        if ui_open is not None:
            resp["open"] = ui_open
        if ui_close:
            resp["close"] = True
        if ui_run_projection:
            resp["run_projection"] = True

        if isinstance(ui_dialog, dict):
            layout_html = ""
            if ui_dialog.get("layout") is not None:
                layout_html = _render_raw_node_layout_response(repo, parsed, class_name, node_id, ui_dialog.get("layout"), node_data)
            resp["dialog"] = {
                "id": ui_dialog.get("id") or "dialog",
                "title": ui_dialog.get("title") or "",
                "positive": ui_dialog.get("positive") or "OK",
                "negative": ui_dialog.get("negative") or "Cancel",
                "layout_html": layout_html,
                "html": ui_dialog.get("html") or "",
            }

        return jsonify(resp)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": {"text": f"Raw-node handler error: {e}", "level": "error"},
        }), 200



@client_bp.route("/api/node/event_web", methods=["POST"])
@login_required
def api_node_event_web():
    j = request.get_json(force=True) or {}

    if j.get("is_raw_node") or j.get("raw_node_id"):
        raw_resp = _api_raw_node_event_web(j)
        if raw_resp is not None:
            return raw_resp

    repo_id = int(j.get("repo_id") or 0)

    
    class_name = str(j.get("class_name") or "").strip()
    node_id = str(j.get("node_id") or "").strip()

    
    target_class_name = str(j.get("target_class_name") or "").strip()
    target_node_id = str(j.get("target_node_id") or "").strip()

    event = str(j.get("event") or "").strip()
    payload = j.get("payload") or {}

    if not repo_id or not class_name or not node_id or not event:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = _get_repo_or_404(repo_id)

   
    eff_class = target_class_name or class_name
    eff_id = target_node_id or node_id

    parsed = get_parsed_config(repo, models.db) or {}
    _ctx_tokens = _nodes_mod.set_runtime_context(repo.config_uid, parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    eff_cls_cfg = (parsed.get("classes") or {}).get(eff_class) or {}
    is_custom_process = _is_singleton_class_type(eff_cls_cfg)

    # listener matching
    listener = ""
    try:
        if isinstance(payload, dict):
            listener = str(payload.get("listener") or payload.get("id") or "").strip()
    except Exception:
        listener = ""

    actions: list[dict] = []
    for ev in (eff_cls_cfg.get("events") or []):
        if ev.get("event") != event:
            continue

        ev_listener = str(ev.get("listener") or "").strip()
        
        if listener:
            if ev_listener and ev_listener != listener:
                continue
        else:
            if ev_listener:
                continue

        actions.extend(ev.get("actions") or [])

    if not actions:
        return jsonify({
            "ok": True,
            "noop": True,
            "handled": {"class_name": eff_class, "node_id": eff_id},
            "node_data": {},
            "patches": []
        })

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    new_layout = None
    node_data: Dict[str, Any] = {}
    ui_message = None
    ui_dialog = None
    ui_open = None
    ui_close = None
    ui_run_projection = None
    patches: list[dict] = []

    try:
        # -------- REMOTE --------
        if base_url and base_url != current:
            for a in actions:
                m = (a.get("method") or "").strip()
                if not m:
                    continue

                r = _api_post_remote(
                    repo,
                    f"/api/config/{repo.config_uid}/node/{eff_class}/{eff_id}/{m}",
                    json_data=payload,
                )

                if isinstance(r, dict) and isinstance(r.get("data"), dict):
                    data = r["data"]
                    if "_ui_layout" in data:
                        new_layout = data.get("_ui_layout")
                    if "_ui_message" in data:
                        ui_message = data.get("_ui_message")
                    if "_ui_dialog" in data:
                        ui_dialog = data.get("_ui_dialog")
                    if "_ui_open" in data:
                        ui_open = data.get("_ui_open")
                    if "_ui_close" in data:
                        ui_close = data.get("_ui_close")
                    if "_ui_run_projection" in data:
                        ui_run_projection = data.get("_ui_run_projection")

            
            node_data = {}

        # -------- LOCAL --------
        else:
            node_class = _load_server_node_class(repo.config_uid, eff_class)

            if is_custom_process:
                defaults = (eff_cls_cfg.get("_data") or {})
                if not isinstance(defaults, dict):
                    defaults = {}
                try:
                    node = node_class.get(eff_id, repo.config_uid)
                except Exception:
                    node = None
                if not node:
                    node = node_class(eff_id, repo.config_uid)

                stored_data = {}
                try:
                    stored_data = node.get_data() or {}
                except Exception:
                    stored_data = {}
                if not isinstance(stored_data, dict):
                    stored_data = {}

                node_data = stored_data.copy()
                for k, v in defaults.items():
                    node_data.setdefault(k, v)
                _apply_projection_defaults_to_data(eff_cls_cfg, node_data, repo.config_uid, eff_class, eff_id)
                node_data.setdefault("_id", eff_id)
                node_data.setdefault("_class", eff_class)

                try:
                    node._data_cache = node_data.copy()
                except Exception:
                    pass

            else:
                node = node_class.get(eff_id, repo.config_uid)
                if not node:
                    return jsonify({"ok": False, "error": "node not found"}), 404

            
            try:
                node._schema_class_name = eff_class
            except Exception:
                pass

            
            try:
                if getattr(node, "_data_cache", None) is None:
                    full = node.get_data() or {}
                    node._data_cache = dict(full) if isinstance(full, dict) else {}
            except Exception:
                if getattr(node, "_data_cache", None) is None:
                    node._data_cache = {}

            
            _apply_web_payload_to_node_data(node, payload if isinstance(payload, dict) else {})

            # call methods
            prev_current = getattr(_nodes_mod, "CURRENT_NODE", None)
            setattr(_nodes_mod, "CURRENT_NODE", node)

            for a in actions:
                m = (a.get("method") or "").strip()
                if m and hasattr(node, m):
                    getattr(node, m)(payload)
                    if getattr(node, "_ui_layout", None) is not None:
                        new_layout = node._ui_layout

            setattr(_nodes_mod, "CURRENT_NODE", prev_current)

            ui_message = getattr(node, "_ui_message", None)
            ui_dialog = getattr(node, "_ui_dialog", None)
            ui_open = getattr(node, "_ui_open", None)
            ui_close = getattr(node, "_ui_close", None)
            ui_run_projection = getattr(node, "_ui_run_projection", None)

            ui_plugins = getattr(node, "_ui_plugins", None)
            try:
                if hasattr(node, "_ui_plugins"):
                    delattr(node, "_ui_plugins")   # one-shot
            except Exception:
                pass


            # refresh data for @vars
            try:
                if getattr(node, "_data_cache", None) is not None:
                    node_data = node._data_cache or {}
                else:
                    node_data = node.get_data() or {}
            except Exception:
                node_data = node_data or {}

            # clear one-shot ui fields
            try:
                for k in ("_ui_message", "_ui_dialog", "_ui_layout", "_ui_open", "_ui_close", "_ui_run_projection"):
                    if hasattr(node, k):
                        delattr(node, k)
            except Exception:
                pass

        
        if target_class_name and target_node_id:
            try:
                patches.append({
                    "type": "cover",
                    "class_name": eff_class,
                    "node_id": eff_id,
                    "html": _node_cover_html(repo, eff_class, eff_id),
                })
            except Exception:
                patches.append({
                    "type": "cover",
                    "class_name": eff_class,
                    "node_id": eff_id,
                    "html": "",
                })

        # dialog render
        ui_dialog_payload = None
        if ui_dialog is not None and isinstance(ui_dialog, dict):
            layout_html = ""
            if ui_dialog.get("layout") is not None:
                try:
                    layout_html = render_nodalayout_html(
                        ui_dialog.get("layout"),
                        node_data if isinstance(node_data, dict) else {},
                        assets_base_dir=_userfiles_dir_for_repo(repo),
                        context=_nl_context(repo, class_name=eff_class, node_id=eff_id),
                    )
                except Exception:
                    layout_html = ""
            ui_dialog_payload = {
                "id": ui_dialog.get("id") or "dialog",
                "title": ui_dialog.get("title") or "",
                "positive": ui_dialog.get("positive") or "OK",
                "negative": ui_dialog.get("negative") or "Cancel",
                "layout_html": layout_html,
                "html": ui_dialog.get("html") or "",
            }

        # layout render
        resp: Dict[str, Any]
        if new_layout is not None:
            try:
                data_for_layout = node_data if isinstance(node_data, dict) else {}
                layout_html = render_nodalayout_html(
                    new_layout,
                    data_for_layout,
                    assets_base_dir=_userfiles_dir_for_repo(repo),
                    context=_nl_context(repo, class_name=eff_class, node_id=eff_id),
                )
            except Exception:
                layout_html = ""
            resp = {"ok": True, "layout_html": layout_html, "node_data": node_data}
        else:
            resp = {"ok": True, "node_data": node_data}

        resp["handled"] = {"class_name": eff_class, "node_id": eff_id}
        resp["patches"] = patches

        if ui_plugins is not None:
            resp["plugins"] = ui_plugins


        if ui_message is not None:
            resp["message"] = ui_message
        if ui_dialog_payload is not None:
            resp["dialog"] = ui_dialog_payload
        if ui_open is not None:
            resp["open"] = ui_open
        if ui_close:
            resp["close"] = True
        if ui_run_projection:
            resp["run_projection"] = True

        return jsonify(resp)

    except _nodes_mod.AcceptRejected as e:


        payload = getattr(e, 'payload', None) or {}


        msg = payload.get('message')


        if not isinstance(msg, dict):


            msg = {'text': payload.get('error') or 'Save rejected', 'level': 'error'}


        return jsonify({'ok': False, 'error': payload.get('error') or 'rejected', 'message': msg}), 200


    except Exception as e:
        
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": {"text": f"Handler error: {e}", "level": "error"},
        }), 200


class _UiHost:
    """
    UI host for CommonEvents (no real node).
    nodes.py free-functions (message/Dialog/CloseNode) use CURRENT_NODE, so we provide the same API as Node.
    """

    def __init__(self):
        # keep the same shapes as in nodes.py Node.Message/Node.Dialog/Node.Show
        self._ui_message = None   # list[{"text":..,"level":..}]
        self._ui_dialog = None    # dict with keys: id,title,positive,negative,layout,html
        self._ui_layout = None    # layout spec (will be rendered by render_nodalayout_html)
        self._ui_open = None
        self._ui_close = None

    def Message(self, text: str, level: str = "info"):
        try:
            msgs = getattr(self, "_ui_message", None)
            if not isinstance(msgs, list):
                msgs = []
            msgs.append({"text": str(text), "level": str(level or "info")})
            self._ui_message = msgs
        except Exception:
            pass

    def Dialog(self, dialog_id: str, title: str = "", *, positive: str = "OK", negative: str = "Cancel", layout=None, html: str = ""):
        self._ui_dialog = {
            "id": str(dialog_id or "dialog"),
            "title": str(title or ""),
            "positive": str(positive or "OK"),
            "negative": str(negative or "Cancel"),
            "layout": layout,
            "html": html,
        }

    def Show(self, layout):
        self._ui_layout = layout
        return True

    def CloseNode(self):
        self._ui_close = True
        return True

    def RunProjection(self):
        self._ui_run_projection = True
        return True



@client_bp.route("/api/common/event_web", methods=["POST"])
@login_required
def api_common_event_web():
    """
    Execute CommonEvents (Configuration.config_events) in web client context.

    Works like onInputWeb but:
      - does NOT call node methods
      - calls python functions from handlers module:
            def <method>(input_data)
      - injects `nodes.CURRENT_NODE = _UiHost()` so message()/Dialog() works

    Payload rules:
      - listener matching
      - expected payload is dict
    """
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    event = str(j.get("event") or "")
    payload = j.get("payload") or {}

    if not repo_id or not event:
        return jsonify({"ok": False, "error": "bad args"}), 400
    if not isinstance(payload, dict):
        payload = {}

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cfg = (parsed.get("cfg") or {}) if isinstance(parsed, dict) else {}

    common_events = cfg.get("CommonEvents") or []

    # listener matching exactly like in api_node_event_web
    listener = ""
    try:
        listener = str(payload.get("listener") or payload.get("id") or "").strip()
    except Exception:
        listener = ""

    actions = []
    for ev in (common_events or []):
        if (ev.get("event") or "") != event:
            continue
        ev_listener = str(ev.get("listener") or "").strip()

        if listener:
            # allow exact + global(empty)
            if ev_listener and ev_listener != listener:
                continue
        else:
            # only global(empty)
            if ev_listener:
                continue

        actions.extend(ev.get("actions") or [])

    if not actions:
        return jsonify({"ok": True, "noop": True})

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    ui_message = None
    ui_dialog_payload = None
    layout_html = None
    ui_open = None
    ui_close = None
    ui_run_projection = None

    try:
        
        if base_url and base_url != current:
            return jsonify({
                "ok": False,
                "error": "CommonEvents are local-only (no remote endpoint implemented)",
                "message": [{"text": "CommonEvents: remote call not supported", "level": "warning"}],
            }), 200

        # -------- LOCAL --------
        ns = _load_server_handlers_ns(repo.config_uid)

        ui = _UiHost()

        # inject CURRENT_NODE so nodes.message()/nodes.Dialog() works
        prev_current = getattr(_nodes_mod, "CURRENT_NODE", None)
        setattr(_nodes_mod, "CURRENT_NODE", ui)

        try:
            for a in actions:
                m = (a.get("method") or "").strip()
                if not m:
                    continue
                fn = ns.get(m)
                if not callable(fn):
                    raise ValueError(f"CommonEvent function '{m}' not found/callable in handlers")

                try:
                    fn(payload)  # <-- KEY POINT: function(input_data)
                except  Exception as e:
                    print(str(e))    

        
        finally:
            setattr(_nodes_mod, "CURRENT_NODE", prev_current)

        # collect ui fields
        ui_message = getattr(ui, "_ui_message", None)
        ui_dialog = getattr(ui, "_ui_dialog", None)
        ui_open = getattr(ui, "_ui_open", None)
        ui_close = getattr(ui, "_ui_close", None)
        ui_run_projection = getattr(ui, "_ui_run_projection", None)

        # dialog render (same as in api_node_event_web, but data for vars is just payload)
        if ui_dialog is not None and isinstance(ui_dialog, dict):
            dlg_layout_html = ""
            if ui_dialog.get("layout") is not None:
                try:
                    dlg_layout_html = render_nodalayout_html(
                        ui_dialog.get("layout"),
                        payload if isinstance(payload, dict) else {},
                        assets_base_dir=_userfiles_dir_for_repo(repo),
                        context=_nl_context(repo, class_name="", node_id="")
                    )
                except Exception:
                    dlg_layout_html = ""

            ui_dialog_payload = {
                "id": ui_dialog.get("id") or "dialog",
                "title": ui_dialog.get("title") or "",
                "positive": ui_dialog.get("positive") or "OK",
                "negative": ui_dialog.get("negative") or "Cancel",
                "layout_html": dlg_layout_html,
                "html": ui_dialog.get("html") or "",
            }

        # layout render (Show(layout))
        if getattr(ui, "_ui_layout", None) is not None:
            try:
                layout_html = render_nodalayout_html(
                    getattr(ui, "_ui_layout"),
                    payload if isinstance(payload, dict) else {},
                    assets_base_dir=_userfiles_dir_for_repo(repo),
                    context=_nl_context(repo, class_name="", node_id="")
                )
            except Exception:
                layout_html = ""

        resp = {"ok": True}
        if ui_message is not None:
            resp["message"] = ui_message
        if ui_dialog_payload is not None:
            resp["dialog"] = ui_dialog_payload
        if layout_html is not None:
            resp["layout_html"] = layout_html
        if ui_open is not None:
            resp["open"] = ui_open
        if ui_close:
            resp["close"] = True
        if ui_run_projection:
            resp["run_projection"] = True

        return jsonify(resp)

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": [{"text": f"CommonEvent error: {e}", "level": "error"}],
        }), 200



TIMER_MIN_PERIOD_SECONDS = 900
SERVER_TIMER_SCAN_SECONDS = 30
_SERVER_TIMER_STATE: Dict[str, Dict[str, Any]] = {}
_SERVER_TIMER_LOCK = threading.RLock()
_SERVER_TIMER_STARTED = False
_SERVER_TIMER_STOP = None


def _timer_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    value = str(value).strip().lower()
    if value in {'1', 'true', 'on', 'yes'}:
        return True
    if value in {'0', 'false', 'off', 'no'}:
        return False
    return default


def _timer_runtime(timer_cfg: Dict[str, Any]) -> str:
    if not isinstance(timer_cfg, dict):
        return 'server'
    raw = str(timer_cfg.get('runtime') or timer_cfg.get('run_on') or '').strip().lower()
    if raw in {'client', 'browser', 'web'}:
        return 'client'
    if raw in {'server', 'backend'}:
        return 'server'
    # Backward compatibility: timers created before the Server/Client switch
    # were intended by design to be server timers.
    return 'server'


def _timer_period_seconds(timer_cfg: Dict[str, Any]) -> int:
    try:
        period = int(float((timer_cfg or {}).get('period_seconds') or (timer_cfg or {}).get('period') or 0))
    except Exception:
        period = 0
    runtime = _timer_runtime(timer_cfg)
    worker = _timer_bool((timer_cfg or {}).get('worker'), False)
    min_period = TIMER_MIN_PERIOD_SECONDS if runtime == 'server' or worker else 1
    return max(min_period, period)


def _timer_id(timer_cfg: Dict[str, Any]) -> str:
    if not isinstance(timer_cfg, dict):
        return ''
    return str(timer_cfg.get('timer_id') or timer_cfg.get('id') or '').strip()


def _timer_actions(timer_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions = (timer_cfg or {}).get('actions') or (timer_cfg or {}).get('Actions') or []
    return [a for a in actions if isinstance(a, dict)]


def _timer_signature(timer_cfg: Dict[str, Any]) -> str:
    try:
        stable = {
            'timer_id': _timer_id(timer_cfg),
            'period_seconds': _timer_period_seconds(timer_cfg),
            'runtime': _timer_runtime(timer_cfg),
            'active': _timer_bool((timer_cfg or {}).get('active'), True),
            'worker': _timer_bool((timer_cfg or {}).get('worker'), False),
            'actions': _timer_actions(timer_cfg),
        }
        return hashlib.sha256(json.dumps(stable, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
    except Exception:
        return str(time.time())


def _execute_timer_actions_for_repo(repo, timer_cfg: Dict[str, Any], payload: Optional[Dict[str, Any]] = None, *, include_ui: bool = False, parsed_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute timer actions exactly like CommonEvents, without requiring a browser.

    Browser/client calls pass include_ui=True so UI helper results can be rendered
    back to the current page. Server scheduler calls include_ui=False; message(),
    Dialog(), Show(), CloseNode() are still safely captured but only logged/returned.
    """
    timer_id = _timer_id(timer_cfg)
    if not timer_id:
        return {"ok": False, "error": "Timer ID is empty"}
    if not _timer_bool(timer_cfg.get("active"), True):
        return {"ok": True, "noop": True, "inactive": True}

    actions = _timer_actions(timer_cfg)
    if not actions:
        return {"ok": True, "noop": True}

    timer_payload = dict(payload or {}) if isinstance(payload, dict) else {}
    timer_payload["_timer_id"] = timer_id
    timer_payload["_timer_runtime"] = _timer_runtime(timer_cfg)
    timer_payload["_timer_period_seconds"] = _timer_period_seconds(timer_cfg)
    data_payload = timer_payload.get("_data")
    if not isinstance(data_payload, dict):
        data_payload = {}
    data_payload["_timer_id"] = timer_id
    data_payload["_timer_runtime"] = _timer_runtime(timer_cfg)
    data_payload["_timer_period_seconds"] = _timer_period_seconds(timer_cfg)
    timer_payload["_data"] = data_payload

    if parsed_config is None:
        try:
            parsed_config = get_parsed_config(repo, models.db) or {}
        except Exception:
            parsed_config = {}

    config_uid = str(getattr(repo, "config_uid", "") or "").strip()
    try:
        cfg_json = parsed_config.get("cfg") if isinstance(parsed_config, dict) else None
        if not config_uid and isinstance(cfg_json, dict):
            config_uid = str(cfg_json.get("uid") or "").strip()
    except Exception:
        pass

    ns = _load_server_handlers_ns(config_uid, parsed_config)
    ui = _UiHost()
    prev_current = getattr(_nodes_mod, "CURRENT_NODE", None)
    ctx_tokens = None
    try:
        ctx_tokens = _nodes_mod.set_runtime_context(config_uid, parsed_config)
    except Exception:
        ctx_tokens = None
    setattr(_nodes_mod, "CURRENT_NODE", ui)

    executed = []
    try:
        for a in actions:
            m = (a.get("method") or "").strip()
            if not m:
                continue
            fn = ns.get(m)
            if not callable(fn):
                raise ValueError(f"Timer function '{m}' not found/callable in handlers")
            try:
                fn(timer_payload)
                executed.append(m)
            except Exception as e:
                print(f"Timer {timer_id} handler {m} error: {e}")
                try:
                    traceback.print_exc()
                except Exception:
                    pass
    finally:
        setattr(_nodes_mod, "CURRENT_NODE", prev_current)
        if ctx_tokens is not None:
            try:
                _nodes_mod.reset_runtime_context(ctx_tokens)
            except Exception:
                pass

    resp: Dict[str, Any] = {"ok": True, "timer_id": timer_id, "executed": executed}

    ui_message = getattr(ui, "_ui_message", None)
    ui_dialog = getattr(ui, "_ui_dialog", None)
    ui_open = getattr(ui, "_ui_open", None)
    ui_close = getattr(ui, "_ui_close", None)
    ui_run_projection = getattr(ui, "_ui_run_projection", None)

    if ui_message is not None:
        resp["message"] = ui_message

    if include_ui:
        ui_dialog_payload = None
        layout_html = None

        if ui_dialog is not None and isinstance(ui_dialog, dict):
            dlg_layout_html = ""
            if ui_dialog.get("layout") is not None:
                try:
                    dlg_layout_html = render_nodalayout_html(
                        ui_dialog.get("layout"),
                        timer_payload,
                        assets_base_dir=_userfiles_dir_for_repo(repo),
                        context=_nl_context(repo, class_name="", node_id="")
                    )
                except Exception:
                    dlg_layout_html = ""

            ui_dialog_payload = {
                "id": ui_dialog.get("id") or "dialog",
                "title": ui_dialog.get("title") or "",
                "positive": ui_dialog.get("positive") or "OK",
                "negative": ui_dialog.get("negative") or "Cancel",
                "layout_html": dlg_layout_html,
                "html": ui_dialog.get("html") or "",
            }

        if getattr(ui, "_ui_layout", None) is not None:
            try:
                layout_html = render_nodalayout_html(
                    getattr(ui, "_ui_layout"),
                    timer_payload,
                    assets_base_dir=_userfiles_dir_for_repo(repo),
                    context=_nl_context(repo, class_name="", node_id="")
                )
            except Exception:
                layout_html = ""

        if ui_dialog_payload is not None:
            resp["dialog"] = ui_dialog_payload
        if layout_html is not None:
            resp["layout_html"] = layout_html
        if ui_open is not None:
            resp["open"] = ui_open
        if ui_close:
            resp["close"] = True
        if ui_run_projection:
            resp["run_projection"] = True
    else:
        if ui_dialog is not None:
            resp["dialog"] = True
        if getattr(ui, "_ui_layout", None) is not None:
            resp["layout"] = True
        if ui_open is not None:
            resp["open"] = ui_open
        if ui_close:
            resp["close"] = True
        if ui_run_projection:
            resp["run_projection"] = True

    return resp


@client_bp.route("/api/timer/event_web", methods=["POST"])
@login_required
def api_timer_event_web():
    """Execute an active Client timer from the browser.

    Server timers are ignored here because they are executed by the background
    server scheduler and must not depend on an open browser tab.
    """
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    timer_id = str(j.get("timer_id") or j.get("id") or "").strip()
    payload = j.get("payload") or {}

    if not repo_id or not timer_id:
        return jsonify({"ok": False, "error": "bad args"}), 400
    if not isinstance(payload, dict):
        payload = {}

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    cfg = (parsed.get("cfg") or {}) if isinstance(parsed, dict) else {}

    timer_cfg = None
    for t in (cfg.get("Timers") or cfg.get("timers") or []):
        if not isinstance(t, dict):
            continue
        if _timer_id(t) == timer_id:
            timer_cfg = t
            break

    if not timer_cfg:
        return jsonify({"ok": False, "error": "Timer not found"}), 404
    if _timer_runtime(timer_cfg) != 'client':
        return jsonify({"ok": True, "noop": True, "server_timer": True})

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")
    if base_url and base_url != current:
        return jsonify({
            "ok": False,
            "error": "Client timers are local-only (no remote endpoint implemented)",
            "message": [{"text": "Timers: remote call not supported", "level": "warning"}],
        }), 200

    try:
        return jsonify(_execute_timer_actions_for_repo(repo, timer_cfg, payload, include_ui=True, parsed_config=parsed))
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": [{"text": f"Timer error: {e}", "level": "error"}],
        }), 200





def _refresh_local_repo_config_cache(repo) -> None:
    """Refresh repository cache only when the config exists in this Designer DB.

    A client repository can contain a cached configuration that was added by a
    public link or copied from another server. In that case repo.base_url may be
    empty because node API access is configured separately, but the configuration
    UID is not present in the local Designer DB. Server timers must still use the
    cached RepoConfig snapshot and should not spam logs with "Configuration ...
    not found in DB".
    """
    try:
        if (getattr(repo, "base_url", "") or "").strip():
            return

        config_uid = str(getattr(repo, "config_uid", "") or "").strip()
        if not config_uid:
            return

        Configuration = getattr(main, "Configuration", None)
        if Configuration is None:
            return

        # Only local Designer configurations can be refreshed immediately from DB.
        # Cached/public repositories remain usable through client_repo_config.
        cfg_exists = models.db.session.execute(
            select(Configuration.id).where(Configuration.uid == config_uid)
        ).first() is not None
        if not cfg_exists:
            return

        cfg = fetch_config_from_local_db(config_uid)
        cfg_json = json.dumps(cfg, ensure_ascii=False)
        row = models.RepoConfig.query.filter_by(repo_id=repo.id).first()
        if not row:
            row = models.RepoConfig(repo_id=repo.id, config_json=cfg_json)
            models.db.session.add(row)
            changed = True
        else:
            changed = (row.config_json != cfg_json)
            if changed:
                row.config_json = cfg_json
        if changed:
            row.updated_at = datetime.now(timezone.utc)
            try:
                repo.config_json = cfg_json
                repo.config_cached_at = row.updated_at
            except Exception:
                pass
            models.db.session.commit()
            CONFIG_MEM.pop(repo.id, None)
    except Exception as e:
        try:
            models.db.session.rollback()
        except Exception:
            pass
        print(f"Server timer: local repo cache refresh skipped for repo={getattr(repo, 'id', '?')}: {e}")


def _iter_server_timer_candidates() -> List[Tuple[Any, Dict[str, Any], Dict[str, Any]]]:
    out: List[Tuple[Any, Dict[str, Any], Dict[str, Any]]] = []
    repos = models.Repo.query.order_by(models.Repo.id.asc()).all()
    for repo in repos:
        _refresh_local_repo_config_cache(repo)
        try:
            parsed = get_parsed_config(repo, models.db) or {}
            cfg = (parsed.get("cfg") or {}) if isinstance(parsed, dict) else {}
        except Exception as e:
            print(f"Server timer: cannot read repo {getattr(repo, 'id', '?')}: {e}")
            continue

        for timer_cfg in (cfg.get("Timers") or cfg.get("timers") or []):
            if not isinstance(timer_cfg, dict):
                continue
            if not _timer_bool(timer_cfg.get("active"), True):
                continue
            if _timer_runtime(timer_cfg) != "server":
                continue
            if not _timer_id(timer_cfg):
                continue
            out.append((repo, timer_cfg, parsed))
    return out


def _server_timer_scheduler_tick() -> None:
    now = time.time()
    active_keys = set()

    for repo, timer_cfg, parsed in _iter_server_timer_candidates():
        timer_id = _timer_id(timer_cfg)
        key = f"{int(getattr(repo, 'id', 0) or 0)}:{timer_id}"
        active_keys.add(key)
        period = _timer_period_seconds(timer_cfg)
        sig = _timer_signature(timer_cfg)

        with _SERVER_TIMER_LOCK:
            st = _SERVER_TIMER_STATE.get(key)
            if not st:
                # New active server timer: fire on the next scheduler pass immediately,
                # then continue by period. The period is still normalized to at least
                # 15 minutes for Server/Worker timers.
                _SERVER_TIMER_STATE[key] = {
                    "next_at": now,
                    "period": period,
                    "signature": sig,
                    "running": True,
                    "last_error": "",
                }
            elif st.get("signature") != sig:
                # Timer config/action changed: apply it right away.
                st.update({
                    "next_at": now,
                    "period": period,
                    "signature": sig,
                    "running": True,
                    "last_error": "",
                })
            else:
                if st.get("running") or now < float(st.get("next_at") or 0):
                    continue
                st["running"] = True

        started_at = time.time()
        try:
            print(f"Server timer fire: repo={getattr(repo, 'id', '?')} config={getattr(repo, 'config_uid', '')} timer={timer_id}")
            resp = _execute_timer_actions_for_repo(
                repo,
                timer_cfg,
                {
                    "_timer_id": timer_id,
                    "_server_timer": True,
                    "repo_id": int(getattr(repo, "id", 0) or 0),
                    "config_uid": str(getattr(repo, "config_uid", "") or ""),
                },
                include_ui=False,
                parsed_config=parsed,
            )
            if resp.get("message"):
                print(f"Server timer message {timer_id}: {resp.get('message')}")
        except Exception as e:
            print(f"Server timer error: repo={getattr(repo, 'id', '?')} timer={timer_id}: {e}")
            try:
                traceback.print_exc()
            except Exception:
                pass
            with _SERVER_TIMER_LOCK:
                if key in _SERVER_TIMER_STATE:
                    _SERVER_TIMER_STATE[key]["last_error"] = str(e)
        finally:
            with _SERVER_TIMER_LOCK:
                st = _SERVER_TIMER_STATE.setdefault(key, {})
                st["running"] = False
                st["last_fired_at"] = started_at
                # Schedule from the start time to avoid drift, but never fire immediately
                # in a tight loop if the handler ran longer than the period.
                st["next_at"] = max(started_at + period, time.time() + 1)
                st["period"] = period
                st["signature"] = sig

    with _SERVER_TIMER_LOCK:
        for key in list(_SERVER_TIMER_STATE.keys()):
            if key not in active_keys:
                _SERVER_TIMER_STATE.pop(key, None)


def _server_timer_scheduler_loop(app_obj):
    print("Server timer scheduler started")
    while True:
        stop_event = globals().get("_SERVER_TIMER_STOP")
        if stop_event is not None and stop_event.is_set():
            break
        try:
            with app_obj.app_context():
                _server_timer_scheduler_tick()
        except Exception as e:
            print(f"Server timer scheduler tick error: {e}")
            try:
                traceback.print_exc()
            except Exception:
                pass
        stop_event = globals().get("_SERVER_TIMER_STOP")
        if stop_event is not None and stop_event.wait(SERVER_TIMER_SCAN_SECONDS):
            break


def start_server_timer_scheduler(app_obj) -> bool:
    """Start one background scheduler thread for Server timers."""
    global _SERVER_TIMER_STARTED, _SERVER_TIMER_STOP
    if _SERVER_TIMER_STARTED:
        return False
    _SERVER_TIMER_STOP = threading.Event()
    t = threading.Thread(
        target=_server_timer_scheduler_loop,
        args=(app_obj,),
        name="noda-server-timer-scheduler",
        daemon=True,
    )
    t.start()
    _SERVER_TIMER_STARTED = True
    return True


@client_bp.route("/api/class/event_web", methods=["POST"])
@login_required
def api_class_event_web():
    j = request.get_json(force=True) or {}
    repo_id = int(j.get("repo_id") or 0)
    class_name = str(j.get("class_name") or "")
    event = str(j.get("event") or "")
    payload = j.get("payload") or {}

    if not repo_id or not class_name or not event:
        return jsonify({"ok": False, "error": "bad args"}), 400
    if not isinstance(payload, dict):
        payload = {}

    repo = _get_repo_or_404(repo_id)
    parsed = get_parsed_config(repo, models.db) or {}
    _ctx_tokens = _nodes_mod.set_runtime_context(repo.config_uid, parsed)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp

    cls = (parsed.get("classes") or {}).get(class_name) or {}

    # listener matching 
    listener = ""
    try:
        listener = str(payload.get("listener") or payload.get("id") or "").strip()
    except Exception:
        listener = ""

    actions = []
    for ev in (cls.get("events") or []):
        if ev.get("event") != event:
            continue
        ev_listener = str(ev.get("listener") or "").strip()

        if listener:
            if ev_listener and ev_listener != listener:
                continue
        else:
            if ev_listener:
                continue

        actions.extend(ev.get("actions") or [])

    if not actions:
        return jsonify({"ok": True, "noop": True})

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    
    if base_url and base_url != current:
        return jsonify({
            "ok": False,
            "error": "class event remote call not implemented",
            "message": [{"text": "ClassCommandWeb: remote call not supported", "level": "warning"}],
        }), 200

    try:
        node_class = _load_server_node_class(repo.config_uid, class_name)

        
        ui = _UiHost()
        prev_current = getattr(_nodes_mod, "CURRENT_NODE", None)
        setattr(_nodes_mod, "CURRENT_NODE", ui)
        try:
            for a in actions:
                m = (a.get("method") or "").strip()
                if not m:
                    continue

                fn = getattr(node_class, m, None)
                if not callable(fn):
                    raise ValueError(f"Class handler '{m}' not found on class '{class_name}'")


                fn(payload)

        finally:
            setattr(_nodes_mod, "CURRENT_NODE", prev_current)

        resp = {"ok": True}

        ui_message = getattr(ui, "_ui_message", None)
        ui_dialog = getattr(ui, "_ui_dialog", None)

        if ui_message is not None:
            resp["message"] = ui_message
        if ui_dialog is not None:
            resp["dialog"] = ui_dialog

        return jsonify(resp)

    except _nodes_mod.AcceptRejected as e:


        payload = getattr(e, 'payload', None) or {}


        msg = payload.get('message')


        if not isinstance(msg, dict):


            msg = {'text': payload.get('error') or 'Save rejected', 'level': 'error'}


        return jsonify({'ok': False, 'error': payload.get('error') or 'rejected', 'message': msg}), 200


    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": [{"text": f"class-event error: {e}", "level": "error"}],
        }), 200


@client_bp.route("/api/class_nodes")
@login_required
def api_class_nodes():
    repo_id = int(request.args.get("repo_id") or 0)
    class_name = (request.args.get("class_name") or "").strip()
    q = (request.args.get("q") or "").strip()
    limit = int(request.args.get("limit") or 50)

    if not class_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    if repo_id:
        repos = [models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()]
    else:
        repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    repos = [r for r in repos if r]
    if not repos:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    items = []
    seen = set()
    for repo in repos:
        try:
            nodes = _fetch_nodes_for_class(repo, config_uid=repo.config_uid, class_name=class_name, q=q, limit=limit) or []
        except Exception:
            nodes = []

        parsed_cfg = get_parsed_config(repo, models.db)
        for n in nodes:
            data = n.get("_data") or {}
            nid = n.get("_id") or data.get("_id") or ""
            if not nid:
                continue
            uid = _nodes_mod.normalize_own_uid(repo.config_uid, class_name, str(nid))
            if not uid or uid in seen:
                continue
            seen.add(uid)
            view = _render_class_record_view(parsed_cfg, class_name, str(nid), data)
            cover_html = ""
            try:
                cover_html = _node_cover_html(repo, class_name, str(nid), mode="table")
            except Exception:
                cover_html = ""
            items.append({
                "uid": uid,
                "_id": str(nid),
                "_class": class_name,
                "_view": str(view),
                "cover_html": cover_html,
                "data": data,
                "repo_id": repo.id,
                "repo_uid": repo.config_uid,
            })
            if len(items) >= limit:
                return jsonify({"ok": True, "items": items})

    return jsonify({"ok": True, "items": items})

@client_bp.route("/api/dataset_items")
@login_required
def api_dataset_items():
    repo_id = int(request.args.get("repo_id") or 0)
    ds_name = (request.args.get("dataset") or "").strip()
    q = (request.args.get("q") or "").strip().lower()
    limit = int(request.args.get("limit") or 100)

    if not ds_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    if repo_id:
        repos = [models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()]
    else:
        repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    repos = [r for r in repos if r]
    if not repos:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    ds = None
    repo = None
    cfg = None
    for candidate_repo in repos:
        cfg_uid = (candidate_repo.config_uid or "").strip()
        if not cfg_uid:
            continue
        candidate_cfg = main.Configuration.query.filter_by(uid=cfg_uid).first()
        if not candidate_cfg:
            continue
        candidate_ds = main.Dataset.query.filter_by(config_id=candidate_cfg.id, name=ds_name).first()
        if candidate_ds:
            repo = candidate_repo
            cfg = candidate_cfg
            ds = candidate_ds
            break

    if not repo or not cfg or not ds:
        return jsonify({"ok": False, "error": "dataset not found"}), 404

    # helper: render view_template like "Item: @name (@code)"
    tmpl_re = re.compile(r"\{([\w.]+)\}", re.UNICODE)
    def render_view(data: dict) -> str:
        if isinstance(data, dict):
            v = data.get("_view")
            if isinstance(v, str) and v.strip():
                return v.strip()

        tpl = (ds.view_template or "").strip()
        if tpl and isinstance(data, dict):
            def repl(m: re.Match) -> str:
                k = m.group(1)
                v = data.get(k)
                return "" if v is None else str(v)
            s = tmpl_re.sub(repl, tpl).strip()
            if s:
                return s
        return ""

    # load items
    items_q = main.DatasetItem.query.filter_by(dataset_id=ds.id)

    
    hard_limit = max(1, min(limit, 500))
    items = items_q.limit(hard_limit * 3).all() if q else items_q.limit(hard_limit).all()

    out = []
    pos = 0
    for it in items:
        data = it.data or {}
        if not isinstance(data, dict):
            data = {}

        item_id = (it.item_id or "").strip()
        if not item_id:
            continue

        view = render_view(data) or item_id

        # search in: item_id + view + top-level string fields
        if q:
            hay = [item_id, view]
            for _, v in data.items():
                if isinstance(v, str):
                    hay.append(v)
            if q not in (" ".join(hay).lower()):
                continue

        pos += 1
        out.append({
            "key": f"{ds_name}${item_id}",
            "_id": item_id,
            "_view": view,
            "data": data,
            "position": pos,
        })

        if len(out) >= hard_limit:
            break

    return jsonify({"ok": True, "items": out})