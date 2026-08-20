# -*- coding: utf-8 -*-
"""Editor/configuration routes extracted from app.py.

The original app uses unprefixed endpoint names in templates and redirects
(``edit_config``, ``save_method`` and so on). A normal Blueprint would prefix
these endpoint names, so this module collects route declarations and registers
them directly on the Flask app while keeping the same endpoints.
"""

import ast
import base64
import hashlib
import io
from io import BytesIO
import json
import pickle
import sqlite3
import os
import re
import smtplib
from email.message import EmailMessage
import traceback
import sys
import time
import uuid
from collections import OrderedDict
from copy import deepcopy
from typing import TYPE_CHECKING, Any
from ast import FunctionDef, fix_missing_locations, parse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from functools import wraps

import requests
from flask import (
    abort,
    after_this_request,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    make_response,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
    current_app,
)
from flask_login import current_user, login_required, login_user, logout_user
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
import sqlalchemy as sa
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pytz
from flask_babel import Babel, _, format_datetime, format_date
from sqlitedict import SqliteDict
from jinja2.sandbox import SandboxedEnvironment
from jinja2 import select_autoescape
from markupsafe import escape as html_escape
from llm_credentials import chat_completion as _shared_chat_completion, message_content as _shared_message_content

try:
    from reportlab.graphics.barcode import createBarcodeDrawing
except Exception:  # optional dependency
    createBarcodeDrawing = None

from extensions import db
from models import (
    Dataset,
    Room,
    RoomDevice,
    RoomAlias,
    User,
    UserConfigAccess,
    UserDevice,
    UserProfile,
    UserProfileRole,
    UserProfileClassAccess,
    ConfigEvent,
    ConfigEventAction,
    ConfigTimer,
    ConfigTimerAction,
    Configuration,
    ConfigSection,
    ConfigClass,
    NGenieCodeFeatureRequest,
    NGenieCodeChatMessage,
    ClassMethod,
    ClassEvent,
    EventAction,
    Contract,
    Server,
)

try:
    import qrcode
except Exception:  # pragma: no cover - optional dependency, checked at route call time
    qrcode = None


# Debug/production switch for nGenie Code lockdown.
# False: nGenie Code still marks configs with ngenie_code_locked, but the editor/export UI
#        remains available so you can inspect and debug what the LLM generated.
# True:  enforce the hard guard and show lock warnings/buttons as disabled.
NGENIE_CODE_LOCK_ENABLED = False

# nGenie Code is an internal engine for Solutions / N-Reactor.  Keep the package
# available to solutions.generator, but do not replace the ordinary configurator's
# AI Generator merely because the ngenie_code directory is installed.
NGENIE_CODE_EDITOR_ENABLED = False


def _ngenie_code_lock_enabled() -> bool:
    return bool(NGENIE_CODE_LOCK_ENABLED)


def _ngenie_code_available() -> bool:
    try:
        import ngenie_code
        return bool(ngenie_code.available())
    except Exception:
        return False


def _ngenie_code_editor_enabled() -> bool:
    # Deliberately separate from _ngenie_code_available(): Solutions / N-Reactor
    # must still see and use the installed ngenie_code package.
    return bool(NGENIE_CODE_EDITOR_ENABLED) and _ngenie_code_available()


def _config_has_ngenie_code_lock_marker(config) -> bool:
    try:
        return bool(getattr(config, 'ngenie_code_locked', False))
    except Exception:
        return False


def _config_is_ngenie_code_locked(config) -> bool:
    # The DB marker is kept, but real blocking is controlled by NGENIE_CODE_LOCK_ENABLED.
    return _ngenie_code_lock_enabled() and _config_has_ngenie_code_lock_marker(config)


def _ngenie_code_bool(value) -> bool:
    return str(value or '').strip().lower() in {'1', 'true', 'on', 'yes', 'y'}


def _ngenie_code_current_json(config) -> dict:
    try:
        return json.loads(get_config(config.uid))
    except Exception:
        return {}


def _ngenie_code_mark_locked(config) -> None:
    if hasattr(config, 'ngenie_code_locked'):
        config.ngenie_code_locked = True


def _ngenie_code_forbid_message():
    return _('This configuration is managed by nGenie Code and is locked for ordinary configurator/export operations.')




def _ngenie_code_add_chat_message(config, role: str, content: str, request_id: str = '', meta: dict | None = None, *, commit: bool = False):
    """Append one persistent nGenie Code chat message."""
    try:
        row = NGenieCodeChatMessage(
            user_id=getattr(current_user, 'id', None),
            config_id=getattr(config, 'id', None),
            config_uid=getattr(config, 'uid', '') or '',
            request_id=str(request_id or ''),
            role=(role or 'assistant')[:20],
            content=(content or '')[:30000],
            meta_json=meta or {},
        )
        db.session.add(row)
        if commit:
            db.session.commit()
        else:
            db.session.flush()
        return row
    except Exception:
        current_app.logger.exception('Could not append nGenie Code chat message')
        try:
            db.session.rollback()
        except Exception:
            pass
        return None


def _ngenie_code_chat_rows(config, limit: int = 200):
    rows = (
        db.session.query(NGenieCodeChatMessage)
        .filter(NGenieCodeChatMessage.config_id == getattr(config, 'id', None))
        .order_by(NGenieCodeChatMessage.created_at.desc())
        .limit(max(1, min(int(limit or 200), 500)))
        .all()
    )
    rows = list(reversed(rows))
    return [{
        'id': r.id,
        'created_at': r.created_at.isoformat() if getattr(r, 'created_at', None) else '',
        'role': r.role or 'assistant',
        'content': r.content or '',
        'request_id': r.request_id or '',
        'meta': r.meta_json or {},
    } for r in rows]


def _ngenie_code_parse_jsonish(value):
    """Parse JSON from a request field that may already be an object."""
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return value


def _ngenie_code_format_question_answers_for_chat(prompt: str, question_answers) -> str:
    """Human-readable chat text for structured question answers."""
    base = (prompt or "").strip()
    if not question_answers:
        return base
    lines = []
    try:
        qa = question_answers if isinstance(question_answers, dict) else {"answers": question_answers}
        questions = qa.get("questions") or []
        answers = qa.get("answers") or {}
        if isinstance(answers, list):
            answers = {str(i): v for i, v in enumerate(answers)}
        q_by_id = {str(q.get("id") or i): q for i, q in enumerate(questions) if isinstance(q, dict)}
        lines.append("Ответы на уточняющие вопросы:")
        if isinstance(answers, dict):
            for qid, answer in answers.items():
                q = q_by_id.get(str(qid)) or {}
                title = str(q.get("text") or qid).strip()
                lines.append(f"- {title}")
                if isinstance(answer, dict):
                    fields = q.get("fields") or []
                    captions = {}
                    skip_fields = set()
                    for f in fields:
                        if not isinstance(f, dict):
                            continue
                        fid = str(f.get("id") or "")
                        if bool(f.get("do_not_save")) or str(f.get("do_not_save") or "").strip().lower() in {"1", "true", "yes", "on", "да"}:
                            skip_fields.add(fid)
                            continue
                        captions[fid] = str(f.get("caption") or f.get("id") or "")
                    for fid, val in answer.items():
                        if str(fid) in skip_fields:
                            continue
                        caption = captions.get(str(fid), str(fid))
                        if not caption:
                            continue
                        lines.append(f"  - {caption}: {val}")
                else:
                    lines.append(f"  - Ответ: {answer}")
        else:
            lines.append(json.dumps(question_answers, ensure_ascii=False, indent=2))
    except Exception:
        lines = ["Ответы на уточняющие вопросы:", str(question_answers)]
    text = "\n".join(lines).strip()
    return (base + "\n\n" + text).strip() if base else text


def _ngenie_code_chat_context_for_llm(config, limit: int = 30) -> str:
    """Compact chat context for LLM: previous questions and answers are important requirements."""
    try:
        rows = _ngenie_code_chat_rows(config, limit=limit)
    except Exception:
        rows = []
    parts = []
    for row in rows[-limit:]:
        role = row.get('role') or 'assistant'
        content = (row.get('content') or '').strip()
        meta = row.get('meta') or {}
        if not content and not meta:
            continue
        if meta.get('kind') == 'questions' and meta.get('questions'):
            parts.append(f"assistant questions: {json.dumps(meta.get('questions'), ensure_ascii=False)}")
        elif meta.get('kind') == 'question_answers' and meta.get('question_answers'):
            parts.append(f"user answers: {json.dumps(meta.get('question_answers'), ensure_ascii=False)}")
        elif content:
            parts.append(f"{role}: {content[:2000]}")
    return "\n".join(parts)[-20000:]







def _optional_feature_call(module_name: str, function_name: str, *args, **kwargs):
    """Call an optional feature package without making it an editor dependency."""
    try:
        module = __import__(module_name, fromlist=[function_name])
    except (ImportError, ModuleNotFoundError):
        return None
    fn = getattr(module, function_name, None)
    if not callable(fn):
        return None
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        if exc.__class__.__name__ in {"GenerationCancelled", "GenerationBudgetExceeded"}:
            raise
        current_app.logger.exception("Optional feature helper failed: %s.%s", module_name, function_name)
        return None
















def _ngenie_code_summarize_generation(before: dict, after: dict, request_id: str = '', instruction_url: str = '') -> str:
    """Deterministic user-visible summary; avoids relying on LLM to explain what changed."""
    before = before or {}
    after = after or {}
    b_classes = {str(c.get('name') or ''): c for c in before.get('classes', []) or [] if isinstance(c, dict)}
    a_classes = {str(c.get('name') or ''): c for c in after.get('classes', []) or [] if isinstance(c, dict)}
    added = [n for n in a_classes if n and n not in b_classes]
    changed = []
    important_keys = [
        'display_name', 'class_type', 'init_screen_layout', 'init_screen_layout_web', 'record_view',
        'methods', 'events', 'indexes', 'data_structure', 'ngenie_role', 'ngenie_prompt', 'ngenie_description'
    ]
    for name, cls in a_classes.items():
        if not name or name not in b_classes:
            continue
        changed_keys = [k for k in important_keys if (b_classes[name].get(k) or None) != (cls.get(k) or None)]
        if changed_keys:
            changed.append(f"{name} ({', '.join(changed_keys[:5])}{'…' if len(changed_keys) > 5 else ''})")

    lines = ['Готово. Конфигурация обновлена и прошла валидацию nGenie Code.']
    if added:
        lines.append('Добавлены классы: ' + ', '.join(added[:20]) + (f' и еще {len(added)-20}' if len(added) > 20 else ''))
    if changed:
        lines.append('Изменены классы: ' + '; '.join(changed[:20]) + (f'; и еще {len(changed)-20}' if len(changed) > 20 else ''))
    if (before.get('sections') or None) != (after.get('sections') or None):
        lines.append('Обновлены разделы/команды конфигурации.')
    if (before.get('CommonEvents') or None) != (after.get('CommonEvents') or None):
        lines.append('Обновлены общие события.')
    if str(before.get('nodes_handlers') or '') != str(after.get('nodes_handlers') or ''):
        lines.append('Сгенерированы/обновлены Android handlers.')
    if str(before.get('nodes_server_handlers') or '') != str(after.get('nodes_server_handlers') or ''):
        lines.append('Сгенерированы/обновлены server handlers.')
    if instruction_url:
        lines.append('Инструкция создана и прикреплена к конфигурации.')
    if request_id:
        lines.append('Debug request id: ' + request_id)
    return '\n'.join(lines)


def _ngenie_code_record_feature_request(config, prompt: str, requested_feature: str, reason: str = '', llm_response: str = ''):
    """Persist a developer-visible request for a capability nGenie Code cannot implement."""
    try:
        row = NGenieCodeFeatureRequest(
            user_id=getattr(current_user, 'id', None),
            config_id=getattr(config, 'id', None),
            config_uid=getattr(config, 'uid', '') or '',
            config_name=getattr(config, 'name', '') or '',
            prompt=(prompt or '')[:20000],
            requested_feature=(requested_feature or '')[:1000],
            reason=(reason or '')[:4000],
            llm_response=(llm_response or '')[:20000],
            status='new',
        )
        db.session.add(row)
        db.session.flush()
        return row
    except Exception:
        current_app.logger.exception('Could not record nGenie Code feature request')
        return None


def _ngenie_code_unavailable_is_handler_patch_contract(unavailable: dict) -> bool:
    """Treat "handlers are forbidden in JSON patch" as routing noise, not a missing platform feature."""
    if not isinstance(unavailable, dict):
        return False
    text = " ".join(str(unavailable.get(k) or "") for k in ("requested_feature", "requested", "feature", "reason", "details"))
    low = text.lower()
    return any(x in low for x in (
        "nodes_handlers", "nodes_server_handlers", "handler", "handlers",
        "обработчик", "обработчиков", "pure configuration", "pure config",
        "json patch", "patch output rules", "контракт генерации"
    ))


def _ngenie_code_minimal_ack_patch():
    try:
        import ngenie_code
        return {"_ngenie_code_instruction_ack": ngenie_code.required_instruction_ack()}
    except Exception:
        return {}


def _ngenie_code_validation_errors_look_like_missing_feature(errors):
    text_value = '\n'.join(str(e or '') for e in (errors or []))
    needles = (
        'unknown UI type', 'unknown input type', 'unsupported UI type', 'unsupported input type',
        'no such ui', 'not supported', 'missing library', 'missing component', 'unknown command',
    )
    low = text_value.lower()
    return any(n.lower() in low for n in needles)


_NGENIE_CODE_LOCK_ALLOWED_ENDPOINTS = {
    'edit_config',
    'ai_generate',
    'ai_generate_layout',
    'ngenie_code_document',
    'ngenie_code_write_instruction',
    'ngenie_code_generate_example',
    'ngenie_code_debug_file',
    'ngenie_code_chat',
    'ngenie_code_chat_add',
    'ngenie_code_question_answers',
    'ngenie_code_chat_new',
}

# A generated Solution configuration is deliberately hidden from Designer, but
# the Solution workspace still has to use a very small subset of nGenie Code
# endpoints. Do not broaden this list to normal editor CRUD routes: users must
# continue working only through /solutions/<uid>/work.
_SOLUTION_WORKSPACE_ALLOWED_HIDDEN_ENDPOINTS = {
    'ai_generate',
    'ngenie_code_chat',
    'ngenie_code_chat_add',
    'ngenie_code_question_answers',
    'ngenie_code_chat_new',
}


def _hidden_config_is_owned_solution(config) -> bool:
    """Return True only when the hidden config belongs to current user's Solution.

    ``designer_hidden`` is shared by installed demo products and generated
    Solutions. Demo products must keep returning 404 for all Designer routes;
    only an actually linked Solution may call the narrow workspace API above.
    The import stays optional so deployments without the ``solutions`` folder
    preserve the old behavior.
    """
    if config is None or not getattr(current_user, 'is_authenticated', False):
        return False
    try:
        from solutions.engine import active_solution_for_config
        return active_solution_for_config(config, user=current_user) is not None
    except Exception:
        return False


def _hidden_config_endpoint_allowed_for_solution(endpoint, config) -> bool:
    return (
        str(endpoint or '') in _SOLUTION_WORKSPACE_ALLOWED_HIDDEN_ENDPOINTS
        and _hidden_config_is_owned_solution(config)
    )


def _ngenie_code_owned_config_by_uid(uid):
    uid = str(uid or '').strip()
    if not uid or not getattr(current_user, 'is_authenticated', False):
        return None
    return db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()


def _ngenie_code_config_from_route_kwargs(kwargs):
    """Best-effort resolution of the configuration touched by an editor route."""
    if not isinstance(kwargs, dict):
        kwargs = {}

    uid = kwargs.get('uid') or kwargs.get('config_uid')
    if uid:
        cfg = _ngenie_code_owned_config_by_uid(uid)
        if cfg is not None:
            return cfg

    class_id = kwargs.get('class_id')
    if class_id is not None:
        obj = db.session.get(ConfigClass, class_id)
        if obj is not None and obj.config and obj.config.user_id == current_user.id:
            return obj.config

    method_id = kwargs.get('method_id')
    if method_id is not None:
        obj = db.session.get(ClassMethod, method_id)
        cfg = getattr(getattr(obj, 'class_obj', None), 'config', None) if obj is not None else None
        if cfg is not None and cfg.user_id == current_user.id:
            return cfg

    dataset_id = kwargs.get('dataset_id')
    if dataset_id is not None:
        obj = db.session.get(Dataset, dataset_id)
        cfg = getattr(obj, 'config', None) if obj is not None else None
        if cfg is not None and cfg.user_id == current_user.id:
            return cfg

    section_id = kwargs.get('section_id')
    if section_id is not None:
        obj = db.session.get(ConfigSection, section_id)
        cfg = getattr(obj, 'config', None) if obj is not None else None
        if cfg is not None and cfg.user_id == current_user.id:
            return cfg

    server_id = kwargs.get('server_id')
    if server_id is not None:
        obj = db.session.get(Server, server_id)
        cfg = getattr(obj, 'config', None) if obj is not None else None
        if cfg is not None and cfg.user_id == current_user.id:
            return cfg

    alias_id = kwargs.get('alias_id')
    if alias_id is not None:
        obj = db.session.get(RoomAlias, alias_id)
        cfg = getattr(obj, 'config', None) if obj is not None else None
        if cfg is not None and cfg.user_id == current_user.id:
            return cfg

    return None


def _ngenie_code_should_block_endpoint(endpoint, method):
    endpoint = str(endpoint or '')
    method = str(method or 'GET').upper()
    if endpoint in _NGENIE_CODE_LOCK_ALLOWED_ENDPOINTS:
        return False
    # edit_config GET is the container for the nGenie Code chat; all normal POST saves are blocked.
    if endpoint == 'edit_config' and method == 'GET':
        return False
    return True


def _ngenie_code_guard_view(endpoint, view_func):
    @wraps(view_func)
    def _wrapped(*args, **kwargs):
        try:
            cfg = _ngenie_code_config_from_route_kwargs(kwargs)
            # Installed demo copies and generated Solution configs are
            # runtime/client instances and remain hidden from Designer. A linked
            # Solution, however, needs the narrow nGenie workspace API so its
            # visible chat can load, save answers and run generation.
            if cfg is not None and _mark_installed_demo_copy_hidden(cfg):
                if not _hidden_config_endpoint_allowed_for_solution(endpoint, cfg):
                    abort(404)
            if cfg is not None and _config_is_ngenie_code_locked(cfg):
                if _ngenie_code_should_block_endpoint(endpoint, request.method):
                    abort(403, description=_ngenie_code_forbid_message())
        except Exception:
            # Do not let the guard hide the original route's own 404/403 logic,
            # except for explicit aborts raised above.
            raise
        return view_func(*args, **kwargs)
    return _wrapped


def _is_probably_print_template_base64(value: Any) -> bool:
    s = str(value or '').strip()
    if not s or len(s) % 4:
        return False
    try:
        raw = base64.b64decode(s.encode('ascii'), validate=True)
        text = raw.decode('utf-8')
    except Exception:
        return False
    return '\x00' not in text


def _decode_print_html_template(value: Any) -> str:
    s = str(value or '')
    if _is_probably_print_template_base64(s):
        try:
            return base64.b64decode(s.strip().encode('ascii'), validate=True).decode('utf-8')
        except Exception:
            return s
    return s


def _encode_print_html_template(value: Any) -> str:
    s = str(value or '')
    if not s:
        return ''
    if _is_probably_print_template_base64(s):
        return s.strip()
    return base64.b64encode(s.encode('utf-8')).decode('ascii')


class _PrintAttrDict(dict):
    """Dictionary wrapper for PrintForm templates.

    Jinja already supports dict.key access, but keys such as `items`, `keys`,
    or `values` collide with dict methods. This wrapper makes dot-access prefer
    data keys while staying limited to values explicitly present in _data.
    """
    def __getattribute__(self, name):
        if name.startswith('__'):
            return dict.__getattribute__(self, name)
        try:
            return dict.__getitem__(self, name)
        except KeyError:
            return dict.__getattribute__(self, name)


def _print_attr_tree(value):
    if isinstance(value, dict):
        return _PrintAttrDict({k: _print_attr_tree(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_print_attr_tree(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_print_attr_tree(v) for v in value)
    return value


class _PrintSandboxedEnvironment(SandboxedEnvironment):
    def is_safe_attribute(self, obj, attr, value):
        if isinstance(obj, _PrintAttrDict) and attr in obj and not str(attr).startswith('__'):
            return True
        return super().is_safe_attribute(obj, attr, value)


def _normalize_print_targets(raw):
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
    return [x.strip() for x in s.split(',') if x.strip()]


def _print_qr_data_url(value):
    if qrcode is None:
        return ''
    try:
        img = qrcode.make(str(value or ''))
        buf = BytesIO()
        img.save(buf, format='PNG')
        return 'data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode('ascii')
    except Exception:
        return ''


def _print_barcode_data_url(value, bar_height=44, bar_width=1.15, human_readable=False):
    raw = str(value or '').strip()
    if not raw or createBarcodeDrawing is None:
        return ''
    try:
        height = max(8.0, min(float(bar_height or 44), 300.0))
        width = max(0.2, min(float(bar_width or 1.15), 4.0))
        drawing = createBarcodeDrawing(
            'Code128',
            value=raw,
            barHeight=height,
            barWidth=width,
            humanReadable=bool(human_readable),
        )
        png = drawing.asString('png')
        return 'data:image/png;base64,' + base64.b64encode(png).decode('ascii')
    except Exception:
        return ''


def _render_print_html_template(template_text, data):
    template_text = _decode_print_html_template(template_text)
    data = data if isinstance(data, dict) else {}
    wrapped_data = _print_attr_tree(data)
    ctx = {'_data': wrapped_data, 'data': wrapped_data, 'qr': _print_qr_data_url, 'barcode': _print_barcode_data_url}
    for k, v in data.items():
        key = str(k)
        if key:
            wrapped_value = _print_attr_tree(v)
            ctx[key] = wrapped_value
            ctx['_' + key.lstrip('_')] = wrapped_value
    env = _PrintSandboxedEnvironment(autoescape=select_autoescape(['html', 'xml']))
    env.globals.update(qr=_print_qr_data_url, barcode=_print_barcode_data_url)
    return env.from_string(template_text or '').render(**ctx)


class _RouteCollector:
    def __init__(self):
        self.rules = []

    def route(self, rule, **options):
        def decorator(view_func):
            self.rules.append((rule, dict(options), view_func))
            return view_func
        return decorator

    def get(self, rule, **options):
        options = dict(options)
        options.setdefault("methods", ["GET"])
        return self.route(rule, **options)

    def post(self, rule, **options):
        options = dict(options)
        options.setdefault("methods", ["POST"])
        return self.route(rule, **options)


_routes = _RouteCollector()


# Names intentionally supplied by app.py at registration time.  Models and db are
# imported directly above; these are shared runtime/API helpers or deployment
# settings that still live in app.py.
_REQUIRED_APP_CONTEXT_NAMES = (
    'ADMIN_LOGIN',
    'DEEPSEEK_API_KEY',
    'NL_FORMAT',
    'NMAKER_SERVER_URL',
    'S3_BUCKET',
    'S3_ENDPOINT',
    'TASKS_DB_PATH',
    '_contract_accessible_configs',
    '_contract_add_payload',
    '_contract_recreate_nodes_for_contract',
    '_contract_total_object_count',
    '_contract_update_from_data',
    '_export_class_json',
    '_get_owned_contract_or_404',
    '_is_http_request_method',
    '_is_script_text_method',
    '_load_server_handlers_ns',
    '_nodes_mod',
    '_runtime_cache_invalidate',
    '_runtime_download_text_cached',
    '_s3_key_from_public_url',
    'active_connections',
    'get_config',
    'get_ws_scheme',
    'user_can_access_config',
    's3',
)

if TYPE_CHECKING:
    ADMIN_LOGIN: str
    DEEPSEEK_API_KEY: str
    NL_FORMAT: str
    NMAKER_SERVER_URL: str
    S3_BUCKET: str
    S3_ENDPOINT: str
    TASKS_DB_PATH: str
    active_connections: Any
    s3: Any
    _contract_accessible_configs: Any
    _contract_add_payload: Any
    _contract_recreate_nodes_for_contract: Any
    _contract_total_object_count: Any
    _contract_update_from_data: Any
    _export_class_json: Any
    _get_owned_contract_or_404: Any
    _is_http_request_method: Any
    _is_script_text_method: Any
    _load_server_handlers_ns: Any
    _nodes_mod: Any
    _runtime_cache_invalidate: Any
    _runtime_download_text_cached: Any
    _s3_key_from_public_url: Any
    get_config: Any
    get_ws_scheme: Any
    user_can_access_config: Any


def _current_user_has_admin_login() -> bool:
    """Return True only for the account configured as ADMIN_LOGIN."""
    try:
        current_email = str(getattr(current_user, 'email', '') or '').strip().casefold()
        admin_email = str(ADMIN_LOGIN or '').strip().casefold()
        return bool(admin_email) and current_email == admin_email
    except Exception:
        return False


def _designer_visible_configuration_clause():
    """SQL predicate for configurations that may be opened in Designer."""
    return sa.or_(
        Configuration.designer_hidden == False,
        Configuration.designer_hidden.is_(None),
    )


def _configuration_is_installed_demo_copy(config) -> bool:
    """Recognize current and legacy demo installations without affecting Client/API.

    New installations carry ``designer_hidden`` and ``demo_source_uid`` explicitly.
    Older installations are recognized only when all of the following are true:
    the configuration is owned by the account, is present in that account's Client
    repository, is not itself a published demo, and its content UID matches a demo
    published by another account.
    """
    if config is None:
        return False
    if bool(getattr(config, 'designer_hidden', False)):
        return True
    if bool(getattr(config, 'demo_product', False)) or bool(getattr(config, 'is_system', False)):
        return False
    if str(getattr(config, 'demo_source_uid', '') or '').strip():
        return True
    content_uid = str(getattr(config, 'content_uid', '') or '').strip()
    config_uid = str(getattr(config, 'uid', '') or '').strip()
    owner_id = getattr(config, 'user_id', None)
    if not content_uid or not config_uid or owner_id is None:
        return False
    try:
        from client_app import models as client_models
        repo_exists = db.session.execute(
            select(client_models.Repo.id).where(
                client_models.Repo.user_id == int(owner_id),
                client_models.Repo.config_uid == config_uid,
            )
        ).scalar_one_or_none()
        if not repo_exists:
            return False
        source = db.session.execute(
            select(Configuration.uid).where(
                Configuration.demo_product == True,
                Configuration.content_uid == content_uid,
                Configuration.user_id != int(owner_id),
                sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
            ).limit(1)
        ).scalar_one_or_none()
        return bool(source)
    except Exception:
        return False


def _mark_installed_demo_copy_hidden(config) -> bool:
    """Persist the Designer-only visibility marker for old installed copies."""
    if not _configuration_is_installed_demo_copy(config):
        return False
    if not bool(getattr(config, 'designer_hidden', False)):
        config.designer_hidden = True
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
    return True


def _backfill_installed_demo_visibility(owner_user_id) -> int:
    """Best-effort one-time upgrade for demos installed before the marker existed."""
    try:
        owner_id = int(owner_user_id)
    except Exception:
        return 0
    try:
        from client_app import models as client_models
        repo_uids = [
            str(x) for x in db.session.execute(
                select(client_models.Repo.config_uid).where(client_models.Repo.user_id == owner_id)
            ).scalars().all() if str(x or '').strip()
        ]
        if not repo_uids:
            return 0
        configs = db.session.execute(
            select(Configuration).where(
                Configuration.user_id == owner_id,
                Configuration.uid.in_(repo_uids),
                sa.or_(Configuration.designer_hidden == False, Configuration.designer_hidden.is_(None)),
                sa.or_(Configuration.demo_product == False, Configuration.demo_product.is_(None)),
                sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
            )
        ).scalars().all()
        changed = 0
        for cfg in configs:
            if _configuration_is_installed_demo_copy(cfg):
                cfg.designer_hidden = True
                changed += 1
        if changed:
            db.session.commit()
        return changed
    except Exception:
        db.session.rollback()
        return 0


def bind_editor_context(context):
    """Bind shared app.py globals used by the extracted editor code."""
    missing = [name for name in _REQUIRED_APP_CONTEXT_NAMES if name not in context]
    if missing:
        raise RuntimeError('Editor routes are missing app context names: ' + ', '.join(missing))
    for name in _REQUIRED_APP_CONTEXT_NAMES:
        globals()[name] = context[name]


def register_editor_routes(flask_app, context=None):
    """Register extracted routes on the Flask app preserving old endpoints."""
    if context is not None:
        bind_editor_context(context)

    for rule, options, view_func in _routes.rules:
        opts = dict(options)
        endpoint = opts.pop("endpoint", None) or view_func.__name__
        if endpoint in flask_app.view_functions:
            continue
        flask_app.add_url_rule(rule, endpoint, _ngenie_code_guard_view(endpoint, view_func), **opts)

    if context is not None:
        for name in MOVED_EDITOR_NAMES:
            if name in globals():
                context[name] = globals()[name]

def init_editor_ui(flask_app):
    """Initialize designer/editor UI hooks: Babel, template helpers and access guard."""
    flask_app.config['BABEL_DEFAULT_LOCALE'] = 'en'
    babel = Babel(flask_app, locale_selector=get_locale, timezone_selector=get_timezone)
    flask_app.context_processor(utility_processor)
    # Keep these available as true Jinja globals as well as context values.
    # This is needed for shared templates (for example templates/base.html and
    # templates/client/settings.html) that can be rendered outside the editor
    # routes after the Babel/editor split.
    flask_app.jinja_env.globals.update(
        get_locale=get_locale,
        LANGUAGES=LANGUAGES,
        format_datetime=format_datetime,
        format_date=format_date,
    )
    flask_app.template_filter('b64decode')(b64decode_filter)
    flask_app.before_request(_enforce_web_access_modes)
    flask_app.before_request(before_request)
    flask_app.after_request(update_config_timestamp)
    return babel


def get_default_server_handlers():
    """Default server handler header used when an API creates a configuration."""
    return NODE_CLASS_CODE



def extract_method_body_from_code(module_code, class_name, method_name):
    
    try:
        tree = ast.parse(module_code)
        
        for node in ast.walk(tree):
            if (isinstance(node, ast.ClassDef) and 
                node.name == class_name):
                
                for class_node in node.body:
                    if (isinstance(class_node, ast.FunctionDef) and 
                        class_node.name == method_name):
                        
                        # Get start and end lines method
                        start_line = class_node.lineno - 1
                        end_line = class_node.end_lineno
                        
                        # Split code into lines
                        lines = module_code.split('\n')
                        
                        # Extract lines body method
                        body_lines = []
                        for i in range(start_line + 1, end_line):
                            if i >= len(lines):
                                break
                            line = lines[i]
                            # Remove indentation (first 8 spaces, corresponding indent method)
                            if line.startswith(' ' * 8):
                                line = line[8:]
                            elif line.startswith('    ' * 2):  # Alternative option: 2 levels indentation
                                line = line[8:]
                            body_lines.append(line)
                        
                        # Join and return body method without indentation
                        return '\n'.join(body_lines).rstrip()
        
        return None
    except Exception as e:
        print(f"Error extracting method body for {class_name}.{method_name}: {str(e)}")
        return None

def sync_methods_from_code(config, exclude_methods=None):
    
    if not config.nodes_handlers and not config.nodes_server_handlers:
        return
    
    try:
        #print(f"Syncing methods for config: {config.name}")
        
        # For Android/Python handlers
        if config.nodes_handlers:
            module_code = base64.b64decode(config.nodes_handlers).decode('utf-8')
            #print(f"Android handlers code length: {len(module_code)}")
            sync_android_methods_from_code(config, module_code, exclude_methods)
        
        # For Server /Python handlers
        if config.nodes_server_handlers:
            module_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            #print(f"Server handlers code length: {len(module_code)}")
            sync_server_methods_from_code(config, module_code, exclude_methods)
        
        db.session.commit()
        
    except Exception as e:
        print(f"Error syncing methods from code: {str(e)}")
        db.session.rollback()

def sync_android_methods_from_code(config, module_code, exclude_methods=None):
    
    # Find all methods inside classes (excluding methods class Node)
    code_methods = {}
    tree = ast.parse(module_code)
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_name = node.name
            # Skip base class Node
            if class_name == 'Node':
                continue
                
            code_methods[class_name] = []
            
            for class_node in node.body:
                if isinstance(class_node, ast.FunctionDef):
                    method_name = class_node.name
                    # Skip magic methods, private and example_method
                    if (not method_name.startswith('__') and 
                        method_name != 'example_method' and
                        method_name != '__init__'):
                        code_methods[class_name].append(method_name)
    
    # Sync with database
    for class_obj in config.classes:
        if class_obj.name in code_methods:
            # Existing methods in DB for Android/Python
            existing_methods = {m.code: m for m in class_obj.methods 
                              if m.engine == 'android_python'}
            
            # Methods from code-Add new
            for method_name in code_methods[class_obj.name]:
                if method_name not in existing_methods:
                    # Create new method in DB (only if not in exclusions)
                    if exclude_methods and (class_obj.name, method_name) in exclude_methods:
                        continue
                        
                    new_method = ClassMethod(
                        name=method_name,
                        source='internal',
                        engine='android_python',
                        code=method_name,
                        class_id=class_obj.id
                    )
                    db.session.add(new_method)
                    #print(f"Added Android method from code: {class_obj.name}.{method_name}")
            
            # Remove methods, that are not in code (except exclusions)
            for method_code, method_obj in existing_methods.items():
                if (method_code not in code_methods[class_obj.name] and 
                    not (exclude_methods and (class_obj.name, method_code) in exclude_methods)):
                    # Not remove methods, that were added via UI
                    if method_obj.name != method_code:
                        continue
                    db.session.delete(method_obj)

def remove_method_from_code(config, class_name, method_name, engine):
    
    try:
        if engine == 'android_python' and config.nodes_handlers:
            module_code = base64.b64decode(config.nodes_handlers).decode('utf-8')
            
            
            is_valid, error = validate_python_syntax(module_code)
            if not is_valid:
                flash(f"Invalid module syntax before removal: {error}", 'danger')
                return False
            
            updated_code = remove_method_from_module(module_code, class_name, method_name)
            
            
            is_valid, error = validate_python_syntax(updated_code)
            if not is_valid:
                flash(f"Invalid module syntax after method removal: {error}", 'danger')
                return False
                
            config.nodes_handlers = base64.b64encode(updated_code.encode('utf-8')).decode('utf-8')
            db.session.add(config)
            db.session.commit() 
            print(f"Removed method from Android code: {class_name}.{method_name}")
            return True
        
        elif engine == 'server_python' and config.nodes_server_handlers:
            module_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            
            is_valid, error = validate_python_syntax(module_code)
            if not is_valid:
                flash(f"Invalid module syntax before removal: {error}", 'danger')
                return False
            
            updated_code = remove_method_from_module(module_code, class_name, method_name)
            
            
            is_valid, error = validate_python_syntax(updated_code)
            if not is_valid:
                flash(f"Invalid module syntax after method removal: {error}", 'danger')
                return False
                
            config.nodes_server_handlers = base64.b64encode(updated_code.encode('utf-8')).decode('utf-8')
            
            # Also update the server handlers file
            handlers_dir = os.path.join('Handlers', config.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                f.write(updated_code)

            db.session.add(config)
            db.session.commit()    
            print(f"Removed method from Server code: {class_name}.{method_name}")
            return True
            
    except Exception as e:
        #print(f"Error removing method from code: {str(e)}")
        flash(f"Error removing method from code: {str(e)}", 'danger')
        return False

def remove_method_from_module(module_code, class_name, method_name):
    
    lines = module_code.split('\n')
    class_start = -1
    class_indent = 0
    in_target_class = False
    
    # Search start target class
    for i, line in enumerate(lines):
        if line.strip().startswith(f'class {class_name}('):
            class_start = i
            class_indent = len(line) - len(line.lstrip())
            in_target_class = True
            break
    
    if class_start == -1:
        return module_code  # Class not found
    
    # Search method inside target class
    method_start = -1
    method_end = -1
    in_method = False
    method_indent = 0
    method_found = False
    
    for i in range(class_start + 1, len(lines)):
        line = lines[i]
        current_indent = len(line) - len(line.lstrip())
        
        # If exited za bounds class
        if current_indent <= class_indent and line.strip():
            break
        
        # Found start method inside target class
        if (line.strip().startswith(f'def {method_name}(') and 
            current_indent > class_indent and
            in_target_class and not method_found):
            method_start = i
            method_indent = current_indent
            in_method = True
            method_found = True
            continue
        
        # If inside method
        if in_method:
            
            if current_indent <= method_indent and line.strip():
                method_end = i
                break
            
            # If this is end line
            if i == len(lines) - 1:
                method_end = i + 1
                break
    
    # Delete if method found
    if method_start != -1 and method_end != -1:
        new_lines = lines[:method_start] + lines[method_end:]
        return '\n'.join(new_lines)
    
    return module_code

def sync_server_methods_from_code(config, module_code, exclude_methods=None):
    
    
    code_methods = {}
    tree = ast.parse(module_code)
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_name = node.name
            code_methods[class_name] = []
            
            for class_node in node.body:
                if isinstance(class_node, ast.FunctionDef):
                    method_name = class_node.name
                    # Skip magic methods, private and example_method
                    if (not method_name.startswith('__') and 
                        method_name != 'example_method' and
                        method_name != '__init__'):
                        code_methods[class_name].append(method_name)
    
    # sync with DB
    for class_obj in config.classes:
        if class_obj.name in code_methods:
            # Existing methods in DB for Server /Python
            existing_methods = {m.code: m for m in class_obj.methods 
                              if m.engine == 'server_python'}
            
            # Methods from code-Add new
            for method_name in code_methods[class_obj.name]:
                if method_name not in existing_methods:
                    # Create new method in DB (only if not in exclusions)
                    if exclude_methods and (class_obj.name, method_name) in exclude_methods:
                        continue
                        
                    new_method = ClassMethod(
                        name=method_name,
                        source='internal',
                        engine='server_python',
                        code=method_name,
                        class_id=class_obj.id
                    )
                    db.session.add(new_method)
                    print(f"Added Server method from code: {class_obj.name}.{method_name}")
            
            # Remove methods, that are not in code (except exclusions)
            for method_code, method_obj in existing_methods.items():
                if (method_code not in code_methods[class_obj.name] and 
                    not (exclude_methods and (class_obj.name, method_code) in exclude_methods)):
                    # Not remove methods, that were added via UI
                    if method_obj.name != method_code:
                        continue
                    db.session.delete(method_obj)
                    print(f"Removed Server method not in code: {class_obj.name}.{method_code}")

def add_new_method_to_class(module_code, class_name, method_name, method_body):

    lines = module_code.split('\n')
    class_start = -1
    class_indent = 0
    
    #  Search start class
    for i, line in enumerate(lines):
        if line.strip().startswith(f'class {class_name}('):
            class_start = i
            class_indent = len(line) - len(line.lstrip())
            break
    
    if class_start == -1:
        return module_code  # Class not found
    
    # Search end class
    class_end = -1
    for i in range(class_start + 1, len(lines)):
        current_indent = len(lines[i]) - len(lines[i].lstrip())
        if current_indent <= class_indent and lines[i].strip():
            class_end = i
            break
    
    if class_end == -1:
        class_end = len(lines)
    

    method_indent = ' ' * (class_indent + 4)
    body_indent = ' ' * (class_indent + 8)
    
    method_code = f'{method_indent}def {method_name}(self, input_data=None):\n'
    
    # Add method with intendations
    for line in method_body.split('\n'):
        
        if line.strip():
            method_code += f'{body_indent}{line}\n'
        else:
            method_code += f'{body_indent}\n'  
    
    # check tuple return
    has_return_tuple = any('return True,' in line or 'return False,' in line for line in method_body.split('\n'))
    
    if not has_return_tuple:
        method_code += f'{body_indent}return True, {{}}\n'
    
    # past method
    new_lines = lines[:class_end] + [method_code] + lines[class_end:]
    return '\n'.join(new_lines)

def add_method_to_class(module_code, class_name, method_name, method_body):
    
    is_valid, error = validate_python_syntax(module_code)
    if not is_valid:
        flash(f"Invalid module syntax before changes: {error}", 'danger')
        return None
    

    if method_exists_in_code(module_code, class_name, method_name):
        updated_code = update_existing_method(module_code, class_name, method_name, method_body)
    else:
        updated_code = add_new_method_to_class(module_code, class_name, method_name, method_body)
    
    is_valid, error = validate_python_syntax(updated_code)
    if not is_valid:
        flash(f"Invalid module syntax after method addition: {error}", 'danger')
        return None
    
    return updated_code

def update_existing_method(module_code, class_name, method_name, new_body):
    
    lines = module_code.split('\n')
    class_start = -1
    class_indent = 0
    in_target_class = False
    

    for i, line in enumerate(lines):
        if line.strip().startswith(f'class {class_name}('):
            class_start = i
            class_indent = len(line) - len(line.lstrip())
            in_target_class = True
            break
    
    if class_start == -1:
        return module_code  
    

    method_start = -1
    method_indent = 0
    method_found = False
    
    for i in range(class_start + 1, len(lines)):
        line = lines[i]
        current_indent = len(line) - len(line.lstrip())
        

        if current_indent <= class_indent and line.strip():
            break
        

        if (line.strip().startswith(f'def {method_name}(') and 
            current_indent > class_indent and
            in_target_class):
            method_start = i
            method_indent = current_indent
            method_found = True
            break
    
    if not method_found or method_start == -1:
        return module_code  
    

    method_end = -1
    for i in range(method_start + 1, len(lines)):
        current_indent = len(lines[i]) - len(lines[i].lstrip())
        if current_indent <= method_indent and lines[i].strip():
            method_end = i
            break
    
    if method_end == -1:
        method_end = len(lines)
    

    body_indent = ' ' * (method_indent + 4)
    new_method_lines = [lines[method_start]]  
    

    for line in new_body.split('\n'):
        if line.strip():  
            new_method_lines.append(f'{body_indent}{line}')
        else:  
            new_method_lines.append('')
    

    new_lines = lines[:method_start] + new_method_lines + lines[method_end:]
    return '\n'.join(new_lines)

def validate_python_syntax(code):

    try:
        ast.parse(code)
        return True, None
    except SyntaxError as e:
        error_msg = f"Syntax error {e.lineno}: {e.msg}"
        return False, error_msg
    except Exception as e:
        return False, f"Validation fault: {str(e)}"

@_routes.route("/api/s3/text-upload-url", methods=["POST"])
@login_required
def get_s3_text_upload_url():
    data = request.get_json(silent=True) or {}
    filename = secure_filename(data.get("filename") or "script.py") or "script.py"
    if not filename.lower().endswith(".py"):
        filename += ".py"

    content_type = "text/x-python; charset=utf-8"
    object_key = f"uploads/python_scripts/{current_user.id}/{uuid.uuid4().hex}_{filename}"

    upload_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": object_key,
            "ContentType": content_type,
        },
        ExpiresIn=600,
    )
    public_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{object_key}"

    return jsonify({
        "ok": True,
        "upload_url": upload_url,
        "file_url": public_url,
        "url": public_url,
        "public_url": public_url,
        "object_key": object_key,
        "key": object_key,
        "headers": {"Content-Type": content_type},
        "method": "PUT",
        "expires_in": 600,
    })

def _s3_text_content_type(filename: str = "script.py") -> str:
    return "text/x-python; charset=utf-8"

def _is_remote_script_ref(value: str) -> bool:
    """True only for explicit URL/S3-key refs, not arbitrary inline code."""
    s = str(value or "").strip()
    if not s:
        return False
    parsed = urlparse(s)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return True
    # Accept explicit raw keys saved by this editor. Do NOT call
    # _s3_key_from_public_url here because it treats any plain text as a key.
    return s.startswith("uploads/python_scripts/")

_PY_SCRIPT_UPLOAD_SESSION_KEY = "last_python_script_s3_upload"

def _remember_python_script_upload(public_url: str, object_key: str = "", filename: str = "") -> None:
    """Remember the last PythonScript S3 save for the current browser session.

    Some existing editor templates save the text to S3 in a popup/editor, but the
    parent edit-event form may still submit an empty methodText field.  The old
    flow relied on the browser propagating file_url back into actions_json; this
    server-side fallback prevents a successful S3 save from being lost on the
    subsequent "Сохранить".
    """
    try:
        session[_PY_SCRIPT_UPLOAD_SESSION_KEY] = {
            "url": str(public_url or ""),
            "file_url": str(public_url or ""),
            "object_key": str(object_key or ""),
            "filename": str(filename or ""),
            "ts": time.time(),
        }
        session.modified = True
    except Exception:
        pass

def _last_python_script_upload_url(max_age_seconds: int = 3600) -> str:
    try:
        rec = session.get(_PY_SCRIPT_UPLOAD_SESSION_KEY) or {}
        url = str(rec.get("url") or rec.get("file_url") or "").strip()
        ts = float(rec.get("ts") or 0)
        if url and (not ts or (time.time() - ts) <= max_age_seconds):
            return url
    except Exception:
        pass
    return ""

def _normalize_special_method_name_for_export(value):
    value = str(value or "").strip()
    return "HTTPRequest" if value == "HTTP Request" else value

def _action_method_value(action_data, key):
    if not isinstance(action_data, dict):
        return ""
    return _normalize_special_method_name_for_export(action_data.get(key, ""))

def _is_builtin_action_method(value):
    value = str(value or "").strip()
    return value in ("NodaScript", "PythonScript", "HTTPRequest", "HTTP Request")

def _action_method_text_value(action_data, *, post=False):
    return _action_python_text_value(action_data, post=post) if isinstance(action_data, dict) else ""

def _action_python_text_value(action: dict, *, post: bool = False) -> str:
    """Return a PythonScript ref from all known UI key variants."""
    if not isinstance(action, dict):
        return ""
    keys = (
        [
            "postExecuteMethodText",
            "post_execute_text",
            "postExecuteText",
            "postMethodText",
            "postExecuteMethodTextUrl",
            "postExecuteMethodTextURL",
            "postExecuteUrl",
            "postExecuteURL",
            "post_url",
            "postFileUrl",
            "post_file_url",
        ]
        if post else
        [
            "methodText",
            "method_text",
            "code",
            "script",
            "sourceCode",
            "methodTextUrl",
            "methodTextURL",
            "scriptUrl",
            "scriptURL",
            "pythonScriptUrl",
            "pythonScriptURL",
            "fileUrl",
            "file_url",
            "url",
        ]
    )
    for k in keys:
        v = action.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _carry_existing_event_python_script_refs(actions: list, existing_actions) -> None:
    """Preserve old method_text/post_execute_text when the submitted JSON is empty.

    This is intentionally conservative: it only fills empty PythonScript fields,
    and it matches by action order because EventAction rows are rewritten on save.
    """
    if not isinstance(actions, list):
        return
    old = list(existing_actions or [])
    for idx, a in enumerate(actions):
        if not isinstance(a, dict):
            continue
        old_action = old[idx] if idx < len(old) else None
        if (a.get("method") or "") == "PythonScript" and not _action_python_text_value(a, post=False):
            old_text = str(getattr(old_action, "method_text", "") or getattr(old_action, "methodText", "") or "").strip() if old_action is not None else ""
            if old_text:
                a["methodText"] = old_text
        if (a.get("postExecuteMethod") or a.get("post_execute_method") or "") == "PythonScript" and not _action_python_text_value(a, post=True):
            old_text = str(getattr(old_action, "post_execute_text", "") or getattr(old_action, "postExecuteMethodText", "") or "").strip() if old_action is not None else ""
            if old_text:
                a["postExecuteMethodText"] = old_text

def _save_python_text_to_s3_via_upload_url(text_value: str, *, filename: str = "script.py", old_url: str = "") -> dict:
    """Save PythonScript text to S3 using the same presigned PUT flow as the editor.

    This keeps server-side form saves compatible with the browser's "Save to S3"
    button and avoids boto3 put_object checksum issues on this S3-compatible storage.
    """
    filename = secure_filename(filename or "script.py") or "script.py"
    if not filename.lower().endswith(".py"):
        filename += ".py"

    content_type = _s3_text_content_type(filename)
    object_key = f"uploads/python_scripts/{current_user.id}/{uuid.uuid4().hex}_{filename}"
    upload_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": object_key,
            "ContentType": content_type,
        },
        ExpiresIn=600,
    )

    raw = str(text_value or "").encode("utf-8")
    resp = requests.put(upload_url, data=raw, headers={"Content-Type": content_type}, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(f"S3 upload failed: HTTP {resp.status_code}: {resp.text[:500]}")

    public_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{object_key}"

    # The old URL may be reused by runtime/editor cache. Drop both old and new cache entries.
    try:
        if old_url:
            _runtime_cache_invalidate(old_url)
        _runtime_cache_invalidate(public_url)
    except Exception:
        pass

    _remember_python_script_upload(public_url, object_key, filename)

    return {
        "ok": True,
        "file_url": public_url,
        "url": public_url,
        "public_url": public_url,
        "object_key": object_key,
        "key": object_key,
        "bytes": len(raw),
    }

def _normalize_python_script_text_for_save(value: str, *, filename: str, old_url: str = "") -> str:
    """Return a S3 URL for inline PythonScript text; keep existing URL/key refs."""
    s = str(value or "").strip()
    if not s or s.lower() in {"none", "null", "undefined"}:
        return ""
    if _is_remote_script_ref(s):
        return s
    return _save_python_text_to_s3_via_upload_url(s, filename=filename, old_url=old_url).get("file_url", "")

def _normalize_event_action_python_scripts_for_save(actions: list, *, filename_prefix: str = "script") -> None:
    """Mutate event actions before DB save: inline PythonScript -> saved S3 URL.

    Existing remote refs are kept. If the browser editor saved to S3 but did not
    propagate the returned URL into actions_json, use the last URL saved in this
    session when there is only one empty PythonScript field in the submitted
    action list.
    """
    if not isinstance(actions, list):
        return

    # Count empty PythonScript slots. Session fallback is safe only when it is
    # unambiguous; otherwise we preserve existing values via
    # _carry_existing_event_python_script_refs or require the UI to submit URLs.
    empty_slots = []
    for idx, a in enumerate(actions):
        if not isinstance(a, dict):
            continue
        if (a.get("method") or "") == "PythonScript" and not _action_python_text_value(a, post=False):
            empty_slots.append((idx, False))
        if (a.get("postExecuteMethod") or "") == "PythonScript" and not _action_python_text_value(a, post=True):
            empty_slots.append((idx, True))

    session_fallback_url = _last_python_script_upload_url() if len(empty_slots) == 1 else ""

    for idx, a in enumerate(actions):
        if not isinstance(a, dict):
            continue

        if (a.get("method") or "") == "PythonScript":
            old_url = (a.get("methodTextUrl") or a.get("methodTextURL") or a.get("oldMethodText") or "")
            value = _action_python_text_value(a, post=False)
            if not value and session_fallback_url:
                value = session_fallback_url
            a["methodText"] = _normalize_python_script_text_for_save(
                value,
                filename=f"{filename_prefix}_action_{idx + 1}.py",
                old_url=old_url,
            )

        if (a.get("postExecuteMethod") or "") == "PythonScript":
            old_url = (a.get("postExecuteMethodTextUrl") or a.get("postExecuteMethodTextURL") or a.get("oldPostExecuteMethodText") or "")
            value = _action_python_text_value(a, post=True)
            if not value and session_fallback_url:
                value = session_fallback_url
            a["postExecuteMethodText"] = _normalize_python_script_text_for_save(
                value,
                filename=f"{filename_prefix}_post_action_{idx + 1}.py",
                old_url=old_url,
            )

@_routes.route("/api/s3/save-text-via-upload-url", methods=["POST"], endpoint="save_s3_text_via_upload_url")
@login_required
def save_s3_text_via_upload_url():
    """Backward-compatible endpoint used by edit_class/code editor.

    It accepts text/code, uploads it to S3 with a presigned PUT URL, and returns
    file_url. This is different from /api/s3/text-upload-url which only returns
    a URL for the browser to upload itself.
    """
    try:
        data = request.get_json(silent=True) or {}
        text_value = data.get("text")
        if text_value is None:
            text_value = data.get("code")
        if text_value is None:
            text_value = data.get("content")
        if text_value is None:
            text_value = ""
        filename = data.get("filename") or data.get("name") or "script.py"
        old_url = data.get("old_url") or data.get("oldUrl") or data.get("url") or ""
        return jsonify(_save_python_text_to_s3_via_upload_url(str(text_value), filename=filename, old_url=old_url))
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 save failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

@_routes.route("/api/s3/delete-text", methods=["POST"])
@login_required
def delete_s3_text():
    data = request.get_json(silent=True) or {}
    old_url = (data.get("old_url") or data.get("oldUrl") or "").strip()
    new_url = (data.get("new_url") or data.get("newUrl") or "").strip()
    old_key = _s3_key_from_public_url(old_url)
    new_key = _s3_key_from_public_url(new_url)
    user_prefix = f"uploads/python_scripts/{current_user.id}/"

    deleted = False
    if old_key and old_key != new_key and old_key.startswith(user_prefix):
        s3.delete_object(Bucket=S3_BUCKET, Key=old_key)
        deleted = True

    return jsonify({"ok": True, "old_deleted": deleted})

@_routes.route("/api/s3/text", methods=["POST"])
@login_required
def upload_s3_text():
    # Backward-compatible endpoint. If text/code is posted, behave like the old
    # server-mediated save endpoint; otherwise return a presigned upload URL.
    data = request.get_json(silent=True) or {}
    if any(k in data for k in ("text", "code", "content")):
        try:
            text_value = data.get("text")
            if text_value is None:
                text_value = data.get("code")
            if text_value is None:
                text_value = data.get("content")
            filename = data.get("filename") or data.get("name") or "script.py"
            old_url = data.get("old_url") or data.get("oldUrl") or data.get("url") or ""
            return jsonify(_save_python_text_to_s3_via_upload_url(str(text_value or ""), filename=filename, old_url=old_url))
        except Exception as exc:
            current_app.logger.exception("PythonScript S3 save failed")
            return jsonify({"ok": False, "error": str(exc)}), 500
    return get_s3_text_upload_url()

@_routes.route("/api/s3/read-text", methods=["GET"])
@login_required
def read_s3_text():
    try:
        key = _s3_key_from_public_url(request.args.get("url") or request.args.get("key") or "")
        if not key:
            return jsonify({"ok": False, "error": "Invalid or unsupported S3 URL"}), 400
        source_url = request.args.get("url") or request.args.get("key") or ""
        force_refresh = str(request.args.get("force") or request.args.get("refresh") or "").lower() in {"1", "true", "yes"}
        # Read through the same cache used by server-side PythonScript execution.
        # This keeps the editor/debug endpoint and runtime behavior consistent.
        body = _runtime_download_text_cached(source_url, force_refresh=force_refresh)
        return jsonify({"ok": True, "object_key": key, "text": body, "cached": not force_refresh})
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 read failed")
        return jsonify({"ok": False, "error": str(exc)}), 404

@_routes.route("/python_s3.html", methods=["GET"])
@login_required
def python_s3_editor():
    return render_template("code_editor.html", initial_url=(request.args.get("url") or ""))

@_routes.route("/code-editor", methods=["GET"])
@login_required
def code_editor():
    return redirect(url_for("python_s3_editor", url=(request.args.get("url") or "")))

@_routes.route('/delete-config/<uid>')
@login_required
def delete_config(uid):
    # Replace the execute with scalar() or first()
    config = db.session.scalar(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    )
    
    if config:
        db.session.delete(config)
        db.session.commit()
        flash(_('Configuration deleted'), 'success')

    return redirect(url_for('dashboard'))

@_routes.route('/upload-handlers/<uid>', methods=['POST'])
@login_required
def upload_handlers(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)
    
    upload_type = request.form.get('upload_type')
    handlers_data = {}

    file_content = None
    metadata = {
        'type': upload_type,
        'uploaded_at': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        if upload_type == 'file':
            if 'python_file' not in request.files:
                flash(_('File not selected'), 'error')
                active_tab = request.form.get("active_tab", "danger")
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            file = request.files['python_file']
            if file.filename == '':
                flash(_('File not selected'), 'error')
                active_tab = request.form.get("active_tab", "config")
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            if not file.filename.endswith('.py'):
                flash(_('Only .py files allowed'), 'danger')
                active_tab = request.form.get("active_tab", "config")
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            file_content = file.read().decode('utf-8')
            metadata['filename'] = file.filename
            
        elif upload_type == 'github':
            github_url = request.form.get('github_url')
            if not github_url:
                flash(_('Enter GitHub URL'), 'danger')
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            
            parsed = urlparse(github_url)
            if 'raw.githubusercontent.com' not in parsed.netloc:
                flash(_('Use GitHub RAW URL'), 'danger')
                active_tab = request.form.get("active_tab", "config")
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            response = requests.get(github_url)
            if response.status_code != 200:
                flash(_('Failed to load file'), 'error')
                active_tab = request.form.get("active_tab", "config")
                return redirect(url_for('edit_config', uid=uid,tab=active_tab))
            
            file_content = response.text
            metadata['url'] = github_url
            
        else:
            flash(_('Invalid upload type'), 'error')
            active_tab = request.form.get("active_tab", "config")
            return redirect(url_for('edit_config', uid=uid,tab=active_tab))
        
        android_imports = ANDROID_IMPORTS_TEMPLATE.format(
            uid=config.uid, 
            config_url=url_for('get_config', uid=config.uid, _external=True)
        )
        
        
        file_content = _rewrite_android_handlers_instance_refs_code(
            file_content,
            config.uid,
            url_for('get_config', uid=config.uid, _external=True)
        )
        
        config.nodes_handlers = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
        config.nodes_handlers_meta = metadata
        db.session.commit()
        
        
        sync_classes_from_android_handlers(config)
        sync_methods_from_code(config)
        
        flash(_('Handlers loaded successfully'), 'success')
    
    except Exception as e:
        db.session.rollback()
        flash(f'Error: {str(e)}', 'error')
    active_tab = request.form.get("active_tab", "config") 
    return redirect(url_for('edit_config', uid=uid, tab=active_tab))

def sync_classes_from_android_handlers(config):
    
    if not config.nodes_handlers:
        return
    
    try:
        module_code = base64.b64decode(config.nodes_handlers).decode('utf-8')
        tree = ast.parse(module_code)
        
        
        node_classes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                
                for base in node.bases:
                    if (isinstance(base, ast.Name) and base.id == 'Node') or \
                       (isinstance(base, ast.Attribute) and base.attr == 'Node'):
                        
                        if node.name != 'Node':
                            node_classes.append(node.name)
                        break
        
        
        existing_classes = {c.name: c for c in config.classes}
        
        for class_name in node_classes:
            if class_name not in existing_classes:
                
                new_class = ConfigClass(
                    name=class_name,
                    display_name=class_name,
                    config_id=config.id,
                    class_type='custom_process',
                    section_code='android'
                )
                db.session.add(new_class)
                print(f"Added new Android class from code: {class_name}")
        
        
        for class_name, class_obj in existing_classes.items():
            if (class_name not in node_classes and 
                class_obj.section_code == 'android' and
                class_obj.name != 'Node'):  
                db.session.delete(class_obj)
                print(f"Removed Android class not in code: {class_name}")
        
        db.session.commit()
        
    except Exception as e:
        print(f"Error syncing classes from Android handlers: {str(e)}")

@_routes.route('/clear-handlers/<uid>', methods=['POST'])
@login_required
def clear_handlers(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if config:
        config.nodes_handlers = ""
        config.nodes_handlers_meta = {}

        db.session.commit()
        flash(_('Handlers cleared'), 'success')
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=uid,tab=active_tab))

@_routes.route('/download-handlers/<uid>', methods=['GET'])
@login_required
def download_handlers(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config or not config.nodes_handlers:
        abort(404)
    if _config_is_ngenie_code_locked(config):
        abort(403, description=_ngenie_code_forbid_message())
    
    try:
        
        file_content = base64.b64decode(config.nodes_handlers)
        
        
        filename = 'handlers.py'
        if config.nodes_handlers_meta:
            if 'filename' in config.nodes_handlers_meta:
                filename = config.nodes_handlers_meta['filename']
            elif 'url' in config.nodes_handlers_meta:
                
                url_path = urlparse(config.nodes_handlers_meta['url']).path
                filename = url_path.split('/')[-1] or 'handlers.py'
        
        
        file_obj = io.BytesIO(file_content)
        file_obj.seek(0)
        
        return send_file(
            file_obj,
            as_attachment=True,
            download_name=filename,
            mimetype='text/x-python'
        )
    
    except Exception as e:
        flash(_('Download error:') +str(e))
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=uid,tab=active_tab))

def method_exists_in_code(module_code, class_name, method_name):
    
    try:
        tree = ast.parse(module_code)
        
        for node in ast.walk(tree):
            if (isinstance(node, ast.ClassDef) and 
                node.name == class_name):
                
                for class_node in node.body:
                    if (isinstance(class_node, ast.FunctionDef) and 
                        class_node.name == method_name):
                        return True
        return False
    except Exception as e:
        print(f"Error checking method existence: {str(e)}")
        return False

@_routes.route('/get-config-methods')
def get_config_methods():
    config_uid = request.args.get('config_uid')
    config = Configuration.query.filter_by(uid=config_uid).first()

    if not config:
        return jsonify({"methods": []})

    methods = []

    # Android handlers
    try:
        methods.extend(extract_functions_from_handlers(getattr(config, "nodes_handlers", None)))
    except Exception:
        pass

    # Server handlers (Handlers/<uid>/handlers.py)
    try:
        methods.extend(extract_functions_from_handlers(getattr(config, "nodes_server_handlers", None)))
    except Exception:
        pass

    # unique + sorted
    methods = sorted({m for m in methods if m})

    return jsonify({"methods": methods})

@_routes.route('/config/<config_uid>/add-event', methods=['POST'])
@login_required
def add_config_event(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})
    
    event_name = request.form.get('event_name')
    listener = request.form.get('listener', '')
    actions_json = request.form.get('actions_json', '[]')
    active_tab = request.form.get('active_tab', 'common-events')
    
    try:
        actions = json.loads(actions_json)
    except:
        actions = []
    
    
    existing_event = ConfigEvent.query.filter_by(
        config_id=config.id, 
        event=event_name, 
        listener=listener
    ).first()
    
    if existing_event:
        return jsonify({"status": "error", "message": "Event already exists"})

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"config_{config.id}_{event_name or 'event'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        return jsonify({"status": "error", "message": f"PythonScript S3 autosave failed: {exc}"})
    
   
    new_event = ConfigEvent(
        event=event_name,
        listener=listener,
        config_id=config.id
    )
    db.session.add(new_event)
    db.session.flush()  
    
    
    for action_data in actions:
        action = ConfigEventAction(
            event_id=new_event.id,
            action=action_data.get('action', 'run'),
            method=_action_method_value(action_data, 'method'),
            source=action_data.get('source', 'internal'),
            server=action_data.get('server', ''),
            post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
            method_text=(_action_python_text_value(action_data, post=False) or '') if _is_script_text_method(action_data.get('method', '')) else '',
            post_execute_text=(_action_python_text_value(action_data, post=True) or '') if _is_script_text_method(action_data.get('postExecuteMethod', '')) else '',
            http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
            post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else '',
            order=action_data.get('order', 0)
        )
        db.session.add(action)
    
    db.session.commit()
    
    return jsonify({
        "status": "success",
        "message": "Event added successfully",
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })

def _wizard_split_top_level(text, delimiter=','):
    out = []
    buf = ''
    paren = 0
    bracket = 0
    quote = None

    for i, ch in enumerate(text):
        prev = text[i - 1] if i > 0 else ''
        if quote:
            buf += ch
            if ch == quote and prev != '\\':
                quote = None
            continue

        if ch in ("'", '"'):
            quote = ch
            buf += ch
            continue

        if ch == '(':
            paren += 1
        elif ch == ')':
            paren = max(0, paren - 1)
        elif ch == '[':
            bracket += 1
        elif ch == ']':
            bracket = max(0, bracket - 1)

        if ch == delimiter and paren == 0 and bracket == 0:
            out.append(buf.strip())
            buf = ''
            continue

        buf += ch

    if buf.strip():
        out.append(buf.strip())
    return out

def _wizard_normalize_id(value):
    value = (value or '').strip()
    value = re.sub(r'^@+', '', value)
    value = re.sub(r'[^a-zA-Z0-9_]+', '_', value)
    value = value.strip('_')
    return value or 'field'

def _wizard_split_once_top_level(src, separator=':'):
    paren = 0
    bracket = 0
    quote = None

    for i, ch in enumerate(src):
        prev = src[i - 1] if i > 0 else ''
        if quote:
            if ch == quote and prev != '\\':
                quote = None
            continue

        if ch in ("'", '"'):
            quote = ch
            continue

        if ch == '(':
            paren += 1
        elif ch == ')':
            paren = max(0, paren - 1)
        elif ch == '[':
            bracket += 1
        elif ch == ']':
            bracket = max(0, bracket - 1)
        elif ch == separator and paren == 0 and bracket == 0:
            return src[:i].strip(), src[i + 1:].strip()

    return src.strip(), ''

def _wizard_parse_fn_call(text):
    m = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\((.*)\)\s*$', text or '')
    if not m:
        return None
    fn = m.group(1).lower()
    arg = m.group(2).strip()
    if (arg.startswith('"') and arg.endswith('"')) or (arg.startswith("'") and arg.endswith("'")):
        arg = arg[1:-1]
    return fn, arg

def _wizard_parse_select(text):
    m = re.match(r'^\s*select\((.*)\)\s*$', text or '', flags=re.I)
    if not m:
        return None
    items = []
    for part in _wizard_split_top_level(m.group(1)):
        left, right = _wizard_split_once_top_level(part, '|')
        caption = left.strip()
        value = right.strip() or caption
        if caption:
            items.append({"_view": caption, "_id": value})
    return items

def _wizard_build_active_field(spec):
    left, right = _wizard_split_once_top_level(spec, ':')
    caption, field_id = _wizard_split_once_top_level(left, '|')

    caption = caption.strip()
    field_id = _wizard_normalize_id(field_id or caption)
    value_ref = '@' + field_id
    right_lc = (right or '').strip().lower()

    fn_call = _wizard_parse_fn_call(right)
    if fn_call:
        fn, arg = fn_call
        if fn == 'node':
            return {
                "type": "NodeInput",
                "caption": caption,
                "id": field_id,
                "dataset": arg,
                "value": value_ref,
            }
        if fn == 'dataset':
            return {
                "type": "DatasetField",
                "caption": caption,
                "id": field_id,
                "dataset": arg,
                "value": value_ref,
            }

    select_items = _wizard_parse_select(right)
    if select_items is not None:
        return {
            "type": "Spinner",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "dataset": select_items,
        }

    if right_lc in ('bool', 'boolean', 'checkbox', 'check', 'switch', 'галочка'):
        return {
            "type": "Switch",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
        }

    if right_lc in ('number', 'numeric', 'int', 'integer', 'float', 'double'):
        return {
            "type": "Input",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "input_type": "number",
        }

    if right_lc in ('date', 'datetime'):
        return {
            "type": "Input",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "input_type": "date",
        }

    return {
        "type": "Input",
        "caption": caption,
        "id": field_id,
        "value": value_ref,
    }

def _wizard_build_cover_field(spec):
    left, right = _wizard_split_once_top_level(spec, ':')
    caption, raw_value = _wizard_split_once_top_level(left, '|')
    caption = caption.strip()

    label = {"type": "Text", "value": caption}
    right = (right or raw_value or '').strip()

    fn_call = _wizard_parse_fn_call(right)
    if fn_call:
        fn, _arg = fn_call
        field_id = _wizard_normalize_id(caption)
        if fn == 'node':
            return [label, {"type": "NodeLink", "value": '@' + field_id}]
        if fn == 'dataset':
            return [label, {"type": "DatasetLink", "value": '@' + field_id}]

    return [label, {"type": "Text", "value": right or ('@' + _wizard_normalize_id(caption))}]

def _wizard_build_table(inner, mode, index):
    cols = [x.strip() for x in _wizard_split_top_level(inner) if x.strip()]
    if not cols:
        raise ValueError('Empty table definition')

    if mode == 'active':
        layout_row = [_wizard_build_active_field(col) for col in cols]
        cover_row = []

        for col in cols:
            left, _right = _wizard_split_once_top_level(col, ':')
            caption, field_id = _wizard_split_once_top_level(left, '|')
            field_id = _wizard_normalize_id(field_id or caption)
            field = _wizard_build_active_field(col)

            if field["type"] == "NodeInput":
                cover_row.append({"type": "NodeLink", "value": '@' + field_id})
            elif field["type"] == "DatasetField":
                cover_row.append({"type": "DatasetLink", "value": '@' + field_id})
            else:
                cover_row.append({"type": "Text", "value": '@' + field_id})

        return [[{
            "type": "Table",
            "id": f"tab{index}",
            "virtual_node": {
                "layout": [layout_row],
                "cover": [cover_row],
            }
        }]]

    header = []
    for col in cols:
        left, _right = _wizard_split_once_top_level(col, ':')
        caption, field_id = _wizard_split_once_top_level(left, '|')
        field_id = _wizard_normalize_id(field_id or caption)
        header.append(f"{caption.strip()}|{field_id}|1")

    return [[{
        "type": "Table",
        "id": f"tab{index}",
        "value": [],
        "table": True,
        "table_header": header,
    }]]

def simplified_markup_to_layout(text, mode):
    mode = (mode or 'active').strip().lower()
    if mode not in ('active', 'cover'):
        raise ValueError('Unsupported mode')

    lines = [x.strip() for x in (text or '').splitlines() if x.strip()]
    rows = []
    tables = []

    for line in lines:
        if line.startswith('[') and line.endswith(']'):
            tables.append(_wizard_build_table(line[1:-1].strip(), mode, len(tables) + 1))
            continue

        parts = _wizard_split_top_level(line)
        if mode == 'active':
            rows.append([_wizard_build_active_field(p) for p in parts])
        else:
            row = []
            for p in parts:
                row.extend(_wizard_build_cover_field(p))
            rows.append(row)

    if not tables:
        return rows

    if len(tables) == 1:
        return rows + tables[0]

    tabs = []
    for i, table_layout in enumerate(tables, start=1):
        tabs.append({
            "type": "Tab",
            "id": f"tab_{i}",
            "caption": f"Table {i}",
            "layout": table_layout,
        })

    return rows + [[{"type": "Tabs", "value": tabs}]]

def _wiz_split_top_level(text, delimiter=','):
    result = []
    buf = ''
    depth_round = 0
    depth_square = 0
    quote = None

    for i, ch in enumerate(text or ''):
        prev = text[i - 1] if i > 0 else ''

        if quote:
            buf += ch
            if ch == quote and prev != '\\':
                quote = None
            continue

        if ch in ('"', "'"):
            quote = ch
            buf += ch
            continue

        if ch == '(':
            depth_round += 1
        elif ch == ')':
            depth_round = max(0, depth_round - 1)
        elif ch == '[':
            depth_square += 1
        elif ch == ']':
            depth_square = max(0, depth_square - 1)

        if ch == delimiter and depth_round == 0 and depth_square == 0:
            if buf.strip():
                result.append(buf.strip())
            buf = ''
            continue

        buf += ch

    if buf.strip():
        result.append(buf.strip())
    return result

def _wiz_split_once_top_level(src, separator=':'):
    depth_round = 0
    depth_square = 0
    quote = None

    for i, ch in enumerate(src or ''):
        prev = src[i - 1] if i > 0 else ''

        if quote:
            if ch == quote and prev != '\\':
                quote = None
            continue

        if ch in ('"', "'"):
            quote = ch
            continue

        if ch == '(':
            depth_round += 1
        elif ch == ')':
            depth_round = max(0, depth_round - 1)
        elif ch == '[':
            depth_square += 1
        elif ch == ']':
            depth_square = max(0, depth_square - 1)
        elif ch == separator and depth_round == 0 and depth_square == 0:
            return src[:i].strip(), src[i + 1:].strip()

    return (src or '').strip(), ''

def _wiz_unquote(value):
    value = (value or '').strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value

def _wiz_norm_id(value):
    value = (value or '').strip()
    value = re.sub(r'^@+', '', value)
    value = re.sub(r'[^A-Za-z0-9_]+', '_', value)
    value = value.strip('_')
    return value or 'field'

def _wiz_parse_fn_call(text):
    m = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$', text or '')
    if not m:
        return None
    return m.group(1).strip().lower(), _wiz_unquote(m.group(2).strip())

def _wiz_parse_select(text):
    m = re.match(r'^\s*select\s*\((.*)\)\s*$', text or '', flags=re.I)
    if not m:
        return None

    items = []
    for part in _wiz_split_top_level(m.group(1)):
        left, right = _wiz_split_once_top_level(part, '|')
        cap = left.strip()
        val = right.strip() or cap
        if cap:
            items.append({"_view": cap, "_id": val})
    return items

def _wiz_parse_line_spec(spec):
    left, right = _wiz_split_once_top_level(spec, ':')
    caption, field_id = _wiz_split_once_top_level(left, '|')
    caption = caption.strip()
    field_id = _wiz_norm_id(field_id or caption)
    return caption, field_id, (right or '').strip()


def _wiz_parse_named_table(line):
    """Parse ``caption|field:[column declarations]`` tabular-part syntax.

    The bracket must contain declarations with their own ``: type``. This keeps
    list-node syntax such as ``positions:[Node("Line")]`` separate.
    """
    left, right = _wiz_split_once_top_level(line, ':')
    right = (right or '').strip()
    if not left or not (right.startswith('[') and right.endswith(']')):
        return None

    inner = right[1:-1].strip()
    specs = [x.strip() for x in _wiz_split_top_level(inner) if x.strip()]
    if not specs:
        return None
    if not any(bool(_wiz_split_once_top_level(spec, ':')[1]) for spec in specs):
        return None

    caption, field_id = _wiz_split_once_top_level(left, '|')
    caption = (caption or field_id or '').strip()
    field_id = _wiz_norm_id(field_id or caption)
    return caption or field_id, field_id, specs

def _wiz_active_field_to_json(spec):
    caption, field_id, right = _wiz_parse_line_spec(spec)
    value_ref = '@' + field_id
    right_lc = right.lower()

    fn_call = _wiz_parse_fn_call(right)
    if fn_call:
        fn, arg = fn_call
        if fn == 'node':
            return {
                "type": "NodeInput",
                "caption": caption,
                "id": field_id,
                "dataset": arg,
                "value": value_ref,
            }
        if fn == 'dataset':
            return {
                "type": "DatasetField",
                "caption": caption,
                "id": field_id,
                "dataset": arg,
                "value": value_ref,
            }

    select_items = _wiz_parse_select(right)
    if select_items is not None:
        return {
            "type": "Spinner",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "dataset": select_items,
        }

    if right_lc in ('bool', 'boolean', 'checkbox', 'check', 'switch', 'галочка'):
        return {
            "type": "Switch",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
        }

    if right_lc in ('number', 'numeric', 'int', 'integer', 'float', 'double'):
        return {
            "type": "Input",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "input_type": "number",
        }

    if right_lc in ('date', 'datetime'):
        return {
            "type": "Input",
            "caption": caption,
            "id": field_id,
            "value": value_ref,
            "input_type": "date",
        }

    return {
        "type": "Input",
        "caption": caption,
        "id": field_id,
        "value": value_ref,
    }

def _wiz_cover_field_to_json(spec):
    caption, field_id, right = _wiz_parse_line_spec(spec)
    label = {"type": "Text", "value": caption}

    fn_call = _wiz_parse_fn_call(right)
    if fn_call:
        fn, _arg = fn_call
        if fn == 'node':
            return [
                label,
                {"type": "NodeLink", "value": '@' + field_id, "bold": True}
            ]
        if fn == 'dataset':
            return [
                label,
                {"type": "DatasetLink", "value": '@' + field_id, "bold": True}
            ]

    value = right or ('@' + field_id)
    return [
        label,
        {"type": "Text", "value": value, "bold": True}
    ]

def _wiz_cover_structure_field_to_json(spec):
    """Generate one cover row from a DataStructure field declaration.

    Cover generation is intentionally conservative: it may emit only Text,
    NodeLink and DatasetLink. Type declarations are never shown as literal
    values; every generated value references the node field through ``@``.
    """
    _caption, field_id, type_expr = _wiz_parse_line_spec(spec)
    value_ref = '@' + field_id

    fn_call = _wiz_parse_fn_call(type_expr)
    if fn_call:
        fn, _arg = fn_call
        if fn in ('node', 'childnode'):
            return [{"type": "NodeLink", "value": value_ref, "bold": True}]
        if fn == 'dataset':
            return [{"type": "DatasetLink", "value": value_ref, "bold": True}]

    # Cover generation intentionally emits only field-bound values. A Text item
    # always uses @field; literal captions and Table/Tabs controls are not added.
    return [{"type": "Text", "value": value_ref, "bold": True}]


def _wiz_cover_from_data_structure(text):
    rows = []
    for raw_line in (text or '').replace(';', '\n').splitlines():
        line = str(raw_line or '').strip().strip(',')
        if not line or line.startswith('#') or line.startswith('//'):
            continue

        named_table = _wiz_parse_named_table(line)
        if named_table:
            _caption, _field_id, specs = named_table
            for spec in specs:
                rows.append(_wiz_cover_structure_field_to_json(spec))
            continue

        if line.startswith('[') and line.endswith(']'):
            inner = line[1:-1].strip()
            specs = [x.strip() for x in _wiz_split_top_level(inner) if x.strip()]
            for spec in specs:
                rows.append(_wiz_cover_structure_field_to_json(spec))
            continue

        specs = [x.strip() for x in _wiz_split_top_level(line) if x.strip()]
        for spec in specs:
            rows.append(_wiz_cover_structure_field_to_json(spec))

    allowed = {'Text', 'NodeLink', 'DatasetLink'}
    for row in rows:
        for item in row:
            if not isinstance(item, dict) or item.get('type') not in allowed:
                raise ValueError('Cover generator produced an unsupported element')
    return rows



def _wiz_build_active_table(specs, index, table_id=None, table_caption=None):
    layout_row = []
    cover_row = []

    for spec in specs:
        field = _wiz_active_field_to_json(spec)
        layout_row.append(field)

        field_id = field.get('id') or 'field'
        if field.get('type') == 'NodeInput':
            cover_row.append({"type": "NodeLink", "value": '@' + field_id, "bold": True})
        elif field.get('type') == 'DatasetField':
            cover_row.append({"type": "DatasetLink", "value": '@' + field_id, "bold": True})
        else:
            cover_row.append({"type": "Text", "value": '@' + field_id, "bold": True})

    explicit_name = str(table_id or '').strip()
    resolved_id = _wiz_norm_id(explicit_name) if explicit_name else f"tab{index}"
    table = {
        "type": "Table",
        "id": resolved_id,
        "virtual_node": {
            "layout": [layout_row],
            "cover": [cover_row],
        }
    }
    if explicit_name:
        table["data_structure_name"] = resolved_id
        table["caption"] = str(table_caption or explicit_name)
    return [[table]]


def _wiz_build_cover_table(specs, index, table_id=None, table_caption=None):
    header = []
    value_row = []

    for spec in specs:
        caption, field_id, right = _wiz_parse_line_spec(spec)
        header.append(f"{caption}|{field_id}|1")
        value_row.append(right or ('@' + field_id))

    explicit_name = str(table_id or '').strip()
    resolved_id = _wiz_norm_id(explicit_name) if explicit_name else f"tab{index}"
    table = {
        "type": "Table",
        "id": resolved_id,
        "value": [value_row],
        "table": True,
        "table_header": header,
    }
    if explicit_name:
        table["data_structure_name"] = resolved_id
        table["caption"] = str(table_caption or explicit_name)
    return [[table]]



def _wiz_parse_node_type_list(expr, allowed=('node', 'childnode')):
    """Parse Node("Class") / ChildNode("Class") separated by | or comma."""
    expr = (expr or '').strip()
    if expr.startswith('[') and expr.endswith(']'):
        expr = expr[1:-1].strip()
    parts = []
    # ChildNode("A")|ChildNode("B") is the compact structure syntax.
    for chunk in _wiz_split_top_level(expr, delimiter='|'):
        for part in _wiz_split_top_level(chunk, delimiter=','):
            part = (part or '').strip()
            if not part:
                continue
            fn_call = _wiz_parse_fn_call(part)
            if not fn_call:
                continue
            fn, arg = fn_call
            if fn in allowed and arg:
                parts.append({'fn': fn, 'class': arg})
    return parts


def _wiz_build_add_buttons(field_id, node_specs):
    buttons = []
    used = set()
    for item in node_specs or []:
        cls = str(item.get('class') or '').strip()
        if not cls or cls in used:
            continue
        used.add(cls)
        buttons.append({
            "type": "Button",
            "id": f"btn_add_{field_id}_{_wiz_norm_id(cls)}",
            "caption": f"Добавить {cls}",
            "target_class": cls,
            "target_field": field_id,
            "target_relation": item.get('fn') or 'node',
        })
    return buttons


def _wiz_try_structure_element(line):
    """Return layout rows for NodeChildren/ListChildNodes/ListNodes structure lines."""
    left, right = _wiz_split_once_top_level(line, ':')
    if not left or not right:
        return None
    caption, field_id = _wiz_split_once_top_level(left, '|')
    raw_field_id = (field_id or caption or '').strip()
    field_id = raw_field_id if raw_field_id == '_children' else _wiz_norm_id(raw_field_id)
    expr = (right or '').strip()

    # NodeChildren: _children: ChildNode("OrderPosition")|ChildNode("Special")
    direct_specs = _wiz_parse_node_type_list(expr, allowed=('childnode',))
    if direct_specs and not (expr.startswith('[') and expr.endswith(']')):
        rows = []
        buttons = _wiz_build_add_buttons(field_id, direct_specs)
        if buttons:
            rows.append(buttons)
        rows.append([{
            "type": "NodeChildren",
            "id": field_id,
            "value": '@' + field_id,
            "child_classes": [x['class'] for x in direct_specs],
        }])
        return rows

    # ListChildNodes/ListNodes: positions:[ChildNode("OrderPosition")] / positions:[Node("CommonLine")]
    if expr.startswith('[') and expr.endswith(']'):
        list_specs = _wiz_parse_node_type_list(expr, allowed=('childnode', 'node'))
        if list_specs:
            rows = []
            buttons = _wiz_build_add_buttons(field_id, list_specs)
            if buttons:
                rows.append(buttons)
            rows.append([
                {"type": "Parameters", "w": 1, "height": -1},
                {
                    "type": "Table",
                    "id": f"tbl_{field_id}",
                    "nodes_source": True,
                    "value": '@' + field_id,
                }
            ])
            return rows

    return None

def simplified_markup_to_layout(text, mode):
    mode = (mode or 'active').strip().lower()
    if mode == 'cover_structure':
        return _wiz_cover_from_data_structure(text)
    if mode not in ('active', 'cover'):
        raise ValueError('Unsupported mode')

    lines = [x.strip() for x in (text or '').splitlines() if x.strip()]
    rows = []
    tables = []

    for line in lines:
        named_table = _wiz_parse_named_table(line)
        if named_table:
            table_caption, table_id, specs = named_table
            index = len(tables) + 1
            if mode == 'active':
                table_layout = _wiz_build_active_table(specs, index, table_id, table_caption)
            else:
                table_layout = _wiz_build_cover_table(specs, index, table_id, table_caption)
            tables.append({"caption": table_caption, "id": table_id, "layout": table_layout})
            continue

        if line.startswith('[') and line.endswith(']'):
            inner = line[1:-1].strip()
            specs = [x.strip() for x in _wiz_split_top_level(inner) if x.strip()]
            index = len(tables) + 1
            if mode == 'active':
                table_layout = _wiz_build_active_table(specs, index)
            else:
                table_layout = _wiz_build_cover_table(specs, index)
            tables.append({"caption": f"Table {index}", "id": f"tab{index}", "layout": table_layout})
            continue

        if mode == 'active':
            structure_rows = _wiz_try_structure_element(line)
            if structure_rows is not None:
                rows.extend(structure_rows)
                continue

        parts = [x.strip() for x in _wiz_split_top_level(line) if x.strip()]
        if mode == 'active':
            rows.append([_wiz_active_field_to_json(part) for part in parts])
        else:
            row = []
            for part in parts:
                row.extend(_wiz_cover_field_to_json(part))
            rows.append(row)

    if not tables:
        return rows

    if len(tables) == 1:
        return rows + tables[0]["layout"]

    tabs = []
    for index, table_info in enumerate(tables, start=1):
        tabs.append({
            "type": "Tab",
            "id": f"tab_{_wiz_norm_id(table_info.get('id') or index)}",
            "caption": str(table_info.get('caption') or f"Table {index}"),
            "layout": table_info.get("layout") or [],
        })

    return rows + [[{"type": "Tabs", "value": tabs}]]


def _wiz_json_field_to_simple(item):
    if not isinstance(item, dict):
        return None

    t = (item.get('type') or '').strip()
    caption = item.get('caption') or item.get('value') or 'Field'
    field_id = item.get('id') or _wiz_norm_id(caption)

    if t == 'Input':
        input_type = (item.get('input_type') or '').lower()
        if input_type == 'number':
            return f'{caption}|{field_id}: number'
        if input_type == 'date':
            return f'{caption}|{field_id}: date'
        return f'{caption}|{field_id}: string'

    if t in ('Switch', 'CheckBox'):
        return f'{caption}|{field_id}: boolean'

    if t == 'NodeInput':
        ds = item.get('dataset') or ''
        return f'{caption}|{field_id}: Node("{ds}")'

    if t in ('DataSetField', 'DatasetField'):
        ds = item.get('dataset') or ''
        return f'{caption}|{field_id}: DataSet("{ds}")'

    if t == 'Spinner':
        ds = item.get('dataset')
        if isinstance(ds, list):
            parts = []
            for x in ds:
                if isinstance(x, dict):
                    parts.append(f'{x.get("_view","")}|{x.get("_id","")}')
            return f'{caption}|{field_id}: select({", ".join(parts)})'
        return f'{caption}|{field_id}: string'

    return None

def _wiz_cover_row_to_simple(row):
    if not isinstance(row, list) or len(row) < 2:
        return None

    parts = []
    i = 0

    while i + 1 < len(row):
        left = row[i]
        right = row[i + 1]

        if not isinstance(left, dict) or not isinstance(right, dict):
            i += 2
            continue

        if left.get('type') != 'Text':
            i += 2
            continue

        caption = left.get('value') or 'Field'
        right_type = right.get('type')
        value = right.get('value') or ''
        field_id = _wiz_norm_id(value if isinstance(value, str) and value.startswith('@') else caption)

        if right_type in ('Text', 'NodeLink', 'DatasetLink'):
            parts.append(f'{caption}|{value or ("@" + field_id)}')

        i += 2

    if parts:
        return ', '.join(parts)

    return None

def _wiz_table_to_simple(table_item, mode):
    if not isinstance(table_item, dict) or table_item.get('type') != 'Table':
        return None

    structure_name = str(table_item.get('data_structure_name') or '').strip()
    prefix = f'{structure_name}:' if structure_name else ''

    if mode == 'active':
        v = table_item.get('virtual_node') or {}
        layout = v.get('layout') or []
        if not layout or not isinstance(layout, list) or not layout[0]:
            return None

        cols = []
        for item in layout[0]:
            value = _wiz_json_field_to_simple(item)
            if value:
                cols.append(value)
        if cols:
            return prefix + '[' + ', '.join(cols) + ']'
        return None

    headers = table_item.get('table_header') or []
    if headers:
        cols = []
        for header in headers:
            if not isinstance(header, str):
                continue
            parts = header.split('|')
            caption = parts[0].strip() if len(parts) > 0 else 'Field'
            field_id = parts[1].strip() if len(parts) > 1 else _wiz_norm_id(caption)
            cols.append(f'{caption}|@{field_id}')
        if cols:
            return prefix + '[' + ', '.join(cols) + ']'
    return None


def layout_to_simplified_markup(layout, mode):
    mode = (mode or 'active').strip().lower()
    if isinstance(layout, str):
        layout = json.loads(layout)

    if not isinstance(layout, list):
        raise ValueError('Layout must be a list')

    lines = []

    for row in layout:
        if not isinstance(row, list) or not row:
            continue

        if len(row) == 1 and isinstance(row[0], dict):
            item = row[0]
            t = item.get('type')

            if t == 'Table':
                if mode == 'active' and item.get('nodes_source'):
                    field_id = str(item.get('id') or 'tbl_nodes')
                    if field_id.startswith('tbl_'):
                        field_id = field_id[4:]
                    classes = item.get('node_classes') or []
                    fn = 'ChildNode' if str(item.get('node_relation') or '').lower() == 'childnode' else 'Node'
                    if classes:
                        lines.append(f'{field_id}:[' + '|'.join([f'{fn}("{c}")' for c in classes]) + ']')
                        continue
                s = _wiz_table_to_simple(item, mode)
                if s:
                    lines.append(s)
                continue

            if t == 'NodeChildren':
                if mode == 'active':
                    field_id = str(item.get('id') or '_children')
                    classes = item.get('child_classes') or []
                    if classes:
                        lines.append(f'{field_id}: ' + '|'.join([f'ChildNode("{c}")' for c in classes]))
                continue

            if t == 'Tabs':
                tabs = item.get('value') or []
                for tab in tabs:
                    if not isinstance(tab, dict):
                        continue
                    tab_layout = tab.get('layout') or []
                    if not tab_layout:
                        continue
                    if isinstance(tab_layout, list):
                        for subrow in tab_layout:
                            if isinstance(subrow, list) and len(subrow) == 1 and isinstance(subrow[0], dict) and subrow[0].get('type') == 'Table':
                                s = _wiz_table_to_simple(subrow[0], mode)
                                if s:
                                    lines.append(s)
                continue

        if mode == 'active':
            parts = []
            for item in row:
                s = _wiz_json_field_to_simple(item)
                if s:
                    parts.append(s)
            if parts:
                lines.append(', '.join(parts))
        else:
            s = _wiz_cover_row_to_simple(row)
            if s:
                lines.append(s)

    return '\n'.join(lines)

@_routes.route('/layout_wizard', methods=['POST'])
@login_required
def layout_wizard():
    data = request.get_json(silent=True) or {}
    direction = (data.get('direction') or 'to_json').strip().lower()
    mode = (data.get('mode') or 'active').strip().lower()

    try:
        if direction == 'to_json':
            text = data.get('text', '')
            layout = simplified_markup_to_layout(text, mode)
            return jsonify({
                'ok': True,
                'layout': layout,
            })

        if direction == 'to_simplified':
            layout = data.get('layout')
            text = layout_to_simplified_markup(layout, mode)
            return jsonify({
                'ok': True,
                'text': text,
            })

        return jsonify({
            'ok': False,
            'error': 'Unsupported direction'
        }), 400

    except Exception as e:
        return jsonify({
            'ok': False,
            'error': str(e),
        }), 400

@_routes.route('/config/<config_uid>/edit-event', methods=['POST'])
@login_required
def edit_config_event(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})
    
    old_event_name = request.form.get('old_event_name')
    old_listener = request.form.get('old_listener', '')
    event_name = request.form.get('event_name')
    listener = request.form.get('listener', '')
    actions_json = request.form.get('actions_json', '[]')
    active_tab = request.form.get('active_tab', 'common-events')
    
    try:
        actions = json.loads(actions_json)
    except:
        actions = []
    
    
    event = ConfigEvent.query.filter_by(
        config_id=config.id, 
        event=old_event_name, 
        listener=old_listener
    ).first()
    
    if not event:
        return jsonify({"status": "error", "message": "Event not found"})

    _carry_existing_event_python_script_refs(actions, getattr(event, "actions", None))

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"config_{config.id}_{event_name or 'event'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        return jsonify({"status": "error", "message": f"PythonScript S3 autosave failed: {exc}"})
    
    
    event.event = event_name
    event.listener = listener
    
    
    ConfigEventAction.query.filter_by(event_id=event.id).delete()
    
    
    for action_data in actions:
        action = ConfigEventAction(
            event_id=event.id,
            action=action_data.get('action', 'run'),
            method=_action_method_value(action_data, 'method'),
            source=action_data.get('source', 'internal'),
            server=action_data.get('server', ''),
            post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
            method_text=(_action_python_text_value(action_data, post=False) or '') if _is_script_text_method(action_data.get('method', '')) else '',
            post_execute_text=(_action_python_text_value(action_data, post=True) or '') if _is_script_text_method(action_data.get('postExecuteMethod', '')) else '',
            http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
            post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else '',
            order=action_data.get('order', 0)
        )
        db.session.add(action)
    
    db.session.commit()
    
    return jsonify({
        "status": "success", 
        "message": "Event updated successfully",
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })

@_routes.route('/config/<config_uid>/delete-event', methods=['POST'])
@login_required
def delete_config_event(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})
    
    event_name = request.form.get('event_name')
    listener = request.form.get('listener', '')
    active_tab = request.form.get('active_tab', 'common-events')
    
    event = ConfigEvent.query.filter_by(
        config_id=config.id, 
        event=event_name, 
        listener=listener
    ).first()
    
    if event:
        db.session.delete(event)
        db.session.commit()
    
    return jsonify({
        "status": "success",
        "message": "Event deleted successfully", 
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })

@_routes.route('/get-config-event-json')
def get_config_event_json():
    event_id = request.args.get('event_id')
    event = ConfigEvent.query.get(event_id)
    
    if not event:
        return jsonify({})
    
    return jsonify({
        "event": event.event,
        "listener": event.listener,
        "actions": event.actions_as_dicts()
    })


def _timer_active_from_form() -> bool:
    return str(request.form.get('active') or '').strip().lower() in {'1', 'true', 'on', 'yes'}


def _timer_worker_from_form() -> bool:
    return str(request.form.get('worker') or '').strip().lower() in {'1', 'true', 'on', 'yes'}


def _normalize_timer_runtime(value, default='server') -> str:
    value = str(value or default or 'server').strip().lower()
    if value in {'client', 'android', 'mobile', 'клиент', 'андроид'}:
        return 'client'
    return 'server'


def _timer_runtime_from_form() -> str:
    return _normalize_timer_runtime(request.form.get('runtime') or request.form.get('run_on'), default='server')


def _timer_runtime_from_timer_data(timer_data) -> str:
    return _normalize_timer_runtime(
        timer_data.get('runtime')
        or timer_data.get('run_on')
        or ('server' if _bool_from_timer_value(timer_data.get('server'), default=False) else None),
        default='server'
    )


def _bool_from_timer_value(value, default=False) -> bool:
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


def _normalize_timer_period_seconds(value, runtime='server', worker=False) -> int:
    try:
        period_seconds = int(float(value or 0))
    except Exception:
        period_seconds = 0
    runtime = _normalize_timer_runtime(runtime, default='server')
    min_period_seconds = 900 if runtime == 'server' or worker else 1
    return max(min_period_seconds, period_seconds)


def _period_seconds_from_form(runtime='server', worker=False) -> int:
    return _normalize_timer_period_seconds(request.form.get('period_seconds'), runtime=runtime, worker=worker)


def _period_seconds_from_timer_data(timer_data, runtime='server', worker=False) -> int:
    return _normalize_timer_period_seconds(
        timer_data.get('period_seconds') or timer_data.get('period') or 900,
        runtime=runtime,
        worker=worker,
    )


def _add_timer_actions(timer, actions):
    for action_data in actions:
        action = ConfigTimerAction(
            timer_obj=timer,
            action=action_data.get('action', 'run'),
            method=_action_method_value(action_data, 'method'),
            source=action_data.get('source', 'internal'),
            server=action_data.get('server', ''),
            post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
            method_text=(_action_python_text_value(action_data, post=False) or '') if _is_script_text_method(action_data.get('method', '')) else '',
            post_execute_text=(_action_python_text_value(action_data, post=True) or '') if _is_script_text_method(action_data.get('postExecuteMethod', '')) else '',
            http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
            post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else '',
            order=action_data.get('order', 0),
        )
        db.session.add(action)


@_routes.route('/config/<config_uid>/add-timer', methods=['POST'])
@login_required
def add_config_timer(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})

    timer_id = (request.form.get('timer_id') or '').strip()
    actions_json = request.form.get('actions_json', '[]')
    active_tab = request.form.get('active_tab', 'timers')
    if not timer_id:
        return jsonify({"status": "error", "message": "Timer ID is required"})

    try:
        actions = json.loads(actions_json)
    except Exception:
        actions = []

    existing_timer = ConfigTimer.query.filter_by(config_id=config.id, timer_id=timer_id).first()
    if existing_timer:
        return jsonify({"status": "error", "message": "Timer already exists"})

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"config_{config.id}_timer_{timer_id or 'timer'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        return jsonify({"status": "error", "message": f"PythonScript S3 autosave failed: {exc}"})

    worker = _timer_worker_from_form()
    runtime = _timer_runtime_from_form()
    new_timer = ConfigTimer(
        timer_id=timer_id,
        period_seconds=_period_seconds_from_form(runtime=runtime, worker=worker),
        active=_timer_active_from_form(),
        worker=worker,
        runtime=runtime,
        config_id=config.id,
    )
    db.session.add(new_timer)
    db.session.flush()
    _add_timer_actions(new_timer, actions)
    db.session.commit()

    return jsonify({
        "status": "success",
        "message": "Timer added successfully",
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })


@_routes.route('/config/<config_uid>/edit-timer', methods=['POST'])
@login_required
def edit_config_timer(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})

    old_timer_id = (request.form.get('old_timer_id') or '').strip()
    timer_id = (request.form.get('timer_id') or '').strip()
    actions_json = request.form.get('actions_json', '[]')
    active_tab = request.form.get('active_tab', 'timers')
    if not timer_id:
        return jsonify({"status": "error", "message": "Timer ID is required"})

    try:
        actions = json.loads(actions_json)
    except Exception:
        actions = []

    timer = ConfigTimer.query.filter_by(config_id=config.id, timer_id=old_timer_id).first()
    if not timer:
        return jsonify({"status": "error", "message": "Timer not found"})

    duplicate = ConfigTimer.query.filter(
        ConfigTimer.config_id == config.id,
        ConfigTimer.timer_id == timer_id,
        ConfigTimer.id != timer.id,
    ).first()
    if duplicate:
        return jsonify({"status": "error", "message": "Timer already exists"})

    _carry_existing_event_python_script_refs(actions, getattr(timer, "actions", None))

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"config_{config.id}_timer_{timer_id or 'timer'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        return jsonify({"status": "error", "message": f"PythonScript S3 autosave failed: {exc}"})

    worker = _timer_worker_from_form()
    runtime = _timer_runtime_from_form()
    timer.timer_id = timer_id
    timer.period_seconds = _period_seconds_from_form(runtime=runtime, worker=worker)
    timer.active = _timer_active_from_form()
    timer.worker = worker
    timer.runtime = runtime

    ConfigTimerAction.query.filter_by(timer_id=timer.id).delete()
    _add_timer_actions(timer, actions)
    db.session.commit()

    return jsonify({
        "status": "success",
        "message": "Timer updated successfully",
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })


@_routes.route('/config/<config_uid>/delete-timer', methods=['POST'])
@login_required
def delete_config_timer(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"})

    timer_id = (request.form.get('timer_id') or '').strip()
    active_tab = request.form.get('active_tab', 'timers')

    timer = ConfigTimer.query.filter_by(config_id=config.id, timer_id=timer_id).first()
    if timer:
        db.session.delete(timer)
        db.session.commit()

    return jsonify({
        "status": "success",
        "message": "Timer deleted successfully",
        "redirect_url": url_for('edit_config', uid=config_uid, tab=active_tab)
    })


@_routes.route('/get-config-timer-json')
def get_config_timer_json():
    timer_pk = request.args.get('timer_id')
    timer = ConfigTimer.query.get(timer_pk)
    if not timer:
        return jsonify({})
    return jsonify(timer.to_dict() if hasattr(timer, 'to_dict') else {})


@_routes.route('/config/<config_uid>/common-layouts', methods=['POST'])
@login_required
def save_common_layouts(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first()
    if not config or config.user_id != current_user.id:
        return jsonify({"status": "error", "message": "Configuration not found"}), 404

    layouts = None

    # preferred: JSON from fetch()
    if request.is_json:
        body = request.get_json(silent=True) or {}
        layouts = body.get("common_layouts", None)

    # fallback: form submit style
    if layouts is None:
        raw = request.form.get("common_layouts_json", "")
        if raw:
            try:
                layouts = json.loads(raw)
            except Exception:
                layouts = None

    if not isinstance(layouts, list):
        return jsonify({"status": "error", "message": "common_layouts must be a list"}), 400

    # minimal sanitize (same spirit as your other handlers: don't crash, keep stable)
    cleaned = []
    for it in layouts:
        if not isinstance(it, dict):
            continue
        _id = str(it.get("id", "")).strip()
        if not _id:
            continue
        cleaned.append({
            "id": _id,
            "layout": it.get("layout", [])
        })

    config.common_layouts = cleaned
    db.session.commit()

    return jsonify({
        "status": "success",
        "message": "CommonLayouts saved",
        "redirect_url": url_for('edit_config', uid=config_uid, tab='common_layouts')
    })

def extract_functions_from_handlers(handlers_code):
    
    if not handlers_code:
        return []
    
    
    handlers_code = base64.b64decode(handlers_code).decode('utf-8')
    
    
    functions = []
    
    
    lines = handlers_code.split('\n')
    in_class = False
    class_indent_level = 0
    
    for line in lines:
        stripped = line.strip()
        
        
        if not stripped or stripped.startswith('#'):
            continue
            
        
        indent_level = len(line) - len(line.lstrip())
        
        
        if stripped.startswith('class '):
            in_class = True
            class_indent_level = indent_level
            continue
            
        
        if in_class and indent_level <= class_indent_level and not stripped.startswith('class '):
            in_class = False
            
        
        if not in_class and stripped.startswith('def '):
            
            match = re.match(r'def\s+(\w+)\s*\(', stripped)
            if match:
                func_name = match.group(1)
                
                if not func_name.startswith('__') or func_name == '__init__':
                    functions.append(func_name)
    
    return sorted(list(set(functions)))

@_routes.route('/edit-config/<uid>', methods=['GET', 'POST'])
@login_required
def edit_config(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)

    # Client-only Demo/Solution instances must not be reachable by a crafted
    # direct Designer URL. Hiding them from the dashboard alone is not access control.
    if bool(getattr(config, 'designer_hidden', False)) and not _current_user_has_admin_login():
        abort(404)

    if request.method == 'GET':
        _ensure_system_config_for_current_user(sync_users=True)
        _cleanup_reserved_user_classes_for_current_user()
        try:
            db.session.refresh(config)
        except Exception:
            pass

    if _config_is_ngenie_code_locked(config) and request.method == 'POST':
        abort(403, description=_ngenie_code_forbid_message())

     
    if request.method == 'GET' and not _config_is_ngenie_code_locked(config):
        sync_classes_from_server_handlers(config)
        sync_classes_from_android_handlers(config)
        sync_methods_from_code(config)  
        db.session.refresh(config) 

    # Solutions/nGenie Code: F5 or direct opening of edit_config must resume
    # unfinished plan.py as well. The chat JSON endpoint also does this, but this
    # server-side hook makes startup independent of JS tab/loading order.
    if request.method == 'GET' and _config_is_ngenie_code_locked(config):
        _optional_feature_call(
        "solutions", "run_plan_from_editor", config, user=current_user,
        start_only=True, model_call=call_llm,
    )
        try:
            db.session.refresh(config)
        except Exception:
            pass

    edit_dataset = None
    if request.args.get('edit_dataset'):
        edit_dataset = db.session.get(Dataset, request.args.get('edit_dataset'))
        if not edit_dataset or edit_dataset.config_id != config.id:
            abort(404)    
    
    if request.method == 'POST':

        raw = request.form.get("common_layouts_json", "")
        if raw:
            try:
                config.common_layouts = json.loads(raw)
            except Exception:
                pass
        config.name = request.form.get('name')
        config.server_name = request.form.get('server_name')
        if 'ngenie_prompt' in request.form:
            config.ngenie_prompt = request.form.get('ngenie_prompt') or ''
        if 'profile_templates_json' in request.form and hasattr(config, 'profile_templates'):
            raw_profile_templates = request.form.get('profile_templates_json') or '[]'
            try:
                config.profile_templates = json.loads(raw_profile_templates) if raw_profile_templates.strip() else []
            except Exception as e:
                flash(_('Profile templates JSON error') + ': ' + str(e), 'error')
                return redirect(url_for('edit_config', uid=uid, tab='profile-templates'))
        db.session.commit()
        if 'profile_templates_json' in request.form and hasattr(config, 'profile_templates'):
            _materialize_profile_templates_for_config(config)
        flash(_('Configuration saved'), 'success')
        return redirect(url_for('dashboard'))
    
    rooms = Room.query.filter_by(user_id=current_user.id).order_by(Room.name.asc()).all()
    ui_tpl_buttons, ui_tpl_map = get_ui_component_templates()
    return render_template('edit_config.html',
                           config=config,
                           base64=base64,
                           rooms=rooms,
                           ui_tpl_buttons=ui_tpl_buttons,
                           ui_tpl_map=ui_tpl_map,
                           ngenie_code_available=_ngenie_code_editor_enabled(),
                           ngenie_code_lock_enabled=_ngenie_code_lock_enabled(),
                           ngenie_code_locked=_config_is_ngenie_code_locked(config),
                           can_publish_demo_product=_current_user_has_admin_login())

@_routes.route('/add-class/<config_uid>', methods=['POST'])
@login_required
def add_class(config_uid):
    config = db.session.execute(
        select(Configuration)
        .where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    
    name = request.form.get('name')
    if name:
        new_class = ConfigClass(name=name, config_id=config.id)
        db.session.add(new_class)
        db.session.commit()
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=config_uid, tab=active_tab))

def remove_class_from_module(module_code: str, class_name: str) -> str:
    lines = module_code.split('\n')

    class_start = -1
    class_indent = 0

    # найти строку "class ClassName("
    for i, line in enumerate(lines):
        if line.strip().startswith(f'class {class_name}('):
            class_start = i
            class_indent = len(line) - len(line.lstrip())
            break

    if class_start == -1:
        return module_code  # класс не найден — ничего не меняем

    # найти конец класса: первая НЕ пустая строка с indent <= class_indent
    class_end = len(lines)
    for i in range(class_start + 1, len(lines)):
        cur = lines[i]
        if not cur.strip():
            continue
        cur_indent = len(cur) - len(cur.lstrip())
        if cur_indent <= class_indent:
            class_end = i
            break

    new_lines = lines[:class_start] + lines[class_end:]
    return '\n'.join(new_lines)

@_routes.route('/delete-class/<class_id>')
@login_required
def delete_class(class_id):
    active_tab = request.args.get("tab", "classes")
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)

    cfg = class_obj.config
    config_uid = cfg.uid
    class_name = class_obj.name

    try:
        # ANDROID handlers
        if cfg.nodes_handlers:
            android_code = base64.b64decode(cfg.nodes_handlers).decode("utf-8", errors="replace")
            android_code2 = remove_class_from_module(android_code, class_name)
            if android_code2 != android_code:
                cfg.nodes_handlers = base64.b64encode(android_code2.encode("utf-8")).decode("utf-8")

        # SERVER handlers
        if cfg.nodes_server_handlers:
            server_code = base64.b64decode(cfg.nodes_server_handlers).decode("utf-8", errors="replace")
            server_code2 = remove_class_from_module(server_code, class_name)
            if server_code2 != server_code:
                cfg.nodes_server_handlers = base64.b64encode(server_code2.encode("utf-8")).decode("utf-8")

                handlers_dir = os.path.join('Handlers', cfg.uid)
                os.makedirs(handlers_dir, exist_ok=True)
                with open(os.path.join(handlers_dir, 'handlers.py'), 'w', encoding='utf-8') as f:
                    f.write(server_code2)

        # теперь можно удалять из БД
        db.session.delete(class_obj)

        cfg.update_last_modified()
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        flash(f"Delete class error: {e}", "danger")

    return redirect(url_for('edit_config', uid=config_uid, tab=active_tab))

@_routes.route('/edit-class/<int:class_id>', methods=['GET', 'POST'])
@login_required
def edit_class(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)
    
    
    if class_obj.config.user_id != current_user.id:
        abort(403)
    
    if request.method == 'POST':
        class_obj.name = request.form.get('name')
        # Display tab
        class_obj.display_name = request.form.get('display_name')
        class_obj.record_view = request.form.get('record_view') or ''
        class_obj.cover_image = request.form.get('cover_image')
        class_obj.display_image_web = request.form.get('display_image_web')
        class_obj.display_image_table = request.form.get('display_image_table')
        class_obj.init_screen_layout = request.form.get('init_screen_layout') or ""
        class_obj.init_screen_layout_web = request.form.get('init_screen_layout_web') or ""
        class_obj.show_tag_cloud = 'show_tag_cloud' in request.form
        class_obj.hide_mobile_client = 'hide_mobile_client' in request.form
        class_obj.hide_web_client = 'hide_web_client' in request.form
        class_obj.dashboard_enabled = 'dashboard_enabled' in request.form
        class_obj.dashboard_width = (request.form.get('dashboard_width') or '100').strip()
        if class_obj.dashboard_width not in ('100', '50', '25'):
            class_obj.dashboard_width = '100'
        class_obj.dashboard_top = 'dashboard_top' in request.form
        class_obj.plug_in = request.form.get('plug_in') or ""
        class_obj.plug_in_web = request.form.get('plug_in_web') or ""

        # Commands tab/group
        class_obj.commands = request.form.get('commands')
        class_obj.use_standard_commands = 'use_standard_commands' in request.form
        class_obj.svg_commands = request.form.get('svg_commands')

        # Migration tab
        class_obj.migration_register_command = 'migration_register_command' in request.form
        class_obj.migration_register_on_save = 'migration_register_on_save' in request.form
        class_obj.migration_send_via_queue = 'migration_send_via_queue' in request.form
        class_obj.migration_default_room_alias = (request.form.get('migration_default_room_alias') or '').strip()
        class_obj.link_share_mode = (request.form.get('link_share_mode') or '').strip()
        class_obj.include_in_contract = 'include_in_contract' in request.form
        # Backward compatibility: keep old UID if it's still posted
        if 'migration_default_room_uid' in request.form:
            class_obj.migration_default_room_uid = (request.form.get('migration_default_room_uid') or '').strip()

        indexes_raw = request.form.get('indexes_json') or '[]'
        try:
            parsed_indexes = json.loads(indexes_raw)
            if not isinstance(parsed_indexes, list):
                parsed_indexes = []
        except Exception:
            parsed_indexes = []
        def _parse_index_float01(value, default=0.5):
            raw = default if value is None else value
            if isinstance(raw, str):
                raw = raw.strip().replace(',', '.')
            try:
                parsed = float(raw)
            except Exception:
                parsed = default
            return max(0.0, min(1.0, parsed))

        normalized_indexes = []
        for idx in parsed_indexes:
            if not isinstance(idx, dict):
                continue
            name = str(idx.get('name') or '').strip()
            if not name:
                continue
            kind = str(idx.get('kind') or idx.get('type') or 'hash_index').strip() or 'hash_index'
            raw_keys = idx.get('keys')
            if raw_keys in (None, '', []):
                raw_keys = idx.get('field')
            if raw_keys in (None, '', []):
                raw_keys = idx.get('key')
            if raw_keys in (None, '', []):
                raw_keys = idx.get('fields')
            if isinstance(raw_keys, (list, tuple, set)):
                raw_keys = '|'.join(str(x or '').strip() for x in raw_keys if str(x or '').strip())
            item = {
                'name': name,
                'kind': kind,
                'keys': str(raw_keys or '').strip(),
                'filter_enabled': bool(idx.get('filter_enabled')),
                'filter_label': str(idx.get('filter_label') or '').strip(),
                'filter_type': str(idx.get('filter_type') or 'string').strip() or 'string',
                'filter_list_enabled': bool(idx.get('filter_list_enabled')),
                'server_only': bool(idx.get('server_only')),
                'ngenie_server_search': bool(idx.get('ngenie_server_search')),
            }
            if kind.lower() in {'semantic', 'semantic_index', 'semanic_index'}:
                default_model = 'intfloat/multilingual-e5-small'
                item['model'] = str(idx.get('model') or idx.get('model_name') or idx.get('embedding_model') or default_model).strip() or default_model
                is_default_e5 = item['model'].strip().lower().rstrip('/') == default_model
                default_threshold = 0.8 if is_default_e5 else 0.5
                default_embedding_weight = 1.0 if is_default_e5 else 0.5
                default_token_weight = 0.0 if is_default_e5 else 0.5
                item['threshold'] = _parse_index_float01(idx.get('threshold', idx.get('min_score', idx.get('min_similarity', default_threshold))), default_threshold)
                item['embedding_weight'] = _parse_index_float01(idx.get('embedding_weight', idx.get('semantic_weight', idx.get('vector_weight', default_embedding_weight))), default_embedding_weight)
                item['technical_token_weight'] = _parse_index_float01(idx.get('technical_token_weight', idx.get('token_weight', idx.get('technical_weight', default_token_weight))), default_token_weight)
            normalized_indexes.append(item)
        class_obj.indexes_json = normalized_indexes

        class_obj.has_storage = 'has_storage' in request.form
        class_obj.class_type = request.form.get('class_type')
        if (class_obj.class_type or '') == 'data_node':
            class_obj.data_structure = request.form.get('data_structure') or ""
        else:
            class_obj.data_structure = ""
        if (class_obj.class_type or '') == 'projection':
            class_obj.projection_type = (request.form.get('projection_type') or 'kanban_projection').strip()
            raw_cols = (request.form.get('projection_kanban_columns') or '').strip()
            if class_obj.projection_type == 'kanban_projection':
                try:
                    parsed_cols = json.loads(raw_cols) if raw_cols else []
                    if not isinstance(parsed_cols, list):
                        parsed_cols = []
                    class_obj.projection_kanban_columns = json.dumps(parsed_cols, ensure_ascii=False, indent=2)
                except Exception:
                    class_obj.projection_kanban_columns = raw_cols
            else:
                class_obj.projection_kanban_columns = raw_cols
        else:
            class_obj.projection_type = ''
            class_obj.projection_kanban_columns = ''

        is_print_form_class = (class_obj.class_type or '') == 'print_form'
        class_obj.mobile_print_enabled = ('mobile_print_enabled' in request.form) and not is_print_form_class
        if is_print_form_class:
            class_obj.print_template_type = (request.form.get('print_template_type') or 'html_jinja').strip() or 'html_jinja'
            class_obj.print_target_classes = _normalize_print_targets(request.form.getlist('print_target_classes'))
            class_obj.print_html_template = _encode_print_html_template(request.form.get('print_html_template') or '')
            class_obj.has_storage = False
            class_obj.use_standard_commands = False
        else:
            class_obj.print_template_type = getattr(class_obj, 'print_template_type', '') or 'html_jinja'
            class_obj.print_target_classes = []
            class_obj.print_html_template = ''
        class_obj.hidden = 'hidden' in request.form or (class_obj.class_type or '') == 'print_form'

        if 'ngenie_role' in request.form:
            class_obj.ngenie_role = (request.form.get('ngenie_role') or '').strip()
        if 'ngenie_prompt' in request.form:
            class_obj.ngenie_prompt = request.form.get('ngenie_prompt') or ''
        if 'ngenie_description' in request.form and hasattr(class_obj, 'ngenie_description'):
            class_obj.ngenie_description = request.form.get('ngenie_description') or ''

        section_code = request.form.get('section_code')
        

        section_name = ""
        if section_code:
            section = next((s for s in class_obj.config.sections if s.code == section_code), None)
            if section:
                section_name = section.name

        class_obj.section = section_name
        class_obj.section_code = section_code
        # Parsed web-client configuration is cached by Configuration.last_modified.
        # Touch the parent row for every class edit so SQL-backed cache invalidation
        # is immediate and does not require rebuilding all repositories in a timer.
        class_obj.config.last_modified = datetime.now(timezone.utc)
        db.session.commit()
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"ok": True, "class_id": class_obj.id})
        flash(_('Class saved'), 'success')
        active_tab = request.form.get("active_tab", request.args.get('tab', 'classes'))
        return redirect(url_for('edit_class', class_id=class_obj.id, tab=active_tab))

    rooms = Room.query.filter_by(user_id=current_user.id).order_by(Room.name.asc()).all()
    room_aliases = RoomAlias.query.filter_by(config_id=class_obj.config_id).order_by(RoomAlias.alias.asc()).all()

    ui_tpl_buttons, ui_tpl_map = get_ui_component_templates()
    plugin_tpl_buttons, plugin_tpl_map = get_plugin_templates()

    return render_template('edit_class.html',
                         class_obj=class_obj,
                         rooms=rooms,
                         room_aliases=room_aliases,
                         available_print_target_classes=[c for c in (class_obj.config.classes or []) if c.id != class_obj.id and (c.class_type or '') != 'print_form'],
                         print_html_template_text=_decode_print_html_template(getattr(class_obj, 'print_html_template', '') or ''),
                         ui_tpl_buttons=ui_tpl_buttons,
                         ui_tpl_map=ui_tpl_map,
                         plugin_tpl_buttons=plugin_tpl_buttons,
                         plugin_tpl_map=plugin_tpl_map,
                         wizard_active_buttons=get_wizard_active_templates(),
                         wizard_cover_buttons=get_wizard_cover_templates(),
                         event_types=['onShow', 'onInput', 'onChange', 'onShowWeb', 'onInputWeb', "onAcceptServer", "onAfterAcceptServer", "onAccept","onAfterAcccept"])

@_routes.route('/edit-class/<int:class_id>/rebuild-indexes', methods=['POST'], endpoint='rebuild_class_indexes')
@login_required
def rebuild_class_indexes(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)
    if class_obj.config.user_id != current_user.id:
        abort(403)

    payload = request.get_json(silent=True) or {}
    raw_index = str(payload.get('index_name') or '').strip()
    index_names = [raw_index] if raw_index else None
    config_uid = str(getattr(class_obj.config, 'uid', '') or '').strip()

    try:
        isolated_globals = _load_server_handlers_ns(config_uid, class_obj.config) or {}
        node_class = isolated_globals.get(class_obj.name)
        if node_class is None:
            return jsonify({
                'ok': False,
                'error': _('Server handler class was not found. Save/generate server handlers first.'),
                'nodes': 0,
                'indexes': 0,
            }), 400

        result = node_class.rebuild_defined_indexes(config_uid, index_names=index_names)
        if not isinstance(result, dict):
            result = {'ok': True, 'nodes': 0, 'indexes': 0}
        result.setdefault('ok', True)
        return jsonify(result)
    except Exception as e:
        tb = traceback.format_exc()
        try:
            current_app.logger.exception('Index rebuild failed for class_id=%s index=%s', class_id, raw_index or '*')
        except Exception:
            print(tb)
        payload = {'ok': False, 'error': str(e), 'nodes': 0, 'indexes': 0}
        if current_app.debug or request.args.get('debug') == '1':
            payload['traceback'] = tb
        return jsonify(payload), 500


@_routes.route('/edit-class/<int:class_id>/print-preview', methods=['POST'])
@login_required
def print_form_template_preview(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)
    if class_obj.config.user_id != current_user.id:
        abort(403)
    payload = request.get_json(silent=True) or {}
    template_text = payload.get('template') or ''
    data = payload.get('data') if isinstance(payload.get('data'), dict) else {}
    try:
        html = _render_print_html_template(template_text, data)
        return jsonify({'ok': True, 'html': html})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'html': ''})


@_routes.route('/add-method/<int:class_id>', methods=['POST'])
@login_required
def add_method(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)

    method_name = request.form['name']    
    
    new_method = ClassMethod(
        name=method_name,
        source='internal',
        engine=request.form['engine'],
        code=method_name,
        class_id=class_id
    )
    
    function_body = request.form.get('function_body', '')
    
    
    if new_method.engine == 'server_python':
        current_module = ""
        if class_obj.config.nodes_server_handlers:
            current_module = base64.b64decode(class_obj.config.nodes_server_handlers).decode('utf-8')
        
        
        new_module = add_method_to_class(current_module, class_obj.name, new_method.code, function_body)
        if new_module!=None:
            
            class_obj.config.nodes_server_handlers = base64.b64encode(new_module.encode('utf-8')).decode('utf-8')
            
            
            handlers_dir = os.path.join('Handlers', class_obj.config.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                f.write(new_module)
    
    
    elif new_method.engine == 'android_python':
        current_module = ""
        if class_obj.config.nodes_handlers:
            current_module = base64.b64decode(class_obj.config.nodes_handlers).decode('utf-8')
        
        
        new_module = add_method_to_class(current_module, class_obj.name, new_method.code, function_body)
        
       
        if new_module!=None:
            class_obj.config.nodes_handlers = base64.b64encode(new_module.encode('utf-8')).decode('utf-8')
    
    db.session.add(new_method)
    db.session.commit()
    
    
    exclude_methods = [(class_obj.name, new_method.code)]
    sync_methods_from_code(class_obj.config, exclude_methods)
    
    return redirect(url_for('edit_class', class_id=class_id, _anchor='handlers-refresh'))

@_routes.route('/delete-method/<int:method_id>')
@login_required
def delete_method(method_id):
    method = db.session.get(ClassMethod, method_id)
    if not method:
        abort(404)
    
    class_id = method.class_id
    config = method.class_obj.config

    class_name = method.class_obj.name
    method_name = method.code
    engine = method.engine

    db.session.delete(method)
    db.session.commit()

    remove_method_from_code(config, class_name, method_name, engine)

    return redirect(url_for('edit_class', class_id=class_id))

@_routes.route('/edit-method/<int:method_id>', methods=['GET', 'POST'])
@login_required
def edit_method(method_id):
    method = db.session.get(ClassMethod, method_id)
    if not method:
        abort(404)
    
    
    if method.class_obj.config.user_id != current_user.id:
        abort(403)
    
    if request.method == 'POST':
        method.name = request.form['name']
        method.source = request.form['source']
        method.engine = request.form['engine']
        method.code = request.form['code']
        db.session.commit()
        flash(_('Method updated successfully'), 'success')
        return redirect(url_for('edit_class', class_id=method.class_id))
    
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({
            'name': method.name,
            'source': method.source,
            'engine': method.engine,
            'code': method.code
        })
    
    return render_template('edit_method.html', method=method)

@_routes.route('/add-event/<int:class_id>', methods=['POST'])
@login_required
def add_event(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj or class_obj.config.user_id != current_user.id:
        abort(403)

    event_name = request.form.get('event_name','').strip()
    listener = request.form.get('listener','').strip()

    actions_json = request.form.get('actions_json')
    try:
        actions = json.loads(actions_json) if actions_json else []
    except Exception:
        flash(_('Invalid actions format (JSON)'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    if not event_name or not isinstance(actions, list) or len(actions)==0:
        flash(_('Event type and at least one action required'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    
    for a in actions:
        mname = a.get('method','').strip()
        if mname and not _is_builtin_action_method(mname):
            m = db.session.execute(
                select(ClassMethod).where(ClassMethod.name == mname, ClassMethod.class_id == class_id)
            ).scalar_one_or_none()
            if not m:
                flash(_('Method')+ mname+_(' not found in class'), 'error')
                return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    
    existing = db.session.scalars(
    select(ClassEvent).where(ClassEvent.class_id==class_id, 
                            ClassEvent.event==event_name, 
                            ClassEvent.listener==listener)
    .limit(1)
    ).first()
    if existing:
        flash(_('Event with this event+listener already exists'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"class_{class_id}_{event_name or 'event'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        flash(f"PythonScript S3 autosave failed: {exc}", 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    ce = ClassEvent(event=event_name, listener=listener, class_id=class_id)
    db.session.add(ce)
    db.session.flush()  

    order = 0
    for a in actions:
        order += 1
        ea = EventAction(
            action = a.get('action','run'),
            source = a.get('source','internal') or 'internal',
            server = a.get('server','') or '',
            method = _action_method_value(a, 'method'),
            post_execute_method = _action_method_value(a, 'postExecuteMethod'),
            method_text = (_action_python_text_value(a, post=False) or '') if _is_script_text_method(a.get('method','')) else '',
            post_execute_text = (_action_python_text_value(a, post=True) or '') if _is_script_text_method(a.get('postExecuteMethod','')) else '',
            http_function_name = (a.get('httpFunctionName','') or '') if _is_http_request_method(a.get('method','')) else '',
            post_http_function_name = (a.get('postHttpFunctionName','') or '') if _is_http_request_method(a.get('postExecuteMethod','')) else '',
            order = order,
            event_id = ce.id
        )
        db.session.add(ea)

    db.session.commit()
    flash(_('Event added'), 'success')
    return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

@_routes.route('/edit-event/<int:class_id>', methods=['POST'])
@login_required
def edit_event(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj or class_obj.config.user_id != current_user.id:
        abort(403)

    old_event = request.form.get('old_event_name','')
    old_listener = request.form.get('old_listener','')

    # find target event
    target = db.session.execute(
        select(ClassEvent).where(ClassEvent.class_id==class_id,
                                 ClassEvent.event==old_event,
                                 ClassEvent.listener==old_listener)
    ).scalar_one_or_none()

    if not target:
        flash(_('Original event not found'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    new_event = request.form.get('event_name','').strip()
    new_listener = request.form.get('listener','').strip()
    actions_json = request.form.get('actions_json')
    try:
        actions = json.loads(actions_json) if actions_json else []
    except Exception:
        flash(_('Invalid actions format (JSON)'), 'error')

        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    if not new_event or not isinstance(actions, list) or len(actions)==0:
        flash(_('Event type and at least one action required'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    
    for a in actions:
        mname = a.get('method','').strip()
        if mname and not _is_builtin_action_method(mname):
            m = db.session.execute(
                select(ClassMethod).where(ClassMethod.name == mname, ClassMethod.class_id == class_id)
            ).first()
            if not m:
                flash(_('Method %(mname)s not found in class', mname=mname), 'error')
                return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    
    _carry_existing_event_python_script_refs(actions, getattr(target, "actions", None))

    try:
        _normalize_event_action_python_scripts_for_save(
            actions,
            filename_prefix=f"class_{class_id}_{new_event or 'event'}"
        )
    except Exception as exc:
        current_app.logger.exception("PythonScript S3 autosave failed")
        flash(f"PythonScript S3 autosave failed: {exc}", 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    target.event = new_event
    target.listener = new_listener

    
    for act in list(target.actions):
        db.session.delete(act)
    db.session.flush()

    order = 0
    for a in actions:
        order += 1
        ea = EventAction(
            action = a.get('action','run'),
            source = a.get('source','internal') or 'internal',
            server = a.get('server','') or '',
            method = _action_method_value(a, 'method'),
            post_execute_method = _action_method_value(a, 'postExecuteMethod'),
            method_text = (_action_python_text_value(a, post=False) or '') if _is_script_text_method(a.get('method','')) else '',
            post_execute_text = (_action_python_text_value(a, post=True) or '') if _is_script_text_method(a.get('postExecuteMethod','')) else '',
            http_function_name = (a.get('httpFunctionName','') or '') if _is_http_request_method(a.get('method','')) else '',
            post_http_function_name = (a.get('postHttpFunctionName','') or '') if _is_http_request_method(a.get('postExecuteMethod','')) else '',
            order = order,
            event_id = target.id
        )
        db.session.add(ea)

    db.session.commit()
    flash(_('Event updated'), 'success')
    return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

@_routes.route('/delete-event/<int:class_id>', methods=['POST'])
@login_required
def delete_event(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj or class_obj.config.user_id != current_user.id:
        abort(403)

    event_name = request.form.get('event_name','')
    listener = request.form.get('listener','').strip()

    target = db.session.execute(
        select(ClassEvent).where(ClassEvent.class_id==class_id,
                                 ClassEvent.event==event_name,
                                 ClassEvent.listener==listener)
    ).scalar_one_or_none()

    if not target:
        flash(_('Event not found'), 'error')
        return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

    db.session.delete(target)
    db.session.commit()
    flash(_('Event deleted'), 'success')
    return redirect(url_for('edit_class', class_id=class_id, _anchor='events'))

@_routes.route('/create-config', methods=['POST'])
@login_required
def create_config():
    
    new_config = Configuration(
    name=_("New configuration"),
    user_id=current_user.id,
    content_uid=str(uuid.uuid4()),
    vendor=current_user.config_display_name or current_user.email,
    version="00.00.01"
)

    new_config.uid = str(uuid.uuid4())

    
    android_imports = ANDROID_IMPORTS_TEMPLATE.format(
        uid=new_config.uid, 
        config_url=url_for('get_config', uid=new_config.uid, _external=True)
    )
    default_handlers = android_imports + NODE_CLASS_CODE_ANDROID 
    new_config.nodes_handlers = base64.b64encode(default_handlers.encode('utf-8')).decode('utf-8')

    
    default_server_handlers = NODE_CLASS_CODE 
    new_config.nodes_server_handlers = base64.b64encode(default_server_handlers.encode('utf-8')).decode('utf-8')

    db.session.add(new_config)
    db.session.commit()

    
    handlers_dir = os.path.join('Handlers', new_config.uid)
    os.makedirs(handlers_dir, exist_ok=True)
    
    
    handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
    with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
        f.write(default_server_handlers)
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=new_config.uid, tab=active_tab))

@_routes.route('/create-class/<config_uid>', methods=['POST'])
@login_required
def create_class(config_uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)
    
    
    class_name = request.form.get('name')
    if not class_name:
        flash(_('Class name not specified'), 'danger')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=config_uid, tab=active_tab))
    
   
    import re
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', class_name):
        flash(_('Class name must start with a letter or underscore and contain only letters, numbers and underscores'), 'error')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=config_uid, tab=active_tab))
    
    
    existing_class = next((c for c in config.classes if c.name == class_name), None)
    if existing_class:
        flash(_('Class with this name already exists'), 'danger')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=config_uid, tab=active_tab))
    
    
    requested_class_type = (request.form.get('class_type') or 'custom_process').strip()
    if requested_class_type not in ('custom_process', 'projection', 'data_node', 'custom_task', 'background_task', 'solo_object', 'print_form'):
        requested_class_type = 'custom_process'

    new_class = ConfigClass(
        name=class_name,
        display_name=class_name,
        config_id=config.id,
        class_type=requested_class_type,
        section_code='' if requested_class_type == 'print_form' else 'server',
        has_storage=False if requested_class_type == 'print_form' else False,
        use_standard_commands=False if requested_class_type == 'print_form' else True,
        hidden=True if requested_class_type == 'print_form' else False,
        print_template_type='html_jinja' if requested_class_type == 'print_form' else '',
        print_target_classes=[] if requested_class_type == 'print_form' else [],
        print_html_template=_encode_print_html_template('''<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body { font-family: Arial, sans-serif; font-size: 14px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border: 1px solid #ddd; padding: 6px; }
  </style>
</head>
<body>
  <h1>{{ _basement_data._view or _basement_data._id }}</h1>
  {# Example: {{ customer._id }} {{ customer.name }} #}
</body>
</html>''') if requested_class_type == 'print_form' else '',
    )
    db.session.add(new_class)
    db.session.commit()
    
    
    if config.nodes_server_handlers:
        try:
            current_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            
            if 'from nodes import Node' not in current_code:
                current_code = NODE_CLASS_CODE + '\n\n' + current_code
            
            
            new_class_code = f'''
class {class_name}(Node):
    
    def __init__(self, node_id=None, config_uid=None):
        super().__init__(node_id, config_uid)
        # Additional initialozation for {class_name}
'''
            current_code += '\n\n' + new_class_code
            
            
            config.nodes_server_handlers = base64.b64encode(current_code.encode('utf-8')).decode('utf-8')
            
            
            handlers_dir = os.path.join('Handlers', config.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                f.write(current_code)
                
        except Exception as e:
            print(f"Error updating server handlers: {str(e)}")
    
    
    if config.nodes_handlers:
        try:
            current_code = base64.b64decode(config.nodes_handlers).decode('utf-8')
            
            
            if 'from nodes import Node' not in current_code:
                current_code = NODE_CLASS_CODE_ANDROID + '\n' + current_code
            
            
            new_class_code = f'''
class {class_name}(Node):
    def __init__(self, modules, jNode, modulename, uid, _data):
        super().__init__(modules, jNode, modulename, uid, _data)

    """Class {class_name}"""
'''
            current_code += '\n\n' + new_class_code
            
            
            config.nodes_handlers = base64.b64encode(current_code.encode('utf-8')).decode('utf-8')
                
        except Exception as e:

            print(f"Error updating android handlers: {str(e)}")
    
    db.session.commit()
    flash(_('Class created successfully'), 'success')
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_class', class_id=new_class.id, tab=active_tab))

@_routes.route('/class/<int:class_id>/export-json', methods=['GET'])
@login_required
def export_class_json(class_id):
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj:
        abort(404)
    if class_obj.config.user_id != current_user.id:
        abort(403)
    if _config_is_ngenie_code_locked(class_obj.config):
        abort(403, description=_ngenie_code_forbid_message())

    payload = _export_class_json(class_obj)
    buf = io.BytesIO(json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8'))
    filename = f"{class_obj.name or 'class'}.json"
    return send_file(buf, as_attachment=True, download_name=filename, mimetype='application/json')

@_routes.route('/export-config/<uid>')
@login_required
def export_config(uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    if _config_is_ngenie_code_locked(config):
        flash(_ngenie_code_forbid_message(), 'error')
        return redirect(url_for('edit_config', uid=uid, tab='ai-generator'))
    
    
    provider = (config.user.config_display_name 
               if config.user and hasattr(config.user, 'config_display_name') 
               else (config.user.email if config.user else 'Unknown'))

    
    local_time = config.last_modified.astimezone(pytz.timezone('Europe/Moscow'))

    base_url = url_for('get_config', uid=config.uid, _external=True)
    
    
    config_data = {
        'name': config.name,
        'server_name': config.server_name,
        'uid': config.uid,
        'url':base_url,
        'content_uid': config.content_uid,
        'vendor': config.vendor,
        'nodes_handlers': config.nodes_handlers,
        'nodes_handlers_meta': config.nodes_handlers_meta,
        'nodes_server_handlers': config.nodes_server_handlers,  
        'nodes_server_handlers_meta': config.nodes_server_handlers_meta,  
        'ngenie_prompt': getattr(config, 'ngenie_prompt', '') or '',
        'ngenie_code_locked': bool(getattr(config, 'ngenie_code_locked', False)),
        'ngenie_code_instruction': getattr(config, 'ngenie_code_instruction', '') or '',
        'ngenie_code_example': getattr(config, 'ngenie_code_example', '') or '',
        'demo_product': bool(getattr(config, 'demo_product', False)),
        'version': getattr(config, 'version', '00.00.01'),
        "NodaLogicFormat": NL_FORMAT,
        "NodaLogicType": "ANDROID_SERVER",
        'last_modified': local_time.isoformat(),
        'provider': config.vendor,
        "CommonLayouts": config.common_layouts or [],
        "profile_templates": getattr(config, 'profile_templates', None) or [],
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
                'data_structure': getattr(c, 'data_structure', '') or '',
                'ngenie_role': getattr(c, 'ngenie_role', '') or '',
                'ngenie_prompt': getattr(c, 'ngenie_prompt', '') or '',
                'ngenie_description': getattr(c, 'ngenie_description', '') or '',
                'show_tag_cloud': bool(getattr(c, 'show_tag_cloud', False)),
                'mobile_print_enabled': bool(getattr(c, 'mobile_print_enabled', False)),
                'hide_mobile_client': bool(getattr(c, 'hide_mobile_client', False)),
                'hide_web_client': bool(getattr(c, 'hide_web_client', False)),
                'dashboard_enabled': bool(getattr(c, 'dashboard_enabled', False)),
                'dashboard_width': str(getattr(c, 'dashboard_width', '') or '100'),
                'dashboard_top': bool(getattr(c, 'dashboard_top', False)),
                'plug_in': getattr(c, 'plug_in', '') or '',
                'plug_in_web': getattr(c, 'plug_in_web', '') or '',

                'commands': getattr(c, 'commands', '') or '',
                'use_standard_commands': bool(getattr(c, 'use_standard_commands', True)),
                'svg_commands': getattr(c, 'svg_commands', '') or '',
                # Migration tab
                'migration_register_command': bool(getattr(c, 'migration_register_command', False)),
                'migration_register_on_save': bool(getattr(c, 'migration_register_on_save', False)),
                'migration_send_via_queue': bool(getattr(c, 'migration_send_via_queue', False)),
                'migration_default_room_uid': getattr(c, 'migration_default_room_uid', '') or '',
                'migration_default_room_alias': getattr(c, 'migration_default_room_alias', '') or '',
                'link_share_mode': getattr(c, 'link_share_mode', '') or '',
                'include_in_contract': bool(getattr(c, 'include_in_contract', False)),
                'indexes': getattr(c, 'indexes_json', None) or [],

                'class_type': c.class_type,
                'projection_type': getattr(c, 'projection_type', '') or '',
                'projection_kanban_columns': getattr(c, 'projection_kanban_columns', '') or '',
                'print_template_type': getattr(c, 'print_template_type', '') or 'html_jinja',
                'print_target_classes': getattr(c, 'print_target_classes', None) or [],
                'print_html_template': _encode_print_html_template(getattr(c, 'print_html_template', '') or ''),
                'hidden': c.hidden,
                'hide_mobile_client': bool(getattr(c, 'hide_mobile_client', False)),
                'hide_web_client': bool(getattr(c, 'hide_web_client', False)),
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
                                'method': _normalize_special_method_name_for_export(a.method),
                                'postExecuteMethod': _normalize_special_method_name_for_export(a.post_execute_method),
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
                'api_url': f"{base_url}/dataset/{d.name}/items",
                'item_count': len(d.items)
            } for d in config.datasets
        ],
        'sections': [
            {
                'name': s.name,
                'code': s.code,
                'commands': s.commands,
                'hide_mobile_client': bool(getattr(s, 'hide_mobile_client', False)),
                'hide_web_client': bool(getattr(s, 'hide_web_client', False))
            } for s in config.sections
        ],
        "servers": [
            {"alias": s.alias, "url": s.url, "is_default": s.is_default}
            for s in config.servers
        ],
        "rooms": [
            {"alias": r.alias, "room_id": r.room_uid}
            for r in (getattr(config, 'room_aliases', None) or [])
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
                        'method': _normalize_special_method_name_for_export(a.method),
                        'postExecuteMethod': _normalize_special_method_name_for_export(a.post_execute_method),
                        **({'methodText': a.method_text} if _is_script_text_method(a.method) else {}),
                        **({'postExecuteMethodText': a.post_execute_text} if _is_script_text_method(a.post_execute_method) else {}),
                        **({'httpFunctionName': a.http_function_name} if _is_http_request_method(a.method) else {}),
                        **({'postHttpFunctionName': a.post_http_function_name} if _is_http_request_method(a.post_execute_method) else {})
                    }
                    for a in e.actions
                ]
            }
            for e in config.config_events
        ],
        'Timers': [
            t.to_dict() if hasattr(t, 'to_dict') else {
                'id': getattr(t, 'timer_id', '') or '',
                'timer_id': getattr(t, 'timer_id', '') or '',
                'period_seconds': _normalize_timer_period_seconds(getattr(t, 'period_seconds', 0) or 0, runtime=getattr(t, 'runtime', '') or 'server', worker=bool(getattr(t, 'worker', False))),
                'active': bool(getattr(t, 'active', False)),
                'worker': bool(getattr(t, 'worker', False)),
                'runtime': _normalize_timer_runtime(getattr(t, 'runtime', '') or 'server'),
                'actions': t.actions_as_dicts() if hasattr(t, 'actions_as_dicts') else [],
            }
            for t in (getattr(config, 'config_timers', None) or [])
        ]
    }
    
    file_obj = io.BytesIO(json.dumps(config_data, ensure_ascii=False, indent=4).encode('utf-8'))
    file_obj.seek(0)
    
    return send_file(
        file_obj,
        as_attachment=True,
        download_name=f'config_{config.name}.nod',
        mimetype='application/json'
    )

def _normalize_imported_indexes(raw_indexes):
    """Normalize both kind/keys and legacy type/field index definitions."""
    if isinstance(raw_indexes, str):
        try:
            raw_indexes = json.loads(raw_indexes)
        except Exception:
            raw_indexes = []
    if not isinstance(raw_indexes, list):
        return []
    out = []
    positions = {}
    for raw in raw_indexes:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        name = str(item.get('name') or item.get('index') or item.get('id') or '').strip()
        if not name:
            continue
        kind = str(item.get('kind') or item.get('type') or 'hash_index').strip().lower() or 'hash_index'
        keys = item.get('keys')
        if keys in (None, '', []):
            keys = item.get('field')
        if keys in (None, '', []):
            keys = item.get('key')
        if keys in (None, '', []):
            keys = item.get('fields')
        if isinstance(keys, (list, tuple, set)):
            keys = '|'.join(str(x or '').strip() for x in keys if str(x or '').strip())
        else:
            keys = str(keys or '').strip()
        item['name'] = name
        item['kind'] = kind
        item['keys'] = keys
        item.setdefault('type', kind)
        if keys and not item.get('field') and '|' not in keys:
            item['field'] = keys
        if name in positions:
            out[positions[name]] = item
        else:
            positions[name] = len(out)
            out.append(item)
    return out


@_routes.route('/import-config-new', methods=['POST'])
@login_required
def import_config_new():
    """Import configuration from file - creates a new one or updates an existing one"""
    if 'config_file' not in request.files:
        flash(_('File not selected'), 'error')
        return redirect(url_for('dashboard'))
    
    file = request.files['config_file']
    if file.filename == '':
        flash(_('File not selected'), 'error')
        return redirect(url_for('dashboard'))
    
    if not file.filename.endswith('.nod'):
        flash(_('Only NOD files allowed'), 'error')
        return redirect(url_for('dashboard'))
    
    try:
        data = json.load(file.stream)
        
        print(f"Starting import of configuration")
        print(f"Data keys: {list(data.keys())}")
        
        
        imported_uid = data.get('uid')
        content_uid = data.get("content_uid")
        if not imported_uid:
            flash(_('Invalid configuration file: missing UID'), 'error')
            return redirect(url_for('dashboard'))
        
        # CHECKING IF A CONFIGURATION WITH THIS UID ALREADY EXISTS
        existing_config = db.session.execute(
            select(Configuration).where(
                Configuration.user_id == current_user.id,
                Configuration.content_uid == content_uid,
                _designer_visible_configuration_clause(),
            )
        ).scalar_one_or_none()
        
        if existing_config:
            # IF THE CONFIGURATION EXISTS, UPDATE IT
            print(f"Updating existing configuration: {existing_config.name}")
            
            # Checking access rights
            if existing_config.user_id != current_user.id:
                flash(_('You do not have permission to update this configuration'), 'error')
                return redirect(url_for('dashboard'))
            
            # UPDATE MAIN CONFIGURATION DATA
            existing_config.name = data.get('name', existing_config.name)
            existing_config.server_name = data.get('server_name', existing_config.server_name)
            existing_config.version = data.get('version', existing_config.version)
            existing_config.vendor = data.get('vendor', data.get('provider', existing_config.vendor))
            existing_config.nodes_handlers = data.get('nodes_handlers', existing_config.nodes_handlers)
            existing_config.nodes_handlers_meta = data.get('nodes_handlers_meta', existing_config.nodes_handlers_meta)
            existing_config.nodes_server_handlers = data.get('nodes_server_handlers', existing_config.nodes_server_handlers)
            existing_config.nodes_server_handlers_meta = data.get('nodes_server_handlers_meta', existing_config.nodes_server_handlers_meta)
            existing_config.ngenie_prompt = data.get('ngenie_prompt', getattr(existing_config, 'ngenie_prompt', '') or '')
            if hasattr(existing_config, 'demo_product'):
                existing_config.demo_product = (
                    bool(data.get('demo_product', False))
                    if _current_user_has_admin_login() else False
                )
            if hasattr(existing_config, 'ngenie_code_locked'):
                existing_config.ngenie_code_locked = _ngenie_code_bool(data.get('ngenie_code_locked'))
            if hasattr(existing_config, 'ngenie_code_instruction'):
                existing_config.ngenie_code_instruction = data.get('ngenie_code_instruction', getattr(existing_config, 'ngenie_code_instruction', '') or '')
            if hasattr(existing_config, 'ngenie_code_example'):
                existing_config.ngenie_code_example = data.get('ngenie_code_example', getattr(existing_config, 'ngenie_code_example', '') or '')
            
            # Delete all existing related data for a complete update
            print("Deleting existing related data...")
            for class_obj in existing_config.classes:
                db.session.delete(class_obj)
            for dataset in existing_config.datasets:
                db.session.delete(dataset)
            for section in existing_config.sections:
                db.session.delete(section)
            for server in existing_config.servers:
                db.session.delete(server)
            for ra in (getattr(existing_config, 'room_aliases', None) or []):
                db.session.delete(ra)
            for event in existing_config.config_events:
                db.session.delete(event)
            for timer in (getattr(existing_config, 'config_timers', None) or []):
                db.session.delete(timer)
            
            config_to_use = existing_config
            is_update = True

            # Import common layouts
            config_to_use.common_layouts = data.get('CommonLayouts', data.get('common_layouts', [])) or []
            if hasattr(config_to_use, 'profile_templates'):
                config_to_use.profile_templates = data.get('profile_templates', data.get('ProfileTemplates', [])) or []
            
        else:
            # IF THERE IS NO CONFIGURATION - CREATE A NEW ONE
            print(f"Creating new configuration with UID: {imported_uid}")
            
            new_config = Configuration(
                name=data.get('name', _('Imported configuration')),
                server_name=data.get('server_name', ''),
                version=data.get('version', '00.00.01'),
                nodes_handlers=data.get('nodes_handlers', ''),
                nodes_handlers_meta=data.get('nodes_handlers_meta', {}),
                nodes_server_handlers=data.get('nodes_server_handlers', ''),
                nodes_server_handlers_meta=data.get('nodes_server_handlers_meta', {}),
                user_id=current_user.id,
                uid=str(uuid.uuid4()), 
                content_uid=content_uid,
                vendor=data.get("vendor", data.get("provider")),
                common_layouts=data.get('CommonLayouts', data.get('common_layouts', [])) or [],
                profile_templates=data.get('profile_templates', data.get('ProfileTemplates', [])) or [],
                ngenie_prompt=data.get('ngenie_prompt', ''),
                ngenie_code_locked=_ngenie_code_bool(data.get('ngenie_code_locked')),
                ngenie_code_instruction=data.get('ngenie_code_instruction', ''),
                ngenie_code_example=data.get('ngenie_code_example', ''),
                demo_product=(
                    bool(data.get('demo_product', False))
                    if _current_user_has_admin_login() else False
                )
            )
            
            db.session.add(new_config)
            db.session.flush()
            config_to_use = new_config
            is_update = False

            # Import common layouts
            config_to_use.common_layouts = data.get('CommonLayouts', data.get('common_layouts', [])) or []
            if hasattr(config_to_use, 'profile_templates'):
                config_to_use.profile_templates = data.get('profile_templates', data.get('ProfileTemplates', [])) or []
        
        config_to_use.nodes_handlers = _rewrite_android_handlers_instance_refs_b64(
            config_to_use.nodes_handlers,
            config_to_use.uid,
            url_for('get_config', uid=config_to_use.uid, _external=True)
        )
        
        # IMPORT CLASSES (same for creation and update)
        classes_data = data.get('classes', [])
        print(f"Importing {len(classes_data)} classes...")
        
        for class_data in classes_data:
            new_class = ConfigClass(
                name=class_data['name'],
                section=class_data.get('section', ''),
                section_code=class_data.get('section_code', ''),
                has_storage=class_data.get('has_storage', False),
                display_name=class_data.get('display_name', class_data['name']),
                record_view=class_data.get('record_view', ''),
                cover_image=class_data.get('cover_image', ''),
                display_image_web=class_data.get('display_image_web', ''),
                display_image_table=class_data.get('display_image_table', ''),
                init_screen_layout=class_data.get('init_screen_layout', ''),
                init_screen_layout_web=class_data.get('init_screen_layout_web', ''),
                data_structure=class_data.get('data_structure', class_data.get('dataStructure', '')),
                ngenie_role=class_data.get('ngenie_role', class_data.get('ngenieRole', class_data.get('nGenieRole', ''))),
                ngenie_prompt=class_data.get('ngenie_prompt', class_data.get('ngeniePrompt', class_data.get('nGeniePrompt', ''))),
                ngenie_description=class_data.get('ngenie_description', class_data.get('ngenieDescription', class_data.get('nGenieDescription', ''))),
                show_tag_cloud=bool(class_data.get('show_tag_cloud', class_data.get('showTagCloud', False))),
                mobile_print_enabled=bool(class_data.get('mobile_print_enabled', class_data.get('mobilePrintEnabled', False))),
                dashboard_enabled=bool(class_data.get('dashboard_enabled', class_data.get('dashboardEnabled', False))),
                dashboard_width=str(class_data.get('dashboard_width', class_data.get('dashboardWidth', '100')) or '100'),
                dashboard_top=bool(class_data.get('dashboard_top', class_data.get('dashboardTop', False))),
                plug_in=class_data.get('plug_in', ''),
                plug_in_web=class_data.get('plug_in_web', ''),

                commands=class_data.get('commands', ''),
                use_standard_commands=bool(class_data.get('use_standard_commands', True)),
                svg_commands=class_data.get('svg_commands', ''),

                migration_register_command=bool(class_data.get('migration_register_command', False)),
                migration_register_on_save=bool(class_data.get('migration_register_on_save', False)),
                migration_send_via_queue=bool(class_data.get('migration_send_via_queue', class_data.get('migrationSendViaQueue', False))),
                migration_default_room_uid=class_data.get('migration_default_room_uid', ''),
                migration_default_room_alias=class_data.get('migration_default_room_alias', ''),
                link_share_mode=class_data.get('link_share_mode', ''),
                include_in_contract=bool(class_data.get('include_in_contract', class_data.get('includeInContract', False))),
                indexes_json=_normalize_imported_indexes(class_data.get('indexes', class_data.get('indexes_json', class_data.get('indexesJson', []))) or []),

                class_type=class_data.get('class_type', ''),
                projection_type=class_data.get('projection_type', ''),
                projection_kanban_columns=class_data.get('projection_kanban_columns', ''),
                print_template_type=class_data.get('print_template_type', class_data.get('printTemplateType', 'html_jinja')),
                print_target_classes=class_data.get('print_target_classes', class_data.get('printTargetClasses', [])) or [],
                print_html_template=_encode_print_html_template(class_data.get('print_html_template', class_data.get('printHtmlTemplate', ''))),
                hidden=class_data.get('hidden', False),
                hide_mobile_client=bool(class_data.get('hide_mobile_client', class_data.get('hideMobileClient', False))),
                hide_web_client=bool(class_data.get('hide_web_client', class_data.get('hideWebClient', False))),
                config_id=config_to_use.id
            )
            db.session.add(new_class)
            db.session.flush()
            
            # Import class methods
            methods_data = class_data.get('methods', [])
            print(f"  Importing {len(methods_data)} methods for class {class_data['name']}")
            
            for method_data in methods_data:
                new_method = ClassMethod(
                    name=method_data['name'],
                    source=method_data.get('source', 'internal'),
                    engine=method_data['engine'],
                    code=method_data['code'],
                    class_id=new_class.id
                )
                db.session.add(new_method)
            
            # Import class events
            events_data = class_data.get('events', [])
            print(f"  Importing {len(events_data)} events for class {class_data['name']}")
            
            for event_data in events_data:
                event_name = event_data['event']
                new_event = ClassEvent(
                    event=event_name,
                    listener=event_data.get('listener', ''),
                    class_id=new_class.id
                )
                db.session.add(new_event)
                db.session.flush()
                
                # Import event actions
                actions_data = event_data.get('actions', [])
                print(f"    Importing {len(actions_data)} actions for event {event_name}")
                
                for action_data in actions_data:
                    new_action = EventAction(
                        action=action_data.get('action', 'run'),
                        source=action_data.get('source', 'internal'),
                        server=action_data.get('server', ''),
                        method=_action_method_value(action_data, 'method'),
                        post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                        method_text=_action_method_text_value(action_data, post=False),
                        post_execute_text=_action_method_text_value(action_data, post=True),
                        http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                        post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else '',
                        order=action_data.get('order', 0),
                        event_id=new_event.id
                    )
                    db.session.add(new_action)
        
        # Import datasets
        datasets_data = data.get('datasets', [])
        print(f"Importing {len(datasets_data)} datasets...")
        
        for dataset_data in datasets_data:
            # Convert arrays back to strings for storage in the database
            hash_indexes = ','.join(dataset_data.get('hash_indexes', [])) if isinstance(dataset_data.get('hash_indexes'), list) else dataset_data.get('hash_indexes', '')
            text_indexes = ','.join(dataset_data.get('text_indexes', [])) if isinstance(dataset_data.get('text_indexes'), list) else dataset_data.get('text_indexes', '')
            
            new_dataset = Dataset(
                name=dataset_data['name'],
                hash_indexes=hash_indexes,
                text_indexes=text_indexes,
                view_template=dataset_data.get('view_template', ''),
                autoload=dataset_data.get('autoload', False),
                config_id=config_to_use.id
            )
            db.session.add(new_dataset)
        
        # Import sections
        sections_data = data.get('sections', [])
        print(f"Importing {len(sections_data)} sections...")
        
        for section_data in sections_data:
            new_section = ConfigSection(
                name=section_data['name'],
                code=section_data['code'],
                commands=section_data.get('commands', ''),
                hide_mobile_client=bool(section_data.get('hide_mobile_client', section_data.get('hideMobileClient', False))),
                hide_web_client=bool(section_data.get('hide_web_client', section_data.get('hideWebClient', False))),
                config_id=config_to_use.id
            )
            db.session.add(new_section)
        
        # Import servers
        servers_data = data.get('servers', [])
        print(f"Importing {len(servers_data)} servers...")
        
        for server_data in servers_data:
            new_server = Server(
                alias=server_data['alias'],
                url=server_data['url'],
                is_default=server_data.get('is_default', False),
                config_id=config_to_use.id
            )
            db.session.add(new_server)


        # Import room aliases (rooms)
        rooms_data = data.get('rooms', []) or []
        print(f"Importing {len(rooms_data)} room aliases...")
        for rdata in rooms_data:
            alias = (rdata.get('alias') or '').strip()
            room_uid = (rdata.get('room_id') or rdata.get('room_uid') or '').strip()
            if not alias:
                continue
            new_ra = RoomAlias(
                alias=alias,
                room_uid=room_uid,
                config_id=config_to_use.id
            )
            db.session.add(new_ra)

        common_events_data = data.get('CommonEvents', [])
        print(f"Importing {len(common_events_data)} common events.")

        for ev_data in common_events_data:
            new_event = ConfigEvent(
                event=ev_data['event'],
                listener=ev_data.get('listener', ''),
                config_id=config_to_use.id
            )
            db.session.add(new_event)

            for action_data in ev_data.get('actions', []):
                new_action = ConfigEventAction(
                    event_obj=new_event,
                    action=action_data.get('action', ''),
                    source=action_data.get('source', ''),
                    server=action_data.get('server', ''),
                    method=_action_method_value(action_data, 'method'),
                    post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                    method_text=_action_method_text_value(action_data, post=False),
                    post_execute_text=_action_method_text_value(action_data, post=True),
                    http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                    post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else ''
                )
                db.session.add(new_action)

        timers_data = data.get('Timers', data.get('timers', []))
        print(f"Importing {len(timers_data)} timers.")

        for timer_data in timers_data:
            timer_id = (timer_data.get('timer_id') or timer_data.get('id') or '').strip()
            if not timer_id:
                continue
            worker = _bool_from_timer_value(timer_data.get('worker'), default=False)
            runtime = _timer_runtime_from_timer_data(timer_data)
            new_timer = ConfigTimer(
                timer_id=timer_id,
                period_seconds=_period_seconds_from_timer_data(timer_data, runtime=runtime, worker=worker),
                active=_bool_from_timer_value(timer_data.get('active'), default=True),
                worker=worker,
                runtime=runtime,
                config_id=config_to_use.id
            )
            db.session.add(new_timer)

            for action_data in timer_data.get('actions', []):
                new_action = ConfigTimerAction(
                    timer_obj=new_timer,
                    action=action_data.get('action', ''),
                    source=action_data.get('source', ''),
                    server=action_data.get('server', ''),
                    method=_action_method_value(action_data, 'method'),
                    post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                    method_text=_action_method_text_value(action_data, post=False),
                    post_execute_text=_action_method_text_value(action_data, post=True),
                    http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                    post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else ''
                )
                db.session.add(new_action)
        
        # CREATE/UPDATE THE SERVER HANDLERS FILE IF THERE ARE ANY
        if config_to_use.nodes_server_handlers:
            handlers_dir = os.path.join('Handlers', config_to_use.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            try:
                handlers_code = base64.b64decode(config_to_use.nodes_server_handlers).decode('utf-8')
                with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                    f.write(handlers_code)
                print(f"Created/updated server handlers file: {handlers_file_path}")
            except Exception as e:
                print(f"Error creating server handlers file: {str(e)}")
        
        # Updating the timestamp
        config_to_use.update_last_modified()
        
        db.session.commit()
        _materialize_profile_templates_for_config(config_to_use)
        
        if is_update:
            print(f"Configuration updated successfully: {config_to_use.name}")
            flash(_('Configuration updated successfully'), 'success')
        else:
            print(f"Configuration imported successfully: {config_to_use.name}")
            flash(_('Configuration imported successfully'), 'success')
        
        return redirect(url_for('edit_config', uid=config_to_use.uid))
        
    except Exception as e:
        db.session.rollback()
        error_msg = f'Import error: {str(e)}'
        print(error_msg)
        traceback.print_exc()
        flash(_('Import error: {error}').format(error=str(e)), 'error')
        return redirect(url_for('dashboard'))

def _cooperate_during_config_apply():
    """Yield long configuration deploy loops to the gevent WSGI server.

    The production server is a single gevent event loop. Rebuilding a full
    configuration performs many ORM object creations/flushes; without an explicit
    cooperative yield, unrelated Client HTTP requests can look completely frozen
    until deploy ends. This helper never changes transaction semantics.
    """
    try:
        import gevent
        gevent.sleep(0)
    except Exception:
        pass


def _atomic_replace_text_file(path: str, text_value: str) -> None:
    """Publish a handler file atomically so readers see old or new, never half."""
    target = os.path.abspath(path)
    parent = os.path.dirname(target)
    os.makedirs(parent, exist_ok=True)
    tmp = os.path.join(parent, f".{os.path.basename(target)}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, 'w', encoding='utf-8', newline="\n") as fh:
            fh.write(text_value)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        os.replace(tmp, target)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def apply_full_config_from_json(config, data):
    """
    Completely updates the config configuration using JSON data.
    1-to-1 logic with the current import_config.
    """
    # COMPLETE UPDATE OF ALL CONFIGURATION FIELDS
    config.name = data.get('name', config.name)
    config.vendor = data.get('vendor', data.get('provider', config.vendor))
    config.content_uid = data.get('content_uid', config.content_uid)
    config.server_name = data.get('server_name', config.server_name)
    config.version = data.get('version', config.version)
    if 'ngenie_prompt' in data:
        config.ngenie_prompt = data.get('ngenie_prompt') or ''
    if 'ngenie_code_instruction' in data and hasattr(config, 'ngenie_code_instruction'):
        config.ngenie_code_instruction = data.get('ngenie_code_instruction') or ''
    if 'ngenie_code_example' in data and hasattr(config, 'ngenie_code_example'):
        config.ngenie_code_example = data.get('ngenie_code_example') or ''
    if 'ngenie_code_locked' in data and hasattr(config, 'ngenie_code_locked'):
        config.ngenie_code_locked = _ngenie_code_bool(data.get('ngenie_code_locked'))
    if hasattr(config, 'demo_product'):
        if _current_user_has_admin_login():
            if 'demo_product' in data:
                config.demo_product = bool(data.get('demo_product'))
        else:
            # A non-admin import/generation must never publish a configuration.
            config.demo_product = False
    config.nodes_handlers = data.get('nodes_handlers', config.nodes_handlers)
    config.nodes_handlers = _rewrite_android_handlers_instance_refs_b64(
        config.nodes_handlers,
        config.uid,
        url_for('get_config', uid=config.uid, _external=True)
    )
    config.nodes_handlers_meta = data.get('nodes_handlers_meta', config.nodes_handlers_meta)
    config.nodes_server_handlers = data.get('nodes_server_handlers', config.nodes_server_handlers)
    config.nodes_server_handlers_meta = data.get('nodes_server_handlers_meta', config.nodes_server_handlers_meta)
    config.common_layouts = data.get('CommonLayouts', data.get('common_layouts', config.common_layouts)) or []
    if hasattr(config, 'profile_templates'):
        config.profile_templates = data.get('profile_templates', data.get('ProfileTemplates', getattr(config, 'profile_templates', []))) or []
    
    # We delete ALL existing related data
    print("Deleting existing data...")
    for class_obj in config.classes:
        db.session.delete(class_obj)
        _cooperate_during_config_apply()
    for dataset in config.datasets:
        db.session.delete(dataset)
        _cooperate_during_config_apply()
    for section in config.sections:
        db.session.delete(section)
        _cooperate_during_config_apply()
    for server in config.servers:
        db.session.delete(server)
        _cooperate_during_config_apply()
    for ra in (getattr(config, 'room_aliases', None) or []):
        db.session.delete(ra)
        _cooperate_during_config_apply()
    for event in config.config_events:
        db.session.delete(event)
        _cooperate_during_config_apply()
    for timer in (getattr(config, 'config_timers', None) or []):
        db.session.delete(timer)
        _cooperate_during_config_apply()
    
    # Importing classes
    classes_data = data.get('classes', [])
    print(f"Importing {len(classes_data)} classes...")
    
    for class_data in classes_data:
        new_class = ConfigClass(
                name=class_data['name'],
                section=class_data.get('section', ''),
                section_code=class_data.get('section_code', ''),
                has_storage=class_data.get('has_storage', False),
                display_name=class_data.get('display_name', class_data['name']),
                record_view=class_data.get('record_view', ''),
                cover_image=class_data.get('cover_image', ''),
                display_image_web=class_data.get('display_image_web', ''),
                display_image_table=class_data.get('display_image_table', ''),
                init_screen_layout=class_data.get('init_screen_layout', ''),
                init_screen_layout_web=class_data.get('init_screen_layout_web', ''),
                data_structure=class_data.get('data_structure', class_data.get('dataStructure', '')),
                ngenie_role=class_data.get('ngenie_role', class_data.get('ngenieRole', class_data.get('nGenieRole', ''))),
                ngenie_prompt=class_data.get('ngenie_prompt', class_data.get('ngeniePrompt', class_data.get('nGeniePrompt', ''))),
                ngenie_description=class_data.get('ngenie_description', class_data.get('ngenieDescription', class_data.get('nGenieDescription', ''))),
                show_tag_cloud=bool(class_data.get('show_tag_cloud', class_data.get('showTagCloud', False))),
                mobile_print_enabled=bool(class_data.get('mobile_print_enabled', class_data.get('mobilePrintEnabled', False))),
                dashboard_enabled=bool(class_data.get('dashboard_enabled', class_data.get('dashboardEnabled', False))),
                dashboard_width=str(class_data.get('dashboard_width', class_data.get('dashboardWidth', '100')) or '100'),
                dashboard_top=bool(class_data.get('dashboard_top', class_data.get('dashboardTop', False))),
                plug_in=class_data.get('plug_in', ''),
                plug_in_web=class_data.get('plug_in_web', ''),

                commands=class_data.get('commands', ''),
                use_standard_commands=bool(class_data.get('use_standard_commands', True)),
                svg_commands=class_data.get('svg_commands', ''),

                migration_register_command=bool(class_data.get('migration_register_command', False)),
                migration_register_on_save=bool(class_data.get('migration_register_on_save', False)),
                migration_send_via_queue=bool(class_data.get('migration_send_via_queue', class_data.get('migrationSendViaQueue', False))),
                migration_default_room_uid=class_data.get('migration_default_room_uid', ''),
                migration_default_room_alias=class_data.get('migration_default_room_alias', ''),
                link_share_mode=class_data.get('link_share_mode', ''),
                include_in_contract=bool(class_data.get('include_in_contract', class_data.get('includeInContract', False))),
                indexes_json=_normalize_imported_indexes(class_data.get('indexes', class_data.get('indexes_json', class_data.get('indexesJson', []))) or []),

                class_type=class_data.get('class_type', ''),
                projection_type=class_data.get('projection_type', ''),
                projection_kanban_columns=class_data.get('projection_kanban_columns', ''),
                print_template_type=class_data.get('print_template_type', class_data.get('printTemplateType', 'html_jinja')),
                print_target_classes=class_data.get('print_target_classes', class_data.get('printTargetClasses', [])) or [],
                print_html_template=_encode_print_html_template(class_data.get('print_html_template', class_data.get('printHtmlTemplate', ''))),
                hidden=class_data.get('hidden', False),
                hide_mobile_client=bool(class_data.get('hide_mobile_client', class_data.get('hideMobileClient', False))),
                hide_web_client=bool(class_data.get('hide_web_client', class_data.get('hideWebClient', False))),
                config_id=config.id
            )
        db.session.add(new_class)
        db.session.flush()
        _cooperate_during_config_apply()
        
        # Importing class methods
        methods_data = class_data.get('methods', [])
        print(f"  Importing {len(methods_data)} methods for class {class_data['name']}")
        
        for method_data in methods_data:
            new_method = ClassMethod(
                name=method_data['name'],
                source=method_data.get('source', 'internal'),
                engine=method_data['engine'],
                code=method_data['code'],
                class_id=new_class.id
            )
            db.session.add(new_method)
            _cooperate_during_config_apply()
        
        # Importing class events
        events_data = class_data.get('events', [])
        print(f"  Importing {len(events_data)} events for class {class_data['name']}")
        
        for event_data in events_data:
            if not isinstance(event_data, dict):
                continue
            event_name = str(event_data.get('event') or event_data.get('name') or '').strip()
            if not event_name:
                continue
            new_event = ClassEvent(
                event=event_name,
                listener=event_data.get('listener', ''),
                class_id=new_class.id
            )
            db.session.add(new_event)
            db.session.flush()
            _cooperate_during_config_apply()
            
            # Importing event actions
            actions_data = event_data.get('actions', [])
            print(f"    Importing {len(actions_data)} actions for event {event_name}")
            
            for action_data in actions_data:
                new_action = EventAction(
                    action=action_data.get('action', 'run'),
                    source=action_data.get('source', 'internal'),
                    server=action_data.get('server', ''),
                    method=_action_method_value(action_data, 'method'),
                    post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                    method_text=_action_method_text_value(action_data, post=False),
                    post_execute_text=_action_method_text_value(action_data, post=True),
                    http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                    post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else '',
                    order=action_data.get('order', 0),
                    event_id=new_event.id
                )
                db.session.add(new_action)
                _cooperate_during_config_apply()
    
    # Importing datasets
    datasets_data = data.get('datasets', [])
    print(f"Importing {len(datasets_data)} datasets...")
    
    for dataset_data in datasets_data:
        # Converting arrays back to strings for storage in the database
        hash_indexes = ','.join(dataset_data.get('hash_indexes', [])) \
            if isinstance(dataset_data.get('hash_indexes'), list) \
            else dataset_data.get('hash_indexes', '')
        text_indexes = ','.join(dataset_data.get('text_indexes', [])) \
            if isinstance(dataset_data.get('text_indexes'), list) \
            else dataset_data.get('text_indexes', '')
        
        new_dataset = Dataset(
            name=dataset_data['name'],
            hash_indexes=hash_indexes,
            text_indexes=text_indexes,
            view_template=dataset_data.get('view_template', ''),
            autoload=dataset_data.get('autoload', False),
            config_id=config.id
        )
        db.session.add(new_dataset)
        _cooperate_during_config_apply()
    
    # Importing sections
    sections_data = data.get('sections', [])
    print(f"Importing {len(sections_data)} sections...")
    
    for section_data in sections_data:
        new_section = ConfigSection(
            name=section_data['name'],
            code=section_data['code'],
            commands=section_data.get('commands', ''),
            hide_mobile_client=bool(section_data.get('hide_mobile_client', section_data.get('hideMobileClient', False))),
            hide_web_client=bool(section_data.get('hide_web_client', section_data.get('hideWebClient', False))),
            config_id=config.id
        )
        db.session.add(new_section)
        _cooperate_during_config_apply()
    
    # Importing servers
    servers_data = data.get('servers', [])
    print(f"Importing {len(servers_data)} servers...")
    
    for server_data in servers_data:
        new_server = Server(
            alias=server_data['alias'],
            url=server_data['url'],
            is_default=server_data.get('is_default', False),
            config_id=config.id
        )
        db.session.add(new_server)
        _cooperate_during_config_apply()


    # Importing room aliases (rooms)
    rooms_data = data.get('rooms', []) or []
    print(f"Importing {len(rooms_data)} room aliases...")
    for rdata in rooms_data:
        alias = (rdata.get('alias') or '').strip()
        room_uid = (rdata.get('room_id') or rdata.get('room_uid') or '').strip()
        if not alias:
            continue
        new_ra = RoomAlias(
            alias=alias,
            room_uid=room_uid,
            config_id=config.id
        )
        db.session.add(new_ra)
        _cooperate_during_config_apply()

     # Importing common events
    common_events_data = data.get('CommonEvents', [])
    print(f"Importing {len(common_events_data)} common events.")

    for ev_data in common_events_data:
        if not isinstance(ev_data, dict):
            continue
        event_name = str(ev_data.get('event') or ev_data.get('name') or '').strip()
        if not event_name:
            continue
        new_event = ConfigEvent(
            event=event_name,
            listener=ev_data.get('listener', ''),
            config_id=config.id
        )
        db.session.add(new_event)

        for action_data in ev_data.get('actions', []):
            new_action = ConfigEventAction(
                event_obj=new_event,
                action=action_data.get('action', ''),
                source=action_data.get('source', ''),
                server=action_data.get('server', ''),
                method=_action_method_value(action_data, 'method'),
                post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                method_text=_action_method_text_value(action_data, post=False),
                post_execute_text=_action_method_text_value(action_data, post=True),
                http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else ''
            )
            db.session.add(new_action)
            _cooperate_during_config_apply()

    timers_data = data.get('Timers', data.get('timers', []))
    print(f"Importing {len(timers_data)} timers.")

    for timer_data in timers_data:
        timer_id = (timer_data.get('timer_id') or timer_data.get('id') or '').strip()
        if not timer_id:
            continue
        new_timer = ConfigTimer(
            timer_id=timer_id,
            period_seconds=_period_seconds_from_timer_data(timer_data),
            active=_bool_from_timer_value(timer_data.get('active'), default=True),
            worker=_bool_from_timer_value(timer_data.get('worker'), default=False),
            runtime=_timer_runtime_from_timer_data(timer_data),
            config_id=config.id
        )
        db.session.add(new_timer)

        for action_data in timer_data.get('actions', []):
            new_action = ConfigTimerAction(
                timer_obj=new_timer,
                action=action_data.get('action', ''),
                source=action_data.get('source', ''),
                server=action_data.get('server', ''),
                method=_action_method_value(action_data, 'method'),
                post_execute_method=_action_method_value(action_data, 'postExecuteMethod'),
                method_text=_action_method_text_value(action_data, post=False),
                post_execute_text=_action_method_text_value(action_data, post=True),
                http_function_name=(action_data.get('httpFunctionName', '') or '') if _is_http_request_method(action_data.get('method', '')) else '',
                post_http_function_name=(action_data.get('postHttpFunctionName', '') or '') if _is_http_request_method(action_data.get('postExecuteMethod', '')) else ''
            )
            db.session.add(new_action)
            _cooperate_during_config_apply()
    
    # Updating the timestamp
    config.update_last_modified()
    
    # CREATE/UPDATE THE SERVER HANDLERS FILE IF THERE ARE ANY
    if config.nodes_server_handlers:
        handlers_dir = os.path.join('Handlers', config.uid)
        os.makedirs(handlers_dir, exist_ok=True)
        handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
        try:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            _atomic_replace_text_file(handlers_file_path, handlers_code)
            _cooperate_during_config_apply()
            print(f"Created/updated server handlers file: {handlers_file_path}")
        except Exception as e:
            print(f"Error creating server handlers file: {str(e)}")

@_routes.route('/import-config/<uid>', methods=['POST'])
@login_required
def import_config(uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    if _config_is_ngenie_code_locked(config):
        flash(_ngenie_code_forbid_message(), 'error')
        return redirect(url_for('edit_config', uid=uid, tab='ai-generator'))
    
    if 'config_file' not in request.files:
        flash(_('File not selected'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    file = request.files['config_file']
    if file.filename == '':
        flash(_('File not selected'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    if not file.filename.lower().endswith(('.nod', '.json')):
        flash(_('Only NOD files allowed'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    try:
        data = json.load(file.stream)
        
        print(f"Starting import for config {uid}")
        print(f"Data keys: {list(data.keys())}")
        
        
        apply_full_config_from_json(config, data)
        
        db.session.commit()
        _materialize_profile_templates_for_config(config)
        print("Import completed successfully")
        
        flash(_('Configuration imported successfully'), 'success')
        
    except Exception as e:
        db.session.rollback()
        error_msg = f'Import error: {str(e)}'
        print(error_msg)
        traceback.print_exc()
        flash(_('Import error: {error}').format(error=str(e)), 'error')
    
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=uid, tab=active_tab))

def _extract_llm_message_content(data):
    """Safe extractor for OpenAI-compatible chat/completions responses."""
    def flat(v):
        if v is None:
            return ""
        if isinstance(v, str):
            return v
        if isinstance(v, (int, float, bool)):
            return str(v)
        if isinstance(v, list):
            return "".join(flat(x) for x in v)
        if isinstance(v, dict):
            for k in ("text", "content", "output_text"):
                if k in v:
                    t = flat(v.get(k))
                    if t:
                        return t
            if "parsed" in v:
                try:
                    return json.dumps(v.get("parsed"), ensure_ascii=False)
                except Exception:
                    return str(v.get("parsed") or "")
        return ""

    try:
        if isinstance(data, dict):
            for k in ("output_text", "content", "text"):
                if k in data:
                    t = flat(data.get(k))
                    if t:
                        return t
            choices = data.get("choices")
            if isinstance(choices, list) and choices:
                choice = choices[0] if isinstance(choices[0], dict) else {}
                msg = choice.get("message") if isinstance(choice, dict) else None
                if isinstance(msg, dict):
                    for k in ("content", "text", "output_text"):
                        if k in msg:
                            t = flat(msg.get(k))
                            if t:
                                return t
                    if "parsed" in msg:
                        try:
                            return json.dumps(msg.get("parsed"), ensure_ascii=False)
                        except Exception:
                            return str(msg.get("parsed") or "")
                for k in ("text", "content", "delta"):
                    if k in choice:
                        t = flat(choice.get(k))
                        if t:
                            return t
        return ""
    except Exception:
        return ""


def _llm_response_shape(data):
    try:
        if isinstance(data, dict):
            choices = data.get("choices")
            if isinstance(choices, list) and choices:
                c0 = choices[0] if isinstance(choices[0], dict) else {}
                msg = c0.get("message") if isinstance(c0, dict) else None
                return f"top_keys={sorted(data.keys())}; choice0_keys={sorted(c0.keys())}; message_keys={sorted(msg.keys()) if isinstance(msg, dict) else []}"
            return f"top_keys={sorted(data.keys())}"
        return f"type={type(data).__name__}"
    except Exception:
        return "response_shape_unavailable"


def call_deepseek(system_prompt: str, user_prompt: str) -> str:
    data = _shared_chat_completion(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        require_json=False,
        temperature=0.2,
        max_tokens=8000,
    )
    content = _shared_message_content(data)
    if not content:
        raise RuntimeError("LLM response did not contain assistant content. " + _llm_response_shape(data))
    return content


def call_lmstudio(system_prompt: str, user_prompt: str) -> str:
    # LM Studio обычно OpenAI-compatible: /v1/chat/completions
    headers = {"Content-Type": "application/json"}
    if LMSTUDIO_API_KEY:
        headers["Authorization"] = f"Bearer {LMSTUDIO_API_KEY}"

    payload = {
        "model": LMSTUDIO_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }

    resp = requests.post(LMSTUDIO_API_URL, headers=headers, json=payload, timeout=1200)
    resp.raise_for_status()
    data = resp.json()
    content = _extract_llm_message_content(data)
    if not content:
        raise RuntimeError("LLM response did not contain assistant content. " + _llm_response_shape(data))
    return content

def _release_db_before_external_llm() -> None:
    """End any current DB transaction before waiting on an external LLM.

    SQLite permits only one writer at a time.  Solutions may enqueue audit/state
    rows before generation; leaving that transaction open while an LLM request
    runs for minutes blocks unrelated users from writing.  Persist the short DB
    work first, then perform the slow network operation outside a transaction.
    """
    try:
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        raise


def call_llm(provider: str, system_prompt: str, user_prompt: str, *, debug_stage: str = "call", debug_meta: dict = None, max_tokens: int = None) -> str:
    # Critical concurrency boundary: never hold a SQLAlchemy/SQLite write
    # transaction across a potentially very long external model request.
    _release_db_before_external_llm()
    provider = (provider or "").strip().lower()
    if provider == "ngenie_code":
        import ngenie_code
        return ngenie_code.call_llm(
            system_prompt,
            user_prompt,
            max_tokens=max_tokens,
            debug_stage=debug_stage,
            debug_meta=debug_meta or {},
        )
    if provider == "lmstudio":
        return call_lmstudio(system_prompt, user_prompt)
    # default
    return call_deepseek(system_prompt, user_prompt)

def extract_json_array_from_text(text: str) -> str:
    """Extract the largest JSON array substring from an LLM response."""
    if not text:
        raise ValueError("Empty LLM response")

    s = text.strip()

    # Strip markdown fences if present
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1:]
        end_fence = s.rfind("```")
        if end_fence != -1:
            s = s[:end_fence].strip()

    start = s.find("[")
    end = s.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON array found in LLM response")

    candidate = s[start:end + 1].strip()
    json.loads(candidate)  # validation
    return candidate

def extract_json_from_text(text: str) -> str:
    if not text:
        raise ValueError("Empty LLM response")

    s = text.strip()

    # Strip markdown fences if present
    if s.startswith("```"):
        # remove first fence line
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl+1:]
        # remove last fence
        end_fence = s.rfind("```")
        if end_fence != -1:
            s = s[:end_fence].strip()

    # Now take the largest JSON object substring
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in LLM response")

    candidate = s[start:end+1].strip()

    # Quick validation pass (raises with location)
    json.loads(candidate)
    return candidate

try:
    # WEB NodaLayout is the source of truth for Show()/static web layouts.  Keeping
    # a second independent list here previously produced false blockers such as
    # ``Show(...): unknown UI type 'Tabs'`` although Tabs is implemented by the
    # renderer and used by the editor itself/reference configurations.
    from client_app.nodalayout import NODALAYOUT_WEB_ELEMENT_TYPES
except Exception:
    # Startup/import fallback for stripped utility environments.  Keep the full
    # production list here too so validation degrades to permissive-correct rather
    # than rejecting supported UI when client_app is temporarily unavailable.
    NODALAYOUT_WEB_ELEMENT_TYPES = frozenset({
        "Text", "Picture", "HTML", "Button", "Input", "TextInput", "Switch", "CheckBox",
        "Table", "Parameters", "NodeChildren", "Spinner",
        "DatasetField", "DatasetInput", "DatasetLink", "DataSetLink", "NodeInput", "NodeLink",
        "VerticalLayout", "HorizontalLayout", "VerticalScroll", "HorizontalScroll", "Card",
        "Tabs", "Tab", "CodeFrame", "ImageSlider", "PictureGallery",
        "gauge", "pie", "bar", "line", "Gauge", "Pie", "Bar", "Line",
    })

ALLOWED_UI_TYPES_AI = set(NODALAYOUT_WEB_ELEMENT_TYPES) | {
    # Android-focused/screen plugin elements.  These are not all rendered by the
    # web NodaLayout module, but are valid NodaLogic UI/PlugIn types.
    "BottomButtons", "FloatingButton", "ToolbarButton",
    "PhotoButton", "GalleryButton", "MediaGallery",
    "CameraBarcodeScannerButton",  # camera scan button
    "BarcodeScanner",              # hardware scanner interception (TSD terminals)
    "ActiveCV",
}

CONTAINER_UI_TYPES_AI = {"VerticalLayout", "HorizontalLayout", "VerticalScroll", "HorizontalScroll", "Card"}

ALLOWED_INPUT_TYPES_AI = {"NUMBER", "number", "PASSWORD", "password", "MULTILINE", "multiline", "DATE", "date"}

def _split_commands_str(commands: str):
    # "Caption|code,Caption2|code2" -> [("Caption","code"), ...]
    items = []
    if commands is None:
        return items, ["commands is null (must be string)"]
    if not isinstance(commands, str):
        return items, [f"commands must be string (got {type(commands).__name__})"]
    s = commands.strip()
    if s == "":
        return [], []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    errors = []
    for p in parts:
        if "|" not in p:
            errors.append(f"bad command '{p}' (missing '|')")
            continue
        title, code = p.split("|", 1)
        title = title.strip()
        code = code.strip()
        if not title or not code:
            errors.append(f"bad command '{p}' (empty title or code)")
            continue
        items.append((title, code))
    return items, errors

def validate_sections_ai(cfg: dict):
    errors = []
    sections = cfg.get("sections", [])
    if sections is None:
        return ["sections is null (must be list)"]
    if not isinstance(sections, list):
        return [f"sections must be list (got {type(sections).__name__})"]
    for i, sec in enumerate(sections):
        if not isinstance(sec, dict):
            errors.append(f"sections[{i}] must be object")
            continue
        name = sec.get("name")
        code = sec.get("code")
        commands = sec.get("commands", "")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"sections[{i}].name must be non-empty string")
        if not isinstance(code, str) or not code.strip():
            errors.append(f"sections[{i}].code must be non-empty string")
        # forbidden UI-like fields (common hallucination)
        for forbidden in ("layout", "type", "value", "items"):
            if forbidden in sec:
                errors.append(f"sections[{i}] must NOT contain '{forbidden}' (sections are navigation, not UI)")
        _, cmd_err = _split_commands_str(commands)
        for e in cmd_err:
            errors.append(f"sections[{i}].commands: {e}")
    return errors

def _iter_layout_elements_ai(layout):
    # layout root is list of rows; each row list of dicts; may include container dicts with nested "value"/"layout"
    if isinstance(layout, list):
        for item in layout:
            if isinstance(item, dict):
                yield item
                t = item.get("type")
                if t in CONTAINER_UI_TYPES_AI and isinstance(item.get("value"), list):
                    yield from _iter_layout_elements_ai(item["value"])
                if t == "BottomButtons" and isinstance(item.get("value"), list):
                    yield from _iter_layout_elements_ai(item["value"])
                if t == "Tabs" and isinstance(item.get("value"), list):
                    # Tabs.value contains Tab descriptors; each Tab owns a normal
                    # row-based layout. Validate both the descriptor type and the
                    # controls nested inside its layout/legacy layput field.
                    for tab in item.get("value") or []:
                        if not isinstance(tab, dict):
                            continue
                        yield tab
                        tab_layout = tab.get("layout")
                        if tab_layout is None:
                            tab_layout = tab.get("layput")
                        if isinstance(tab_layout, list):
                            yield from _iter_layout_elements_ai(tab_layout)
                if t == "Table" and isinstance(item.get("layout"), list):
                    yield from _iter_layout_elements_ai(item["layout"])
                if t == "Table" and isinstance(item.get("virtual_node"), dict):
                    vnode = item.get("virtual_node") or {}
                    if isinstance(vnode.get("layout"), list):
                        yield from _iter_layout_elements_ai(vnode.get("layout"))
                    if isinstance(vnode.get("cover"), list):
                        yield from _iter_layout_elements_ai(vnode.get("cover"))
            else:
                yield from _iter_layout_elements_ai(item)

def validate_layout_types_ai(layout, where="layout"):
    errors = []
    for el in _iter_layout_elements_ai(layout):
        if not isinstance(el, dict):
            continue
        t = el.get("type")
        if not isinstance(t, str) or not t:
            errors.append(f"{where}: element without valid 'type'")
            continue
        if t not in ALLOWED_UI_TYPES_AI:
            errors.append(f"{where}: unknown UI type '{t}' (type is CASE-SENSITIVE)")
        # Text.size must be int
        if t == "Text" and "size" in el and not isinstance(el.get("size"), int):
            errors.append(f"{where}: Text.size must be integer (got {type(el.get('size')).__name__})")
        # Input.input_type must be one of allowed (if present)
        if t in ("Input", "TextInput") and "input_type" in el:
            it = el.get("input_type")
            if not isinstance(it, str) or it not in ALLOWED_INPUT_TYPES_AI:
                errors.append(f"{where}: Input.input_type must be one of {sorted(ALLOWED_INPUT_TYPES_AI)} (got {it!r})")
    return errors

def validate_cover_images_ai(cfg: dict):
    errors = []
    classes = cfg.get("classes", []) or []
    if not isinstance(classes, list):
        return [f"classes must be list (got {type(classes).__name__})"]
    for i, cls in enumerate(classes):
        if not isinstance(cls, dict):
            errors.append(f"classes[{i}] must be object")
            continue
        ci = cls.get("cover_image")
        if not isinstance(ci, str) or not ci.strip():
            errors.append(f"classes[{i}].cover_image must be non-empty string (JSON-in-string layout)")
            continue
        try:
            layout = json.loads(ci)
        except Exception as e:
            errors.append(f"classes[{i}].cover_image must be valid JSON string layout: {e}")
            continue
        if not isinstance(layout, list):
            errors.append(f"classes[{i}].cover_image root must be a list")
            continue
        errors.extend(validate_layout_types_ai(layout, where=f"classes[{i}].cover_image"))
    return errors

def split_handlers_by_immutable_prefix_ai(current_code: str, llm_code: str):
    """
    Preserve everything ABOVE and INCLUDING the line 'from nodes import Node' from current_code.
    Replace everything below that line by llm_code's below-marker part.
    """
    marker = "from nodes import Node"
    cur_idx = current_code.find(marker)
    llm_idx = llm_code.find(marker)
    if cur_idx == -1 or llm_idx == -1:
        # if marker not found, safest is to use llm_code as is (or keep current). Here: use llm_code.
        return llm_code

    cur_line_end = current_code.find("\n", cur_idx)
    llm_line_end = llm_code.find("\n", llm_idx)
    if cur_line_end == -1 or llm_line_end == -1:
        return llm_code

    immutable_prefix = current_code[:cur_line_end + 1]
    mutable_suffix = llm_code[llm_line_end + 1:]
    return immutable_prefix + mutable_suffix

def _decode_b64_py(b64: str):
    if not b64:
        return ""
    return base64.b64decode(b64).decode("utf-8", errors="replace")

def _encode_b64_py(code: str):
    return base64.b64encode(code.encode("utf-8")).decode("utf-8")

def validate_handlers_semantics_ai(py_code: str, where="handlers"):
    """Do not block nGenie Code on handler helper signatures.

    Older strict validation treated every Python function/helper as a NodaLogic
    event method and caused the LLM to refuse fixes with messages like
    "handler fields are forbidden in JSON patch". Syntax is still checked in
    validate_full_llm_config_ai(); method signatures are guided by instructions,
    not enforced here.
    """
    return []

class _ShowPlugInLiteralValidatorAI(ast.NodeVisitor):
    """Validate only static literals for Show([...]) and PlugIn([...]) calls."""
    def __init__(self):
        self.errors = []

    def visit_Call(self, node: ast.Call):
        func = node.func
        name = None
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "self":
            name = func.attr

        if name in ("Show", "PlugIn") and node.args:
            arg0 = node.args[0]
            if name == "PlugIn":
                # PlugIn must be 1D list of dicts
                if isinstance(arg0, ast.List):
                    for el in arg0.elts:
                        if not isinstance(el, ast.Dict):
                            self.errors.append("PlugIn(...): must be list of objects (dict)")
                        else:
                            self._validate_element_dict(el, where="PlugIn(... )")
                else:
                    # don't hard-fail non-literal; skip
                    pass
            else:
                # Show must be layout (2D list)
                self._validate_layout_literal(arg0, where="Show(... )")

        self.generic_visit(node)

    def _validate_layout_literal(self, n, where):
        if isinstance(n, ast.List):
            for el in n.elts:
                self._validate_layout_literal(el, where)
        elif isinstance(n, ast.Dict):
            self._validate_element_dict(n, where)

    def _validate_element_dict(self, dnode: ast.Dict, where):
        keys = []
        for k in dnode.keys:
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.append(k.value)
            else:
                keys.append(None)
        d = dict(zip(keys, dnode.values))

        tnode = d.get("type")
        if isinstance(tnode, ast.Constant) and isinstance(tnode.value, str):
            t = tnode.value
            if t not in ALLOWED_UI_TYPES_AI:
                self.errors.append(f"{where}: unknown UI type '{t}' (CASE-SENSITIVE)")
            if t == "Text":
                snode = d.get("size")
                # Static validator: reject only a literal value that is
                # definitely wrong. Variables/calls/expressions are runtime
                # values and are verified by the real NodaLayout renderer.
                if isinstance(snode, ast.Constant) and not isinstance(snode.value, int):
                    self.errors.append(f"{where}: Text.size literal must be integer")
            if t in ("Input", "TextInput"):
                inode = d.get("input_type")
                if isinstance(inode, ast.Constant):
                    if not (isinstance(inode.value, str) and inode.value in ALLOWED_INPUT_TYPES_AI):
                        self.errors.append(f"{where}: Input.input_type literal must be one of {sorted(ALLOWED_INPUT_TYPES_AI)} (CASE-SENSITIVE)")

            # recurse for containers / bottom buttons / table
            if t in CONTAINER_UI_TYPES_AI:
                self._validate_layout_literal(d.get("value"), where)
            if t == "BottomButtons":
                self._validate_layout_literal(d.get("value"), where)
            if t == "Tabs":
                tabs_node = d.get("value")
                if isinstance(tabs_node, ast.List):
                    for tab_node in tabs_node.elts:
                        if isinstance(tab_node, ast.Dict):
                            # For an inline literal we can prove the descriptor type.
                            # Tabs.value accepts Tab descriptors only; a literal
                            # {"type": "Text"} is therefore a real deterministic error.
                            tab_type_node = None
                            for key_node, value_node in zip(tab_node.keys, tab_node.values):
                                if (
                                    isinstance(key_node, ast.Constant)
                                    and key_node.value == "type"
                                ):
                                    tab_type_node = value_node
                                    break
                            if (
                                isinstance(tab_type_node, ast.Constant)
                                and isinstance(tab_type_node.value, str)
                                and tab_type_node.value != "Tab"
                            ):
                                self.errors.append(f"{where}: Tabs.value must contain Tab objects")
                                continue
                            self._validate_element_dict(tab_node, where)
                            continue

                        # This validator intentionally checks STATIC literals only.
                        # A local variable such as `main_tab` is ast.Name even when
                        # it was assigned a perfectly valid {"type": "Tab", ...}
                        # above.  The old code treated every such non-literal as a
                        # hard error, creating an impossible LLM repair loop.
                        #
                        # Reject only values that are statically known NOT to be a
                        # Tab object. Dynamic expressions are left to runtime/layout
                        # validation, exactly like non-literal Show/PlugIn values.
                        if isinstance(tab_node, (ast.Constant, ast.List, ast.Tuple, ast.Set)):
                            self.errors.append(f"{where}: Tabs.value must contain Tab objects")
            if t == "Table":
                self._validate_layout_literal(d.get("layout"), where)
                vnode = d.get("virtual_node")
                if isinstance(vnode, ast.Dict):
                    vnode_keys = []
                    for key in vnode.keys:
                        vnode_keys.append(key.value if isinstance(key, ast.Constant) and isinstance(key.value, str) else None)
                    vnode_dict = dict(zip(vnode_keys, vnode.values))
                    self._validate_layout_literal(vnode_dict.get("layout"), where)
                    self._validate_layout_literal(vnode_dict.get("cover"), where)

        # Tab is a descriptor, not a standalone control. Its child layout is
        # nevertheless ordinary NodaLayout and must be checked recursively.
        if isinstance(tnode, ast.Constant) and isinstance(tnode.value, str) and tnode.value == "Tab":
            self._validate_layout_literal(d.get("layout") or d.get("layput"), where)

def validate_show_plugin_literals_ai(py_code: str):
    try:
        tree = ast.parse(py_code)
    except SyntaxError:
        return []
    v = _ShowPlugInLiteralValidatorAI()
    v.visit(tree)
    return v.errors

def extract_method_names_ai(py_code: str):
    """Collect method names from all non-Node classes (android handlers)."""
    names = set()
    try:
        tree = ast.parse(py_code)
    except Exception:
        return names
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            if node.name == "Node":
                continue
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    if item.name.startswith("__") or item.name == "__init__":
                        continue
                    names.add(item.name)
    return names

def validate_sections_command_targets_ai(cfg: dict, android_method_names: set):
    """Optional cross-check: each command_code must exist in android handlers methods."""
    errors = []
    for i, sec in enumerate(cfg.get("sections", []) or []):
        if not isinstance(sec, dict):
            continue
        commands = sec.get("commands", "")
        items, cmd_errs = _split_commands_str(commands)
        # syntax errors already reported in validate_sections_ai; skip those here
        if cmd_errs:
            continue
        for _title, code in items:
            if code not in android_method_names:
                errors.append(f"sections[{i}].commands references missing android handler method '{code}'")
    return errors

def _deep_merge_dict_keep_existing(dst: dict, src: dict) -> dict:
    """
    Merge src into dst recursively:
    - if src has key -> it overwrites/merges
    - if src missing key -> keep dst
    Lists are replaced as a whole unless handled specially elsewhere.
    """
    out = dict(dst)
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict_keep_existing(out[k], v)
        else:
            out[k] = v
    return out

def _upsert_list_by_key_keep_missing(current_list, patch_list, key_fn, merge_item_fn=None):
    """
    Upsert items from patch_list into current_list by identity key_fn(item).
    Items not present in patch_list remain unchanged.
    """
    if current_list is None:
        current_list = []
    if patch_list is None:
        return list(current_list)

    if not isinstance(current_list, list):
        current_list = []
    if not isinstance(patch_list, list):
        return list(current_list)

    out = list(current_list)
    index = {}
    for i, it in enumerate(out):
        if isinstance(it, dict):
            try:
                index[key_fn(it)] = i
            except Exception:
                pass

    for pit in patch_list:
        if not isinstance(pit, dict):
            continue
        try:
            k = key_fn(pit)
        except Exception:
            continue
        if k in index:
            i = index[k]
            if merge_item_fn:
                out[i] = merge_item_fn(out[i], pit)
            else:
                out[i] = _deep_merge_dict_keep_existing(out[i], pit)
        else:
            out.append(pit)
            index[k] = len(out) - 1
    return out

def _merge_class(old_cls: dict, new_cls: dict) -> dict:
    out = _deep_merge_dict_keep_existing(old_cls, new_cls)

    # methods: upsert by name
    out["methods"] = _upsert_list_by_key_keep_missing(
        old_cls.get("methods", []) if isinstance(old_cls, dict) else [],
        new_cls.get("methods", []) if isinstance(new_cls, dict) else [],
        key_fn=lambda m: m.get("name"),
        merge_item_fn=_deep_merge_dict_keep_existing,
    )

    # events: upsert by (event, listener)
    out["events"] = _upsert_list_by_key_keep_missing(
        old_cls.get("events", []) if isinstance(old_cls, dict) else [],
        new_cls.get("events", []) if isinstance(new_cls, dict) else [],
        key_fn=lambda e: (e.get("event"), e.get("listener", "")),
        merge_item_fn=_deep_merge_dict_keep_existing,
    )
    return out

def merge_llm_config_into_current_ai(current_cfg: dict, llm_cfg: dict):
    """
    PATCH semantics (safe):
    - Upsert classes/datasets/sections/CommonEvents by identity keys.
    - Do NOT delete anything unless TT explicitly requests (we don't support delete via AI by default).
    - Merge handlers preserving immutable prefix.
    - Keep all unrelated root fields from current_cfg.
    """
    out = dict(current_cfg)

    for root_key in ("ngenie_prompt", "ngenie_code_instruction", "ngenie_code_example"):
        if root_key in llm_cfg:
            out[root_key] = llm_cfg.get(root_key) or ""

    # classes upsert by name
    if "classes" in llm_cfg:
        out["classes"] = _upsert_list_by_key_keep_missing(
            current_cfg.get("classes", []),
            llm_cfg.get("classes", []),
            key_fn=lambda c: c.get("name"),
            merge_item_fn=_merge_class,
        )

    # datasets upsert by name
    if "datasets" in llm_cfg:
        out["datasets"] = _upsert_list_by_key_keep_missing(
            current_cfg.get("datasets", []),
            llm_cfg.get("datasets", []),
            key_fn=lambda d: d.get("name"),
            merge_item_fn=_deep_merge_dict_keep_existing,
        )

    # sections upsert by code (fallback to name if code missing)
    if "sections" in llm_cfg:
        out["sections"] = _upsert_list_by_key_keep_missing(
            current_cfg.get("sections", []),
            llm_cfg.get("sections", []),
            key_fn=lambda s: s.get("code") or s.get("name"),
            merge_item_fn=_deep_merge_dict_keep_existing,
        )

    # CommonEvents upsert by (event, listener)
    if "CommonEvents" in llm_cfg:
        out["CommonEvents"] = _upsert_list_by_key_keep_missing(
            current_cfg.get("CommonEvents", []),
            llm_cfg.get("CommonEvents", []),
            key_fn=lambda e: (e.get("event"), e.get("listener", "")),
            merge_item_fn=_deep_merge_dict_keep_existing,
        )

    # Handlers: preserve current prefix up to+including "from nodes import Node"
    for field in ("nodes_handlers", "nodes_server_handlers"):
        cur_code = _decode_b64_py(current_cfg.get(field) or "")
        llm_code = _decode_b64_py(llm_cfg.get(field) or "")
        if llm_code.strip():
            merged = split_handlers_by_immutable_prefix_ai(cur_code, llm_code) if cur_code.strip() else llm_code
            out[field] = _encode_b64_py(merged)
        else:
            out[field] = current_cfg.get(field)

    return out



def _ngenie_reference_field_lines(text: str):
    """Return ordered (field_name, original_line) pairs from data_structure."""
    rows = []
    for raw in str(text or "").splitlines():
        line = raw.rstrip()
        m = re.search(r"\|\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        rows.append((m.group(1) if m else "", line))
    return rows


def _ngenie_reference_merge_data_structure(reference_text: str, candidate_text: str) -> str:
    """Reference fields are immutable baseline; project-specific fields are additive."""
    ref_rows = _ngenie_reference_field_lines(reference_text)
    cand_rows = _ngenie_reference_field_lines(candidate_text)
    ref_names = {name for name, _line in ref_rows if name}
    out = [line for _name, line in ref_rows if line.strip()]
    for name, line in cand_rows:
        if not line.strip():
            continue
        if name and name in ref_names:
            continue
        if line not in out:
            out.append(line)
    return "\n".join(out)


def _ngenie_reference_merge_named_dicts(reference_items, candidate_items, key_name="name"):
    """Keep exact reference dictionaries for existing identities and append extras."""
    ref = [deepcopy(x) for x in (reference_items or []) if isinstance(x, dict)]
    cand = [deepcopy(x) for x in (candidate_items or []) if isinstance(x, dict)]
    out = list(ref)
    known = {str(x.get(key_name) or ""): i for i, x in enumerate(out) if str(x.get(key_name) or "")}
    for item in cand:
        key = str(item.get(key_name) or "")
        if key and key in known:
            # Preserve the proven reference values, but allow truly new metadata
            # keys that the reference row did not know about.
            merged = dict(item)
            merged.update(out[known[key]])
            out[known[key]] = merged
        else:
            out.append(item)
            if key:
                known[key] = len(out) - 1
    return out


def _ngenie_reference_merge_event_actions(reference_events, candidate_events):
    def event_key(ev):
        return (str(ev.get("event") or ""), str(ev.get("listener") or ""))
    out = [deepcopy(x) for x in (reference_events or []) if isinstance(x, dict)]
    idx = {event_key(x): i for i, x in enumerate(out)}
    for ev in [x for x in (candidate_events or []) if isinstance(x, dict)]:
        key = event_key(ev)
        if key not in idx:
            out.append(deepcopy(ev)); idx[key] = len(out) - 1; continue
        base = out[idx[key]]
        merged = dict(ev)
        merged.update({k: deepcopy(v) for k, v in base.items() if k != "actions"})
        ref_actions = [deepcopy(a) for a in (base.get("actions") or []) if isinstance(a, dict)]
        cand_actions = [deepcopy(a) for a in (ev.get("actions") or []) if isinstance(a, dict)]
        seen = {json.dumps(a, ensure_ascii=False, sort_keys=True, default=str) for a in ref_actions}
        for action in cand_actions:
            sig = json.dumps(action, ensure_ascii=False, sort_keys=True, default=str)
            if sig not in seen:
                ref_actions.append(action); seen.add(sig)
        merged["actions"] = ref_actions
        out[idx[key]] = merged
    return out


def _ngenie_layout_elements_by_id(value, result=None):
    result = result if result is not None else {}
    if isinstance(value, dict):
        element_id = str(value.get("id") or "").strip()
        if element_id:
            result.setdefault(element_id, value)
        for child in value.values():
            if isinstance(child, (dict, list)):
                _ngenie_layout_elements_by_id(child, result)
    elif isinstance(value, list):
        for child in value:
            _ngenie_layout_elements_by_id(child, result)
    return result


def _ngenie_reference_restore_layout_options(reference_layout: str, candidate_layout: str) -> str:
    """Keep candidate layout additions while restoring reference option sets."""
    try:
        ref = json.loads(str(reference_layout or ""))
        cand = json.loads(str(candidate_layout or ""))
    except Exception:
        return candidate_layout or reference_layout
    ref_map = _ngenie_layout_elements_by_id(ref)
    cand_map = _ngenie_layout_elements_by_id(cand)
    for element_id, ref_el in ref_map.items():
        cur = cand_map.get(element_id)
        if not isinstance(cur, dict):
            # Missing reference controls are a validator concern; do not place a
            # nested widget at an invalid top-level position here.
            continue
        # Existing reference widget identity/type stays authoritative.
        for k in ("type", "id"):
            if k in ref_el:
                cur[k] = deepcopy(ref_el[k])
        if isinstance(ref_el.get("dataset"), list):
            ref_ds = [deepcopy(x) for x in ref_el.get("dataset") or []]
            cur_ds = [deepcopy(x) for x in (cur.get("dataset") or [])]
            if all(isinstance(x, dict) for x in ref_ds + cur_ds):
                by_id = {str(x.get("_id") or x.get("id") or ""): i for i, x in enumerate(ref_ds)}
                for x in cur_ds:
                    key = str(x.get("_id") or x.get("id") or "")
                    if key and key in by_id:
                        merged = dict(x); merged.update(ref_ds[by_id[key]]); ref_ds[by_id[key]] = merged
                    elif x not in ref_ds:
                        ref_ds.append(x)
                cur["dataset"] = ref_ds
        if isinstance(ref_el.get("table_header"), list):
            headers = list(ref_el.get("table_header") or [])
            for item in (cur.get("table_header") or []):
                if item not in headers:
                    headers.append(item)
            cur["table_header"] = headers
    return json.dumps(cand, ensure_ascii=False, separators=(",", ":"))


def _ngenie_reference_merge_display_table(reference_text: str, candidate_text: str) -> str:
    ref = [x.strip() for x in str(reference_text or "").split(",") if x.strip()]
    cand = [x.strip() for x in str(candidate_text or "").split(",") if x.strip()]
    out = list(ref)
    ref_fields = set()
    for item in ref:
        m = re.search(r"@([A-Za-z_][A-Za-z0-9_]*)", item)
        if m: ref_fields.add(m.group(1))
    for item in cand:
        m = re.search(r"@([A-Za-z_][A-Za-z0-9_]*)", item)
        if m and m.group(1) in ref_fields:
            continue
        if item not in out:
            out.append(item)
    return ",".join(out)


def _ngenie_preserve_reference_surface(reference_cfg: dict, candidate_cfg: dict) -> dict:
    """Make reference-based generation additive instead of subtractive.

    The approved answers choose defaults/seed data and project extensions.  They
    must not silently delete working reference capabilities (enum options, indexes,
    UI, events, reports) from the exact reference base.
    """
    ref_cfg = reference_cfg if isinstance(reference_cfg, dict) else {}
    out = json.loads(json.dumps(candidate_cfg if isinstance(candidate_cfg, dict) else {}, ensure_ascii=False, default=str))
    if not ref_cfg.get("classes"):
        return out

    # Root engineering prompt of the reference is authoritative.  Project facts
    # already live in Solution context and class metadata; replacing this prompt
    # previously invented STOCK_SPACE v1 while the working reference uses v3.
    if str(ref_cfg.get("ngenie_prompt") or "").strip():
        out["ngenie_prompt"] = ref_cfg.get("ngenie_prompt")

    ref_classes = {str(c.get("name") or ""): c for c in (ref_cfg.get("classes") or []) if isinstance(c, dict)}
    out_classes = {str(c.get("name") or ""): c for c in (out.get("classes") or []) if isinstance(c, dict)}
    ordered = []
    for ref_cls in ref_cfg.get("classes") or []:
        if not isinstance(ref_cls, dict):
            continue
        name = str(ref_cls.get("name") or "")
        cur = deepcopy(out_classes.get(name) or ref_cls)
        # Stable product surface/identity of existing reference classes.
        for key in (
            "name", "section", "section_code", "has_storage", "display_name",
            "record_view", "cover_image", "class_type", "projection_type",
            "hidden", "hide_mobile_client", "hide_web_client", "include_in_contract",
            "print_template_type", "print_target_classes", "print_html_template",
        ):
            if key in ref_cls:
                cur[key] = deepcopy(ref_cls.get(key))
        cur["data_structure"] = _ngenie_reference_merge_data_structure(
            ref_cls.get("data_structure"), cur.get("data_structure")
        )
        cur["indexes"] = _ngenie_reference_merge_named_dicts(ref_cls.get("indexes"), cur.get("indexes"), "name")
        cur["methods"] = _ngenie_reference_merge_named_dicts(ref_cls.get("methods"), cur.get("methods"), "name")
        cur["events"] = _ngenie_reference_merge_event_actions(ref_cls.get("events"), cur.get("events"))
        cur["display_image_table"] = _ngenie_reference_merge_display_table(
            ref_cls.get("display_image_table"), cur.get("display_image_table")
        )
        for layout_key in ("init_screen_layout", "init_screen_layout_web"):
            if ref_cls.get(layout_key) and cur.get(layout_key):
                cur[layout_key] = _ngenie_reference_restore_layout_options(ref_cls.get(layout_key), cur.get(layout_key))
            elif ref_cls.get(layout_key):
                cur[layout_key] = deepcopy(ref_cls.get(layout_key))
        ordered.append(cur)
    # Keep only project-specific additions after the complete reference surface.
    ref_names = set(ref_classes)
    ordered.extend(deepcopy(c) for c in (out.get("classes") or []) if isinstance(c, dict) and str(c.get("name") or "") not in ref_names)
    out["classes"] = ordered

    # Existing reference sections/events are also an additive baseline.
    ref_sections = [deepcopy(x) for x in (ref_cfg.get("sections") or []) if isinstance(x, dict)]
    cur_sections = [deepcopy(x) for x in (out.get("sections") or []) if isinstance(x, dict)]
    sec_keys = {str(x.get("code") or x.get("name") or "") for x in ref_sections}
    out["sections"] = ref_sections + [x for x in cur_sections if str(x.get("code") or x.get("name") or "") not in sec_keys]
    out["CommonEvents"] = _ngenie_reference_merge_event_actions(ref_cfg.get("CommonEvents"), out.get("CommonEvents"))
    return out


def validate_full_llm_config_ai(cfg: dict):
    """
    Full AI-only validation:
    - sections structure + commands format (+ cross-check to android handlers)
    - cover_image JSON-in-string layout + allowed UI types + Text.size + Input.input_type
    - handlers: python syntax + method signature + return tuple + Show/PlugIn literal checks
    """
    errors = []
    #errors.extend(validate_sections_ai(cfg))
    #errors.extend(validate_cover_images_ai(cfg))

    android_code = _decode_b64_py(cfg.get("nodes_handlers") or "")
    server_code = _decode_b64_py(cfg.get("nodes_server_handlers") or "")

    # handlers python parse
    for field, code in (("nodes_handlers", android_code), ("nodes_server_handlers", server_code)):
        # A solution may legitimately be server-only or Android-only.  Runtime
        # handler presence is required by the JSON->Python contract only when
        # metadata actually declares methods/events for that runtime.
        if not code.strip():
            continue
        try:
            ast.parse(code)
        except SyntaxError as e:
            errors.append(f"{field}: syntax error: {e}")
            continue
        errors.extend(validate_handlers_semantics_ai(code, where=field))
        errors.extend(validate_show_plugin_literals_ai(code))

    # cross-check sections commands -> android methods
    #android_methods = extract_method_names_ai(android_code) if android_code.strip() else set()
    #errors.extend(validate_sections_command_targets_ai(cfg, android_methods))

    return errors

def _decode_b64_text(b64: str) -> str:
    if not b64:
        return ""
    try:
        return base64.b64decode(b64).decode("utf-8")
    except Exception:
       
        return ""

def _encode_b64_text(text: str) -> str:
    return base64.b64encode((text or "").encode("utf-8")).decode("utf-8")

def _split_handlers_header_and_body(code: str):
    """
    Header = everything before and including the line 'from nodes import Node'
    Body = everything after this line (usually class ...).
    If the marker is not found, the header is empty, and body = all code.
    """
    if not code:
        return "", ""
    marker = "from nodes import Node"
    idx = code.find(marker)
    if idx == -1:
        return "", code

    # we take the whole line with the marker
    line_end = code.find("\n", idx)
    if line_end == -1:
        
        return code + "\n", ""

    header = code[: line_end + 1]
    body = code[line_end + 1 :]

    # We don't touch the header, but the body can be slightly normalized by leading line breaks
    body = body.lstrip("\n")
    return header, body

def _call_llm_code_only(provider: str, system_prompt: str, user_prompt: str, *, debug_stage: str = "handler_body", max_tokens: int = None) -> str:
    """
    Calls LLM and returns the text "as is", but:
    - truncates the ``` if LLM did send it
    """
    txt = call_llm(provider, system_prompt, user_prompt, debug_stage=debug_stage, max_tokens=max_tokens) or ""
    s = txt.strip()
    if s.startswith("```"):
        # снять fence
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1 :]
        end_fence = s.rfind("```")
        if end_fence != -1:
            s = s[:end_fence].strip()
    return s.strip()

def _handler_contract_requirements_ai(config_json: dict, kind_label: str):
    """Return the exact internal JSON -> Python obligations for one runtime.

    This is the single source of truth used by validation, retry prompts and
    coverage heuristics. A class is required only when metadata actually wires
    an internal method to the selected runtime.
    """
    runtime = "android_python" if str(kind_label or "").upper() == "ANDROID" else "server_python"
    android_events = {"onshow", "onresume", "oninput", "onaccept", "onafteraccept"}
    server_events = {"onshowweb", "oninputweb", "oninputserver", "onacceptserver", "onafteracceptserver"}
    runtime_events = android_events if runtime == "android_python" else server_events

    requirements = []
    for cls in (config_json or {}).get("classes") or []:
        if not isinstance(cls, dict):
            continue
        cname = str(cls.get("name") or "").strip()
        if not cname:
            continue

        declared_runtime_methods = []
        method_runtime_by_name = {}
        for method in cls.get("methods") or []:
            if not isinstance(method, dict):
                continue
            mname = str(method.get("name") or method.get("code") or "").strip()
            mengine = str(method.get("engine") or "").strip()
            msource = str(method.get("source") or "internal").strip()
            if mname:
                method_runtime_by_name[mname] = mengine
            if mengine == runtime and msource in {"", "internal"} and mname:
                declared_runtime_methods.append(mname)

        runtime_event_targets = set()
        all_event_targets = set()
        for event in cls.get("events") or []:
            if not isinstance(event, dict):
                continue
            event_name = str(event.get("event") or "").strip().lower()
            for action in event.get("actions") or []:
                if not isinstance(action, dict):
                    continue
                action_source = str(action.get("source") or "internal").strip().lower()
                if action_source not in {"", "internal"}:
                    continue
                mname = str(action.get("method") or "").strip()
                if not mname:
                    continue
                all_event_targets.add(mname)
                declared_engine = method_runtime_by_name.get(mname, "")
                if declared_engine == runtime or (not declared_engine and event_name in runtime_events):
                    runtime_event_targets.add(mname)

        required_methods = []
        for mname in declared_runtime_methods:
            if mname.startswith("_") and mname not in all_event_targets:
                continue
            if mname not in required_methods:
                required_methods.append(mname)
        for mname in sorted(runtime_event_targets):
            if mname not in required_methods:
                required_methods.append(mname)

        if required_methods:
            requirements.append((cname, required_methods))
    return requirements


def _handler_contract_checklist_ai(config_json: dict, kind_label: str) -> str:
    """Compact checklist shown to the model before it writes a full handler."""
    rows = []
    for cname, methods in _handler_contract_requirements_ai(config_json, kind_label):
        rows.append(f"- {cname}: " + ", ".join(methods))
    return "\n".join(rows)


def _handler_contract_errors_ai(config_json: dict, code: str, kind_label: str):
    """Validate only the JSON contract that belongs to this Python runtime."""
    runtime = "android_python" if str(kind_label or "").upper() == "ANDROID" else "server_python"
    try:
        tree = ast.parse(str(code or ""))
    except SyntaxError as exc:
        return [f"{kind_label}: Python syntax error: {exc}"]

    class_methods = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        class_methods[node.name] = {
            child.name
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

    errors = []
    for cname, required_methods in _handler_contract_requirements_ai(config_json, kind_label):
        implemented = class_methods.get(cname)
        if implemented is None:
            errors.append(f"class {cname}: missing from {runtime} handler file")
            continue
        for mname in required_methods:
            if mname not in implemented:
                errors.append(
                    f"class {cname}: JSON declares {runtime} method {mname}, but Python class does not implement it"
                )
    return errors


def _handler_repair_preservation_errors_ai(before_code: str, after_code: str):
    """Reject only mechanically impossible/destructive handler repair shapes.

    Deleting or substantially shrinking a class/file can be a legitimate repair
    when metadata/requirements changed, so those heuristics must never be hard
    blockers.  The only preservation invariant kept here is duplicate top-level
    class definitions: Python would silently shadow one definition and the repair
    transaction would be ambiguous.
    """
    try:
        after_tree = ast.parse(str(after_code or ""))
    except SyntaxError:
        # Syntax itself is reported by the normal Python gate.
        return []

    after_counts = {}
    for node in getattr(after_tree, "body", []):
        if isinstance(node, ast.ClassDef):
            after_counts[node.name] = int(after_counts.get(node.name, 0)) + 1
    duplicates = sorted(name for name, count in after_counts.items() if count > 1)
    if not duplicates:
        return []
    return [
        "repair produced duplicate top-level Python class definitions: "
        + ", ".join(duplicates)
        + "; edit the existing class instead of appending another copy"
    ]

def _handler_contract_target_count_ai(config_json: dict, kind_label: str) -> int:
    """Count only classes/methods that really belong to this runtime contract."""
    return sum(
        1 + len(methods)
        for _cname, methods in _handler_contract_requirements_ai(config_json, kind_label)
    )


def _handler_contract_needs_full_retry_ai(config_json: dict, contract_errors, kind_label: str) -> bool:
    """Retry whole-body generation only for a genuinely incomplete artifact.

    A few JSON/Python misses are exactly what focused SEARCH/REPLACE repair is for.
    Regenerating a 100k+ handler because one public method is absent is expensive and
    can destroy good code.  A large coverage failure, on the other hand, usually means
    the model returned a fragment/skeleton and should be regenerated as one coherent body.
    """
    errors = [str(x) for x in (contract_errors or []) if str(x or "").strip()]
    if not errors:
        return False
    targets = max(1, _handler_contract_target_count_ai(config_json, kind_label))
    # Allow a small local-repair tail: up to 8% of the declared contract, minimum 3.
    local_limit = max(3, int((targets * 0.08) + 0.9999))
    missing_classes = sum(1 for row in errors if "missing from" in row and "handler file" in row)
    # Multiple missing classes are a strong sign of a fragment/skeleton even in a
    # small solution. Otherwise use proportional contract coverage.
    if missing_classes >= max(2, int((targets * 0.10) + 0.9999)):
        return True
    return len(errors) > local_limit


def _normalize_generated_handler_body_ai(candidate_text: str, canonical_header: str):
    """Accept the canonical BODY protocol and legacy complete-file responses.

    nGenie generation contracts define handler output as the editable BODY because
    the runtime header is platform-owned and restored verbatim before persistence.
    Older prompts asked for a complete file, so remain backward compatible: when a
    `from nodes import Node...` marker is present, discard that model-supplied header;
    otherwise treat the whole response as the editable body.
    """
    candidate = str(candidate_text or "").strip()
    if not candidate:
        return "", "LLM returned an empty handler body."
    response_header, response_body = _split_handlers_header_and_body(candidate)
    body = response_body if response_header else candidate
    if not str(body or "").strip():
        return "", "LLM returned no editable handler body."
    canonical_full = str(canonical_header or "").rstrip() + "\n\n" + str(body).strip() + "\n"
    ok, err = validate_python_syntax(canonical_full)
    if not ok:
        return "", err
    return str(body).strip(), ""


def _generate_handlers_body_ai(
    provider: str,
    system_prompt: str,
    user_request: str,
    merged_config_json: dict,
    current_header: str,
    current_body: str,
    kind_label: str,   # "ANDROID" or "SERVER"
    max_attempts: int = 3,
    reference_code: str = "",
    strict_contract: bool = False,
    initial_validation_error: str = "",
):
    """Generate one coherent COMPLETE editable handler BODY.

    Retry semantics are deliberately iterative: every retry edits the immediately
    preceding model response. Older code rebuilt the prompt from the original
    pre-generation body on every attempt, so the model was not actually repairing
    its own latest result. The pipeline does not rank attempts or roll back to an
    earlier response based on contract-error counts; validation only supplies the
    factual errors for the next model turn.

    The immutable NodaLogic runtime header belongs to the platform and is never
    generated by the model.
    """
    extra_contract = ""
    if (provider or "").strip().lower() == "ngenie_code":
        try:
            import ngenie_code
            extra_contract = ngenie_code.build_generation_contract(kind_label)
        except Exception:
            extra_contract = ""

    canonical_header = str(current_header or "").rstrip() + "\n"
    original_body_text = str(current_body or "").strip()
    exact_checklist = _handler_contract_checklist_ai(merged_config_json, kind_label)
    checklist_section = (
        "\n\nEXACT JSON -> " + str(kind_label or "").upper() + " RUNTIME CONTRACT CHECKLIST.\n"
        "This list is generated by the SAME validator that will accept/reject the result. "
        "Every listed class must exist in this handler and every listed method must be implemented "
        "as a real callable method. Do not drop an already implemented checklist item while fixing another one.\n"
        + (exact_checklist or "(No internal methods are required for this runtime.)")
    )

    ref_full = str(reference_code or "").strip()
    ref_body = ""
    if ref_full:
        _ref_header, split_ref_body = _split_handlers_header_and_body(ref_full)
        ref_body = str(split_ref_body if _ref_header else ref_full).strip()

    def _full_generation_prompt(body_text: str) -> str:
        reference_section = ""
        if ref_body and ref_body != str(body_text or "").strip():
            reference_section = (
                "\n\nFULL REFERENCE EDITABLE HANDLER BODY (proven engineering baseline; NOT mandatory copy/paste):\n"
                "For functional areas required by the request, preserve the reference's proven mechanics/UX/integration while adapting them; "
                "a shorter request is not permission to replace them with a poorer duplicate. "
                "Do not copy genuinely unrelated code only to match the reference. Do not return the reference runtime header.\n"
                + ref_body
            )
        return (
            f"You are updating the COMPLETE editable NodaLogic {kind_label} Python handler BODY.\n"
            + (("\nMandatory nGenie Code generation contract:\n" + extra_contract + "\n\n") if extra_contract else "")
            + "Return ONLY the COMPLETE final editable BODY from its first generated import/helper/class to its last character. No markdown and no ``` fences.\n"
            + "DO NOT return the immutable NodaLogic runtime header. The platform owns it and will prepend its exact canonical header after your response.\n"
            + "You have the whole current editable body in context. Make all mutually dependent changes coherently across generated imports, helpers and classes.\n"
            + "Do NOT answer with patches, diffs, AST fragments, isolated classes or ellipses. Do NOT omit unchanged parts of the editable body.\n"
            + "If the output reaches the provider token limit, continue from the exact next character when the platform asks you to continue; never restart or summarize the body.\n"
            + "The reference is a proven implementation baseline, not a mandatory product clone. Approved facts/TZ/graph/standards determine which functional areas are in scope; "
              "for an in-scope area, preserve proven reference mechanics/UX/integration unless an explicit requirement conflicts, and do not add unrelated areas merely for parity.\n"
            + "Keep NodaLogic event/class methods declared in JSON class.methods callable as def MethodName(self, input_data=None) returning (bool, dict).\n"
            + "Normal Python helpers may be module-level functions with ordinary arguments.\n"
            + checklist_section
            + "\n\nUser request / approved Solution requirements:\n"
            + str(user_request or "")
            + "\n\nMerged configuration JSON (handler base64 fields omitted):\n"
            + json.dumps({k: v for k, v in (merged_config_json or {}).items() if k not in {'nodes_handlers', 'nodes_server_handlers'}}, ensure_ascii=False, indent=2)
            + "\n\nIMMUTABLE PLATFORM RUNTIME HEADER (context only; NEVER return this block):\n"
            + canonical_header
            + "\nCURRENT COMPLETE EDITABLE HANDLER BODY (return this artifact in full after editing):\n"
            + str(body_text or "").strip()
            + reference_section
        )

    def _classes_named_in_error(error_text: str):
        names = []
        for match in re.finditer(r"\bclass\s+([A-Za-z_]\w*)\s*:", str(error_text or "")):
            name = str(match.group(1) or "").strip()
            if name and name not in names:
                names.append(name)
        return names

    def _metadata_snippet(class_names):
        wanted = set(class_names or [])
        if not wanted:
            return ""
        rows = [
            cls for cls in ((merged_config_json or {}).get("classes") or [])
            if isinstance(cls, dict) and str(cls.get("name") or "").strip() in wanted
        ]
        return json.dumps(rows, ensure_ascii=False, indent=2) if rows else ""

    def _reference_class_snippet(class_names):
        wanted = set(class_names or [])
        if not wanted or not ref_body:
            return ""
        try:
            tree = ast.parse(ref_body)
            lines = ref_body.splitlines()
            chunks = []
            for node in tree.body:
                if not isinstance(node, ast.ClassDef) or node.name not in wanted:
                    continue
                start_line = max(1, int(getattr(node, "lineno", 1)))
                end_line = int(getattr(node, "end_lineno", start_line))
                chunks.append("\n".join(lines[start_line - 1:end_line]))
            return "\n\n".join(chunks)
        except Exception:
            return ""

    def _retry_prompt(body_text: str, validation_error: str) -> str:
        affected = _classes_named_in_error(validation_error)
        metadata_excerpt = _metadata_snippet(affected)
        reference_excerpt = _reference_class_snippet(affected)
        return (
            f"TECHNICAL WHOLE-BODY REPAIR for the NodaLogic {kind_label} handler.\n"
            "The body below is the IMMEDIATELY PREVIOUS model response. It is the current repair state. "
            "DO NOT regenerate from the old/original handler and do not redesign unrelated working code.\n"
            "Fix the listed validation errors while preserving every already working class/method. "
            "Return ONLY the COMPLETE corrected editable BODY, not a patch/diff/fragment and not the immutable runtime header.\n"
            "Do not satisfy the contract with TODO/placeholder/no-op stubs: use the surrounding implementation and supplied metadata/reference evidence.\n"
            + checklist_section
            + "\n\nVALIDATION ERRORS TO FIX:\n"
            + str(validation_error or "")
            + (
                "\n\nMETADATA FOR AFFECTED CLASSES:\n" + metadata_excerpt
                if metadata_excerpt else ""
            )
            + (
                "\n\nREFERENCE IMPLEMENTATION FOR AFFECTED CLASSES (when present; adapt, do not blindly clone):\n" + reference_excerpt
                if reference_excerpt else ""
            )
            + "\n\nIMMUTABLE PLATFORM RUNTIME HEADER (context only; NEVER return this block):\n"
            + canonical_header
            + "\nLATEST CURRENT COMPLETE EDITABLE HANDLER BODY:\n"
            + str(body_text or "").strip()
        )

    working_body_text = original_body_text
    last_err = None
    start_with_focused_repair = False

    # Resume may already carry a model-generated BODY.  If the saved BODY itself
    # is syntactically broken, treat that validator result as authoritative even
    # when an older Stop/Continue checkpoint did not persist handler_validation_error.
    # This lets Continue repair the saved response instead of regenerating the file.
    seed_error = str(initial_validation_error or "").strip()
    if str(kind_label or "").strip().upper() == "SERVER" and original_body_text:
        seed_body, seed_normalize_error = _normalize_generated_handler_body_ai(original_body_text, canonical_header)
        if not seed_body and seed_normalize_error and not seed_error:
            seed_error = str(seed_normalize_error)

    # A saved validation error means resume from THIS body. Syntax errors are handled
    # below by the dedicated exact SEARCH/REPLACE path; contract errors keep the
    # historical coherent whole-body retry semantics.
    if seed_error and original_body_text:
        seed_body, seed_normalize_error = _normalize_generated_handler_body_ai(original_body_text, canonical_header)
        if seed_body:
            seed_full = canonical_header.rstrip() + "\n\n" + seed_body.strip() + "\n"
            seed_contract_errors = _handler_contract_errors_ai(merged_config_json, seed_full, kind_label)
            if not seed_contract_errors:
                return seed_body.strip()
            working_body_text = seed_body.strip()
            last_err = seed_error
            start_with_focused_repair = True
        else:
            # The latest saved response may itself be syntactically broken. Keep it
            # as the current repair state rather than rolling back to an older body.
            working_body_text = original_body_text
            last_err = seed_error + ("\n" + str(seed_normalize_error) if seed_normalize_error and str(seed_normalize_error) not in seed_error else "")
            start_with_focused_repair = True

    for attempt in range(1, max_attempts + 1):
        # Syntax-only retry is deliberately NOT a complete-handler rewrite.  The
        # model receives a small verbatim window around the current SyntaxError and
        # returns one literal SEARCH/REPLACE edit.  The edit is accepted only when
        # SEARCH occurs exactly once in the complete handler; then the existing full
        # Python syntax validator is run again.
        if (
            str(kind_label or "").strip().upper() == "SERVER"
            and working_body_text
            and _handler_validation_is_syntax_error_ai(last_err)
        ):
            try:
                candidate_body = _repair_handler_syntax_exact_ai(
                    provider=provider,
                    current_header=canonical_header,
                    current_body=working_body_text,
                    kind_label=kind_label,
                    validation_error=str(last_err or ""),
                    max_attempts=3,
                )
            except Exception as syntax_exc:
                _attach_handler_resume_state_ai(
                    syntax_exc, kind_label, canonical_header, working_body_text, last_err
                )
                raise

            canonical_full = canonical_header.rstrip() + "\n\n" + candidate_body.strip() + "\n"
            contract_errors = _handler_contract_errors_ai(merged_config_json, canonical_full, kind_label)
            if not contract_errors:
                return candidate_body.strip()
            if not strict_contract and not _handler_contract_needs_full_retry_ai(merged_config_json, contract_errors, kind_label):
                return candidate_body.strip()

            working_body_text = candidate_body.strip()
            active_errors = contract_errors
            last_err = (
                ("JSON/Python generation contract is incomplete; the complete Solution handler BODY must satisfy EVERY declared runtime method in this same call.\n"
                 if strict_contract else
                 "JSON/Python generation contract is substantially incomplete; regenerate the coherent editable body.\n")
                + "\n".join(active_errors[:120])
            )
            start_with_focused_repair = True
            continue

        prompt = (
            _retry_prompt(working_body_text, last_err or "Previous body did not validate.")
            if (attempt > 1 or start_with_focused_repair)
            else _full_generation_prompt(working_body_text)
        )

        try:
            candidate_text = _call_llm_code_only(
                provider,
                system_prompt,
                prompt,
                debug_stage=f"{kind_label.lower()}_handlers_attempt_{attempt}",
            )
        except Exception as call_exc:
            # Server Stop / budget / provider failure must preserve the latest
            # complete repair state so Continue can enter exact syntax repair
            # instead of starting the 100k+ handler over again.
            if str(kind_label or "").strip().upper() == "SERVER":
                _attach_handler_resume_state_ai(
                    call_exc, kind_label, canonical_header, working_body_text, last_err
                )
            raise

        # Preserve the model's immediately previous response even when Python syntax
        # is broken. The next attempt must see and repair THIS response instead of
        # restarting from the pre-generation artifact.
        response_header, response_body = _split_handlers_header_and_body(str(candidate_text or "").strip())
        raw_candidate_body = str(response_body if response_header else candidate_text or "").strip()

        candidate_body, normalize_error = _normalize_generated_handler_body_ai(candidate_text, canonical_header)
        if not candidate_body:
            last_err = normalize_error or "LLM returned an invalid editable handler body."
            if raw_candidate_body:
                working_body_text = raw_candidate_body
            # Next iteration uses exact SEARCH/REPLACE when this is a SyntaxError.
            continue

        canonical_full = canonical_header.rstrip() + "\n\n" + candidate_body.strip() + "\n"
        contract_errors = _handler_contract_errors_ai(merged_config_json, canonical_full, kind_label)
        if not contract_errors:
            return candidate_body.strip()

        if not strict_contract and not _handler_contract_needs_full_retry_ai(merged_config_json, contract_errors, kind_label):
            return candidate_body.strip()

        # Retry from the immediately previous model response. The pipeline does not
        # compare attempts or decide that an older response was semantically better.
        working_body_text = candidate_body.strip()
        active_errors = contract_errors

        last_err = (
            ("JSON/Python generation contract is incomplete; the complete Solution handler BODY must satisfy EVERY declared runtime method in this same call.\n"
             if strict_contract else
             "JSON/Python generation contract is substantially incomplete; regenerate the coherent editable body.\n")
            + "\n".join(active_errors[:120])
        )

    exc = RuntimeError(
        f"Failed to generate valid complete {kind_label} handler body after {max_attempts} attempts: {last_err}"
    )
    # Private resume-state retains the latest model response. It is NEVER published
    # until the full JSON+Android+Server package passes the gate.
    setattr(exc, "ngenie_handler_kind", str(kind_label or "").upper())
    setattr(exc, "ngenie_handler_body", str(working_body_text or "").strip())
    setattr(exc, "ngenie_handler_header", canonical_header)
    setattr(exc, "ngenie_handler_error", str(last_err or ""))
    raise exc


def _exact_text_match_positions(text: str, needle: str, limit: int = 64):
    positions = []
    start = 0
    while len(positions) < limit:
        pos = text.find(needle, start)
        if pos < 0:
            break
        positions.append(pos)
        start = pos + max(1, len(needle))
    return positions


def _normalize_escaped_control_text_for_exact_patch(value: str) -> str:
    """Decode only transport-style escaped control characters for exact patches.

    Some providers occasionally return JSON strings where structural newlines are
    double escaped (literal ``\\n`` after json.loads).  We never use fuzzy matching:
    this normalization is considered only after the original SEARCH has zero
    matches, and only a uniquely matching normalized SEARCH may be applied.
    """
    text = str(value or "")
    return (
        text.replace("\\r\\n", "\r\n")
            .replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace("\\r", "\r")
    )


def _exact_text_match_error(source_text: str, edit_index: int, search: str, positions) -> str:
    """Human/LLM-readable rejection reason without applying fuzzy edits."""
    count = len(positions)
    if count == 0:
        preview = search[:700]
        return (
            f"handler repair edit #{edit_index}: SEARCH must match exactly once in the ORIGINAL file, got 0. "
            "The SEARCH text below is not present verbatim. Copy a fresh unique anchor from CURRENT COMPLETE HANDLER FILE.\n"
            f"REJECTED SEARCH:\n{preview}"
        )
    contexts = []
    radius = 320
    for hit_no, pos in enumerate(positions[:4], start=1):
        left = max(0, pos - radius)
        right = min(len(source_text), pos + len(search) + radius)
        contexts.append(f"MATCH {hit_no} CONTEXT:\n{source_text[left:right]}")
    return (
        f"handler repair edit #{edit_index}: SEARCH must match exactly once in the ORIGINAL file, got {count}. "
        "Do not ask the platform to choose an occurrence. Extend SEARCH with surrounding unique context, or merge overlapping fixes into one edit.\n"
        + "\n\n".join(contexts)
    )


def _apply_exact_text_edits_ai(source_text: str, patch_obj: dict) -> str:
    """Apply exact SEARCH/REPLACE edits atomically against one original file.

    No AST and no fuzzy matching are used. Every SEARCH is resolved against the
    SAME original source before any replacement is made. Search ranges must be
    non-overlapping; related changes must be combined into one edit.
    """
    if not isinstance(patch_obj, dict):
        raise ValueError("handler repair patch must be a JSON object")
    edits = patch_obj.get("edits")
    if not isinstance(edits, list) or not edits:
        raise ValueError("handler repair patch must contain non-empty edits[]")
    if len(edits) > 60:
        raise ValueError("handler repair patch contains too many edits (>60)")

    source = str(source_text or "")
    resolved = []
    for index, edit in enumerate(edits, start=1):
        if not isinstance(edit, dict):
            raise ValueError(f"handler repair edit #{index} must be an object")
        search = edit.get("search")
        replace = edit.get("replace")
        if not isinstance(search, str) or not search:
            raise ValueError(f"handler repair edit #{index}: search must be non-empty text")
        if not isinstance(replace, str):
            raise ValueError(f"handler repair edit #{index}: replace must be text")
        positions = _exact_text_match_positions(source, search)
        effective_search = search
        effective_replace = replace
        if not positions:
            normalized_search = _normalize_escaped_control_text_for_exact_patch(search)
            if normalized_search != search:
                normalized_positions = _exact_text_match_positions(source, normalized_search)
                if len(normalized_positions) == 1:
                    # Transport escaping, not fuzzy matching: the normalized anchor
                    # is still required to exist verbatim and uniquely in source.
                    effective_search = normalized_search
                    effective_replace = _normalize_escaped_control_text_for_exact_patch(replace)
                    positions = normalized_positions
        if len(positions) != 1:
            raise ValueError(_exact_text_match_error(source, index, effective_search, positions))
        pos = positions[0]
        resolved.append((pos, pos + len(effective_search), index, effective_search, effective_replace))

    ordered = sorted(resolved, key=lambda row: row[0])
    for prev, cur in zip(ordered, ordered[1:]):
        if cur[0] < prev[1]:
            raise ValueError(
                f"handler repair edits #{prev[2]} and #{cur[2]} overlap in the ORIGINAL file. "
                "Combine them into one SEARCH/REPLACE edit with a unique anchor."
            )

    result = source
    for pos, end, _index, _search, replace in sorted(resolved, key=lambda row: row[0], reverse=True):
        result = result[:pos] + replace + result[end:]
    return result



def _handler_validation_is_syntax_error_ai(error_text: str) -> bool:
    """Return True only for the existing validate_python_syntax() SyntaxError shape."""
    return bool(re.search(r"(?:^|\n)\s*Syntax error\s+\d+\s*:", str(error_text or ""), flags=re.I))


def _attach_handler_resume_state_ai(exc, kind_label: str, canonical_header: str, body_text: str, validation_error: str):
    """Attach the latest private handler state to an exception without overwriting newer state.

    solutions/generator.py already persists these attributes into the private
    atomic transaction.  The helper is intentionally tiny so Stop/Continue can
    resume the exact last body even when cancellation happens inside an LLM call.
    """
    try:
        if not str(getattr(exc, "ngenie_handler_kind", "") or "").strip():
            setattr(exc, "ngenie_handler_kind", str(kind_label or "").upper())
        if not str(getattr(exc, "ngenie_handler_body", "") or "").strip():
            setattr(exc, "ngenie_handler_body", str(body_text or "").strip())
        if not str(getattr(exc, "ngenie_handler_header", "") or "").strip():
            setattr(exc, "ngenie_handler_header", str(canonical_header or ""))
        if not str(getattr(exc, "ngenie_handler_error", "") or "").strip():
            setattr(exc, "ngenie_handler_error", str(validation_error or ""))
    except Exception:
        pass
    return exc


def _syntax_repair_window_ai(full_source: str, validation_error: str, radius_lines: int = 36):
    """Return a verbatim source window around the current SyntaxError line.

    The window is context only. SEARCH uniqueness is always checked against the
    complete handler file, never against this window.
    """
    source = str(full_source or "")
    lines = source.splitlines(keepends=True)
    if not lines:
        return source, 1, 1, 1
    match = re.search(r"Syntax error\s+(\d+)\s*:", str(validation_error or ""), flags=re.I)
    line_no = int(match.group(1)) if match else 1
    line_no = max(1, min(line_no, len(lines)))
    radius = max(8, int(radius_lines or 36))
    start = max(0, line_no - 1 - radius)
    end = min(len(lines), line_no + radius)
    return "".join(lines[start:end]), start + 1, end, line_no


def _syntax_search_collision_feedback_ai(full_source: str, search: str, error_line: int, max_contexts: int = 12) -> str:
    """Describe every relevant exact SEARCH collision without choosing one.

    The model does not need the complete handler merely to *guess* uniqueness.
    The pipeline owns the complete text, checks global literal uniqueness itself,
    and on collision returns verbatim contexts (target-nearest first) so the next
    SEARCH can be extended safely.  No regex/fuzzy matching or occurrence choice
    is performed here.
    """
    source = str(full_source or "")
    needle = str(search or "")
    if not needle:
        return "SEARCH is empty."

    total = source.count(needle)
    if total <= 1:
        return ""

    positions = []
    start = 0
    # Enough positions to show useful collision evidence while keeping the retry
    # prompt compact.  The authoritative total above is still over the whole file.
    while len(positions) < max(1, int(max_contexts or 12)):
        pos = source.find(needle, start)
        if pos < 0:
            break
        positions.append(pos)
        start = pos + max(1, len(needle))

    def _line_no(pos: int) -> int:
        return source.count("\n", 0, max(0, pos)) + 1

    target_line = max(1, int(error_line or 1))
    ranked = sorted(positions, key=lambda pos: (abs(_line_no(pos) - target_line), _line_no(pos)))
    contexts = []
    radius = 520
    for shown_no, pos in enumerate(ranked, start=1):
        line_no = _line_no(pos)
        left = max(0, pos - radius)
        right = min(len(source), pos + len(needle) + radius)
        marker = "TARGET-NEAREST" if shown_no == 1 else "OTHER MATCH"
        contexts.append(
            f"{marker} #{shown_no}, complete-file line ~{line_no}:\n"
            + source[left:right]
        )

    hidden = max(0, total - len(ranked))
    tail = f"\n\n{hidden} additional match(es) are not shown." if hidden else ""
    return (
        f"SEARCH is NOT unique in the COMPLETE handler: exact literal count = {total}.\n"
        f"The current SyntaxError is reported near complete-file line {target_line}.\n"
        "The platform did NOT apply any replacement and will NOT choose an occurrence. "
        "Use the target-nearest context plus surrounding text to return a longer SEARCH "
        "that still contains the intended syntax-error location and is globally unique.\n\n"
        + "\n\n--- COLLISION CONTEXT ---\n\n".join(contexts)
        + tail
    )


def _repair_handler_syntax_exact_ai(
    provider: str,
    current_header: str,
    current_body: str,
    kind_label: str,
    validation_error: str,
    max_attempts: int = 3,
):
    """Repair only Python syntax with one exact, unique SEARCH/REPLACE per turn.

    This path is deliberately narrower than semantic/runtime handler repair:
    - no regex, AST merge, fuzzy matching or automatic occurrence choice;
    - the model first sees a verbatim window around the current SyntaxError;
    - exactly one edit is accepted;
    - SEARCH must occur exactly once in the COMPLETE current handler;
    - on collision the pipeline returns verbatim contexts for the competing matches
      and expands the target window; the model never has to guess global uniqueness;
    - SEARCH must itself be copied from the supplied target repair window;
    - the immutable runtime header cannot change;
    - the existing full-file validate_python_syntax() decides whether to stop or
      expose the next syntax error.
    """
    canonical_header = str(current_header or "").rstrip() + "\n"
    current_body_text = str(current_body or "").strip()
    current_full = canonical_header.rstrip() + ("\n\n" if current_body_text else "\n") + current_body_text + "\n"

    ok, actual_error = validate_python_syntax(current_full)
    if ok:
        return current_body_text
    current_error = str(actual_error or validation_error or "Python syntax validation failed")
    if not _handler_validation_is_syntax_error_ai(current_error):
        raise ValueError("exact syntax repair requires a SyntaxError from validate_python_syntax")

    system_prompt = (
        "You are repairing ONLY a local Python syntax error in an existing NodaLogic "
        + str(kind_label or "").upper()
        + " handler. Do not redesign, refactor, complete contracts, or change business logic. "
          "Return one JSON object only: {\"edits\":[{\"search\":\"...\",\"replace\":\"...\"}]}. "
          "Return EXACTLY ONE edit. SEARCH is literal text, never regex. Copy SEARCH verbatim from "
          "the supplied CURRENT SYNTAX REPAIR WINDOW. You are NOT expected to know global uniqueness from that window: "
          "the platform checks the COMPLETE handler itself. If SEARCH collides, no replacement is applied; the next retry will show "
          "verbatim contexts of the competing matches and a larger target window. The platform will never choose an occurrence for you. "
          "REPLACE must be the smallest correction of that exact block. Never use ellipses, line numbers, markdown, comments outside JSON, "
          "or a complete rewritten handler. Preserve all unrelated text byte-for-byte."
    )

    last_rejection = ""
    collision_retry = False
    for attempt in range(1, max(1, int(max_attempts or 1)) + 1):
        # The first turn is intentionally small. If the previous SEARCH collided
        # globally, widen only the target context; collision evidence itself is
        # also returned below. This avoids forcing the model to reread the whole
        # handler merely to discover uniqueness.
        radius_lines = 36 if not collision_retry else min(180, 36 + 72 * max(1, attempt - 1))
        window, start_line, end_line, error_line = _syntax_repair_window_ai(
            current_full, current_error, radius_lines=radius_lines
        )
        collision_retry = False
        prompt = (
            "Fix this Python syntax error with one exact literal SEARCH/REPLACE edit.\n"
            "CURRENT VALIDATOR ERROR:\n" + current_error + "\n\n"
            f"The following is a VERBATIM excerpt of COMPLETE handler lines {start_line}-{end_line}; "
            f"the parser reports the error at complete-file line {error_line}.\n"
            "SEARCH must be copied from this excerpt. Global uniqueness is checked by the platform against the COMPLETE handler file. "
            "If a previous SEARCH collided, use the returned collision contexts only to see why, then extend SEARCH with text from this target window.\n\n"
            "CURRENT SYNTAX REPAIR WINDOW:\n" + window
        )
        if last_rejection:
            prompt += (
                "\n\nPREVIOUS PATCH WAS REJECTED / DID NOT FINISH SYNTAX REPAIR:\n"
                + last_rejection
                + "\nReturn a corrected single exact edit against the CURRENT window above."
            )

        try:
            completion_text = call_llm(
                provider,
                system_prompt,
                prompt,
                debug_stage=f"{str(kind_label or '').lower()}_handler_syntax_search_replace_{attempt}",
                max_tokens=4096,
            )
        except Exception as call_exc:
            _candidate_header, candidate_body = _split_handlers_header_and_body(current_full)
            _attach_handler_resume_state_ai(
                call_exc, kind_label, canonical_header, candidate_body or current_body_text, current_error
            )
            raise

        try:
            patch_obj = json.loads(extract_json_from_text(completion_text))
            edits = patch_obj.get("edits") if isinstance(patch_obj, dict) else None
            if not isinstance(edits, list) or len(edits) != 1:
                raise ValueError("syntax repair must return exactly one edits[] item")
            edit = edits[0]
            if not isinstance(edit, dict):
                raise ValueError("syntax repair edit must be an object")
            search = edit.get("search")
            replace = edit.get("replace")
            if not isinstance(search, str) or not search:
                raise ValueError("syntax repair SEARCH must be non-empty literal text")
            if not isinstance(replace, str):
                raise ValueError("syntax repair REPLACE must be literal text")
            if search == replace:
                raise ValueError("syntax repair SEARCH and REPLACE are identical")
            # A one-character token can technically be unique, but it is a poor
            # safety anchor in a large generated handler. Require real context.
            if len(search) < 16:
                raise ValueError("syntax repair SEARCH is too short; include more unique surrounding context")
            if search not in window:
                raise ValueError(
                    "syntax repair SEARCH must be copied verbatim from CURRENT SYNTAX REPAIR WINDOW; "
                    "do not target unseen code"
                )

            # Global uniqueness is the pipeline's responsibility, not the model's.
            # Check the COMPLETE handler before applying anything. On collision,
            # return verbatim competing contexts (target-nearest first) and retry
            # with a larger target window. No occurrence is ever auto-selected.
            global_count = current_full.count(search)
            if global_count > 1:
                last_rejection = _syntax_search_collision_feedback_ai(
                    current_full, search, error_line, max_contexts=12
                )
                collision_retry = True
                continue

            # _apply_exact_text_edits_ai still authoritatively enforces count == 1
            # (including the existing transport-escape normalization). It performs
            # plain literal replacement only; no regex/fuzzy matching.
            patched_full = _apply_exact_text_edits_ai(current_full, {"edits": [edit]})
            patched_header, patched_body = _split_handlers_header_and_body(patched_full)
            if str(patched_header or "").rstrip() != canonical_header.rstrip():
                raise ValueError("syntax repair attempted to change the immutable runtime header")
            if not str(patched_body or "").strip():
                raise ValueError("syntax repair produced an empty handler body")

            syntax_ok, next_error = validate_python_syntax(patched_full)
            if syntax_ok:
                return str(patched_body).strip()

            next_error = str(next_error or "Python syntax validation failed")
            if next_error == current_error:
                # Do not advance to a changed file when the exact same parser error
                # remains; retry from the authoritative current text with a better edit.
                last_rejection = (
                    "The edit matched uniquely and was applied in a temporary copy, but the exact same "
                    "SyntaxError remained. The edit was NOT accepted. Fix the reported syntax location itself."
                )
                continue

            # The local edit made observable parser progress. Keep it privately and
            # expose only the next SyntaxError to another single exact repair turn.
            current_full = patched_full
            current_error = next_error
            current_body_text = str(patched_body).strip()
            last_rejection = (
                "The previous unique edit was applied and changed the parser result. "
                "The handler is still syntactically invalid; fix ONLY the new validator error shown above."
            )
        except Exception as patch_exc:
            last_rejection = str(patch_exc)
            continue

    exc = RuntimeError(
        f"Failed exact SEARCH/REPLACE syntax repair for {str(kind_label or '').upper()} handler after "
        f"{max(1, int(max_attempts or 1))} attempt(s): {current_error}; {last_rejection}"
    )
    _candidate_header, candidate_body = _split_handlers_header_and_body(current_full)
    _attach_handler_resume_state_ai(
        exc, kind_label, canonical_header, candidate_body or current_body_text, current_error
    )
    raise exc


def _compact_handler_repair_system_prompt(kind_label: str) -> str:
    """Small repair-only prompt instead of the full ~150k-char instruction bundle."""
    contract = ""
    try:
        import ngenie_code
        contract = ngenie_code.build_generation_contract(kind_label)
    except Exception:
        contract = ""
    return (
        "You are nGenie Code repairing an existing COMPLETE NodaLogic "
        + str(kind_label or "").upper()
        + " Python handler file. The current complete file is authoritative. "
          "Preserve all unrelated working behavior. Make only the changes required "
          "by the supplied validation errors. Do not redesign the solution.\n\n"
        + (("Critical NodaLogic contract:\n" + contract + "\n\n") if contract else "")
        + "OUTPUT CONTRACT: return one JSON object only, with key edits. "
          "edits is a list of {search, replace}. SEARCH must be copied EXACTLY from "
          "the current complete file and must be a smallest practical UNIQUE block. "
          "REPLACE is the complete replacement text for that exact block. "
          "All SEARCH blocks are resolved against the SAME ORIGINAL current file before any edit is applied. "
          "Therefore every SEARCH must be unique in that original file and SEARCH ranges must not overlap. "
          "If two changes touch the same block, combine them into ONE edit. For an insertion, "
          "include a short unique anchor in SEARCH and repeat that anchor plus the "
          "new text in REPLACE. Never use ellipses, line numbers, unified diff, AST "
          "fragments, markdown fences or commentary."
    )


def _scoped_handler_repair_source_ai(current_full: str, issue_text: str, max_chars: int = 120000):
    """Return a verbatim, dependency-aware subset for focused handler repair.

    SEARCH/REPLACE patches are still applied to the complete original file.  The
    subset exists only to reduce prompt tokens.  If the affected class cannot be
    identified safely, callers receive the full file.
    """
    source = str(current_full or "")
    issue = str(issue_text or "")
    if not source.strip() or not issue.strip():
        return source, [], True
    try:
        tree = ast.parse(source)
    except Exception:
        return source, [], True

    lines = source.splitlines(keepends=True)
    top_nodes = [
        node for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and getattr(node, "lineno", None) and getattr(node, "end_lineno", None)
    ]
    classes = {node.name: node for node in top_nodes if isinstance(node, ast.ClassDef)}
    functions = {node.name: node for node in top_nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    lowered = issue.lower()
    selected_class_names = [
        name for name in classes
        if re.search(r"(?<![A-Za-z0-9_])" + re.escape(name.lower()) + r"(?![A-Za-z0-9_])", lowered)
    ]
    selected_function_names = [
        name for name in functions
        if re.search(r"(?<![A-Za-z0-9_])" + re.escape(name.lower()) + r"(?![A-Za-z0-9_])", lowered)
    ]
    selected_names = selected_class_names + selected_function_names
    if not selected_names:
        return source, [], True

    selected_nodes = [classes[name] for name in selected_class_names] + [functions[name] for name in selected_function_names]
    helper_names = set(selected_function_names)
    frontier = list(selected_nodes)
    # Include direct and second-order module helper dependencies.  This usually
    # captures validation helpers without dragging every unrelated business class.
    for _depth in range(3):
        wanted = set()
        for node in frontier:
            for child in ast.walk(node):
                if isinstance(child, ast.Name) and child.id in functions:
                    wanted.add(child.id)
        wanted -= helper_names
        if not wanted:
            break
        helper_names.update(wanted)
        frontier = [functions[name] for name in wanted]

    chosen = selected_nodes + [
        functions[name] for name in functions
        if name in helper_names and name not in selected_function_names
    ]
    chosen.sort(key=lambda node: int(node.lineno))
    first_decl_line = min((int(node.lineno) for node in top_nodes), default=1)
    pieces = ["".join(lines[:max(0, first_decl_line - 1)]).rstrip()]
    for node in chosen:
        pieces.append("".join(lines[int(node.lineno) - 1:int(node.end_lineno)]).rstrip())
    scoped = "\n\n".join(piece for piece in pieces if piece).strip() + "\n"
    if not scoped.strip() or len(scoped) >= len(source) * 0.92 or len(scoped) > max_chars:
        return source, selected_names, True
    return scoped, selected_names, False



def _handler_repair_metadata_context_ai(config_json: dict, kind_label: str, issue_text: str) -> dict:
    """Return compact model-facing metadata for one complete handler repair.

    The complete Python BODY remains authoritative and is still returned in full.
    Sending the complete configuration metadata as well made a one-line Server fix
    carry another 100k+ characters of forms/layouts/access-policy context.  The
    handler needs two things instead: the exact JSON->runtime contract for every
    runtime class, and richer metadata only for classes actually mentioned by the
    current issue.  Acceptance/validation still runs against the complete candidate.
    """
    runtime = "android_python" if str(kind_label or "").strip().upper() == "ANDROID" else "server_python"
    config_json = config_json if isinstance(config_json, dict) else {}
    issue = str(issue_text or "")
    # ``user_request`` also contains the broad approved generation goal. For
    # metadata scoping, match against the concrete obligations when present;
    # otherwise every WMS class scores merely because the overall goal says WMS.
    focus = issue
    for marker in (
        "CURRENT Server obligations for this atomic repair transaction:",
        "CURRENT Android obligations for this atomic repair transaction:",
    ):
        pos = issue.find(marker)
        if pos >= 0:
            focus = issue[pos + len(marker):]
            break
    issue_low = focus.lower()
    issue_tokens = {
        token.lower() for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]{3,}", focus)
        if token.lower() not in {
            "current", "complete", "handler", "repair", "server", "android",
            "python", "runtime", "metadata", "candidate", "issue", "issues",
            "solution", "fix", "preserve", "approved", "generation", "goal",
        }
    }

    contract_rows = []
    scored_rows = []
    for row in (config_json.get("classes") or []):
        if not isinstance(row, dict):
            continue
        cname = str(row.get("name") or "").strip()
        methods = []
        relevant_method_names = []
        for method in (row.get("methods") or []):
            if not isinstance(method, dict):
                continue
            source = str(method.get("source") or "internal").strip().lower()
            engine = str(method.get("engine") or "").strip().lower()
            if source == "internal" and engine == runtime:
                mname = str(method.get("name") or method.get("code") or "").strip()
                if mname:
                    methods.append({
                        "name": mname,
                        "code": str(method.get("code") or mname),
                    })
                    relevant_method_names.append(mname)

        runtime_events = []
        for event in (row.get("events") or []):
            if not isinstance(event, dict):
                continue
            event_name = str(event.get("event") or event.get("name") or "").strip()
            event_low = event_name.lower()
            belongs = (
                (runtime == "server_python" and event_low in {"onshowweb", "oninputweb", "oninputserver", "onacceptserver", "onafteracceptserver"})
                or (runtime == "android_python" and event_low in {"onshow", "oninput", "onaccept", "onafteraccept", "onresume"})
            )
            if belongs:
                runtime_events.append(event)

        if methods or runtime_events:
            contract_rows.append({
                "name": cname,
                "has_storage": row.get("has_storage"),
                "class_type": row.get("class_type"),
                "methods": methods,
                "events": runtime_events,
            })

        haystack = json.dumps(row, ensure_ascii=False, default=str).lower()
        score = 0
        if cname and cname.lower() in issue_low:
            score += 20
        for mname in relevant_method_names:
            if mname.lower() in issue_low:
                score += 12
        for token in issue_tokens:
            if token in haystack:
                score += 1
        if score > 0:
            compact_row = {
                key: deepcopy(row.get(key))
                for key in (
                    "name", "display_name", "has_storage", "class_type", "data_structure",
                    "ngenie_role", "ngenie_prompt", "ngenie_description", "methods", "events", "indexes",
                )
                if key in row
            }
            scored_rows.append((score, cname, compact_row))

    scored_rows.sort(key=lambda item: (-item[0], item[1]))
    relevant_rows = [row for _score, _name, row in scored_rows[:4]]
    return {
        "runtime": runtime,
        "ngenie_prompt": str(config_json.get("ngenie_prompt") or "")[:12000],
        "runtime_contract": contract_rows,
        "issue_relevant_classes": relevant_rows,
    }


def _repair_handlers_body_full_ai(
    provider: str,
    user_request: str,
    merged_config_json: dict,
    current_header: str,
    current_body: str,
    kind_label: str,
    max_attempts: int = 2,
    require_clean_contract: bool = False,
):
    """Repair one Solutions handler by replacing its COMPLETE editable BODY.

    This path deliberately does not use SEARCH/REPLACE.  The model receives the
    complete current handler body and must return the complete final body.  The
    platform validates that complete artifact and commits it with one assignment.
    Therefore a correct method cannot be lost because an unrelated textual edit
    failed to match elsewhere in the same file.
    """
    canonical_header = str(current_header or "").rstrip() + "\n"
    current_body_text = str(current_body or "").strip()
    current_full = canonical_header.rstrip() + "\n\n" + current_body_text + "\n"

    extra_contract = ""
    if (provider or "").strip().lower() == "ngenie_code":
        try:
            import ngenie_code
            extra_contract = ngenie_code.build_generation_contract(kind_label)
        except Exception:
            extra_contract = ""

    system_prompt = (
        "You are nGenie Code repairing an existing COMPLETE NodaLogic "
        + str(kind_label or "").upper()
        + " Python handler BODY. The current body is authoritative. Preserve all "
          "unrelated working behavior and make every mutually dependent change needed "
          "for the supplied current issues.\n\n"
        + (("Critical NodaLogic contract:\n" + extra_contract + "\n\n") if extra_contract else "")
        + "OUTPUT CONTRACT: return ONLY the COMPLETE FINAL EDITABLE HANDLER BODY, "
          "from its first generated import/helper/class through its last character. "
          "Do not return the immutable platform runtime header. Do not return JSON, "
          "SEARCH/REPLACE, diff, isolated classes, ellipses, markdown fences or commentary. "
          "Unchanged code must remain present in the returned body."
    )
    metadata_json = json.dumps(
        _handler_repair_metadata_context_ai(merged_config_json or {}, kind_label, user_request),
        ensure_ascii=False,
        indent=2,
    )
    exact_checklist = _handler_contract_checklist_ai(merged_config_json or {}, kind_label)
    last_err = ""
    for attempt in range(1, max_attempts + 1):
        prompt = (
            "Repair the CURRENT COMPLETE handler BODY as one coherent artifact.\n\n"
            + str(user_request or "")
            + "\n\nEXACT JSON -> " + str(kind_label or "").upper() + " RUNTIME CONTRACT CHECKLIST "
              "(generated by the same validator that accepts/rejects the result):\n"
            + (exact_checklist or "(No internal methods are required for this runtime.)")
            + "\n\nCOMPACT RELEVANT CONFIGURATION METADATA:\n"
            + metadata_json
            + "\n\nIMMUTABLE PLATFORM RUNTIME HEADER (context only; DO NOT return):\n"
            + canonical_header
            + "\nCURRENT COMPLETE EDITABLE HANDLER BODY (return this artifact in full after editing):\n"
            + current_body_text
        )
        if last_err:
            prompt += (
                "\n\nThe previous complete-body repair was rejected by the platform:\n"
                + last_err
                + "\nReturn the COMPLETE corrected editable BODY again."
            )

        candidate_text = _call_llm_code_only(
            provider,
            system_prompt,
            prompt,
            debug_stage=f"{str(kind_label or '').lower()}_handler_full_body_repair_{attempt}",
            max_tokens=65536,
        )
        candidate_body, normalize_error = _normalize_generated_handler_body_ai(candidate_text, canonical_header)
        if not candidate_body:
            last_err = normalize_error or "LLM returned an invalid complete editable handler body."
            continue

        canonical_full = canonical_header.rstrip() + "\n\n" + candidate_body.strip() + "\n"
        ok, err = validate_python_syntax(canonical_full)
        if not ok:
            last_err = err
            continue

        preservation_errors = _handler_repair_preservation_errors_ai(current_full, canonical_full)
        if preservation_errors:
            last_err = "; ".join(preservation_errors)
            current_body_text = candidate_body.strip()
            current_full = canonical_full
            continue

        contract_errors = set(_handler_contract_errors_ai(merged_config_json, canonical_full, kind_label))
        if require_clean_contract and contract_errors:
            # Solution repair is a semantic transaction: metadata and the complete
            # runtime BODY must leave the round together. A method added to JSON is
            # NOT accepted as pre-existing debt to be discovered by the next round.
            last_err = (
                "Solution repair still violates the JSON/Python contract; fix ALL of these declarations in this same complete BODY: "
                + "; ".join(sorted(contract_errors)[:120])
            )
            current_body_text = candidate_body.strip()
            current_full = canonical_full
            continue
        # A complete Server BODY is not accepted merely because it parses and
        # satisfies JSON-declared methods.  NameError-class defects are equally
        # deterministic and must be repaired inside THIS same full-body call, not
        # leaked into the next outer repair round.
        if require_clean_contract and str(kind_label or "").strip().upper() == "SERVER":
            _runtime_candidate = deepcopy(merged_config_json or {})
            _runtime_candidate["nodes_server_handlers"] = _encode_b64_text(canonical_full)
            unresolved_errors = _ngenie_unresolved_server_global_errors(_runtime_candidate)
            if unresolved_errors:
                last_err = (
                    "Solution Server BODY still has unresolved runtime globals; define/import/fix ALL of them in this same complete BODY: "
                    + "; ".join(str(x) for x in unresolved_errors[:120])
                )
                current_body_text = candidate_body.strip()
                current_full = canonical_full
                continue

        # One atomic result: caller replaces the complete BODY only after all
        # checks above have succeeded.  No individual method/edit can disappear
        # because of an unrelated textual-anchor failure.
        return candidate_body.strip()

    raise RuntimeError(
        f"Failed to validate complete {str(kind_label or '').upper()} handler BODY repair after "
        f"{max_attempts} attempt(s): {last_err}"
    )


def _repair_handlers_body_ai(
    provider: str,
    user_request: str,
    merged_config_json: dict,
    current_header: str,
    current_body: str,
    kind_label: str,
    max_attempts: int = 2,
    force_full_source: bool = False,
    require_clean_contract: bool = False,
):
    """Repair a complete handler with exact textual edits, not a full-file rewrite.

    The first attempt sees only a dependency-aware verbatim scope when the
    failing classes can be identified; a rejected patch retries against the full
    file.  The response is always compact SEARCH/REPLACE JSON, so unchanged Python
    is neither regenerated nor normally resent.
    """
    canonical_header = str(current_header or "").rstrip() + "\n"
    current_full = canonical_header + ("\n" if str(current_body or "").strip() else "") + str(current_body or "").strip() + "\n"
    system_prompt = _compact_handler_repair_system_prompt(kind_label)
    context_json = json.dumps(
        {k: v for k, v in (merged_config_json or {}).items() if k not in {"nodes_handlers", "nodes_server_handlers"}},
        ensure_ascii=False,
        indent=2,
    )

    last_err = ""
    # A complete-runtime repair round must see one coherent file.  Earlier versions
    # split validation issues into many tiny calls and showed only scoped fragments;
    # after metadata changed, those calls could work from mutually stale assumptions.
    # Keep scoped mode for non-Solutions callers, but Solutions generation passes
    # force_full_source=True and repairs one complete Android/Server artifact per round.
    scoped_source, scoped_classes, scope_is_full = _scoped_handler_repair_source_ai(current_full, user_request)
    for attempt in range(1, max_attempts + 1):
        use_full = bool(force_full_source) or scope_is_full or attempt > 1
        repair_source = current_full if use_full else scoped_source
        source_label = "CURRENT COMPLETE HANDLER FILE" if use_full else "VERBATIM REPAIR SCOPE EXTRACTED FROM THE CURRENT COMPLETE HANDLER FILE"
        scope_note = ""
        if not use_full:
            scope_note = (
                "\nThe platform selected affected classes: " + ", ".join(scoped_classes) + ". "
                "Every shown source block is copied verbatim from the complete file. "
                "Return SEARCH anchors only from the shown blocks; the platform applies them atomically to the complete original file.\n"
            )
        prompt = (
            "Repair the existing handler using exact textual edits.\n\n"
            + str(user_request or "")
            + "\n\nRelevant configuration metadata:\n"
            + context_json
            + scope_note
            + "\n\n" + source_label + ":\n"
            + repair_source
        )
        if last_err:
            prompt += (
                "\n\nThe previous exact-text repair patch was rejected by the platform:\n"
                + last_err
                + "\nReturn a corrected JSON edits patch against the CURRENT complete file shown above."
            )

        completion_text = call_llm(
            provider,
            system_prompt,
            prompt,
            debug_stage=f"{str(kind_label or '').lower()}_handler_json_patch_{attempt}",
            max_tokens=32768,
        )
        try:
            patch_obj = json.loads(extract_json_from_text(completion_text))
            patched_full = _apply_exact_text_edits_ai(current_full, patch_obj)

            # The immutable runtime prefix always comes from the current file.
            # This is still plain-text repair: no Python AST merge is performed.
            _candidate_header, candidate_body = _split_handlers_header_and_body(patched_full)
            if not _candidate_header:
                raise ValueError("repair patch removed the runtime header/from nodes import Node marker")
            if not str(candidate_body or "").strip():
                raise ValueError("repair patch produced an empty handler body")
            canonical_full = canonical_header + "\n" + candidate_body.strip() + "\n"
            ok, err = validate_python_syntax(canonical_full)
            if not ok:
                raise ValueError(err)
            preservation_errors = _handler_repair_preservation_errors_ai(current_full, canonical_full)
            if preservation_errors:
                raise ValueError("; ".join(preservation_errors))
            contract_errors = set(_handler_contract_errors_ai(merged_config_json, canonical_full, kind_label))
            if require_clean_contract and contract_errors:
                raise ValueError(
                    "Solution focused repair still violates the JSON/Python contract; fix ALL declarations in this same full-file patch: "
                    + "; ".join(sorted(contract_errors)[:120])
                )
            if require_clean_contract and str(kind_label or "").strip().upper() == "SERVER":
                _runtime_candidate = deepcopy(merged_config_json or {})
                _runtime_candidate["nodes_server_handlers"] = _encode_b64_text(canonical_full)
                unresolved_errors = _ngenie_unresolved_server_global_errors(_runtime_candidate)
                if unresolved_errors:
                    raise ValueError(
                        "Solution focused Server repair still has unresolved runtime globals; fix ALL of them in this same full-file patch: "
                        + "; ".join(str(x) for x in unresolved_errors[:120])
                    )
            return candidate_body.strip()
        except Exception as exc:
            last_err = str(exc)

    raise RuntimeError(
        f"Failed to apply focused {str(kind_label or '').upper()} handler repair after "
        f"{max_attempts} attempt(s): {last_err}"
    )


def ensure_handlers_skeleton_and_headers(config_uid: str, config_url: str, cfg: dict):
    """
    Ensures that:
    - nodes_handlers contains ANDROID_IMPORTS_TEMPLATE + from nodes import Node
    - nodes_server_handlers contains from nodes import Node
    Even if LLM did not return a server file.
    """
    # ANDROID
    android_code = _decode_b64_text(cfg.get("nodes_handlers", "") or "")
    if not android_code.strip():
        android_imports = ANDROID_IMPORTS_TEMPLATE.format(uid=config_uid, config_url=config_url)
        android_code = android_imports + NODE_CLASS_CODE_ANDROID.strip() + "\n"
        cfg["nodes_handlers"] = _encode_b64_text(android_code)
    else:
        # If someone brings an Android without Node, we'll add it (as in upload/create_class)
        if "from nodes import Node" not in android_code:
            android_imports = ANDROID_IMPORTS_TEMPLATE.format(uid=config_uid, config_url=config_url)
            android_code = android_imports + NODE_CLASS_CODE_ANDROID.strip() + "\n" + android_code
        android_code = _rewrite_android_handlers_instance_refs_code(android_code, config_uid, config_url)
        cfg["nodes_handlers"] = _encode_b64_text(android_code)

    # SERVER
    server_code = _decode_b64_text(cfg.get("nodes_server_handlers", "") or "")
    if not server_code.strip():
        server_code = NODE_CLASS_CODE.strip() + "\n"
        cfg["nodes_server_handlers"] = _encode_b64_text(server_code)
    else:
        if "from nodes import Node" not in server_code:
            server_code = NODE_CLASS_CODE.strip() + "\n\n" + server_code
            cfg["nodes_server_handlers"] = _encode_b64_text(server_code)

def ensure_all_classes_present_in_handlers(cfg: dict):
    """
    For each class in the JSON, it guarantees that the class exists:
    - in android handlers
    - in server handlers

    IMPORTANT: We use the same init signatures as in create_class().
    """
    classes = cfg.get("classes") or []
    if not isinstance(classes, list) or not classes:
        return

    android_code = _decode_b64_text(cfg.get("nodes_handlers", "") or "")
    server_code = _decode_b64_text(cfg.get("nodes_server_handlers", "") or "")

    def has_class(code: str, name: str) -> bool:
        return f"class {name}(" in code

    # Android stub 
    def android_stub(name: str) -> str:
        return f"""
class {name}(Node):
    def __init__(self, modules, jNode, modulename, uid, _data):
        super().__init__(modules, jNode, modulename, uid, _data)

    \"\"\"Class {name}\"\"\"
"""

    # Server stub 
    def server_stub(name: str) -> str:
        return f"""
class {name}(Node):

    def __init__(self, node_id=None, config_uid=None):
        super().__init__(node_id, config_uid)
        # Additional initialization for {name}
"""

    for cls in classes:
        if not isinstance(cls, dict):
            continue
        name = (cls.get("name") or "").strip()
        if not name:
            continue

        if not has_class(android_code, name):
            android_code += "\n\n" + android_stub(name).lstrip("\n")

        if not has_class(server_code, name):
            server_code += "\n\n" + server_stub(name).lstrip("\n")

    cfg["nodes_handlers"] = _encode_b64_text(android_code)
    cfg["nodes_server_handlers"] = _encode_b64_text(server_code)



























def _ngenie_handler_ui_map(code: str):
    result = {}
    if not str(code or "").strip():
        return result
    try:
        tree = ast.parse(code)
    except Exception:
        return result
    for cls in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
        methods = {}
        for fn in [n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            direct_show = False
            delegates = set()
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                f = node.func
                if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == "self":
                    if f.attr == "Show":
                        direct_show = True
                    else:
                        delegates.add(f.attr)
            methods[fn.name] = {"show": direct_show, "delegates": delegates}
        result[cls.name] = methods
    return result


def _ngenie_method_draws_ui(methods: dict, name: str, seen=None) -> bool:
    seen = set(seen or set())
    if name in seen:
        return False
    seen.add(name)
    info = methods.get(name) or {}
    if info.get("show"):
        return True
    return any(_ngenie_method_draws_ui(methods, str(child), seen) for child in (info.get("delegates") or set()))


def _ngenie_ensure_method_metadata(cls: dict, method: str, engine: str) -> bool:
    methods = cls.setdefault("methods", [])
    for row in methods:
        if isinstance(row, dict) and str(row.get("name") or "") == method and str(row.get("engine") or "") == engine:
            return False
    methods.append({"name": method, "source": "internal", "engine": engine, "code": method})
    return True


def _ngenie_ensure_event(cls: dict, event: str, listener: str, method: str) -> bool:
    events = cls.setdefault("events", [])
    for row in events:
        if not isinstance(row, dict) or str(row.get("event") or "") != event:
            continue
        if str(row.get("listener") or "") != str(listener or ""):
            continue
        for action in row.get("actions") or []:
            if isinstance(action, dict) and str(action.get("method") or "") == method:
                return False
        row.setdefault("actions", []).append({"action": "run", "source": "internal", "server": "internal", "method": method, "postExecuteMethod": ""})
        return True
    events.append({"event": event, "listener": listener, "actions": [{"action": "run", "source": "internal", "server": "internal", "method": method, "postExecuteMethod": ""}]})
    return True


def _ngenie_autofix_generated_ui_wiring(cfg: dict):
    """Deterministically repair unambiguous lifecycle metadata.

    A generated ``Open/Show/BuildView`` that already calls ``self.Show`` has one
    obvious web/mobile lifecycle: onShowWeb/onShow.  Asking the LLM to spend
    minutes rediscovering that metadata is both slow and fragile.  Projection
    ``onRunProjection`` wiring is equally canonical.
    """
    changes = []
    classes = [c for c in (cfg.get("classes") or []) if isinstance(c, dict)]
    by_name = {str(c.get("name") or ""): c for c in classes if str(c.get("name") or "")}
    server_map = _ngenie_handler_ui_map(_decode_b64_text(cfg.get("nodes_server_handlers", "")))
    android_map = _ngenie_handler_ui_map(_decode_b64_text(cfg.get("nodes_handlers", "")))

    for cname, methods in server_map.items():
        cls = by_name.get(cname)
        if not cls:
            continue
        for method in methods:
            if method.lower() not in {"open", "show", "buildview"} or not _ngenie_method_draws_ui(methods, method):
                continue
            changed = _ngenie_ensure_method_metadata(cls, method, "server_python")
            changed = _ngenie_ensure_event(cls, "onShowWeb", "", method) or changed
            if changed:
                changes.append(f"{cname}.onShowWeb->{method}")
        if str(cls.get("class_type") or "") == "projection" and "onRunProjection" in methods:
            changed = _ngenie_ensure_method_metadata(cls, "onRunProjection", "server_python")
            changed = _ngenie_ensure_event(cls, "onInputWeb", "onRunProjection", "onRunProjection") or changed
            if changed:
                changes.append(f"{cname}.onInputWeb->onRunProjection")

    for cname, methods in android_map.items():
        cls = by_name.get(cname)
        if not cls:
            continue
        for method in methods:
            if method.lower() not in {"open", "show", "buildview"} or not _ngenie_method_draws_ui(methods, method):
                continue
            changed = _ngenie_ensure_method_metadata(cls, method, "android_python")
            changed = _ngenie_ensure_event(cls, "onShow", "", method) or changed
            if changed:
                changes.append(f"{cname}.onShow->{method}")
    return changes


def _ngenie_normalize_generated_event_shapes(cfg: dict):
    """Canonicalize event rows emitted by LLMs before validation/apply.

    NodaLogic export/import uses ``event`` + ``listener`` + ``actions``.  Models
    occasionally emit UI-ish aliases such as ``name``/``handlers``.  A missing
    ``event`` used to survive validation and then crash apply_full_config_from_json
    with KeyError('event').  Normalize the unambiguous aliases here so a staged
    candidate can be resumed without another expensive LLM pass.
    """
    changes = []

    def normalize_rows(rows, scope):
        if not isinstance(rows, list):
            return []
        out = []
        for idx, raw in enumerate(rows):
            if not isinstance(raw, dict):
                changes.append(f"{scope}[{idx}]: dropped non-object event row")
                continue
            row = raw
            if row.get('enabled') is False:
                changes.append(f"{scope}[{idx}]: dropped disabled event")
                continue
            event_name = str(row.get('event') or row.get('name') or '').strip()
            if not event_name:
                # A row without an event cannot be imported or dispatched.  Keep
                # it out of apply rather than allowing a late KeyError.
                changes.append(f"{scope}[{idx}]: dropped event without event/name")
                continue
            if not str(row.get('event') or '').strip():
                row['event'] = event_name
                changes.append(f"{scope}[{idx}]: name -> event ({event_name})")
            row.setdefault('listener', '')
            actions = row.get('actions')
            if not isinstance(actions, list):
                actions = []
                handlers = row.get('handlers')
                if isinstance(handlers, dict):
                    for listener, handler in handlers.items():
                        if isinstance(handler, str) and handler.strip():
                            actions.append({
                                'action': 'run', 'source': 'internal', 'server': 'internal',
                                'method': handler.strip(), 'postExecuteMethod': ''
                            })
                        elif isinstance(handler, dict):
                            method = str(handler.get('method') or handler.get('handler') or '').strip()
                            if method:
                                actions.append({
                                    'action': str(handler.get('action') or 'run'),
                                    'source': str(handler.get('source') or 'internal'),
                                    'server': str(handler.get('server') or 'internal'),
                                    'method': method,
                                    'postExecuteMethod': str(handler.get('postExecuteMethod') or ''),
                                })
                row['actions'] = actions
                if 'handlers' in row:
                    changes.append(f"{scope}[{idx}]: handlers -> actions")
            # Empty CommonEvents are pure no-ops.  Dropping them is cleaner than
            # persisting an inert alias event that came from a non-canonical LLM
            # shape (the supplied candidate had exactly this case).
            if scope == 'CommonEvents' and not row.get('actions'):
                changes.append(f"{scope}[{idx}]: dropped no-op event {event_name}")
                continue
            out.append(row)
        return out

    cfg['CommonEvents'] = normalize_rows(cfg.get('CommonEvents') or [], 'CommonEvents')
    for ci, cls in enumerate(cfg.get('classes') or []):
        if not isinstance(cls, dict):
            continue
        cls['events'] = normalize_rows(cls.get('events') or [], f"classes[{ci}].events")
    return changes


_NGENIE_QUANT_LEDGER_RUNTIME_NAMES = {
    "QuantLedgerError", "QuantFormatError", "ScopeRequiredError", "ResourceError",
    "NegativeBalanceError", "OperationConflictError", "SelectorConflictError",
    "MoveResult", "BalanceRow", "MovementRow", "StatementRow", "VerifyResult",
    "quant", "parse_quant", "quant_part", "transaction", "LedgerTransaction", "move",
    "get_balance", "balance", "select_balances", "balances", "select_movements", "movements",
    "statement", "verify_space", "rebuild_balances",
}


# Supported server-runtime names which may be used by generated business code but
# are intentionally not required in every immutable handler header.  If the body
# actually references one of these names, generation supplies the import
# deterministically rather than wasting an LLM repair on platform boilerplate.
_NGENIE_NODES_SERVER_RUNTIME_NAMES = {
    "AcceptRejected",
    "system_user_node",
    "current_config_uid_from_handlers",
    "register_in_config_rooms_many",
}


def _ngenie_loaded_python_names(code: str) -> set:
    """Best-effort set of identifiers loaded by generated Python code."""
    if not str(code or "").strip():
        return set()
    try:
        tree = ast.parse(code)
    except Exception:
        return set()
    return {
        node.id for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }


def _ngenie_top_level_imported_names(code: str) -> set:
    """Names imported at module level (local imports do not satisfy other methods)."""
    if not str(code or "").strip():
        return set()
    try:
        tree = ast.parse(code)
    except Exception:
        return set()
    names = set()
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or str(alias.name).split(".", 1)[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    continue
                names.add(alias.asname or alias.name)
    return names


def _ngenie_insert_header_imports(code: str, import_lines: list[str]) -> str:
    """Insert deterministic platform imports into the immutable handler header.

    The handler LLM is intentionally allowed to edit only the body.  Supported
    platform dependencies therefore have to be supplied by the generator, not
    hallucinated as method-local imports.  Keep them before ``from nodes import
    Node`` so subsequent body generations continue to regard them as immutable.
    """
    rows = [str(row or "").strip() for row in (import_lines or []) if str(row or "").strip()]
    if not rows:
        return code
    text_code = str(code or "")
    marker = "from nodes import Node"
    idx = text_code.find(marker)
    block = "\n".join(rows) + "\n"
    if idx >= 0:
        line_start = text_code.rfind("\n", 0, idx) + 1
        return text_code[:line_start] + block + text_code[line_start:]
    return block + text_code


def _ngenie_autofix_generated_server_runtime_imports(cfg: dict) -> list[str]:
    """Supply supported runtime imports referenced by a generated server handler.

    A real failure from reference-based generation exposed the design
    hole here: server-body generation is told to return *no imports*, while the
    immutable default header historically contained only ``nodes``.  The model
    therefore produced valid-looking calls to ``quant``/``move``/``Decimal``
    that compiled but failed at runtime.  Make these dependencies deterministic
    exactly as the working reference implementation does.
    """
    code = _decode_b64_text((cfg or {}).get("nodes_server_handlers", "") or "")
    if not code.strip():
        return []
    loaded = _ngenie_loaded_python_names(code)
    imported = _ngenie_top_level_imported_names(code)
    lines = []
    changes = []

    quant_needed = sorted((loaded & _NGENIE_QUANT_LEDGER_RUNTIME_NAMES) - imported)
    if quant_needed:
        lines.append("from quant_ledger.api import " + ", ".join(quant_needed))
        changes.append("server handlers: injected QuantLedger imports: " + ", ".join(quant_needed))

    nodes_needed = sorted((loaded & _NGENIE_NODES_SERVER_RUNTIME_NAMES) - imported)
    if nodes_needed:
        lines.append("from nodes import " + ", ".join(nodes_needed))
        changes.append("server handlers: injected supported nodes runtime imports: " + ", ".join(nodes_needed))

    if "Decimal" in loaded and "Decimal" not in imported:
        lines.append("from decimal import Decimal")
        changes.append("server handlers: injected Decimal import")

    if not lines:
        return []
    code = _ngenie_insert_header_imports(code, lines)
    cfg["nodes_server_handlers"] = _encode_b64_text(code)
    return changes


def _ngenie_autofix_hidden_no_ui_server_artifacts(cfg: dict) -> list[str]:
    """Remove hallucinated server UI from classes explicitly designed as no-UI.

    Handler-only repair cannot create real NodaLogic lifecycle wiring because it
    lives in JSON ``class.events``.  A model previously tried to work around that
    by inventing ``self.listen(...)`` and a ShowDispatcher method inside a class
    whose approved metadata said ``hidden`` / ``не имеет UI``.  Such code is both
    contrary to the design and invalid against the real Node runtime.  This case
    is unambiguous enough to normalize without another model call.
    """
    code = _decode_b64_text((cfg or {}).get("nodes_server_handlers", "") or "")
    if not code.strip():
        return []
    try:
        tree = ast.parse(code)
    except Exception:
        return []
    by_name = {
        str(c.get("name") or ""): c
        for c in (cfg.get("classes") or [])
        if isinstance(c, dict) and str(c.get("name") or "")
    }
    lines = code.splitlines(keepends=True)
    removals = []
    changes = []

    def add_range(node):
        start = max(1, int(getattr(node, "lineno", 1) or 1))
        end = max(start, int(getattr(node, "end_lineno", start) or start))
        removals.append((start, end))

    for class_node in [n for n in getattr(tree, "body", []) if isinstance(n, ast.ClassDef)]:
        meta = by_name.get(str(class_node.name or ""))
        if not meta or not bool(meta.get("hidden")):
            continue
        if str(meta.get("init_screen_layout") or "").strip() or str(meta.get("init_screen_layout_web") or "").strip():
            continue
        if any(isinstance(ev, dict) for ev in (meta.get("events") or [])):
            continue
        desc = " ".join(
            str(meta.get(k) or "") for k in ("display_name", "ngenie_role", "ngenie_prompt", "ngenie_description")
        ).lower()
        if not ("no ui" in desc or "не имеет ui" in desc or "без ui" in desc):
            continue
        declared = {
            str(m.get("code") or m.get("name") or "").strip()
            for m in (meta.get("methods") or []) if isinstance(m, dict)
        }
        removed_listener = False
        removed_methods = []
        for fn in [n for n in getattr(class_node, "body", []) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            if fn.name == "__init__":
                for stmt in getattr(fn, "body", []):
                    if not isinstance(stmt, ast.Expr) or not isinstance(getattr(stmt, "value", None), ast.Call):
                        continue
                    call = stmt.value
                    func = call.func
                    if (
                        isinstance(func, ast.Attribute)
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "self"
                        and func.attr in {"listen", "add_listener", "addListener"}
                    ):
                        add_range(stmt)
                        removed_listener = True
                continue
            if fn.name in declared:
                continue
            uses_show = False
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr == "Show":
                    uses_show = True
                    break
                if isinstance(func, ast.Name) and func.id == "Show":
                    uses_show = True
                    break
            if uses_show:
                add_range(fn)
                removed_methods.append(fn.name)
        if removed_listener or removed_methods:
            detail = []
            if removed_listener:
                detail.append("unsupported self.listen wiring")
            if removed_methods:
                detail.append("undeclared UI methods " + ", ".join(sorted(removed_methods)))
            changes.append(f"{class_node.name}: removed accidental no-UI artifacts ({'; '.join(detail)})")

    if not removals:
        return []
    # Remove whole source ranges from bottom to top so AST line numbers remain valid.
    for start, end in sorted(set(removals), reverse=True):
        del lines[start - 1:end]
    cleaned = "".join(lines)
    try:
        ast.parse(cleaned)
    except Exception:
        return []
    cfg["nodes_server_handlers"] = _encode_b64_text(cleaned)
    return changes


def _ngenie_autofix_shadowed_orphan_class_methods(candidate: dict) -> list[str]:
    """Remove repair debris that shadows an existing module-level callable.

    A focused repair once tried to fix ``plan_putaway_by_zone`` by replacing the
    next top-level ``class`` anchor with an indented ``def``.  Python therefore
    attached the new function to the *previous* Node class, while the real
    module-level strategy remained unchanged.  If a class method is not declared
    in that class metadata and has the same name as an existing top-level
    function, the method is unambiguously shadow/orphan repair debris.
    """
    code = _decode_b64_text((candidate or {}).get("nodes_server_handlers", "") or "")
    if not code.strip():
        return []
    try:
        tree = ast.parse(code)
    except Exception:
        return []
    metadata = {
        str(cls.get("name") or ""): cls
        for cls in (candidate.get("classes") or [])
        if isinstance(cls, dict) and str(cls.get("name") or "")
    }
    top_functions = {
        node.name for node in getattr(tree, "body", [])
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if not top_functions:
        return []
    removals = []
    details = []
    for class_node in [n for n in getattr(tree, "body", []) if isinstance(n, ast.ClassDef)]:
        cls_meta = metadata.get(class_node.name) or {}
        declared = {
            str(m.get("name") or m.get("code") or "").strip()
            for m in (cls_meta.get("methods") or [])
            if isinstance(m, dict) and str(m.get("name") or m.get("code") or "").strip()
        }
        for fn in getattr(class_node, "body", []):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if fn.name in declared or fn.name == "__init__":
                continue
            if fn.name not in top_functions:
                continue
            start = int(getattr(fn, "lineno", 1) or 1)
            end = int(getattr(fn, "end_lineno", start) or start)
            removals.append((start, end))
            details.append(f"{class_node.name}.{fn.name}")
    if not removals:
        return []
    lines = code.splitlines(keepends=True)
    for start, end in sorted(set(removals), reverse=True):
        del lines[start - 1:end]
    cleaned = "".join(lines)
    try:
        ast.parse(cleaned)
    except Exception:
        return []
    candidate["nodes_server_handlers"] = _encode_b64_text(cleaned)
    return ["Server handler: removed orphan class methods shadowing active module functions: " + ", ".join(sorted(details))]


def _ngenie_autofix_unreferenced_orphan_node_classes(candidate: dict) -> list[str]:
    """Drop unreferenced Node subclasses that do not exist in configuration metadata.

    Earlier repair rounds could append a Python-only report class while trying to
    satisfy an old exact-name metadata rule.  Such a class cannot be opened or
    instantiated as a configuration class and only pollutes later repair scopes.
    Removal is intentionally conservative: direct ``Node`` subclass, absent from
    metadata, and no executable/string reference outside its own definition.
    """
    code = _decode_b64_text((candidate or {}).get("nodes_server_handlers", "") or "")
    if not code.strip():
        return []
    try:
        tree = ast.parse(code)
    except Exception:
        return []
    metadata_names = {
        str(cls.get("name") or "") for cls in (candidate.get("classes") or [])
        if isinstance(cls, dict) and str(cls.get("name") or "")
    }
    class_nodes = [n for n in getattr(tree, "body", []) if isinstance(n, ast.ClassDef)]
    candidates = []
    for node in class_nodes:
        if node.name in metadata_names:
            continue
        direct_node = any(isinstance(base, ast.Name) and base.id == "Node" for base in node.bases)
        if not direct_node:
            continue
        candidates.append(node)
    if not candidates:
        return []
    orphan_names = {node.name for node in candidates}
    referenced = set()
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.ClassDef) and node.name in orphan_names:
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load) and child.id in orphan_names:
                referenced.add(child.id)
            elif isinstance(child, ast.Constant) and isinstance(child.value, str) and child.value in orphan_names:
                referenced.add(child.value)
    removable = [node for node in candidates if node.name not in referenced]
    if not removable:
        return []
    lines = code.splitlines(keepends=True)
    for node in sorted(removable, key=lambda n: int(n.lineno), reverse=True):
        start = int(node.lineno)
        end = int(getattr(node, "end_lineno", start) or start)
        del lines[start - 1:end]
    cleaned = "".join(lines)
    try:
        ast.parse(cleaned)
    except Exception:
        return []
    candidate["nodes_server_handlers"] = _encode_b64_text(cleaned)
    return ["Server handler: removed unreferenced Python-only Node classes: " + ", ".join(sorted(n.name for n in removable))]


def _ngenie_unresolved_server_global_errors(candidate: dict) -> list[str]:
    """Detect definite unresolved global names in generated server Python.

    Python syntax validation does not catch ``NameError``.  This check uses the
    stdlib symbol table and only reports names that a function/class resolves as
    global while the module neither defines nor imports them.  Star-import files
    are skipped to avoid false positives.
    """
    import builtins
    import symtable

    code = _decode_b64_text((candidate or {}).get("nodes_server_handlers", "") or "")
    if not code.strip():
        return []
    try:
        tree = ast.parse(code)
        table = symtable.symtable(code, "<generated server handler>", "exec")
    except Exception:
        return []
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
            return []
    module_names = set(dir(builtins))
    for symbol in table.get_symbols():
        if symbol.is_assigned() or symbol.is_imported() or symbol.is_namespace() or symbol.is_parameter():
            module_names.add(symbol.get_name())
    unresolved = set()
    def visit_table(current):
        for symbol in current.get_symbols():
            if symbol.is_referenced() and symbol.is_global() and symbol.get_name() not in module_names:
                unresolved.add(symbol.get_name())
        for child in current.get_children():
            visit_table(child)
    visit_table(table)
    if not unresolved:
        return []
    first_line = {}
    first_scope = {}
    # Prefer a concrete function/method owner in the error text so focused repair
    # can extract that exact symbol instead of resending the whole handler.
    for top in getattr(tree, "body", []):
        scopes = []
        if isinstance(top, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scopes.append((top.name, top))
        elif isinstance(top, ast.ClassDef):
            for fn in top.body:
                if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    scopes.append((f"{top.name}.{fn.name}", fn))
        for scope_name, scope_node in scopes:
            for node in ast.walk(scope_node):
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in unresolved:
                    line = int(getattr(node, "lineno", 0) or 0)
                    if node.id not in first_line or (line and line < first_line[node.id]):
                        first_line[node.id] = line
                        first_scope[node.id] = scope_name
    errors = []
    for name in sorted(unresolved):
        scope = first_scope.get(name) or "module"
        # Preserve the runtime target at the point where the deterministic
        # validator KNOWS it.  Do not throw that information away and later
        # rediscover it by fuzzy parsing of the human-readable message.
        errors.append(
            f"[targets:server_python] SERVER Python: unresolved global name '{name}' in function {scope} near line {first_line.get(name) or '?'}; "
            "generated handler would raise NameError at runtime."
        )
    return errors


def _ngenie_dedupe_top_level_handler_classes(candidate: dict) -> list[str]:
    """Collapse duplicate top-level Python class/function definitions, keeping the last.

    Python itself resolves duplicate top-level bindings by replacing the
    earlier binding with the last one.  Focused LLM repair can accidentally append
    another copy while trying to satisfy a metadata error.  Removing the shadowed
    earlier definitions therefore preserves actual runtime semantics while keeping
    the handler deterministic for validation and future repair.
    """
    changes = []
    for field_name, label in (("nodes_handlers", "Android"), ("nodes_server_handlers", "Server")):
        code = _decode_b64_text((candidate or {}).get(field_name, ""))
        if not str(code or "").strip():
            continue
        try:
            tree = ast.parse(code)
        except Exception:
            continue
        definitions_by_key = {}
        for node in getattr(tree, "body", []):
            if isinstance(node, ast.ClassDef):
                definitions_by_key.setdefault(("class", node.name), []).append(node)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions_by_key.setdefault(("function", node.name), []).append(node)
        duplicate_keys = [key for key, rows in definitions_by_key.items() if len(rows) > 1]
        if not duplicate_keys:
            continue
        lines = code.splitlines(keepends=True)
        removals = []
        for key in duplicate_keys:
            rows = definitions_by_key[key]
            # Python exposes only the last top-level binding with a given name.
            # Removing shadowed earlier definitions therefore preserves runtime semantics.
            for node in rows[:-1]:
                start = int(getattr(node, "lineno", 1) or 1)
                end = int(getattr(node, "end_lineno", start) or start)
                removals.append((start, end))
        for start, end in sorted(removals, reverse=True):
            del lines[start - 1:end]
        cleaned = "".join(lines)
        try:
            ast.parse(cleaned)
        except Exception:
            continue
        candidate[field_name] = _encode_b64_text(cleaned)
        changes.append(f"{label} handler: removed shadowed duplicate top-level definitions: {', '.join(sorted(kind + ' ' + name for kind, name in duplicate_keys))}")
    return changes



def _ngenie_validate_generation_candidate(config, candidate: dict, ack_errors=None, reference_config: dict | None = None):
    """Normalize/autofix a generated candidate and run every active validator."""
    config_url = url_for('get_config', uid=config.uid, _external=True)
    ensure_handlers_skeleton_and_headers(config.uid, config_url, candidate)
    auto_changes = _ngenie_dedupe_top_level_handler_classes(candidate)
    # Never delete candidate methods/classes from heuristic "orphan" guesses.
    # Those old autofixes silently changed legitimate helper architecture and
    # are less trustworthy than the model-authored atomic repair. Unsupported
    # APIs such as self.listen are reported by deterministic validation instead.
    auto_changes.extend(_ngenie_autofix_generated_server_runtime_imports(candidate))
    # IMPORTANT: finish every deterministic metadata normalization BEFORE checking
    # JSON -> Python.  Older ordering checked the contract first and then rewired
    # events/UI, allowing the validator itself to create a method obligation that
    # was visible only on the next round.
    auto_changes.extend(_ngenie_normalize_generated_event_shapes(candidate))
    auto_changes.extend(_ngenie_autofix_generated_ui_wiring(candidate))

    # Generation must satisfy the structural JSON -> Python contract itself.
    # Never fabricate empty handler classes here: that hides the primary defect
    # and turns it into a noisy list of missing methods during repair.
    android_contract_errors = _handler_contract_errors_ai(
        candidate, _decode_b64_text(candidate.get("nodes_handlers", "")), "ANDROID"
    )
    server_contract_errors = _handler_contract_errors_ai(
        candidate, _decode_b64_text(candidate.get("nodes_server_handlers", "")), "SERVER"
    )

    errors = list(android_contract_errors) + list(server_contract_errors)
    errors.extend(_ngenie_unresolved_server_global_errors(candidate))
    errors.extend(validate_full_llm_config_ai(candidate))
    try:
        import ngenie_code
        errors.extend(str(x) for x in (ack_errors or []) if str(x or '').strip())
        errors.extend(ngenie_code.validate_no_config_exfiltration(candidate))
        quality_errors = list(ngenie_code.validate_generation_quality(candidate))
        # A typed reference is an accepted implementation benchmark. Generic
        # validator warnings that are also produced by the exact reference are
        # baseline limitations of that validator, not regressions introduced by
        # the generated candidate. Suppress only exact matching messages; new
        # project-specific warnings still fail validation.
        if isinstance(reference_config, dict) and reference_config:
            try:
                baseline_quality = set(str(x) for x in ngenie_code.validate_generation_quality(reference_config))
                quality_errors = [str(x) for x in quality_errors if str(x) not in baseline_quality]
            except Exception:
                pass
        errors.extend(quality_errors)
    except Exception as exc:
        errors.append(f"nGenie Code validation failed: {exc}")
    # Preserve order while eliminating duplicate validator messages.
    errors = list(dict.fromkeys(str(err) for err in errors if str(err or '').strip()))
    return errors, auto_changes





def _ngenie_draft_apply_safety_errors(candidate: dict) -> list[str]:
    """Minimal gate for persisting an intermediate working draft.

    A draft does not need to pass release/quality validation.  It only needs to
    be safe enough to become the next repair base: both Python artifacts must be
    syntactically valid, JSON runtime declarations must have matching Python
    implementations, and nGenie security must not detect configuration
    exfiltration/cross-config behavior.  apply_full_config_from_json remains the
    final structural guard and rolls back atomically if metadata cannot be loaded.
    """
    if not isinstance(candidate, dict):
        return ["candidate is not a JSON object"]
    errors = []
    for field_name, label, runtime_label in (
        ("nodes_handlers", "Android", "ANDROID"),
        ("nodes_server_handlers", "Server", "SERVER"),
    ):
        raw = candidate.get(field_name) or ""
        code = _decode_b64_text(raw) if raw else ""
        if code.strip():
            ok, err = validate_python_syntax(code)
            if not ok:
                errors.append(f"{label} handler Python syntax: {err}")
                continue
        # JSON<->Python coherence is part of draft integrity, not release quality.
        # Run this even when the handler artifact is empty: metadata that requires
        # runtime methods with no Python file is exactly the state we must reject.
        errors.extend(_handler_contract_errors_ai(candidate, code, runtime_label))
    try:
        import ngenie_code
        errors.extend(str(x) for x in ngenie_code.validate_no_config_exfiltration(candidate) if str(x or '').strip())
    except Exception as exc:
        errors.append(f"nGenie safety validation failed: {exc}")
    return list(dict.fromkeys(errors))

























def _ngenie_repair_goal_prompt(prompt: str) -> str:
    """Stable normalized goal used only for generic repair-loop progress."""
    text = str(prompt or "").replace("\r\n", "\n").strip()
    for marker in ("--- ОСНОВНОЙ ПРОМПТ ГЕНЕРАЦИИ ---", "--- ОСНОВНОЙ ПРОМПТ ЭТАПА ---"):
        idx = text.find(marker)
        if idx >= 0:
            tail = text[idx + len(marker):].lstrip()
            next_sep = tail.find("\n--- ")
            if next_sep >= 0:
                tail = tail[:next_sep]
            if tail.strip():
                return re.sub(r"\s+", " ", tail).strip()[:4000]
    # Post-generation focused changes do not carry the plan marker.  A bounded
    # normalized prefix is enough together with config UID/base fingerprint.
    return re.sub(r"\s+", " ", text).strip()[:4000]


def _ngenie_repair_issue_id(value: Any) -> str:
    """Stable key for one repair question/blocker.

    Rows may carry [ID:...] in the human-readable error.  Mechanical
    validators do not, so fall back to a normalized text fingerprint.  Progress is
    therefore based on whether a question already sent to repair comes back, not on
    the total number of newly discovered issues.
    """
    text = str(value or "").strip()
    match = re.search(r"\[ID:([^\]]+)\]", text, flags=re.I)
    if match:
        return "semantic:" + re.sub(r"[^A-Za-z0-9_.:-]+", "_", match.group(1).strip()).upper()
    normalized = " ".join(text.lower().split())
    if not normalized:
        return ""
    return "mechanical:" + hashlib.sha1(normalized.encode("utf-8", errors="replace")).hexdigest()[:20]


def _ngenie_repair_issue_state(value: Any) -> str:
    """Reviewer-declared progress for a semantic issue; empty for mechanical errors."""
    text = str(value or "")
    match = re.search(r"\[repair_state:([^\]]+)\]", text, flags=re.I)
    if not match:
        return ""
    state = str(match.group(1) or "").strip().lower()
    return state if state in {"new", "partial", "unchanged"} else ""


def _ngenie_repair_issue_fingerprint(value: Any) -> str:
    """Fingerprint the concrete current remainder, not just the stable issue_id."""
    text = str(value or "").strip()
    text = re.sub(r"\[repair_state:[^\]]+\]", "", text, flags=re.I)
    normalized = " ".join(text.lower().split())
    return hashlib.sha1(normalized.encode("utf-8", errors="replace")).hexdigest()[:20] if normalized else ""




def _ngenie_repair_needs_android(errors) -> bool:
    """Generic nGenie routing; Solution-specific routing lives in solutions/."""
    rows = [str(x or "").strip().lower() for x in (errors or []) if str(x or "").strip()]
    for row in rows:
        targets_match = re.search(r"\[targets:([^\]]+)\]", row, flags=re.I)
        if targets_match:
            targets = {x.strip().lower() for x in targets_match.group(1).split(",") if x.strip()}
            if "android_python" in targets:
                return True
            if targets:
                continue
        if any(k in row for k in ("server_python", "nodes_server_handlers", "server handler", "server method")):
            continue
        if any(k in row for k in (
            "android_python", "nodes_handlers", "android handler", "android method",
            "android", "mobile", "terminal_steps", "scanner", "barcode", "ocr",
        )):
            return True
    return False


def _ngenie_repair_needs_server(errors) -> bool:
    """Generic nGenie routing; domain-specific server hints live in solutions/."""
    rows = [str(x or "").strip().lower() for x in (errors or []) if str(x or "").strip()]
    for row in rows:
        targets_match = re.search(r"\[targets:([^\]]+)\]", row, flags=re.I)
        if targets_match:
            targets = {x.strip().lower() for x in targets_match.group(1).split(",") if x.strip()}
            if "server_python" in targets:
                return True
            if targets:
                continue
        if any(marker in row for marker in ("must wire oninputweb", "must wire onshowweb", "class.events", "event wiring")):
            continue
        if any(k in row for k in (
            "server_python", "nodes_server_handlers", "server handler", "server method",
            "top-level server handler", "onshowweb", "oninputweb", "oninputserver",
        )):
            return True
    return False


def _ngenie_repair_needs_metadata(errors) -> bool:
    """Generic metadata routing; Solution/domain heuristics live in solutions/."""
    rows = [str(x or "").strip().lower() for x in (errors or []) if str(x or "").strip()]
    metadata_markers = (
        "data_structure", "index ", "indexes", "missing field", "section",
        "common events shape", "commonevents shape", "class metadata", "dataset",
        "record_view", "cover_image", "display_image_table", "screen layout",
        "class_type", "has_storage", "missing class", "must wire oninputweb",
        "must wire onshowweb", "class.events", "event wiring",
    )
    for row in rows:
        targets_match = re.search(r"\[targets:([^\]]+)\]", row, flags=re.I)
        if targets_match:
            targets = {x.strip().lower() for x in targets_match.group(1).split(",") if x.strip()}
            if "metadata" in targets:
                return True
            if targets:
                continue
        if any(marker in row for marker in metadata_markers):
            return True
    return False

def _ngenie_metadata_repair_errors(errors):
    return [str(e) for e in (errors or []) if _ngenie_repair_needs_metadata([e])]


def _ngenie_android_repair_errors(errors):
    return [str(e) for e in (errors or []) if _ngenie_repair_needs_android([e])]


def _ngenie_server_repair_errors(errors):
    return [str(e) for e in (errors or []) if _ngenie_repair_needs_server([e])]



def _ngenie_validation_error_signature(errors):
    """Stable signature used only to detect repair loops inside one run.

    Validation messages are deterministic enough that whitespace/case normalization
    catches the dangerous case: the same set returns after a repair.  We do NOT
    weaken this to fuzzy matching because a genuinely changed message is useful
    evidence of progress; the separate error-count rule still requires the total
    number of blockers to decrease before another automatic round is allowed.
    """
    rows = []
    for value in (errors or []):
        text = " ".join(str(value or "").strip().lower().split())
        if text:
            rows.append(text)
    return tuple(sorted(set(rows)))

def _ngenie_handler_syntax_only_errors(errors) -> bool:
    rows = [str(x or "").strip().lower() for x in (errors or []) if str(x or "").strip()]
    if not rows:
        return False
    return all(
        (("nodes_handlers" in row or "nodes_server_handlers" in row) and "syntax error" in row)
        for row in rows
    )


@_routes.route('/config/<uid>/ai-generate', methods=['POST'])
@login_required
def ai_generate(uid):
    config = db.session.execute(select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)).scalar_one_or_none()
    if not config:
        abort(404)

    # Optional feature delegation only. The full Solution lifecycle lives in
    # solutions/generator.py; if that package is absent, this route remains the
    # original generic nGenie generator.
    try:
        import solutions as _solutions_feature
    except (ImportError, ModuleNotFoundError):
        _solutions_feature = None
    if _solutions_feature is not None:
        _owned_solution = _solutions_feature.active_solution_for_config(config, user=current_user)
        if _owned_solution is not None:
            _solution_generator = getattr(_solutions_feature, 'run_solution_ai_generate', None)
            if not callable(_solution_generator):
                return jsonify({'status': 'error', 'message': 'Solutions generation runtime is unavailable'}), 500
            return _solution_generator(
                config, request_obj=request, user=current_user, editor_module=sys.modules[__name__]
            )
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form or {}
    prompt = (data.get('prompt') or '').strip()
    question_answers = _ngenie_code_parse_jsonish(data.get('ngenie_code_question_answers') or data.get('question_answers'))
    if not prompt and question_answers:
        prompt = 'Продолжи генерацию с учетом ответов на уточняющие вопросы.'
    original_user_prompt = prompt
    write_instruction = _ngenie_code_bool(data.get('write_instruction'))
    runtime_error_repair = _ngenie_code_bool(data.get('runtime_error_repair'))
    force_old_ai = _ngenie_code_bool(data.get('old_ai') or data.get('force_old_ai'))
    llm_provider = (data.get('llm') or 'deepseek').strip().lower()
    ngenie_code_mode = _ngenie_code_editor_enabled() and (not force_old_ai)
    ngenie_code_request_id = uuid.uuid4().hex
    early_attachments_text = ''
    if ngenie_code_mode and getattr(request, 'files', None):
        try:
            import ngenie_code
            early_attachments_text = ngenie_code.read_uploaded_files(request.files.getlist('attachments'))
        except Exception:
            early_attachments_text = ''
    if force_old_ai and _config_is_ngenie_code_locked(config):
        return (jsonify({'status': 'error', 'message': _ngenie_code_forbid_message()}), 403)
    if not prompt and (not question_answers) and (not early_attachments_text):
        return (jsonify({'status': 'error', 'message': 'Empty prompt'}), 400)
    if ngenie_code_mode:
        user_chat_content = _ngenie_code_format_question_answers_for_chat(original_user_prompt, question_answers)
        user_meta_kind = 'question_answers' if question_answers else 'generate'
        user_meta = {'kind': user_meta_kind, 'write_instruction': bool(write_instruction)}
        if question_answers:
            user_meta['question_answers'] = question_answers
        _ngenie_code_add_chat_message(config, 'user', user_chat_content, request_id=ngenie_code_request_id, meta=user_meta, commit=True)
    try:
        attachments_text = ''
        if ngenie_code_mode:
            import ngenie_code
            llm_provider = 'ngenie_code'
            ngenie_code.set_debug_context(request_id=ngenie_code_request_id, user_id=getattr(current_user, 'id', None), user_email=getattr(current_user, 'email', ''), config_uid=getattr(config, 'uid', ''), config_name=getattr(config, 'name', ''), original_prompt=original_user_prompt[:4000])
            system_prompt = ngenie_code.build_system_prompt(request_id=ngenie_code_request_id)
            android_system_prompt = system_prompt
            server_system_prompt = system_prompt
            attachments_text = early_attachments_text
            chat_context = _ngenie_code_chat_context_for_llm(config)
            prompt = ngenie_code.build_user_prompt(prompt, attachments_text, chat_context=chat_context, question_answers=question_answers)
        else:
            llm_url = 'https://raw.githubusercontent.com/dvdocumentation/nodalogic/refs/heads/main/LLM.txt'
            r = requests.get(llm_url, timeout=10)
            if r.status_code == 200:
                system_prompt = r.text
            else:
                system_prompt = 'You are the NodaLogic configuration generation assistant. Always return valid JSON without any explanations.'
            android_system_prompt = system_prompt
            server_system_prompt = system_prompt
        persisted_config_json = json.loads(get_config(config.uid))
        before_config_json_for_summary = json.loads(json.dumps(persisted_config_json, ensure_ascii=False))
        current_config_json = persisted_config_json
        reference_generation_mode = False
        reference_config_json = {}
        handler_user_request = prompt
        reference_android_code = ''
        reference_server_code = ''
        if ngenie_code_mode and runtime_error_repair:
            try:
                fast_candidate = json.loads(json.dumps(current_config_json, ensure_ascii=False))
                _fast_validation_errors, fast_changes = _ngenie_validate_generation_candidate(config, fast_candidate, [])
                missing_names = re.findall('NameError\\s*:\\s*name\\s+[\'\\"]([^\'\\"]+)[\'\\"]\\s+is\\s+not\\s+defined', str(original_user_prompt or ''), flags=re.I)
                fast_server_code = _decode_b64_text(fast_candidate.get('nodes_server_handlers', '') or '')
                fast_imported = _ngenie_top_level_imported_names(fast_server_code)
                deterministic_name_fix = bool(missing_names) and all((name in fast_imported for name in missing_names))
                if fast_changes and deterministic_name_fix:
                    apply_full_config_from_json(config, fast_candidate)
                    _ngenie_code_mark_locked(config)
                    assistant_message = 'Исправил runtime-ошибку без обращения к модели: ' + '; '.join(fast_changes)
                    _ngenie_code_add_chat_message(config, 'assistant', assistant_message, request_id=ngenie_code_request_id, meta={'kind': 'runtime_autofix', 'changes': fast_changes, 'validation_warnings': _fast_validation_errors}, commit=False)
                    None
                    None
                    db.session.commit()
                    return jsonify({'status': 'ok', 'message': assistant_message, 'fast_runtime_repair': True, 'auto_changes': fast_changes, 'ngenie_code_request_id': ngenie_code_request_id})
            except Exception:
                db.session.rollback()
                current_config_json = json.loads(get_config(config.uid))
                before_config_json_for_summary = json.loads(json.dumps(current_config_json, ensure_ascii=False))
        ngenie_code_generation_contract = ''
        if ngenie_code_mode:
            try:
                import ngenie_code
                ngenie_code_generation_contract = ngenie_code.build_generation_contract('PATCH')
            except Exception:
                ngenie_code_generation_contract = ''
        merged_config_data = None
        llm_patch_data = {}
        ngenie_code_ack_errors = []
        if merged_config_data is None:
            structural_current_config = {k: v for k, v in current_config_json.items() if k not in {'nodes_handlers', 'nodes_server_handlers', 'nodes_handlers_meta', 'nodes_server_handlers_meta'}}
            reference_structural_json = {k: v for k, v in (reference_config_json or {}).items() if k not in {'nodes_handlers', 'nodes_server_handlers', 'nodes_handlers_meta', 'nodes_server_handlers_meta'}}
            user_prompt_patch = None
            user_prompt_patch = 'User request:\n' + str(prompt or '') + '\n\n' + ('Mandatory nGenie Code generation contract:\n' + ngenie_code_generation_contract + '\n\n' if ngenie_code_generation_contract else '') + 'Below is the CURRENT TARGET configuration in JSON format (handler base64 omitted because handlers are edited in the next dedicated stages).\nReturn ONE JSON object. Normally this is a JSON patch with only changed/added: classes, datasets, sections, CommonEvents, ngenie_prompt.\nIf important semantics are ambiguous, return a question response with root field ngenie_code_questions instead of a patch; do not change the configuration in that response.\nHandler Python is regenerated in the next step from the current handler body and the user request.\nFor every new/changed class fill ngenie_role, ngenie_prompt and ngenie_description.\n' + (ngenie_code.instruction_ack_prompt_text() + '\n\n' if ngenie_code_mode else '') + 'Do not generate handlers or methods that export/download/send the configuration or access other configs.\nUnchanged fields can be omitted. Do not delete anything unless explicitly asked.\nNo comments, ONLY JSON.\n\nCurrent configuration:\n' + json.dumps(structural_current_config, ensure_ascii=False, indent=2)
            completion_text = call_llm(llm_provider, system_prompt, user_prompt_patch, debug_stage='json_patch_initial')
            json_str = extract_json_from_text(completion_text)
            llm_patch_data = json.loads(json_str)
            if ngenie_code_mode:
                try:
                    import ngenie_code
                    clarification_questions = ngenie_code.extract_questions_response(llm_patch_data)
                except Exception:
                    clarification_questions = []
                if clarification_questions:
                    assistant_text = str(llm_patch_data.get('message') or llm_patch_data.get('reply') or 'Для начала уточни вот эти данные').strip()
                    None
                    _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'questions', 'questions': clarification_questions, 'source_prompt': original_user_prompt}, commit=True)
                    return jsonify({'status': 'ok', 'message': assistant_text, 'ngenie_code_questions': clarification_questions, 'ngenie_code_request_id': ngenie_code_request_id})
                try:
                    ngenie_code_ack_errors = ngenie_code.validate_instruction_ack(llm_patch_data)
                except Exception as _ack_error:
                    ngenie_code_ack_errors = [f'nGenie Code instruction ack validation failed: {_ack_error}']
                try:
                    unavailable = ngenie_code.extract_unavailable_request(llm_patch_data)
                except Exception:
                    unavailable = None
                if unavailable:
                    if _ngenie_code_unavailable_is_handler_patch_contract(unavailable):
                        llm_patch_data = _ngenie_code_minimal_ack_patch()
                        ngenie_code_ack_errors = []
                    else:
                        requested = str(unavailable.get('requested_feature') or unavailable.get('requested') or unavailable.get('feature') or 'запрошенная возможность').strip()
                        reason = str(unavailable.get('reason') or unavailable.get('details') or 'В текущих инструкциях/платформе нет такой возможности.').strip()
                        _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, completion_text)
                        assistant_text = 'Такой возможности пока нет: ' + requested + '\n' + reason + '\nЯ записал заявку разработчику; конфигурация не изменялась.'
                        _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'unavailable'}, commit=False)
                        db.session.commit()
                        return jsonify({'status': 'ok', 'message': assistant_text, 'ngenie_code_feature_request': True, 'ngenie_code_request_id': ngenie_code_request_id})
            llm_patch_data_for_merge = ngenie_code.strip_instruction_ack(llm_patch_data) if ngenie_code_mode else llm_patch_data
            merged_config_data = merge_llm_config_into_current_ai(current_config_json, llm_patch_data_for_merge)
            current_android_code = _decode_b64_text(current_config_json.get('nodes_handlers', ''))
            android_header, android_body = _split_handlers_header_and_body(current_android_code)
            if not android_header:
                base_url = current_config_json.get('url', '')
                android_header = ANDROID_IMPORTS_TEMPLATE.format(uid=config.uid, config_url=base_url) + '\n' + NODE_CLASS_CODE_ANDROID.strip() + '\n'
                android_body = android_body or ''
            new_android_body = _generate_handlers_body_ai(provider=llm_provider, system_prompt=locals().get('android_system_prompt', system_prompt), user_request=handler_user_request, merged_config_json=merged_config_data, current_header=android_header, current_body=android_body, kind_label='ANDROID', max_attempts=3, reference_code=reference_android_code, strict_contract=bool(False))
            merged_config_data['nodes_handlers'] = _encode_b64_text(android_header.rstrip() + '\n\n' + new_android_body.strip() + '\n')
            current_server_code = _decode_b64_text(current_config_json.get('nodes_server_handlers', ''))
            server_header, server_body = _split_handlers_header_and_body(current_server_code)
            server_contract_required = bool(_handler_contract_errors_ai(merged_config_data, current_server_code, 'SERVER'))
            if current_config_json.get('nodes_server_handlers') or server_header or server_body or server_contract_required:
                if not server_header:
                    server_header = NODE_CLASS_CODE.strip() + '\n'
                    server_body = server_body or ''
                new_server_body = _generate_handlers_body_ai(provider=llm_provider, system_prompt=locals().get('server_system_prompt', system_prompt), user_request=handler_user_request, merged_config_json=merged_config_data, current_header=server_header, current_body=server_body, kind_label='SERVER', max_attempts=3, reference_code=reference_server_code, strict_contract=bool(False))
                merged_config_data['nodes_server_handlers'] = _encode_b64_text(server_header.rstrip() + '\n\n' + new_server_body.strip() + '\n')
        errors, auto_changes = _ngenie_validate_generation_candidate(config, merged_config_data, ngenie_code_ack_errors, reference_config_json if reference_generation_mode else None)
        attempts = 1
        repair_rounds_completed = 0
        repair_stop_reason = ''
        repair_issue_attempts = {}
        unchanged_issue_failures = {}
        max_unchanged_issue_failures = 3
        while errors:
            if not errors:
                break
            attempts += 1
            repair_rounds_completed += 1
            handler_syntax_only = _ngenie_handler_syntax_only_errors(errors)
            errors_before_repair = list(errors)
            errors_before_count = len(errors_before_repair)
            round_base_candidate = deepcopy(merged_config_data)
            repair_runtime_failures = []
            round_issue_keys = {key for key in (_ngenie_repair_issue_id(x) for x in errors_before_repair) if key}
            round_issue_rows = {}
            round_issue_fingerprints = {}
            round_repair_attempted_keys = set()
            for _row in errors_before_repair:
                _key = _ngenie_repair_issue_id(_row)
                if not _key:
                    continue
                round_issue_rows[_key] = str(_row)
                round_issue_fingerprints[_key] = _ngenie_repair_issue_fingerprint(_row)

            def _mark_repair_attempt(rows):
                """Count a retry only when an issue is actually sent to an LLM repair.

                Validation-only loops must never advance plateau counters or retry
                numbers.  One issue may target several artifacts in the same round;
                it still counts as one focused repair round.
                """
                for _value in rows or []:
                    _key = _ngenie_repair_issue_id(_value)
                    if not _key or _key in round_repair_attempted_keys:
                        continue
                    round_repair_attempted_keys.add(_key)
                    repair_issue_attempts[_key] = int(repair_issue_attempts.get(_key) or 0) + 1

            def _repair_retry_note(rows):
                counts = []
                for _value in rows or []:
                    _key = _ngenie_repair_issue_id(_value)
                    if _key:
                        counts.append(int(repair_issue_attempts.get(_key) or 1))
                retry_no = max(counts or [1])
                if retry_no <= 1:
                    return ''
                return f'\nIMPORTANT: this requirement is still open after {retry_no - 1} previous focused repair round(s). The CURRENT candidate below is authoritative. Re-read the current implementation and repair only the remaining root cause. Do NOT repeat an earlier edit blindly and do NOT restore an already-fixed old state. If one target layer is already correct, preserve it and change only the layer that is still inconsistent.\n'
            metadata_errors = _ngenie_metadata_repair_errors(errors_before_repair)
            if not handler_syntax_only and metadata_errors:
                _mark_repair_attempt(metadata_errors)
                fix_prompt_patch = ('Mandatory nGenie Code generation contract:\n' + ngenie_code_generation_contract + '\n\n' if ngenie_code_generation_contract else '') + 'The CURRENT generated candidate did NOT validate.\n' + 'Fix ALL currently listed METADATA errors in ONE PATCH and preserve all already-correct metadata.\n' + 'Return ONE JSON object (PATCH) with only: classes, datasets, sections, CommonEvents and _ngenie_code_instruction_ack.\n' + (ngenie_code.instruction_ack_prompt_text() + '\n' if ngenie_code_mode else '') + 'Do not repair Python handler code in this call.\nNo comments, ONLY JSON.\n' + _repair_retry_note(metadata_errors) + '\nMetadata errors:\n- ' + '\n- '.join(metadata_errors) + '\n\nCURRENT COMPLETE candidate metadata (handlers omitted):\n' + json.dumps({k: v for k, v in (merged_config_data or {}).items() if k not in {'nodes_handlers', 'nodes_server_handlers'}}, ensure_ascii=False, indent=2)
                if not isinstance(fix_prompt_patch, str) or not fix_prompt_patch.strip():
                    raise RuntimeError('Metadata repair prompt is unavailable')
                completion_text = call_llm(llm_provider, system_prompt, fix_prompt_patch, debug_stage=f'json_patch_repair_{attempts}')
                llm_patch_data = json.loads(extract_json_from_text(completion_text))
                ngenie_code_ack_errors = []
                if ngenie_code_mode:
                    try:
                        ngenie_code_ack_errors = ngenie_code.validate_instruction_ack(llm_patch_data)
                    except Exception as _ack_error:
                        ngenie_code_ack_errors = [f'nGenie Code instruction ack validation failed: {_ack_error}']
                    try:
                        unavailable = ngenie_code.extract_unavailable_request(llm_patch_data)
                    except Exception:
                        unavailable = None
                    if unavailable:
                        if _ngenie_code_unavailable_is_handler_patch_contract(unavailable):
                            llm_patch_data = _ngenie_code_minimal_ack_patch()
                            ngenie_code_ack_errors = []
                        else:
                            requested = str(unavailable.get('requested_feature') or unavailable.get('requested') or unavailable.get('feature') or 'запрошенная возможность').strip()
                            reason = str(unavailable.get('reason') or unavailable.get('details') or 'В текущих инструкциях/платформе нет такой возможности.').strip()
                            _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, completion_text)
                            assistant_text = 'Такой возможности пока нет: ' + requested + '\n' + reason + '\nЯ записал заявку разработчику; конфигурация не изменялась.'
                            _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'unavailable'}, commit=False)
                            db.session.commit()
                            return jsonify({'status': 'ok', 'message': assistant_text, 'ngenie_code_feature_request': True, 'ngenie_code_request_id': ngenie_code_request_id})
                llm_patch_data_for_merge = ngenie_code.strip_instruction_ack(llm_patch_data) if ngenie_code_mode else llm_patch_data
                merged_config_data = merge_llm_config_into_current_ai(merged_config_data, llm_patch_data_for_merge)
            else:
                llm_patch_data = {}
                ngenie_code_ack_errors = []
            android_errors = _ngenie_android_repair_errors(errors_before_repair)
            server_errors = _ngenie_server_repair_errors(errors_before_repair)
            if android_errors:
                _mark_repair_attempt(android_errors)
                android_code_now = _decode_b64_text(merged_config_data.get('nodes_handlers', ''))
                android_header, android_body = _split_handlers_header_and_body(android_code_now)
                if not android_header:
                    config_url = url_for('get_config', uid=config.uid, _external=True)
                    android_header = ANDROID_IMPORTS_TEMPLATE.format(uid=config.uid, config_url=config_url) + '\n' + NODE_CLASS_CODE_ANDROID.strip() + '\n'
                android_retry_no = max([int(repair_issue_attempts.get(_ngenie_repair_issue_id(x)) or 1) for x in android_errors] or [1])
                android_request = 'Repair the CURRENT COMPLETE Android handler as one coherent artifact. Fix ALL currently listed Android/runtime issues in the complete final BODY. Preserve every already-correct capability and unrelated implementation. Do not redesign metadata.\n' + _repair_retry_note(android_errors) + '\nApproved generation goal: ' + _ngenie_repair_goal_prompt(original_user_prompt) + '\n\nCURRENT Android obligations for this atomic repair transaction:\n- ' + '\n- '.join(android_errors)
                try:
                    new_android_body = _repair_handlers_body_full_ai(provider=llm_provider, user_request=android_request, merged_config_json=merged_config_data, current_header=android_header, current_body=android_body, kind_label='ANDROID', max_attempts=3 if android_retry_no > 1 else 2, require_clean_contract=True)
                    merged_config_data['nodes_handlers'] = _encode_b64_text(android_header.rstrip() + '\n\n' + new_android_body.strip() + '\n')
                except Exception as repair_exc:
                    if repair_exc.__class__.__name__ in {'GenerationCancelled', 'GenerationBudgetExceeded'}:
                        raise
                    repair_runtime_failures.append('ANDROID: ' + str(repair_exc))
            if server_errors:
                _mark_repair_attempt(server_errors)
                server_code_now = _decode_b64_text(merged_config_data.get('nodes_server_handlers', ''))
                server_header, server_body = _split_handlers_header_and_body(server_code_now)
                if not server_header:
                    server_header = NODE_CLASS_CODE.strip() + '\n'
                server_retry_no = max([int(repair_issue_attempts.get(_ngenie_repair_issue_id(x)) or 1) for x in server_errors] or [1])
                server_request = 'Repair the CURRENT COMPLETE server handler as one coherent artifact. Fix ALL currently listed Server/runtime issues in the complete final BODY. Preserve every already-correct capability and unrelated implementation. Do not redesign metadata.\n' + _repair_retry_note(server_errors) + '\nApproved generation goal: ' + _ngenie_repair_goal_prompt(original_user_prompt) + '\n\nCURRENT Server obligations for this atomic repair transaction:\n- ' + '\n- '.join(server_errors)
                try:
                    new_server_body = _repair_handlers_body_full_ai(provider=llm_provider, user_request=server_request, merged_config_json=merged_config_data, current_header=server_header, current_body=server_body, kind_label='SERVER', max_attempts=3 if server_retry_no > 1 else 2, require_clean_contract=True)
                    merged_config_data['nodes_server_handlers'] = _encode_b64_text(server_header.rstrip() + '\n\n' + new_server_body.strip() + '\n')
                except Exception as repair_exc:
                    if repair_exc.__class__.__name__ in {'GenerationCancelled', 'GenerationBudgetExceeded'}:
                        raise
                    repair_runtime_failures.append('SERVER: ' + str(repair_exc))
            errors, auto_changes = _ngenie_validate_generation_candidate(config, merged_config_data, ngenie_code_ack_errors, reference_config_json if reference_generation_mode else None)
            if errors:
                errors_after_count = len(errors)
                after_issue_rows = {}
                after_issue_fingerprints = {}
                for _row in errors:
                    _key = _ngenie_repair_issue_id(_row)
                    if not _key:
                        continue
                    after_issue_rows[_key] = str(_row)
                    after_issue_fingerprints[_key] = _ngenie_repair_issue_fingerprint(_row)
                after_issue_keys = set(after_issue_rows)
                repeated_round_keys = sorted(after_issue_keys & round_issue_keys)
                new_issue_keys = sorted(after_issue_keys - round_issue_keys)
                resolved_issue_keys = sorted(round_issue_keys - after_issue_keys)
                for _key in resolved_issue_keys:
                    unchanged_issue_failures.pop(_key, None)
                partial_issue_keys = []
                unchanged_issue_keys = []
                unrouted_repeated_keys = []
                for _key in repeated_round_keys:
                    if _key not in round_repair_attempted_keys:
                        unrouted_repeated_keys.append(_key)
                        continue
                    _row = after_issue_rows.get(_key, '')
                    _state = _ngenie_repair_issue_state(_row)
                    _before_fp = round_issue_fingerprints.get(_key, '')
                    _after_fp = after_issue_fingerprints.get(_key, '')
                    if _state == 'partial' or (_state != 'unchanged' and _after_fp and (_after_fp != _before_fp)):
                        unchanged_issue_failures[_key] = 0
                        partial_issue_keys.append(_key)
                    else:
                        unchanged_issue_failures[_key] = int(unchanged_issue_failures.get(_key) or 0) + 1
                        unchanged_issue_keys.append(_key)
                if unrouted_repeated_keys:
                    repair_stop_reason = 'repair routing did not select a target for: ' + ', '.join((x.split(':', 1)[-1] for x in unrouted_repeated_keys[:6]))
                    break
                hard_stalled_keys = sorted((_key for _key in unchanged_issue_keys if int(unchanged_issue_failures.get(_key) or 0) >= max_unchanged_issue_failures))
                if not hard_stalled_keys:
                    continue
                stalled_preview = ', '.join((x.split(':', 1)[-1] for x in hard_stalled_keys[:4]))
                repair_stop_reason = f"один и тот же конкретный остаток проблемы не изменился после {max_unchanged_issue_failures} последовательных repair-попыток ({stalled_preview or 'тот же blocker'})"
                break
        if errors:
            error_text = 'AI generation failed validation:\n- ' + '\n- '.join(errors)
            if ngenie_code_mode and _ngenie_code_validation_errors_look_like_missing_feature(errors):
                requested = ' / '.join((str(e) for e in errors[:3]))[:1000]
                reason = 'Валидация показала, что LLM использовала компонент/возможность, которой нет в текущем NodaLogic/UI.'
                _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, '\n'.join(errors))
                assistant_text = 'Похоже, в текущей платформе нет нужного UI-компонента или возможности.\n- ' + '\n- '.join(errors)
                assistant_text += '\nЯ записал заявку разработчику; конфигурация не изменялась.'
                _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'missing_feature_validation', 'errors': errors}, commit=False)
                db.session.commit()
                return jsonify({'status': 'ok', 'message': assistant_text, 'ngenie_code_feature_request': True, 'generation_candidate_saved': False, 'can_resume_generation': False, 'ngenie_code_request_id': ngenie_code_request_id})
            if ngenie_code_mode:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=ngenie_code_request_id, meta={'kind': 'validation_error', 'errors': errors, 'candidate_saved': False}, commit=True)
            return (jsonify({'status': 'error', 'message': error_text, 'generation_candidate_saved': False, 'can_resume_generation': False, 'ngenie_code_request_id': ngenie_code_request_id if ngenie_code_mode else ''}), 400)
        new_config_data = merged_config_data
    except Exception as e:
        error_text = f'An error occurred while requesting LLM or parsing the response.: {e}'
        if locals().get('ngenie_code_mode'):
            try:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=locals().get('ngenie_code_request_id', ''), meta={'kind': 'exception_before_apply', 'candidate_saved': False}, commit=True)
            except Exception:
                pass
        return (jsonify({'status': 'error', 'message': error_text, 'generation_candidate_saved': False, 'can_resume_generation': False, 'ngenie_code_request_id': locals().get('ngenie_code_request_id', '')}), 500)
    try:
        apply_full_config_from_json(config, new_config_data)
        if ngenie_code_mode:
            _ngenie_code_mark_locked(config)
            if write_instruction:
                try:
                    import ngenie_code
                    cfg_after = _ngenie_code_current_json(config) or new_config_data
                    doc_prompt = ngenie_code.build_instruction_prompt(cfg_after, prompt)
                    _release_db_before_external_llm()
                    config.ngenie_code_instruction = ngenie_code.call_llm(ngenie_code.build_system_prompt(request_id=ngenie_code_request_id), doc_prompt, max_tokens=8000, debug_stage='write_instruction_after_generation')
                except Exception as doc_error:
                    current_app.logger.exception('nGenie Code instruction generation failed')
                    config.ngenie_code_instruction = (getattr(config, 'ngenie_code_instruction', '') or '') + ('\n\n> Instruction generation failed: ' + str(doc_error))
        instruction_url = url_for('ngenie_code_document', uid=config.uid, kind='instruction') if ngenie_code_mode and getattr(config, 'ngenie_code_instruction', '') else ''
        if ngenie_code_mode:
            cfg_after_summary = _ngenie_code_current_json(config) or new_config_data
            assistant_message = _ngenie_code_summarize_generation(before_config_json_for_summary, cfg_after_summary, request_id=ngenie_code_request_id, instruction_url=instruction_url)
            _ngenie_code_add_chat_message(config, 'assistant', assistant_message, request_id=ngenie_code_request_id, meta={'kind': 'generation_success'}, commit=False)
        else:
            assistant_message = 'Configuration successfully updated via AI generator'
        db.session.commit()
        return jsonify({'status': 'ok', 'message': assistant_message if ngenie_code_mode else 'Configuration successfully updated via AI generator', 'ngenie_code_locked': bool(getattr(config, 'ngenie_code_locked', False)), 'instruction_url': instruction_url, 'ngenie_code_request_id': ngenie_code_request_id if ngenie_code_mode else ''})
    except Exception as e:
        db.session.rollback()
        error_text = f'Error applying configuration: {e}'
        if ngenie_code_mode:
            try:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=ngenie_code_request_id, meta={'kind': 'apply_error', 'candidate_saved': False}, commit=True)
            except Exception:
                pass
        return (jsonify({'status': 'error', 'message': error_text, 'generation_candidate_saved': False, 'can_resume_generation': False, 'ngenie_code_request_id': ngenie_code_request_id if ngenie_code_mode else ''}), 500)



@_routes.route('/config/<uid>/ngenie-code-chat', methods=['GET', 'POST'])
@login_required
def ngenie_code_chat(uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        role = 'user' if str(data.get('role') or 'user').strip() == 'user' else 'assistant'
        content = str(data.get('content') or '').strip()
        meta = data.get('meta') if isinstance(data.get('meta'), dict) else {}
        if not content and not meta:
            return jsonify({'status': 'error', 'message': 'Empty chat message'}), 400
        try:
            _ngenie_code_add_chat_message(config, role, content, request_id=str(data.get('request_id') or ''), meta=meta, commit=False)
            if role == 'user' and isinstance(meta, dict) and meta.get('kind') == 'question_answers':
                qa = meta.get('question_answers')
                _optional_feature_call(
                    "solutions", "record_question_answers_for_config", config, qa,
                    user=current_user, commit=True,
                )
                if _ngenie_code_bool(meta.get('resume_plan')):
                    plan_payload = _optional_feature_call(
                        "solutions", "run_plan_from_editor", config,
                        user=current_user, question_answers=qa, start_only=False, model_call=call_llm,
                    )
                    if isinstance(plan_payload, dict):
                        ptype = str(plan_payload.get('type') or '').lower()
                        response = {
                            'status': 'ok',
                            'plan_type': ptype,
                            'message': str(plan_payload.get('message') or 'План решения обновлен.'),
                            'messages': _ngenie_code_chat_rows(config),
                        }
                        if ptype == 'questions':
                            response['ngenie_code_questions'] = plan_payload.get('questions') or []
                        elif ptype == 'generate':
                            response['generate_prompt'] = str(plan_payload.get('prompt') or '')
                        db.session.commit()
                        return jsonify(response)
            db.session.commit()
            return jsonify({'status': 'ok', 'messages': _ngenie_code_chat_rows(config)})
        except Exception as e:
            db.session.rollback()
            return jsonify({'status': 'error', 'message': str(e)}), 500
    _optional_feature_call(
        "solutions", "run_plan_from_editor", config, user=current_user,
        start_only=True, model_call=call_llm,
    )
    messages = _ngenie_code_chat_rows(config)
    if not messages:
        fallback = _optional_feature_call('solutions', 'solution_chat_rows_for_config', config, user=current_user, limit=200)
        if isinstance(fallback, list):
            messages = fallback
    pending_question_key = _optional_feature_call(
        'solutions', 'pending_question_key_for_config', config, user=current_user
    ) or ''
    return jsonify({'status': 'ok', 'messages': messages, 'pending_question_key': pending_question_key})



@_routes.route('/config/<uid>/ngenie-code-chat/add', methods=['POST'])
@login_required
def ngenie_code_chat_add(uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    data = request.get_json(silent=True) or {}
    role = 'user' if str(data.get('role') or 'user').strip() == 'user' else 'assistant'
    content = str(data.get('content') or '').strip()
    meta = data.get('meta') if isinstance(data.get('meta'), dict) else {}
    if not content and not meta:
        return jsonify({'status': 'error', 'message': 'Empty chat message'}), 400
    try:
        _ngenie_code_add_chat_message(config, role, content, request_id=str(data.get('request_id') or ''), meta=meta, commit=False)
        if role == 'user' and isinstance(meta, dict) and meta.get('kind') == 'question_answers':
            qa = meta.get('question_answers')
            _optional_feature_call(
                "solutions", "record_question_answers_for_config", config, qa,
                user=current_user, commit=True,
            )
        db.session.commit()
        return jsonify({'status': 'ok', 'messages': _ngenie_code_chat_rows(config)})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@_routes.route('/config/<uid>/ngenie-code-question-answers', methods=['POST'])
@login_required
def ngenie_code_question_answers(uid):
    """Save structured question answers and optionally resume Solutions plan.py.

    The solution workspace sends this endpoint when the user presses the
    explicit «Ответить» button. Partial values stay as a local browser draft;
    resume=true is accepted only when all required fields are present, then the
    backend replays plan.py and continues with the next DSL statement.
    """
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    data = request.get_json(silent=True) or {}
    questions = data.get('questions') if isinstance(data.get('questions'), list) else []
    answers = data.get('answers') if isinstance(data.get('answers'), dict) else {}
    action = str(data.get('action') or 'nothing').strip().lower()
    if action in {'none', 'silent', 'save_only'}:
        action = 'nothing'
    if action not in {'nothing', 'straight', 'if_all'}:
        action = 'nothing'
    resume = _ngenie_code_bool(data.get('resume'))
    if resume:
        try:
            import ngenie_code
            validation_questions = ngenie_code.normalize_questions(questions)
        except Exception:
            validation_questions = questions
        missing = []
        for qi, question in enumerate(validation_questions):
            if not isinstance(question, dict):
                continue
            qid = str(question.get('id') or question.get('key') or f'q_{qi + 1}')
            q_answers = answers.get(qid) if isinstance(answers.get(qid), dict) else {}
            for fi, raw_field in enumerate(question.get('fields') or []):
                field = raw_field if isinstance(raw_field, dict) else {'id': f'field_{fi + 1}', 'caption': str(raw_field or '')}
                if field.get('required') is False:
                    continue
                field_id = str(field.get('id') or f'field_{fi + 1}')
                value = q_answers.get(field_id)
                complete = bool(value) if isinstance(value, list) else (True if isinstance(value, bool) else bool(str(value or '').strip()))
                if not complete:
                    missing.append(str(field.get('caption') or field_id))
        if missing:
            return jsonify({
                'status': 'error',
                'message': 'Заполните обязательные поля: ' + ', '.join(missing[:10]),
                'missing_fields': missing,
            }), 400
    qa = {'questions': questions, 'answers': answers, 'action': action}
    try:
        _optional_feature_call(
            "solutions", "record_question_answers_for_config", config, qa,
            user=current_user, commit=True,
        )
        if resume:
            content = str(data.get('content') or '').strip() or _ngenie_code_format_question_answers_for_chat('', qa)
            _ngenie_code_add_chat_message(
                config,
                'user',
                content,
                request_id=str(data.get('request_id') or ''),
                meta={'kind': 'question_answers', 'question_answers': qa, 'resume_plan': True},
                commit=False,
            )
            plan_payload = _optional_feature_call(
                "solutions", "run_plan_from_editor", config,
                user=current_user, question_answers=qa, start_only=False, model_call=call_llm,
            )
            if isinstance(plan_payload, dict):
                ptype = str(plan_payload.get('type') or '').lower()
                response = {
                    'status': 'ok',
                    'plan_type': ptype,
                    'message': str(plan_payload.get('message') or 'План решения обновлен.'),
                    'answers': answers,
                }
                if ptype == 'questions':
                    response['ngenie_code_questions'] = plan_payload.get('questions') or []
                elif ptype == 'generate':
                    response['generate_prompt'] = str(plan_payload.get('prompt') or '')
                db.session.commit()
                return jsonify(response)
        db.session.commit()
        return jsonify({'status': 'ok', 'message': 'Ответ сохранен.', 'answers': answers})
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('Failed to save nGenie question answers')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@_routes.route('/config/<uid>/ngenie-code-chat/new', methods=['POST'])
@login_required
def ngenie_code_chat_new(uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    try:
        solution = _optional_feature_call('solutions', 'active_solution_for_config', config, user=current_user)
        if solution is not None:
            # A generated-solution session is an audit trail. "New chat" creates
            # a visible boundary but never destroys evidence needed by diagnostics.
            _ngenie_code_add_chat_message(
                config,
                'assistant',
                'Новый этап чата создан. Предыдущая история сохранена в диагностике решения.',
                request_id='',
                meta={'kind': 'new_chat', 'solution_uid': getattr(solution, 'uid', ''), 'history_preserved': True},
                commit=False,
            )
        else:
            db.session.query(NGenieCodeChatMessage).filter(NGenieCodeChatMessage.config_id == config.id).delete(synchronize_session=False)
            _ngenie_code_add_chat_message(config, 'assistant', 'Новый чат создан. История очищена.', request_id='', meta={'kind': 'new_chat'}, commit=False)
        db.session.commit()
        return jsonify({'status': 'ok', 'messages': _ngenie_code_chat_rows(config)})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@_routes.route('/admin/ngenie-code-debug/<path:filename>')
@login_required
def ngenie_code_debug_file(filename):
    if current_user.email != ADMIN_LOGIN:
        abort(403)
    try:
        import ngenie_code
        safe = secure_filename(filename)
        if safe != filename:
            abort(400)
        return send_from_directory(str(ngenie_code.DEBUG_DIR), safe, as_attachment=True, mimetype='application/json')
    except FileNotFoundError:
        abort(404)
    except Exception:
        current_app.logger.exception('nGenie Code debug file download failed')
        abort(404)

@_routes.route('/config/<uid>/ngenie-code-document/<kind>')
@login_required
def ngenie_code_document(uid, kind):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    kind = (kind or '').strip().lower()
    if kind == 'example':
        text_value = getattr(config, 'ngenie_code_example', '') or ''
        title = f"nGenie Code example: {config.name or config.uid}"
    else:
        text_value = getattr(config, 'ngenie_code_instruction', '') or ''
        title = f"nGenie Code instruction: {config.name or config.uid}"
    if not text_value:
        abort(404)
    html = """
<!doctype html><html><head><meta charset="utf-8"><title>{title}</title>
<style>body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;max-width:980px;margin:32px auto;padding:0 18px;line-height:1.45}}pre{{white-space:pre-wrap;background:#f7f7fb;border:1px solid #ddd;border-radius:12px;padding:18px}}</style>
</head><body><p><a href="{back}">← Back to configuration</a></p><h1>{title}</h1><pre>{body}</pre></body></html>
""".format(
        title=str(title).replace('<','&lt;').replace('>','&gt;'),
        back=url_for('edit_config', uid=config.uid, tab='config'),
        body=str(text_value).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;'),
    )
    return html


@_routes.route('/config/<uid>/ngenie-code-write-instruction', methods=['POST'])
@login_required
def ngenie_code_write_instruction(uid):
    if not _ngenie_code_available():
        return jsonify({"status": "error", "message": "nGenie Code module is not available"}), 404
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    data = request.get_json(silent=True) if request.is_json else (request.form or {})
    extra_prompt = (data.get('prompt') or '').strip()
    try:
        import ngenie_code
        request_id = uuid.uuid4().hex
        ngenie_code.set_debug_context(request_id=request_id, user_id=getattr(current_user, 'id', None), user_email=getattr(current_user, 'email', ''), config_uid=getattr(config, 'uid', ''), config_name=getattr(config, 'name', ''), original_prompt=extra_prompt[:4000])
        cfg = _ngenie_code_current_json(config)
        doc_prompt = ngenie_code.build_instruction_prompt(cfg, extra_prompt)
        _release_db_before_external_llm()
        config.ngenie_code_instruction = ngenie_code.call_llm(ngenie_code.build_system_prompt(request_id=request_id), doc_prompt, max_tokens=8000, debug_stage="write_instruction_manual")
        _ngenie_code_mark_locked(config)
        assistant_text = "Инструкция создана и прикреплена к конфигурации."
        _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=request_id, meta={'kind': 'write_instruction_manual'}, commit=False)
        db.session.commit()
        return jsonify({
            "status": "ok",
            "message": assistant_text,
            "url": url_for('ngenie_code_document', uid=config.uid, kind='instruction')
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@_routes.route('/config/<uid>/ngenie-code-generate-example', methods=['POST'])
@login_required
def ngenie_code_generate_example(uid):
    if not _ngenie_code_available():
        return jsonify({"status": "error", "message": "nGenie Code module is not available"}), 404
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not config:
        abort(404)
    data = request.get_json(silent=True) if request.is_json else (request.form or {})
    extra_prompt = (data.get('prompt') or '').strip()
    try:
        import ngenie_code
        request_id = uuid.uuid4().hex
        ngenie_code.set_debug_context(request_id=request_id, user_id=getattr(current_user, 'id', None), user_email=getattr(current_user, 'email', ''), config_uid=getattr(config, 'uid', ''), config_name=getattr(config, 'name', ''), original_prompt=extra_prompt[:4000])
        cfg = _ngenie_code_current_json(config)
        example_prompt = ngenie_code.build_example_prompt(cfg, extra_prompt)
        _release_db_before_external_llm()
        config.ngenie_code_example = ngenie_code.call_llm(ngenie_code.build_system_prompt(request_id=request_id), example_prompt, max_tokens=8000, debug_stage="generate_example_manual")
        _ngenie_code_mark_locked(config)
        assistant_text = "Пример создан и прикреплен к конфигурации."
        _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=request_id, meta={'kind': 'generate_example_manual'}, commit=False)
        db.session.commit()
        return jsonify({
            "status": "ok",
            "message": assistant_text,
            "url": url_for('ngenie_code_document', uid=config.uid, kind='example')
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


@_routes.route('/config/<uid>/ai-generate-layout', methods=['POST'])
@login_required
def ai_generate_layout(uid):
    """Generate ONLY a UI layout JSON (2D array) for copy/paste.
    Does NOT apply anything to the configuration.
    """
    config = db.session.execute(
        select(Configuration).where(
            Configuration.uid == uid,
            Configuration.user_id == current_user.id
        )
    ).scalar_one_or_none()

    if not config:
        abort(404)

    data = request.get_json() or {}
    prompt = (data.get('prompt') or '').strip()
    llm_provider = (data.get('llm') or 'deepseek').strip().lower()

    if not prompt:
        return jsonify({"status": "error", "message": "Empty prompt"}), 400

    try:
        # system prompt 
        llm_url = "https://raw.githubusercontent.com/dvdocumentation/nodalogic/refs/heads/main/LLM.txt"
        r = requests.get(llm_url, timeout=10)
        if r.status_code == 200:
            system_prompt = r.text
        else:
            system_prompt = "You are the NodaLogic configuration generation assistant. Always return valid JSON without any explanations."

        
        current_config_json = json.loads(get_config(config.uid))

        allowed = sorted(ALLOWED_UI_TYPES_AI)
        allowed_inputs = sorted(ALLOWED_INPUT_TYPES_AI)

        user_prompt = (
            "Generate ONLY a UI layout JSON for NodaLogic.\n"
            "Return ONLY a JSON ARRAY, no comments, no markdown.\n\n"
            "Format requirements:\n"
            "- Root is a list of ROWS\n"
            "- Each row is a list of element objects (dict)\n"
            "- Each element MUST have a CASE-SENSITIVE field: type\n"
            "- If you use container types (VerticalLayout/HorizontalLayout/VerticalScroll/HorizontalScroll/Card), put nested layout into value as a list of rows\n"
            "- If you use Table, put nested layout into layout as a list of rows\n\n"
            f"Allowed types: {allowed}\n"
            f"Allowed Input.input_type (if present): {allowed_inputs}\n\n"
            "User request:\n"
            f"{prompt}\n\n"
           # "Current configuration (for names/reference; do not return it):\n"
           # f"{json.dumps(current_config_json, ensure_ascii=False, indent=2)}"
        )

        completion_text = call_llm(llm_provider, system_prompt, user_prompt)
        json_arr_str = extract_json_array_from_text(completion_text)
        layout = json.loads(json_arr_str)

        # Validate basic structure + allowed UI types
        errors = []
        if not isinstance(layout, list):
            errors.append("layout root must be a list")
        else:
            for i, row in enumerate(layout):
                if not isinstance(row, list):
                    errors.append(f"layout[{i}] must be a list (row)")

        errors.extend(validate_layout_types_ai(layout, where="layout"))

        if errors:
            return jsonify({
                "status": "error",
                "message": "Generated layout failed validation:\n- " + "\n- ".join(errors),
            }), 400

        return jsonify({
            "status": "ok",
            "layout": layout,
            "layout_pretty": json.dumps(layout, ensure_ascii=False, indent=2),
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"An error occurred while generating layout: {e}",
        }), 500

def get_user_local_time():
    return datetime.now(g.user_timezone)

@_routes.route('/get-method-body')
@login_required
def get_method_body():
    class_id = request.args.get('class_id')
    method_name = request.args.get('method_name')
    engine = request.args.get('engine')
    
    class_obj = db.session.get(ConfigClass, class_id)
    if not class_obj or class_obj.config.user_id != current_user.id:
        abort(404)
    
    
    if engine == 'server_python' and class_obj.config.nodes_server_handlers:
        try:
            module_code = base64.b64decode(class_obj.config.nodes_server_handlers).decode('utf-8')
            body = extract_method_body_from_code(module_code, class_obj.name, method_name)
            
            if body is None:
               
                method_obj = next((m for m in class_obj.methods 
                                 if m.code == method_name and m.engine == 'server_python'), None)
                if method_obj:
                    
                    return jsonify({'body': '', 'warning': 'Method not found in code'})
                else:
                    return jsonify({'body': '', 'error': 'The method does not exist'})
            
            return jsonify({'body': body})
        except Exception as e:
            return jsonify({'body': '', 'error': str(e)})
    
   
    elif engine == 'android_python' and class_obj.config.nodes_handlers:
        try:
            module_code = base64.b64decode(class_obj.config.nodes_handlers).decode('utf-8')
            body = extract_method_body_from_code(module_code, class_obj.name, method_name)
            
            if body is None:
               
                method_obj = next((m for m in class_obj.methods 
                                 if m.code == method_name and m.engine == 'android_python'), None)
                if method_obj:
                    
                    return jsonify({'body': '', 'warning': 'Method not found in code'})
                else:
                    return jsonify({'body': '', 'error': 'The method does not exist'})
            
            return jsonify({'body': body})
        except Exception as e:
            return jsonify({'body': '', 'error': str(e)})
    
    return jsonify({'body': ''})

def ensure_class_stub_in_module(module_code: str, class_name: str) -> str:
    """
    Ensures class stub exists in handlers module in the form:

    class MyClass(Node):

        def __init__(self, node_id=None, config_uid=None):
            super().__init__(node_id, config_uid)
    """

    module_code = module_code or ""

    
    class_pattern = re.compile(
        rf'^\s*class\s+{re.escape(class_name)}\s*\(',
        re.MULTILINE
    )
    if class_pattern.search(module_code):
        return module_code

    
    if not module_code.strip():
        module_code = NODE_CLASS_CODE.strip() + "\n"

    module = module_code.rstrip() + "\n\n"

    
    stub = (
        f"class {class_name}(Node):\n"
        f"    \n"
        f"    def __init__(self, node_id=None, config_uid=None):\n"
        f"        super().__init__(node_id, config_uid)\n"
    )

    return module + stub + "\n"

@_routes.route('/save-method/<int:method_id>', methods=['POST'])
@login_required
def save_method(method_id):
    method = db.session.get(ClassMethod, method_id)
    if not method or method.class_obj.config.user_id != current_user.id:
        abort(404)
    
    method.name = request.form['name']
    method.source = request.form['source']
    method.engine = request.form['engine']
    method.code = request.form['name']
    
   
    function_body = request.form['function_body']
    
    try:
        
        if method.engine == 'server_python':
            current_module = ""
            if method.class_obj.config.nodes_server_handlers:
                current_module = base64.b64decode(
                    method.class_obj.config.nodes_server_handlers
                ).decode('utf-8')

            
            if not current_module.strip():
                current_module = NODE_CLASS_CODE.strip() + "\n"

           
            current_module = ensure_class_stub_in_module(
                current_module,
                method.class_obj.name
            )

           
            new_module = add_method_to_class(
                current_module,
                method.class_obj.name,
                method.name,
                function_body
            )

            if new_module is None:
                return redirect(url_for('edit_class', class_id=method.class_id, _anchor='handlers-refresh'))

            method.class_obj.config.nodes_server_handlers = base64.b64encode(
                new_module.encode('utf-8')
            ).decode('utf-8')

            
            handlers_dir = os.path.join('Handlers', method.class_obj.config.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                f.write(new_module)
        
        
        elif method.engine == 'android_python':
            current_module = ""
            if method.class_obj.config.nodes_handlers:
                current_module = base64.b64decode(method.class_obj.config.nodes_handlers).decode('utf-8')
            
            
            new_module = add_method_to_class(current_module, method.class_obj.name, method.name, function_body)
            
            if new_module is None:  
                return redirect(url_for('edit_class', class_id=method.class_id, _anchor='handlers-refresh'))
            
            
            method.class_obj.config.nodes_handlers = base64.b64encode(new_module.encode('utf-8')).decode('utf-8')
        
        method.class_obj.config.update_last_modified()
        db.session.commit()
        flash(_('Method saved successfully'), 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(_('Save error: ')+ str(e), 'danger')
    
    return redirect(url_for('edit_class', class_id=method.class_id, _anchor='handlers-refresh'))

@_routes.route('/update-config/<uid>', methods=['POST'])
@login_required
def update_config(uid):
    config = Configuration.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    if _config_is_ngenie_code_locked(config):
        abort(403, description=_ngenie_code_forbid_message())
    
    if 'name' in request.form:
        config.name = request.form['name']
    if 'version' in request.form:
        config.version = request.form['version']
    if 'server_name' in request.form: 
        config.server_name = request.form['server_name']    
    if 'ngenie_prompt' in request.form:
        config.ngenie_prompt = request.form.get('ngenie_prompt') or ''
    if (
        _current_user_has_admin_login()
        and 'demo_product_present' in request.form
        and hasattr(config, 'demo_product')
    ):
        config.demo_product = 'demo_product' in request.form
    if 'profile_templates_json' in request.form and hasattr(config, 'profile_templates'):
        raw_profile_templates = request.form.get('profile_templates_json') or '[]'
        try:
            config.profile_templates = json.loads(raw_profile_templates) if raw_profile_templates.strip() else []
        except Exception as e:
            flash(_('Profile templates JSON error') + ': ' + str(e), 'error')
            return redirect(url_for('edit_config', uid=uid, tab=request.form.get("active_tab", "profile-templates")))
    

    config.last_modified = get_user_local_time()
    db.session.commit()
    if 'profile_templates_json' in request.form and hasattr(config, 'profile_templates'):
        _materialize_profile_templates_for_config(config)
    
    flash(_('Configuration updated'), 'success')
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=uid,tab=active_tab))

@_routes.route('/update-handlers-code/<uid>', methods=['POST'])
@login_required
def update_handlers_code(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)
    
    handlers_code = request.form.get('handlers_code', '')
    
    if not handlers_code:
        flash(_('Empty handler code received'), 'danger')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=uid,tab=active_tab))
    
    try:
        
        is_valid, error = validate_python_syntax(handlers_code)
        if not is_valid:
            
            flash(_('Python syntax error')+error, 'danger')
            active_tab = request.form.get("active_tab", "config")
            
            return jsonify({"status": "error", "message": _('Python syntax error')+error })
        
        android_imports = ANDROID_IMPORTS_TEMPLATE.format(
            uid=config.uid, 
            config_url=url_for('get_config', uid=config.uid, _external=True)
        )
        
        
        if 'from nodes import Node' not in handlers_code:
            
            handlers_code = android_imports + NODE_CLASS_CODE_ANDROID + '\n' + handlers_code
            is_valid, error = validate_python_syntax(handlers_code)
            if not is_valid:
                flash(_('Syntax error after adding imports:')+error, 'danger')
                active_tab = request.form.get("active_tab", "config")
                #return redirect(url_for('edit_config', uid=uid, tab=active_tab))
                return jsonify({"status": "error", "message": _('Syntax error after adding imports:')+error})


        encoded = base64.b64encode(handlers_code.encode('utf-8')).decode('utf-8')
        config.nodes_handlers = encoded
        config.update_last_modified()
        db.session.commit()
        
        
        sync_classes_from_android_handlers(config)
        sync_methods_from_code(config)
        #from flask import session
        #session['_flashes'] = []
        session.modified = True
        flash(_('Code saved successfully'), 'success')
    except Exception as e:
        db.session.rollback()
        #print(f"Error saving code: {str(e)}")
        flash(_('Save error:') +str(e), 'error')
        return redirect(url_for("edit_config", uid=config.uid, tab="handlers", subtab="code"))
        
    active_tab = request.form.get("active_tab", "config")
    #return redirect(url_for('edit_config', uid=uid, tab=active_tab))
    return jsonify({"status": "ok"})

@_routes.route('/get-dataset-json')
@login_required
def get_dataset_json():
    dataset_id = request.args.get('dataset_id')
    dataset = db.session.get(Dataset, dataset_id)
    
    if not dataset or dataset.config.user_id != current_user.id:
        abort(404)
    
    return jsonify({
        'name': dataset.name,
        'hash_indexes': dataset.hash_indexes,
        'text_indexes': dataset.text_indexes,
        'view_template': dataset.view_template,
        'autoload': dataset.autoload
    })

@_routes.route('/add-dataset/<config_uid>', methods=['POST'])
@login_required
def add_dataset(config_uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)
    
    name = request.form.get('name')
    hash_indexes = request.form.get('hash_indexes', '')
    text_indexes = request.form.get('text_indexes', '')
    view_template = request.form.get('view_template', '')
    autoload = 'autoload' in request.form  # Check if checkbox was checked
    
    if name:
        new_dataset = Dataset(
            name=name,
            hash_indexes=hash_indexes,
            text_indexes=text_indexes,
            view_template=view_template,
            autoload=autoload,
            config_id=config.id
        )
        db.session.add(new_dataset)
        db.session.commit()
    
    return jsonify({
            "status": "success",
            "message": "Dataset created",
            "dataset": {
                "id": new_dataset.id,
                "name": new_dataset.name
            }
        })

@_routes.route('/get-section-json')
@login_required
def get_section_json():
    section_id = request.args.get('section_id')
    section = db.session.get(ConfigSection, section_id)
    
    if not section or section.config.user_id != current_user.id:
        abort(404)
    
    return jsonify({
        'id': section.id,
        'code': section.code,
        'name': section.name,
        'commands': section.commands,
        'hide_mobile_client': bool(getattr(section, 'hide_mobile_client', False)),
        'hide_web_client': bool(getattr(section, 'hide_web_client', False))
    })

@_routes.route('/edit-dataset/<dataset_id>', methods=['GET', 'POST'])
@login_required
def edit_dataset(dataset_id):
    dataset = db.session.get(Dataset, dataset_id)
    if not dataset or dataset.config.user_id != current_user.id:
        abort(404)

    if request.method == 'POST':
        dataset.name = request.form.get('name')
        dataset.hash_indexes = request.form.get('hash_indexes', '')
        dataset.text_indexes = request.form.get('text_indexes', '')
        dataset.view_template = request.form.get('view_template', '')
        dataset.autoload = 'autoload' in request.form
        db.session.commit()
        flash(_('Dataset updated successfully'), 'success')
        #active_tab = request.form.get("active_tab", "datasets")
        active_tab = "datasets"
        return redirect(url_for('edit_config', uid=dataset.config.uid,tab=active_tab))

    return render_template('edit_dataset.html', dataset=dataset)

@_routes.route('/update-dataset/<dataset_id>', methods=['POST'])
@login_required
def update_dataset(dataset_id):
    dataset = db.session.get(Dataset, dataset_id)
    if not dataset or dataset.config.user_id != current_user.id:
        abort(404)

    # Getting the active tab from the form
    active_tab = request.form.get('active_tab', 'datasets')
    
    dataset.name = request.form.get('name')
    dataset.hash_indexes = request.form.get('hash_indexes', '')
    dataset.text_indexes = request.form.get('text_indexes', '')
    dataset.view_template = request.form.get('view_template', '')
    dataset.autoload = 'autoload' in request.form
    db.session.commit()

    # Returning JSON with the URL for redirection
    return jsonify({
        "status": "success",
        "message": "Dataset updated",
        "redirect_url": url_for('edit_config', uid=dataset.config.uid, tab=active_tab),
        "dataset": {
            "id": dataset.id,
            "name": dataset.name
        }
    })

@_routes.route('/delete-dataset/<dataset_id>')
@login_required
def delete_dataset(dataset_id):
    dataset = db.session.get(Dataset, dataset_id)
    if not dataset or dataset.config.user_id != current_user.id:
        abort(404)
    
    config_uid = dataset.config.uid
    db.session.delete(dataset)
    db.session.commit()
    #active_tab = request.form.get("active_tab", "datasets")
    active_tab = "datasets"
    return redirect(url_for('edit_config', uid=config_uid,tab=active_tab))

@_routes.route('/add-section/<config_uid>', methods=['POST'])
@login_required
def add_section(config_uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).first()

    
    if not config:
        abort(404)
    
    code = request.form.get('code')
    name = request.form.get('name')
    commands = request.form.get('commands', '')
    hide_mobile_client = 'hide_mobile_client' in request.form
    hide_web_client = 'hide_web_client' in request.form
    
    if code and name:
        new_section = ConfigSection(
            code=code,
            name=name,
            commands=commands,
            hide_mobile_client=hide_mobile_client,
            hide_web_client=hide_web_client,
            config_id=config.id
        )
        db.session.add(new_section)
        config.last_modified = datetime.now(timezone.utc)
        db.session.commit()
        return jsonify({"status": "success"})
    
    return jsonify({"status": "error", "message": "No code or name specified"}), 400

@_routes.route('/update-section/<section_id>', methods=['POST'])
@login_required
def update_section(section_id):
    section = db.session.get(ConfigSection, section_id)
    if not section or section.config.user_id != current_user.id:
        abort(404)
    
    section.code = request.form.get('code')
    section.name = request.form.get('name')
    section.commands = request.form.get('commands', '')
    section.hide_mobile_client = 'hide_mobile_client' in request.form
    section.hide_web_client = 'hide_web_client' in request.form
    section.config.last_modified = datetime.now(timezone.utc)
    db.session.commit()
    
    return jsonify({"status": "success"})

@_routes.route('/delete-section/<section_id>')
@login_required
def delete_section(section_id):
    section = db.session.get(ConfigSection, section_id)
    if not section or section.config.user_id != current_user.id:
        abort(404)
    
    config_uid = section.config.uid
    section.config.last_modified = datetime.now(timezone.utc)
    db.session.delete(section)
    db.session.commit()
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=config_uid,tab =active_tab))

@_routes.route('/debug-room/<room_uid>')
@login_required
def debug_room(room_uid):
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first_or_404()
    
    #ws_url = f"wss://{request.host}/ws?room={room.uid}"
    ws_scheme = get_ws_scheme()
    ws_url = f"{ws_scheme}://{request.host}/ws?room={room.uid}"
    qr_img = generate_qr_code(ws_url)
    
    return render_template('debug_room.html', 
                         room=room,
                         ws_url=ws_url,
                         qr_img=qr_img)

@_routes.route('/create-debug-room', methods=['POST'])
@login_required
def create_debug_room():
    name = request.form.get('name', 'Debug room')
    new_room = Room(
        name=name,
        user_id=current_user.id
    )
    db.session.add(new_room)
    db.session.commit()
    return redirect(url_for('debug_room', room_uid=new_room.uid))

def sync_classes_from_server_handlers(config):
    """Synchronizes classes from server handlers with the database"""
    if not config.nodes_server_handlers:
        return
    
    try:
        module_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
        tree = ast.parse(module_code)
        
        # We are looking for all classes that inherit from Node
        node_classes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if it inherits from Node
                for base in node.bases:
                    if (isinstance(base, ast.Name) and base.id == 'Node') or \
                       (isinstance(base, ast.Attribute) and base.attr == 'Node'):
                        # Exclude the Node class itself
                        if node.name != 'Node':
                            node_classes.append(node.name)
                        break
        
        # Synchronize with the database
        existing_classes = {c.name: c for c in config.classes}
        
        for class_name in node_classes:
            if class_name not in existing_classes:
                # Create a new class in the database
                new_class = ConfigClass(
                    name=class_name,
                    display_name=class_name,
                    config_id=config.id,
                    class_type='custom_process',
                    section_code='server'
                )
                db.session.add(new_class)
                #print(f"Added new class from code: {class_name}")
        
        # We remove only server classes that are not in the code
        for class_name, class_obj in existing_classes.items():
            if (class_name not in node_classes and 
                class_obj.section_code == 'server' and
                class_obj.name != 'Node'):
                db.session.delete(class_obj)
                print(f"Removed class not in code: {class_name}")
        
        db.session.commit()
        
    except Exception as e:
        print(f"Error syncing classes from server handlers: {str(e)}")

@_routes.route('/config/<uid>/upload-server-handlers', methods=['POST'])
@login_required
def upload_server_handlers(uid):
    config = Configuration.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    
    upload_type = request.form.get('upload_type')
    handlers_code = ''
    
    if upload_type == 'file':
        file = request.files['python_file']
        if file and file.filename.endswith('.py'):
            handlers_code = file.read().decode('utf-8')
    
    elif upload_type == 'github':
        github_url = request.form.get('github_url')
        try:
            response = requests.get(github_url)
            response.raise_for_status()
            handlers_code = response.text
        except Exception as e:
            flash(_('GitHub load error:')+str(e), 'error')
            active_tab = request.form.get("active_tab", "config")
            return redirect(url_for('edit_config', uid=uid, tab=active_tab))
    
    
    config.nodes_server_handlers = base64.b64encode(handlers_code.encode('utf-8')).decode('utf-8')
    db.session.commit()
    
    
    sync_classes_from_server_handlers(config)
    sync_methods_from_code(config)
    
    
    handlers_dir = os.path.join('Handlers', config.uid)
    os.makedirs(handlers_dir, exist_ok=True)
    handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
    with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
        f.write(handlers_code)
    
    flash(_('Server handlers loaded successfully'), 'success')
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=uid, tab=active_tab))

@_routes.route('/config/<uid>/download-server-handlers')
@login_required
def download_server_handlers(uid):
    config = Configuration.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    if _config_is_ngenie_code_locked(config):
        abort(403, description=_ngenie_code_forbid_message())
    
    if not config.nodes_server_handlers:
        flash(_('No server handlers available for download'), 'error')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=uid,tab=active_tab))
    
    handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
    
    response = make_response(handlers_code)
    response.headers['Content-Type'] = 'text/x-python'
    response.headers['Content-Disposition'] = f'attachment; filename=server_handlers_{config.uid}.py'
    
    return response

@_routes.route('/config/<uid>/clear-server-handlers', methods=['POST'])
@login_required
def clear_server_handlers(uid):
    config = Configuration.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    
    config.nodes_server_handlers = None
    db.session.commit()
    
    
    handlers_file_path = os.path.join('Handlers', config.uid, 'handlers.py')
    if os.path.exists(handlers_file_path):
        os.remove(handlers_file_path)
    
    flash(_('Server handlers deleted'), 'success')
    active_tab = request.form.get("active_tab", "config")
    return redirect(url_for('edit_config', uid=uid, tab=active_tab))

@_routes.route('/update-server-handlers-code/<uid>', methods=['POST'])
@login_required
def update_server_handlers_code(uid):
    config = db.session.scalars(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    ).first()
    
    if not config:
        abort(404)
    
    handlers_code = request.form.get('handlers_code', '')
    
    if not handlers_code:
        flash(_('Empty server handler code received'), 'danger')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=uid, tab=active_tab))
    
    try:
        
        is_valid, error = validate_python_syntax(handlers_code)
        if not is_valid:
            flash(_('Python syntax error')+error, 'danger')
            active_tab = request.form.get("active_tab", "config")
            return jsonify({"status": "error", "message": _('Python syntax error')+error })

        

        
        encoded = base64.b64encode(handlers_code.encode('utf-8')).decode('utf-8')
        config.nodes_server_handlers = encoded
        config.update_last_modified()
        db.session.commit()
        
        
        handlers_dir = os.path.join('Handlers', config.uid)
        os.makedirs(handlers_dir, exist_ok=True)
        
        handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
        with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
            f.write(handlers_code)
        
        
        sync_classes_from_server_handlers(config)
        sync_methods_from_code(config)
        
        session.modified = True
        flash(_('Server handler code saved successfully'), 'success')
    except Exception as e:
        db.session.rollback()
        #print(f"Error saving server handlers code: {str(e)}")
        flash(_('Server handler save error:')+str(e), 'error')
        return redirect(url_for("edit_config", uid=config.uid, tab="handlers-server", subtab="code"))
        
    active_tab = request.form.get("active_tab", "config")
    return jsonify({"status": "ok"})

@_routes.route('/config/<config_uid>/servers/create', methods=['POST'])
@login_required
def create_server(config_uid):
    config = Configuration.query.filter_by(uid=config_uid, user_id=current_user.id).first_or_404()
    alias = request.form['alias']
    url = request.form['url']
    existing_count = Server.query.filter_by(config_id=config.id).count()
    is_default = ('is_default' in request.form) or existing_count == 0

    if is_default:
        Server.query.filter_by(config_id=config.id).update({"is_default": False})

    new_server = Server(alias=alias, url=url, config_id=config.id, is_default=is_default)
    db.session.add(new_server)
    db.session.commit()
    flash(_("Server added"), "success")
    return redirect(url_for('edit_config', uid=config_uid, tab="servers"))

@_routes.route('/config/servers/<int:server_id>/delete')
@login_required
def delete_server(server_id):
    server = Server.query.join(Configuration).filter(
        Server.id == server_id, Configuration.user_id == current_user.id
    ).first_or_404()
    config_uid = server.config.uid
    config_id = server.config_id
    was_default = bool(server.is_default)
    db.session.delete(server)
    db.session.flush()
    if was_default:
        replacement = Server.query.filter_by(config_id=config_id).order_by(Server.id.asc()).first()
        if replacement:
            replacement.is_default = True
    db.session.commit()
    flash(_("Server deleted"), "success")
    return redirect(url_for('edit_config', uid=config_uid, tab="servers"))

@_routes.route('/config/servers/<int:server_id>/update', methods=['POST'])
@login_required
def update_server(server_id):
    server = Server.query.join(Configuration).filter(
        Server.id == server_id, Configuration.user_id == current_user.id
    ).first_or_404()

    server.alias = request.form['alias']
    server.url = request.form['url']
    make_default = 'is_default' in request.form
    was_default = bool(server.is_default)
    others = Server.query.filter(Server.config_id == server.config_id, Server.id != server.id).order_by(Server.id.asc()).all()

    if make_default:
        for other in others:
            other.is_default = False
        server.is_default = True
    elif was_default:
        if others:
            server.is_default = False
            others[0].is_default = True
        else:
            server.is_default = True
    elif not any(bool(x.is_default) for x in others):
        server.is_default = True
    else:
        server.is_default = False

    db.session.commit()
    flash(_("Server updated"), "success")
    return redirect(url_for('edit_config', uid=server.config.uid, tab="servers"))

@_routes.route('/config/<config_uid>/rooms/create', methods=['POST'])
@login_required
def create_room_alias(config_uid):
    config = Configuration.query.filter_by(uid=config_uid, user_id=current_user.id).first_or_404()
    alias = (request.form.get('alias') or '').strip()
    room_uid = (request.form.get('room_uid') or '').strip()
    if not alias or not room_uid:
        flash('Alias and room are required', 'danger')
        return redirect(url_for('edit_config', uid=config_uid, tab='rooms'))

    # Validate room exists and belongs to user
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first()
    if not room:
        flash('Room not found', 'danger')
        return redirect(url_for('edit_config', uid=config_uid, tab='rooms'))

    # Upsert-ish: if alias exists -> update mapping
    existing = RoomAlias.query.filter_by(config_id=config.id, alias=alias).first()
    if existing:
        existing.room_uid = room_uid
    else:
        db.session.add(RoomAlias(alias=alias, room_uid=room_uid, config_id=config.id))
    db.session.commit()
    flash('Room alias saved', 'success')
    return redirect(url_for('edit_config', uid=config_uid, tab='rooms'))

@_routes.route('/config/rooms/<int:alias_id>/update', methods=['POST'])
@login_required
def update_room_alias(alias_id):
    ra = RoomAlias.query.join(Configuration).filter(
        RoomAlias.id == alias_id,
        Configuration.user_id == current_user.id
    ).first_or_404()

    alias = (request.form.get('alias') or '').strip()
    room_uid = (request.form.get('room_uid') or '').strip()
    if not alias or not room_uid:
        flash('Alias and room are required', 'danger')
        return redirect(url_for('edit_config', uid=ra.config.uid, tab='rooms'))

    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first()
    if not room:
        flash('Room not found', 'danger')
        return redirect(url_for('edit_config', uid=ra.config.uid, tab='rooms'))

    ra.alias = alias
    ra.room_uid = room_uid
    db.session.commit()
    flash('Room alias updated', 'success')
    return redirect(url_for('edit_config', uid=ra.config.uid, tab='rooms'))

@_routes.route('/config/rooms/<int:alias_id>/delete')
@login_required
def delete_room_alias(alias_id):
    ra = RoomAlias.query.join(Configuration).filter(
        RoomAlias.id == alias_id,
        Configuration.user_id == current_user.id
    ).first_or_404()
    cfg_uid = ra.config.uid
    db.session.delete(ra)
    db.session.commit()
    flash('Room alias deleted', 'success')
    return redirect(url_for('edit_config', uid=cfg_uid, tab='rooms'))


# -----------------------------------------------------------------------------
# Designer/editor UI, templates, users, rooms and contracts routes moved from app.py
# -----------------------------------------------------------------------------

DEEPSEEK_API_URL = 'https://api.deepseek.com/v1/chat/completions'

LMSTUDIO_API_URL = os.environ.get("LMSTUDIO_API_URL", "http://127.0.0.1:1234/v1/chat/completions")

LMSTUDIO_MODEL = os.environ.get("LMSTUDIO_MODEL", "local-model")

LMSTUDIO_API_KEY = os.environ.get("LMSTUDIO_API_KEY", "")

NODE_CLASS_CODE = '''
from nodes import Node, CallSwarm, message, Dialog, to_uid, from_uid, CloseNode, DataSets, convertBase64ArrayToFilePaths,convertImageFilesToBase64Array,getBase64FromImageFile,saveBase64ToFile, getByIndex, findByIndex, getByGlobalIndex, findByGlobalIndex, sendTextMessage, sendImageMessage, sendTextToNodeDiscussion, sendImageToNodeDiscussion, downloadJsonCached, dispatch_json_node_event, dispatch_downloaded_node_event, banner, banner_html, banner_layout, close_banner, ngenie, node_view, ngenie_nodes, ngenie_data, ngenie_rows, ngenie_ref_view
'''

NODE_CLASS_CODE_ANDROID = '''
from nodes import Node
'''

ANDROID_IMPORTS_TEMPLATE = '''from nodesclient import RefreshTab,SetTitle,CloseNode,RunGPS,StopGPS,UpdateView,Dialog,ScanBarcode,GetLocation,AddTimer,StopTimer,ShowProgressButton,HideProgressButton,ShowProgressGlobal,HideProgressGlobal,Controls,SetCover,getBase64FromImageFile,convertImageFilesToBase64Array,saveBase64ToFile,convertBase64ArrayToFilePaths,UpdateMediaGallery
from android import *
from nodes import * #NewNode, DeleteNode, GetAllNodes, GetNode, GetAllNodesStr, GetRemoteClass, CreateDataSet, GetDataSet, DeleteDataSet,to_uid, from_uid, getByIndex, findByIndex, getByGlobalIndex, findByGlobalIndex, sendTextMessage, sendImageMessage
from com.dv.noda import DataSet
from com.dv.noda import DataSets
from com.dv.noda import SimpleUtilites as su
from datasets import GetDataSetData

# Configuration constants
current_module_name="{uid}"
current_configuration_url="{config_url}"
_data_dir = su.get_data_dir(current_module_name)
_downloads_dir = su.get_downloads_dir(current_module_name)

'''


def _rewrite_android_handlers_instance_refs_code(code: str, config_uid: str, config_url: str) -> str:
    """Keep imported Android handlers portable between configuration instances."""
    code = code or ""
    android_imports = ANDROID_IMPORTS_TEMPLATE.format(uid=config_uid, config_url=config_url)

    if not code.strip():
        code = android_imports + NODE_CLASS_CODE_ANDROID + "\n"
    elif "current_module_name" not in code or "current_configuration_url" not in code:
        # Imported body from another source: add the runtime Android header once.
        if "from nodes import Node" not in code:
            code = android_imports + NODE_CLASS_CODE_ANDROID + "\n" + code
        elif "from com.dv.noda import SimpleUtilites as su" not in code:
            code = android_imports + "\n" + code

    def repl_const(src: str, name: str, value: str) -> str:
        line = f'{name}="{value}"'
        pattern = rf'(?m)^\s*{re.escape(name)}\s*=\s*([\"\']).*?\1\s*$'
        if re.search(pattern, src):
            return re.sub(pattern, line, src)
        return line + "\n" + src

    code = repl_const(code, "current_module_name", config_uid)
    code = repl_const(code, "current_configuration_url", config_url)

    # These paths must follow the current instance uid as well.
    if "_data_dir = su.get_data_dir(current_module_name)" not in code:
        code = re.sub(r'(?m)^\s*_data_dir\s*=.*$', '_data_dir = su.get_data_dir(current_module_name)', code)
    if "_downloads_dir = su.get_downloads_dir(current_module_name)" not in code:
        code = re.sub(r'(?m)^\s*_downloads_dir\s*=.*$', '_downloads_dir = su.get_downloads_dir(current_module_name)', code)
    return code


def _rewrite_android_handlers_instance_refs_b64(encoded: str, config_uid: str, config_url: str) -> str:
    try:
        code = base64.b64decode((encoded or "").encode("utf-8")).decode("utf-8", errors="replace") if encoded else ""
    except Exception:
        code = ""
    code = _rewrite_android_handlers_instance_refs_code(code, config_uid, config_url)
    return base64.b64encode(code.encode("utf-8")).decode("utf-8")

UI_COMPONENT_TEMPLATES = OrderedDict([
    ('Text', '{"type":"Text","value":"my text"}'),
    ('Text(tag)', '{"type":"Text","value":"my text","radius":10,"background":"#F54927"}'),
    ('Picture', '{"type":"Picture","value":"filename/path"}'),
    ('ImageSlider', '{"type":"ImageSlider","value":"array of filename/path"}'),
    ('Gauge', '{"type":"gauge","id":"load_gauge","caption":"Load","min":0,"max":100,"value":"@load_percent","unit":"%","show_range_labels":true,"scale_precision":0,"ranges":[{"from":0,"to":40,"color":"#55D956"},{"from":40,"to":70,"color":"#FFC107"},{"from":70,"to":85,"color":"#F05252"},{"from":85,"to":100,"color":"#760000"}],"height":300,"width":-1}'),
    ('Pie chart', '{"type":"pie","id":"sales_pie","caption":"Sales by channel","value":"@pie_points","show_legend":true,"height":280,"width":-1}'),
    ('Bar chart', '{"type":"bar","id":"sales_bar","caption":"Monthly sales","value":"@bar_points","show_legend":true,"height":300,"width":-1}'),
    ('Line chart', '{"type":"line","id":"sales_line","caption":"Sales trend","value":"@line_points","show_legend":true,"smooth":true,"height":300,"width":-1}'),
    ('Button', '{"type":"Button","id":"btn_update","caption":"Simple button"}'),
    ('Switch', '{"type":"Switch","caption":"Setting 1","id":"sw1","value":"@sw1"}'),
    ('CheckBox', '{"type":"CheckBox","caption":"My checkbox","id":"cb1","value":"@cb1"}'),
    ('Input', '{"type":"Input","caption":"My input","id":"my_input1","input_type":"number","value":"@my_input1"}'),
    ('Table(flat)', '{"type":"Table","id":"tab4","value":lines,"table":True,"table_header":["#|n|1","Position|position|7","Qty|qty|1"]}'),
    ('Table(list)', '{"type":"Table","id":"table1","layout":tab1_layout,"value":"@lines"}'),
    ('Tabs', '{"type":"Tabs","value":[{"type":"Tab","id":"tab1","caption":"My tab1","layout":[]}]}'),
    ('DatasetField', '{"type":"DatasetField","dataset":"goods","value":"@product"}'),
    ('NodeInput', '{"type":"NodeInput","dataset":"operations","value":"@my_node"}'),
    ('Spinner', '{"type":"Spinner","id":"my_spinner","caption":"my select:","value":"@my_spinner", "dataset":spinner_dataset}'),
    ('NodeLink', '{"type":"NodeLink","value":""}'),
    ('DatasetLink', '{"type":"DatasetLink","value":""}'),
    ('Card', '{"type":"Card","value":[[]]}'),
    ('VerticalLayout', '{"type":"VerticalLayout","value":[]}'),
    ('HorizontalLayout', '{"type":"HorizontalLayout","value":[]}'),
    ('VerticalScroll', '{"type":"VerticalScroll","value":[]}'),
    ('HorizontalScroll', '{"type":"HorizontalScroll","value":[]}'),
    ('Parameters', '{"type":"Parameters","height":0,"w":1}'),
    ('ActiveCV', ' [ {"type":"Parameters","height":0,"w":1}, {"type":"ActiveCV","id":"active_cv","width":-1,"height":-1} ],   [ {"type":"Parameters","height":0,"w":1}, {"type":"VerticalLayout","id":"cv_info_container","width":-1,"height":-1,"value":[]} ]'),
])

WIZARD_ACTIVE_TEMPLATES = OrderedDict([
    ('String', 'Title|id: string'),
    ('Date', 'Date|date: date'),
    ('Number', 'Number|num: number'),
    ('Boolean', 'Closed|closed: boolean'),
    ('NodeInput', 'Partner|partner: Node("Partner")'),
    ('DatasetField', 'Product|product: DataSet("goods")'),
    ('Spinner', 'Operation|operation: select(Receipt|StockIn, Shipment|StockOut)'),
    ('Table', 'lines:[Product|product: Node("Product"), Quantity|qty: number]'),
    ('NodeChildren', '_children: ChildNode("OrderPosition")|ChildNode("OrderPositionSpecial")'),
    ('ListChildNodes', 'positions:[ChildNode("OrderPosition")]'),
    ('ListNodes', 'linked_lines:[Node("CommonLine")]'),
])

WIZARD_COVER_TEMPLATES = OrderedDict([
    ('Text', 'Title|@value'),
    ('NodeLink', 'Partner|partner: Node("Partner")'),
    ('DatasetLink', 'Items|items: Dataset("goods")'),
    ('Table', '[Product|@product, Quantity|@qty]'),
])

def get_wizard_active_templates():
    return [{'key': k, 'label': k, 'value': v} for k, v in WIZARD_ACTIVE_TEMPLATES.items()]

def get_wizard_cover_templates():
    return [{'key': k, 'label': k, 'value': v} for k, v in WIZARD_COVER_TEMPLATES.items()]

PLUGIN_TEMPLATES = OrderedDict([
    ('FloatingButton', '{"type":"FloatingButton","id":"my_fab","caption":"My <b>button</b>"}'),
    ('CameraBarcodeScannerButton', '{"type":"CameraBarcodeScannerButton","id":"cam_barcode"}'),
    ('BarcodeScanner ', '{"type":"BarcodeScanner ","id":"barcode"}'),
    ('ToolbarButton ', '{"type":"ToolbarButton","id":"pin","caption":"Save","svg":svg2,"svg_size":24,"svg_color":"#FFFFFF"}'),
    ('PhotoButton', '{"type":"PhotoButton","id":"photo","compress":72,"size":55}'),
    ('GalleryButton', '{"type":"GalleryButton","id":"photo"'),
    ('MediaGallery', '{"type":"MediaGallery","id":"gallery"}'),
])

def get_plugin_templates():
    """Return (buttons, map) for PlugIn templates used by editors."""
    buttons = [{'key': k, 'label': k} for k in PLUGIN_TEMPLATES.keys()]
    return buttons, dict(PLUGIN_TEMPLATES)

def get_ui_component_templates():

    """Return (buttons, map) for UI component templates used by editors."""
    buttons = [{'key': k, 'label': k} for k in UI_COMPONENT_TEMPLATES.keys()]
    return buttons, dict(UI_COMPONENT_TEMPLATES)

def _scope_owner_id(user=None):
    """Return tenant owner for owner and delegated child accounts."""
    user = user or current_user
    try:
        return int(getattr(user, "parent_user_id", None) or getattr(user, "id", None))
    except Exception:
        return getattr(user, "parent_user_id", None) or getattr(user, "id", None)


def _can_manage(kind: str, user=None) -> bool:
    user = user or current_user
    if not getattr(user, "is_authenticated", False):
        return False
    if bool(getattr(user, "can_designer", False)):
        return True
    attr = {
        "users": "can_manage_users",
        "rooms": "can_manage_rooms",
        "servers": "can_manage_servers",
    }.get(str(kind or "").strip().lower())
    return bool(attr and getattr(user, attr, False))


def _can_open_mobile_setup(user=None) -> bool:
    user = user or current_user
    return any(_can_manage(k, user) for k in ("users", "rooms", "servers"))


def _has_administrative_role(user) -> bool:
    """Administrative accounts may only be changed by Configurator users."""
    return any(bool(getattr(user, attr, False)) for attr in (
        "can_designer",
        "can_manage_users",
        "can_manage_rooms",
        "can_manage_servers",
    ))


def _enforce_web_access_modes():
    """Restrict Designer UI while allowing delegated mobile administration."""
    if not getattr(current_user, "is_authenticated", False):
        return

    endpoint = str(request.endpoint or "")

    # Common authenticated pages that do not expose configuration internals.
    if endpoint in {
        "index", "public_offer", "logout", "choose_mode", "static", "set_language",
        "edit_profile", "update_device_token",
    }:
        return

    # API and Client have their own authorization checks.
    if (request.path or "").startswith("/api/"):
        return
    if (request.path or "").startswith("/client"):
        return

    if bool(getattr(current_user, "can_designer", False)):
        return

    if endpoint == "mobile_setup" and _can_open_mobile_setup():
        return

    if endpoint in {"users_manage", "users_create", "users_update", "users_delete"} and _can_manage("users"):
        return

    if endpoint in {"create_room", "room_detail", "update_room", "delete_room"} and _can_manage("rooms"):
        return

    if endpoint in {
        "mobile_server_save", "mobile_server_delete",
        "mobile_room_alias_save", "mobile_room_alias_delete",
    } and (_can_manage("servers") or _can_manage("rooms")):
        return

    abort(403)


LANGUAGES = {
    'en': 'English', 
    'ru': 'Русский'
}

def get_locale():
    
    lang = request.args.get('lang')
    if lang in LANGUAGES:
        session['current_language'] = lang
        return lang
    
    
    if 'current_language' in session and session['current_language'] in LANGUAGES:
        return session['current_language']
    
    
    lang_cookie = request.cookies.get('language')
    if lang_cookie in LANGUAGES:
        return lang_cookie
    
    
    if hasattr(g, 'user') and g.user is not None:
        return g.user.locale
    
   
    return request.accept_languages.best_match(LANGUAGES.keys())

def get_timezone():
    if hasattr(g, 'user') and g.user is not None:
        return g.user.timezone
    return 'UTC'

def utility_processor():
    return {
        'get_locale': get_locale,
        'LANGUAGES': LANGUAGES,
        'format_datetime': format_datetime,
        'format_date': format_date
    }


@_routes.route('/set_language/<lang>')
def set_language(lang):
    if lang in LANGUAGES:
        
        session['current_language'] = lang
        session.permanent = True  
        
        
        response = redirect(request.referrer or url_for('index'))
       
        response.set_cookie('language', lang, max_age=365*24*60*60)  # 1 год
        return response
    
    return redirect(request.referrer or url_for('index'))


@_routes.route('/update-device-token/<int:device_id>', methods=['POST'])
@login_required
def update_device_token(device_id):
    device = UserDevice.query.get_or_404(device_id)
    if device.user_id != current_user.id:
        abort(403)
    device.token = request.form.get('token')
    db.session.commit()
    flash('Token updated', 'success')
    return redirect(url_for('edit_profile'))


@_routes.route('/edit-profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    if request.method == 'POST':
        current_user.email = request.form.get('email')
        if request.form.get('password'):
            pwd = request.form.get('password')
            current_user.password = generate_password_hash(pwd)
            current_user.android_password_sha256 = hashlib.sha256(pwd.encode('utf-8')).hexdigest()
        current_user.config_display_name = request.form.get('config_display_name')
        # Main/owner account is also a real _System/_User for Android business login.
        # These flags are edited here, while child users are edited in /users.
        current_user.android_authorization = bool(request.form.get('android_authorization'))
        current_user.offer_pin = bool(request.form.get('offer_pin'))
        db.session.commit()
        _sync_system_users_for_current_user()
        flash(_('Profile updated successfully'), 'success')
        return redirect(url_for('dashboard'))

    devices = UserDevice.query.filter_by(user_id=current_user.id).all()
    qr_img = None
    qr_payload = None

    forwarded_host = (request.headers.get('X-Forwarded-Host') or '').split(',')[0].strip().lower()
    request_host = (request.host or '').strip().lower()

    current_host = forwarded_host or request_host
    current_host = current_host.split(':')[0]

    nmaker_host = urlparse(NMAKER_SERVER_URL).netloc.strip().lower().split(':')[0]

    if current_host == nmaker_host:
        qr_payload = json.dumps({
            'type': 'account_connect',
            'server_url': NMAKER_SERVER_URL,
            'register_device_url': f'{NMAKER_SERVER_URL}/api/me/register-device',
            'login_url': f'{NMAKER_SERVER_URL}/api/auth/login',
            'email': current_user.email,
            'display_name': current_user.config_display_name,
        }, ensure_ascii=False)
        qr_img = generate_qr_code(qr_payload)

    

    return render_template(
        'edit_profile.html',
        devices=devices,
        qr_img=qr_img,
        qr_payload=qr_payload,
        nmaker_server_url=NMAKER_SERVER_URL
    )


@_routes.route('/admin')
@login_required
def admin_dashboard():
    if current_user.email != ADMIN_LOGIN:  
        abort(403)
    

    total_users = db.session.query(User).count()
    total_devices = db.session.query(UserDevice).count()
    
    
    active_users = set()
    for room_connections in active_connections.values():
        active_users.update(room_connections.keys())
    active_users_count = len(active_users)
    
    
    active_devices_count = sum(len(connections) for connections in active_connections.values())
    
    
    users_with_stats = db.session.query(
        User,
        db.func.count(UserDevice.id).label('device_count')
    ).outerjoin(UserDevice).group_by(User.id).all()

    ngenie_code_feature_requests = (
        db.session.query(NGenieCodeFeatureRequest, User)
        .outerjoin(User, NGenieCodeFeatureRequest.user_id == User.id)
        .order_by(NGenieCodeFeatureRequest.created_at.desc())
        .limit(100)
        .all()
    )
    try:
        import ngenie_code
        ngenie_code_debug_records = ngenie_code.list_debug_records(limit=100)
    except Exception:
        ngenie_code_debug_records = []
    try:
        from solutions.models import Solution
        admin_solutions = (
            db.session.query(Solution, User)
            .outerjoin(User, Solution.user_id == User.id)
            .order_by(Solution.updated_at.desc(), Solution.created_at.desc())
            .limit(200)
            .all()
        )
    except Exception:
        admin_solutions = []
    
    return render_template('admin_dashboard.html',
                         total_users=total_users,
                         total_devices=total_devices,
                         active_users_count=active_users_count,
                         active_devices_count=active_devices_count,
                         users_with_stats=users_with_stats,
                         ngenie_code_feature_requests=ngenie_code_feature_requests,
                         ngenie_code_debug_records=ngenie_code_debug_records,
                         admin_solutions=admin_solutions,
                         active_connections=active_connections)


@_routes.route('/admin/user/<int:user_id>')
@login_required
def admin_user_detail(user_id):
    
    if current_user.email != ADMIN_LOGIN:
        abort(403)
    
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    
    
    devices = UserDevice.query.filter_by(user_id=user_id).all()
    
   
    configurations = Configuration.query.filter_by(user_id=user_id, is_system=False).all()
    
    
    rooms = Room.query.filter_by(user_id=user_id).all()
    
    
    is_active = any(user.email in connections for connections in active_connections.values())
    
    return render_template('admin_user_detail.html',
                         user=user,
                         devices=devices,
                         configurations=configurations,
                         rooms=rooms,
                         is_active=is_active)


@_routes.route('/admin/user/<int:user_id>/toggle-active', methods=['POST'])
@login_required
def admin_toggle_user_active(user_id):
    if current_user.email != ADMIN_LOGIN:
        abort(403)
    
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    
    flash(f'User status {user.email} changed', 'success')
    return redirect(url_for('admin_user_detail', user_id=user_id))


@_routes.route('/choose-mode')
@login_required
def choose_mode():
    return render_template('choose_mode.html')


def _materialize_profile_templates_for_config(config):
    """Create/update UserProfile rows from config.profile_templates."""
    if not config or not getattr(config, 'profile_templates', None):
        return []
    out = []
    templates = getattr(config, 'profile_templates', None) or []
    for tpl in templates:
        if not isinstance(tpl, dict):
            continue
        tpl_uid = str(tpl.get('uid') or tpl.get('id') or tpl.get('name') or uuid.uuid4())
        name = str(tpl.get('name') or tpl_uid)
        profile = db.session.execute(
            select(UserProfile).where(
                UserProfile.owner_user_id == config.user_id,
                UserProfile.source_config_id == config.id,
                UserProfile.source_template_uid == tpl_uid,
            )
        ).scalar_one_or_none()
        if not profile:
            profile = UserProfile(
                uid=str(uuid.uuid4()),
                name=name,
                description=str(tpl.get('description') or ''),
                owner_user_id=config.user_id,
                source_config_id=config.id,
                source_template_uid=tpl_uid,
                is_template_generated=True,
            )
            db.session.add(profile)
            db.session.flush()
        else:
            profile.name = name
            profile.description = str(tpl.get('description') or '')
        classes = tpl.get('classes') or tpl.get('access') or []
        for item in classes:
            if isinstance(item, str):
                item = {'class_name': item, 'visible': True}
            if not isinstance(item, dict):
                continue
            class_name = str(item.get('class_name') or item.get('class') or item.get('name') or '').strip()
            if not class_name:
                continue
            access = db.session.execute(
                select(UserProfileClassAccess).where(
                    UserProfileClassAccess.profile_id == profile.id,
                    UserProfileClassAccess.config_id == config.id,
                    UserProfileClassAccess.class_name == class_name,
                )
            ).scalar_one_or_none()
            if not access:
                access = UserProfileClassAccess(profile_id=profile.id, config_id=config.id, class_name=class_name)
                db.session.add(access)
            access.visible = bool(item.get('visible', item.get('access', True)))
            rls = item.get('rls') if isinstance(item.get('rls'), dict) else item
            access.rls_enabled = bool(rls.get('rls_enabled', rls.get('enabled', False)))
            access.rls_mode = str(rls.get('rls_mode', rls.get('mode', 'allow')) or 'allow')
            access.rls_rules_json = rls.get('rules', rls.get('rls_rules_json', [])) or []
            access.rls_handler_code = str(rls.get('handler_code', rls.get('rls_handler_code', '')) or '')
        out.append(profile)
    db.session.commit()
    return out



def _app_helper(name):
    app_mod = sys.modules.get('app') or sys.modules.get('__main__')
    return getattr(app_mod, name, None) if app_mod else None


def _ensure_system_config_for_current_user(sync_users=True):
    fn = _app_helper('_ensure_system_config_for_owner')
    if callable(fn):
        try:
            return fn(_scope_owner_id(), sync_users=sync_users)
        except Exception as e:
            print('Could not ensure system configuration:', e)
    return None


def _sync_system_users_for_current_user():
    fn = _app_helper('_sync_system_users_for_owner')
    if callable(fn):
        try:
            return fn(_scope_owner_id())
        except Exception as e:
            print('Could not sync system users:', e)
    return 0


def _cleanup_reserved_user_classes_for_current_user():
    fn = _app_helper('_cleanup_reserved_user_classes_from_business_configs')
    if callable(fn):
        try:
            return fn(_scope_owner_id())
        except Exception as e:
            print('Could not cleanup old _User classes:', e)
    return 0


def _ensure_reserved_user_class_from_app(config):
    # Compatibility no-op: _User now belongs only to the hidden system config.
    return None


def _parse_class_fields_meta_for_profile_editor(cls):
    """Best-effort field metadata for RLS editor: name/type/ref class."""
    meta = [
        {"name": "_id", "type": "string", "caption": "_id"},
        {"name": "_class", "type": "string", "caption": "_class"},
    ]
    seen = {"_id", "_class"}
    raw = getattr(cls, "data_structure", None) or ""
    try:
        parsed = json.loads(raw) if isinstance(raw, str) and raw.strip() else raw
    except Exception:
        parsed = None

    fields = []
    if isinstance(parsed, dict):
        fields = parsed.get("fields") or parsed.get("Fields") or []
    elif isinstance(parsed, list):
        fields = parsed

    node_re = re.compile(r'Node\(["\']([^"\']+)["\']\)', re.I)
    for item in fields or []:
        name = ""
        caption = ""
        ftype = "string"
        ref_class = ""
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("id") or item.get("field") or "").strip()
            caption = str(item.get("caption") or item.get("title") or name or "")
            ftype = str(item.get("type") or item.get("input_type") or item.get("field_type") or "string").strip() or "string"
            ref_class = str(item.get("class") or item.get("class_name") or item.get("node_class") or item.get("ref_class") or "").strip()
        elif isinstance(item, str):
            txt = item.strip()
            caption = txt
            if "|" in txt:
                caption, txt = txt.split("|", 1)
            if ":" in txt:
                name_part, type_part = txt.split(":", 1)
                name = name_part.strip()
                ftype = type_part.strip()
            else:
                name = txt.strip()
            m = node_re.search(ftype or "")
            if m:
                ref_class = m.group(1)
                ftype = "node"
            elif str(ftype).lower().startswith('node'):
                ftype = "node"
        if not name or name in seen:
            continue
        norm_type = str(ftype or "string").strip().lower()
        if norm_type in ("str", "text", "stringfield"):
            norm_type = "string"
        elif norm_type in ("bool", "checkbox"):
            norm_type = "boolean"
        elif norm_type in ("int", "integer", "float", "decimal", "numeric"):
            norm_type = "number"
        elif norm_type.startswith("node"):
            norm_type = "node"
        meta.append({"name": name, "type": norm_type, "caption": caption or name, "ref_class": ref_class})
        seen.add(name)
    return meta


def _parse_class_fields_for_profile_editor(cls):
    return [x.get('name') for x in _parse_class_fields_meta_for_profile_editor(cls)]


def _profile_access_key(config_id, class_id):
    return f"{int(config_id)}_{int(class_id)}"


def _rls_normalize_rule_values(values):
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        return [str(x).strip() for x in values if str(x).strip()]
    return [x.strip() for x in str(values or '').replace(';', ',').replace('\n', ',').split(',') if x.strip()]


def _build_rls_handler_from_rules(rules):
    """Generated editable Python body. It sets result=True when the row matches the table."""
    compact_rules = []
    for r in rules or []:
        if not isinstance(r, dict):
            continue
        field = str(r.get("field") or "").strip()
        if not field:
            continue
        op = str(r.get("op") or "in").strip().lower() or "in"
        value_type = str(r.get("value_type") or r.get("type") or "string").strip().lower() or "string"
        values = _rls_normalize_rule_values(r.get("values") or r.get("value") or [])
        if op in ("=", "eq", "equal", "equals", "!=", "<>", "ne", "neq", "not_equal", "not equal") and values:
            values = values[:1]
        compact_rules.append({
            "field": field,
            "op": op,
            "value_type": value_type,
            "ref_config_uid": str(r.get("ref_config_uid") or ""),
            "ref_class_name": str(r.get("ref_class_name") or ""),
            "values": values,
        })

    lines = [
        "# Generated from the RLS table. You may edit this code.",
        "# Context/signature:",
        "#   data / _data  - current node data dict",
        "#   node_id / _id  - current node id",
        "#   config_uid, class_name, profile, _system_user",
        "# Helpers: _rls_get(data, path), _rls_norm(value, type)",
        "rules = " + json.dumps(compact_rules, ensure_ascii=False, indent=4),
        "",
        "result = True",
    ]

    for idx, rule in enumerate(compact_rules, 1):
        field = rule.get('field') or ''
        op = (rule.get('op') or 'in').lower()
        value_type = rule.get('value_type') or 'string'
        values = rule.get('values') or []
        lines.append("")
        lines.append(f"# Rule {idx}: {field} {op} {values}")
        lines.append(f"actual = _rls_norm(_rls_get(data, {json.dumps(field, ensure_ascii=False)}), {json.dumps(value_type, ensure_ascii=False)})")
        if op in ('=', 'eq', 'equal', 'equals'):
            expected = values[0] if values else ''
            lines.append(f"expected = _rls_norm({json.dumps(expected, ensure_ascii=False)}, {json.dumps(value_type, ensure_ascii=False)})")
            lines.append("if result and actual != expected:")
            lines.append("    result = False")
        elif op in ('!=', '<>', 'ne', 'neq', 'not_equal', 'not equal'):
            forbidden = values[0] if values else ''
            lines.append(f"forbidden = _rls_norm({json.dumps(forbidden, ensure_ascii=False)}, {json.dumps(value_type, ensure_ascii=False)})")
            lines.append("if result and actual == forbidden:")
            lines.append("    result = False")
        elif op in ('not', 'not in', 'not_in', 'exclude'):
            lines.append(f"forbidden_values = [_rls_norm(x, {json.dumps(value_type, ensure_ascii=False)}) for x in {json.dumps(values, ensure_ascii=False)}]")
            lines.append("if result and actual in forbidden_values:")
            lines.append("    result = False")
        else:
            lines.append(f"allowed_values = [_rls_norm(x, {json.dumps(value_type, ensure_ascii=False)}) for x in {json.dumps(values, ensure_ascii=False)}]")
            lines.append("if result and actual not in allowed_values:")
            lines.append("    result = False")

    return "\n".join(lines).lstrip() + "\n"

def _profile_rules_from_form(form, key):
    fields = form.getlist(f"rls_field_{key}")
    ops = form.getlist(f"rls_op_{key}")
    value_types = form.getlist(f"rls_value_type_{key}")
    ref_config_uids = form.getlist(f"rls_ref_config_uid_{key}")
    ref_class_names = form.getlist(f"rls_ref_class_name_{key}")
    values = form.getlist(f"rls_values_{key}")
    rules = []
    for i, field in enumerate(fields):
        field = str(field or "").strip()
        if not field:
            continue
        op = str(ops[i] if i < len(ops) else "in").strip() or "in"
        value_type = str(value_types[i] if i < len(value_types) else "string").strip() or "string"
        raw_values = values[i] if i < len(values) else ""
        vals = _rls_normalize_rule_values(raw_values)
        if str(op).strip().lower() in ('=', 'eq', 'equal', 'equals', '!=', '<>', 'ne', 'neq', 'not_equal', 'not equal') and vals:
            vals = vals[:1]
        rules.append({
            "field": field,
            "op": op,
            "value_type": value_type,
            "ref_config_uid": str(ref_config_uids[i] if i < len(ref_config_uids) else "").strip(),
            "ref_class_name": str(ref_class_names[i] if i < len(ref_class_names) else "").strip(),
            "values": vals,
        })
    return rules


def _profile_rebuild_rls_for_access(access):
    """Rebuild decisions for existing records after profile/RLS settings change."""
    if not access or not getattr(access, 'rls_enabled', False):
        return 0
    cfg = getattr(access, 'config', None) or db.session.get(Configuration, access.config_id)
    if not cfg:
        return 0
    app_mod = sys.modules.get('app') or sys.modules.get('__main__')
    fn = getattr(app_mod, 'update_node_rls_index_global', None) if app_mod else None
    if not callable(fn):
        return 0
    try:
        import nodes as _nodes_mod_local
        from sqlitedict import SqliteDict as _SqliteDict
        storage_path = os.path.join(getattr(_nodes_mod_local, 'STORAGE_BASE_PATH', 'node_storage'), f"{access.class_name}_{cfg.uid}.sqlite")
        if not os.path.exists(storage_path):
            return 0
        count = 0
        with _SqliteDict(storage_path, autocommit=True) as st:
            for node_id in list(st.keys()):
                payload = st.get(node_id) or {}
                data = payload.get('_data') if isinstance(payload, dict) else {}
                fn(str(cfg.uid), str(access.class_name), str(node_id), data or {})
                count += 1
        return count
    except Exception as e:
        print('Could not rebuild RLS index:', e)
        return 0

def _assign_profiles_to_user(user_id, profile_ids, owner_id):
    owned = db.session.execute(select(UserProfile.id).where(UserProfile.owner_user_id == owner_id)).scalars().all()
    owned_set = {int(x) for x in owned}
    wanted = set()
    for pid in profile_ids or []:
        try:
            ipid = int(pid)
        except Exception:
            continue
        if ipid in owned_set:
            wanted.add(ipid)
    db.session.execute(sa.delete(UserProfileRole).where(UserProfileRole.user_id == user_id))
    for ipid in sorted(wanted):
        db.session.add(UserProfileRole(user_id=user_id, profile_id=ipid))
    db.session.commit()


@_routes.route('/users', methods=['GET'])
@login_required
def users_manage():
    if not _can_manage("users"):
        abort(403)

    owner_id = _scope_owner_id()
    _ensure_system_config_for_current_user(sync_users=True)
    _cleanup_reserved_user_classes_for_current_user()

    # users created under current_user
    users = db.session.execute(
        select(User).where(User.parent_user_id == owner_id).order_by(User.email)
    ).scalars().all()

    # configs owned by current_user (only business configs can be shared here)
    cfgs = db.session.execute(
        select(Configuration)
        .where(
            Configuration.user_id == owner_id,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
            _designer_visible_configuration_clause(),
        )
        .order_by(Configuration.name)
    ).scalars().all()

    profiles = db.session.execute(
        select(UserProfile).where(UserProfile.owner_user_id == owner_id).order_by(UserProfile.name)
    ).scalars().all()

    # map: user_id -> set(config_id)
    access_map = {}
    for u in users:
        ids = set()
        for a in (u.config_access or []):
            try:
                ids.add(int(a.config_id))
            except Exception:
                pass
        access_map[u.id] = ids

    role_map = {}
    for u in users:
        role_map[u.id] = {int(r.profile_id) for r in (u.profile_roles or [])}

    return render_template('users_manage.html', users=users, configs=cfgs, access_map=access_map, profiles=profiles, role_map=role_map, can_grant_admin=bool(getattr(current_user, 'can_designer', False)))


@_routes.route('/users/create', methods=['POST'])
@login_required
def users_create():
    if not _can_manage("users"):
        abort(403)

    owner_id = _scope_owner_id()
    can_grant_admin = bool(getattr(current_user, 'can_designer', False))
    email = (request.form.get('email') or '').strip()
    password = (request.form.get('password') or '').strip()
    if not email or not password:
        flash('Email и пароль обязательны', 'error')
        return redirect(url_for('users_manage'))

    exists = db.session.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if exists:
        flash('Такой email уже существует', 'error')
        return redirect(url_for('users_manage'))

    u = User(
        email=email,
        password=generate_password_hash(password),
        android_password_sha256=hashlib.sha256(password.encode('utf-8')).hexdigest(),
        parent_user_id=owner_id,
        can_designer=bool(request.form.get('can_designer')) if can_grant_admin else False,
        can_client=bool(request.form.get('can_client')),
        can_api=bool(request.form.get('can_api')),
        can_manage_users=bool(request.form.get('can_manage_users')) if can_grant_admin else False,
        can_manage_rooms=bool(request.form.get('can_manage_rooms')) if can_grant_admin else False,
        can_manage_servers=bool(request.form.get('can_manage_servers')) if can_grant_admin else False,
        android_authorization=bool(request.form.get('android_authorization')),
        offer_pin=bool(request.form.get('offer_pin')),
    )
    db.session.add(u)
    db.session.commit()

    # config access (only configs owned by current_user)
    cfg_ids = request.form.getlist('config_ids')
    owned_cfgs = db.session.execute(select(Configuration.id).where(Configuration.user_id == owner_id, sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)))).scalars().all()
    owned_set = set(int(x) for x in owned_cfgs)
    for cid in cfg_ids:
        try:
            icid = int(cid)
        except Exception:
            continue
        if icid not in owned_set:
            continue
        db.session.add(UserConfigAccess(user_id=u.id, config_id=icid))
    db.session.commit()
    _assign_profiles_to_user(u.id, request.form.getlist('profile_ids'), owner_id)
    _sync_system_users_for_current_user()

    flash('Пользователь создан', 'success')
    return redirect(url_for('users_manage'))


@_routes.route('/users/<int:user_id>/update', methods=['POST'])
@login_required
def users_update(user_id: int):
    if not _can_manage("users"):
        abort(403)

    owner_id = _scope_owner_id()
    can_grant_admin = bool(getattr(current_user, 'can_designer', False))
    u = db.session.get(User, user_id)
    if not u or u.parent_user_id != owner_id:
        abort(404)
    if not can_grant_admin and _has_administrative_role(u):
        abort(403)

    if can_grant_admin:
        u.can_designer = bool(request.form.get('can_designer'))
        u.can_manage_users = bool(request.form.get('can_manage_users'))
        u.can_manage_rooms = bool(request.form.get('can_manage_rooms'))
        u.can_manage_servers = bool(request.form.get('can_manage_servers'))
    u.can_client = bool(request.form.get('can_client'))
    u.can_api = bool(request.form.get('can_api'))
    u.android_authorization = bool(request.form.get('android_authorization'))
    u.offer_pin = bool(request.form.get('offer_pin'))

    new_pwd = (request.form.get('password') or '').strip()
    if new_pwd:
        u.password = generate_password_hash(new_pwd)
        u.android_password_sha256 = hashlib.sha256(new_pwd.encode('utf-8')).hexdigest()

    # rewrite config access set
    cfg_ids = request.form.getlist('config_ids')
    owned_cfgs = db.session.execute(select(Configuration.id).where(Configuration.user_id == owner_id, sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)))).scalars().all()
    owned_set = set(int(x) for x in owned_cfgs)
    wanted = set()
    for cid in cfg_ids:
        try:
            icid = int(cid)
        except Exception:
            continue
        if icid in owned_set:
            wanted.add(icid)

    # delete old
    db.session.execute(
        sa.delete(UserConfigAccess).where(UserConfigAccess.user_id == u.id)
    )
    db.session.commit()

    for icid in sorted(wanted):
        db.session.add(UserConfigAccess(user_id=u.id, config_id=icid))
    db.session.commit()
    _assign_profiles_to_user(u.id, request.form.getlist('profile_ids'), owner_id)
    _sync_system_users_for_current_user()

    flash('Права обновлены', 'success')
    return redirect(url_for('users_manage'))


@_routes.route('/profiles/create', methods=['POST'])
@login_required
def profiles_create():
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    data = request.get_json(silent=True) or {}
    name = (request.form.get('name') or data.get('name') or '').strip()
    if not name:
        flash(_('Profile name is required'), 'error')
        return redirect(url_for('users_manage'))
    profile = UserProfile(uid=str(uuid.uuid4()), name=name, owner_user_id=current_user.id)
    db.session.add(profile)
    db.session.commit()
    flash(_('Profile created'), 'success')
    return redirect(url_for('profile_edit', profile_uid=profile.uid))


@_routes.route('/api/profiles', methods=['GET', 'POST'])
@login_required
def api_profiles():
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        profile = UserProfile(
            uid=str(data.get('uid') or uuid.uuid4()),
            name=str(data.get('name') or '').strip(),
            description=str(data.get('description') or ''),
            owner_user_id=current_user.id,
        )
        db.session.add(profile)
        db.session.commit()
        return jsonify({'uid': profile.uid, 'id': profile.id})
    profiles = db.session.execute(select(UserProfile).where(UserProfile.owner_user_id == current_user.id).order_by(UserProfile.name)).scalars().all()
    return jsonify([
        {
            'id': p.id,
            'uid': p.uid,
            'name': p.name,
            'description': p.description,
            'classes': [
                {
                    'config_uid': a.config.uid if a.config else '',
                    'config_id': a.config_id,
                    'class_name': a.class_name,
                    'visible': bool(a.visible),
                    'rls_enabled': bool(a.rls_enabled),
                    'rls_mode': a.rls_mode or 'allow',
                    'rules': a.rls_rules_json or [],
                    'handler_code': a.rls_handler_code or '',
                }
                for a in (p.class_access or [])
            ]
        }
        for p in profiles
    ])


@_routes.route('/api/profiles/<profile_uid>/access', methods=['POST'])
@login_required
def api_profile_access(profile_uid):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    profile = db.session.execute(select(UserProfile).where(UserProfile.uid == profile_uid, UserProfile.owner_user_id == current_user.id)).scalar_one_or_none()
    if not profile:
        abort(404)
    data = request.get_json(silent=True) or {}
    items = data.get('classes') or data.get('access') or []
    for item in items:
        config_uid = str(item.get('config_uid') or '').strip()
        cfg = db.session.execute(select(Configuration).where(Configuration.uid == config_uid, Configuration.user_id == current_user.id, sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)))).scalar_one_or_none()
        if not cfg:
            continue
        class_name = str(item.get('class_name') or item.get('class') or '').strip()
        if not class_name:
            continue
        access = db.session.execute(select(UserProfileClassAccess).where(
            UserProfileClassAccess.profile_id == profile.id,
            UserProfileClassAccess.config_id == cfg.id,
            UserProfileClassAccess.class_name == class_name,
        )).scalar_one_or_none()
        if not access:
            access = UserProfileClassAccess(profile_id=profile.id, config_id=cfg.id, class_name=class_name)
            db.session.add(access)
        access.visible = bool(item.get('visible', True))
        access.rls_enabled = bool(item.get('rls_enabled', item.get('rls', False)))
        access.rls_mode = str(item.get('rls_mode', item.get('mode', 'allow')) or 'allow')
        access.rls_rules_json = item.get('rules', item.get('rls_rules_json', [])) or []
        access.rls_handler_code = str(item.get('handler_code', item.get('rls_handler_code', '')) or '')
    db.session.commit()
    return jsonify({'status': True})


def _profile_value_source_options(configs=None):
    """Class sources for RLS Node values.

    Keep this list in sync with the profile editor's available classes.  The
    previous implementation filtered by ``has_storage`` and this made the RLS
    class dropdown empty while the same classes were visible in the right-side
    available-classes panel.  RLS should let the designer select any class that
    is present in the configuration; the node picker itself will simply return
    no nodes if a selected class has no stored records yet.
    """
    # When the caller already loaded configurations, do not run another
    # _System/user synchronisation merely to populate a class dropdown.
    if configs is None:
        _ensure_system_config_for_current_user(sync_users=False)
    cfgs = list(configs or [])
    if not cfgs:
        cfgs = db.session.execute(
            select(Configuration)
            .where(
                Configuration.user_id == current_user.id,
                _designer_visible_configuration_clause(),
            )
            .order_by(Configuration.is_system, Configuration.name)
        ).scalars().all()
    out = []
    for cfg in cfgs:
        classes = []
        for cls in sorted((cfg.classes or []), key=lambda c: (c.display_name or c.name or '').lower()):
            name = str(getattr(cls, 'name', '') or '').strip()
            if not name:
                continue
            classes.append({
                'name': name,
                'display_name': getattr(cls, 'display_name', None) or name,
            })
        out.append({
            'id': cfg.id,
            'uid': cfg.uid,
            'name': cfg.name or cfg.uid,
            'classes': classes,
        })
    return out


def _profile_node_option_label(cls, node_id, data):
    data = data if isinstance(data, dict) else {}
    view = data.get('_view')
    if isinstance(view, str) and view.strip():
        return view.strip()
    for key in ('name', 'title', 'caption', 'display_name', 'login', 'email', 'code'):
        value = data.get(key)
        if value not in (None, ''):
            return str(value)
    tpl = str(getattr(cls, 'record_view', '') or '').strip()
    if tpl:
        try:
            env = SandboxedEnvironment(autoescape=False)
            rendered = str(env.from_string(tpl).render(**data, _data=data, _id=node_id)).strip()
            if rendered:
                return rendered[:200]
        except Exception:
            pass
    return str(node_id)


def _profile_node_cover_html(class_name, node_uid, label, data=None):
    """Small NodeInput-like card for RLS picker. Keep it safe and independent from client templates."""
    subtitle = node_uid or ''
    label = label or node_uid or ''
    return (
        '<div class="card"><div class="card-body p-2">'
        f'<div class="fw-semibold">{html_escape(label)}</div>'
        f'<div class="text-muted small">{html_escape(class_name)} · {html_escape(subtitle)}</div>'
        '</div></div>'
    )


def _profile_storage_items(config_uid, class_name, *, q='', ids=None, limit=100):
    """Read nodes for profile RLS picker from the same sqlitedict storage as normal node pages."""
    q = (q or '').strip().lower()
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    id_set = set(ids)
    storage_key = f"{class_name}_{config_uid}"
    db_path = os.path.join('node_storage', f"{storage_key}.sqlite")

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    def normalize_uid(node_id):
        node_id = str(node_id or '').strip()
        if not node_id:
            return ''
        if '$' in node_id:
            return node_id
        return f"{config_uid}${class_name}${node_id}"

    def matches(node_id, label, data):
        uid = normalize_uid(node_id)
        if id_set:
            return node_id in id_set or uid in id_set
        if not q:
            return True
        hay = (str(node_id) + ' ' + str(uid) + ' ' + str(label)).lower()
        if q in hay:
            return True
        try:
            return q in json.dumps(data or {}, ensure_ascii=False).lower()
        except Exception:
            return False

    raw_items = []
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                if id_set:
                    candidate_ids = set()
                    for raw in id_set:
                        candidate_ids.add(raw)
                        parts = raw.split('$')
                        if len(parts) >= 3:
                            candidate_ids.add(parts[-1])
                    for cid in candidate_ids:
                        cur.execute("SELECT key, value FROM unnamed WHERE key = ?", (str(cid),))
                        row = cur.fetchone()
                        if row:
                            obj = unpack(row[1])
                            raw_items.append((str(row[0]), obj))
                else:
                    cur.execute("SELECT key, value FROM unnamed ORDER BY key")
                    for key, val_blob in cur.fetchall():
                        raw_items.append((str(key), unpack(val_blob)))
            finally:
                conn.close()
        except Exception:
            raw_items = []

    if not raw_items and os.path.exists(db_path):
        try:
            with SqliteDict(db_path, autocommit=False) as st:
                keys = list(st.keys())
                if id_set:
                    wanted = set()
                    for raw in id_set:
                        wanted.add(raw)
                        parts = raw.split('$')
                        if len(parts) >= 3:
                            wanted.add(parts[-1])
                    keys = [k for k in keys if str(k) in wanted]
                for key in keys:
                    raw_items.append((str(key), st.get(key)))
        except Exception:
            raw_items = []

    out = []
    seen = set()
    cls_obj = None
    try:
        cfg = db.session.execute(select(Configuration).where(Configuration.uid == config_uid)).scalar_one_or_none()
        if cfg:
            for c in (cfg.classes or []):
                if str(c.name) == str(class_name):
                    cls_obj = c
                    break
    except Exception:
        cls_obj = None

    for node_id, payload in raw_items:
        if not isinstance(payload, dict):
            continue
        data = payload.get('_data') if isinstance(payload.get('_data'), dict) else {}
        real_id = str(payload.get('_id') or data.get('_id') or node_id)
        uid = normalize_uid(real_id)
        if uid in seen:
            continue
        label = _profile_node_option_label(cls_obj, real_id, data or {}) if cls_obj else real_id
        if not matches(real_id, label, data):
            continue
        seen.add(uid)
        text_value = f"{label} · {real_id}"
        out.append({
            'id': uid,
            'uid': uid,
            '_id': real_id,
            '_class': class_name,
            '_view': label,
            'text': text_value,
            'cover_html': _profile_node_cover_html(class_name, uid, label, data),
            'data': data or {},
        })
        if len(out) >= int(limit or 100):
            break
    return out


@_routes.route('/api/profile-value-options')
@login_required
def api_profile_value_options():
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    config_uid = (request.args.get('config_uid') or '').strip()
    class_name = (request.args.get('class_name') or '').strip()
    q = (request.args.get('q') or '').strip()
    ids_raw = (request.args.get('ids') or '').strip()
    try:
        limit = max(1, min(int(request.args.get('limit') or 100), 500))
    except Exception:
        limit = 100
    cfg = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if not cfg or not class_name:
        return jsonify([])
    cls = None
    for c in (cfg.classes or []):
        if str(c.name) == class_name:
            cls = c
            break
    if not cls:
        return jsonify([])
    ids = _rls_normalize_rule_values(ids_raw) if ids_raw else []
    try:
        return jsonify(_profile_storage_items(config_uid, class_name, q=q, ids=ids, limit=limit))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@_routes.route('/profiles/<profile_uid>/edit', methods=['GET', 'POST'])
@login_required
def profile_edit(profile_uid):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    profile = db.session.execute(
        select(UserProfile)
        .options(selectinload(UserProfile.class_access))
        .where(UserProfile.uid == profile_uid, UserProfile.owner_user_id == current_user.id)
    ).scalar_one_or_none()
    if not profile:
        abort(404)

    # Opening a role only needs class schemas. Avoid the old synchronous
    # _System user refresh and eager-load all class rows in one SQL round trip.
    _ensure_system_config_for_current_user(sync_users=False)
    all_cfgs = db.session.execute(
        select(Configuration)
        .options(selectinload(Configuration.classes))
        .where(
            Configuration.user_id == current_user.id,
            _designer_visible_configuration_clause(),
        )
        .order_by(Configuration.is_system, Configuration.name)
    ).scalars().unique().all()
    cfgs = [cfg for cfg in all_cfgs if not bool(getattr(cfg, 'is_system', False))]

    class_rows = []
    config_groups = []
    access_by_key = {}
    for access in (profile.class_access or []):
        access_by_key[(int(access.config_id), str(access.class_name))] = access

    for cfg in cfgs:
        group_rows = []
        for cls in sorted((cfg.classes or []), key=lambda c: (c.display_name or c.name or '').lower()):
            key = _profile_access_key(cfg.id, cls.id)
            access = access_by_key.get((int(cfg.id), str(cls.name)))
            rules = []
            if access and access.rls_rules_json:
                rules = access.rls_rules_json if isinstance(access.rls_rules_json, list) else []
            field_meta = _parse_class_fields_meta_for_profile_editor(cls)
            row = {
                'key': key,
                'config': cfg,
                'class': cls,
                'access': access,
                'enabled': bool(access and access.visible),
                'fields': [x.get('name') for x in field_meta],
                'field_meta': field_meta,
                'rules': rules,
                'handler_code': ((access.rls_handler_code if access else '') or _build_rls_handler_from_rules(rules)).lstrip(),
            }
            class_rows.append(row)
            group_rows.append(row)
        config_groups.append({'config': cfg, 'rows': group_rows})

    if request.method == 'POST':
        profile.name = (request.form.get('name') or '').strip() or profile.name
        profile.description = request.form.get('description') or ''
        rebuild_count = 0
        for row in class_rows:
            cfg = row['config']
            cls = row['class']
            key = row['key']
            enabled = bool(request.form.get(f'class_enabled_{key}'))
            access = row['access']
            if not enabled:
                if access:
                    db.session.delete(access)
                continue

            if not access:
                access = UserProfileClassAccess(profile_id=profile.id, config_id=cfg.id, class_name=cls.name)
                db.session.add(access)
                db.session.flush()
            access.visible = True
            access.rls_enabled = bool(request.form.get(f'rls_enabled_{key}'))
            access.rls_mode = request.form.get(f'rls_mode_{key}') or 'allow'
            rules = _profile_rules_from_form(request.form, key)
            access.rls_rules_json = rules
            posted_code = request.form.get(f'rls_handler_{key}')
            generated = _build_rls_handler_from_rules(rules)
            access.rls_handler_code = posted_code.lstrip() if posted_code is not None and posted_code.strip() else generated
            db.session.flush()
            rebuild_count += _profile_rebuild_rls_for_access(access)
        db.session.commit()
        flash(_('Profile saved') + (f'. RLS index updated: {rebuild_count}' if rebuild_count else ''), 'success')
        return redirect(url_for('profile_edit', profile_uid=profile.uid))

    return render_template(
        'profile_access_edit.html',
        profile=profile,
        class_rows=class_rows,
        config_groups=config_groups,
        value_source_configs=_profile_value_source_options(configs=all_cfgs),
        selected_count=sum(1 for r in class_rows if r.get('enabled')),
    )


@_routes.route('/profiles/<profile_uid>/delete', methods=['POST'])
@login_required
def profile_delete(profile_uid):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    profile = db.session.execute(
        select(UserProfile).where(UserProfile.uid == profile_uid, UserProfile.owner_user_id == current_user.id)
    ).scalar_one_or_none()
    if not profile:
        abort(404)
    db.session.delete(profile)
    db.session.commit()
    flash(_('Profile deleted'), 'success')
    return redirect(url_for('users_manage'))


@_routes.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
def users_delete(user_id: int):
    if not _can_manage("users"):
        abort(403)
    owner_id = _scope_owner_id()
    u = db.session.get(User, user_id)
    if not u or u.parent_user_id != owner_id:
        abort(404)
    if int(getattr(u, 'id', 0) or 0) == int(getattr(current_user, 'id', 0) or 0):
        flash('Нельзя удалить текущего пользователя', 'error')
        return redirect(url_for('users_manage'))
    if _has_administrative_role(u) and not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    db.session.delete(u)
    db.session.commit()
    _sync_system_users_for_current_user()
    flash('Пользователь удален', 'success')
    return redirect(url_for('users_manage'))


def _landing_language() -> str:
    """Return the public landing language without putting landing copy in Babel catalogs."""
    posted = str(request.form.get('landing_lang') or '').strip().lower()
    requested = str(request.args.get('lang') or '').strip().lower()
    if posted in {'ru', 'en'}:
        session['current_language'] = posted
        session.permanent = True
        return posted
    if requested in {'ru', 'en'}:
        session['current_language'] = requested
        session.permanent = True
        return requested
    try:
        locale = str(get_locale() or '').strip().lower()
    except Exception:
        locale = ''
    return 'en' if locale.startswith('en') else 'ru'


def _clean_contact_header(value: str, limit: int) -> str:
    return re.sub(r'[\r\n]+', ' ', str(value or '')).strip()[:limit]


def _send_landing_contact_email(*, name: str, email: str, company: str, message: str) -> None:
    """Send landing-page feedback through a configurable SMTP relay.

    No mail credentials are stored in the repository. Configure at least
    NODALOGIC_CONTACT_SMTP_HOST. Authentication is optional for trusted/local
    relays; when it is required, also set USERNAME/PASSWORD.
    """
    host = str(os.getenv('NODALOGIC_CONTACT_SMTP_HOST') or 'smtp.beget.com').strip()

    try:
        port = int(str(os.getenv('NODALOGIC_CONTACT_SMTP_PORT') or '465').strip())
    except Exception:
        port = 465

    username = str(os.getenv('NODALOGIC_CONTACT_SMTP_USERNAME') or 'site@nmaker.pw').strip()
    password = str(os.getenv('NODALOGIC_CONTACT_SMTP_PASSWORD') or 'Ferret_2016')
    recipient = str(os.getenv('NODALOGIC_CONTACT_TO') or 'dv1555@hotmail.com').strip()
    sender = str(os.getenv('NODALOGIC_CONTACT_SMTP_FROM') or username or recipient).strip()
    use_ssl = str(os.getenv('NODALOGIC_CONTACT_SMTP_SSL') or '1').strip().lower() in {'1', 'true', 'yes', 'on'}
    use_starttls = str(os.getenv('NODALOGIC_CONTACT_SMTP_STARTTLS') or '0').strip().lower() in {'1', 'true', 'yes', 'on'}

    mail = EmailMessage()
    mail['Subject'] = f'NodaLogic website: {name or email}'
    mail['From'] = sender
    mail['To'] = recipient
    mail['Reply-To'] = email
    mail.set_content(
        'New message from the NodaLogic public website\n\n'
        f'Name: {name}\n'
        f'Email: {email}\n'
        f'Company / project: {company or "-"}\n\n'
        f'{message}\n'
    )

    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_cls(host, port, timeout=20) as smtp:
        if not use_ssl and use_starttls:
            smtp.starttls()
        if username:
            smtp.login(username, password)
        smtp.send_message(mail)


@_routes.route('/index.html', methods=['GET'])
@_routes.route('/', methods=['GET', 'POST'])
def index():
    # The public landing page must stay reachable even for an authenticated
    # user.  Authentication only changes the CTA target; choose-mode remains
    # the post-login workspace and is not used as a replacement for `/`.
    landing_lang = _landing_language()
    open_auth = False
    auth_tab = 'login'
    contact_values = {}

    if request.method == 'POST':
        form_type = request.form.get('form_type')

        if form_type == 'login':
            email = request.form.get('email')
            password = request.form.get('password')
            user = db.session.execute(
                select(User).where(User.email == email)
            ).scalar_one_or_none()

            if user and check_password_hash(user.password, password):
                login_user(user)
                return redirect(url_for('choose_mode'))
            flash('Invalid email or password' if landing_lang == 'en' else 'Неверный email или пароль', 'error')
            open_auth = True
            auth_tab = 'login'

        elif form_type == 'register':
            email = request.form.get('email')
            password = request.form.get('password')

            if db.session.execute(
                select(User).where(User.email == email)
            ).scalar_one_or_none():
                flash('Email already taken' if landing_lang == 'en' else 'Этот email уже зарегистрирован', 'error')
                open_auth = True
                auth_tab = 'register'
            else:
                new_user = User(
                    email=email,
                    password=generate_password_hash(password),
                    android_password_sha256=hashlib.sha256(password.encode('utf-8')).hexdigest(),
                    can_designer=True,
                    can_client=True,
                    can_api=True,
                )
                db.session.add(new_user)
                db.session.commit()
                login_user(new_user)
                return redirect(url_for('choose_mode'))

        elif form_type == 'contact':
            # Honeypot: bots tend to fill this field; real users never see it.
            if request.form.get('website'):
                return redirect(url_for('index', lang=landing_lang) + '#contact')

            name = _clean_contact_header(request.form.get('name'), 120)
            email = _clean_contact_header(request.form.get('email'), 180)
            company = _clean_contact_header(request.form.get('company'), 180)
            message = str(request.form.get('message') or '').strip()[:6000]
            contact_values = {
                'name': name,
                'email': email,
                'company': company,
                'message': message,
            }

            if not name or not email or not message or '@' not in email:
                flash(
                    'Please fill in your name, a valid email and a message.' if landing_lang == 'en'
                    else 'Заполните имя, корректный email и текст сообщения.',
                    'warning',
                )
            else:
                try:
                    _send_landing_contact_email(
                        name=name,
                        email=email,
                        company=company,
                        message=message,
                    )
                    flash(
                        'Message sent. Thank you — we will get back to you.' if landing_lang == 'en'
                        else 'Сообщение отправлено. Спасибо — мы свяжемся с вами.',
                        'success',
                    )
                    return redirect(url_for('index', lang=landing_lang) + '#contact')
                except Exception as exc:
                    current_app.logger.warning('Landing contact email failed: %s', exc)
                    flash(
                        'Could not send the form right now. Please write directly to dv1555@hotmail.com.' if landing_lang == 'en'
                        else 'Сейчас не удалось отправить форму. Напишите напрямую на dv1555@hotmail.com.',
                        'warning',
                    )

    return render_template(
        'index.html',
        landing_lang=landing_lang,
        open_auth=open_auth,
        auth_tab=auth_tab,
        contact_values=contact_values,
        landing_authenticated=bool(current_user.is_authenticated),
        current_year=datetime.now(timezone.utc).year,
    )


@_routes.route('/offer', methods=['GET'])
@_routes.route('/public-offer', methods=['GET'])
def public_offer():
    return render_template('public_offer.html')


@_routes.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))


@_routes.route('/create-room', methods=['POST'])
@login_required
def create_room():
    if not _can_manage("rooms"):
        abort(403)
    name = (request.form.get('name') or 'New room').strip()
    transport = (request.form.get('transport') or 'websocket').strip().lower()
    if transport not in ('websocket', 'fcm'):
        transport = 'websocket'
    new_room = Room(
        name=name,
        transport=transport,
        user_id=_scope_owner_id()
    )
    db.session.add(new_room)
    db.session.commit()
    if request.form.get('return_to') == 'mobile_setup':
        flash(_('Room created'), 'success')
        return redirect(url_for('mobile_setup'))
    return redirect(url_for('room_detail', room_uid=new_room.uid))


def generate_qr_code(data):
    import qrcode
    from io import BytesIO
    import base64
    
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill='black', back_color='white')
    
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

@_routes.route('/room/<room_uid>')
@login_required
def room_detail(room_uid):
    if not _can_manage("rooms"):
        abort(403)
    room = Room.query.filter_by(uid=room_uid, user_id=_scope_owner_id()).first_or_404()

    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        tasks = tasks_db.get(room_uid, [])
        active_tasks = [t for t in tasks if not t.get('_done')]

    ws_scheme = get_ws_scheme()
    ws_url = f"{ws_scheme}://{request.host}/ws?room={room.uid}"
    api_base = request.url_root.rstrip('/')
    if (room.transport or 'websocket') == 'websocket':
        qr_payload =  ws_url
        qr_payload_text = ws_url
    else:       
        qr_payload = {
            "type": "room_connect",
            "room_uid": room.uid,
            "transport": (room.transport or 'websocket'),
            "ws_url": ws_url if (room.transport or 'websocket') == 'websocket' else '',
            "register_device_url": f"{api_base}/api/room/{room.uid}/register-device",
            "room_url": f"{api_base}/api/room/{room_uid}/objects"
        }
        qr_payload_text = json.dumps(qr_payload, ensure_ascii=False)
    qr_img = generate_qr_code(qr_payload_text)

    room_devices = RoomDevice.query.filter_by(room_uid=room.uid).order_by(RoomDevice.last_seen.desc()).all()

    return render_template('room_detail.html',
                         room=room,
                         tasks=tasks,
                         active_tasks=active_tasks,
                         ws_url=ws_url,
                         qr_img=qr_img,
                         qr_payload=qr_payload_text,
                         room_devices=room_devices)


@_routes.route('/room/<room_uid>/update', methods=['POST'])
@login_required
def update_room(room_uid):
    if not _can_manage("rooms"):
        abort(403)
    room = Room.query.filter_by(uid=room_uid, user_id=_scope_owner_id()).first_or_404()
    room.name = (request.form.get('name') or room.name or 'Room').strip()
    transport = (request.form.get('transport') or room.transport or 'websocket').strip().lower()
    room.transport = transport if transport in ('websocket', 'fcm') else 'websocket'
    db.session.commit()
    flash(_('Room updated'), 'success')
    return redirect(url_for('mobile_setup') if request.form.get('return_to') == 'mobile_setup' else url_for('room_detail', room_uid=room.uid))


@_routes.route('/delete-room/<room_uid>')
@login_required
def delete_room(room_uid):
    if not _can_manage("rooms"):
        abort(403)
    room = Room.query.filter_by(uid=room_uid, user_id=_scope_owner_id()).first_or_404()
    room_uid_value = room.uid
    owner_id = _scope_owner_id()
    owner_config_ids = db.session.execute(
        select(Configuration.id).where(Configuration.user_id == owner_id)
    ).scalars().all()
    if owner_config_ids:
        db.session.execute(
            sa.delete(RoomAlias).where(
                RoomAlias.room_uid == room_uid_value,
                RoomAlias.config_id.in_(list(owner_config_ids)),
            )
        )
    db.session.delete(room)
    db.session.commit()

    with SqliteDict(TASKS_DB_PATH, autocommit=True) as tasks_db:
        if room_uid_value in tasks_db:
            del tasks_db[room_uid_value]

    flash(_('Room deleted successfully'), 'success')
    return redirect(url_for('mobile_setup') if request.args.get('return_to') == 'mobile_setup' else url_for('dashboard') + '#rooms')



@_routes.route('/mobile-setup')
@login_required
def mobile_setup():
    if not _can_open_mobile_setup():
        abort(403)

    owner_id = _scope_owner_id()
    configs = db.session.execute(
        select(Configuration)
        .where(
            Configuration.user_id == owner_id,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
            _designer_visible_configuration_clause(),
        )
        .order_by(Configuration.name)
    ).scalars().all()
    rooms = db.session.execute(
        select(Room).where(Room.user_id == owner_id).order_by(Room.name)
    ).scalars().all()

    config_rows = []
    for cfg in configs:
        cfg_servers = list(Server.query.filter_by(config_id=cfg.id).order_by(Server.is_default.desc(), Server.alias.asc()).all())
        cfg_aliases = list(RoomAlias.query.filter_by(config_id=cfg.id).order_by(RoomAlias.alias.asc()).all())
        config_url = url_for('get_config', uid=cfg.uid, _external=True)
        config_rows.append({
            'config': cfg,
            'config_url': config_url,
            'qr_img': generate_qr_code(config_url),
            'servers': cfg_servers,
            'room_aliases': cfg_aliases,
            'default_server': next((x for x in cfg_servers if bool(x.is_default)), None),
        })

    ws_scheme = get_ws_scheme()
    api_base = request.url_root.rstrip('/')
    room_rows = []
    for room in rooms:
        ws_url = f"{ws_scheme}://{request.host}/ws?room={room.uid}"
        if (room.transport or 'websocket') == 'websocket':
            room_payload = ws_url
        else:
            room_payload = json.dumps({
                "type": "room_connect",
                "room_uid": room.uid,
                "transport": (room.transport or 'websocket'),
                "ws_url": '',
                "register_device_url": f"{api_base}/api/room/{room.uid}/register-device",
                "room_url": f"{api_base}/api/room/{room.uid}/objects",
            }, ensure_ascii=False)
        room_rows.append({
            'room': room,
            'qr_img': generate_qr_code(room_payload),
            'payload': room_payload,
        })

    rooms_by_uid = {str(r.uid): r for r in rooms}
    return render_template(
        'mobile_setup.html',
        config_rows=config_rows,
        rooms=rooms,
        room_rows=room_rows,
        rooms_by_uid=rooms_by_uid,
        can_manage_users=_can_manage('users'),
        can_manage_rooms=_can_manage('rooms'),
        can_manage_servers=_can_manage('servers'),
    )


@_routes.route('/mobile-setup/server/save', methods=['POST'])
@login_required
def mobile_server_save():
    if not _can_manage('servers'):
        abort(403)
    owner_id = _scope_owner_id()
    config_uid = (request.form.get('config_uid') or '').strip()
    cfg = db.session.execute(
        select(Configuration).where(
            Configuration.uid == config_uid,
            Configuration.user_id == owner_id,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
        )
    ).scalar_one_or_none()
    if not cfg:
        abort(404)

    alias = (request.form.get('alias') or '').strip()
    url = (request.form.get('url') or '').strip().rstrip('/')
    if not alias or not url:
        flash(_('Alias and URL are required'), 'error')
        return redirect(url_for('mobile_setup') + '#servers')

    server_id = request.form.get('server_id')
    server = None
    if server_id:
        try:
            server = Server.query.filter_by(id=int(server_id), config_id=cfg.id).first()
        except Exception:
            server = None
        if not server:
            abort(404)
    if server is None:
        server = Server(config_id=cfg.id)
        db.session.add(server)

    make_default = bool(request.form.get('is_default'))
    was_default = bool(getattr(server, 'is_default', False))
    existing_servers = list(Server.query.filter_by(config_id=cfg.id).order_by(Server.id.asc()).all())
    other_servers = [x for x in existing_servers if getattr(server, 'id', None) is None or x.id != server.id]
    if make_default:
        for other in existing_servers:
            other.is_default = False
    elif was_default:
        if other_servers:
            # Do not leave RemoteClass without a default when the current default
            # is explicitly unchecked. Promote the oldest remaining server.
            other_servers[0].is_default = True
        else:
            make_default = True
    elif not any(bool(x.is_default) for x in other_servers):
        # First server (or repair of old inconsistent data) becomes default.
        make_default = True

    server.alias = alias
    server.url = url
    server.is_default = make_default
    db.session.commit()
    flash(_('Server saved'), 'success')
    return redirect(url_for('mobile_setup') + '#servers')


@_routes.route('/mobile-setup/server/<int:server_id>/delete', methods=['POST'])
@login_required
def mobile_server_delete(server_id):
    if not _can_manage('servers'):
        abort(403)
    owner_id = _scope_owner_id()
    server = Server.query.join(Configuration).filter(
        Server.id == server_id,
        Configuration.user_id == owner_id,
        sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
    ).first_or_404()
    config_id = server.config_id
    was_default = bool(server.is_default)
    db.session.delete(server)
    db.session.flush()
    if was_default:
        replacement = Server.query.filter_by(config_id=config_id).order_by(Server.id.asc()).first()
        if replacement:
            replacement.is_default = True
    db.session.commit()
    flash(_('Server deleted'), 'success')
    return redirect(url_for('mobile_setup') + '#servers')


@_routes.route('/mobile-setup/room-alias/save', methods=['POST'])
@login_required
def mobile_room_alias_save():
    if not _can_manage('rooms'):
        abort(403)
    owner_id = _scope_owner_id()
    config_uid = (request.form.get('config_uid') or '').strip()
    cfg = db.session.execute(
        select(Configuration).where(
            Configuration.uid == config_uid,
            Configuration.user_id == owner_id,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
        )
    ).scalar_one_or_none()
    if not cfg:
        abort(404)
    alias = (request.form.get('alias') or '').strip()
    room_uid = (request.form.get('room_uid') or '').strip()
    room = Room.query.filter_by(uid=room_uid, user_id=owner_id).first()
    if not alias or not room:
        flash(_('Alias and room are required'), 'error')
        return redirect(url_for('mobile_setup') + '#rooms')

    alias_id = request.form.get('alias_id')
    row = None
    if alias_id:
        try:
            row = RoomAlias.query.filter_by(id=int(alias_id), config_id=cfg.id).first()
        except Exception:
            row = None
        if not row:
            abort(404)
    if row is None:
        row = RoomAlias.query.filter_by(config_id=cfg.id, alias=alias).first()
    if row is None:
        row = RoomAlias(config_id=cfg.id)
        db.session.add(row)
    row.alias = alias
    row.room_uid = room_uid
    db.session.commit()
    flash(_('Room alias saved'), 'success')
    return redirect(url_for('mobile_setup') + '#rooms')


@_routes.route('/mobile-setup/room-alias/<int:alias_id>/delete', methods=['POST'])
@login_required
def mobile_room_alias_delete(alias_id):
    if not _can_manage('rooms'):
        abort(403)
    owner_id = _scope_owner_id()
    row = RoomAlias.query.join(Configuration).filter(
        RoomAlias.id == alias_id,
        Configuration.user_id == owner_id,
        sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
    ).first_or_404()
    db.session.delete(row)
    db.session.commit()
    flash(_('Room alias deleted'), 'success')
    return redirect(url_for('mobile_setup') + '#rooms')



def _copy_model_columns(source, target, *, exclude=None):
    """Copy mapped scalar columns without copying primary/foreign keys."""
    excluded = set(exclude or ())
    for column in source.__table__.columns:
        name = str(column.name)
        if name in excluded:
            continue
        setattr(target, name, getattr(source, name))
    return target


def _clone_demo_configuration(source, owner_user_id):
    """Install/update one demo configuration exactly like a Designer import."""
    source_content_uid = str(getattr(source, 'content_uid', '') or getattr(source, 'uid', '') or uuid.uuid4())
    target = db.session.execute(
        select(Configuration).where(
            Configuration.user_id == int(owner_user_id),
            Configuration.content_uid == source_content_uid,
        )
    ).scalar_one_or_none()
    if target is None:
        target = Configuration(
            uid=str(uuid.uuid4()),
            content_uid=source_content_uid,
            user_id=int(owner_user_id),
        )
        db.session.add(target)
        db.session.flush()
    else:
        for relation_name in ('classes', 'datasets', 'sections', 'servers', 'room_aliases', 'config_events', 'config_timers'):
            for row in list(getattr(target, relation_name, None) or []):
                db.session.delete(row)
        db.session.flush()

    preserved_uid = target.uid
    _copy_model_columns(
        source, target,
        exclude={
            'id', 'uid', 'user_id', 'is_system', 'demo_product',
            'designer_hidden', 'demo_source_uid', 'last_modified',
        },
    )
    target.uid = preserved_uid
    target.user_id = int(owner_user_id)
    target.is_system = False
    target.demo_product = False
    target.designer_hidden = True
    target.demo_source_uid = str(source.uid or '')
    target.last_modified = datetime.now(timezone.utc)
    target.nodes_handlers = _rewrite_android_handlers_instance_refs_b64(
        target.nodes_handlers,
        target.uid,
        url_for('get_config', uid=target.uid, _external=True),
    )
    db.session.flush()

    for source_class in list(source.classes or []):
        target_class = ConfigClass(config_id=target.id)
        _copy_model_columns(source_class, target_class, exclude={'id', 'config_id'})
        db.session.add(target_class)
        db.session.flush()
        for source_method in list(source_class.methods or []):
            target_method = ClassMethod(class_id=target_class.id)
            _copy_model_columns(source_method, target_method, exclude={'id', 'class_id'})
            db.session.add(target_method)
        for source_event in list(source_class.event_objs or []):
            target_event = ClassEvent(class_id=target_class.id)
            _copy_model_columns(source_event, target_event, exclude={'id', 'class_id'})
            db.session.add(target_event)
            db.session.flush()
            for source_action in list(source_event.actions or []):
                target_action = EventAction(event_id=target_event.id)
                _copy_model_columns(source_action, target_action, exclude={'id', 'event_id'})
                db.session.add(target_action)

    for source_dataset in list(source.datasets or []):
        target_dataset = Dataset(config_id=target.id)
        _copy_model_columns(source_dataset, target_dataset, exclude={'id', 'config_id', 'created_at', 'updated_at'})
        db.session.add(target_dataset)

    for source_section in list(source.sections or []):
        target_section = ConfigSection(config_id=target.id)
        _copy_model_columns(source_section, target_section, exclude={'id', 'config_id'})
        db.session.add(target_section)

    for source_server in list(source.servers or []):
        target_server = Server(config_id=target.id)
        _copy_model_columns(source_server, target_server, exclude={'id', 'config_id'})
        db.session.add(target_server)

    for source_alias in list(getattr(source, 'room_aliases', None) or []):
        target_alias = RoomAlias(config_id=target.id)
        _copy_model_columns(source_alias, target_alias, exclude={'id', 'config_id', 'created_at'})
        db.session.add(target_alias)

    for source_event in list(source.config_events or []):
        target_event = ConfigEvent(config_id=target.id)
        _copy_model_columns(source_event, target_event, exclude={'id', 'config_id'})
        db.session.add(target_event)
        db.session.flush()
        for source_action in list(source_event.actions or []):
            target_action = ConfigEventAction(event_id=target_event.id)
            _copy_model_columns(source_action, target_action, exclude={'id', 'event_id'})
            db.session.add(target_action)

    for source_timer in list(getattr(source, 'config_timers', None) or []):
        target_timer = ConfigTimer(config_id=target.id)
        _copy_model_columns(source_timer, target_timer, exclude={'id', 'config_id', 'created_at', 'updated_at'})
        db.session.add(target_timer)
        db.session.flush()
        for source_action in list(source_timer.actions or []):
            target_action = ConfigTimerAction(timer_id=target_timer.id)
            _copy_model_columns(source_action, target_action, exclude={'id', 'timer_id', 'created_at'})
            db.session.add(target_action)

    db.session.flush()
    if target.nodes_server_handlers:
        handlers_dir = os.path.join('Handlers', target.uid)
        os.makedirs(handlers_dir, exist_ok=True)
        try:
            handlers_code = base64.b64decode(target.nodes_server_handlers).decode('utf-8')
            with open(os.path.join(handlers_dir, 'handlers.py'), 'w', encoding='utf-8', newline='\n') as fh:
                fh.write(handlers_code)
        except Exception as exc:
            current_app.logger.warning('Could not write demo handlers for %s: %s', target.uid, exc)
    return target


def _add_configuration_to_client_repo(config, user_id):
    """Create/update the local client repository entry for an installed product."""
    from client_app import models as client_models

    config_payload = json.loads(get_config(config.uid))
    config_url = (request.host_url or '').rstrip('/') + '/api/config/' + config.uid
    repo = client_models.Repo.query.filter_by(user_id=int(user_id), config_uid=config.uid).first()
    if repo is None:
        repo = client_models.Repo(user_id=int(user_id), config_uid=config.uid, config_url=config_url)
        client_models.db.session.add(repo)
        client_models.db.session.flush()
    repo.name = config_payload.get('name') or config.name or config.uid
    repo.display_name = config_payload.get('display_name') or config_payload.get('name') or config.name or ''
    repo.vendor = config_payload.get('vendor') or config_payload.get('provider') or ''
    repo.version = config_payload.get('version') or ''
    repo.base_url = ''
    repo.config_url = config_url
    repo.username = ''
    repo.password = ''
    repo.config_json = json.dumps(config_payload, ensure_ascii=False)
    repo.config_cached_at = datetime.now(timezone.utc)

    row = client_models.RepoConfig.query.filter_by(repo_id=repo.id).first()
    if row is None:
        row = client_models.RepoConfig(repo_id=repo.id, config_json=repo.config_json)
        client_models.db.session.add(row)
    else:
        row.config_json = repo.config_json
    row.updated_at = datetime.now(timezone.utc)
    client_models.db.session.flush()
    try:
        from client_app.routes import _invalidate_repo_config_mem
        _invalidate_repo_config_mem(repo.id)
    except Exception:
        pass
    return repo


@_routes.route('/demo-products')
@login_required
def demo_products_page():
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    products = db.session.execute(
        select(Configuration).where(
            Configuration.demo_product == True,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
        ).order_by(Configuration.name, Configuration.version)
    ).scalars().all()
    from client_app import models as client_models
    installed_config_uids = {
        str(value) for value in client_models.db.session.execute(
            select(client_models.Repo.config_uid).where(client_models.Repo.user_id == current_user.id)
        ).scalars().all() if str(value or '').strip()
    }
    installed_content_uids = {
        str(value) for value in db.session.execute(
            select(Configuration.content_uid).where(
                Configuration.user_id == current_user.id,
                Configuration.uid.in_(installed_config_uids),
                Configuration.content_uid.is_not(None),
                Configuration.content_uid != '',
            )
        ).scalars().all()
    } if installed_config_uids else set()
    return render_template(
        'demo_products.html',
        products=products,
        installed_content_uids=installed_content_uids,
    )


@_routes.route('/demo-products/<source_uid>/install', methods=['POST'])
@login_required
def demo_products_install(source_uid):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    source = db.session.execute(
        select(Configuration).where(
            Configuration.uid == source_uid,
            Configuration.demo_product == True,
            sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
        )
    ).scalar_one_or_none()
    if source is None:
        abort(404)
    try:
        if not str(getattr(source, 'content_uid', '') or '').strip():
            source.content_uid = str(source.uid)
        target = source if int(source.user_id) == int(current_user.id) else _clone_demo_configuration(source, current_user.id)
        db.session.flush()
        _add_configuration_to_client_repo(target, current_user.id)
        db.session.commit()
        _materialize_profile_templates_for_config(target)
        flash(_('Demo product installed and added to the repository'), 'success')
        return redirect(url_for('client.sections_home'))
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception('Demo product install failed')
        flash(_('Demo product installation failed') + ': ' + str(exc), 'error')
        return redirect(url_for('demo_products_page'))


def _prepare_configuration_contract(config, actor):
    selected = [
        {'config_uid': str(config.uid), 'class_name': str(class_obj.name)}
        for class_obj in list(config.classes or [])
        if bool(getattr(class_obj, 'include_in_contract', False)) and str(getattr(class_obj, 'name', '') or '').strip()
    ]
    if not selected:
        raise ValueError(_('No classes are marked for inclusion in the contract'))

    stable_name = 'config-' + str(config.uid)
    contracts = db.session.execute(
        select(Contract).where(Contract.user_id == actor.id, Contract.source_type == 'class')
        .order_by(Contract.updated_at.desc(), Contract.created_at.desc())
    ).scalars().all()
    contract = next((row for row in contracts if str(row.name or '') == stable_name), None)
    if contract is None:
        for row in contracts:
            refs = list(row.source_classes_json or [])
            if not refs and row.source_config_uid and row.class_name:
                refs = [{'config_uid': row.source_config_uid, 'class_name': row.class_name}]
            if any(str(ref.get('config_uid') or '') == str(config.uid) for ref in refs if isinstance(ref, dict)):
                contract = row
                break

    if contract is None:
        contract = Contract(user_id=actor.id)
        db.session.add(contract)
        existing_refs = []
    else:
        existing_refs = list(contract.source_classes_json or [])
        if not existing_refs and contract.source_config_uid and contract.class_name:
            existing_refs = [{'config_uid': contract.source_config_uid, 'class_name': contract.class_name}]

    preserved_other_configs = [
        ref for ref in existing_refs
        if isinstance(ref, dict) and str(ref.get('config_uid') or '') != str(config.uid)
    ]
    merged_refs = preserved_other_configs + selected
    source_text = '\n'.join(str(ref['config_uid']) + '$' + str(ref['class_name']) for ref in merged_refs)
    _contract_update_from_data(contract, {
        'name': contract.name or stable_name,
        'display_name': contract.display_name or ((config.name or stable_name) + ' · ' + _('Mobile data')),
        'source_type': 'class',
        'source_classes_text': source_text,
    }, actor)
    db.session.flush()
    stats = _contract_recreate_nodes_for_contract(contract)
    db.session.commit()
    return contract, selected, stats


@_routes.route('/contracts/config/<config_uid>/prepare', methods=['POST'])
@login_required
def contracts_prepare_config(config_uid):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    if config is None or not user_can_access_config(current_user, str(config_uid)):
        abort(404)
    try:
        contract, selected, stats = _prepare_configuration_contract(config, current_user)
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), 'error')
        return redirect(url_for('edit_config', uid=config.uid, tab='classes'))
    payload = json.dumps(_contract_add_payload(contract), ensure_ascii=False, indent=2)
    return render_template(
        'contract_ready.html',
        config=config,
        contract=contract,
        selected=selected,
        stats=stats,
        payload=payload,
    )


@_routes.route('/dashboard')
@login_required
def dashboard():
    _backfill_installed_demo_visibility(current_user.id)
    stmt = select(Configuration).where(
        Configuration.user_id == current_user.id,
        sa.or_(Configuration.is_system == False, Configuration.is_system.is_(None)),
        _designer_visible_configuration_clause(),
    )
    configs = db.session.execute(stmt).scalars().all()
    
    stmt = select(Room).where(Room.user_id == current_user.id)
    rooms = db.session.execute(stmt).scalars().all()
    
    return render_template('dashboard.html', configs=configs, rooms=rooms)


@_routes.route('/contracts')
@login_required
def contracts_page():
    contracts = db.session.execute(
        select(Contract).where(Contract.user_id == current_user.id).order_by(Contract.updated_at.desc(), Contract.created_at.desc())
    ).scalars().all()
    contracts_with_stats = []
    for contract in contracts:
        contracts_with_stats.append({
            'model': contract,
            'object_count': _contract_total_object_count(contract),
        })
    configs = _contract_accessible_configs(current_user)
    config_classes = {str(cfg.uid): [str(getattr(c, 'name', '') or '') for c in (cfg.classes or []) if str(getattr(c, 'name', '') or '').strip()] for cfg in configs}
    return render_template('contracts.html', contracts=contracts_with_stats, configs=configs, config_classes=config_classes)


@_routes.route('/contracts/create', methods=['POST'])
@login_required
def contracts_create():
    data = {
        'name': request.form.get('name'),
        'display_name': request.form.get('display_name'),
        'source_type': request.form.get('source_type'),
        'source_classes_text': request.form.get('source_classes_text'),
        'global_index_name': request.form.get('global_index_name'),
        'global_index_value': request.form.get('global_index_value'),
    }

    contract = Contract(user_id=current_user.id)
    try:
        _contract_update_from_data(contract, data, current_user)
    except ValueError as e:
        flash(str(e), 'error')
        return redirect(url_for('contracts_page'))
    except PermissionError:
        abort(403)

    db.session.add(contract)
    db.session.commit()
    flash(_('Contract created'), 'success')
    return redirect(url_for('contracts_page'))


@_routes.route('/contracts/<contract_uid>/update', methods=['POST'])
@login_required
def contracts_update(contract_uid):
    contract = _get_owned_contract_or_404(contract_uid, actor=current_user)

    data = {
        'name': request.form.get('name'),
        'display_name': request.form.get('display_name'),
        'source_type': request.form.get('source_type'),
        'source_classes_text': request.form.get('source_classes_text'),
        'global_index_name': request.form.get('global_index_name'),
        'global_index_value': request.form.get('global_index_value'),
    }

    try:
        _contract_update_from_data(contract, data, current_user)
    except ValueError as e:
        flash(str(e), 'error')
        return redirect(url_for('contracts_page'))
    except PermissionError:
        abort(403)

    db.session.commit()
    flash(_('Contract updated'), 'success')
    return redirect(url_for('contracts_page'))


@_routes.route('/contracts/<contract_uid>/delete', methods=['POST'])
@login_required
def contracts_delete(contract_uid):
    contract = _get_owned_contract_or_404(contract_uid, actor=current_user)
    db.session.delete(contract)
    db.session.commit()
    flash(_('Contract deleted'), 'success')
    return redirect(url_for('contracts_page'))


@_routes.route('/contracts/<contract_uid>/recreate-nodes', methods=['POST'])
@login_required
def contracts_recreate_nodes(contract_uid):
    contract = _get_owned_contract_or_404(contract_uid, actor=current_user)
    if str(contract.source_type or 'class') != 'class':
        flash(_('Only class contracts can recreate nodes'), 'error')
        return redirect(url_for('contracts_page'))
    stats = _contract_recreate_nodes_for_contract(contract)
    db.session.commit()
    msg = _('Nodes recreated: %(created)s created, %(updated)s updated, %(skipped)s skipped',
            created=stats.get('created', 0), updated=stats.get('updated', 0), skipped=stats.get('skipped', 0))
    if stats.get('errors'):
        msg += ' / ' + _('errors') + ': ' + str(len(stats.get('errors') or []))
        flash(msg, 'warning')
    else:
        flash(msg, 'success')
    return redirect(url_for('contracts_page'))


@_routes.route('/contracts/<contract_uid>/qr.png', methods=['GET'])
@login_required
def contracts_qr(contract_uid):
    if qrcode is None:
        abort(500, description='qrcode package is not installed')
    contract = _get_owned_contract_or_404(contract_uid, actor=current_user)
    qr_payload = json.dumps(_contract_add_payload(contract), ensure_ascii=False, separators=(',', ':'))
    img = qrcode.make(qr_payload)
    buf = BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return send_file(buf, mimetype='image/png', download_name=f'contract-{contract.uid}.png')


def utility_processor():
    def safe_getattr(obj, attr, default=None):
        return getattr(obj, attr, default)
    return dict(safe_getattr=safe_getattr)




# Additional editor/UI Flask hooks moved from app.py

def b64decode_filter(s):
    if s:
        try:
            return base64.b64decode(s).decode('utf-8')
        except Exception as e:
            print(f"Decoding error: {str(e)}")
            return "# Decoding error:" + str(e)
    return ""


def before_request():
    # Set the user's time zone (can be saved in the user settings)
    g.user_timezone = pytz.timezone('Europe/Moscow')  


def update_config_timestamp(response):
    if request.endpoint in ['add_method', 'delete_method', 'edit_method', 
                          'add_event', 'edit_event', 'edit_class']:
        class_id = request.view_args.get('class_id')
        if class_id:
            class_obj = db.session.get(ConfigClass, class_id)
            if class_obj:
                class_obj.config.update_last_modified()
    return response    




MOVED_EDITOR_NAMES = ['b64decode_filter', 'before_request', 'update_config_timestamp', 'ANDROID_IMPORTS_TEMPLATE', 'DEEPSEEK_API_URL', 'LANGUAGES', 'LMSTUDIO_API_KEY', 'LMSTUDIO_API_URL', 'LMSTUDIO_MODEL', 'NODE_CLASS_CODE', 'NODE_CLASS_CODE_ANDROID', 'PLUGIN_TEMPLATES', 'UI_COMPONENT_TEMPLATES', 'WIZARD_ACTIVE_TEMPLATES', 'WIZARD_COVER_TEMPLATES', '_enforce_web_access_modes', 'admin_dashboard', 'admin_toggle_user_active', 'admin_user_detail', 'choose_mode', 'contracts_create', 'contracts_delete', 'contracts_page', 'contracts_qr', 'contracts_update', 'create_room', 'dashboard', 'delete_room', 'edit_profile', 'generate_qr_code', 'get_default_server_handlers', 'get_locale', 'get_plugin_templates', 'get_timezone', 'get_ui_component_templates', 'get_wizard_active_templates', 'get_wizard_cover_templates', 'index', 'public_offer', 'init_editor_ui', 'logout', 'room_detail', 'set_language', 'update_device_token', 'users_create', 'users_delete', 'users_manage', 'users_update', 'utility_processor', 'ALLOWED_INPUT_TYPES_AI', 'ALLOWED_UI_TYPES_AI', 'CONTAINER_UI_TYPES_AI', '_PY_SCRIPT_UPLOAD_SESSION_KEY', '_ShowPlugInLiteralValidatorAI', '_action_python_text_value', '_call_llm_code_only', '_carry_existing_event_python_script_refs', '_decode_b64_py', '_decode_b64_text', '_deep_merge_dict_keep_existing', '_encode_b64_py', '_encode_b64_text', '_generate_handlers_body_ai', '_is_remote_script_ref', '_iter_layout_elements_ai', '_last_python_script_upload_url', '_merge_class', '_normalize_event_action_python_scripts_for_save', '_normalize_python_script_text_for_save', '_remember_python_script_upload', '_s3_text_content_type', '_save_python_text_to_s3_via_upload_url', '_split_commands_str', '_split_handlers_header_and_body', '_upsert_list_by_key_keep_missing', '_wiz_active_field_to_json', '_wiz_build_active_table', '_wiz_build_cover_table', '_wiz_cover_field_to_json', '_wiz_cover_row_to_simple', '_wiz_json_field_to_simple', '_wiz_norm_id', '_wiz_parse_fn_call', '_wiz_parse_line_spec', '_wiz_parse_select', '_wiz_split_once_top_level', '_wiz_split_top_level', '_wiz_table_to_simple', '_wiz_unquote', '_wizard_build_active_field', '_wizard_build_cover_field', '_wizard_build_table', '_wizard_normalize_id', '_wizard_parse_fn_call', '_wizard_parse_select', '_wizard_split_once_top_level', '_wizard_split_top_level', 'add_class', 'add_config_event', 'add_dataset', 'add_event', 'add_method', 'add_method_to_class', 'add_new_method_to_class', 'add_section', 'ai_generate', 'ai_generate_layout', 'apply_full_config_from_json', 'call_deepseek', 'call_llm', 'call_lmstudio', 'clear_handlers', 'clear_server_handlers', 'code_editor', 'create_class', 'create_config', 'create_debug_room', 'create_room_alias', 'create_server', 'debug_room', 'delete_class', 'delete_config', 'delete_config_event', 'delete_dataset', 'delete_event', 'delete_method', 'delete_room_alias', 'delete_s3_text', 'delete_section', 'delete_server', 'download_handlers', 'download_server_handlers', 'edit_class', 'edit_config', 'edit_config_event', 'edit_dataset', 'edit_event', 'edit_method', 'ensure_all_classes_present_in_handlers', 'ensure_class_stub_in_module', 'ensure_handlers_skeleton_and_headers', 'export_class_json', 'export_config', 'extract_functions_from_handlers', 'extract_json_array_from_text', 'extract_json_from_text', 'extract_method_body_from_code', 'extract_method_names_ai', 'get_config_event_json', 'get_config_methods', 'get_dataset_json', 'get_method_body', 'get_s3_text_upload_url', 'get_section_json', 'get_user_local_time', 'import_config', 'import_config_new', 'layout_to_simplified_markup', 'layout_wizard', 'merge_llm_config_into_current_ai', 'method_exists_in_code', 'print_form_template_preview', 'python_s3_editor', 'read_s3_text', 'remove_class_from_module', 'remove_method_from_code', 'remove_method_from_module', 'save_common_layouts', 'save_method', 'save_s3_text_via_upload_url', 'simplified_markup_to_layout', 'split_handlers_by_immutable_prefix_ai', 'sync_android_methods_from_code', 'sync_classes_from_android_handlers', 'sync_classes_from_server_handlers', 'sync_methods_from_code', 'sync_server_methods_from_code', 'update_config', 'update_dataset', 'update_existing_method', 'update_handlers_code', 'update_room_alias', 'update_section', 'update_server', 'update_server_handlers_code', 'upload_handlers', 'upload_s3_text', 'upload_server_handlers', 'validate_cover_images_ai', 'validate_full_llm_config_ai', 'validate_handlers_semantics_ai', 'validate_layout_types_ai', 'validate_python_syntax', 'validate_sections_ai', 'validate_sections_command_targets_ai', 'validate_show_plugin_literals_ai']
