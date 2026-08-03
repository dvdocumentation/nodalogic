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
import traceback
import sys
import time
import uuid
from collections import OrderedDict
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


def _ngenie_code_lock_enabled() -> bool:
    return bool(NGENIE_CODE_LOCK_ENABLED)


def _ngenie_code_available() -> bool:
    try:
        import ngenie_code
        return bool(ngenie_code.available())
    except Exception:
        return False


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





def _solutions_optional_call(function_name: str, *args, **kwargs):
    """Call optional Solutions helper if the folder-based feature is installed."""
    try:
        import solutions
        fn = getattr(solutions, function_name, None)
        if callable(fn):
            return fn(*args, **kwargs)
    except Exception:
        current_app.logger.exception('Optional Solutions helper failed: %s', function_name)
    return None


def _solutions_enrich_prompt_if_needed(config, prompt: str, question_answers=None) -> str:
    enriched = _solutions_optional_call(
        'enrich_prompt_for_config',
        config,
        prompt,
        question_answers=question_answers,
        user=current_user,
    )
    return enriched if isinstance(enriched, str) and enriched.strip() else prompt


def _solutions_record_answers_if_needed(config, question_answers) -> None:
    if question_answers:
        _solutions_optional_call(
            'record_question_answers_for_config',
            config,
            question_answers,
            user=current_user,
            commit=True,
        )


def _solutions_record_questions_if_needed(config, questions, message: str = '') -> None:
    if questions:
        _solutions_optional_call(
            'record_questions_for_config',
            config,
            questions,
            message=message,
            user=current_user,
            commit=False,
        )


def _solutions_record_success_if_needed(config, message: str = '') -> None:
    _solutions_optional_call(
        'record_generation_success_for_config',
        config,
        message=message,
        user=current_user,
        commit=False,
    )


def _solutions_run_plan_if_needed(config, question_answers=None, start_only: bool = False):
    """Continue optional Solutions plan.py.

    Returns a payload with type=questions/message/generate/waiting/finished, or None.
    Kept here so the core editor still works when the solutions folder is absent.
    """
    def _solution_llm(plan_prompt: str, debug_stage: str = 'solution_plan') -> str:
        import ngenie_code
        system_prompt = ngenie_code.build_system_prompt()
        return call_llm('ngenie_code', system_prompt, plan_prompt, debug_stage=debug_stage)

    return _solutions_optional_call(
        'run_plan_for_config',
        config,
        user=current_user,
        question_answers=question_answers,
        call_llm=None if start_only else _solution_llm,
        commit=True,
        start_only=start_only,
    )


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
            # Installed demo copies are runtime/client instances. They remain
            # available through Client/API/Android but are never Designer objects.
            if cfg is not None and _mark_installed_demo_copy_hidden(cfg):
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
        _solutions_run_plan_if_needed(config, start_only=True)
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
                           ngenie_code_available=_ngenie_code_available(),
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
                new_event = ClassEvent(
                    event=event_data['event'],
                    listener=event_data.get('listener', ''),
                    class_id=new_class.id
                )
                db.session.add(new_event)
                db.session.flush()
                
                # Import event actions
                actions_data = event_data.get('actions', [])
                print(f"    Importing {len(actions_data)} actions for event {event_data['event']}")
                
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
    for dataset in config.datasets:
        db.session.delete(dataset)
    for section in config.sections:
        db.session.delete(section)
    for server in config.servers:
        db.session.delete(server)
    for ra in (getattr(config, 'room_aliases', None) or []):
        db.session.delete(ra)
    for event in config.config_events:
        db.session.delete(event)
    for timer in (getattr(config, 'config_timers', None) or []):
        db.session.delete(timer)
    
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
        
        # Importing class events
        events_data = class_data.get('events', [])
        print(f"  Importing {len(events_data)} events for class {class_data['name']}")
        
        for event_data in events_data:
            new_event = ClassEvent(
                event=event_data['event'],
                listener=event_data.get('listener', ''),
                class_id=new_class.id
            )
            db.session.add(new_event)
            db.session.flush()
            
            # Importing event actions
            actions_data = event_data.get('actions', [])
            print(f"    Importing {len(actions_data)} actions for event {event_data['event']}")
            
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

     # Importing common events
    common_events_data = data.get('CommonEvents', [])
    print(f"Importing {len(common_events_data)} common events.")

    for ev_data in common_events_data:
        new_event = ConfigEvent(
            event=ev_data['event'],
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
    
    # Updating the timestamp
    config.update_last_modified()
    
    # CREATE/UPDATE THE SERVER HANDLERS FILE IF THERE ARE ANY
    if config.nodes_server_handlers:
        handlers_dir = os.path.join('Handlers', config.uid)
        os.makedirs(handlers_dir, exist_ok=True)
        handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
        try:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            with open(handlers_file_path, 'w', encoding='utf-8', newline="\n") as f:
                f.write(handlers_code)
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

def call_llm(provider: str, system_prompt: str, user_prompt: str, *, debug_stage: str = "call", debug_meta: dict = None) -> str:
    provider = (provider or "").strip().lower()
    if provider == "ngenie_code":
        import ngenie_code
        return ngenie_code.call_llm(system_prompt, user_prompt, debug_stage=debug_stage, debug_meta=debug_meta or {})
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

ALLOWED_UI_TYPES_AI = {
    # BASIC
    "Text", "Picture", "HTML", "Button", "BottomButtons", "Input", "Switch", "CheckBox",
    "Table", "Parameters", "NodeChildren", "Spinner",
    "gauge", "pie", "bar", "line", "Gauge", "Pie", "Bar", "Line",

    # DATA REFERENCES (these are valid runtime UI controls and are documented in nGenie Code instructions)
    "DatasetField", "DatasetInput", "DatasetLink", "DataSetLink",
    "NodeInput", "NodeLink",

    # CONTAINERS
    "VerticalLayout", "HorizontalLayout", "VerticalScroll", "HorizontalScroll", "Card",

    # PLUGINS (PlugIn)
    "FloatingButton", "ToolbarButton",
    "PhotoButton", "GalleryButton", "MediaGallery",
    "CameraBarcodeScannerButton",  # camera scan button
    "BarcodeScanner",              # hardware scanner interception (TSD terminals)
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
                if t == "Table" and isinstance(item.get("layout"), list):
                    yield from _iter_layout_elements_ai(item["layout"])
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
        if t == "Input" and "input_type" in el:
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
                if snode is not None and not (isinstance(snode, ast.Constant) and isinstance(snode.value, int)):
                    self.errors.append(f"{where}: Text.size must be integer literal")
            if t == "Input":
                inode = d.get("input_type")
                if inode is not None and not (isinstance(inode, ast.Constant) and isinstance(inode.value, str) and inode.value in ALLOWED_INPUT_TYPES_AI):
                    self.errors.append(f"{where}: Input.input_type must be one of {sorted(ALLOWED_INPUT_TYPES_AI)} (CASE-SENSITIVE)")

            # recurse for containers / bottom buttons / table
            if t in CONTAINER_UI_TYPES_AI:
                self._validate_layout_literal(d.get("value"), where)
            if t == "BottomButtons":
                self._validate_layout_literal(d.get("value"), where)
            if t == "Table":
                self._validate_layout_literal(d.get("layout"), where)

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
        if not code.strip():
            errors.append(f"{field}: empty")
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

def _call_llm_code_only(provider: str, system_prompt: str, user_prompt: str, *, debug_stage: str = "handler_body") -> str:
    """
    Calls LLM and returns the text "as is", but:
    - truncates the ``` if LLM did send it
    """
    txt = call_llm(provider, system_prompt, user_prompt, debug_stage=debug_stage) or ""
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

def _generate_handlers_body_ai(
    provider: str,
    system_prompt: str,
    user_request: str,
    merged_config_json: dict,
    current_header: str,
    current_body: str,
    kind_label: str,   # "ANDROID" or "SERVER"
    max_attempts: int = 3,
):
    """
    Generates ONLY the body (after the header) for handlers.
    We keep the header exactly the same as in the current configuration.
    """
    extra_contract = ""
    if (provider or "").strip().lower() == "ngenie_code":
        try:
            import ngenie_code
            extra_contract = ngenie_code.build_generation_contract(kind_label)
        except Exception:
            extra_contract = ""

    # Strict requirements for the response format
    base_prompt = (
        f"You are updating NodaLogic {kind_label} handlers.\n"
        + (("\nMandatory nGenie Code generation contract:\n" + extra_contract + "\n\n") if extra_contract else "")
        + "Return ONLY python code BODY (no imports, no constants, no markdown, no ```).\n"
        "The BODY must start with class definitions (e.g., 'class ...').\n"
        "Do NOT repeat the header. Do NOT include 'from nodes import Node'.\n"
        "Keep NodaLogic event/class methods declared in JSON class.methods callable as def MethodName(self, input_data=None) returning (bool, dict).\n"
        "Normal Python helpers may be module-level functions with ordinary arguments; avoid making them class methods unless they follow the same NodaLogic callable contract.\n"
        "\n"
        "User request:\n"
        f"{user_request}\n\n"
        "Merged configuration JSON (without needing to include huge handler base64):\n"
        f"{json.dumps(merged_config_json, ensure_ascii=False, indent=2)}\n\n"
        "Current immutable header (DO NOT CHANGE IT):\n"
        f"{current_header}\n\n"
        "Current handlers BODY (edit this):\n"
        f"{current_body}\n"
    )

    body = None
    last_err = None

    for attempt in range(1, max_attempts + 1):
        prompt = base_prompt if attempt == 1 else (
            base_prompt
            + "\n\n"
            "The previous BODY is invalid.\n"
            f"Error:\n{last_err}\n\n"
            "Fix the BODY and return ONLY the corrected BODY.\n"
        )

        candidate_body = _call_llm_code_only(provider, system_prompt, prompt, debug_stage=f"{kind_label.lower()}_handlers_attempt_{attempt}")

        # Quick check: Does it look like body (must start with class/decorator)
        if not candidate_body or ("from nodes import Node" in candidate_body) or ("import " in candidate_body[:200]):
            last_err = "LLM returned header/imports or empty text. Must return only class body."
            continue

        full_code = (current_header or "") + "\n" + candidate_body.strip() + "\n"
        ok, err = validate_python_syntax(full_code)  # Do not touch validate_python_syntax globally.
        if ok:
            return candidate_body.strip()

        last_err = err

    raise RuntimeError(f"Failed to generate valid {kind_label} handlers body after {max_attempts} attempts: {last_err}")

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

@_routes.route('/config/<uid>/ai-generate', methods=['POST'])
@login_required
def ai_generate(uid):
    config = db.session.execute(
        select(Configuration).where(
            Configuration.uid == uid,
            Configuration.user_id == current_user.id
        )
    ).scalar_one_or_none()

    if not config:
        abort(404)

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
    solution_plan_generate = _ngenie_code_bool(data.get('solution_plan_generate'))
    force_old_ai = _ngenie_code_bool(data.get('old_ai') or data.get('force_old_ai'))
    llm_provider = (data.get('llm') or 'deepseek').strip().lower()
    ngenie_code_mode = _ngenie_code_available() and not force_old_ai
    ngenie_code_request_id = uuid.uuid4().hex

    if force_old_ai and _config_is_ngenie_code_locked(config):
        return jsonify({"status": "error", "message": _ngenie_code_forbid_message()}), 403

    if not prompt and not question_answers:
        return jsonify({"status": "error", "message": "Empty prompt"}), 400

    if ngenie_code_mode:
        if solution_plan_generate:
            user_chat_content = 'Генерация конфигурации по утвержденному ТЗ Solution.'
        else:
            user_chat_content = _ngenie_code_format_question_answers_for_chat(original_user_prompt, question_answers)
        user_meta = {'kind': 'question_answers' if question_answers else 'generate', 'write_instruction': bool(write_instruction), 'solution_plan_generate': bool(solution_plan_generate)}
        if question_answers:
            user_meta['question_answers'] = question_answers
        _ngenie_code_add_chat_message(
            config,
            'user',
            user_chat_content,
            request_id=ngenie_code_request_id,
            meta=user_meta,
            commit=True,
        )
        _solutions_record_answers_if_needed(config, question_answers)
        if question_answers:
            plan_payload = _solutions_run_plan_if_needed(config, question_answers=question_answers, start_only=False)
            if isinstance(plan_payload, dict):
                ptype = str(plan_payload.get('type') or '').lower()
                if ptype == 'questions':
                    assistant_text = str(plan_payload.get('message') or 'Для начала уточни вот эти данные')
                    return jsonify({
                        "status": "ok",
                        "message": assistant_text,
                        "ngenie_code_questions": plan_payload.get('questions') or [],
                        "ngenie_code_request_id": ngenie_code_request_id,
                    })
                if ptype in {'message', 'waiting', 'finished'}:
                    assistant_text = str(plan_payload.get('message') or 'План решения обновлен.')
                    return jsonify({
                        "status": "ok",
                        "message": assistant_text,
                        "ngenie_code_request_id": ngenie_code_request_id,
                    })
                if ptype == 'generate':
                    prompt = str(plan_payload.get('prompt') or prompt or 'Сгенерируй конфигурацию по утвержденному решению.')
                    original_user_prompt = prompt

    try:
        # 1. System prompt. nGenie Code is used unless the request explicitly asks for old_ai.
        attachments_text = ""
        if ngenie_code_mode:
            import ngenie_code
            llm_provider = "ngenie_code"
            ngenie_code.set_debug_context(
                request_id=ngenie_code_request_id,
                user_id=getattr(current_user, 'id', None),
                user_email=getattr(current_user, 'email', ''),
                config_uid=getattr(config, 'uid', ''),
                config_name=getattr(config, 'name', ''),
                original_prompt=original_user_prompt[:4000],
            )
            system_prompt = ngenie_code.build_system_prompt(request_id=ngenie_code_request_id)
            attachments_text = ngenie_code.read_uploaded_files(request.files.getlist('attachments')) if getattr(request, 'files', None) else ""
            chat_context = _ngenie_code_chat_context_for_llm(config)
            prompt = _solutions_enrich_prompt_if_needed(config, prompt, question_answers=question_answers)
            prompt = ngenie_code.build_user_prompt(prompt, attachments_text, chat_context=chat_context, question_answers=question_answers)
        else:
            llm_url = "https://raw.githubusercontent.com/dvdocumentation/nodalogic/refs/heads/main/LLM.txt"
            r = requests.get(llm_url, timeout=10)
            if r.status_code == 200:
                system_prompt = r.text
            else:
                system_prompt = "You are the NodaLogic configuration generation assistant. Always return valid JSON without any explanations."

        # 2. current configuration
        current_config_json = json.loads(get_config(config.uid))
        before_config_json_for_summary = json.loads(json.dumps(current_config_json, ensure_ascii=False))

        # 3. form a request to LLM:
        #    Request return the COMPLETE new configuration in the same JSON format.
        #3) STEP 1: Ask LLM for a JSON patch. Handler code is generated in STEP 2.
        ngenie_code_generation_contract = ""
        if ngenie_code_mode:
            try:
                import ngenie_code
                ngenie_code_generation_contract = ngenie_code.build_generation_contract("PATCH")
            except Exception:
                ngenie_code_generation_contract = ""

        user_prompt_patch = (
            "User request:\n"
            f"{prompt}\n\n"
            + (("Mandatory nGenie Code generation contract:\n" + ngenie_code_generation_contract + "\n\n") if ngenie_code_generation_contract else "")
            + "Below is the current configuration in JSON format.\n"
            "Return ONE JSON object. Normally this is a JSON patch with only changed/added: classes, datasets, sections, CommonEvents, ngenie_prompt.\n"
            "If important semantics are ambiguous, return a question response with root field ngenie_code_questions instead of a patch; do not change the configuration in that response.\n"
            "Handler Python is regenerated in the next step from the current handler body and the user request.\n"
            "If the user asks to fix handler logic, do NOT answer that handlers are forbidden in this JSON patch; return the needed class/event metadata changes or an otherwise minimal patch, then the handler generation step will update nodes_handlers/nodes_server_handlers.\n"
            "For every new/changed class fill ngenie_role, ngenie_prompt and ngenie_description.\n"
            + ((ngenie_code.instruction_ack_prompt_text() + "\n\n") if ngenie_code_mode else "")
            + "Do not generate handlers or methods that export/download/send the configuration or access other configs.\n"
            "Unchanged fields can be omitted. Do not delete anything unless explicitly asked.\n"
            "No comments, ONLY JSON.\n\n"
            "Current configuration:\n"
            f"{json.dumps(current_config_json, ensure_ascii=False, indent=2)}"
        )

        completion_text = call_llm(llm_provider, system_prompt, user_prompt_patch, debug_stage="json_patch_initial")
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
                _solutions_record_questions_if_needed(config, clarification_questions, assistant_text)
                _ngenie_code_add_chat_message(
                    config,
                    'assistant',
                    assistant_text,
                    request_id=ngenie_code_request_id,
                    meta={'kind': 'questions', 'questions': clarification_questions, 'source_prompt': original_user_prompt},
                    commit=True,
                )
                return jsonify({
                    "status": "ok",
                    "message": assistant_text,
                    "ngenie_code_questions": clarification_questions,
                    "ngenie_code_request_id": ngenie_code_request_id,
                })
        ngenie_code_ack_errors = []
        if ngenie_code_mode:
            try:
                import ngenie_code
                ngenie_code_ack_errors = ngenie_code.validate_instruction_ack(llm_patch_data)
            except Exception as _ack_error:
                ngenie_code_ack_errors = [f"nGenie Code instruction ack validation failed: {_ack_error}"]

        if ngenie_code_mode:
            try:
                import ngenie_code
                unavailable = ngenie_code.extract_unavailable_request(llm_patch_data)
            except Exception:
                unavailable = None
            if unavailable:
                if _ngenie_code_unavailable_is_handler_patch_contract(unavailable):
                    # Continue to handler generation instead of recording a fake missing-feature request.
                    llm_patch_data = _ngenie_code_minimal_ack_patch()
                    ngenie_code_ack_errors = []
                else:
                    requested = str(unavailable.get('requested_feature') or unavailable.get('requested') or unavailable.get('feature') or 'запрошенная возможность').strip()
                    reason = str(unavailable.get('reason') or unavailable.get('details') or 'В текущих инструкциях/платформе нет такой возможности.').strip()
                    _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, completion_text)
                    assistant_text = "Такой возможности пока нет: " + requested + "\n" + reason + "\nЯ записал заявку разработчику; конфигурация не изменялась."
                    _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'unavailable'}, commit=False)
                    db.session.commit()
                    return jsonify({
                        "status": "ok",
                        "message": assistant_text,
                        "ngenie_code_feature_request": True,
                        "ngenie_code_request_id": ngenie_code_request_id if 'ngenie_code_request_id' in locals() else ""
                    })

        # Merge patch into current (handlers remain current for now—we'll update them in step 2)
        if ngenie_code_mode:
            llm_patch_data_for_merge = ngenie_code.strip_instruction_ack(llm_patch_data)
        else:
            llm_patch_data_for_merge = llm_patch_data
        merged_config_data = merge_llm_config_into_current_ai(current_config_json, llm_patch_data_for_merge)

        # 4) STEP 2: Generate handlers as CODE (body), and do base64 yourself
        # Android handlers
        current_android_code = _decode_b64_text(current_config_json.get("nodes_handlers", ""))
        android_header, android_body = _split_handlers_header_and_body(current_android_code)

        # If the header is empty (the marker wasn't found), we use the current one as "all immutable."
        # and the body is then empty: LLM will return the full file as the body (but we don't want that).
        # Therefore, we use a fallback: if the marker isn't found, immutable = ANDROID_IMPORTS_TEMPLATE + NODE_CLASS_CODE_ANDROID
        if not android_header:
            base_url = current_config_json.get("url", "")
            android_header = (ANDROID_IMPORTS_TEMPLATE.format(uid=config.uid, config_url=base_url) + "\n" + NODE_CLASS_CODE_ANDROID.strip() + "\n")
            # body — the current code without the header (if any), otherwise the entire code
            android_body = android_body or ""

        new_android_body = _generate_handlers_body_ai(
            provider=llm_provider,
            system_prompt=system_prompt,
            user_request=prompt,
            merged_config_json=merged_config_data,
            current_header=android_header,
            current_body=android_body,
            kind_label="ANDROID",
            max_attempts=3,
        )
        new_android_full = (android_header.rstrip() + "\n\n" + new_android_body.strip() + "\n")
        merged_config_data["nodes_handlers"] = _encode_b64_text(new_android_full)

        # Server handlers (if used; if empty, you can leave it empty or also generate it)
        current_server_code = _decode_b64_text(current_config_json.get("nodes_server_handlers", ""))
        server_header, server_body = _split_handlers_header_and_body(current_server_code)

        if current_config_json.get("nodes_server_handlers") or server_header or server_body:
            if not server_header:
                
                server_header = (NODE_CLASS_CODE.strip() + "\n")
                server_body = server_body or ""

            new_server_body = _generate_handlers_body_ai(
                provider=llm_provider,
                system_prompt=system_prompt,
                user_request=prompt,
                merged_config_json=merged_config_data,
                current_header=server_header,
                current_body=server_body,
                kind_label="SERVER",
                max_attempts=3,
            )
            new_server_full = (server_header.rstrip() + "\n\n" + new_server_body.strip() + "\n")
            merged_config_data["nodes_server_handlers"] = _encode_b64_text(new_server_full)

        # 5) Final validation of the entire configuration (including syntax + UI types)
        config_url = url_for('get_config', uid=config.uid, _external=True)

        # 1) ensure basic headers/skeleton handlers (with ANDROID_IMPORTS_TEMPLATE)
        ensure_handlers_skeleton_and_headers(config.uid, config_url, merged_config_data)

        # 2) We guarantee classes from JSON in both handlers (even if LLM “forgot”)
        ensure_all_classes_present_in_handlers(merged_config_data)

        errors = validate_full_llm_config_ai(merged_config_data)
        if ngenie_code_mode:
            import ngenie_code
            errors.extend(ngenie_code_ack_errors)
            errors.extend(ngenie_code.validate_no_config_exfiltration(merged_config_data))
            errors.extend(ngenie_code.validate_generation_quality(merged_config_data))

        # Retry up to 3 times: fix patch+body handlers (leave the header alone)
        attempts = 1
        while errors and attempts < 3:
            attempts += 1

            fix_prompt_patch = (
                (("Mandatory nGenie Code generation contract:\n" + ngenie_code_generation_contract + "\n\n") if ngenie_code_generation_contract else "")
                + "Your configuration PATCH did NOT validate.\n"
                "Fix ONLY the errors below.\n"
                "Return ONE JSON object (PATCH) with only: classes, datasets, sections, CommonEvents and _ngenie_code_instruction_ack.\n"
                + ((ngenie_code.instruction_ack_prompt_text() + "\n") if ngenie_code_mode else "")
                + "If errors mention handler code, do not claim this is impossible; handler bodies are regenerated immediately after this JSON repair step. Fix class/event metadata here and let the handler step fix Python code.\n"
                "No comments, ONLY JSON.\n\n"
                "Errors:\n- " + "\n- ".join(errors) + "\n\n"
                "Previous PATCH JSON:\n"
                + json.dumps(llm_patch_data, ensure_ascii=False, indent=2)
            )

            completion_text = call_llm(llm_provider, system_prompt, fix_prompt_patch, debug_stage=f"json_patch_repair_{attempts}")
            json_str = extract_json_from_text(completion_text)
            llm_patch_data = json.loads(json_str)
            ngenie_code_ack_errors = []
            if ngenie_code_mode:
                try:
                    import ngenie_code
                    ngenie_code_ack_errors = ngenie_code.validate_instruction_ack(llm_patch_data)
                except Exception as _ack_error:
                    ngenie_code_ack_errors = [f"nGenie Code instruction ack validation failed: {_ack_error}"]

            if ngenie_code_mode:
                try:
                    import ngenie_code
                    unavailable = ngenie_code.extract_unavailable_request(llm_patch_data)
                except Exception:
                    unavailable = None
                if unavailable:
                    if _ngenie_code_unavailable_is_handler_patch_contract(unavailable):
                        # Continue to handler generation instead of recording a fake missing-feature request.
                        llm_patch_data = _ngenie_code_minimal_ack_patch()
                        ngenie_code_ack_errors = []
                    else:
                        requested = str(unavailable.get('requested_feature') or unavailable.get('requested') or unavailable.get('feature') or 'запрошенная возможность').strip()
                        reason = str(unavailable.get('reason') or unavailable.get('details') or 'В текущих инструкциях/платформе нет такой возможности.').strip()
                        _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, completion_text)
                        assistant_text = "Такой возможности пока нет: " + requested + "\n" + reason + "\nЯ записал заявку разработчику; конфигурация не изменялась."
                        _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'unavailable'}, commit=False)
                        db.session.commit()
                        return jsonify({
                            "status": "ok",
                            "message": assistant_text,
                            "ngenie_code_feature_request": True,
                            "ngenie_code_request_id": ngenie_code_request_id if 'ngenie_code_request_id' in locals() else ""
                        })

            if ngenie_code_mode:
                llm_patch_data_for_merge = ngenie_code.strip_instruction_ack(llm_patch_data)
            else:
                llm_patch_data_for_merge = llm_patch_data
            merged_config_data = merge_llm_config_into_current_ai(current_config_json, llm_patch_data_for_merge)

            config_url = url_for('get_config', uid=config.uid, _external=True)
            ensure_handlers_skeleton_and_headers(config.uid, config_url, merged_config_data)
            ensure_all_classes_present_in_handlers(merged_config_data)

            # regen ANDROID body with knowledge of errors
            new_android_body = _generate_handlers_body_ai(
                provider=llm_provider,
                system_prompt=system_prompt,
                user_request=prompt + "\n\nValidation errors to fix:\n- " + "\n- ".join(errors),
                merged_config_json=merged_config_data,
                current_header=android_header,
                current_body=android_body,
                kind_label="ANDROID",
                max_attempts=3,
            )
            new_android_full = (android_header.rstrip() + "\n\n" + new_android_body.strip() + "\n")
            merged_config_data["nodes_handlers"] = _encode_b64_text(new_android_full)

            # regen SERVER body if it exists/used
            if current_config_json.get("nodes_server_handlers") or server_header or server_body:
                new_server_body = _generate_handlers_body_ai(
                    provider=llm_provider,
                    system_prompt=system_prompt,
                    user_request=prompt + "\n\nValidation errors to fix:\n- " + "\n- ".join(errors),
                    merged_config_json=merged_config_data,
                    current_header=server_header,
                    current_body=server_body,
                    kind_label="SERVER",
                    max_attempts=3,
                )
                new_server_full = (server_header.rstrip() + "\n\n" + new_server_body.strip() + "\n")
                merged_config_data["nodes_server_handlers"] = _encode_b64_text(new_server_full)

            config_url = url_for('get_config', uid=config.uid, _external=True)
            ensure_handlers_skeleton_and_headers(config.uid, config_url, merged_config_data)
            ensure_all_classes_present_in_handlers(merged_config_data)

            errors = validate_full_llm_config_ai(merged_config_data)
            if ngenie_code_mode:
                import ngenie_code
                errors.extend(ngenie_code_ack_errors)
                errors.extend(ngenie_code.validate_no_config_exfiltration(merged_config_data))
                errors.extend(ngenie_code.validate_generation_quality(merged_config_data))

        if errors:
            if ngenie_code_mode and _ngenie_code_validation_errors_look_like_missing_feature(errors):
                requested = ' / '.join(str(e) for e in errors[:3])[:1000]
                reason = 'Валидация показала, что LLM использовала компонент/возможность, которой нет в текущем NodaLogic/UI.'
                _ngenie_code_record_feature_request(config, original_user_prompt, requested, reason, "\n".join(errors))
                assistant_text = "Похоже, в текущей платформе нет нужного UI-компонента или возможности.\n" + "- " + "\n- ".join(errors) + "\nЯ записал заявку разработчику; конфигурация не изменялась."
                _ngenie_code_add_chat_message(config, 'assistant', assistant_text, request_id=ngenie_code_request_id, meta={'kind': 'missing_feature_validation', 'errors': errors}, commit=False)
                db.session.commit()
                return jsonify({
                    "status": "ok",
                    "message": assistant_text,
                    "ngenie_code_feature_request": True,
                    "ngenie_code_request_id": ngenie_code_request_id if 'ngenie_code_request_id' in locals() else ""
                })
            error_text = "AI generation failed validation:\n- " + "\n- ".join(errors)
            if ngenie_code_mode:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=ngenie_code_request_id, meta={'kind': 'validation_error', 'errors': errors}, commit=True)
            return jsonify({
                "status": "error",
                "message": error_text,
                "ngenie_code_request_id": ngenie_code_request_id if ngenie_code_mode else ""
            }), 400

        

        new_config_data = merged_config_data

        

    except Exception as e:
        #current_app.logger.exception("AI generator error")
        error_text = f"An error occurred while requesting LLM or parsing the response.: {e}"
        if locals().get('ngenie_code_mode'):
            try:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=locals().get('ngenie_code_request_id', ''), meta={'kind': 'exception_before_apply'}, commit=True)
            except Exception:
                pass
        return jsonify({
            "status": "error",
            "message": error_text,
            "ngenie_code_request_id": locals().get('ngenie_code_request_id', '')
        }), 500

    try:
        apply_full_config_from_json(config, new_config_data)
        if ngenie_code_mode:
            _ngenie_code_mark_locked(config)
            if write_instruction:
                try:
                    import ngenie_code
                    cfg_after = _ngenie_code_current_json(config) or new_config_data
                    doc_prompt = ngenie_code.build_instruction_prompt(cfg_after, prompt)
                    config.ngenie_code_instruction = ngenie_code.call_llm(ngenie_code.build_system_prompt(request_id=ngenie_code_request_id), doc_prompt, max_tokens=8000, debug_stage="write_instruction_after_generation")
                except Exception as doc_error:
                    current_app.logger.exception('nGenie Code instruction generation failed')
                    config.ngenie_code_instruction = (getattr(config, 'ngenie_code_instruction', '') or '') + (
                        '\n\n> Instruction generation failed: ' + str(doc_error)
                    )
        instruction_url = url_for('ngenie_code_document', uid=config.uid, kind='instruction') if ngenie_code_mode and getattr(config, 'ngenie_code_instruction', '') else ""
        if ngenie_code_mode:
            cfg_after_summary = _ngenie_code_current_json(config) or new_config_data
            assistant_message = _ngenie_code_summarize_generation(
                locals().get('before_config_json_for_summary', {}),
                cfg_after_summary,
                request_id=ngenie_code_request_id,
                instruction_url=instruction_url,
            )
            _ngenie_code_add_chat_message(config, 'assistant', assistant_message, request_id=ngenie_code_request_id, meta={'kind': 'generation_success'}, commit=False)
            _solutions_record_success_if_needed(config, assistant_message)
        else:
            assistant_message = "Configuration successfully updated via AI generator"
        db.session.commit()
        return jsonify({
            "status": "ok",
            "message": assistant_message if ngenie_code_mode else "Configuration successfully updated via AI generator",
            "ngenie_code_locked": bool(getattr(config, 'ngenie_code_locked', False)),
            "instruction_url": instruction_url,
            "ngenie_code_request_id": ngenie_code_request_id if ngenie_code_mode else ""
        })
    except Exception as e:
        db.session.rollback()
        #current_app.logger.exception("AI generator apply config error")
        error_text = f"Error applying configuration: {e}"
        if locals().get('ngenie_code_mode'):
            try:
                _ngenie_code_add_chat_message(config, 'assistant', error_text, request_id=locals().get('ngenie_code_request_id', ''), meta={'kind': 'apply_error'}, commit=True)
            except Exception:
                pass
        return jsonify({
            "status": "error",
            "message": error_text,
            "ngenie_code_request_id": locals().get('ngenie_code_request_id', '')
        }), 500



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
                _solutions_record_answers_if_needed(config, qa)
                if _ngenie_code_bool(meta.get('resume_plan')):
                    plan_payload = _solutions_run_plan_if_needed(config, question_answers=qa, start_only=False)
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
    _solutions_run_plan_if_needed(config, start_only=True)
    return jsonify({'status': 'ok', 'messages': _ngenie_code_chat_rows(config)})



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
            _solutions_record_answers_if_needed(config, qa)
        db.session.commit()
        return jsonify({'status': 'ok', 'messages': _ngenie_code_chat_rows(config)})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@_routes.route('/config/<uid>/ngenie-code-question-answers', methods=['POST'])
@login_required
def ngenie_code_question_answers(uid):
    """Save structured question answers and optionally resume Solutions plan.py.

    The UI calls this on every field change. Most calls only persist
    Solution.answers_json. When the question action is `straight`, or `if_all`
    and the whole question array is complete, the UI sends resume=true; then the
    backend replays plan.py from the beginning and continues with the next DSL
    statement.
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
    qa = {'questions': questions, 'answers': answers, 'action': action}
    try:
        _solutions_record_answers_if_needed(config, qa)
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
            plan_payload = _solutions_run_plan_if_needed(config, question_answers=qa, start_only=False)
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
        "index", "logout", "choose_mode", "static", "set_language",
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
    
    return render_template('admin_dashboard.html',
                         total_users=total_users,
                         total_devices=total_devices,
                         active_users_count=active_users_count,
                         active_devices_count=active_devices_count,
                         users_with_stats=users_with_stats,
                         ngenie_code_feature_requests=ngenie_code_feature_requests,
                         ngenie_code_debug_records=ngenie_code_debug_records,
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


@_routes.route('/', methods=['GET', 'POST'])
def index():
    if current_user.is_authenticated:
        return redirect(url_for('choose_mode'))
    
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
            flash(_('Invalid email or password'), 'error')

        elif form_type == 'register':
            email = request.form.get('email')
            password = request.form.get('password')
            
            if db.session.execute(
                select(User).where(User.email == email)
            ).scalar_one_or_none():
                flash(_('Email already taken'), 'error')
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
    
    return render_template('index.html')


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
        select(Configuration).where(Configuration.uid == config_uid, Configuration.user_id == current_user.id)
    ).scalar_one_or_none()
    if config is None:
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




MOVED_EDITOR_NAMES = ['b64decode_filter', 'before_request', 'update_config_timestamp', 'ANDROID_IMPORTS_TEMPLATE', 'DEEPSEEK_API_URL', 'LANGUAGES', 'LMSTUDIO_API_KEY', 'LMSTUDIO_API_URL', 'LMSTUDIO_MODEL', 'NODE_CLASS_CODE', 'NODE_CLASS_CODE_ANDROID', 'PLUGIN_TEMPLATES', 'UI_COMPONENT_TEMPLATES', 'WIZARD_ACTIVE_TEMPLATES', 'WIZARD_COVER_TEMPLATES', '_enforce_web_access_modes', 'admin_dashboard', 'admin_toggle_user_active', 'admin_user_detail', 'choose_mode', 'contracts_create', 'contracts_delete', 'contracts_page', 'contracts_qr', 'contracts_update', 'create_room', 'dashboard', 'delete_room', 'edit_profile', 'generate_qr_code', 'get_default_server_handlers', 'get_locale', 'get_plugin_templates', 'get_timezone', 'get_ui_component_templates', 'get_wizard_active_templates', 'get_wizard_cover_templates', 'index', 'init_editor_ui', 'logout', 'room_detail', 'set_language', 'update_device_token', 'users_create', 'users_delete', 'users_manage', 'users_update', 'utility_processor', 'ALLOWED_INPUT_TYPES_AI', 'ALLOWED_UI_TYPES_AI', 'CONTAINER_UI_TYPES_AI', '_PY_SCRIPT_UPLOAD_SESSION_KEY', '_ShowPlugInLiteralValidatorAI', '_action_python_text_value', '_call_llm_code_only', '_carry_existing_event_python_script_refs', '_decode_b64_py', '_decode_b64_text', '_deep_merge_dict_keep_existing', '_encode_b64_py', '_encode_b64_text', '_generate_handlers_body_ai', '_is_remote_script_ref', '_iter_layout_elements_ai', '_last_python_script_upload_url', '_merge_class', '_normalize_event_action_python_scripts_for_save', '_normalize_python_script_text_for_save', '_remember_python_script_upload', '_s3_text_content_type', '_save_python_text_to_s3_via_upload_url', '_split_commands_str', '_split_handlers_header_and_body', '_upsert_list_by_key_keep_missing', '_wiz_active_field_to_json', '_wiz_build_active_table', '_wiz_build_cover_table', '_wiz_cover_field_to_json', '_wiz_cover_row_to_simple', '_wiz_json_field_to_simple', '_wiz_norm_id', '_wiz_parse_fn_call', '_wiz_parse_line_spec', '_wiz_parse_select', '_wiz_split_once_top_level', '_wiz_split_top_level', '_wiz_table_to_simple', '_wiz_unquote', '_wizard_build_active_field', '_wizard_build_cover_field', '_wizard_build_table', '_wizard_normalize_id', '_wizard_parse_fn_call', '_wizard_parse_select', '_wizard_split_once_top_level', '_wizard_split_top_level', 'add_class', 'add_config_event', 'add_dataset', 'add_event', 'add_method', 'add_method_to_class', 'add_new_method_to_class', 'add_section', 'ai_generate', 'ai_generate_layout', 'apply_full_config_from_json', 'call_deepseek', 'call_llm', 'call_lmstudio', 'clear_handlers', 'clear_server_handlers', 'code_editor', 'create_class', 'create_config', 'create_debug_room', 'create_room_alias', 'create_server', 'debug_room', 'delete_class', 'delete_config', 'delete_config_event', 'delete_dataset', 'delete_event', 'delete_method', 'delete_room_alias', 'delete_s3_text', 'delete_section', 'delete_server', 'download_handlers', 'download_server_handlers', 'edit_class', 'edit_config', 'edit_config_event', 'edit_dataset', 'edit_event', 'edit_method', 'ensure_all_classes_present_in_handlers', 'ensure_class_stub_in_module', 'ensure_handlers_skeleton_and_headers', 'export_class_json', 'export_config', 'extract_functions_from_handlers', 'extract_json_array_from_text', 'extract_json_from_text', 'extract_method_body_from_code', 'extract_method_names_ai', 'get_config_event_json', 'get_config_methods', 'get_dataset_json', 'get_method_body', 'get_s3_text_upload_url', 'get_section_json', 'get_user_local_time', 'import_config', 'import_config_new', 'layout_to_simplified_markup', 'layout_wizard', 'merge_llm_config_into_current_ai', 'method_exists_in_code', 'print_form_template_preview', 'python_s3_editor', 'read_s3_text', 'remove_class_from_module', 'remove_method_from_code', 'remove_method_from_module', 'save_common_layouts', 'save_method', 'save_s3_text_via_upload_url', 'simplified_markup_to_layout', 'split_handlers_by_immutable_prefix_ai', 'sync_android_methods_from_code', 'sync_classes_from_android_handlers', 'sync_classes_from_server_handlers', 'sync_methods_from_code', 'sync_server_methods_from_code', 'update_config', 'update_dataset', 'update_existing_method', 'update_handlers_code', 'update_room_alias', 'update_section', 'update_server', 'update_server_handlers_code', 'upload_handlers', 'upload_s3_text', 'upload_server_handlers', 'validate_cover_images_ai', 'validate_full_llm_config_ai', 'validate_handlers_semantics_ai', 'validate_layout_types_ai', 'validate_python_syntax', 'validate_sections_ai', 'validate_sections_command_targets_ai', 'validate_show_plugin_literals_ai']
