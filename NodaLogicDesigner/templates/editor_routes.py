# -*- coding: utf-8 -*-
"""Editor/configuration routes extracted from app.py.

The original app uses unprefixed endpoint names in templates and redirects
(``edit_config``, ``save_method`` and so on). A normal Blueprint would prefix
these endpoint names, so this module collects route declarations and registers
them directly on the Flask app while keeping the same endpoints.
"""

import ast
import base64
import io
from io import BytesIO
import json
import os
import re
import traceback
import time
import uuid
from collections import OrderedDict
from typing import TYPE_CHECKING, Any
from ast import FunctionDef, fix_missing_locations, parse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

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
import sqlalchemy as sa
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pytz
from flask_babel import Babel, _, format_datetime, format_date
from sqlitedict import SqliteDict

from extensions import db
from models import (
    Dataset,
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
    Server,
)

try:
    import qrcode
except Exception:  # pragma: no cover - optional dependency, checked at route call time
    qrcode = None


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
    '_contract_total_object_count',
    '_contract_update_from_data',
    '_export_class_json',
    '_get_owned_contract_or_404',
    '_is_http_request_method',
    '_is_script_text_method',
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
    _contract_total_object_count: Any
    _contract_update_from_data: Any
    _export_class_json: Any
    _get_owned_contract_or_404: Any
    _is_http_request_method: Any
    _is_script_text_method: Any
    _runtime_cache_invalidate: Any
    _runtime_download_text_cached: Any
    _s3_key_from_public_url: Any
    get_config: Any
    get_ws_scheme: Any


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
        flask_app.add_url_rule(rule, endpoint, view_func, **opts)

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
        
        
        if 'from nodes import Node' not in file_content:
            
            file_content =android_imports + NODE_CLASS_CODE_ANDROID + '\n' + file_content
        
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
            method=action_data.get('method', ''),
            source=action_data.get('source', 'internal'),
            server=action_data.get('server', ''),
            post_execute_method=action_data.get('postExecuteMethod', ''),
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

def _wiz_build_active_table(specs, index):
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

    return [[{
        "type": "Table",
        "id": f"tab{index}",
        "virtual_node": {
            "layout": [layout_row],
            "cover": [cover_row],
        }
    }]]

def _wiz_build_cover_table(specs, index):
    header = []
    value_row = []

    for spec in specs:
        caption, field_id, right = _wiz_parse_line_spec(spec)
        header.append(f"{caption}|{field_id}|1")
        value_row.append(right or ('@' + field_id))

    return [[{
        "type": "Table",
        "id": f"tab{index}",
        "value": [value_row],
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
            inner = line[1:-1].strip()
            specs = [x.strip() for x in _wiz_split_top_level(inner) if x.strip()]
            if mode == 'active':
                tables.append(_wiz_build_active_table(specs, len(tables) + 1))
            else:
                tables.append(_wiz_build_cover_table(specs, len(tables) + 1))
            continue

        parts = [x.strip() for x in _wiz_split_top_level(line) if x.strip()]
        if mode == 'active':
            rows.append([_wiz_active_field_to_json(p) for p in parts])
        else:
            row = []
            for p in parts:
                row.extend(_wiz_cover_field_to_json(p))
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

    if mode == 'active':
        v = table_item.get('virtual_node') or {}
        layout = v.get('layout') or []
        if not layout or not isinstance(layout, list) or not layout[0]:
            return None

        cols = []
        for item in layout[0]:
            s = _wiz_json_field_to_simple(item)
            if s:
                cols.append(s)
        if cols:
            return '[' + ', '.join(cols) + ']'
        return None

    headers = table_item.get('table_header') or []
    if headers:
        cols = []
        for h in headers:
            if not isinstance(h, str):
                continue
            parts = h.split('|')
            caption = parts[0].strip() if len(parts) > 0 else 'Field'
            field_id = parts[1].strip() if len(parts) > 1 else _wiz_norm_id(caption)
            cols.append(f'{caption}|@{field_id}')
        if cols:
            return '[' + ', '.join(cols) + ']'
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
                s = _wiz_table_to_simple(item, mode)
                if s:
                    lines.append(s)
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
            method=action_data.get('method', ''),
            source=action_data.get('source', 'internal'),
            server=action_data.get('server', ''),
            post_execute_method=action_data.get('postExecuteMethod', ''),
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
        sync_classes_from_server_handlers(config)
        sync_classes_from_android_handlers(config)
        sync_methods_from_code(config)  
        db.session.refresh(config) 

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
        db.session.commit()
        flash(_('Configuration saved'), 'success')
        return redirect(url_for('dashboard'))
    
    rooms = Room.query.filter_by(user_id=current_user.id).order_by(Room.name.asc()).all()
    ui_tpl_buttons, ui_tpl_map = get_ui_component_templates()
    return render_template('edit_config.html',
                           config=config,
                           base64=base64,
                           rooms=rooms,
                           ui_tpl_buttons=ui_tpl_buttons,
                           ui_tpl_map=ui_tpl_map)

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
        class_obj.plug_in = request.form.get('plug_in') or ""
        class_obj.plug_in_web = request.form.get('plug_in_web') or ""

        # Commands tab/group
        class_obj.commands = request.form.get('commands')
        class_obj.use_standard_commands = 'use_standard_commands' in request.form
        class_obj.svg_commands = request.form.get('svg_commands')

        # Migration tab
        class_obj.migration_register_command = 'migration_register_command' in request.form
        class_obj.migration_register_on_save = 'migration_register_on_save' in request.form
        class_obj.migration_default_room_alias = (request.form.get('migration_default_room_alias') or '').strip()
        class_obj.link_share_mode = (request.form.get('link_share_mode') or '').strip()
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
        normalized_indexes = []
        for idx in parsed_indexes:
            if not isinstance(idx, dict):
                continue
            name = str(idx.get('name') or '').strip()
            if not name:
                continue
            normalized_indexes.append({
                'name': name,
                'kind': str(idx.get('kind') or 'hash_index').strip() or 'hash_index',
                'keys': str(idx.get('keys') or '').strip(),
                'filter_enabled': bool(idx.get('filter_enabled')),
                'filter_label': str(idx.get('filter_label') or '').strip(),
                'filter_type': str(idx.get('filter_type') or 'string').strip() or 'string',
                'filter_list_enabled': bool(idx.get('filter_list_enabled')),
            })
        class_obj.indexes_json = normalized_indexes

        class_obj.has_storage = 'has_storage' in request.form
        class_obj.class_type = request.form.get('class_type')
        class_obj.hidden = 'hidden' in request.form 

        section_code = request.form.get('section_code')
        

        section_name = ""
        if section_code:
            section = next((s for s in class_obj.config.sections if s.code == section_code), None)
            if section:
                section_name = section.name

        class_obj.section = section_name
        class_obj.section_code = section_code
        db.session.commit()
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"ok": True, "class_id": class_obj.id})
        flash(_('Class saved'), 'success')
        active_tab = request.form.get("active_tab", "config")
        return redirect(url_for('edit_config', uid=class_obj.config.uid, tab=active_tab))

    rooms = Room.query.filter_by(user_id=current_user.id).order_by(Room.name.asc()).all()
    room_aliases = RoomAlias.query.filter_by(config_id=class_obj.config_id).order_by(RoomAlias.alias.asc()).all()

    ui_tpl_buttons, ui_tpl_map = get_ui_component_templates()
    plugin_tpl_buttons, plugin_tpl_map = get_plugin_templates()

    return render_template('edit_class.html',
                         class_obj=class_obj,
                         rooms=rooms,
                         room_aliases=room_aliases,
                         ui_tpl_buttons=ui_tpl_buttons,
                         ui_tpl_map=ui_tpl_map,
                         plugin_tpl_buttons=plugin_tpl_buttons,
                         plugin_tpl_map=plugin_tpl_map,
                         wizard_active_buttons=get_wizard_active_templates(),
                         wizard_cover_buttons=get_wizard_cover_templates(),
                         event_types=['onShow', 'onInput', 'onChange', 'onShowWeb', 'onInputWeb', "onAcceptServer", "onAfterAcceptServer", "onAccept","onAfterAcccept"])

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
        if mname and mname not in ('NodaScript', 'PythonScript', 'HTTP Request'):
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
            method = a.get('method','') or '',
            post_execute_method = a.get('postExecuteMethod','') or '',
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
        if mname and mname not in ('NodaScript', 'PythonScript', 'HTTP Request'):
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
            method = a.get('method','') or '',
            post_execute_method = a.get('postExecuteMethod','') or '',
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
    
    
    new_class = ConfigClass(
        name=class_name,
        display_name=class_name,
        config_id=config.id,
        class_type='custom_process',
        section_code='server'
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
        'version': getattr(config, 'version', '00.00.01'),
        "NodaLogicFormat": NL_FORMAT,
        "NodaLogicType": "ANDROID_SERVER",
        'last_modified': local_time.isoformat(),
        'provider': provider,
        "CommonLayouts": config.common_layouts or [],
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
                'commands': s.commands
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
                        'method': a.method,
                        'postExecuteMethod': a.post_execute_method,
                        **({'httpFunctionName': a.http_function_name} if _is_http_request_method(a.method) else {}),
                        **({'postHttpFunctionName': a.post_http_function_name} if _is_http_request_method(a.post_execute_method) else {})
                    }
                    for a in e.actions
                ]
            }
            for e in config.config_events
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
            select(Configuration).where(Configuration.user_id==current_user.id, Configuration.content_uid==content_uid)
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
            existing_config.nodes_handlers = data.get('nodes_handlers', existing_config.nodes_handlers)
            existing_config.nodes_handlers_meta = data.get('nodes_handlers_meta', existing_config.nodes_handlers_meta)
            existing_config.nodes_server_handlers = data.get('nodes_server_handlers', existing_config.nodes_server_handlers)
            existing_config.nodes_server_handlers_meta = data.get('nodes_server_handlers_meta', existing_config.nodes_server_handlers_meta)
            
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
            
            config_to_use = existing_config
            is_update = True

            # Import common layouts
            config_to_use.common_layouts = data.get('CommonLayouts', data.get('common_layouts', [])) or []
            
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
                vendor=data.get("vendor"),
                common_layouts=data.get('CommonLayouts', data.get('common_layouts', [])) or []
            )
            
            db.session.add(new_config)
            db.session.flush()
            config_to_use = new_config
            is_update = False

            # Import common layouts
            config_to_use.common_layouts = data.get('CommonLayouts', data.get('common_layouts', [])) or []
        
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
                plug_in=class_data.get('plug_in', ''),
                plug_in_web=class_data.get('plug_in_web', ''),

                commands=class_data.get('commands', ''),
                use_standard_commands=bool(class_data.get('use_standard_commands', True)),
                svg_commands=class_data.get('svg_commands', ''),

                migration_register_command=bool(class_data.get('migration_register_command', False)),
                migration_register_on_save=bool(class_data.get('migration_register_on_save', False)),
                migration_default_room_uid=class_data.get('migration_default_room_uid', ''),
                migration_default_room_alias=class_data.get('migration_default_room_alias', ''),
                link_share_mode=class_data.get('link_share_mode', ''),
                indexes_json=class_data.get('indexes', class_data.get('indexes_json', [])) or [],

                class_type=class_data.get('class_type', ''),
                hidden=class_data.get('hidden', False),
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
                        method=action_data.get('method', ''),
                        post_execute_method=action_data.get('postExecuteMethod', ''),
                        method_text=(_action_python_text_value(action_data, post=False) or '') if _is_script_text_method(action_data.get('method', '')) else '',
                        post_execute_text=(_action_python_text_value(action_data, post=True) or '') if _is_script_text_method(action_data.get('postExecuteMethod', '')) else '',
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
                    method=action_data.get('method', ''),
                    post_execute_method=action_data.get('postExecuteMethod', '')
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
    config.vendor = data.get('vendor', config.vendor)
    config.server_name = data.get('server_name', config.server_name)
    config.version = data.get('version', config.version)
    config.nodes_handlers = data.get('nodes_handlers', config.nodes_handlers)
    config.nodes_handlers_meta = data.get('nodes_handlers_meta', config.nodes_handlers_meta)
    config.nodes_server_handlers = data.get('nodes_server_handlers', config.nodes_server_handlers)
    config.nodes_server_handlers_meta = data.get('nodes_server_handlers_meta', config.nodes_server_handlers_meta)
    config.common_layouts = data.get('CommonLayouts', data.get('common_layouts', config.common_layouts)) or []
    
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
                plug_in=class_data.get('plug_in', ''),
                plug_in_web=class_data.get('plug_in_web', ''),

                commands=class_data.get('commands', ''),
                use_standard_commands=bool(class_data.get('use_standard_commands', True)),
                svg_commands=class_data.get('svg_commands', ''),

                migration_register_command=bool(class_data.get('migration_register_command', False)),
                migration_register_on_save=bool(class_data.get('migration_register_on_save', False)),
                migration_default_room_uid=class_data.get('migration_default_room_uid', ''),
                migration_default_room_alias=class_data.get('migration_default_room_alias', ''),
                link_share_mode=class_data.get('link_share_mode', ''),
                indexes_json=class_data.get('indexes', class_data.get('indexes_json', [])) or [],

                class_type=class_data.get('class_type', ''),
                hidden=class_data.get('hidden', False),
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
                    method=action_data.get('method', ''),
                    post_execute_method=action_data.get('postExecuteMethod', ''),
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
                method=action_data.get('method', ''),
                post_execute_method=action_data.get('postExecuteMethod', '')
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
    
    if 'config_file' not in request.files:
        flash(_('File not selected'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    file = request.files['config_file']
    if file.filename == '':
        flash(_('File not selected'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    if not file.filename.endswith('.json'):
        flash(_('Only JSON files allowed'), 'error')
        return redirect(url_for('edit_config', uid=uid))
    
    try:
        data = json.load(file.stream)
        
        print(f"Starting import for config {uid}")
        print(f"Data keys: {list(data.keys())}")
        
        
        apply_full_config_from_json(config, data)
        
        db.session.commit()
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

def call_deepseek(system_prompt: str, user_prompt: str) -> str:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY is not set")

    headers = {
        'Authorization': f'Bearer {DEEPSEEK_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 8000
    }
    resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]

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
    return data["choices"][0]["message"]["content"]

def call_llm(provider: str, system_prompt: str, user_prompt: str) -> str:
    provider = (provider or "").strip().lower()
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
    "Table", "Parameters", "NodeChildren", "DatasetField",

    # CONTAINERS
    "VerticalLayout", "HorizontalLayout", "VerticalScroll", "HorizontalScroll", "Card",

    # PLUGINS (PlugIn)
    "FloatingButton", "ToolbarButton",
    "PhotoButton", "GalleryButton", "MediaGallery",
    "CameraBarcodeScannerButton",  # camera scan button
    "BarcodeScanner",              # hardware scanner interception (TSD terminals)
}

CONTAINER_UI_TYPES_AI = {"VerticalLayout", "HorizontalLayout", "VerticalScroll", "HorizontalScroll", "Card"}

ALLOWED_INPUT_TYPES_AI = {"NUMBER", "PASSWORD", "MULTILINE", "DATE"}

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
    """Validate: methods have input_data=None and return (bool, dict)."""
    errors = []
    try:
        tree = ast.parse(py_code)
    except SyntaxError as e:
        return [f"{where}: syntax error: {e}"]

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # ignore dunder and __init__
            if node.name.startswith("__") or node.name == "__init__":
                continue

            # must accept input_data
            args = node.args.args or []
            has_input = any(a.arg == "input_data" for a in args)
            if not has_input:
                errors.append(f"{where}: {node.name} must accept parameter input_data=None")
            else:
                # check default None
                total = len(args)
                ndef = len(node.args.defaults or [])
                default_map = {}
                for a, d in zip(args[total-ndef:], node.args.defaults):
                    default_map[a.arg] = d
                d = default_map.get("input_data")
                if d is None or not isinstance(d, ast.Constant) or d.value is not None:
                    errors.append(f"{where}: {node.name} input_data must default to None")

            # must return tuple of 2 elements somewhere
            returns = [n for n in ast.walk(node) if isinstance(n, ast.Return)]
            if not returns:
                errors.append(f"{where}: {node.name} must return (bool, dict)")
            else:
                ok_any = any(isinstance(r.value, ast.Tuple) and len(r.value.elts) == 2 for r in returns)
                if not ok_any:
                    errors.append(f"{where}: {node.name} must return a tuple of 2 elements (bool, dict)")
    return errors

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

def _call_llm_code_only(provider: str, system_prompt: str, user_prompt: str) -> str:
    """
    Calls LLM and returns the text "as is", but:
    - truncates the ``` if LLM did send it
    """
    txt = call_llm(provider, system_prompt, user_prompt) or ""
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
    # Strict requirements for the response format
    base_prompt = (
        f"You are updating NodaLogic {kind_label} handlers.\n"
        "Return ONLY python code BODY (no imports, no constants, no markdown, no ```).\n"
        "The BODY must start with class definitions (e.g., 'class ...').\n"
        "Do NOT repeat the header. Do NOT include 'from nodes import Node'.\n"
        "Keep method signatures and return types exactly as required by the NodaLogic LLM rules.\n"
        "Each method must have parameters and must return a tuple: (bool, dict).\n"
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

        candidate_body = _call_llm_code_only(provider, system_prompt, prompt)

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

    data = request.get_json() or {}
    prompt = (data.get('prompt') or '').strip()
    llm_provider = (data.get('llm') or 'deepseek').strip().lower()

    if not prompt:
        return jsonify({"status": "error", "message": "Empty prompt"}), 400

    try:
        # 1. Downloading the system prompt from GitHub
        llm_url = "https://raw.githubusercontent.com/dvdocumentation/nodalogic/refs/heads/main/LLM.txt"
        r = requests.get(llm_url, timeout=10)
        if r.status_code == 200:
            system_prompt = r.text
        else:
            system_prompt = "You are the NodaLogic configuration generation assistant. Always return valid JSON without any explanations."

        # 2. current configuration
        current_config_json = json.loads(get_config(config.uid))

        # 3. form a request to LLM:
        #    Request return the COMPLETE new configuration in the same JSON format.
        #3) STEP 1: Ask LLM for ONLY the JSON patch WITHOUT handlers.
        user_prompt_patch = (
            "User request:\n"
            f"{prompt}\n\n"
            "Below is the current configuration in JSON format.\n"
            "Return ONE JSON object of the SAME FORMAT as NodaLogic config, BUT DO NOT include:\n"
            "nodes_handlers, nodes_server_handlers.\n"
            "Return only changed/added: classes, datasets, sections, CommonEvents.\n"
            "Unchanged fields can be omitted. Do not delete anything unless explicitly asked.\n"
            "No comments, ONLY JSON.\n\n"
            "Current configuration:\n"
            f"{json.dumps(current_config_json, ensure_ascii=False, indent=2)}"
        )

        completion_text = call_llm(llm_provider, system_prompt, user_prompt_patch)
        json_str = extract_json_from_text(completion_text)
        llm_patch_data = json.loads(json_str)

        # Merge patch into current (handlers remain current for now—we'll update them in step 2)
        merged_config_data = merge_llm_config_into_current_ai(current_config_json, llm_patch_data)

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

        # Retry up to 3 times: fix patch+body handlers (leave the header alone)
        attempts = 1
        while errors and attempts < 3:
            attempts += 1

            fix_prompt_patch = (
                "Your configuration PATCH did NOT validate.\n"
                "Fix ONLY the errors below.\n"
                "Return ONE JSON object (PATCH) with only: classes, datasets, sections, CommonEvents.\n"
                "DO NOT include nodes_handlers/nodes_server_handlers in this JSON.\n"
                "No comments, ONLY JSON.\n\n"
                "Errors:\n- " + "\n- ".join(errors) + "\n\n"
                "Previous PATCH JSON:\n"
                + json.dumps(llm_patch_data, ensure_ascii=False, indent=2)
            )

            completion_text = call_llm(llm_provider, system_prompt, fix_prompt_patch)
            json_str = extract_json_from_text(completion_text)
            llm_patch_data = json.loads(json_str)

            merged_config_data = merge_llm_config_into_current_ai(current_config_json, llm_patch_data)

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

        if errors:
            return jsonify({
                "status": "error",
                "message": "AI generation failed validation:\n- " + "\n- ".join(errors)
            }), 400

        

        new_config_data = merged_config_data

        

    except Exception as e:
        #current_app.logger.exception("AI generator error")
        return jsonify({
            "status": "error",
            "message": f"An error occurred while requesting LLM or parsing the response.: {e}"
        }), 500

    try:
        apply_full_config_from_json(config, new_config_data)
        db.session.commit()
        return jsonify({
            "status": "ok",
            "message": "Configuration successfully updated via AI generator"
        })
    except Exception as e:
        db.session.rollback()
        #current_app.logger.exception("AI generator apply config error")
        return jsonify({
            "status": "error",
            "message": f"Error applying configuration: {e}"
        }), 500

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
    
    if 'name' in request.form:
        config.name = request.form['name']
    if 'version' in request.form:
        config.version = request.form['version']
    if 'server_name' in request.form: 
        config.server_name = request.form['server_name']    
    

    config.last_modified = get_user_local_time()
    db.session.commit()
    
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
        'commands': section.commands
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
    
    if code and name:
        new_section = ConfigSection(
            code=code,
            name=name,
            commands=commands,
            config_id=config.id
        )
        db.session.add(new_section)
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
    db.session.commit()
    
    return jsonify({"status": "success"})

@_routes.route('/delete-section/<section_id>')
@login_required
def delete_section(section_id):
    section = db.session.get(ConfigSection, section_id)
    if not section or section.config.user_id != current_user.id:
        abort(404)
    
    config_uid = section.config.uid
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
    is_default = 'is_default' in request.form

    if is_default:
        
        Server.query.filter_by(config_id=config.id, is_default=True).update({"is_default": False})

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
    db.session.delete(server)
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
    server.is_default = 'is_default' in request.form

    if server.is_default:
        
        Server.query.filter_by(config_id=server.config_id, is_default=True).update({"is_default": False})

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
from nodes import Node, message, Dialog, to_uid, from_uid, CloseNode, DataSets, convertBase64ArrayToFilePaths,convertImageFilesToBase64Array,getBase64FromImageFile,saveBase64ToFile, getByIndex, findByIndex, getByGlobalIndex, findByGlobalIndex, sendTextMessage, sendImageMessage, sendTextToNodeDiscussion, sendImageToNodeDiscussion, downloadJsonCached, dispatch_json_node_event, dispatch_downloaded_node_event
'''

NODE_CLASS_CODE_ANDROID = '''
from nodes import Node
'''

ANDROID_IMPORTS_TEMPLATE = '''from nodesclient import RefreshTab,SetTitle,CloseNode,RunGPS,StopGPS,UpdateView,Dialog,ScanBarcode,GetLocation,AddTimer,StopTimer,ShowProgressButton,HideProgressButton,ShowProgressGlobal,HideProgressGlobal,Controls,SetCover,getBase64FromImageFile,convertImageFilesToBase64Array,saveBase64ToFile,convertBase64ArrayToFilePaths,UpdateMediaGallery
from android import *
from nodes import NewNode, DeleteNode, GetAllNodes, GetNode, GetAllNodesStr, GetRemoteClass, CreateDataSet, GetDataSet, DeleteDataSet,to_uid, from_uid, getByIndex, findByIndex, getByGlobalIndex, findByGlobalIndex, sendTextMessage, sendImageMessage
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

UI_COMPONENT_TEMPLATES = OrderedDict([
    ('Text', '{"type":"Text","value":"my text"}'),
    ('Text(tag)', '{"type":"Text","value":"my text","radius":10,"background":"#F54927"}'),
    ('Picture', '{"type":"Picture","value":"filename/path"}'),
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
])

WIZARD_ACTIVE_TEMPLATES = OrderedDict([
    ('String', 'Title|id: string'),
    ('Date', 'Date|date: date'),
    ('Number', 'Number|num: number'),
    ('Boolean', 'Closed|closed: boolean'),
    ('NodeInput', 'Partner|partner: Node("Partner")'),
    ('DatasetField', 'Product|product: DataSet("goods")'),
    ('Spinner', 'Operation|operation: select(Receipt|StockIn, Shipment|StockOut)'),
    ('Table', '[Product|product: Node("Product"), Quantity|qty: number]'),
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
    ('PhotoButton', '{"type":"PhotoButton","id":"photo"}'),
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

def _enforce_web_access_modes():
    """Restrict Designer (server UI) for users without can_designer.

    Client UI is handled in client_app blueprint.
    API uses basic auth decorators.
    """
    if not getattr(current_user, "is_authenticated", False):
        return

    # allow landing / mode switch / logout
    if request.endpoint in {"index", "logout", "choose_mode", "static"}:
        return

    # allow API routes (their own auth)
    if (request.path or "").startswith("/api/"):
        return

    # allow client blueprint routes (blueprint has its own guard)
    if (request.path or "").startswith("/client"):
        return

    # everything else is Designer/Server UI
    if not bool(getattr(current_user, "can_designer", False)):
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
            current_user.password = generate_password_hash(request.form.get('password'))
        current_user.config_display_name = request.form.get('config_display_name')
        db.session.commit()
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
    
    return render_template('admin_dashboard.html',
                         total_users=total_users,
                         total_devices=total_devices,
                         active_users_count=active_users_count,
                         active_devices_count=active_devices_count,
                         users_with_stats=users_with_stats,active_connections=active_connections)


@_routes.route('/admin/user/<int:user_id>')
@login_required
def admin_user_detail(user_id):
    
    if current_user.email != ADMIN_LOGIN:
        abort(403)
    
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    
    
    devices = UserDevice.query.filter_by(user_id=user_id).all()
    
   
    configurations = Configuration.query.filter_by(user_id=user_id).all()
    
    
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


@_routes.route('/users', methods=['GET'])
@login_required
def users_manage():
    # only Designer accounts can manage users
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)

    # users created under current_user
    users = db.session.execute(
        select(User).where(User.parent_user_id == current_user.id).order_by(User.email)
    ).scalars().all()

    # configs owned by current_user (only these can be shared)
    cfgs = db.session.execute(
        select(Configuration).where(Configuration.user_id == current_user.id).order_by(Configuration.name)
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

    return render_template('users_manage.html', users=users, configs=cfgs, access_map=access_map)


@_routes.route('/users/create', methods=['POST'])
@login_required
def users_create():
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)

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
        parent_user_id=current_user.id,
        can_designer=bool(request.form.get('can_designer')),
        can_client=bool(request.form.get('can_client')),
        can_api=bool(request.form.get('can_api')),
    )
    db.session.add(u)
    db.session.commit()

    # config access (only configs owned by current_user)
    cfg_ids = request.form.getlist('config_ids')
    owned_cfgs = db.session.execute(select(Configuration.id).where(Configuration.user_id == current_user.id)).scalars().all()
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

    flash('Пользователь создан', 'success')
    return redirect(url_for('users_manage'))


@_routes.route('/users/<int:user_id>/update', methods=['POST'])
@login_required
def users_update(user_id: int):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)

    u = db.session.get(User, user_id)
    if not u or u.parent_user_id != current_user.id:
        abort(404)

    u.can_designer = bool(request.form.get('can_designer'))
    u.can_client = bool(request.form.get('can_client'))
    u.can_api = bool(request.form.get('can_api'))

    new_pwd = (request.form.get('password') or '').strip()
    if new_pwd:
        u.password = generate_password_hash(new_pwd)

    # rewrite config access set
    cfg_ids = request.form.getlist('config_ids')
    owned_cfgs = db.session.execute(select(Configuration.id).where(Configuration.user_id == current_user.id)).scalars().all()
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

    flash('Права обновлены', 'success')
    return redirect(url_for('users_manage'))


@_routes.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
def users_delete(user_id: int):
    if not bool(getattr(current_user, 'can_designer', False)):
        abort(403)
    u = db.session.get(User, user_id)
    if not u or u.parent_user_id != current_user.id:
        abort(404)
    db.session.delete(u)
    db.session.commit()
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
    name = request.form.get('name', 'New room')
    transport = (request.form.get('transport') or 'websocket').strip().lower()
    if transport not in ('websocket', 'fcm'):
        transport = 'websocket'
    new_room = Room(
        name=name,
        transport=transport,
        user_id=current_user.id
    )
    db.session.add(new_room)
    db.session.commit()
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
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first_or_404()

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


@_routes.route('/delete-room/<room_uid>')
@login_required
def delete_room(room_uid):
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first_or_404()
    db.session.delete(room)
    db.session.commit()
    
    
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        if room.uid in tasks_db:
            del tasks_db[room.uid]
        tasks_db.commit()
    
    return redirect(url_for('dashboard') + '#rooms')


@_routes.route('/dashboard')
@login_required
def dashboard():
    stmt = select(Configuration).where(Configuration.user_id == current_user.id)
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
        'source_config_uid': request.form.get('source_config_uid'),
        'class_name': request.form.get('class_name'),
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
        'source_config_uid': request.form.get('source_config_uid'),
        'class_name': request.form.get('class_name'),
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




MOVED_EDITOR_NAMES = ['b64decode_filter', 'before_request', 'update_config_timestamp', 'ANDROID_IMPORTS_TEMPLATE', 'DEEPSEEK_API_URL', 'LANGUAGES', 'LMSTUDIO_API_KEY', 'LMSTUDIO_API_URL', 'LMSTUDIO_MODEL', 'NODE_CLASS_CODE', 'NODE_CLASS_CODE_ANDROID', 'PLUGIN_TEMPLATES', 'UI_COMPONENT_TEMPLATES', 'WIZARD_ACTIVE_TEMPLATES', 'WIZARD_COVER_TEMPLATES', '_enforce_web_access_modes', 'admin_dashboard', 'admin_toggle_user_active', 'admin_user_detail', 'choose_mode', 'contracts_create', 'contracts_delete', 'contracts_page', 'contracts_qr', 'contracts_update', 'create_room', 'dashboard', 'delete_room', 'edit_profile', 'generate_qr_code', 'get_default_server_handlers', 'get_locale', 'get_plugin_templates', 'get_timezone', 'get_ui_component_templates', 'get_wizard_active_templates', 'get_wizard_cover_templates', 'index', 'init_editor_ui', 'logout', 'room_detail', 'set_language', 'update_device_token', 'users_create', 'users_delete', 'users_manage', 'users_update', 'utility_processor', 'ALLOWED_INPUT_TYPES_AI', 'ALLOWED_UI_TYPES_AI', 'CONTAINER_UI_TYPES_AI', '_PY_SCRIPT_UPLOAD_SESSION_KEY', '_ShowPlugInLiteralValidatorAI', '_action_python_text_value', '_call_llm_code_only', '_carry_existing_event_python_script_refs', '_decode_b64_py', '_decode_b64_text', '_deep_merge_dict_keep_existing', '_encode_b64_py', '_encode_b64_text', '_generate_handlers_body_ai', '_is_remote_script_ref', '_iter_layout_elements_ai', '_last_python_script_upload_url', '_merge_class', '_normalize_event_action_python_scripts_for_save', '_normalize_python_script_text_for_save', '_remember_python_script_upload', '_s3_text_content_type', '_save_python_text_to_s3_via_upload_url', '_split_commands_str', '_split_handlers_header_and_body', '_upsert_list_by_key_keep_missing', '_wiz_active_field_to_json', '_wiz_build_active_table', '_wiz_build_cover_table', '_wiz_cover_field_to_json', '_wiz_cover_row_to_simple', '_wiz_json_field_to_simple', '_wiz_norm_id', '_wiz_parse_fn_call', '_wiz_parse_line_spec', '_wiz_parse_select', '_wiz_split_once_top_level', '_wiz_split_top_level', '_wiz_table_to_simple', '_wiz_unquote', '_wizard_build_active_field', '_wizard_build_cover_field', '_wizard_build_table', '_wizard_normalize_id', '_wizard_parse_fn_call', '_wizard_parse_select', '_wizard_split_once_top_level', '_wizard_split_top_level', 'add_class', 'add_config_event', 'add_dataset', 'add_event', 'add_method', 'add_method_to_class', 'add_new_method_to_class', 'add_section', 'ai_generate', 'ai_generate_layout', 'apply_full_config_from_json', 'call_deepseek', 'call_llm', 'call_lmstudio', 'clear_handlers', 'clear_server_handlers', 'code_editor', 'create_class', 'create_config', 'create_debug_room', 'create_room_alias', 'create_server', 'debug_room', 'delete_class', 'delete_config', 'delete_config_event', 'delete_dataset', 'delete_event', 'delete_method', 'delete_room_alias', 'delete_s3_text', 'delete_section', 'delete_server', 'download_handlers', 'download_server_handlers', 'edit_class', 'edit_config', 'edit_config_event', 'edit_dataset', 'edit_event', 'edit_method', 'ensure_all_classes_present_in_handlers', 'ensure_class_stub_in_module', 'ensure_handlers_skeleton_and_headers', 'export_class_json', 'export_config', 'extract_functions_from_handlers', 'extract_json_array_from_text', 'extract_json_from_text', 'extract_method_body_from_code', 'extract_method_names_ai', 'get_config_event_json', 'get_config_methods', 'get_dataset_json', 'get_method_body', 'get_s3_text_upload_url', 'get_section_json', 'get_user_local_time', 'import_config', 'import_config_new', 'layout_to_simplified_markup', 'layout_wizard', 'merge_llm_config_into_current_ai', 'method_exists_in_code', 'python_s3_editor', 'read_s3_text', 'remove_class_from_module', 'remove_method_from_code', 'remove_method_from_module', 'save_common_layouts', 'save_method', 'save_s3_text_via_upload_url', 'simplified_markup_to_layout', 'split_handlers_by_immutable_prefix_ai', 'sync_android_methods_from_code', 'sync_classes_from_android_handlers', 'sync_classes_from_server_handlers', 'sync_methods_from_code', 'sync_server_methods_from_code', 'update_config', 'update_dataset', 'update_existing_method', 'update_handlers_code', 'update_room_alias', 'update_section', 'update_server', 'update_server_handlers_code', 'upload_handlers', 'upload_s3_text', 'upload_server_handlers', 'validate_cover_images_ai', 'validate_full_llm_config_ai', 'validate_handlers_semantics_ai', 'validate_layout_types_ai', 'validate_python_syntax', 'validate_sections_ai', 'validate_sections_command_targets_ai', 'validate_show_plugin_literals_ai']
