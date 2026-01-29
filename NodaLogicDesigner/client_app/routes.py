from __future__ import annotations

import json
import os
import pickle
import sqlite3
import base64
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, url_for, after_this_request
from markupsafe import escape
from flask_login import current_user, login_required

from .nodalayout import render_nodalayout_html, DEFAULT_NL_CSS
from . import models
import nodes as _nodes_mod
import hashlib
import inspect
from sqlalchemy import select

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

# in-memory parsed config cache (per repo)
CONFIG_MEM: Dict[int, Dict[str, Any]] = {}
# in-memory cache for exec()'ed server handlers modules (per config_uid)
SERVER_HANDLERS_MEM: Dict[str, Dict[str, Any]] = {}
_SERVER_HANDLERS_NS_MEM: Dict[str, Dict[str, Any]] = {}
_SERVER_NODE_CLASS_MEM: Dict[Tuple[str, str, str], Any] = {}


# -------- client settings (stored in client.sqlite) --------

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

def _node_cover_html(repo: models.Repo, class_name: str, node_id: str, mode: str = "") -> str:

    # NOTE: `mode` is kept for backward/forward compatibility.
    # Some callers (e.g. Table rows) may pass mode="table" to request a more
    # compact look in the future. Currently we render the same cover.
    data = _fetch_node_data_for_repo(repo, class_name, node_id)

    try:
        cov = data.get("_cover") if isinstance(data, dict) else None
        if cov:
            if isinstance(cov, (dict, list)):
                html = str(render_nodalayout_html(cov, data) or "").strip()
                if html:
                    return html
            elif isinstance(cov, str):
                s = cov.strip()
                # json layout as string
                if (s.startswith("[") or s.startswith("{")):
                    html = str(render_nodalayout_html(s, data) or "").strip()
                    if html:
                        return html
                # plain image src
                pic_layout = [[{"type": "Picture", "value": s, "width": -1}]]
                html = str(render_nodalayout_html(pic_layout, data) or "").strip()
                if html:
                    return html
    except Exception:
        pass

    
    try:
        parsed = get_parsed_config(repo, models.db) or {}
        cls = (parsed.get("classes") or {}).get(class_name) or {}

        cover_web_layout = (cls.get("display_image_web") or "").strip()
        cover_layout = cls.get("cover_image")  # может быть dict layout

        layout_to_use = None
        if cover_web_layout:
            layout_to_use = cover_web_layout
        elif cover_layout:
            layout_to_use = cover_layout

        if layout_to_use:
            
            html = str(render_nodalayout_html(layout_to_use, data) or "").strip()
            if html:
                return html
    except Exception:
        pass

    
    title = _pick_node_title(data)
    subtitle = f"{class_name}/{node_id}"

    if title:
        return (
            f'<div class="card"><div class="card-body p-2">'
            f'<div class="fw-semibold">{escape(title)}</div>'
            f'<div class="text-muted small">{escape(subtitle)}</div>'
            f'</div></div>'
        )
    return (
        f'<div class="card"><div class="card-body p-2">'
        f'<div class="fw-semibold">{escape(subtitle)}</div>'
        f'</div></div>'
    )


def _node_children_tree(repo: models.Repo, class_name: str, node_id: str) -> List[Dict[str, Any]]:
    """Build recursive tree for NodeChildren renderer. UID format supported: 'Class$Id'."""
    visited: set[tuple[str, str]] = set()

    def _parse_uid(s: str):
        s = str(s or "").strip()
        if "$" in s:
            c, i = s.split("$", 1)
            return c.strip(), i.strip()
        return "", s

    def build(cn: str, nid: str) -> List[Dict[str, Any]]:
        key = (cn, nid)
        if key in visited:
            return []
        visited.add(key)

        data = _fetch_node_data_for_repo(repo, cn, nid)
        children = data.get("_children") or []
        out: List[Dict[str, Any]] = []

        if isinstance(children, list):
            for ch in children:
                cc = ""
                ci = ""

                if isinstance(ch, dict):
                    cc = str(ch.get("class") or ch.get("_class") or "").strip()
                    ci = str(ch.get("id") or ch.get("_id") or "").strip()
                    if (not cc or not ci) and ch.get("uid"):
                        cc2, ci2 = _parse_uid(ch.get("uid"))
                        cc, ci = cc2, ci2

                elif isinstance(ch, str):
                    cc, ci = _parse_uid(ch)

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


def _nl_context(repo: models.Repo, *, class_name: str, node_id: str) -> Dict[str, Any]:
    def uid_resolve(uid: str):
        try:
            lst = _nodes_mod.from_uid([str(uid)], config_uid=str(repo.config_uid))
            if not lst:
                return ("", "")
            n = lst[0]

            
            d = None
            try:
                d = n._data_cache if hasattr(n, "_data_cache") else None
            except Exception:
                d = None
            if not isinstance(d, dict):
                try:
                    d = n._data if isinstance(getattr(n, "_data", None), dict) else None
                except Exception:
                    d = None

            cls = ""
            if isinstance(d, dict):
                cls = (d.get("_class") or d.get("class") or "").strip()

            if not cls:
                
                cls = (getattr(n, "_schema_class_name", "") or n.__class__.__name__).strip()

            return (cls, str(n._id))
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

    
    url = (request.host_url or "").rstrip("/") + f"/api/config/{cfg_obj.uid}"

    classes = []
    for c in cfg_obj.classes:
        classes.append({
            "name": c.name,
            "section": c.section,
            "section_code": c.section_code,
            "has_storage": c.has_storage,
            "display_name": c.display_name,
            "cover_image": c.cover_image,
            
            "display_image_web": getattr(c, "display_image_web", "") or "",
            "display_image_table": getattr(c, "display_image_table", "") or "",
            "commands": getattr(c, "commands", "") or "",
            "use_standard_commands": bool(getattr(c, "use_standard_commands", True)),
            "svg_commands": getattr(c, "svg_commands", "") or "",
            
            "migration_register_command": bool(getattr(c, "migration_register_command", False)),
            "migration_register_on_save": bool(getattr(c, "migration_register_on_save", False)),
            "migration_default_room_uid": getattr(c, "migration_default_room_uid", "") or "",
            "migration_default_room_alias": getattr(c, "migration_default_room_alias", "") or "",
            "class_type": c.class_type,
            "hidden": getattr(c, "hidden", False),
            "init_screen_layout": getattr(c, "init_screen_layout", "") or "",
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
                            "postExecuteMethod": a.post_execute_method,
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
        "CommonLayouts": getattr(cfg_obj, "common_layouts", None) or getattr(cfg_obj, "CommonLayouts", None) or [],
    }



def _handlers_file_path(config_uid: str) -> str:
    
    base_dir = os.path.dirname(os.path.abspath(__file__))  # client_app/
   
    root = os.path.abspath(os.path.join(base_dir, ".."))
    return os.path.join(root, "Handlers", config_uid, "handlers.py")

def _load_server_handlers_ns(config_uid: str) -> Dict[str, Any]:
    fp = _handlers_file_path(config_uid)
    if not os.path.isfile(fp):
        raise ValueError(f"Handlers file not found: {fp}")

    with open(fp, "r", encoding="utf-8") as f:
        code = f.read()

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
        from sqlalchemy import select
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
        classes = cfg.get("classes") or []
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


def _nodes_storage_page(config_uid: str, class_name: str, *, offset: int, limit: int, q: str = "") -> List[Dict[str, Any]]:
    """Read nodes directly from the same storage as /api/.../page (no HTTP call)."""
    storage_key = f"{class_name}_{config_uid}"
    db_path = os.path.join("node_storage", f"{storage_key}.sqlite")
    if not os.path.exists(db_path):
        return []

    table = "unnamed"
    q = (q or "").strip().lower()

    def unpack(blob):
        try:
            return pickle.loads(blob)
        except Exception:
            return None

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

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


def _fetch_nodes_for_class(repo: models.Repo, *, config_uid: str, class_name: str, q: str, limit: int) -> List[Dict[str, Any]]:
    """Fetch nodes either locally (same server) or remotely (repo.base_url override)."""
    # Default: no base_url override or points to this server -> read local storage.
    # If base_url is configured and does not match current host, do remote HTTP.
    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    if not base_url or base_url == current:
        return _nodes_storage_page(config_uid, class_name, offset=0, limit=limit, q=q)

    # Remote
    try:
        payload = _api_get_remote(
            repo,
            f"/api/config/{config_uid}/node/{class_name}/page",
            params={"offset": 0, "limit": limit, "q": q} if q else {"offset": 0, "limit": limit},
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
    sections = build_global_sections(repos, models.db)
    section_code = request.args.get("section", None)
    if section_code is None:
        section_code = sections[0]["code"] if sections else ""
    scode_url = section_code if section_code != "" else "__empty__"
    return redirect(url_for("client.section_view", section_code=scode_url))


@client_bp.route("/sections")
@login_required
def sections_home():
    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    sections = build_global_sections(repos, models.db) if repos else []
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
    first = sections[0]["code"]
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


def _node_local_update_data(config_uid: str, class_name: str, node_id: str, data: Dict[str, Any]) -> None:
    node_class = _load_server_node_class(config_uid, class_name)
    node = node_class.get(node_id, config_uid)
    if not node:
        raise ValueError("node not found")

    
    merged = dict(data or {})
    #merged["_id"] = node_id
    merged = dict(data or {})
    merged.setdefault("_class", class_name)

    
    try:
        node._data = merged 
    except Exception:
        node.update_data(merged)

    
    if hasattr(node, "_save") and callable(getattr(node, "_save")):
        node._save()


    
    node.update_data(data or {})
    #if hasattr(node, "_save") and callable(getattr(node, "_save")):
    #    node._save()


def _node_local_delete(config_uid: str, class_name: str, node_id: str) -> None:
    node_class = _load_server_node_class(config_uid, class_name)
    node = node_class.get(node_id, config_uid)
    if not node:
        return

    node.delete()


def _node_local_create(config_uid: str, class_name: str, initial_data: Optional[Dict[str, Any]] = None) -> str:

    node_class = _load_server_node_class(config_uid, class_name)

    data = initial_data or {}
    
    user_data = {k: v for k, v in data.items() if not str(k).startswith("_")}

    #node_id = (data.get("_id") or str(uuid.uuid4()))
    #node = node_class(node_id, config_uid)
    raw_id = data.get("_id")
    node_id = _nodes_mod.extract_internal_id(raw_id) if raw_id else str(uuid.uuid4())
    node = node_class(node_id, config_uid)
    if user_data:
        node.update_data(user_data)
        if hasattr(node, "_save") and callable(getattr(node, "_save")):
            node._save()
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
    _ctx_tokens = _nodes_mod.set_runtime_context(cfg_uid, parsed_ctx)

    @after_this_request
    def _reset_ctx(resp):
        _nodes_mod.reset_runtime_context(_ctx_tokens)
        return resp


    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if not base_url or base_url == current:
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
    sections = build_global_sections(repos, models.db)
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

    repos = models.Repo.query.filter_by(user_id=current_user.id).all()
    merged: List[Dict[str, Any]] = []
    any_desc = False

    classes_ui: List[Dict[str, Any]] = []
    std_map: Dict[Tuple[int, str], bool] = {}            # (repo_id, class)->use_standard_commands
    display_name_map: Dict[Tuple[int, str], str] = {}    # (repo_id, class)->display_name
    commands_map: Dict[Tuple[int, str], str] = {}        # (repo_id, class)->commands string

    table_headers: List[str] = []
    table_headers_set: set = set()

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

    def build_cover_html(cover_web_layout: Any, cover_layout: Any, data: Dict[str, Any]) -> str:
        """Cover renderer with per-node override via _data['_cover'].

        Priority:
        1) _cover in data
        2) display_image_web
        3) cover_image
        """
        # 1) _cover override
        try:
            cov = data.get("_cover") if isinstance(data, dict) else None
            if cov:
                if isinstance(cov, (dict, list)):
                    return str(render_nodalayout_html(cov, data) or "")
                if isinstance(cov, str):
                    s = cov.strip()
                    if s.startswith("[") or s.startswith("{"):
                        return str(render_nodalayout_html(s, data) or "")
                    pic_layout = [[{"type": "Picture", "value": s, "width": -1}]]
                    return str(render_nodalayout_html(pic_layout, data) or "")
        except Exception:
            pass

        # 2) class cover layouts (existing)
        try:
            if (str(cover_web_layout or "").strip()):
                return str(render_nodalayout_html(cover_web_layout, data) or "")
            return str(render_nodalayout_html(cover_layout, data) or "")
        except Exception:
            return ""

    start_menu_cmds_ui: List[Dict[str, Any]] = []

    for repo in repos:
        parsed = get_parsed_config(repo, models.db)
        if not parsed:
            continue

        classes_by_section = parsed["classes_by_section"]
        cls_in_section = classes_by_section.get(section_code, []) if section_code != "" else classes_by_section.get("", [])

        cls_in_section = [c for c in (cls_in_section or []) if not bool(c.get("hidden"))]

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

            classes_ui.append({
                "repo": repo.name,
                "repo_id": repo.id,
                "class": cn,
                "display_name": disp,
                "use_standard_commands": use_std,
                "commands": cmds,
                "repo_uid": repo.config_uid,
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
            if ctype == "custom_process":
                node_id = f"{repo.config_uid}${cn}"
                data = (c.get("_data") or {}).copy()
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

                # cover html for web (priority: display_image_web else cover_image)
                display_image_html = ""
                display_image_html = build_cover_html(cover_web_layout, cover_layout, data)
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
                    "is_custom_process": True,
                    "display_image_html": display_image_html,
                    "table_values": tv,
                    "use_standard_commands": bool(std_map.get((repo.id, cn), False)),
                    "repo_uid": repo.config_uid,
                })

                if "_sort_string_desc" in data:
                    any_desc = True
                continue

            # data_node
            nodes = _fetch_nodes_for_class(repo, config_uid=repo.config_uid, class_name=cn, q=q, limit=DEFAULT_LIMIT_PER_CLASS)
            for n in nodes:
                data = n.get("_data") or {}
                node_id = n.get("_id") or data.get("_id") or ""

                if isinstance(data, dict) and data.get("_hidden"):
                    continue

                # cover html for web (priority: display_image_web else cover_image)
                display_image_html = ""
                display_image_html = build_cover_html(cover_web_layout, cover_layout, data)
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
                    "is_custom_process": False,
                    "display_image_html": display_image_html,
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
        }
    })



@client_bp.route("/api/node/create", methods=["POST"])
@login_required
def api_node_create():
    payload = request.get_json(force=True) or {}
    repo_id = int(payload.get("repo_id") or 0)
    class_name = (payload.get("class_name") or "").strip()
    if not repo_id or not class_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    # deny bulk delete for custom_process classes (they are virtual, not deletable)
    parsed = get_parsed_config(repo, models.db)
    try:
        cmeta = (parsed or {}).get("classes", {}).get(class_name) if isinstance(parsed, dict) else None
        if isinstance(cmeta, dict) and (cmeta.get("class_type") or "data_node") == "custom_process":
            return jsonify({"ok": False, "error": "custom_process cannot be deleted"}), 400
    except Exception:
        pass

    base_url = (repo.base_url or "").strip().rstrip("/")
    current = (request.host_url or "").rstrip("/")

    try:
        if not base_url or base_url == current:
            
            node_id = _node_local_create(repo.config_uid, class_name, initial_data={})
        else:
            j = _api_post_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}", json_data={})
            node_id = None
            if isinstance(j, dict):
                node_id = (j.get("_id") or (j.get("_data") or {}).get("_id"))
            if not node_id:
                return jsonify({"ok": False, "error": "create: no node_id"}), 500

        return jsonify({"ok": True, "node_id": node_id})
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
        if isinstance(cmeta, dict) and (cmeta.get("class_type") or "data_node") == "custom_process":
            return jsonify({"ok": False, "error": "custom_process cannot be deleted"}), 400
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

    use_std = bool(cls.get("use_standard_commands"))
    is_custom_process = (cls.get("class_type") or "data_node") == "custom_process"
    has_onshowweb = any((ev.get("event") or "") == "onShowWeb" for ev in (cls.get("events") or []))

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
                node_data.setdefault("_id", node_id)
                node_data.setdefault("_class", class_name)
            else:
                n = _api_get_remote(repo, f"/api/config/{repo.config_uid}/node/{class_name}/{node_id}")
                node_data = (n.get("_data") or {}) if isinstance(n, dict) else {}

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
            show_register_command=bool(cls.get("migration_register_command")) and bool(use_std),
            default_room_uid=_resolve_class_default_room_uid(parsed, cls),
            initial_message=ui_message,
            ui_plugins=ui_plugins
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

    layout_html = ""
    if layout is not None:
        try:
            layout_html = render_nodalayout_html(layout, node_data if isinstance(node_data, dict) else {}, context=_nl_context(repo, class_name=class_name, node_id=node_id))
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
        show_register_command=bool(cls.get("migration_register_command")) and bool(use_std),
        default_room_uid=_resolve_class_default_room_uid(parsed, cls),
        initial_message=ui_message
    )



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
        # merge, не replace
        node._data.update(base)
    except Exception:
        pass



@client_bp.route("/api/node/event_web", methods=["POST"])
@login_required
def api_node_event_web():
    j = request.get_json(force=True) or {}

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
    is_custom_process = (eff_cls_cfg.get("class_type") or "data_node") == "custom_process"

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

            
            node_data = {}

        # -------- LOCAL --------
        else:
            node_class = _load_server_node_class(repo.config_uid, eff_class)

            if is_custom_process:
                node_data = (eff_cls_cfg.get("_data") or {}).copy()
                node_data.setdefault("_id", eff_id)
                node_data.setdefault("_class", eff_class)

                try:
                    node = node_class.get(eff_id, repo.config_uid)
                except Exception:
                    node = None
                if not node:
                    node = node_class(eff_id, repo.config_uid)

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
                for k in ("_ui_message", "_ui_dialog", "_ui_layout", "_ui_open", "_ui_close"):
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

        # dialog render (same as in api_node_event_web, but data for vars is just payload)
        if ui_dialog is not None and isinstance(ui_dialog, dict):
            dlg_layout_html = ""
            if ui_dialog.get("layout") is not None:
                try:
                    dlg_layout_html = render_nodalayout_html(
                        ui_dialog.get("layout"),
                        payload if isinstance(payload, dict) else {},
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

        return jsonify(resp)

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": [{"text": f"CommonEvent error: {e}", "level": "error"}],
        }), 200

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

    if not repo_id or not class_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    nodes = _fetch_nodes_for_class(repo, config_uid=repo.config_uid, class_name=class_name, q=q, limit=limit) or []

    items = []
    for n in nodes:
        data = n.get("_data") or {}
        nid = n.get("_id") or data.get("_id") or ""
        if not nid:
            continue
        view = data.get("_view") or data.get("title") or nid
        items.append({
            "uid": f"{class_name}${nid}",
            "_id": str(nid),
            "_class": class_name,
            "_view": str(view),
            "data": data,
        })

    return jsonify({"ok": True, "items": items})

@client_bp.route("/api/dataset_items")
@login_required
def api_dataset_items():
    repo_id = int(request.args.get("repo_id") or 0)
    ds_name = (request.args.get("dataset") or "").strip()
    q = (request.args.get("q") or "").strip().lower()
    limit = int(request.args.get("limit") or 100)

    if not repo_id or not ds_name:
        return jsonify({"ok": False, "error": "bad args"}), 400

    # repo -> config_id
    repo = models.Repo.query.filter_by(id=repo_id, user_id=current_user.id).first()
    if not repo:
        return jsonify({"ok": False, "error": "repo not found"}), 404

    # Dataset is bound to configuration
    # IMPORTANT: pick correct config_id field depending on your Repo model
    #
    cfg_uid = (repo.config_uid or "").strip()
    if not cfg_uid:
        return jsonify({"ok": False, "error": "repo has no config_uid"}), 400

    cfg = main.Configuration.query.filter_by(uid=cfg_uid).first()
    if not cfg:
        return jsonify({"ok": False, "error": "configuration not found"}), 404

    ds = main.Dataset.query.filter_by(config_id=cfg.id, name=ds_name).first()


    if not ds:
        return jsonify({"ok": False, "error": "dataset not found"}), 404

    # helper: render view_template like "Item: @name (@code)"
    tmpl_re = re.compile(r"\{([A-Za-z0-9_]+)\}")
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