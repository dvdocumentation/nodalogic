# -*- coding: utf-8 -*-
"""NodaLogic MCP runtime bridge.

One MCP credential is bound to one deployed Configuration.  It exposes only
runtime data/methods/nGenie and, when explicitly enabled, the existing client
extension layer.  It never edits the base configuration schema or handlers.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, Response, abort, g, jsonify, request, url_for, render_template
from flask_login import current_user, login_required
from sqlalchemy import select, func

from extensions import db
from models import (
    MCPAccessLink, Configuration, ConfigClass, ConfigSection, ClassMethod,
    User, UserConfigAccess, ClientExtensionState, RawNode,
    Room, RoomDevice, RoomAlias, RoomObjects,
    MessageGroup, MessageGroupMember, Contract,
)

_bp = Blueprint("mcp_runtime", __name__)
MAIN = None

SUPPORTED_HANDSHAKE_VERSIONS = {"2025-03-26", "2025-06-18", "2025-11-25", "2024-11-05"}
LATEST_HANDSHAKE_VERSION = "2025-11-25"
MODERN_VERSION = "2026-07-28"
SERVER_INFO = {"name": "NodaLogic MCP", "version": "1.1"}


def register_mcp(app, main_module):
    global MAIN
    MAIN = main_module
    app.register_blueprint(_bp)


def _utcnow():
    return datetime.now(timezone.utc)


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(str(secret or "").encode("utf-8")).hexdigest()


def _new_secret() -> str:
    return "nlmcp_" + secrets.token_urlsafe(40)


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _jsonrpc_result(req_id, result: Any, status: int = 200, extra_headers: Optional[Dict[str, str]] = None):
    resp = jsonify({"jsonrpc": "2.0", "id": req_id, "result": result})
    resp.status_code = status
    for k, v in (extra_headers or {}).items():
        resp.headers[k] = v
    return resp


def _jsonrpc_error(req_id, code: int, message: str, data: Any = None, status: int = 200):
    err = {"code": int(code), "message": str(message)}
    if data is not None:
        err["data"] = data
    resp = jsonify({"jsonrpc": "2.0", "id": req_id, "error": err})
    resp.status_code = status
    return resp


def _tool_result(value: Any, is_error: bool = False):
    text = value if isinstance(value, str) else _json_text(value)
    out = {"content": [{"type": "text", "text": text}], "isError": bool(is_error)}
    if not isinstance(value, str):
        out["structuredContent"] = value
    return out


def _owner_can_manage(config: Configuration) -> bool:
    return bool(getattr(current_user, "is_authenticated", False) and int(current_user.id) == int(config.user_id))


def _user_has_config(user: User, config: Configuration) -> bool:
    if not user or not config:
        return False
    if int(user.id) == int(config.user_id):
        return True
    return UserConfigAccess.query.filter_by(user_id=user.id, config_id=config.id).first() is not None


def _candidate_users(config: Configuration) -> List[User]:
    ids = {int(config.user_id)}
    for row in UserConfigAccess.query.filter_by(config_id=config.id).all():
        ids.add(int(row.user_id))
    # Tenant children are useful even before access is granted; filter to actual config access below.
    out = []
    for uid in sorted(ids):
        u = db.session.get(User, uid)
        if u and _user_has_config(u, config):
            out.append(u)
    return out


def _resolve_link_by_secret(secret: str, config_uid: str = "") -> Tuple[Optional[MCPAccessLink], Optional[Configuration], Optional[User], str]:
    secret = str(secret or "").strip()
    if not secret:
        return None, None, None, "missing credential"
    digest = _hash_secret(secret)
    link = MCPAccessLink.query.filter_by(token_hash=digest, revoked_at=None).first()
    if not link:
        return None, None, None, "invalid or revoked credential"
    config = db.session.get(Configuration, link.config_id)
    if not config or (config_uid and str(config.uid) != str(config_uid)):
        return None, None, None, "credential is not valid for this configuration"
    actor = db.session.get(User, link.user_id) if link.access_mode == "user" and link.user_id else db.session.get(User, config.user_id)
    if not actor or not _user_has_config(actor, config):
        return None, None, None, "effective user no longer has access to this configuration"
    link.last_used_at = _utcnow()
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
    return link, config, actor, ""


def _request_secret(path_secret: str = "") -> str:
    if path_secret:
        return path_secret
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return request.headers.get("X-MCP-Token", "").strip()


def _audit(link: MCPAccessLink, actor: User, tool: str, ok: bool, *, class_name: str = "", node_id: str = "", method_name: str = "", detail: str = ""):
    try:
        from models import MCPAuditLog
        row = MCPAuditLog(
            link_id=link.id,
            config_id=link.config_id,
            effective_user_id=getattr(actor, "id", None),
            tool_name=str(tool or "")[:120],
            class_name=str(class_name or "")[:120],
            node_id=str(node_id or "")[:255],
            method_name=str(method_name or "")[:120],
            success=bool(ok),
            detail=str(detail or "")[:2000],
            created_at=_utcnow(),
        )
        db.session.add(row)
        db.session.commit()
    except Exception:
        db.session.rollback()


def _class_methods(cls: ConfigClass) -> List[Dict[str, Any]]:
    return [
        {
            "name": m.name,
            "source": m.source or "internal",
            "server": m.server or "",
            "engine": m.engine or "",
            "description": getattr(m, "ngenie_description", "") or "",
        }
        for m in (cls.methods or [])
    ]


def _class_description(cls: ConfigClass) -> Dict[str, Any]:
    indexes = cls.indexes_json if isinstance(cls.indexes_json, list) else []
    return {
        "name": cls.name,
        "display_name": cls.display_name or cls.name,
        "class_type": cls.class_type or "",
        "section": cls.section or "",
        "section_code": cls.section_code or "",
        "has_storage": bool(cls.has_storage),
        "ngenie_role": cls.ngenie_role or "",
        "ngenie_description": cls.ngenie_description or "",
        "ngenie_prompt": cls.ngenie_prompt or "",
        "data_structure": cls.data_structure or "",
        "indexes": indexes,
        "methods": _class_methods(cls),
        "allow_client_extensions": getattr(cls, "allow_client_extensions", True) is not False,
    }


def _visible_classes(config: Configuration, actor: User) -> List[ConfigClass]:
    rows = ConfigClass.query.filter_by(config_id=config.id).order_by(ConfigClass.id.asc()).all()
    out = []
    for cls in rows:
        try:
            if MAIN.user_can_access_class(actor, config.uid, cls.name):
                out.append(cls)
        except Exception:
            continue
    return out


def _runtime_context(config: Configuration, actor: User):
    parsed = MAIN._build_runtime_parsed_config(config)
    system_user = MAIN._resolve_request_system_user_payload(actor)
    return MAIN._nodes_mod.set_runtime_context(config.uid, parsed, system_user=system_user)


def _node_class(config: Configuration, actor: User, class_name: str):
    if not MAIN.user_can_access_class(actor, config.uid, class_name):
        raise PermissionError("class is not available to this user")
    ns = MAIN._load_server_handlers_ns(config.uid, config)
    cls = ns.get(class_name)
    if cls is None or not hasattr(cls, "__bases__") or not any(base.__name__ == "Node" for base in cls.__bases__):
        raise LookupError("class runtime handler not found")
    return cls


def _node_payload_allowed(actor: User, config: Configuration, class_name: str, node_id: str, payload: Dict[str, Any]) -> bool:
    data = payload.get("_data", {}) if isinstance(payload, dict) else {}
    return bool(MAIN.user_can_access_node(actor, config.uid, class_name, node_id, data))


def _list_nodes(config: Configuration, actor: User, class_name: str, limit: int = 100, filters: Optional[Dict[str, Any]] = None):
    cls = _node_class(config, actor, class_name)
    limit = max(1, min(int(limit or 100), 1000))
    filters = filters if isinstance(filters, dict) else {}
    out = []
    for node_id, node in cls.get_all(config.uid).items():
        payload = node.to_dict()
        if not _node_payload_allowed(actor, config, class_name, str(node_id), payload):
            continue
        data = payload.get("_data", {}) or {}
        if filters and any(str(data.get(k)) != str(v) for k, v in filters.items()):
            continue
        out.append(payload)
        if len(out) >= limit:
            break
    return out


def _get_node(config: Configuration, actor: User, class_name: str, node_id: str):
    cls = _node_class(config, actor, class_name)
    internal = MAIN.extract_internal_id(str(node_id))
    node = cls.get(internal, config.uid)
    if not node:
        raise LookupError("node not found")
    payload = node.to_dict()
    if not _node_payload_allowed(actor, config, class_name, internal, payload):
        raise PermissionError("node is not available to this user")
    return node, payload


def _find_nodes(config: Configuration, actor: User, args: Dict[str, Any]):
    class_name = str(args.get("class_name") or "").strip()
    limit = max(1, min(int(args.get("limit") or 20), 200))
    filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
    query = str(args.get("query") or "").strip().lower()
    index_name = str(args.get("index_name") or "").strip()
    index_value = args.get("index_value", args.get("query"))
    cls = _node_class(config, actor, class_name)
    candidates = None
    if index_name:
        # Use the same runtime index API when the generated Node class exposes it.
        for fn_name in ("find_by_index", "findByIndex", "get_by_index", "getByIndex"):
            fn = getattr(cls, fn_name, None)
            if callable(fn):
                try:
                    got = fn(index_name, index_value, config.uid)
                except TypeError:
                    try:
                        got = fn(index_name, index_value)
                    except Exception:
                        continue
                except Exception:
                    continue
                if isinstance(got, dict):
                    candidates = list(got.items())
                elif isinstance(got, (list, tuple)):
                    candidates = []
                    for item in got:
                        nid = getattr(item, "_id", None) or getattr(item, "id", None) or str(uuid.uuid4())
                        candidates.append((nid, item))
                elif got is not None:
                    nid = getattr(got, "_id", None) or getattr(got, "id", None) or str(uuid.uuid4())
                    candidates = [(nid, got)]
                break
    if candidates is None:
        candidates = list(cls.get_all(config.uid).items())
    out = []
    for node_id, node in candidates:
        if not hasattr(node, "to_dict"):
            continue
        payload = node.to_dict()
        if not _node_payload_allowed(actor, config, class_name, str(node_id), payload):
            continue
        data = payload.get("_data", {}) or {}
        if filters and any(str(data.get(k)) != str(v) for k, v in filters.items()):
            continue
        if query:
            hay = json.dumps(data, ensure_ascii=False, default=str).lower()
            if query not in hay:
                continue
        out.append(payload)
        if len(out) >= limit:
            break
    return out


def _declared_method(config: Configuration, class_name: str, method_name: str) -> bool:
    cls = ConfigClass.query.filter_by(config_id=config.id, name=class_name).first()
    if not cls:
        return False
    return any(str(m.name) == str(method_name) for m in (cls.methods or []))


def _ngenie_call(config: Configuration, actor: User, args: Dict[str, Any], action_mode: bool):
    """Reuse the existing web nGenie route in an isolated internal request context.

    This deliberately keeps one nGenie implementation.  Flask-Login is supplied
    with the effective MCP actor only inside the nested request.
    """
    from flask_login import login_user, logout_user
    from client_app import routes as cr
    payload = {
        "message": str(args.get("message") or "").strip(),
        "config_uid": config.uid,
        "chat_history": args.get("chat_history") or [],
        "conversation_artifact": args.get("conversation_artifact") or {},
        "clarification_response": args.get("clarification_response"),
        "scope": str(args.get("scope") or "mcp"),
        "allow_catalog_create": bool(args.get("allow_catalog_create")) if action_mode else False,
        "debug": bool(args.get("debug")),
    }
    if not payload["message"]:
        raise ValueError("message is required")
    with MAIN.app.test_request_context("/client/api/ngenie/chat", method="POST", json=payload):
        login_user(actor, force=True)
        try:
            if not action_mode:
                # Read-only nGenie mode: use the same semantic context/router, but
                # stop before operation execution.  The returned plan/reply may
                # guide the external agent to the ordinary read tools.
                attachments = cr._ngenie_prepare_attachments_for_chat([], config.uid, payload["message"])
                messages = cr._ngenie_build_messages(
                    payload["message"], config.uid, None, allow_catalog_create=False,
                    clarification_response=payload.get("clarification_response"),
                    scope=payload.get("scope") or "mcp", attachments=attachments,
                    conversation_history=payload.get("chat_history") or [],
                    conversation_artifact=payload.get("conversation_artifact") or {},
                )
                answer = cr._ngenie_call_deepseek(messages)
                if not isinstance(answer, dict):
                    answer = {"reply": str(answer or "")}
                return {
                    "ok": True,
                    "read_only": True,
                    "reply": str(answer.get("reply") or ""),
                    "clarification_requests": answer.get("clarification_requests") or answer.get("clarifications") or [],
                    "display_requests": answer.get("display_requests") or answer.get("displayRequests") or [],
                    "data_requests": answer.get("data_requests") or answer.get("dataRequests") or [],
                    "raw": answer if bool(args.get("debug")) else None,
                }
            fn = getattr(cr.api_ngenie_chat, "__wrapped__", cr.api_ngenie_chat)
            response = fn()
            if isinstance(response, tuple):
                response = response[0]
            data = response.get_json() if hasattr(response, "get_json") else response
            return data
        finally:
            try:
                logout_user()
            except Exception:
                pass


def _extension_state(user_id: int) -> Dict[str, Any]:
    from client_app import routes as cr
    return cr._client_extension_load_state(user_id=user_id)


def _extension_save(user_id: int, state: Dict[str, Any]):
    from client_app import routes as cr
    return cr._client_extension_save_state(state, user_id=user_id)


def _extension_section_allowed(config: Configuration, state: Dict[str, Any], section_code: str) -> bool:
    if any(str(x.get("id") or x.get("code")) == section_code for x in state.get("sections", []) if isinstance(x, dict)):
        return True
    sec = ConfigSection.query.filter_by(config_id=config.id, code=section_code).first()
    return bool(sec and getattr(sec, "allow_client_extensions", True) is not False)


def _extension_layout_from_fields(fields: Any) -> List[Any]:
    if not isinstance(fields, list):
        return []
    rows = []
    for raw in fields:
        if not isinstance(raw, dict):
            continue
        fid = str(raw.get("name") or raw.get("id") or "").strip()
        if not fid:
            continue
        typ = str(raw.get("type") or "string").strip().lower()
        field = {"id": fid, "caption": str(raw.get("caption") or raw.get("label") or fid), "value": "@" + fid, "extension_field": True, "extension_plain": True}
        if typ in ("number", "numeric", "float", "int", "integer"):
            field.update({"type": "Input", "input_type": "NUMBER"})
        elif typ in ("date", "datetime"):
            field.update({"type": "Input", "input_type": "DATE"})
        elif typ in ("bool", "boolean", "check"):
            field.update({"type": "CheckBox"})
        elif typ in ("node", "reference", "link"):
            field.update({"type": "NodeInput", "dataset": str(raw.get("class") or raw.get("dataset") or "")})
        elif typ in ("image", "picture", "photo"):
            field.update({"type": "ExtensionPicture", "height": 180, "extension_show_caption": bool(raw.get("show_caption"))})
        elif typ in ("tags", "tag"):
            field.update({"type": "ExtensionTags", "extension_show_caption": bool(raw.get("show_caption"))})
        elif typ in ("geotag", "geo", "coordinates"):
            field.update({"type": "ExtensionGeoTag"})
        elif typ in ("table", "rows"):
            field.update({"type": "Table", "table": True, "extension_table": True, "table_header": [], "virtual_node": {"layout": [], "cover": []}})
        else:
            field.update({"type": "Input"})
        if raw.get("cover"):
            field["extension_cover"] = True
        rows.append([field])
    return rows


def _extension_tools(config: Configuration, actor: User, name: str, args: Dict[str, Any]):
    from client_app import routes as cr
    uid = int(actor.id)
    state = _extension_state(uid)
    if name == "extensions_list":
        nodes = []
        for obj in RawNode.query.filter_by(owner_user_id=uid).order_by(RawNode.updated_at.desc()).limit(500).all():
            payload = obj.payload_json if isinstance(obj.payload_json, dict) else {}
            data = payload.get("_data", {}) if isinstance(payload, dict) else {}
            if data.get("_extension_managed"):
                nodes.append({"node_id": obj.node_id, "section_code": data.get("_extension_section"), "class_id": data.get("_extension_class_id"), "class_name": data.get("_extension_class_name"), "data": data})
        return {"sections": state.get("sections", []), "classes": state.get("classes", []), "nodes": nodes}
    if name == "extension_create_section":
        title = str(args.get("name") or "").strip()
        if not title:
            raise ValueError("name is required")
        sid = "ext_section_" + uuid.uuid4().hex[:16]
        row = {"id": sid, "code": sid, "name": title[:200], "is_extension": True, "allow_client_extensions": True}
        state.setdefault("sections", []).append(row)
        _extension_save(uid, state)
        return row
    if name == "extension_create_class":
        section_code = str(args.get("section_code") or "").strip()
        title = str(args.get("name") or "").strip()
        if not section_code or not title:
            raise ValueError("section_code and name are required")
        if not _extension_section_allowed(config, state, section_code):
            raise PermissionError("extensions are disabled for this section")
        cid = "ext_class_" + uuid.uuid4().hex[:16]
        layout = args.get("layout") if isinstance(args.get("layout"), list) else _extension_layout_from_fields(args.get("fields"))
        row = {"id": cid, "name": title[:200], "section": section_code, "layout": layout, "tables": args.get("tables") if isinstance(args.get("tables"), list) else [], "record_template": str(args.get("record_template") or "").strip(), "is_extension": True, "allow_client_extensions": True}
        state.setdefault("classes", []).append(row)
        _extension_save(uid, state)
        return row
    if name == "extension_update_class":
        cid = str(args.get("class_id") or "").strip()
        row = next((x for x in state.get("classes", []) if str(x.get("id")) == cid), None)
        if not row:
            raise LookupError("extension class not found")
        if "name" in args:
            row["name"] = str(args.get("name") or row.get("name"))[:200]
        if isinstance(args.get("layout"), list):
            row["layout"] = args["layout"]
        elif isinstance(args.get("fields"), list):
            row["layout"] = _extension_layout_from_fields(args["fields"])
        if "record_template" in args:
            row["record_template"] = str(args.get("record_template") or "")
        _extension_save(uid, state)
        return row
    if name == "extension_create_node":
        section_code = str(args.get("section_code") or "").strip()
        class_id = str(args.get("class_id") or "").strip()
        data_in = args.get("data") if isinstance(args.get("data"), dict) else {}
        if not section_code:
            raise ValueError("section_code is required")
        if not _extension_section_allowed(config, state, section_code):
            raise PermissionError("extensions are disabled for this section")
        class_meta = next((x for x in state.get("classes", []) if str(x.get("id")) == class_id), None) if class_id else None
        if class_id and not class_meta:
            raise LookupError("extension class not found")
        section_meta = next((x for x in state.get("sections", []) if str(x.get("id") or x.get("code")) == section_code), None)
        if section_meta is None:
            sec = ConfigSection.query.filter_by(config_id=config.id, code=section_code).first()
            section_meta = {"name": sec.name if sec else section_code}
        class_name = str((class_meta or {}).get("name") or "Arbitrary node")
        layout = (class_meta or {}).get("layout") if isinstance((class_meta or {}).get("layout"), list) else []
        nid = "ext_node_" + uuid.uuid4().hex
        data = dict(data_in)
        data.update({"_id": nid, "_extension_managed": True, "_extension_section": section_code, "_extension_section_name": str(section_meta.get("name") or section_code), "_extension_class_id": class_id or None, "_extension_class_name": class_name, "_extension_created_at": _utcnow().isoformat(), "_layout": layout})
        cr._client_extension_refresh_presentation_data(data, layout, managed=True, class_cfg=class_meta or {"name": class_name, "display_name": class_name})
        embedded = cr._client_extension_embedded_class(class_id, class_name, section_code, layout, str((class_meta or {}).get("record_template") or ""))
        payload = {"_id": nid, "_class": embedded, "_data": data}
        cr._client_extension_store_raw(nid, payload, uid)
        return payload
    if name in ("extension_update_node", "extension_delete_node"):
        nid = str(args.get("node_id") or "").strip()
        obj = RawNode.query.filter_by(node_id=nid, owner_user_id=uid).first()
        if not obj:
            raise LookupError("extension node not found")
        payload = obj.payload_json if isinstance(obj.payload_json, dict) else {}
        data = payload.get("_data", {}) if isinstance(payload, dict) else {}
        if not data.get("_extension_managed"):
            raise ValueError("not an extension node")
        if name == "extension_delete_node":
            db.session.delete(obj)
            db.session.commit()
            return {"status": "deleted", "node_id": nid}
        patch = args.get("data") if isinstance(args.get("data"), dict) else {}
        protected = {"_id", "_extension_managed", "_extension_section", "_extension_section_name", "_extension_class_id", "_extension_class_name"}
        for k, v in patch.items():
            if k not in protected:
                data[k] = v
        payload["_data"] = data
        obj.payload_json = payload
        obj.updated_at = _utcnow()
        db.session.commit()
        return payload
    if name in ("extension_delete_class", "extension_delete_section"):
        if name == "extension_delete_class":
            cid = str(args.get("class_id") or "").strip()
            before = len(state.get("classes", []))
            state["classes"] = [x for x in state.get("classes", []) if str(x.get("id")) != cid]
            if len(state["classes"]) == before:
                raise LookupError("extension class not found")
            state.setdefault("deleted_classes", []).append(cid)
            _extension_save(uid, state)
            return {"status": "deleted", "class_id": cid}
        sid = str(args.get("section_code") or "").strip()
        before = len(state.get("sections", []))
        state["sections"] = [x for x in state.get("sections", []) if str(x.get("id") or x.get("code")) != sid]
        if len(state["sections"]) == before:
            raise LookupError("extension section not found")
        removed_ids = [str(x.get("id")) for x in state.get("classes", []) if str(x.get("section")) == sid]
        state["classes"] = [x for x in state.get("classes", []) if str(x.get("section")) != sid]
        state.setdefault("deleted_sections", []).append(sid)
        state.setdefault("deleted_classes", []).extend(removed_ids)
        _extension_save(uid, state)
        return {"status": "deleted", "section_code": sid, "classes_removed": removed_ids}
    raise LookupError("unknown extension tool")


def _actor_user_key(actor: User) -> str:
    return str(getattr(actor, "email", "") or "").strip()


def _response_payload(value: Any) -> Any:
    """Unwrap a Flask response/tuple returned by existing NodaLogic helpers."""
    if isinstance(value, tuple):
        value = value[0] if value else None
    if hasattr(value, "get_json"):
        try:
            return value.get_json()
        except Exception:
            pass
    return value


def _actor_can_manage_rooms(config: Configuration, actor: User) -> bool:
    if not actor:
        return False
    try:
        if int(actor.id) == int(config.user_id):
            return True
    except Exception:
        pass
    try:
        same_tenant = int(getattr(actor, "parent_user_id", 0) or 0) == int(config.user_id)
    except Exception:
        same_tenant = False
    return bool(same_tenant and (getattr(actor, "can_designer", False) or getattr(actor, "can_manage_rooms", False)))


def _config_room_refs(config: Configuration) -> Dict[str, Dict[str, Any]]:
    refs: Dict[str, Dict[str, Any]] = {}
    for alias in RoomAlias.query.filter_by(config_id=config.id).order_by(RoomAlias.id.asc()).all():
        room_uid = str(alias.room_uid or "").strip()
        if not room_uid:
            continue
        row = refs.setdefault(room_uid, {"aliases": [], "class_defaults": []})
        row["aliases"].append(str(alias.alias or "").strip())
    for cls in ConfigClass.query.filter_by(config_id=config.id).order_by(ConfigClass.id.asc()).all():
        room_uid = str(getattr(cls, "migration_default_room_uid", "") or "").strip()
        if not room_uid:
            continue
        row = refs.setdefault(room_uid, {"aliases": [], "class_defaults": []})
        row["class_defaults"].append(cls.name)
    return refs


def _room_to_dict(room: Room, config: Configuration, refs: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    refs = refs if isinstance(refs, dict) else _config_room_refs(config)
    meta = refs.get(str(room.uid), {"aliases": [], "class_defaults": []})
    devices = RoomDevice.query.filter_by(room_uid=room.uid).order_by(RoomDevice.last_seen.desc()).all()
    return {
        "uid": room.uid,
        "name": room.name or "",
        "transport": room.transport or "websocket",
        "aliases": meta.get("aliases", []),
        "class_defaults": meta.get("class_defaults", []),
        "device_count": len(devices),
        "devices": [
            {
                "device_uid": d.device_uid,
                "user_key": d.user_key or "",
                "push_channel": d.push_channel or "",
                "device_model": d.device_model or "",
                "last_seen": d.last_seen.isoformat() if d.last_seen else None,
            }
            for d in devices[:100]
        ],
    }


def _list_rooms(config: Configuration, actor: User) -> List[Dict[str, Any]]:
    refs = _config_room_refs(config)
    manage = _actor_can_manage_rooms(config, actor)
    rows = Room.query.filter_by(user_id=config.user_id).order_by(Room.created_at.desc(), Room.id.desc()).all()
    if not manage:
        rows = [room for room in rows if str(room.uid) in refs]
    return [_room_to_dict(room, config, refs) for room in rows]


def _get_room_for_mcp(config: Configuration, actor: User, room_uid: str, *, require_manage: bool = False) -> Room:
    room_uid = str(room_uid or "").strip()
    if not room_uid:
        raise ValueError("room_uid is required")
    room = Room.query.filter_by(uid=room_uid, user_id=config.user_id).first()
    if not room:
        raise LookupError("room not found in this NodaLogic tenant")
    if require_manage:
        if not _actor_can_manage_rooms(config, actor):
            raise PermissionError("effective user cannot manage rooms")
        return room
    if _actor_can_manage_rooms(config, actor):
        return room
    if room_uid not in _config_room_refs(config):
        raise PermissionError("room is not attached to this configuration")
    return room


def _messaging_targets(config: Configuration, actor: User) -> Dict[str, Any]:
    user_ids = {int(config.user_id)}
    for row in UserConfigAccess.query.filter_by(config_id=config.id).all():
        user_ids.add(int(row.user_id))
    users = []
    for uid in sorted(user_ids):
        user = db.session.get(User, uid)
        if not user:
            continue
        aliases = []
        for rd in RoomDevice.query.filter_by(user_id=user.id).order_by(RoomDevice.last_seen.desc()).limit(100).all():
            key = str(rd.user_key or "").strip()
            if key and key.lower() not in {x.lower() for x in aliases}:
                aliases.append(key)
        users.append({
            "id": user.id,
            "user_key": user.email or "",
            "display_name": user.config_display_name or user.email or "",
            "aliases": aliases,
        })

    actor_key = _actor_user_key(actor)
    groups = []
    if actor_key:
        rows = MessageGroup.query.join(
            MessageGroupMember,
            MessageGroupMember.group_id == MessageGroup.group_id,
        ).filter(
            func.lower(MessageGroupMember.user_key) == actor_key.lower()
        ).order_by(MessageGroup.updated_at.desc(), MessageGroup.created_at.desc()).all()
        groups = [MAIN._serialize_group(gp, include_members=True) for gp in rows]
    return {"users": users, "groups": groups, "rooms": _list_rooms(config, actor)}


def _require_group_access(actor: User, group_id: str) -> MessageGroup:
    group_id = str(group_id or "").strip()
    group = MessageGroup.query.filter_by(group_id=group_id).first()
    if not group:
        raise LookupError("message group not found")
    if not MAIN._user_can_access_group(_actor_user_key(actor), group_id):
        raise PermissionError("effective user is not a member of this message group")
    return group


def _messaging_tools(config: Configuration, actor: User, name: str, args: Dict[str, Any]):
    actor_key = _actor_user_key(actor)
    if not actor_key:
        raise PermissionError("effective user has no messaging user key")

    if name == "list_message_targets":
        return _messaging_targets(config, actor)

    if name == "list_message_groups":
        return _messaging_targets(config, actor)["groups"]

    if name == "create_message_group":
        title = str(args.get("title") or "").strip()
        if not title:
            raise ValueError("title is required")
        members = MAIN._normalize_member_user_keys(args.get("members"), include_user_key=actor_key)
        if not members:
            raise ValueError("members must contain at least one user")
        group = MessageGroup(group_id=MAIN._make_group_id(), title=title[:255], created_by=actor_key)
        db.session.add(group)
        db.session.flush()
        for member_key in members:
            db.session.add(MessageGroupMember(group_id=group.group_id, user_key=member_key))
        db.session.commit()
        return MAIN._serialize_group(group, include_members=True)

    if name == "add_message_group_members":
        group = _require_group_access(actor, args.get("group_id"))
        members = MAIN._normalize_member_user_keys(args.get("members"))
        if not members:
            raise ValueError("members must be a non-empty list")
        existing = {x.lower() for x in MAIN._get_group_member_keys(group.group_id)}
        added = []
        for member_key in members:
            if member_key.lower() in existing:
                continue
            db.session.add(MessageGroupMember(group_id=group.group_id, user_key=member_key))
            existing.add(member_key.lower())
            added.append(member_key)
        group.updated_at = _utcnow()
        db.session.commit()
        return {"group": MAIN._serialize_group(group, include_members=True), "added_members": added}

    if name == "remove_message_group_member":
        group = _require_group_access(actor, args.get("group_id"))
        member_key = str(args.get("user_key") or "").strip()
        if not member_key:
            raise ValueError("user_key is required")
        members = MAIN._get_group_member_keys(group.group_id)
        match = next((x for x in members if x.lower() == member_key.lower()), None)
        if not match:
            raise LookupError("member not found")
        if len(members) <= 1:
            raise ValueError("group must have at least one member")
        row = MessageGroupMember.query.filter(
            MessageGroupMember.group_id == group.group_id,
            func.lower(MessageGroupMember.user_key) == member_key.lower(),
        ).first()
        if row:
            db.session.delete(row)
        group.updated_at = _utcnow()
        db.session.commit()
        return MAIN._serialize_group(group, include_members=True)

    if name == "get_group_messages":
        group = _require_group_access(actor, args.get("group_id"))
        payload, status = MAIN._get_group_messages_history_impl(
            group.group_id,
            limit=args.get("limit", 100),
            before=args.get("before"),
        )
        if int(status or 200) >= 400:
            raise RuntimeError(str(payload))
        return payload

    if name == "send_message":
        target_type = str(args.get("target_type") or "user").strip().lower()
        target_id = str(args.get("target_id") or "").strip()
        if not target_id:
            raise ValueError("target_id is required")
        title = str(args.get("title") or getattr(actor, "config_display_name", "") or actor_key or "NodaLogic")
        body = str(args.get("body") or args.get("message") or "").strip()
        if not body:
            raise ValueError("body is required")
        payload = args.get("data") if isinstance(args.get("data"), dict) else {}
        if target_type == "user":
            result = MAIN.send_message_to_user_global(target_id, title, body, payload, sender_user=actor_key)
        elif target_type == "group":
            group = _require_group_access(actor, target_id)
            result = MAIN.send_message_to_group_global(group.group_id, title, body, payload, sender_user=actor_key)
        elif target_type == "room":
            room = _get_room_for_mcp(config, actor, target_id)
            result = MAIN.notify_room_transport(room.uid, title=title, body=body, data_payload=payload)
        else:
            raise ValueError("target_type must be user, group or room")
        if not isinstance(result, dict) or not result.get("ok"):
            raise RuntimeError(str((result or {}).get("error") if isinstance(result, dict) else result))
        return {"target_type": target_type, "target_id": target_id, "result": result}

    if name == "send_node":
        target_type = str(args.get("target_type") or "user").strip().lower()
        target_id = str(args.get("target_id") or "").strip()
        class_name = str(args.get("class_name") or "").strip()
        node_id = str(args.get("node_id") or "").strip()
        if not target_id or not class_name or not node_id:
            raise ValueError("target_id, class_name and node_id are required")
        node, node_payload = _get_node(config, actor, class_name, node_id)
        node_payload = dict(node_payload or {})
        node_payload.setdefault("_id", (node_payload.get("_data") or {}).get("_id") or node_id)
        node_payload.setdefault("_class", f"{config.uid}${class_name}")

        if target_type == "room":
            room = _get_room_for_mcp(config, actor, target_id)
            result = _response_payload(MAIN.handle_room_objects(config.uid, class_name, room.uid, [node_payload]))
            if not isinstance(result, dict) or not result.get("queued"):
                raise RuntimeError(str(result))
            return {"target_type": "room", "target_id": room.uid, "node": node_payload, "result": result}

        if target_type == "group":
            group = _require_group_access(actor, target_id)
            group_id = group.group_id
        elif target_type == "user":
            group_id = None
        else:
            raise ValueError("target_type must be user, group or room")

        placed = MAIN._place_uploaded_node(MAIN._current_public_base_url(), node_payload, api_user=actor)
        title = str(args.get("title") or class_name or node_id or "Node")
        body = str(args.get("body") or args.get("text") or "Node")
        message_data = MAIN._build_node_message_payload(
            class_name=placed.get("_class_name") or class_name,
            node_id=placed.get("_id") or node_id,
            download_url=placed.get("raw_node_url") or "",
            sender_user=actor_key,
            sender_display_name=getattr(actor, "config_display_name", "") or actor_key,
            group_id=group_id,
            text=body,
        )
        try:
            MAIN._remember_node_delivery_target(placed.get("_id") or node_id, "group" if group_id else "user", group_id or target_id, sender_user=actor_key)
        except Exception:
            pass
        if group_id:
            result = MAIN.send_message_to_group_global(group_id, title, body, message_data, sender_user=actor_key)
        else:
            result = MAIN.send_message_to_user_global(target_id, title, body, message_data, sender_user=actor_key)
        if not isinstance(result, dict) or not result.get("ok"):
            raise RuntimeError(str((result or {}).get("error") if isinstance(result, dict) else result))
        return {
            "target_type": target_type,
            "target_id": group_id or target_id,
            "placed": placed,
            "message": message_data,
            "result": result,
        }

    raise LookupError("unknown messaging tool")


def _room_tools(config: Configuration, actor: User, name: str, args: Dict[str, Any]):
    if name == "list_rooms":
        return _list_rooms(config, actor)

    if name == "create_room":
        if not _actor_can_manage_rooms(config, actor):
            raise PermissionError("effective user cannot manage rooms")
        room_name = str(args.get("name") or "New room").strip()[:100]
        transport = str(args.get("transport") or "websocket").strip().lower()
        if transport not in ("websocket", "fcm"):
            raise ValueError("transport must be websocket or fcm")
        room = Room(name=room_name or "New room", transport=transport, user_id=config.user_id)
        db.session.add(room)
        db.session.commit()
        alias = str(args.get("alias") or "").strip()
        if alias:
            alias = alias[:100]
            existing_alias = RoomAlias.query.filter_by(config_id=config.id, alias=alias).first()
            if existing_alias:
                existing_alias.room_uid = room.uid
            else:
                db.session.add(RoomAlias(alias=alias, room_uid=room.uid, config_id=config.id))
            db.session.commit()
        return _room_to_dict(room, config)

    if name == "update_room":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"), require_manage=True)
        if "name" in args:
            room.name = str(args.get("name") or room.name or "Room").strip()[:100]
        if "transport" in args:
            transport = str(args.get("transport") or room.transport or "websocket").strip().lower()
            if transport not in ("websocket", "fcm"):
                raise ValueError("transport must be websocket or fcm")
            room.transport = transport
        db.session.commit()
        return _room_to_dict(room, config)

    if name == "delete_room":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"), require_manage=True)
        room_uid = room.uid
        owner_config_ids = [row[0] for row in db.session.query(Configuration.id).filter_by(user_id=config.user_id).all()]
        if owner_config_ids:
            RoomAlias.query.filter(
                RoomAlias.room_uid == room_uid,
                RoomAlias.config_id.in_(owner_config_ids),
            ).delete(synchronize_session=False)
        RoomDevice.query.filter_by(room_uid=room_uid).delete(synchronize_session=False)
        RoomObjects.query.filter_by(room_uid=room_uid).delete(synchronize_session=False)
        db.session.delete(room)
        db.session.commit()
        try:
            with MAIN.SqliteDict(MAIN.TASKS_DB_PATH, autocommit=True) as tasks_db:
                if room_uid in tasks_db:
                    del tasks_db[room_uid]
        except Exception:
            pass
        return {"status": "deleted", "room_uid": room_uid}

    if name == "set_room_alias":
        if not _actor_can_manage_rooms(config, actor):
            raise PermissionError("effective user cannot manage rooms")
        room = _get_room_for_mcp(config, actor, args.get("room_uid"), require_manage=True)
        alias = str(args.get("alias") or "").strip()
        if not alias:
            raise ValueError("alias is required")
        row = RoomAlias.query.filter_by(config_id=config.id, alias=alias).first()
        if row:
            row.room_uid = room.uid
        else:
            row = RoomAlias(alias=alias[:100], room_uid=room.uid, config_id=config.id)
            db.session.add(row)
        db.session.commit()
        return {"id": row.id, "alias": row.alias, "room_uid": row.room_uid}

    if name == "delete_room_alias":
        if not _actor_can_manage_rooms(config, actor):
            raise PermissionError("effective user cannot manage rooms")
        alias = str(args.get("alias") or "").strip()
        row = RoomAlias.query.filter_by(config_id=config.id, alias=alias).first()
        if not row:
            raise LookupError("room alias not found")
        result = {"id": row.id, "alias": row.alias, "room_uid": row.room_uid}
        db.session.delete(row)
        db.session.commit()
        return {"status": "deleted", **result}

    if name == "list_room_objects":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        limit = max(1, min(int(args.get("limit") or 100), 500))
        query = RoomObjects.query.filter_by(room_uid=room.uid, config_uid=config.uid)
        if args.get("class_name"):
            query = query.filter_by(class_name=str(args.get("class_name")))
        rows = query.order_by(RoomObjects.created_at.desc()).limit(limit).all()
        return [
            {
                "id": row.id,
                "room_uid": row.room_uid,
                "config_uid": row.config_uid,
                "class_name": row.class_name,
                "objects": row.objects_data,
                "acknowledged_by": row.acknowledged_by or [],
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            }
            for row in rows
        ]

    if name == "list_room_tasks":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        status = str(args.get("status") or "all").strip().lower()
        if status not in ("all", "active", "completed", "blocked"):
            raise ValueError("status must be all, active, completed or blocked")
        with MAIN.SqliteDict(MAIN.TASKS_DB_PATH) as tasks_db:
            rows = list(tasks_db.get(room.uid, []) or [])
        if status == "active":
            rows = [x for x in rows if not x.get("_done") and not x.get("_blocked")]
        elif status == "completed":
            rows = [x for x in rows if x.get("_done")]
        elif status == "blocked":
            rows = [x for x in rows if x.get("_blocked") and not x.get("_done")]
        return rows

    if name == "add_room_tasks":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        tasks = args.get("tasks")
        if not isinstance(tasks, list):
            raise ValueError("tasks must be an array")
        added = []
        with MAIN.SqliteDict(MAIN.TASKS_DB_PATH) as tasks_db:
            room_tasks = list(tasks_db.get(room.uid, []) or [])
            for raw in tasks:
                if not isinstance(raw, dict):
                    continue
                task = dict(raw)
                task.setdefault("uid", str(uuid.uuid4()))
                task["_created"] = _utcnow().isoformat()
                room_tasks.append(task)
                added.append(task)
            tasks_db[room.uid] = room_tasks
            tasks_db.commit()
        MAIN.send_tasks_update(room.uid)
        return {"status": "success", "count": len(added), "tasks": added}

    if name == "take_room_task":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        selected = None
        with MAIN.SqliteDict(MAIN.TASKS_DB_PATH) as tasks_db:
            room_tasks = list(tasks_db.get(room.uid, []) or [])
            for idx, task in enumerate(room_tasks):
                if not task.get("_done") and not task.get("_blocked"):
                    room_tasks[idx]["_blocked"] = True
                    room_tasks[idx]["_blocked_at"] = _utcnow().isoformat()
                    selected = dict(room_tasks[idx])
                    tasks_db[room.uid] = room_tasks
                    tasks_db.commit()
                    break
        if selected is None:
            return {"status": "no_tasks_available"}
        MAIN.send_tasks_update(room.uid)
        return selected

    if name == "complete_room_task":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        task_uid = str(args.get("task_uid") or "").strip()
        if not task_uid:
            raise ValueError("task_uid is required")
        found = False
        with MAIN.SqliteDict(MAIN.TASKS_DB_PATH) as tasks_db:
            room_tasks = list(tasks_db.get(room.uid, []) or [])
            for idx, task in enumerate(room_tasks):
                if str(task.get("uid") or "") == task_uid:
                    room_tasks[idx]["_done"] = True
                    room_tasks[idx]["_completed_at"] = _utcnow().isoformat()
                    tasks_db[room.uid] = room_tasks
                    tasks_db.commit()
                    found = True
                    break
        if not found:
            raise LookupError("room task not found")
        MAIN.send_tasks_update(room.uid)
        return {"status": "success", "task_uid": task_uid}

    if name == "clear_completed_room_tasks":
        room = _get_room_for_mcp(config, actor, args.get("room_uid"))
        with MAIN.SqliteDict(MAIN.TASKS_DB_PATH) as tasks_db:
            room_tasks = list(tasks_db.get(room.uid, []) or [])
            updated = [task for task in room_tasks if not task.get("_done")]
            removed = len(room_tasks) - len(updated)
            tasks_db[room.uid] = updated
            tasks_db.commit()
        MAIN.send_tasks_update(room.uid)
        return {"status": "success", "removed": removed, "remaining": len(updated)}

    raise LookupError("unknown room tool")


def _contract_is_scoped_to_config(contract: Contract, config: Configuration) -> bool:
    source_type = MAIN._normalize_contract_source_type(getattr(contract, "source_type", "class"))
    if source_type == "class":
        refs = MAIN._contract_class_refs(contract)
        return bool(refs) and all(str((ref or {}).get("config_uid") or "") == str(config.uid) for ref in refs)
    if source_type == "global_index":
        return str(getattr(contract, "source_config_uid", "") or "") == str(config.uid)
    return False


def _get_mcp_contract(config: Configuration, actor: User, contract_uid: str) -> Contract:
    row = Contract.query.filter_by(uid=str(contract_uid or "").strip(), user_id=actor.id).first()
    if not row:
        raise LookupError("contract not found")
    if not _contract_is_scoped_to_config(row, config):
        raise PermissionError("contract is outside this MCP configuration")
    return row


def _contract_input_for_mcp(config: Configuration, actor: User, data: Dict[str, Any], existing: Optional[Contract] = None) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if existing is not None:
        merged.update(MAIN._contract_to_dict(existing))
        merged["source_classes_json"] = merged.get("source_classes") or []
    if any(k in (data or {}) for k in ("source_classes_json", "source_classes", "classes", "class_refs", "class_name", "source_config_uid", "config_uid")):
        for key in ("source_classes_json", "source_classes", "classes", "class_refs"):
            merged.pop(key, None)
    merged.update(data or {})
    source_type = MAIN._normalize_contract_source_type(merged.get("source_type"))
    if source_type == "external_only":
        raise PermissionError("external-only contracts are not configuration-scoped and are not exposed through instance MCP")

    if source_type == "class":
        refs = MAIN._contract_class_refs_from_data(
            merged,
            str(merged.get("source_config_uid") or config.uid),
            str(merged.get("class_name") or ""),
        )
        if not refs:
            raise ValueError("at least one class is required")
        for ref in refs:
            if str((ref or {}).get("config_uid") or "") != str(config.uid):
                raise PermissionError("MCP contracts may reference only this configuration")
            class_name = str((ref or {}).get("class_name") or "").strip()
            if not class_name or not MAIN.user_can_access_class(actor, config.uid, class_name):
                raise PermissionError(f"class is not visible to effective user: {class_name}")
        merged["source_classes_json"] = refs
        merged["source_config_uid"] = config.uid
        merged["class_name"] = str((refs[0] or {}).get("class_name") or "")
    elif source_type == "global_index":
        refs = MAIN._contract_class_refs_from_data(
            merged,
            str(merged.get("source_config_uid") or config.uid),
            str(merged.get("class_name") or ""),
        )
        if not refs:
            raise ValueError("one source class is required for source_type=global_index")
        first = refs[0]
        if str((first or {}).get("config_uid") or "") != str(config.uid):
            raise PermissionError("MCP contracts may reference only this configuration")
        class_name = str((first or {}).get("class_name") or "").strip()
        if not class_name or not MAIN.user_can_access_class(actor, config.uid, class_name):
            raise PermissionError("class is not visible to effective user")
        merged["source_config_uid"] = config.uid
        merged["class_name"] = class_name
        merged["source_classes_json"] = [first]
    return merged


def _contract_tools(config: Configuration, actor: User, name: str, args: Dict[str, Any]):
    if name == "list_contracts":
        rows = Contract.query.filter_by(user_id=actor.id).order_by(Contract.updated_at.desc(), Contract.created_at.desc()).all()
        return [MAIN._contract_to_dict(row) for row in rows if _contract_is_scoped_to_config(row, config)]

    if name == "get_contract":
        return MAIN._contract_to_dict(_get_mcp_contract(config, actor, args.get("contract_uid")))

    if name == "create_contract":
        data = _contract_input_for_mcp(config, actor, args)
        contract = Contract(user_id=actor.id)
        MAIN._contract_update_from_data(contract, data, actor)
        db.session.add(contract)
        db.session.commit()
        return MAIN._contract_to_dict(contract)

    if name == "update_contract":
        contract = _get_mcp_contract(config, actor, args.get("contract_uid"))
        data = dict(args)
        data.pop("contract_uid", None)
        data = _contract_input_for_mcp(config, actor, data, existing=contract)
        MAIN._contract_update_from_data(contract, data, actor)
        db.session.commit()
        return MAIN._contract_to_dict(contract)

    if name == "delete_contract":
        contract = _get_mcp_contract(config, actor, args.get("contract_uid"))
        uid = contract.uid
        db.session.delete(contract)
        db.session.commit()
        return {"status": "deleted", "contract_uid": uid}

    raise LookupError("unknown contract tool")


def _tool_catalog(link: MCPAccessLink) -> List[Dict[str, Any]]:
    ro = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
    write = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False}
    destructive = {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False}
    tools = [
        {"name": "describe_instance", "description": "Describe this NodaLogic solution: semantic prompt, visible classes, sections and nGenie skills.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
        {"name": "describe_class", "description": "Return the structure, AI semantics, declared indexes and published methods of one visible class.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}}, "required": ["class_name"]}, "annotations": ro},
        {"name": "list_nodes", "description": "List RLS-visible nodes of a class. Use a small limit and filters when possible.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "filters": {"type": "object"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}}, "required": ["class_name"]}, "annotations": ro},
        {"name": "find_nodes", "description": "Find RLS-visible nodes. Can use a declared index or a textual fallback over returned node data.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "query": {"type": "string"}, "index_name": {"type": "string"}, "index_value": {}, "filters": {"type": "object"}, "limit": {"type": "integer", "minimum": 1, "maximum": 200}}, "required": ["class_name"]}, "annotations": ro},
        {"name": "get_node", "description": "Read one RLS-visible node.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "node_id": {"type": "string"}}, "required": ["class_name", "node_id"]}, "annotations": ro},
        {"name": "create_node", "description": "Create a node through the normal NodaLogic runtime, including server acceptance handlers.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "data": {"type": "object"}}, "required": ["class_name", "data"]}, "annotations": write},
        {"name": "update_node", "description": "Update an RLS-visible node through the normal runtime.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "node_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["class_name", "node_id", "data"]}, "annotations": write},
        {"name": "delete_node", "description": "Delete an RLS-visible node.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "node_id": {"type": "string"}}, "required": ["class_name", "node_id"]}, "annotations": destructive},
        {"name": "call_node_method", "description": "Call a method explicitly declared for a node class. Arbitrary Python attributes cannot be invoked.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "node_id": {"type": "string"}, "method_name": {"type": "string"}, "args": {"type": "array"}, "kwargs": {"type": "object"}, "input_data": {}}, "required": ["class_name", "node_id", "method_name"]}, "annotations": write},
        {"name": "call_class_method", "description": "Call a method explicitly declared for a class.", "inputSchema": {"type": "object", "properties": {"class_name": {"type": "string"}, "method_name": {"type": "string"}, "args": {"type": "array"}, "kwargs": {"type": "object"}, "input_data": {}}, "required": ["class_name", "method_name"]}, "annotations": write},
        {"name": "ngenie_query", "description": "Ask the solution's nGenie to analyze/search using its prompts, skills and indexes. Intended for read-oriented work.", "inputSchema": {"type": "object", "properties": {"message": {"type": "string"}, "chat_history": {"type": "array"}, "conversation_artifact": {"type": "object"}, "clarification_response": {"type": "object"}, "scope": {"type": "string"}}, "required": ["message"]}, "annotations": ro},
        {"name": "ngenie_action", "description": "Ask the solution's nGenie to perform a runtime task using its native skills. May create or modify business data.", "inputSchema": {"type": "object", "properties": {"message": {"type": "string"}, "chat_history": {"type": "array"}, "conversation_artifact": {"type": "object"}, "clarification_response": {"type": "object"}, "scope": {"type": "string"}, "allow_catalog_create": {"type": "boolean"}}, "required": ["message"]}, "annotations": write},
        {"name": "list_message_targets", "description": "List messaging users, groups available to the effective user, and rooms available in this solution context.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
        {"name": "list_message_groups", "description": "List message groups where the effective user is a member.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
        {"name": "create_message_group", "description": "Create a normal NodaLogic message group. The effective user is always included as a member.", "inputSchema": {"type": "object", "properties": {"title": {"type": "string"}, "members": {"type": "array", "items": {"type": "string"}}}, "required": ["title"]}, "annotations": write},
        {"name": "add_message_group_members", "description": "Add members to a message group available to the effective user.", "inputSchema": {"type": "object", "properties": {"group_id": {"type": "string"}, "members": {"type": "array", "items": {"type": "string"}}}, "required": ["group_id", "members"]}, "annotations": write},
        {"name": "remove_message_group_member", "description": "Remove one member from a message group available to the effective user.", "inputSchema": {"type": "object", "properties": {"group_id": {"type": "string"}, "user_key": {"type": "string"}}, "required": ["group_id", "user_key"]}, "annotations": destructive},
        {"name": "get_group_messages", "description": "Read message history for a group where the effective user is a member.", "inputSchema": {"type": "object", "properties": {"group_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 500}, "before": {"type": "string"}}, "required": ["group_id"]}, "annotations": ro},
        {"name": "send_message", "description": "Send a normal NodaLogic message to a user, message group, or room. Sender identity is always the effective MCP user.", "inputSchema": {"type": "object", "properties": {"target_type": {"type": "string", "enum": ["user", "group", "room"]}, "target_id": {"type": "string"}, "title": {"type": "string"}, "body": {"type": "string"}, "data": {"type": "object"}}, "required": ["target_type", "target_id", "body"]}, "annotations": write},
        {"name": "send_node", "description": "Send one RLS-visible NodaLogic node to a user/group through Messenger or queue it in a Room using the native delivery mechanisms.", "inputSchema": {"type": "object", "properties": {"target_type": {"type": "string", "enum": ["user", "group", "room"]}, "target_id": {"type": "string"}, "class_name": {"type": "string"}, "node_id": {"type": "string"}, "title": {"type": "string"}, "body": {"type": "string"}}, "required": ["target_type", "target_id", "class_name", "node_id"]}, "annotations": write},
        {"name": "list_rooms", "description": "List rooms available in this configuration context, including aliases, transport and registered devices.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
        {"name": "create_room", "description": "Create a NodaLogic room in this solution tenant. Requires owner/delegated room-management rights.", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}, "transport": {"type": "string", "enum": ["websocket", "fcm"]}, "alias": {"type": "string"}}}, "annotations": write},
        {"name": "update_room", "description": "Update room name/transport. Requires owner/delegated room-management rights.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}, "name": {"type": "string"}, "transport": {"type": "string", "enum": ["websocket", "fcm"]}}, "required": ["room_uid"]}, "annotations": write},
        {"name": "delete_room", "description": "Delete a room owned by this solution tenant, including its aliases/devices/queued room objects. Requires room-management rights.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}}, "required": ["room_uid"]}, "annotations": destructive},
        {"name": "set_room_alias", "description": "Create or update a configuration room alias.", "inputSchema": {"type": "object", "properties": {"alias": {"type": "string"}, "room_uid": {"type": "string"}}, "required": ["alias", "room_uid"]}, "annotations": write},
        {"name": "delete_room_alias", "description": "Delete a configuration room alias.", "inputSchema": {"type": "object", "properties": {"alias": {"type": "string"}}, "required": ["alias"]}, "annotations": destructive},
        {"name": "list_room_objects", "description": "List queued objects for one room, limited to this configuration.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}, "class_name": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 500}}, "required": ["room_uid"]}, "annotations": ro},
        {"name": "list_room_tasks", "description": "List tasks in a room. Can return all, active, blocked or completed tasks.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}, "status": {"type": "string", "enum": ["all", "active", "blocked", "completed"]}}, "required": ["room_uid"]}, "annotations": ro},
        {"name": "add_room_tasks", "description": "Append normal NodaLogic tasks to a room and notify connected clients.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}, "tasks": {"type": "array", "items": {"type": "object"}}}, "required": ["room_uid", "tasks"]}, "annotations": write},
        {"name": "take_room_task", "description": "Take the first available room task, marking it blocked exactly like the native room API.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}}, "required": ["room_uid"]}, "annotations": write},
        {"name": "complete_room_task", "description": "Mark a room task completed and notify connected clients.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}, "task_uid": {"type": "string"}}, "required": ["room_uid", "task_uid"]}, "annotations": write},
        {"name": "clear_completed_room_tasks", "description": "Remove completed tasks from a room.", "inputSchema": {"type": "object", "properties": {"room_uid": {"type": "string"}}, "required": ["room_uid"]}, "annotations": destructive},
    ]
    # Contracts are integration/export artifacts rather than ordinary row-level
    # data.  Expose mutation only to a Full credential so a user-scoped MCP link
    # cannot become an RLS-bypass export channel.
    if str(link.access_mode or "").lower() == "full":
        tools += [
            {"name": "list_contracts", "description": "List contracts owned by this MCP actor and scoped to this configuration.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
            {"name": "get_contract", "description": "Describe one contract scoped to this configuration.", "inputSchema": {"type": "object", "properties": {"contract_uid": {"type": "string"}}, "required": ["contract_uid"]}, "annotations": ro},
            {"name": "create_contract", "description": "Create a class/global-index contract scoped strictly to this MCP configuration.", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}, "display_name": {"type": "string"}, "source_type": {"type": "string", "enum": ["class", "global_index"]}, "class_name": {"type": "string"}, "source_classes": {"type": "array", "items": {"type": "object"}}, "global_index_name": {"type": "string"}, "global_index_value": {"type": "string"}}, "required": ["name"]}, "annotations": write},
            {"name": "update_contract", "description": "Update a contract scoped to this MCP configuration.", "inputSchema": {"type": "object", "properties": {"contract_uid": {"type": "string"}, "name": {"type": "string"}, "display_name": {"type": "string"}, "source_type": {"type": "string", "enum": ["class", "global_index"]}, "class_name": {"type": "string"}, "source_classes": {"type": "array", "items": {"type": "object"}}, "global_index_name": {"type": "string"}, "global_index_value": {"type": "string"}}, "required": ["contract_uid"]}, "annotations": write},
            {"name": "delete_contract", "description": "Delete a contract scoped to this MCP configuration.", "inputSchema": {"type": "object", "properties": {"contract_uid": {"type": "string"}}, "required": ["contract_uid"]}, "annotations": destructive},
        ]
    if link.allow_extensions:
        tools += [
            {"name": "extensions_list", "description": "List user-created extension sections/classes/nodes visible in the effective user's extension workspace.", "inputSchema": {"type": "object", "properties": {}}, "annotations": ro},
            {"name": "extension_create_section", "description": "Create a normal NodaLogic user extension section.", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}, "annotations": write},
            {"name": "extension_create_class", "description": "Create a normal user extension class in an extension-enabled section. Supply either fields or native layout.", "inputSchema": {"type": "object", "properties": {"section_code": {"type": "string"}, "name": {"type": "string"}, "fields": {"type": "array", "items": {"type": "object"}}, "layout": {"type": "array"}, "record_template": {"type": "string"}}, "required": ["section_code", "name"]}, "annotations": write},
            {"name": "extension_update_class", "description": "Update a user-created extension class layout/name.", "inputSchema": {"type": "object", "properties": {"class_id": {"type": "string"}, "name": {"type": "string"}, "fields": {"type": "array"}, "layout": {"type": "array"}, "record_template": {"type": "string"}}, "required": ["class_id"]}, "annotations": write},
            {"name": "extension_delete_class", "description": "Delete extension class metadata. Existing raw extension documents are not silently converted into base configuration data.", "inputSchema": {"type": "object", "properties": {"class_id": {"type": "string"}}, "required": ["class_id"]}, "annotations": destructive},
            {"name": "extension_delete_section", "description": "Delete a user-created extension section and its extension class metadata.", "inputSchema": {"type": "object", "properties": {"section_code": {"type": "string"}}, "required": ["section_code"]}, "annotations": destructive},
            {"name": "extension_create_node", "description": "Create a normal extension RawNode so Web/Android see it exactly like a human-created extension node.", "inputSchema": {"type": "object", "properties": {"section_code": {"type": "string"}, "class_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["section_code", "data"]}, "annotations": write},
            {"name": "extension_update_node", "description": "Update a normal extension node owned by the effective user.", "inputSchema": {"type": "object", "properties": {"node_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["node_id", "data"]}, "annotations": write},
            {"name": "extension_delete_node", "description": "Delete a normal extension node owned by the effective user.", "inputSchema": {"type": "object", "properties": {"node_id": {"type": "string"}}, "required": ["node_id"]}, "annotations": destructive},
        ]
    return tools


def _describe_instance(config: Configuration, actor: User, link: MCPAccessLink):
    try:
        from client_app.ngenie_skill_registry import skill_catalog
        skills = skill_catalog()
    except Exception:
        skills = []
    classes = _visible_classes(config, actor)
    sections = []
    visible_names = {c.name for c in classes}
    for sec in ConfigSection.query.filter_by(config_id=config.id).order_by(ConfigSection.id.asc()).all():
        sections.append({"code": sec.code or "", "name": sec.name or sec.code or "", "allow_client_extensions": getattr(sec, "allow_client_extensions", True) is not False})
    return {
        "name": config.name,
        "uid": config.uid,
        "version": config.version,
        "vendor": config.vendor or "",
        "ngenie_prompt": config.ngenie_prompt or "",
        "access_mode": link.access_mode,
        "effective_user": {"id": actor.id, "email": actor.email, "name": actor.config_display_name or actor.email},
        "extensions_enabled": bool(link.allow_extensions),
        "capabilities": {
            "messaging": True,
            "rooms": True,
            "room_management": _actor_can_manage_rooms(config, actor),
            "contracts": str(link.access_mode or "").lower() == "full",
            "extensions": bool(link.allow_extensions),
        },
        "sections": sections,
        "classes": [{"name": c.name, "display_name": c.display_name or c.name, "ngenie_role": c.ngenie_role or "", "ngenie_description": c.ngenie_description or ""} for c in classes if c.name in visible_names],
        "ngenie_skills": skills,
    }


def _resource_list(config: Configuration, actor: User, link: MCPAccessLink):
    out = [
        {"uri": "nodalogic://instance", "name": "NodaLogic solution", "description": "Solution identity, semantics and visible class catalog", "mimeType": "application/json"},
        {"uri": "nodalogic://classes", "name": "Classes", "description": "Visible classes and AI descriptions", "mimeType": "application/json"},
        {"uri": "nodalogic://ngenie/skills", "name": "nGenie skills", "description": "Runtime nGenie skill catalog", "mimeType": "application/json"},
        {"uri": "nodalogic://messaging/targets", "name": "Messaging targets", "description": "Users, groups and rooms available to the effective MCP actor", "mimeType": "application/json"},
        {"uri": "nodalogic://rooms", "name": "Rooms", "description": "Rooms available in this configuration context", "mimeType": "application/json"},
    ]
    for cls in _visible_classes(config, actor):
        out.append({"uri": f"nodalogic://class/{cls.name}", "name": cls.display_name or cls.name, "description": cls.ngenie_description or f"NodaLogic class {cls.name}", "mimeType": "application/json"})
    try:
        from client_app.ngenie_skill_registry import load_skills
        for skill in load_skills():
            out.append({"uri": f"nodalogic://ngenie/skill/{skill.id}", "name": skill.name, "description": skill.description, "mimeType": "application/json"})
    except Exception:
        pass
    if link.allow_extensions:
        out.append({"uri": "nodalogic://extensions", "name": "User extensions", "description": "Existing client-created extension metadata", "mimeType": "application/json"})
    if str(link.access_mode or "").lower() == "full":
        out.append({"uri": "nodalogic://contracts", "name": "Contracts", "description": "Contracts scoped to this configuration", "mimeType": "application/json"})
    return out


def _read_resource(config: Configuration, actor: User, link: MCPAccessLink, uri: str):
    uri = str(uri or "")
    if uri == "nodalogic://instance":
        value = _describe_instance(config, actor, link)
    elif uri == "nodalogic://classes":
        value = [_class_description(c) for c in _visible_classes(config, actor)]
    elif uri == "nodalogic://ngenie/skills":
        from client_app.ngenie_skill_registry import skill_catalog
        value = skill_catalog()
    elif uri == "nodalogic://messaging/targets":
        value = _messaging_targets(config, actor)
    elif uri == "nodalogic://rooms":
        value = _list_rooms(config, actor)
    elif uri == "nodalogic://contracts" and str(link.access_mode or "").lower() == "full":
        value = _contract_tools(config, actor, "list_contracts", {})
    elif uri.startswith("nodalogic://ngenie/skill/"):
        sid = uri.rsplit("/", 1)[-1]
        from client_app.ngenie_skill_registry import load_skills
        skill = next((x for x in load_skills() if x.id == sid), None)
        if not skill:
            raise LookupError("nGenie skill not found")
        value = {
            "id": skill.id, "name": skill.name, "description": skill.description,
            "prompt": skill.prompt,
            "functions_prompt": str(getattr(skill.module, "FUNCTIONS_PROMPT", "") or ""),
        }
    elif uri == "nodalogic://extensions" and link.allow_extensions:
        value = _extension_tools(config, actor, "extensions_list", {})
    elif uri.startswith("nodalogic://class/"):
        name = uri.split("/", 3)[-1]
        cls = next((c for c in _visible_classes(config, actor) if c.name == name), None)
        if not cls:
            raise LookupError("class resource not found")
        value = _class_description(cls)
    else:
        raise LookupError("resource not found")
    return {"contents": [{"uri": uri, "mimeType": "application/json", "text": _json_text(value)}]}


def _call_tool(link: MCPAccessLink, config: Configuration, actor: User, name: str, args: Dict[str, Any]):
    tokens = _runtime_context(config, actor)
    try:
        if name == "describe_instance":
            return _describe_instance(config, actor, link)
        if name == "describe_class":
            cname = str(args.get("class_name") or "")
            cls = next((c for c in _visible_classes(config, actor) if c.name == cname), None)
            if not cls:
                raise PermissionError("class is not visible")
            return _class_description(cls)
        if name == "list_nodes":
            return _list_nodes(config, actor, str(args.get("class_name") or ""), args.get("limit") or 100, args.get("filters"))
        if name == "find_nodes":
            return _find_nodes(config, actor, args)
        if name == "get_node":
            return _get_node(config, actor, str(args.get("class_name") or ""), str(args.get("node_id") or ""))[1]
        if name == "create_node":
            cname = str(args.get("class_name") or "")
            data = args.get("data") if isinstance(args.get("data"), dict) else {}
            cls = _node_class(config, actor, cname)
            node_id = MAIN.extract_internal_id(str(data.get("_id") or "")) if data.get("_id") else str(uuid.uuid4())
            node = cls(node_id, config.uid)
            node.update_data(dict(data))
            return node.to_dict()
        if name == "update_node":
            cname = str(args.get("class_name") or "")
            node, old = _get_node(config, actor, cname, str(args.get("node_id") or ""))
            patch = args.get("data") if isinstance(args.get("data"), dict) else {}
            node.update_data(dict(patch))
            return node.to_dict()
        if name == "delete_node":
            cname = str(args.get("class_name") or "")
            node, _ = _get_node(config, actor, cname, str(args.get("node_id") or ""))
            node.delete()
            return {"status": "deleted"}
        if name == "call_node_method":
            cname = str(args.get("class_name") or "")
            mname = str(args.get("method_name") or "")
            if not _declared_method(config, cname, mname):
                raise PermissionError("method is not declared in the configuration")
            node, _ = _get_node(config, actor, cname, str(args.get("node_id") or ""))
            fn = getattr(node, mname, None)
            if not callable(fn):
                raise LookupError("runtime method not found")
            a = args.get("args") if isinstance(args.get("args"), list) else []
            kw = args.get("kwargs") if isinstance(args.get("kwargs"), dict) else {}
            if "input_data" in args and not a and not kw:
                a = [args.get("input_data")]
            return fn(*a, **kw)
        if name == "call_class_method":
            cname = str(args.get("class_name") or "")
            mname = str(args.get("method_name") or "")
            if not _declared_method(config, cname, mname):
                raise PermissionError("method is not declared in the configuration")
            cls = _node_class(config, actor, cname)
            fn = getattr(cls, mname, None)
            if not callable(fn):
                raise LookupError("runtime class method not found")
            a = args.get("args") if isinstance(args.get("args"), list) else []
            kw = args.get("kwargs") if isinstance(args.get("kwargs"), dict) else {}
            if "input_data" in args and not a and not kw:
                a = [args.get("input_data")]
            return fn(*a, **kw)
        if name == "ngenie_query":
            return _ngenie_call(config, actor, args, action_mode=False)
        if name == "ngenie_action":
            return _ngenie_call(config, actor, args, action_mode=True)
        if name in {
            "list_message_targets", "list_message_groups", "create_message_group",
            "add_message_group_members", "remove_message_group_member",
            "get_group_messages", "send_message", "send_node",
        }:
            return _messaging_tools(config, actor, name, args)
        if name in {
            "list_rooms", "create_room", "update_room", "delete_room",
            "set_room_alias", "delete_room_alias", "list_room_objects",
            "list_room_tasks", "add_room_tasks", "take_room_task",
            "complete_room_task", "clear_completed_room_tasks",
        }:
            return _room_tools(config, actor, name, args)
        if name in {"list_contracts", "get_contract", "create_contract", "update_contract", "delete_contract"}:
            if str(link.access_mode or "").lower() != "full":
                raise PermissionError("contract tools require a Full MCP credential")
            return _contract_tools(config, actor, name, args)
        if name.startswith("extension") or name == "extensions_list":
            if not link.allow_extensions:
                raise PermissionError("extensions are disabled for this MCP credential")
            return _extension_tools(config, actor, name, args)
        raise LookupError("unknown tool")
    finally:
        MAIN._nodes_mod.reset_runtime_context(tokens)


def _dispatch_mcp(link: MCPAccessLink, config: Configuration, actor: User):
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _jsonrpc_error(None, -32700, "Parse error", status=400)
    req_id = body.get("id")
    method = str(body.get("method") or "")
    params = body.get("params") if isinstance(body.get("params"), dict) else {}

    # JSON-RPC notifications intentionally have no response body.
    if method in ("notifications/initialized", "notifications/cancelled"):
        return Response(status=202)

    if method == "server/discover":
        return _jsonrpc_result(req_id, {
            "supportedVersions": [MODERN_VERSION, LATEST_HANDSHAKE_VERSION, "2025-06-18", "2025-03-26"],
            "serverInfo": SERVER_INFO,
            "capabilities": {"tools": {"listChanged": False}, "resources": {"listChanged": False}, "prompts": {"listChanged": False}},
            "instructions": "This endpoint controls one deployed NodaLogic solution and its runtime services. Read class semantics before modifying data. Messenger and Rooms use native NodaLogic delivery. Contract mutation is Full-access only. Base configuration schema is immutable through MCP; user extensions require an explicitly enabled credential.",
        })
    if method == "initialize":
        wanted = str(params.get("protocolVersion") or "")
        negotiated = wanted if wanted in SUPPORTED_HANDSHAKE_VERSIONS else LATEST_HANDSHAKE_VERSION
        return _jsonrpc_result(req_id, {
            "protocolVersion": negotiated,
            "capabilities": {"tools": {"listChanged": False}, "resources": {"listChanged": False}, "prompts": {"listChanged": False}},
            "serverInfo": SERVER_INFO,
            "instructions": "NodaLogic runtime MCP. First inspect nodalogic://instance or call describe_instance. Respect RLS, use nGenie when business semantics are non-trivial, and use native messaging/room tools for communication and delivery.",
        }, extra_headers={"Mcp-Session-Id": secrets.token_urlsafe(18)})
    if method == "ping":
        return _jsonrpc_result(req_id, {})
    if method == "tools/list":
        return _jsonrpc_result(req_id, {"tools": _tool_catalog(link)})
    if method == "tools/call":
        name = str(params.get("name") or "")
        args = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
        try:
            value = _call_tool(link, config, actor, name, args)
            _audit(link, actor, name, True, class_name=str(args.get("class_name") or ""), node_id=str(args.get("node_id") or ""), method_name=str(args.get("method_name") or ""))
            return _jsonrpc_result(req_id, _tool_result(value))
        except Exception as exc:
            _audit(link, actor, name, False, class_name=str(args.get("class_name") or ""), node_id=str(args.get("node_id") or ""), method_name=str(args.get("method_name") or ""), detail=str(exc))
            return _jsonrpc_result(req_id, _tool_result({"error": str(exc)}, is_error=True))
    if method == "resources/list":
        return _jsonrpc_result(req_id, {"resources": _resource_list(config, actor, link)})
    if method == "resources/read":
        try:
            return _jsonrpc_result(req_id, _read_resource(config, actor, link, str(params.get("uri") or "")))
        except Exception as exc:
            return _jsonrpc_error(req_id, -32002, str(exc))
    if method == "resources/templates/list":
        return _jsonrpc_result(req_id, {"resourceTemplates": [{"uriTemplate": "nodalogic://class/{class_name}", "name": "NodaLogic class", "description": "Semantic class description", "mimeType": "application/json"}]})
    if method == "prompts/list":
        return _jsonrpc_result(req_id, {"prompts": [{"name": "work_with_solution", "description": "Instructions for an external agent working with this NodaLogic solution", "arguments": []}]})
    if method == "prompts/get":
        if str(params.get("name") or "") != "work_with_solution":
            return _jsonrpc_error(req_id, -32602, "Unknown prompt")
        text = "Inspect the solution schema and nGenie descriptions first. Use read tools before writes when entity identity is ambiguous. Use nGenie for project-specific semantics. Use send_message/send_node and Room tools instead of inventing transport payloads. Contracts are integration artifacts and are available only to Full credentials. Never attempt to alter the base configuration; only user extensions are mutable when enabled."
        return _jsonrpc_result(req_id, {"description": "NodaLogic solution working instructions", "messages": [{"role": "user", "content": {"type": "text", "text": text}}]})
    return _jsonrpc_error(req_id, -32601, "Method not found")


@_bp.route("/mcp/<path_secret>", methods=["POST", "GET", "DELETE"])
def mcp_by_link(path_secret):
    link, config, actor, err = _resolve_link_by_secret(path_secret)
    if err:
        return jsonify({"error": err}), 401
    if request.method == "DELETE":
        return Response(status=204)
    if request.method == "GET":
        # Stateful server->client event stream is not needed by NodaLogic v1.
        return Response(status=405, headers={"Allow": "POST, DELETE"})
    return _dispatch_mcp(link, config, actor)


@_bp.route("/mcp/config/<config_uid>", methods=["POST", "GET", "DELETE"])
def mcp_by_bearer(config_uid):
    link, config, actor, err = _resolve_link_by_secret(_request_secret(), config_uid=config_uid)
    if err:
        return jsonify({"error": err}), 401
    if request.method == "DELETE":
        return Response(status=204)
    if request.method == "GET":
        return Response(status=405, headers={"Allow": "POST, DELETE"})
    return _dispatch_mcp(link, config, actor)



@_bp.route("/edit-config/<config_uid>/mcp", methods=["GET"])
@login_required
def mcp_manage_page(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first_or_404()
    if not _owner_can_manage(config):
        abort(403)
    return render_template("mcp_manage.html", config=config, users=_candidate_users(config))

@_bp.route("/api/config/<config_uid>/mcp-links", methods=["GET", "POST"])
@login_required
def mcp_links(config_uid):
    config = Configuration.query.filter_by(uid=config_uid).first_or_404()
    if not _owner_can_manage(config):
        abort(403)
    if request.method == "GET":
        rows = MCPAccessLink.query.filter_by(config_id=config.id).order_by(MCPAccessLink.id.desc()).all()
        return jsonify({"ok": True, "links": [_link_public(x) for x in rows], "users": [{"id": u.id, "email": u.email, "name": u.config_display_name or u.email} for u in _candidate_users(config)]})
    data = request.get_json(silent=True) or request.form.to_dict()
    mode = str(data.get("access_mode") or "full").strip().lower()
    if mode not in ("full", "user"):
        return jsonify({"ok": False, "error": "access_mode must be full or user"}), 400
    target_user = None
    if mode == "user":
        try:
            target_user = db.session.get(User, int(data.get("user_id") or 0))
        except Exception:
            target_user = None
        if not target_user or not _user_has_config(target_user, config):
            return jsonify({"ok": False, "error": "selected user has no access to this configuration"}), 400
    secret = _new_secret()
    row = MCPAccessLink(
        config_id=config.id,
        label=str(data.get("label") or "MCP")[:200],
        access_mode=mode,
        user_id=target_user.id if target_user else None,
        allow_extensions=str(data.get("allow_extensions") or "").lower() in ("1", "true", "yes", "on") if not isinstance(data.get("allow_extensions"), bool) else bool(data.get("allow_extensions")),
        token_hash=_hash_secret(secret),
        token_prefix=secret[:18],
        created_by_user_id=current_user.id,
        created_at=_utcnow(),
    )
    db.session.add(row)
    db.session.commit()
    public = _link_public(row)
    # Public MCP links are always HTTPS.  The app can sit behind an HTTP
    # reverse-proxy internally, so request.url_root may otherwise expose http://.
    public_root = request.url_root.rstrip("/")
    if public_root.startswith("http://"):
        public_root = "https://" + public_root[len("http://"):]
    public.update({
        "token": secret,
        "url": public_root + "/mcp/" + secret,
        "endpoint": public_root + "/mcp/config/" + config.uid,
    })
    return jsonify({"ok": True, "link": public}), 201


def _link_public(row: MCPAccessLink):
    user = db.session.get(User, row.user_id) if row.user_id else None
    return {
        "id": row.id,
        "label": row.label or "",
        "access_mode": row.access_mode,
        "user_id": row.user_id,
        "user": (user.config_display_name or user.email) if user else "",
        "allow_extensions": bool(row.allow_extensions),
        "token_prefix": row.token_prefix or "",
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "last_used_at": row.last_used_at.isoformat() if row.last_used_at else None,
        "revoked_at": row.revoked_at.isoformat() if row.revoked_at else None,
        "active": row.revoked_at is None,
    }


@_bp.route("/api/config/<config_uid>/mcp-links/<int:link_id>", methods=["DELETE"])
@login_required
def mcp_link_delete(config_uid, link_id):
    config = Configuration.query.filter_by(uid=config_uid).first_or_404()
    if not _owner_can_manage(config):
        abort(403)
    row = MCPAccessLink.query.filter_by(id=link_id, config_id=config.id).first_or_404()
    row.revoked_at = _utcnow()
    db.session.commit()
    return jsonify({"ok": True})
