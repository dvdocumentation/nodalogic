# -*- coding: utf-8 -*-
"""OAuth 2.1 authorization-code + S256 PKCE for per-configuration MCP.

The OAuth resource is ``/mcp/config/<config_uid>``.  It authenticates with the
existing NodaLogic user table, keeps authorization codes and tokens separate
from legacy MCP credentials, and stores only SHA-256 token hashes.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from urllib.parse import urlencode, urlsplit, urlunsplit

from flask import Blueprint, Response, jsonify, make_response, redirect, render_template_string, request, session
from flask_login import current_user, login_user, logout_user
from sqlalchemy import update
from werkzeug.security import check_password_hash

from extensions import db
from models import Configuration, User, UserConfigAccess


BP = Blueprint("mcp_runtime_oauth", __name__)
MAIN = None
SCOPE = "mcp:data"
ACCESS_SECONDS = 3600
REFRESH_SECONDS = 30 * 24 * 3600
CODE_SECONDS = 90
LOGIN_SECONDS = 10 * 60


class OAuthClient(db.Model):
    __tablename__ = "mcp_runtime_oauth_client"
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.String(160), unique=True, nullable=False, index=True)
    config_id = db.Column(db.Integer, nullable=False, index=True)
    client_name = db.Column(db.String(160), nullable=False)
    redirect_uris_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class OAuthCode(db.Model):
    __tablename__ = "mcp_runtime_oauth_code"
    id = db.Column(db.Integer, primary_key=True)
    code_hash = db.Column(db.String(64), unique=True, nullable=False, index=True)
    client_id = db.Column(db.String(160), nullable=False)
    config_id = db.Column(db.Integer, nullable=False, index=True)
    user_id = db.Column(db.Integer, nullable=False)
    redirect_uri = db.Column(db.Text, nullable=False)
    resource = db.Column(db.Text, nullable=False)
    code_challenge = db.Column(db.String(128), nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
    consumed_at = db.Column(db.DateTime)


class OAuthToken(db.Model):
    __tablename__ = "mcp_runtime_oauth_token"
    id = db.Column(db.Integer, primary_key=True)
    token_hash = db.Column(db.String(64), unique=True, nullable=False, index=True)
    token_kind = db.Column(db.String(12), nullable=False)
    client_id = db.Column(db.String(160), nullable=False)
    config_id = db.Column(db.Integer, nullable=False, index=True)
    user_id = db.Column(db.Integer, nullable=False)
    resource = db.Column(db.Text, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
    revoked_at = db.Column(db.DateTime)


TABLES = [OAuthClient.__table__, OAuthCode.__table__, OAuthToken.__table__]


def register(app, main_module):
    global MAIN
    MAIN = main_module
    with app.app_context():
        db.metadata.create_all(bind=db.engine, tables=TABLES, checkfirst=True)
    app.register_blueprint(BP)


def _public_base():
    # Pinned trusted configuration, never Host/X-Forwarded-Host from the client.
    base = os.getenv("MCP_PUBLIC_BASE_URL") or getattr(MAIN, "PUBLIC_API_BASE_URL", "")
    base = str(base).rstrip("/")
    parts = urlsplit(base)
    if parts.scheme != "https" or not parts.netloc or parts.path not in ("", "/"):
        raise RuntimeError("MCP_PUBLIC_BASE_URL must be the HTTPS public origin")
    return base


def _resource(config_uid):
    return _public_base() + "/mcp/config/" + str(config_uid)


def _issuer(config_uid):
    return _resource(config_uid)


def _metadata_url(config_uid):
    return _resource(config_uid) + "/.well-known/oauth-protected-resource"


def _now():
    return datetime.utcnow()


def _sha256(raw):
    return hashlib.sha256(str(raw or "").encode("utf-8")).hexdigest()


def _hash_pkce(verifier):
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")


def _random_token():
    return secrets.token_urlsafe(48)


def _configuration(config_uid):
    return Configuration.query.filter_by(uid=str(config_uid)).first()


def _user_has_config(user, config):
    if not user or not config:
        return False
    if int(user.id) == int(config.user_id):
        return True
    return UserConfigAccess.query.filter_by(user_id=user.id, config_id=config.id).first() is not None


def _json(body, status=200):
    response = make_response(jsonify(body), status)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


def _oauth_error(error, description, status=400):
    return _json({"error": error, "error_description": description}, status)


def _valid_redirect(uri):
    if not isinstance(uri, str) or len(uri) > 2048:
        return False
    try:
        parsed = urlsplit(uri)
        if parsed.fragment or parsed.username or parsed.password or not parsed.hostname:
            return False
        host = parsed.hostname.lower()
        if parsed.scheme == "https" and host == "chatgpt.com":
            return bool(
                parsed.path == "/connector_platform_oauth_redirect"
                or re.fullmatch(r"/connector/oauth/[A-Za-z0-9_-]{1,128}", parsed.path)
            )
        allowed = {
            item.strip().lower()
            for item in os.getenv("MCP_OAUTH_EXTRA_REDIRECT_HOSTS", "").split(",")
            if item.strip()
        }
        if host in allowed and parsed.scheme == "https":
            return True
        if host in {"localhost", "127.0.0.1", "::1"} and host in allowed and parsed.scheme == "http":
            return True
    except (TypeError, ValueError):
        return False
    return False


def _client_for(config, client_id, redirect_uri):
    client = OAuthClient.query.filter_by(client_id=client_id, config_id=config.id).first()
    if not client:
        return None
    try:
        redirects = json.loads(client.redirect_uris_json)
        return client if redirect_uri in redirects else None
    except (TypeError, ValueError):
        return None


def _add_query(uri, **params):
    parts = urlsplit(uri)
    query = parts.query + ("&" if parts.query else "") + urlencode(
        {key: value for key, value in params.items() if value is not None}
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))


def _bearer_token():
    authorization = request.headers.get("Authorization", "")
    if not authorization.lower().startswith("bearer "):
        return ""
    return authorization.split(" ", 1)[1].strip()


def resolve_access(config_uid):
    """Resolve an OAuth access token to the link-like runtime access contract."""
    raw = _bearer_token()
    if not raw or len(raw) > 512:
        return None, None, None, "missing or invalid OAuth access token"
    config = _configuration(config_uid)
    if not config:
        return None, None, None, "configuration not found"
    entry = OAuthToken.query.filter_by(
        token_hash=_sha256(raw), token_kind="access", config_id=config.id, revoked_at=None
    ).first()
    if not entry or entry.expires_at <= _now() or entry.resource != _resource(config.uid):
        return None, None, None, "invalid, expired or revoked OAuth access token"
    actor = db.session.get(User, entry.user_id)
    if not _user_has_config(actor, config):
        return None, None, None, "user no longer has access to this configuration"
    access_mode = "full" if int(actor.id) == int(config.user_id) else "user"
    access = SimpleNamespace(
        id=None,
        config_id=config.id,
        access_mode=access_mode,
        user_id=actor.id if access_mode == "user" else None,
        allow_extensions=False,
    )
    return access, config, actor, ""


def unauthorized(config_uid):
    response = _oauth_error("invalid_token", "Sign in to NodaLogic to access this MCP configuration", 401)
    response.headers["WWW-Authenticate"] = (
        'Bearer resource_metadata="' + _metadata_url(config_uid) + '", scope="' + SCOPE + '"'
    )
    return response


@BP.route("/mcp/config/<config_uid>/.well-known/oauth-protected-resource", methods=["GET"])
@BP.route("/.well-known/oauth-protected-resource/mcp/config/<config_uid>", methods=["GET"])
def protected_resource(config_uid):
    if not _configuration(config_uid):
        return _oauth_error("invalid_target", "Unknown MCP configuration", 404)
    return _json(
        {
            "resource": _resource(config_uid),
            "authorization_servers": [_issuer(config_uid)],
            "scopes_supported": [SCOPE],
        }
    )


@BP.route("/mcp/config/<config_uid>/.well-known/oauth-authorization-server", methods=["GET"])
@BP.route("/.well-known/oauth-authorization-server/mcp/config/<config_uid>", methods=["GET"])
def authorization_server_metadata(config_uid):
    if not _configuration(config_uid):
        return _oauth_error("invalid_target", "Unknown MCP configuration", 404)
    issuer = _issuer(config_uid)
    return _json(
        {
            "issuer": issuer,
            "authorization_endpoint": issuer + "/oauth/authorize",
            "token_endpoint": issuer + "/oauth/token",
            "registration_endpoint": issuer + "/oauth/register",
            "revocation_endpoint": issuer + "/oauth/revoke",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": [SCOPE],
            "authorization_response_iss_parameter_supported": True,
        }
    )


@BP.route("/mcp/config/<config_uid>/oauth/register", methods=["POST"])
def dynamic_client_registration(config_uid):
    config = _configuration(config_uid)
    if not config:
        return _oauth_error("invalid_target", "Unknown MCP configuration", 404)
    if request.content_length and request.content_length > 16_384:
        return _oauth_error("invalid_client_metadata", "Too large", 413)
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return _oauth_error("invalid_client_metadata", "Expected JSON object")
    redirects = data.get("redirect_uris")
    if not isinstance(redirects, list) or not (1 <= len(redirects) <= 6) or any(
        not _valid_redirect(url) for url in redirects
    ):
        return _oauth_error(
            "invalid_redirect_uri",
            "ChatGPT redirect URI is required. Additional hosts require server opt-in.",
        )
    if data.get("token_endpoint_auth_method", "none") != "none":
        return _oauth_error("invalid_client_metadata", "Only public PKCE clients are supported")
    grants = data.get("grant_types", ["authorization_code", "refresh_token"])
    if not isinstance(grants, list) or not set(grants).issubset({"authorization_code", "refresh_token"}):
        return _oauth_error("invalid_client_metadata", "Unsupported grant")
    if OAuthClient.query.filter_by(config_id=config.id).count() >= 300:
        return _oauth_error("temporarily_unavailable", "Client registration limit reached", 503)
    client_id = "nlmcp_oauth_" + secrets.token_urlsafe(24)
    client = OAuthClient(
        client_id=client_id,
        config_id=config.id,
        client_name=str(data.get("client_name") or "ChatGPT MCP client")[:160],
        redirect_uris_json=json.dumps(redirects),
    )
    db.session.add(client)
    db.session.commit()
    return _json(
        {
            "client_id": client_id,
            "client_id_issued_at": int(time.time()),
            "client_name": client.client_name,
            "redirect_uris": redirects,
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
        },
        201,
    )


_PAGE = """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NodaLogic — MCP</title>
<style>body{font-family:system-ui,Arial,sans-serif;background:#f7f9f8;color:#142323;
min-height:100vh;display:grid;place-items:center;margin:0;padding:16px;box-sizing:border-box}
main{max-width:430px;width:100%;background:white;padding:28px;border:1px solid #dfe6e3;
border-radius:14px;box-shadow:0 8px 26px #15282012}h1{font-size:23px;margin:0 0 12px}
p{line-height:1.5}label{display:block;margin-top:12px}input{width:100%;box-sizing:border-box;
padding:11px;border:1px solid #becac5;border-radius:8px}button{margin-top:20px;padding:12px;
border-radius:8px;border:0;background:#187a55;color:white;font-size:15px;width:100%;cursor:pointer}
.muted{color:#52615e;font-size:13px}.error{color:#a12424}</style></head><body><main>
<h1>Подключение NodaLogic MCP</h1>
<p>Доступ к конфигурации <b>{{ config_name }}</b> запрашивает <b>{{ client_name }}</b>.</p>
<p class="muted">Владелец получает полный доступ к данным конфигурации. Другие пользователи работают
со своими текущими правами и RLS. Изменение базовой схемы и обработчиков недоступно.</p>
{% if error %}<p class="error">{{ error }}</p>{% endif %}
<form method="post" autocomplete="on"><input type="hidden" name="flow" value="{{ flow }}">
<input type="hidden" name="csrf" value="{{ csrf }}">{% if not logged_in %}
<label>Email</label><input type="email" name="email" required autocomplete="username">
<label>Пароль NodaLogic</label><input type="password" name="password" required autocomplete="current-password">
{% else %}<p>Вы вошли как <b>{{ email }}</b>.</p>{% endif %}
<button type="submit" name="action" value="approve">Войти и разрешить подключение</button>
<button type="submit" name="action" value="deny" style="background:#777">Отмена</button>
</form></main></body></html>"""


def _flow_error(config_uid, error, message, state, redirect_uri):
    return redirect(
        _add_query(redirect_uri, error=error, error_description=message, state=state, iss=_issuer(config_uid)),
        code=302,
    )


def _flow_session_key(config_uid):
    return "mcp_runtime_oauth_flow_" + hashlib.sha256(str(config_uid).encode("utf-8")).hexdigest()[:20]


@BP.route("/mcp/config/<config_uid>/oauth/authorize", methods=["GET", "POST"])
def authorize(config_uid):
    config = _configuration(config_uid)
    if not config:
        return _oauth_error("invalid_target", "Unknown MCP configuration", 404)
    session_key = _flow_session_key(config_uid)
    if request.method == "GET":
        client_id = str(request.args.get("client_id", ""))
        redirect_uri = str(request.args.get("redirect_uri", ""))
        if not _client_for(config, client_id, redirect_uri) or not _valid_redirect(redirect_uri):
            return _oauth_error("invalid_request", "Unknown client or redirect URI")
        state = str(request.args.get("state", ""))
        if len(state) > 4096:
            return _oauth_error("invalid_request", "state too long")
        if request.args.get("response_type") != "code":
            return _flow_error(config_uid, "unsupported_response_type", "Only authorization code is supported", state, redirect_uri)
        if request.args.get("code_challenge_method") != "S256":
            return _flow_error(config_uid, "invalid_request", "S256 PKCE is required", state, redirect_uri)
        challenge = str(request.args.get("code_challenge", ""))
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}", challenge):
            return _flow_error(config_uid, "invalid_request", "Invalid code challenge", state, redirect_uri)
        scope = request.args.get("scope") or SCOPE
        if scope != SCOPE:
            return _flow_error(config_uid, "invalid_scope", "Only mcp:data scope is supported", state, redirect_uri)
        resource = request.args.get("resource")
        if resource != _resource(config_uid):
            return _flow_error(config_uid, "invalid_target", "Unknown MCP resource", state, redirect_uri)
        session[session_key] = {
            "id": secrets.token_urlsafe(18),
            "csrf": secrets.token_urlsafe(24),
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "state": state,
            "challenge": challenge,
            "resource": resource,
            "at": int(time.time()),
            "failures": 0,
        }
    flow = session.get(session_key)
    if not isinstance(flow, dict) or int(time.time()) - flow.get("at", 0) > LOGIN_SECONDS:
        session.pop(session_key, None)
        return _oauth_error("invalid_request", "Authorization request expired, reconnect from ChatGPT")
    if request.method == "POST":
        if not (
            hmac.compare_digest(str(request.form.get("flow") or ""), flow["id"])
            and hmac.compare_digest(str(request.form.get("csrf") or ""), flow["csrf"])
        ):
            return _oauth_error("invalid_request", "CSRF validation failed", 403)
        if request.form.get("action") == "deny":
            session.pop(session_key, None)
            return _flow_error(config_uid, "access_denied", "User declined access", flow["state"], flow["redirect_uri"])
        if request.form.get("action") != "approve":
            return _oauth_error("invalid_request", "Invalid confirmation action")
        if flow.get("failures", 0) >= 5:
            session.pop(session_key, None)
            return _oauth_error("access_denied", "Too many login attempts; reconnect from ChatGPT", 429)
        actor = current_user if current_user.is_authenticated else None
        if not _user_has_config(actor, config):
            email = str(request.form.get("email") or "").strip().lower()
            password = str(request.form.get("password") or "")
            candidate = User.query.filter_by(email=email).first()
            if _user_has_config(candidate, config) and check_password_hash(candidate.password, password):
                if current_user.is_authenticated:
                    logout_user()
                login_user(candidate, remember=False, fresh=True)
                actor = candidate
            else:
                flow["failures"] = flow.get("failures", 0) + 1
                session[session_key] = flow
                return _render_authorization(config, flow, "Неверные данные или нет доступа к конфигурации")
        if not _user_has_config(actor, config):
            return _oauth_error("access_denied", "Configuration access required", 403)
        code = _random_token()
        db.session.add(
            OAuthCode(
                code_hash=_sha256(code),
                client_id=flow["client_id"],
                config_id=config.id,
                user_id=actor.id,
                redirect_uri=flow["redirect_uri"],
                resource=flow["resource"],
                code_challenge=flow["challenge"],
                expires_at=_now() + timedelta(seconds=CODE_SECONDS),
            )
        )
        db.session.commit()
        session.pop(session_key, None)
        return redirect(
            _add_query(flow["redirect_uri"], code=code, state=flow["state"], iss=_issuer(config_uid)),
            code=302,
        )
    return _render_authorization(config, flow)


def _render_authorization(config, flow, error=""):
    client = _client_for(config, flow["client_id"], flow["redirect_uri"])
    if not client:
        return _oauth_error("invalid_client", "Client registration disappeared")
    actor = current_user if current_user.is_authenticated else None
    allowed = _user_has_config(actor, config)
    response = make_response(
        render_template_string(
            _PAGE,
            client_name=client.client_name,
            config_name=getattr(config, "name", "") or getattr(config, "uid", ""),
            logged_in=allowed,
            email=getattr(actor, "email", "") or "" if allowed else "",
            flow=flow["id"],
            csrf=flow["csrf"],
            error=error,
        )
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    response.headers["X-Frame-Options"] = "DENY"
    return response


def _save_token(raw, kind, client_id, config_id, user_id, resource, seconds):
    db.session.add(
        OAuthToken(
            token_hash=_sha256(raw),
            token_kind=kind,
            client_id=client_id,
            config_id=config_id,
            user_id=user_id,
            resource=resource,
            expires_at=_now() + timedelta(seconds=seconds),
        )
    )


def _token_pair(client_id, config_id, user_id, resource):
    access = _random_token()
    refresh = _random_token()
    _save_token(access, "access", client_id, config_id, user_id, resource, ACCESS_SECONDS)
    _save_token(refresh, "refresh", client_id, config_id, user_id, resource, REFRESH_SECONDS)
    return access, refresh


@BP.route("/mcp/config/<config_uid>/oauth/token", methods=["POST"])
def exchange_token(config_uid):
    config = _configuration(config_uid)
    if not config:
        return _oauth_error("invalid_target", "Unknown MCP configuration", 404)
    if request.content_length and request.content_length > 12_288:
        return _oauth_error("invalid_request", "Request too large", 413)
    if request.authorization or request.form.get("client_secret"):
        return _oauth_error("invalid_client", "Public client, no client secret is accepted", 401)
    grant = request.form.get("grant_type", "")
    client_id = request.form.get("client_id", "")
    if not OAuthClient.query.filter_by(client_id=client_id, config_id=config.id).first():
        return _oauth_error("invalid_client", "Unknown OAuth client", 401)
    resource = request.form.get("resource", "")
    if grant == "refresh_token" and not resource:
        resource = _resource(config_uid)
    if resource != _resource(config_uid):
        return _oauth_error("invalid_target", "MCP resource mismatch")
    now = _now()
    if grant == "authorization_code":
        code = request.form.get("code", "")
        redirect_uri = request.form.get("redirect_uri", "")
        verifier = request.form.get("code_verifier", "")
        if not re.fullmatch(r"[A-Za-z0-9._~-]{43,128}", verifier):
            return _oauth_error("invalid_grant", "Invalid PKCE verifier")
        if not _client_for(config, client_id, redirect_uri):
            return _oauth_error("invalid_grant", "Unregistered redirect URI")
        row = OAuthCode.query.filter_by(
            code_hash=_sha256(code), client_id=client_id, config_id=config.id
        ).first()
        if not (
            row
            and row.expires_at > now
            and row.consumed_at is None
            and row.redirect_uri == redirect_uri
            and row.resource == resource
            and hmac.compare_digest(row.code_challenge, _hash_pkce(verifier))
        ):
            return _oauth_error("invalid_grant", "Code expired, invalid or already used")
        actor = db.session.get(User, row.user_id)
        if not _user_has_config(actor, config):
            return _oauth_error("access_denied", "Configuration access is no longer valid", 403)
        updated = db.session.execute(
            update(OAuthCode)
            .where(OAuthCode.id == row.id, OAuthCode.consumed_at.is_(None))
            .values(consumed_at=now)
        )
        if updated.rowcount != 1:
            db.session.rollback()
            return _oauth_error("invalid_grant", "Code already used")
        access, refresh = _token_pair(client_id, config.id, actor.id, resource)
    elif grant == "refresh_token":
        given = request.form.get("refresh_token", "")
        row = OAuthToken.query.filter_by(
            token_hash=_sha256(given),
            token_kind="refresh",
            client_id=client_id,
            config_id=config.id,
            revoked_at=None,
        ).first()
        if not row or row.expires_at <= now or row.resource != resource:
            return _oauth_error("invalid_grant", "Refresh token expired or invalid")
        actor = db.session.get(User, row.user_id)
        if not _user_has_config(actor, config):
            return _oauth_error("access_denied", "Configuration access is no longer valid", 403)
        updated = db.session.execute(
            update(OAuthToken)
            .where(OAuthToken.id == row.id, OAuthToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        if updated.rowcount != 1:
            db.session.rollback()
            return _oauth_error("invalid_grant", "Refresh token already rotated")
        access, refresh = _token_pair(client_id, config.id, actor.id, resource)
    else:
        return _oauth_error("unsupported_grant_type", "Use authorization_code or refresh_token")
    db.session.commit()
    return _json(
        {
            "access_token": access,
            "token_type": "Bearer",
            "expires_in": ACCESS_SECONDS,
            "refresh_token": refresh,
            "scope": SCOPE,
        }
    )


@BP.route("/mcp/config/<config_uid>/oauth/revoke", methods=["POST"])
def revoke_token(config_uid):
    config = _configuration(config_uid)
    if not config:
        return Response(status=200)
    client_id = request.form.get("client_id", "")
    raw = request.form.get("token", "")
    if client_id and raw:
        row = OAuthToken.query.filter_by(
            token_hash=_sha256(raw), client_id=client_id, config_id=config.id
        ).first()
        if row and row.revoked_at is None:
            row.revoked_at = _now()
            db.session.commit()
    return Response(status=200)
