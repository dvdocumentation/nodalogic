# -*- coding: utf-8 -*-
"""Shared root-level LLM credentials and OpenAI-compatible HTTP client.

This module deliberately lives in the project root rather than inside the
optional ``ngenie_code`` package.  The ordinary nGenie runtime, editor AI,
vision code and nGenie Code can therefore use the same ``credentials.json``.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import requests

PROJECT_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = PROJECT_DIR / "credentials.json"
DEFAULT_URL = "https://api.deepseek.com/v1/chat/completions"
DEFAULT_MODEL = "deepseek-chat"


def _as_float(value: Any, default: float) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _as_int(value: Any, default: int) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def load_credentials() -> Dict[str, Any]:
    """Read root ``credentials.json`` and apply environment overrides."""
    data: Dict[str, Any] = {}
    try:
        if CREDENTIALS_PATH.is_file():
            loaded = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data.update(loaded)
    except Exception as exc:
        print(f"[LLM credentials] cannot read {CREDENTIALS_PATH.name}: {exc}")

    env_map = {
        "deepseek_api_key": ("DEEPSEEK_API_KEY", "NGENIE_API_KEY"),
        "deepseek_url": ("DEEPSEEK_API_URL", "NGENIE_API_URL"),
        "model": ("DEEPSEEK_MODEL", "NGENIE_MODEL"),
        "hf_token": ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HUGGINGFACE_HUB_TOKEN"),
    }
    for key, names in env_map.items():
        for name in names:
            value = os.environ.get(name)
            if value not in (None, ""):
                data[key] = value
                break

    api_key = str(
        data.get("deepseek_api_key")
        or data.get("routerai_api_key")
        or data.get("api_key")
        or ""
    ).strip()
    data["deepseek_api_key"] = api_key
    return data


def completion_url(credentials: Optional[Mapping[str, Any]] = None) -> str:
    creds = dict(credentials or load_credentials())
    explicit = str(
        creds.get("deepseek_url")
        or creds.get("url")
        or creds.get("chat_completions_url")
        or ""
    ).strip()
    if explicit:
        return explicit.rstrip("/")
    base = str(creds.get("base_url") or creds.get("routerai_base_url") or "").strip().rstrip("/")
    if base:
        return base if base.endswith("/chat/completions") else base + "/chat/completions"
    return DEFAULT_URL


def request_timeout(credentials: Optional[Mapping[str, Any]] = None) -> Tuple[float, float]:
    creds = dict(credentials or load_credentials())
    total = _as_float(creds.get("request_timeout_seconds"), 80.0)
    connect = _as_float(creds.get("connect_timeout_seconds"), min(30.0, total))
    read = _as_float(creds.get("read_timeout_seconds"), total)
    return max(1.0, connect), max(1.0, read)


def _post_nonblocking(url: str, **kwargs: Any):
    """Do not freeze the gevent server while an LLM provider is answering."""
    try:
        from gevent import get_hub
        return get_hub().threadpool.apply(requests.post, args=(url,), kwds=kwargs)
    except (ImportError, RuntimeError, AttributeError):
        return requests.post(url, **kwargs)


def message_content(data: Mapping[str, Any]) -> str:
    try:
        message = (((data.get("choices") or [{}])[0].get("message") or {}))
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            pieces: List[str] = []
            for item in content:
                if isinstance(item, str):
                    pieces.append(item)
                elif isinstance(item, dict):
                    text = item.get("text") or item.get("content")
                    if text:
                        pieces.append(str(text))
            return "".join(pieces)
    except Exception:
        pass
    return ""


def chat_completion(
    messages: Iterable[Mapping[str, Any]],
    *,
    require_json: bool = False,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    """Call the configured OpenAI-compatible provider with transient retries.

    TLS EOF, connection resets, timeouts, HTTP 429 and temporary 5xx responses
    are retried.  This specifically prevents timer-driven nGenie calls from
    failing immediately on a one-off provider/network disconnect.
    """
    creds = load_credentials()
    api_key = str(creds.get("deepseek_api_key") or "").strip()
    if not api_key:
        raise RuntimeError("LLM API key is not configured in root credentials.json or DEEPSEEK_API_KEY")

    payload: Dict[str, Any] = {
        "model": str(creds.get("model") or DEFAULT_MODEL),
        "messages": [dict(item) for item in messages],
        "temperature": float(temperature if temperature is not None else _as_float(creds.get("temperature"), 0.2)),
    }
    if max_tokens is not None:
        token_param = str(creds.get("token_param") or "max_tokens").strip() or "max_tokens"
        payload[token_param] = int(max_tokens)

    extra_body = creds.get("extra_body") or creds.get("openai_extra_body")
    if isinstance(extra_body, dict):
        payload.update(extra_body)

    response_format_added = False
    if require_json and "response_format" not in payload:
        response_format = creds.get("json_response_format") or {"type": "json_object"}
        if response_format:
            payload["response_format"] = response_format
            response_format_added = True

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    url = completion_url(creds)
    timeout = request_timeout(creds)
    attempts = max(1, _as_int(creds.get("request_retries"), 3))
    retry_statuses = {429, 500, 502, 503, 504}
    last_error: Optional[BaseException] = None
    json_mode_fallback_used = False

    for attempt in range(attempts):
        try:
            response = _post_nonblocking(url, headers=headers, json=payload, timeout=timeout)
            if (
                require_json
                and response_format_added
                and not json_mode_fallback_used
                and response.status_code in {400, 404, 415, 422}
            ):
                # Some OpenAI-compatible providers/models do not implement
                # response_format. The prompt still requests JSON, so retry once
                # without this optional transport hint.
                payload.pop("response_format", None)
                json_mode_fallback_used = True
                continue
            if response.status_code in retry_statuses and attempt + 1 < attempts:
                time.sleep(min(4.0, 0.75 * (2 ** attempt)))
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError("LLM provider returned a non-object JSON response")
            return data
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(min(4.0, 0.75 * (2 ** attempt)))
        except requests.exceptions.RequestException as exc:
            last_error = exc
            break
        except ValueError as exc:
            last_error = RuntimeError("LLM provider returned invalid JSON")
            break

    provider = url.split("/", 3)[2] if "://" in url else url
    raise RuntimeError(f"LLM request to {provider} failed after {attempts} attempt(s): {last_error}") from last_error
