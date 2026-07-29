"""Process-local dashboard banners.

Banners are intentionally ephemeral: they are never written to SQL/NoSQL and
vanish when the server process restarts.
"""
from __future__ import annotations
from collections import OrderedDict
from threading import RLock
from typing import Any, Dict, List
import time

_lock = RLock()
_items: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()


def _size(value: Any) -> str:
    try:
        n = float(value)
    except Exception:
        n = 0.25
    if n >= 0.75:
        return "100"
    if n >= 0.375:
        return "50"
    return "25"


def put(config_uid: str, banner_id: Any, kind: str, value: Any, size: Any = 0.25, background: Any = None) -> Dict[str, Any]:
    bid = str(banner_id or "").strip()
    if not bid:
        raise ValueError("banner id is required")
    key = f"{str(config_uid or '').strip()}::{bid}"
    item = {
        "id": bid,
        "key": key,
        "config_uid": str(config_uid or "").strip(),
        "kind": str(kind or "layout"),
        "value": value,
        "size": _size(size),
        "background": None if background is None else str(background),
        "updated_at": time.time(),
    }
    with _lock:
        _items[key] = item
        _items.move_to_end(key, last=False)
    return dict(item)


def close(config_uid: str, banner_id: Any) -> bool:
    key = f"{str(config_uid or '').strip()}::{str(banner_id or '').strip()}"
    with _lock:
        return _items.pop(key, None) is not None


def list_for(config_uids) -> List[Dict[str, Any]]:
    allowed = {str(x or "").strip() for x in (config_uids or [])}
    with _lock:
        return [dict(v) for v in _items.values() if not allowed or not v.get("config_uid") or v.get("config_uid") in allowed]
