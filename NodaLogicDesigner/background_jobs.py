from __future__ import annotations

import contextvars
import os
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

_MAX_WORKERS = max(1, min(8, int(os.environ.get("NODA_BACKGROUND_WORKERS") or 2)))
_EXECUTOR = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="noda-job")
_LOCK = threading.RLock()
_JOBS: dict[str, dict[str, Any]] = {}
_CURRENT_JOB_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("NODA_CURRENT_JOB_ID", default=None)
_TTL_SECONDS = max(300, int(os.environ.get("NODA_BACKGROUND_JOB_TTL") or 86400))


def _now() -> float:
    return time.time()


def _public(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job.get("id"),
        "mode": job.get("mode"),
        "status": job.get("status"),
        "progress": job.get("progress"),
        "current": job.get("current"),
        "total": job.get("total"),
        "text": job.get("text") or "",
        "cancel_requested": bool(job.get("cancel_requested")),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "result": job.get("result") if job.get("status") == "completed" else None,
        "error": job.get("error") if job.get("status") == "failed" else None,
    }


def _cleanup_locked() -> None:
    cutoff = _now() - _TTL_SECONDS
    stale = [
        job_id
        for job_id, job in _JOBS.items()
        if job.get("status") in {"completed", "failed", "cancelled"}
        and float(job.get("finished_at") or 0) < cutoff
    ]
    for job_id in stale:
        _JOBS.pop(job_id, None)


def submit(
    fn: Callable[[], Any],
    *,
    owner: str = "",
    mode: str = "runasync",
    title: str = "",
) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = _now()
    job = {
        "id": job_id,
        "owner": str(owner or ""),
        "mode": str(mode or "runasync"),
        "title": str(title or ""),
        "status": "queued",
        "progress": 0.0,
        "current": 0,
        "total": 0,
        "text": str(title or ""),
        "cancel_requested": False,
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }
    with _LOCK:
        _cleanup_locked()
        _JOBS[job_id] = job

    def runner() -> None:
        token = _CURRENT_JOB_ID.set(job_id)
        try:
            with _LOCK:
                current = _JOBS.get(job_id)
                if current is None:
                    return
                if current.get("cancel_requested"):
                    current["status"] = "cancelled"
                    current["finished_at"] = _now()
                    return
                current["status"] = "running"
                current["started_at"] = _now()
            result = fn()
            with _LOCK:
                current = _JOBS.get(job_id)
                if current is None:
                    return
                if current.get("cancel_requested"):
                    current["status"] = "cancelled"
                else:
                    current["status"] = "completed"
                    current["progress"] = 1.0
                    current["result"] = result
                current["finished_at"] = _now()
        except JobCancelled:
            with _LOCK:
                current = _JOBS.get(job_id)
                if current is not None:
                    current["status"] = "cancelled"
                    current["text"] = current.get("text") or "Cancelled"
                    current["finished_at"] = _now()
        except Exception as exc:
            with _LOCK:
                current = _JOBS.get(job_id)
                if current is not None:
                    current["status"] = "failed"
                    current["error"] = {
                        "message": str(exc),
                        "traceback": traceback.format_exc(limit=30),
                    }
                    current["finished_at"] = _now()
        finally:
            _CURRENT_JOB_ID.reset(token)

    _EXECUTOR.submit(runner)
    return _public(job)


def get(job_id: str, *, owner: str | None = None) -> dict[str, Any] | None:
    with _LOCK:
        job = _JOBS.get(str(job_id or ""))
        if job is None:
            return None
        if owner is not None and str(job.get("owner") or "") != str(owner or ""):
            return None
        return _public(job)


def request_cancel(job_id: str, *, owner: str | None = None) -> bool:
    with _LOCK:
        job = _JOBS.get(str(job_id or ""))
        if job is None:
            return False
        if owner is not None and str(job.get("owner") or "") != str(owner or ""):
            return False
        if job.get("status") in {"completed", "failed", "cancelled"}:
            return False
        job["cancel_requested"] = True
        if job.get("status") == "queued":
            job["status"] = "cancelled"
            job["finished_at"] = _now()
        return True


def current_job_id() -> str | None:
    return _CURRENT_JOB_ID.get()


def cancel_requested() -> bool:
    job_id = current_job_id()
    if not job_id:
        return False
    with _LOCK:
        job = _JOBS.get(job_id)
        return bool(job and job.get("cancel_requested"))


def update_progress(current=None, total=None, text: str | None = None, progress=None) -> None:
    job_id = current_job_id()
    if not job_id:
        return
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        if current is not None:
            try:
                job["current"] = int(current)
            except Exception:
                pass
        if total is not None:
            try:
                job["total"] = max(0, int(total))
            except Exception:
                pass
        if text is not None:
            job["text"] = str(text)
        if progress is not None:
            try:
                job["progress"] = max(0.0, min(1.0, float(progress)))
            except Exception:
                pass
        elif job.get("total"):
            try:
                job["progress"] = max(
                    0.0,
                    min(1.0, float(job.get("current") or 0) / float(job.get("total") or 1)),
                )
            except Exception:
                pass


class JobCancelled(RuntimeError):
    pass


def raise_if_cancelled() -> None:
    if cancel_requested():
        raise JobCancelled("Background job cancelled")
