from __future__ import annotations

import contextvars
import faulthandler
import logging
import multiprocessing
import os
import pickle
import sys
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
_MAX_QUEUED = max(1, int(os.environ.get("NODA_BACKGROUND_QUEUE_MAX") or 32))
_PROCESS_MEMORY_MB = max(0, int(os.environ.get("NODA_JOB_MEMORY_MB") or 1024))
_PROCESS_TIMEOUT_SECONDS = max(0, int(os.environ.get("NODA_JOB_TIMEOUT_SECONDS") or 7200))
_CANCEL_GRACE_SECONDS = max(1, int(os.environ.get("NODA_JOB_CANCEL_GRACE_SECONDS") or 10))
_TRACE_AFTER_SECONDS = max(0, int(os.environ.get("NODA_JOB_TRACE_AFTER_SECONDS") or 0))
_RESULT_MAX_BYTES = max(65536, int(os.environ.get("NODA_JOB_RESULT_MAX_BYTES") or 2097152))
_REQUESTED_MODE = str(os.environ.get("NODA_BACKGROUND_MODE") or "process").strip().lower()
_PROCESS_MODE = (
    _REQUESTED_MODE == "process"
    and os.name == "posix"
    and "fork" in multiprocessing.get_all_start_methods()
)
_PROCESS_SENDER = None
_PROCESS_CANCEL_EVENT = None
_LOGGER = logging.getLogger(__name__)


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
        "isolation": job.get("isolation") or "thread",
        "pid": job.get("pid"),
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


def _set_progress_locked(job: dict[str, Any], payload: dict[str, Any]) -> None:
    if payload.get("current") is not None:
        try:
            job["current"] = int(payload.get("current"))
        except Exception:
            pass
    if payload.get("total") is not None:
        try:
            job["total"] = max(0, int(payload.get("total")))
        except Exception:
            pass
    if payload.get("text") is not None:
        job["text"] = str(payload.get("text"))
    if payload.get("progress") is not None:
        try:
            job["progress"] = max(0.0, min(1.0, float(payload.get("progress"))))
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


def _send_process_event(payload: dict[str, Any]) -> None:
    sender = _PROCESS_SENDER
    if sender is None:
        return
    try:
        sender.send(payload)
    except Exception:
        pass


def _apply_process_limits() -> None:
    if os.name != "posix":
        return
    try:
        import resource

        if _PROCESS_MEMORY_MB > 0:
            memory_bytes = int(_PROCESS_MEMORY_MB) * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except Exception:
        # Some containers do not permit lowering rlimits. systemd MemoryMax is
        # the second protection layer in the supplied deployment unit.
        pass


def _process_entry(job_id: str, fn: Callable[[], Any], sender, cancel_event) -> None:
    global _PROCESS_SENDER, _PROCESS_CANCEL_EVENT
    _PROCESS_SENDER = sender
    _PROCESS_CANCEL_EVENT = cancel_event
    token = _CURRENT_JOB_ID.set(job_id)
    if _TRACE_AFTER_SECONDS:
        _LOGGER.warning("Tracing background process job %s (pid=%s)", job_id, os.getpid())
        faulthandler.dump_traceback_later(_TRACE_AFTER_SECONDS)
    _apply_process_limits()
    try:
        nodes_module = sys.modules.get("nodes")
        if nodes_module is not None:
            nodes_module.Node._reset_storage_after_fork()
        result = fn()
        try:
            encoded_size = len(pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))
        except Exception:
            encoded_size = _RESULT_MAX_BYTES + 1
        if encoded_size > _RESULT_MAX_BYTES:
            result = {
                "ok": True,
                "result_omitted": True,
                "reason": "Background result exceeded NODA_JOB_RESULT_MAX_BYTES",
            }
        _send_process_event({"type": "completed", "result": result})
    except JobCancelled:
        _send_process_event({"type": "cancelled"})
    except BaseException as exc:
        _send_process_event({
            "type": "failed",
            "error": {
                "message": str(exc) or exc.__class__.__name__,
                "traceback": traceback.format_exc(limit=30),
            },
        })
    finally:
        if _TRACE_AFTER_SECONDS:
            faulthandler.cancel_dump_traceback_later()
        _CURRENT_JOB_ID.reset(token)
        try:
            sender.close()
        except Exception:
            pass


def _run_process_job(job_id: str, fn: Callable[[], Any]) -> None:
    ctx = multiprocessing.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    cancel_event = ctx.Event()
    process = ctx.Process(
        target=_process_entry,
        args=(job_id, fn, sender, cancel_event),
        name=f"noda-job-{job_id[:8]}",
        daemon=False,
    )
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None or job.get("cancel_requested"):
            if job is not None:
                job["status"] = "cancelled"
                job["finished_at"] = _now()
            receiver.close()
            sender.close()
            return
        job["status"] = "running"
        job["started_at"] = _now()
        job["_cancel_event"] = cancel_event
    try:
        process.start()
    except Exception as exc:
        receiver.close()
        sender.close()
        with _LOCK:
            job = _JOBS.get(job_id)
            if job is not None:
                job["status"] = "failed"
                job["error"] = {
                    "message": f"Could not start isolated background process: {exc}"
                }
                job["finished_at"] = _now()
        return
    sender.close()
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is not None:
            job["pid"] = process.pid

    started = time.monotonic()
    cancel_started = None
    terminal_event = None
    timed_out = False
    terminate_started = None
    while process.is_alive():
        while receiver.poll(0.2):
            try:
                event = receiver.recv()
            except EOFError:
                break
            if event.get("type") == "progress":
                with _LOCK:
                    job = _JOBS.get(job_id)
                    if job is not None:
                        _set_progress_locked(job, event)
            else:
                terminal_event = event
        with _LOCK:
            job = _JOBS.get(job_id)
            cancelling = bool(job and job.get("cancel_requested"))
        if cancelling:
            cancel_event.set()
            cancel_started = cancel_started or time.monotonic()
            if time.monotonic() - cancel_started >= _CANCEL_GRACE_SECONDS:
                terminate_started = terminate_started or time.monotonic()
                process.terminate()
        if _PROCESS_TIMEOUT_SECONDS and time.monotonic() - started > _PROCESS_TIMEOUT_SECONDS:
            timed_out = True
            terminate_started = terminate_started or time.monotonic()
            process.terminate()
        if terminate_started and time.monotonic() - terminate_started > 5:
            try:
                process.kill()
            except AttributeError:
                process.terminate()
        process.join(timeout=0.2)

    process.join(timeout=1.0)
    while receiver.poll():
        try:
            event = receiver.recv()
        except EOFError:
            break
        if event.get("type") == "progress":
            with _LOCK:
                job = _JOBS.get(job_id)
                if job is not None:
                    _set_progress_locked(job, event)
        else:
            terminal_event = event
    receiver.close()

    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        job.pop("_cancel_event", None)
        if job.get("cancel_requested"):
            job["status"] = "cancelled"
        elif timed_out:
            job["status"] = "failed"
            job["error"] = {"message": "Background job exceeded its time limit"}
        elif terminal_event and terminal_event.get("type") == "completed":
            job["status"] = "completed"
            job["progress"] = 1.0
            job["result"] = terminal_event.get("result")
        elif terminal_event and terminal_event.get("type") == "cancelled":
            job["status"] = "cancelled"
        elif terminal_event and terminal_event.get("type") == "failed":
            job["status"] = "failed"
            job["error"] = terminal_event.get("error")
        else:
            job["status"] = "failed"
            detail = (
                f"terminated by signal {-process.exitcode}"
                if process.exitcode is not None and process.exitcode < 0
                else f"exited with code {process.exitcode}"
            )
            job["error"] = {
                "message": (
                    "Isolated background process " + detail
                    + "; it may have reached NODA_JOB_MEMORY_MB"
                )
            }
        job["finished_at"] = _now()


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
        "isolation": "process" if _PROCESS_MODE else "thread",
        "pid": None,
    }
    with _LOCK:
        _cleanup_locked()
        pending = sum(
            1 for item in _JOBS.values()
            if item.get("status") in {"queued", "running"}
        )
        if pending >= _MAX_WORKERS + _MAX_QUEUED:
            raise JobQueueFull(
                f"Background queue is full ({pending} pending jobs)"
            )
        _JOBS[job_id] = job

    def runner() -> None:
        token = _CURRENT_JOB_ID.set(job_id)
        try:
            if _PROCESS_MODE:
                _run_process_job(job_id, fn)
                return
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
            if _PROCESS_MODE:
                _LOGGER.exception("Background process job %s failed", job_id)
            with _LOCK:
                current = _JOBS.get(job_id)
                if current is not None and current["status"] not in {"completed", "failed", "cancelled"}:
                    current.pop("_cancel_event", None)
                    current["status"] = "failed"
                    current["error"] = {
                        "message": str(exc) or exc.__class__.__name__,
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
        cancel_event = job.get("_cancel_event")
        if cancel_event is not None:
            try:
                cancel_event.set()
            except Exception:
                pass
        if job.get("status") == "queued":
            job["status"] = "cancelled"
            job["finished_at"] = _now()
        return True


def current_job_id() -> str | None:
    return _CURRENT_JOB_ID.get()


def cancel_requested() -> bool:
    if _PROCESS_CANCEL_EVENT is not None:
        try:
            return bool(_PROCESS_CANCEL_EVENT.is_set())
        except Exception:
            pass
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
    if _PROCESS_SENDER is not None:
        _send_process_event({
            "type": "progress",
            "current": current,
            "total": total,
            "text": text,
            "progress": progress,
        })
        return
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        _set_progress_locked(job, {
            "current": current,
            "total": total,
            "text": text,
            "progress": progress,
        })


class JobCancelled(RuntimeError):
    pass


class JobQueueFull(RuntimeError):
    pass


def raise_if_cancelled() -> None:
    if cancel_requested():
        raise JobCancelled("Background job cancelled")
