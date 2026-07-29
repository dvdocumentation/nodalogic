from __future__ import annotations

"""File-based nGenie skill registry.

A skill is a Python file in client_app/ngenie_skills/ with this minimal shape:

SKILL_ID = "node_operations"
NAME = "Node operations"
DESCRIPTION = "Short routing description sent to the LLM router."
PROMPT = "Long prompt sent only after the router selected this skill."

def prepare_context(base_context):
    return {}

def validate_answer(answer, skill_context=None, base_context=None):
    return []

The router sees only DESCRIPTION/metadata. The second LLM step receives PROMPT
and the optional prepared context for selected skills.
"""

from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional
import importlib.util
import traceback


@dataclass
class NGenieSkill:
    id: str
    name: str
    description: str
    prompt: str
    module: ModuleType
    path: str


_CACHE: Optional[List[NGenieSkill]] = None


def skills_dir() -> Path:
    return Path(__file__).resolve().parent / "ngenie_skills"


def _load_module(path: Path) -> Optional[ModuleType]:
    try:
        module_name = "client_app.ngenie_skills." + path.stem
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        return mod
    except Exception:
        traceback.print_exc()
        return None


def load_skills(reload: bool = False) -> List[NGenieSkill]:
    global _CACHE
    if _CACHE is not None and not reload:
        return list(_CACHE)
    root = skills_dir()
    root.mkdir(parents=True, exist_ok=True)
    out: List[NGenieSkill] = []
    for path in sorted(root.glob("*.py")):
        if path.name.startswith("__"):
            continue
        mod = _load_module(path)
        if mod is None:
            continue
        if getattr(mod, "ENABLED", True) is False:
            continue
        sid = str(getattr(mod, "SKILL_ID", path.stem) or path.stem).strip()
        desc = str(getattr(mod, "DESCRIPTION", "") or "").strip()
        prompt = str(getattr(mod, "PROMPT", "") or "").strip()
        if not sid or not desc or not prompt:
            continue
        name = str(getattr(mod, "NAME", sid) or sid).strip()
        out.append(NGenieSkill(id=sid, name=name, description=desc, prompt=prompt, module=mod, path=str(path)))
    _CACHE = out
    return list(out)


def skill_catalog() -> List[Dict[str, str]]:
    """Small routing catalog: safe to send before the full prompt."""
    return [
        {"id": s.id, "name": s.name, "description": s.description}
        for s in load_skills()
        if getattr(s.module, "ROUTER_VISIBLE", True) is not False
    ]


def normalize_skill_ids(ids: Any) -> List[str]:
    if ids is None:
        return []
    if isinstance(ids, str):
        raw = [ids]
    elif isinstance(ids, (list, tuple, set)):
        raw = list(ids)
    else:
        raw = []
    available = {s.id for s in load_skills()}
    out: List[str] = []
    for x in raw:
        sid = str(x or "").strip()
        if sid and sid in available and sid not in out:
            out.append(sid)
    return out


def selected_skill_payloads(skill_ids: Any, base_context: Dict[str, Any]) -> List[Dict[str, Any]]:
    wanted = set(normalize_skill_ids(skill_ids))
    out: List[Dict[str, Any]] = []
    for skill in load_skills():
        if skill.id not in wanted:
            continue
        prepared: Any = {}
        fn = getattr(skill.module, "prepare_context", None)
        if callable(fn):
            try:
                prepared = fn(base_context)
            except Exception as e:
                prepared = {"error": f"prepare_context failed: {e}"}
        functions_prompt = str(getattr(skill.module, "FUNCTIONS_PROMPT", "") or "").strip()
        out.append({
            "id": skill.id,
            "name": skill.name,
            "description": skill.description,
            "prompt": skill.prompt,
            "functions_prompt": functions_prompt,
            "context": prepared if isinstance(prepared, (dict, list, str, int, float, bool)) or prepared is None else str(prepared),
        })
    return out


def validate_answer(answer: Dict[str, Any], skill_ids: Any, base_context: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    selected = set(normalize_skill_ids(skill_ids))
    for skill in load_skills():
        if skill.id not in selected:
            continue
        fn = getattr(skill.module, "validate_answer", None)
        if not callable(fn):
            continue
        try:
            res = fn(answer, None, base_context)
            if isinstance(res, str) and res.strip():
                errors.append(res.strip())
            elif isinstance(res, (list, tuple)):
                errors.extend([str(x) for x in res if str(x or "").strip()])
        except Exception as e:
            errors.append(f"{skill.id}: validate_answer failed: {e}")
    return errors
