"""Shared NodaLogic DataStructure parser.

This module is deliberately dependency-free.  The web Wizard/runtime and
Solutions validators must use the same parser so generation checks cannot
reject syntax that the real UI accepts (or accept syntax the UI cannot read).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def split_top_level(text: Any, delimiter: str = ",") -> List[str]:
    raw = str(text or "")
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    quote = ""
    esc = False
    for ch in raw:
        if quote:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                quote = ""
            continue
        if ch in ('"', "'"):
            quote = ch
            buf.append(ch)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}" and depth > 0:
            depth -= 1
        if ch == delimiter and depth == 0:
            item = "".join(buf).strip()
            if item:
                parts.append(item)
            buf = []
        else:
            buf.append(ch)
    item = "".join(buf).strip()
    if item:
        parts.append(item)
    return parts


def split_once_top_level(text: Any, delimiter: str) -> Tuple[str, str]:
    raw = str(text or "")
    depth = 0
    quote = ""
    esc = False
    for i, ch in enumerate(raw):
        if quote:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                quote = ""
            continue
        if ch in ('"', "'"):
            quote = ch
            continue
        if ch in "([{":
            depth += 1
            continue
        if ch in ")]}" and depth > 0:
            depth -= 1
            continue
        if ch == delimiter and depth == 0:
            return raw[:i].strip(), raw[i + 1:].strip()
    return raw.strip(), ""


def unquote(value: Any) -> str:
    s = str(value or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    return s


def parse_type(type_text: Any) -> Dict[str, Any]:
    t = str(type_text or "").strip()
    m = re.match(r"^(Node|ChildNode|DataSet|Dataset)\s*\((.*)\)$", t, re.I)
    if m:
        kind = m.group(1).lower()
        if kind in {"dataset", "dataset"}:
            kind = "dataset"
        return {"kind": kind, "target": unquote(m.group(2)), "raw": t}

    if t.startswith("[") and t.endswith("]"):
        inner = t[1:-1].strip()
        inner_type = parse_type(inner)
        return {
            "kind": "list",
            "item_type": inner_type,
            "target": inner_type.get("target") or "",
            "relation": inner_type.get("kind") or "",
            "raw": t,
        }

    if re.match(r"^select\s*\((.*)\)$", t, re.I):
        return {"kind": "select", "raw": t}

    low = t.lower()
    if low in {"str", "string", "text", "textarea"}:
        kind = "string"
    elif low in {"int", "integer", "number", "float", "double", "decimal"}:
        kind = "number"
    elif low in {"bool", "boolean", "checkbox", "check", "switch", "галочка"}:
        kind = "boolean"
    elif low in {"date", "datetime", "time"}:
        kind = low
    else:
        kind = low or "string"
    return {"kind": kind, "raw": t}


def parse_field_spec(spec: Any) -> Optional[Dict[str, Any]]:
    left, typ = split_once_top_level(spec, ":")
    if not left:
        return None
    label, name = split_once_top_level(left, "|")
    if not name:
        name = re.sub(r"[^a-zA-Z0-9_]+", "_", label.strip()).strip("_") or label.strip()
    name = str(name or "").strip()
    if not name:
        return None
    parsed_type = parse_type(typ or "string")
    return {
        "name": name,
        "label": str(label or name).strip(),
        "type": parsed_type.get("raw") or str(typ or "string"),
        "kind": parsed_type.get("kind") or "string",
        "target": parsed_type.get("target") or "",
        "item_type": parsed_type.get("item_type"),
    }


def parse_data_structure(text: Any) -> Dict[str, Any]:
    fields: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    virtual_tables: List[Dict[str, Any]] = []

    raw = str(text or "").strip()
    if not raw:
        return {"fields": fields, "tables": tables, "virtual_tables": virtual_tables}

    lines: List[str] = []
    for line in raw.replace(";", "\n").splitlines():
        line = line.strip().strip(",")
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        lines.extend(split_top_level(line, ","))

    for item in lines:
        item = str(item or "").strip()
        if not item:
            continue

        table_left, table_type = split_once_top_level(item, ":")
        if table_left and table_type.startswith("[") and table_type.endswith("]"):
            inner = table_type[1:-1].strip()
            inner_parts = split_top_level(inner, ",")
            has_field_declarations = any(bool(split_once_top_level(part, ":")[1]) for part in inner_parts)
            if has_field_declarations:
                row_fields = []
                for part in inner_parts:
                    fld = parse_field_spec(part)
                    if fld:
                        row_fields.append(fld)
                if row_fields:
                    table_label, table_name = split_once_top_level(table_left, "|")
                    if not table_name:
                        table_name = re.sub(r"[^a-zA-Z0-9_]+", "_", table_label.strip()).strip("_") or table_label.strip()
                    tables.append({
                        "name": str(table_name or "").strip(),
                        "label": str(table_label or table_name or "").strip(),
                        "kind": "inline_table",
                        "relation": "inline",
                        "row_class": "",
                        "fields": row_fields,
                        "inline": True,
                        "raw": item,
                    })
                    continue

        if item.startswith("[") and item.endswith("]"):
            inner = item[1:-1].strip()
            row_fields = []
            for part in split_top_level(inner, ","):
                fld = parse_field_spec(part)
                if fld:
                    row_fields.append(fld)
            if row_fields:
                virtual_tables.append({
                    "name": f"_table_{len(virtual_tables) + 1}",
                    "fields": row_fields,
                    "raw": item,
                })
            continue

        fld = parse_field_spec(item)
        if not fld:
            continue
        if fld.get("kind") == "list":
            item_type = fld.get("item_type") or {}
            fld["row_class"] = item_type.get("target") or fld.get("target") or ""
            fld["relation"] = item_type.get("kind") or fld.get("relation") or "node"
            tables.append(fld)
        else:
            fields.append(fld)

    return {"fields": fields, "tables": tables, "virtual_tables": virtual_tables}
