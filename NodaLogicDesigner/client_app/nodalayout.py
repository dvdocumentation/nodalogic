from __future__ import annotations

import base64
import json
import mimetypes
import os
import re
from html import escape
from typing import Any, Callable, Dict, List, Optional, Union

Layout = Union[str, List[List[Dict[str, Any]]]]

_VAR_RE = re.compile(r"@([A-Za-z0-9_]+)")


def _coerce_layout(layout: Layout) -> List[List[Dict[str, Any]]]:
    """Normalize layout into 2D rows."""
    if layout is None:
        return []
    if isinstance(layout, str):
        try:
            obj = json.loads(layout)
            if isinstance(obj, list):
                layout = obj
            else:
                return []
        except Exception:
            return []
    if not isinstance(layout, list):
        return []
    # if already 2d (list of list)
    if layout and all(isinstance(r, list) for r in layout):
        return layout  # type: ignore[return-value]
    # 1d -> 1 row
    if layout and all(isinstance(e, dict) for e in layout):
        return [layout]  # type: ignore[return-value]
    return []


def _resolve_vars(text: str, data: Dict[str, Any]) -> str:
    """Replace '@key' with data[key] string value."""
    def repl(m: re.Match) -> str:
        k = m.group(1)
        val = data.get(k)
        return "" if val is None else str(val)
    return _VAR_RE.sub(repl, text or "")


def _looks_like_data_uri(s: str) -> bool:
    return isinstance(s, str) and s.startswith("data:") and ";base64," in s


def _detect_image_mime(b: bytes) -> Optional[str]:
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if b.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if b.startswith(b"GIF87a") or b.startswith(b"GIF89a"):
        return "image/gif"
    if b.startswith(b"RIFF") and b[8:12] == b"WEBP":
        return "image/webp"
    return None


def _try_decode_base64(s: str) -> Optional[bytes]:
    ss = (s or "").strip()
    if len(ss) < 16:
        return None
    if not re.fullmatch(r"[A-Za-z0-9+/=\s]+", ss):
        return None
    ss = re.sub(r"\s+", "", ss)
    if len(ss) % 4 != 0:
        return None
    try:
        return base64.b64decode(ss, validate=False)
    except Exception:
        return None


def _picture_src(raw: str, assets_base_dir: Optional[str]) -> str:
    """
    picture value can be:
      - http(s) url
      - data uri
      - base64 raw bytes (png/jpg/webp/gif)
      - relative file name (resolved under assets_base_dir)
    """
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if _looks_like_data_uri(raw):
        return raw

    b = _try_decode_base64(raw)
    if b:
        mime = _detect_image_mime(b) or "application/octet-stream"
        return f"data:{mime};base64,{base64.b64encode(b).decode('ascii')}"

    if assets_base_dir:
        p = os.path.join(assets_base_dir, raw)
        if os.path.isfile(p):
            with open(p, "rb") as f:
                bb = f.read()
            mime = _detect_image_mime(bb) or mimetypes.guess_type(p)[0] or "application/octet-stream"
            return f"data:{mime};base64,{base64.b64encode(bb).decode('ascii')}"
    return ""


def _style_attr(
    el: Dict[str, Any],
    *,
    extra_css: Optional[List[str]] = None,
    default_full_width: bool = False,
    default_full_height: bool = False,
) -> str:
    """Build inline style based on *common* element props from LLM.txt.

    Supported common props:
      - visible: 1 (default) / 0 (hidden but takes space) / -1 (not rendered)
      - width, height: >0 => px; -1 => 100%; -2 => auto (wrap)
      - padding
      - size (font-size)
      - text_color, background (aliases of fg/bg)

    NOTE: "w" (weight) is applied in flex/grid contexts separately.
    """
    css: List[str] = []

    # visibility
    vis = el.get("visible", 1)
    if isinstance(vis, (int, float)):
        if int(vis) == 0:
            css.append("visibility:hidden")
        elif int(vis) < 0:
            css.append("display:none")

    # colors (aliases)
    bg = el.get("background")
    if not (isinstance(bg, str) and bg.strip()):
        bg = el.get("bg")
    if isinstance(bg, str) and bg.strip():
        css.append(f"background:{bg}")

    fg = el.get("text_color")
    if not (isinstance(fg, str) and fg.strip()):
        fg = el.get("fg")
    if isinstance(fg, str) and fg.strip():
        css.append(f"color:{fg}")

    pad = el.get("padding")
    if isinstance(pad, (int, float)):
        css.append(f"padding:{int(pad)}px")
    rad = el.get("radius")
    if isinstance(rad, (int, float)):
        css.append(f"border-radius:{int(rad)}px")
    size = el.get("size")
    if isinstance(size, (int, float)):
        css.append(f"font-size:{int(size)}px")

    if el.get("bold") is True:
        css.append("font-weight:700")

    # width/height
    wv = el.get("width")
    if wv is None and default_full_width:
        wv = -1
    if isinstance(wv, (int, float)):
        if wv > 0:
            css.append(f"width:{int(wv)}px")
        elif int(wv) == -1:
            css.append("width:100%")
        elif int(wv) == -2:
            css.append("width:auto")

    hv = el.get("height")
    if hv is None and default_full_height:
        hv = -1
    if isinstance(hv, (int, float)):
        if hv > 0:
            css.append(f"height:{int(hv)}px")
        elif int(hv) == -1:
            css.append("height:100%")
        elif int(hv) == -2:
            css.append("height:auto")

    if extra_css:
        css.extend([c for c in extra_css if isinstance(c, str) and c.strip()])

    if not css:
        return ""
    return ' style="' + escape(";".join(css)) + '"'


def _weight_css(el: Dict[str, Any], *, direction: str) -> List[str]:
    """Return extra CSS snippets to apply element weight `w` in flex containers.

    direction: "row" or "column".
    """
    w = el.get("w")
    if not isinstance(w, (int, float)):
        return []
    w = float(w)
    if w <= 0:
        return []

    # If explicit width/height is set (px/100%), we won't override it.
    # Otherwise use flex-grow to distribute remaining space.
    if direction == "row":
        if isinstance(el.get("width"), (int, float)) and float(el.get("width")) > 0:
            return [f"flex-grow:{w}"]
        return [f"flex:{w} 1 0"]
    # column
    if isinstance(el.get("height"), (int, float)) and float(el.get("height")) > 0:
        return [f"flex-grow:{w}"]
    return [f"flex:{w} 1 0"]


def _parse_table_header(cols: Any) -> List[Dict[str, Any]]:
    """
    table_header: ["Title|key|weight", ...]
    returns: [{title,key,weight},...]
    """
    out: List[Dict[str, Any]] = []
    if not isinstance(cols, list):
        return out
    for c in cols:
        if not isinstance(c, str):
            continue
        parts = c.split("|")
        title = (parts[0] if len(parts) > 0 else "").strip()
        key = (parts[1] if len(parts) > 1 else "").strip()
        w = 1.0
        if len(parts) > 2:
            try:
                w = float(parts[2].strip() or "1")
            except Exception:
                w = 1.0
        if not key:
            continue
        out.append({"title": title or key, "key": key, "weight": max(w, 0.1)})
    return out


def render_nodalayout_html(
    layout: Layout,
    node_data: Dict[str, Any],
    *,
    assets_base_dir: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Server-side renderer for the web client.

    context may include:
      - target: {"repo_id": int, "class_name": str, "node_id": str, "config_uid": str}
      - node_url: Callable[[str,str], str]
      - node_cover: Callable[[str,str], str]  (returns HTML)
      - node_children_tree: Callable[[str,str], List[Dict]] (recursive tree)
    """
    rows = _coerce_layout(layout)
    ctx_target = (context or {}).get("target") or {}

    def ctx_attr() -> str:
        attrs = []
        if "repo_id" in ctx_target:
            attrs.append(f'data-nl-repo="{escape(str(ctx_target.get("repo_id")))}"')
        if "class_name" in ctx_target:
            attrs.append(f'data-nl-class="{escape(str(ctx_target.get("class_name")))}"')
        if "node_id" in ctx_target:
            attrs.append(f'data-nl-node="{escape(str(ctx_target.get("node_id")))}"')
        return " " + " ".join(attrs) if attrs else ""

    out: List[str] = []
    out.append(f'<div class="nl-screen"{ctx_attr()}>')

    def render_inline(el: Any, *, parent_direction: Optional[str] = None) -> str:
        """Render one element into HTML.

        el may be:
          - dict (standard element)
          - str (shorthand text / title-value)
        """
        if isinstance(el, str):
            s = _resolve_vars(el, node_data)
            # "Title|Value" => horizontal row (value bold)
            if "|" in s:
                left, right = s.split("|", 1)
                return (
                    '<div class="nl-title-value">'
                    f'<span class="nl-tv-title">{escape(left.strip())}</span>'
                    f'<span class="nl-tv-value">{escape(right.strip())}</span>'
                    '</div>'
                )
            return f'<div class="nl-text">{escape(s)}</div>'

        if not isinstance(el, dict):
            return ""

        t = str(el.get("type") or "")
        extra_css = _weight_css(el, direction=parent_direction) if parent_direction in ("row", "column") else None
        style_attr = _style_attr(el, extra_css=extra_css)

        # Container-based layout elements
        if t in ("HorizontalLayout", "VerticalLayout"):
            direction = "row" if t == "HorizontalLayout" else "column"
            children = el.get("value") or []
            if not isinstance(children, list):
                children = []
            items = "".join(render_inline(ch, parent_direction=direction) for ch in children if isinstance(ch, (dict, str)))
            return f'<div class="nl-container nl-{direction}"{style_attr}>{items}</div>'

        if t in ("HorizontalScroll", "VerticalScroll"):
            axis = "x" if t == "HorizontalScroll" else "y"
            children = el.get("value") or []
            if not isinstance(children, list):
                children = []
            items = "".join(render_inline(ch) for ch in children if isinstance(ch, (dict, str)))
            return f'<div class="nl-scroll nl-scroll-{axis}"{style_attr}>{items}</div>'

        if t == "Card":
            # Card.value may be row-based (2D) or a list of dicts (1D)
            cv = el.get("value") or []
            card_inner = ""
            if isinstance(cv, list) and cv and all(isinstance(r, list) for r in cv):
                card_inner = render_nodalayout_html(cv, node_data, assets_base_dir=assets_base_dir, context=context)
            elif isinstance(cv, list):
                card_inner = "".join(render_inline(ch) for ch in cv if isinstance(ch, (dict, str)))
            elif isinstance(cv, dict):
                card_inner = render_inline(cv)
            return f'<div class="nl-card"{style_attr}>{card_inner}</div>'

        # Tabs / Tab
        # Schema (as in TT):
        # {"type":"Tabs","value":[{"type":"Tab","id":"my_tab_1","caption":"My tab 1","layout": ...}]}.
        # Note: some configs may contain a typo "layput" instead of "layout".
        if t == "Tabs":
            # Tabs should behave like a full-size container by default (match_parent).
            # IMPORTANT: when Tabs are placed inside flex-containers (HorizontalLayout/VerticalLayout),
            # plain width:100% is not always enough because the flex item may shrink to content.
            # Force a "take the whole line" flex-basis when parent is row/column.
            tabs_extra_css: List[str] = []
            if parent_direction in ("row", "column"):
                tabs_extra_css.extend(["flex:1 1 100%", "min-width:0"])
            merged_extra = []
            if isinstance(extra_css, list):
                merged_extra.extend(extra_css)
            merged_extra.extend(tabs_extra_css)
            style_attr = _style_attr(el, extra_css=merged_extra or None, default_full_width=True, default_full_height=True)
            tabs_any = el.get("value") or []
            if not isinstance(tabs_any, list):
                tabs_any = []

            # Build a stable (but unique enough) prefix for nested tabs.
            tabs_id = str(el.get("id") or "tabs")
            prefix = f"nl-tabs-{tabs_id}-{abs(hash((tabs_id, str(ctx_target.get('node_id') or ''))))%100000}"

            # Determine active tab
            active_id = str(el.get("active") or "").strip()
            if not active_id and tabs_any:
                for tt in tabs_any:
                    if isinstance(tt, dict) and (tt.get("id") or ""):
                        active_id = str(tt.get("id"))
                        break

            nav_parts: List[str] = []
            pane_parts: List[str] = []

            for idx, tab in enumerate(tabs_any):
                if not isinstance(tab, dict):
                    continue
                if str(tab.get("type") or "") not in ("Tab", "", None):
                    # Allow "Tab" but don't hard-fail for missing type.
                    pass
                tid = str(tab.get("id") or f"tab_{idx}")
                caption = _resolve_vars(str(tab.get("caption") or tid), node_data)
                pane_id = f"{prefix}-{escape(tid)}"
                is_active = (tid == active_id) or (idx == 0 and not active_id)
                btn_cls = "nav-link" + (" active" if is_active else "")
                pane_cls = "tab-pane fade" + (" show active" if is_active else "")

                nav_parts.append(
                    f'<li class="nav-item" role="presentation">'
                    f'<button class="{btn_cls}" data-bs-toggle="tab" data-bs-target="#{pane_id}" '
                    f'type="button" role="tab" aria-controls="{pane_id}" aria-selected="{"true" if is_active else "false"}">'
                    f'{escape(caption)}'
                    f'</button></li>'
                )

                tab_layout = tab.get("layout")
                if tab_layout is None:
                    tab_layout = tab.get("layput")
                inner = ""
                if tab_layout is not None:
                    try:
                        inner = render_nodalayout_html(tab_layout, node_data, assets_base_dir=assets_base_dir, context=context)
                    except Exception:
                        inner = '<div class="nl-text text-muted">layout error</div>'
                pane_parts.append(f'<div class="{pane_cls}" id="{pane_id}" role="tabpanel">{inner}</div>')

            nav_html = '<ul class="nav nav-tabs" role="tablist">' + "".join(nav_parts) + "</ul>"
            panes_html = '<div class="tab-content nl-tabs-content">' + "".join(pane_parts) + "</div>"
            return f'<div class="nl-tabs"{style_attr}>{nav_html}{panes_html}</div>'

        if t == "Text":
            txt = _resolve_vars(str(el.get("value") or ""), node_data)
            #allow_html = bool(el.get("html") or el.get("allow_html"))
            allow_html =True
            return f'<div class="nl-text"{style_attr}>{txt if allow_html else escape(txt)}</div>'

        if t == "HTML":
            html = _resolve_vars(str(el.get("value") or ""), node_data)
            return f'<div class="nl-html"{style_attr}>{html}</div>'

        if t == "Picture":
            raw = _resolve_vars(str(el.get("value") or ""), node_data)
            src = _picture_src(raw, assets_base_dir)
            alt = escape(str(el.get("caption") or ""))
            if src:
                return f'<img class="nl-picture"{style_attr} src="{escape(src)}" alt="{alt}"/>'
            return f'<div class="nl-picture nl-empty"{style_attr}></div>'

        
                # NodeInput (readonly input + pick button, value is "Class$Id")
        if t == "NodeInput":
            nid = str(el.get("id") or "").strip() or "node_input"
            iid = escape(nid)
            path = escape(nid)

            caption_raw = _resolve_vars(str(el.get("caption") or ""), node_data)
            show_label = bool(caption_raw.strip())
            label_html = f'<div class="nl-label">{escape(caption_raw)}</div>' if show_label else ""

            # what to show in the readonly field: prefer <id>_view, else value
            view_key = f"{nid}_view"
            display = node_data.get(view_key)
            if display is None:
                raw_val = el.get("value")
                if isinstance(raw_val, str) and raw_val.startswith("@"):
                    display = node_data.get(raw_val[1:], "")
                else:
                    display = _resolve_vars(str(raw_val or ""), node_data)
            display = "" if display is None else str(display)

            # dataset can be:
            # - string: class name
            # - list: uid list like Table nodes_source (["Class$Id", ...] or [{"_class":..,"_id":..}])
            ds = el.get("dataset")
            ds_kind = "none"
            ds_payload = ""
            if isinstance(ds, str) and ds.strip():
                ds_kind = "class"
                ds_payload = escape(ds.strip())
            elif isinstance(ds, list):
                ds_kind = "uids"
                try:
                    ds_payload = escape(json.dumps(ds, ensure_ascii=False))
                except Exception:
                    ds_payload = "[]"

            style_attr_input = _style_attr(el, extra_css=extra_css, default_full_width=True)
            # button is wrap_content
            btn_style = _style_attr({"width": -2, "height": -2}, extra_css=None)

            return (
                f'<div class="nl-field nl-field-row">'
                f'{label_html}'
                f'<div class="nl-nodeinput" style="display:flex;gap:8px;align-items:center;flex:1 1 auto;min-width:0">'
                f'<input class="nl-input nl-nodeinput-text"{style_attr_input} '
                f'data-path="{path}" data-id="{iid}" data-nl-nodeinput="1" '
                f'readonly value="{escape(display)}"/>'
                f'<button class="nl-button nl-nodeinput-btn"{btn_style} type="button" '
                f'data-nl-nodepick="1" data-nl-listener="{iid}" '
                f'data-nl-ds-kind="{ds_kind}" data-nl-ds="{ds_payload}">…</button>'
                f'<button class="nl-button nl-nodeinput-clear"{btn_style} type="button" '
                f'data-nl-clear="1" data-nl-clear-target="{iid}">×</button>'
                f'</div>'
                f'</div>'
            )

                # DatasetInput (readonly input + pick + clear, value is "dataset$Id")
        
        if t == "DatasetInput":
            fid = str(el.get("id") or "").strip() or "dataset_input"
            iid = escape(fid)
            path = escape(fid)

            caption_raw = _resolve_vars(str(el.get("caption") or ""), node_data)
            show_label = bool(caption_raw.strip())
            label_html = f'<div class="nl-label">{escape(caption_raw)}</div>' if show_label else ""

            # what to show: prefer <id>_view, else value
            view_key = f"{fid}_view"
            display = node_data.get(view_key)
            if display is None:
                raw_val = el.get("value")
                if isinstance(raw_val, str) and raw_val.startswith("@"):
                    display = node_data.get(raw_val[1:], "")
                else:
                    display = _resolve_vars(str(raw_val or ""), node_data)
            display = "" if display is None else str(display)

            ds_name = str(el.get("dataset") or "").strip()  # e.g. "goods"

            style_attr_input = _style_attr(el, extra_css=extra_css, default_full_width=True)
            btn_style = _style_attr({"width": -2, "height": -2}, extra_css=None)

            return (
                f'<div class="nl-field nl-field-row">'
                f'{label_html}'
                f'<div class="nl-nodeinput" style="display:flex;gap:8px;align-items:center;flex:1 1 auto;min-width:0">'
                f'<input class="nl-input nl-datasetinput-text"{style_attr_input} '
                f'data-path="{path}" data-id="{iid}" data-nl-datasetinput="1" '
                f'readonly value="{escape(display)}"/>'
                f'<button class="nl-button nl-datasetinput-btn"{btn_style} type="button" '
                f'data-nl-datasetpick="1" data-nl-listener="{iid}" '
                f'data-nl-dataset-name="{escape(ds_name)}">…</button>'
                f'<button class="nl-button nl-datasetinput-clear"{btn_style} type="button" '
                f'data-nl-clear="1" data-nl-clear-target="{iid}">×</button>'
                f'</div>'
                f'</div>'
            )


        # Inputs
        if t in ("Input", "TextInput"):
            path = escape(str(el.get("id") or ""))
            v = _resolve_vars(str(el.get("value") or ""), node_data)
            caption_raw = _resolve_vars(str(el.get("caption") or ""), node_data)
            ph = escape(caption_raw)
            iid = escape(str(el.get("id") or path))
            itype = str(el.get("input_type") or "").upper().strip()
            events_attr = ' data-nl-events="1"' if el.get("events") else ""

            # Inputs default to full width in row-based string layouts (Android-like MATCH_PARENT)
            style_attr_input = _style_attr(el, extra_css=extra_css, default_full_width=False)

            # Web UX: show caption as a label (not only as placeholder).
            # - MULTILINE: label on top
            # - others: label on the left, taking half of the available field width
            show_label = bool(caption_raw.strip())
            label_html = f'<div class="nl-label">{escape(caption_raw)}</div>' if show_label else ""

            if itype == "MULTILINE":
                textarea_cls = "nl-input nl-input-multiline"
                field_cls = "nl-field nl-field-col"
                # help height:100% work inside flex containers
                if isinstance(el.get("height"), (int, float)) and int(el.get("height")) == -1:
                    textarea_cls += " nl-input-fill"
                    field_cls += " nl-field-fill"
                return (
                    f'<div class="{field_cls}">'
                    f'{label_html}'
                    f'<textarea class="{textarea_cls}"{style_attr_input} '
                    f'data-path="{path}" data-id="{iid}"{events_attr} '
                    f'placeholder="{ph}">{escape(v)}</textarea>'
                    f'</div>'
                )

            html_type = "text"
            if itype == "NUMBER":
                html_type = "number"
            elif itype == "PASSWORD":
                html_type = "password"
            elif itype == "DATE":
                html_type = "date"

            date_attr = ' data-nl-date="1"' if itype == "DATE" else ""
            return (
                f'<div class="nl-field nl-field-row">'
                f'{label_html}'
                f'<input class="nl-input"{style_attr_input} data-path="{path}" data-id="{iid}" '
                f'type="{html_type}"{events_attr}{date_attr} '
                f'value="{escape(v)}" placeholder="{ph}"/>'
                f'</div>'
            )

        if t == "CheckBox":
            iid = escape(str(el.get("id") or "cb"))
            cap = _resolve_vars(str(el.get("caption") or ""), node_data)
            val = el.get("value")
            path = str(el.get("id") or "")
            if isinstance(val, str) and val.startswith("@"):
                val = node_data.get(val[1:])
                if not path:
                    path = val[1:] if isinstance(val, str) else str(el.get("id") or "")
            checked = " checked" if str(val).lower() in ("1", "true", "yes", "on") else ""
            events_attr = ' data-nl-events="1"' if el.get("events") else ""
            # data-path enables automatic _data sync; data-id is used as listener
            return (
                f'<label class="nl-check"{style_attr}>'
                f'<input type="checkbox" data-id="{iid}" data-path="{escape(path or str(el.get("id") or ""))}"{events_attr}{checked}/> {escape(cap)}'
                f"</label>"
            )
                
        # Spinner (dropdown)
        if t == "Spinner":
            sid = str(el.get("id") or "").strip()
            if not sid:
                sid = "spinner"
            iid = escape(sid)

            # path is where selected _id is stored
            path = escape(sid)

            # caption (label)
            caption_raw = _resolve_vars(str(el.get("caption") or ""), node_data)
            show_label = bool(caption_raw.strip())
            label_html = f'<div class="nl-label">{escape(caption_raw)}</div>' if show_label else ""

            # dataset
            ds = el.get("dataset") or []
            if not isinstance(ds, list):
                ds = []

            # current value
            raw_val = el.get("value")
            cur = ""
            if isinstance(raw_val, str) and raw_val.startswith("@"):
                cur = str(node_data.get(raw_val[1:], "") or "")
            else:
                cur = _resolve_vars(str(raw_val or ""), node_data)

            # options
            opt_parts: List[str] = []
            for item in ds:
                if not isinstance(item, dict):
                    continue
                oid = str(item.get("_id") or "").strip()
                if not oid:
                    continue
                view = str(item.get("_view") or oid)
                sel = " selected" if oid == cur else ""
                opt_parts.append(
                    f'<option value="{escape(oid)}"{sel}>{escape(view)}</option>'
                )

            # store dataset on element for JS
            try:
                ds_json = escape(json.dumps(ds, ensure_ascii=False))
            except Exception:
                ds_json = "[]"

            events_attr = ' data-nl-events="1"' if el.get("events") else ""

            # Spinner sizing: same rules as Input
            style_attr_input = _style_attr(el, extra_css=extra_css, default_full_width=True)

            return (
                f'<div class="nl-field nl-field-row">'
                f'{label_html}'
                f'<select class="nl-input nl-spinner"{style_attr_input} '
                f'data-path="{path}" data-id="{iid}" '
                f'data-nl-spinner="1" data-nl-dataset="{ds_json}"{events_attr}>'
                + "".join(opt_parts) +
                '</select>'
                '</div>'
            )

        if t == "Switch":
            iid = escape(str(el.get("id") or "sw"))
            cap = _resolve_vars(str(el.get("caption") or ""), node_data)
            val = el.get("value")
            path = str(el.get("id") or "")
            if isinstance(val, str) and val.startswith("@"):
                val = node_data.get(val[1:])
            events_attr = ' data-nl-events="1"' if el.get("events") else ""
            checked = " checked" if str(val).lower() in ("1", "true", "yes", "on") else ""
            return (
                f'<label class="nl-switch"{style_attr}>'
                f'<span class="nl-switch-caption">{escape(cap)}</span>'
                f'<span class="nl-switch-control">'
                f'<input type="checkbox" data-id="{iid}" data-path="{escape(path or str(el.get("id") or ""))}"{events_attr}{checked}/>'
                f'<span class="nl-switch-slider"></span>'
                f'</span>'
                f'</label>'
            )

        # Active elements
        if t == "Button":
            caption = _resolve_vars(str(el.get("caption", el.get("value", "Button"))), node_data)
            bid = escape(str(el.get("id") or "btn"))
            # no match_parent in web: use wrap_content by default
            return (
                f'<button class="nl-button"{style_attr} data-nl-click="1" data-nl-listener="{bid}" '
                f'type="button">{escape(caption)}</button>'
            )

        if t == "Table":
            return render_table(el, context)

        if t == "NodeChildren":
            return render_node_children(el)

        # Unknown
        return f'<pre class="nl-unknown"{style_attr}>{escape(json.dumps(el, ensure_ascii=False, indent=2))}</pre>'

    def render_table(el: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
        """
        Table renderer.

        Если nodes_source=True:
        value may be:
            - [{"class": "...", "id": "..."}] или [{"_class": "...", "_id": "..."}]
            - ["Class$Id", "Class$Id", ...]  <-- new UID format
            - ["Id", "Id", ...]             <-- old format
        """
        tid = escape(str(el.get("id") or "tbl"))
        nodes_source = bool(el.get("nodes_source"))
        as_table = bool(el.get("table"))
        # Ensure tables never overflow the available content width.
        # (Bootstrap tables can become wider than the container if long content appears.)
        style_attr = _style_attr(
            el,
            extra_css=["max-width:100%", "overflow-x:auto"],
            default_full_width=True,
        )

        value = el.get("value") or []
        rows_data: List[Any] = value if isinstance(value, list) else []

        # ------- helpers -------
        def _parse_uid(uid):
            if uid is None:
                return None, None
            parts = str(uid).split("$")
            if len(parts) >= 3:
                # cfg$Class$Id
                return parts[-2], parts[-1]
            if len(parts) == 2:
                # Class$Id
                return parts[0], parts[1]
            return None, parts[0]

        if not nodes_source:
            
            return old_render_table(el)

        # ------- nodes_source=True -------
        rows_nodes: List[Dict[str, str]] = []

        for r in rows_data:
            if isinstance(r, dict):
                cid = str(r.get("id") or r.get("_id") or "").strip()
                ccl = str(r.get("class") or r.get("_class") or "").strip()
                if cid and ccl:
                    rows_nodes.append({"class": ccl, "id": cid})
                else:
                    # if dict contain uid as string
                    u = str(r.get("uid") or "").strip()
                    if u:
                        c2, i2 = _parse_uid(u)
                        rows_nodes.append({"class": c2, "id": i2})

            elif isinstance(r, str):
                c2, i2 = _parse_uid(r)
                rows_nodes.append({"class": c2, "id": i2})

        if as_table:
            cover_fn: Optional[Callable[[str, str], str]] = (context or {}).get("node_cover_table") or (context or {}).get("node_cover")
        else:
            cover_fn = (context or {}).get("node_cover") or (context or {}).get("node_cover_table")
        url_fn: Optional[Callable[[str, str], str]] = (context or {}).get("node_url")
        res_fn: Optional[Callable[[str], Any]] = (context or {}).get("uid_resolve")  # for old format

        items_html: List[str] = []

        for rn in rows_nodes:
            node_id = (rn.get("id") or "").strip()
            node_class = (rn.get("class") or "").strip()

            
            if (not node_class) and node_id and callable(res_fn):
                try:
                    c3, i3 = res_fn(node_id)
                    if c3 and i3:
                        node_class, node_id = str(c3), str(i3)
                except Exception:
                    pass

            cover_html = ""
            if callable(cover_fn) and node_class and node_id:
                try:
                    cover_html = str(cover_fn(node_class, node_id) or "")
                except Exception:
                    cover_html = ""
            if not cover_html:
                # fallback
                label = f"{node_class}/" if node_class else ""
                cover_html = f'<div class="nl-text">{escape(label)}{escape(node_id)}</div>'

            open_url = ""
            if callable(url_fn) and node_class and node_id:
                try:
                    open_url = str(url_fn(node_class, node_id) or "")
                except Exception:
                    open_url = ""

            
            open_attr = f' data-nl-open="1" data-nl-open-url="{escape(open_url)}"' if open_url else ""
            target_attr = f' data-nl-target-class="{escape(node_class)}" data-nl-target-node="{escape(node_id)}"'
            items_html.append(
                f'<div class="nl-node-row nl-clickable"{open_attr}{target_attr}>{cover_html}</div>'
            )

        # table-like container
        if as_table:
            cols = _parse_table_header(el.get("table_header"))
            if cols:
                # Header columns
                thead = "".join(f'<th scope="col">{escape(str(c["title"]))}</th>' for c in cols)

                # Column weights (3rd part of table_header):
                #   "Title|key|5" -> weight=5
                #   "Title|key"   -> weight defaults to 1
                total_w = 0.0
                for c in cols:
                    try:
                        total_w += float(c.get("weight") or 1)
                    except Exception:
                        total_w += 1.0
                if total_w <= 0:
                    total_w = 1.0
                colgroup = "".join(
                    f'<col style="width:{(float(c.get("weight") or 1)/total_w)*100:.6f}%">'
                    for c in cols
                )

                # For nodes_source=True we want real column cells (not a single colspan cell)
                # so that `table_header` behaves the same as for plain `value=[{...}]`.
                node_data_fn: Optional[Callable[[str, str], Any]] = (context or {}).get("node_data")

                def _get_by_path(obj: Any, path: str) -> Any:
                    if not path:
                        return None
                    cur = obj
                    for part in str(path).split("."):
                        if cur is None:
                            return None
                        if isinstance(cur, dict):
                            cur = cur.get(part)
                        else:
                            cur = getattr(cur, part, None)
                    return cur

                body_rows: List[str] = []
                for rn in rows_nodes:
                    node_id = (rn.get("id") or "").strip()
                    node_class = (rn.get("class") or "").strip()

                    # fetch node data once per row (best-effort)
                    nd: Any = {}
                    if callable(node_data_fn) and node_class and node_id:
                        try:
                            nd = node_data_fn(node_class, node_id) or {}
                        except Exception:
                            nd = {}

                    # build cell values
                    tds: List[str] = []
                    for c in cols:
                        # For Table header we use the 2nd part as a key in node._data
                        key = str(c.get("key") or "").strip()
                        v: Any = ""
                        if key:
                            v = _get_by_path(nd, key)

                        if isinstance(v, (dict, list)):
                            try:
                                v = json.dumps(v, ensure_ascii=False)
                            except Exception:
                                v = str(v)
                        if v is None:
                            v = ""
                        tds.append(
                            f'<td style="overflow:hidden;text-overflow:ellipsis;">{escape(str(v))}</td>'
                        )

                    # make the whole row clickable (open node form)
                    open_url = ""
                    if callable(url_fn) and node_class and node_id:
                        try:
                            open_url = str(url_fn(node_class, node_id) or "")
                        except Exception:
                            open_url = ""
                    open_attr = f' data-nl-open="1" data-nl-open-url="{escape(open_url)}"' if open_url else ""
                    target_attr = f' data-nl-target-class="{escape(node_class)}" data-nl-target-node="{escape(node_id)}"'
                    body_rows.append(
                        f'<tr class="nl-tr nl-clickable"{open_attr}{target_attr}>' + "".join(tds) + "</tr>"
                    )

                body = "".join(body_rows)
                return (
                    f'<div class="nl-table-wrap" data-nl-table="{tid}"{style_attr}>'
                    f'<table class="nl-table-el" style="width:100%;max-width:100%;table-layout:fixed;">'
                    f'<colgroup>{colgroup}</colgroup>'
                    f'<thead><tr>{thead}</tr></thead>'
                    f'<tbody>{body}</tbody></table></div>'
                )

        # table-like list container (default)
        return f'<div class="nl-table nl-nodes" id="{tid}"{style_attr}>' + "".join(items_html) + "</div>"

    def old_render_table(el: Dict[str, Any]) -> str:
        """
        Default behavior:
          - grid cards if "table" is not true
          - real table if "table": true

        Additionally:
          - nodes_source: true => rows are nodes, use node cover for each row.
        """
        tid = escape(str(el.get("id") or "tbl"))
        style_attr = _style_attr(el, default_full_width=True)
        nodes_source = bool(el.get("nodes_source"))
        as_table = bool(el.get("table"))

        table_layout = el.get("layout")
        if not isinstance(table_layout, list):
            table_layout = None

        value = el.get("value") or []
        # Normalize rows
        rows_data: List[Any] = []
        if isinstance(value, list):
            rows_data = value
        elif isinstance(value, str):
            ds_name = value.strip()
            as_table = bool(el.get("table"))
            
            return (
                f'<div class="nl-dataset-table" data-nl-dataset-table="1" '
                f'data-nl-table-id="{tid}" data-nl-dataset-name="{escape(ds_name)}" '
                f'data-nl-as-table="{"1" if as_table else "0"}"{style_attr}>'
                f'<input class="form-control form-control-sm" '
                f'data-nl-dataset-search="1" placeholder="Поиск..."/>'
                f'<div class="mt-2" data-nl-dataset-body="1"></div>'
                f'</div>'
            )

        if nodes_source:
            # Try to interpret each row as {"class":..,"id":..} or {"_class":..,"_id":..}
            rows_nodes: List[Dict[str, str]] = []
            for r in rows_data:
                if isinstance(r, dict):
                    cid = str(r.get("id") or r.get("_id") or "")
                    ccl = str(r.get("class") or r.get("_class") or "")
                    if cid and ccl:
                        rows_nodes.append({"class": ccl, "id": cid})
                elif isinstance(r, str):
                    # if only id is given, we can't reliably infer class; fallback to showing id.
                    rows_nodes.append({"class": "", "id": r})
            # Render as table-like list
            items_html: List[str] = []
            for rn in rows_nodes:
                node_id = rn.get("id", "")
                node_class = rn.get("class", "")

                if (not node_class) and node_id:
                    res_fn = (context or {}).get("uid_resolve")
                    if callable(res_fn):
                        try:
                            c2, i2 = res_fn(node_id)
                            if c2 and i2:
                                node_class, node_id = str(c2), str(i2)
                        except Exception:
                            pass

                cover_fn: Optional[Callable[[str, str], str]] = (context or {}).get("node_cover_table") or (context or {}).get("node_cover")
                url_fn: Optional[Callable[[str, str], str]] = (context or {}).get("node_url")
                cover_html = ""
                if callable(cover_fn) and node_class and node_id:
                    cover_html = str(cover_fn(node_class, node_id))
                else:
                    cover_html = f'<div class="nl-text">{escape(node_class + "/" if node_class else "")}{escape(node_id)}</div>'
                open_url = ""
                if callable(url_fn) and node_class and node_id:
                    open_url = str(url_fn(node_class, node_id))

                items_html.append(
                    f'<div class="nl-node-row nl-clickable" '
                    f'data-nl-open="1" data-nl-open-url="{escape(open_url)}" '
                    f'data-nl-target-class="{escape(node_class)}" data-nl-target-node="{escape(node_id)}">'
                    f'{cover_html}</div>'
                )

            if as_table:
                cols = _parse_table_header(el.get("table_header"))
                if cols:
                    thead = "".join(f'<th scope="col">{escape(str(c["title"]))}</th>' for c in cols)
                    
                    body = "".join(
                        f'<tr class="nl-tr"><td colspan="{len(cols)}">{h}</td></tr>'
                        for h in items_html
                    )
                    return (
                        f'<div class="nl-table-wrap" data-nl-table="{tid}"{style_attr}>'
                        f'<table class="nl-table-el">'
                        f'<thead><tr>{thead}</tr></thead>'
                        f'<tbody>{body}</tbody></table></div>'
                    )
            # nodes_source + grid:
            return f'<div class="nl-table nl-nodes" data-nl-table="{tid}"{style_attr}>' + "".join(items_html) + "</div>"

        # Non-nodes rows (plain dict rows)
        row_dicts: List[Dict[str, Any]] = [r for r in rows_data if isinstance(r, dict)]

        if not as_table:
            # Grid of cards
            cards: List[str] = []
            for i, r in enumerate(row_dicts):
                key = escape(str(r.get("key") or r.get("_id") or i))

                # per-row overrides
                row_bg = r.get("_background")
                row_style = f' style="background:{escape(str(row_bg))}"' if isinstance(row_bg, str) and row_bg.strip() else ""

                row_layout = r.get("_layout")
                if not isinstance(row_layout, (list, dict)):
                    row_layout = table_layout

                if isinstance(row_layout, (list, dict)):
                    # special layout for this row (context is the row itself)
                    try:
                        inner = render_nodalayout_html(row_layout, r, assets_base_dir=assets_base_dir, context=context)
                    except Exception:
                        inner = '<div class="nl-text text-muted">layout error</div>'
                else:
                    # Card default: show key/value lines (values may include inline UI elements)
                    parts = []
                    for k, v in r.items():
                        if str(k).startswith("_"):
                            continue
                        if isinstance(v, (dict, str)):
                            vv = render_inline(v)
                        else:
                            vv = escape("" if v is None else str(v))
                        parts.append(f'<div class="nl-kv"><span class="nl-k">{escape(str(k))}</span><span class="nl-v">{vv}</span></div>')
                    inner = "".join(parts) or '<div class="nl-text text-muted">empty</div>'

                cards.append(
                    f'<div class="nl-grid-card nl-clickable" data-nl-click="1" data-nl-listener="{tid}" '
                    f'data-nl-table="{tid}" data-nl-row="{key}" data-nl-row-index="{i}"{row_style}>{inner}</div>'
                )
            return f'<div class="nl-grid" data-nl-table="{tid}"{style_attr}>' + "".join(cards) + "</div>"

        # Real table
        cols = _parse_table_header(el.get("table_header"))
        if not cols and row_dicts:
            # default headers from first row keys (skip internal _keys)
            cols = [{"title": k, "key": k, "weight": 1.0} for k in row_dicts[0].keys() if not str(k).startswith("_")]

        total_w = sum(float(c.get("weight") or 1.0) for c in cols) or 1.0
        col_perc = [100.0 * float(c.get("weight") or 1.0) / total_w for c in cols]

        thead = "".join(f'<th scope="col">{escape(str(c["title"]))}</th>' for c in cols)
        rows_html: List[str] = []
        for i, r in enumerate(row_dicts):
            key = escape(str(r.get("key") or r.get("_id") or i))

            row_bg = r.get("_background")
            row_style = f' style="background:{escape(str(row_bg))}"' if isinstance(row_bg, str) and row_bg.strip() else ""

            # per-row custom layout inside table (spans all columns)
            row_layout = r.get("_layout")
            if not isinstance(row_layout, (list, dict)):
                row_layout = table_layout

            if isinstance(row_layout, (list, dict)):
                try:
                    inner = render_nodalayout_html(row_layout, r, assets_base_dir=assets_base_dir, context=context)
                except Exception:
                    inner = '<div class="nl-text text-muted">layout error</div>'
                tds_html = f'<td colspan="{len(cols)}">{inner}</td>'
            else:
                tds = []
                for c in cols:
                    v = r.get(c["key"])
                    if isinstance(v, (dict, str)):
                        cell = render_inline(v)
                    else:
                        cell = escape("" if v is None else str(v))
                    tds.append(f"<td>{cell}</td>")
                tds_html = "".join(tds)

            rows_html.append(
                f'<tr class="nl-tr nl-clickable" data-nl-click="1" data-nl-listener="{tid}" data-nl-table="{tid}" '
                f'data-nl-row="{key}" data-nl-row-index="{i}"{row_style}>' + tds_html + "</tr>"
            )

        colgroup = "<colgroup>" + "".join(f'<col style="width:{p:.4f}%"/>' for p in col_perc) + "</colgroup>"
        return (
            f'<div class="nl-table-wrap" data-nl-table="{tid}"{style_attr}>'
            f'<table class="nl-table-el">'
            f"{colgroup}<thead><tr>{thead}</tr></thead>"
            f"<tbody>{''.join(rows_html)}</tbody></table></div>"
        )


    def render_node_children(el: Dict[str, Any]) -> str:
        """Recursive child cards with indentation, using node_children_tree() context if available."""
        nid = escape(str(el.get("id") or "children"))
        style_attr = _style_attr(el, default_full_width=True)
        tree_fn: Optional[Callable[[str, str], List[Dict[str, Any]]]] = (context or {}).get("node_children_tree")
        if not callable(tree_fn):
            return f'<div class="nl-stub" data-type="NodeChildren">NodeChildren (no provider)</div>'

        class_name = str(ctx_target.get("class_name") or "")
        node_id = str(ctx_target.get("node_id") or "")
        tree = tree_fn(class_name, node_id) or []

        def render_tree(nodes: List[Dict[str, Any]], depth: int) -> str:
            if not nodes:
                return ""
            parts: List[str] = []
            for n in nodes:
                c = str(n.get("class") or "")
                i = str(n.get("id") or "")
                cover = str(n.get("cover_html") or n.get("cover") or "")
                url = str(n.get("open_url") or "")
                if (not url) and c and i:
                    url_fn = (context or {}).get("node_url")
                    if callable(url_fn):
                        try:
                            url = str(url_fn(c, i))
                        except Exception:
                            url = ""
                children = n.get("children") or []
                indent_px = depth * 18
                if depth > 0:
                    parts.append(f'<div class="nl-child-item" style="margin-left:{indent_px}px">'
                                 f'<div class="nl-child-line" style="left:{indent_px - 10}px"></div>')
                else:
                    parts.append(f'<div class="nl-child-item" style="margin-left:{indent_px}px">')
                parts.append(
                    f'<div class="nl-child-card nl-clickable" data-nl-open="1" '
                    f'data-nl-open-url="{escape(url)}" data-nl-target-class="{escape(c)}" data-nl-target-node="{escape(i)}">'
                    f'{cover}</div>'
                )
                if children:
                    parts.append(render_tree(children, depth + 1))
                parts.append("</div>")
            return "".join(parts)

        return f'<div class="nl-node-children" data-nl-id="{nid}"{style_attr}>' + render_tree(tree, 0) + "</div>"

    # Render row-based root layout
    for r_i, row in enumerate(rows):
        # "Parameters" is a special pseudo-element that applies layout params to the *row itself*
        # (similar to Android LayoutParams for a LinearLayout child).
        row_params: Optional[Dict[str, Any]] = None
        if isinstance(row, list) and row:
            first = row[0]
            if isinstance(first, dict) and str(first.get("type") or "") == "Parameters":
                row_params = first
                row = row[1:]

        row_extra_css: List[str] = []
        if isinstance(row_params, dict):
            # rows are stacked vertically => "column" direction
            row_extra_css.extend(_weight_css(row_params, direction="column"))
            # allow min-height:0 so nested 100% heights can work
            row_extra_css.append("min-height:0")

        row_style_attr = _style_attr(row_params or {}, extra_css=row_extra_css) if row_params else ' style="min-height:0"'
        def _is_full_width_cell(e: Any) -> bool:
            
            if not isinstance(e, dict):
                return False
            if e.get("type") in ("Table", "NodeChildren", "Tabs"):
                return True
            if isinstance(e.get("width"), (int, float)) and int(e.get("width")) == -1:
                return True
            return False

        renderable = [e for e in row if isinstance(e, (dict, str))]
        full_flags = [(_is_full_width_cell(e) if isinstance(e, dict) else False) for e in renderable]
        normal_count = sum(1 for f in full_flags if not f)

        # default rule:
        # - 1 normal element => half width centered => cols=2 + center flag
        # - N normal elements => cols=N
        row_cols = 1
        row_center_half = False
        if normal_count == 1:
            row_cols = 2
            row_center_half = True
        elif normal_count > 1:
            row_cols = normal_count

        row_cls = "nl-row"
        

        # add css var --nl-cols to existing style attr
        if row_style_attr.startswith(' style="'):
            row_style_attr = row_style_attr[:-1] + f";--nl-cols:{row_cols}" + '"'
        elif row_style_attr:
            # unexpected, but keep safe
            row_style_attr = row_style_attr + f' style="--nl-cols:{row_cols}"'
        else:
            row_style_attr = f' style="--nl-cols:{row_cols}"'

        out.append(f'<div class="{row_cls}"{row_style_attr}>')
        #out.append(f'<div class="nl-row"{row_style_attr}>')

        for c_i, el in enumerate(row):
            if not isinstance(el, (dict, str)):
                continue
            cell_cls = "nl-cell"
            # Full-width elements (match_parent) should occupy the whole row.
            is_field = False
            if isinstance(el, dict):
                t = el.get("type")
   
                if t in ("Input", "TextInput", "Spinner",  "Date", "DateInput",   "NodeInput", "DatasetField", "DatasetInput"):
                    is_field = True
                # Some elements are containers and should take full row width by default.
                # Tabs in particular looked "shrunken" because their wrapper cell is a flex item
                # with auto width; mark them as full-width like Table/NodeChildren.
                if el.get("type") in ("Table", "NodeChildren", "Tabs"):
                    cell_cls += " nl-cell-full"
                elif isinstance(el.get("width"), (int, float)) and int(el.get("width")) == -1:
                    cell_cls += " nl-cell-full"
                #elif el.get("type") in ("Input", "TextInput") and el.get("width") is None:
                #    # Inputs default to full width (match_parent) in string layouts
                #    cell_cls += " nl-cell-full"

            if is_field and "nl-cell-full" not in cell_cls:
                cell_cls += " nl-cell-field"
            out.append(f'<div class="{cell_cls}" data-col="{c_i}">')
            out.append(render_inline(el))
            out.append("</div>")

        out.append("</div>")

    out.append("</div>")
    return "\n".join(out)


DEFAULT_NL_CSS = """
/* NodaLayout (web) */
.nl-screen{display:flex;flex-direction:column;gap:10px;max-width:100%;height:100%;min-height:0}
.nl-row{
  display:flex;
  flex-direction:row;
  gap:10px;
  align-items:stretch;
  flex-wrap:wrap;
  width:100%;
  max-width:100%;
  min-height:0;
  --nl-cols: 1;
}


.nl-row > .nl-cell{
  flex: 0 1 auto;
  max-width: 100%;
  min-width: 0;
  min-height: 0;
}


.nl-row > .nl-cell.nl-cell-field{
  flex: 0 0 calc(100% / var(--nl-cols));
  max-width: calc(100% / var(--nl-cols));
}


.nl-cell-full{flex:1 1 100%; width:100%; min-width:0; max-width:100%}



.nl-cell-full{flex:1 1 100%; width:100%; min-width:0}

.nl-text,.nl-html,.nl-stub{line-height:1.25}
.nl-title-value{display:flex;gap:8px;align-items:baseline}
.nl-tv-title{opacity:.8}
.nl-tv-value{font-weight:700}
.nl-picture{max-width:100%;height:auto}
.nl-unknown{white-space:pre-wrap;background:#f6f6f6;padding:8px;border-radius:8px}

.nl-container{display:flex;gap:10px;flex-wrap:wrap;max-width:100%}
.nl-row{max-width:100%}
.nl-column{flex-direction:column;align-items:stretch}
.nl-row.nl-container{flex-direction:row}

.nl-scroll{max-width:100%}
.nl-scroll-x{overflow-x:auto;overflow-y:hidden}
.nl-scroll-y{overflow-y:auto;overflow-x:hidden;max-height:60vh}

.nl-card{border:1px solid rgba(0,0,0,.125);border-radius:12px;padding:10px;box-shadow:0 0.25rem 0.75rem rgba(0,0,0,.06);background:#fff;max-width:100%}

/* Input fields with labels */
.nl-field{display:flex;gap:10px;max-width:100%;min-width:0; width:100%}
.nl-field-row{flex-direction:row;align-items:center}
.nl-field-col{flex-direction:column;align-items:stretch;min-height:0}
.nl-label{flex:0 0 33.333%;max-width:33.333%;opacity:.85}

.nl-field-col .nl-label{flex:0 0 auto;max-width:100%}

.nl-input{max-width:100%;min-width:140px;flex:1 1 auto}
.nl-input-fill{flex:1 1 auto;min-height:0}

.nl-field-fill{height:100%;min-height:0;flex:1 1 auto}

.nl-button{
  display:inline-flex;
  align-items:center;
  gap:.35rem;
  width:auto;
  font-size: inherit;
  line-height: 1;
  padding: .18rem .45rem;
  border-radius:.55rem;
  border:1px solid rgba(0,0,0,.25);
  background:#fff;
  cursor:pointer;
  user-select:none;
}
.nl-button:hover{background:rgba(0,0,0,.04)}
.nl-check{cursor:pointer;user-select:none}

/* Switch */
.nl-switch{display:inline-flex;align-items:center;gap:10px;cursor:pointer;user-select:none}
.nl-switch-caption{opacity:.9}
.nl-switch-control{position:relative;display:inline-flex;align-items:center}
.nl-switch-control input{opacity:0;width:0;height:0}
.nl-switch-slider{position:relative;display:inline-block;width:42px;height:24px;border-radius:999px;border:1px solid rgba(0,0,0,.25);background:#fff;transition:all .15s ease}
.nl-switch-slider:before{content:"";position:absolute;left:2px;top:2px;width:18px;height:18px;border-radius:999px;background:rgba(0,0,0,.35);transition:all .15s ease}
.nl-switch-control input:checked + .nl-switch-slider{background:rgba(0,0,0,.1)}
.nl-switch-control input:checked + .nl-switch-slider:before{transform:translateX(18px);background:rgba(0,0,0,.55)}

/* Tabs */
.nl-tabs .nav-tabs{gap:6px}
.nl-tabs-content{padding-top:10px}

.nl-clickable{cursor:pointer}
.nl-clickable:hover{box-shadow:0 0.5rem 1rem rgba(0,0,0,.12)}
/* table */
.nl-table-wrap{max-width:100%;overflow-x:auto}
.nl-table-el{width:100%;border-collapse:collapse;table-layout:fixed}
/*
  IMPORTANT (flex layouts): a <table> containing long unbreakable strings (like UUIDs)
  can increase the "min-content" width of the surrounding flex item, causing the
  whole left pane to expand under the right-side panel.

  To keep the layout stable, we allow breaking inside cells (including inside UUIDs),
  while keeping a fixed table layout.
*/
.nl-table-el th,.nl-table-el td{
  border:1px solid rgba(0,0,0,.12);
  padding:.35rem .5rem;
  vertical-align:middle;
  overflow:hidden;
  overflow-wrap:anywhere;
  word-break:break-word;
}

.nl-table-el{
  width: 100%;
  max-width: 100%;
  border-collapse: collapse;   
  border-spacing: 0;           
  table-layout: fixed;         
  box-sizing: border-box;
}
.nl-table-wrap{
  width: 100%;
  max-width: 100%;
  overflow-x: auto;
  box-sizing: border-box;
}
.nl-table-el th,
.nl-table-el td{
  box-sizing: border-box;
}


.tab-pane { min-width: 0; }



.nl-cell, .nl-cell-full { min-width: 0; }


.nl-tr:hover{background:rgba(0,0,0,.03)}

.nl-grid{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(220px,1fr));
  gap:12px;
  max-width:100%;
  align-items:start;   
}
.nl-grid-card{
  border:1px solid rgba(0,0,0,.125);
  border-radius:12px;
  padding:10px;
  background:#fff;
  height:fit-content;   
  align-self:start;
}
.nl-grid-card:hover{background:rgba(0,0,0,.02)}
.nl-kv{display:flex;justify-content:space-between;gap:10px}
.nl-k{font-weight:600;opacity:.75}
.nl-v{overflow:hidden;text-overflow:ellipsis}

/* nodes list (nodes_source) */
.nl-table.nl-nodes{display:flex;flex-direction:column;gap:8px}
.nl-node-row{border:1px solid rgba(0,0,0,.125);border-radius:12px;padding:8px;background:#fff}
.nl-node-row:hover{background:rgba(0,0,0,.02)}

/* NodeChildren */
.nl-child-line{position:absolute;top:18px;width:14px;height:2px;background:rgba(0,0,0,.25)}
.nl-node-children{display:flex;flex-direction:column;gap:10px}
.nl-child-item{position:relative}
.nl-child-card{border:1px solid rgba(0,0,0,.125);border-radius:12px;padding:8px;background:#fff}
.nl-child-card:hover{background:rgba(0,0,0,.02)}
.nl-container.nl-row { align-items: center; }
"""