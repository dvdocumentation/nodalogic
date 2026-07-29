from __future__ import annotations

from typing import Any, Dict, Optional
import json
import re


MOBILE_CANDIDATE_RUNTIME_INSTRUCTION = """
Mobile/Android candidate-resolution rules:
- candidate_handler_code is executed on the NodaLogic server against server repositories and server indexes. Android must not execute candidate search locally.
- This is intentionally the same candidate-search path as web nGenie, so semantic_index/server_only/ngenie_remote indexes are available and produce the same candidates.
- candidate_handler_code is ONLY for resolving existing candidate nodes. It must not create or update nodes and must never enumerate a class with GetAllNodes/Class.get_all.
- First inspect the target class indexes included in the NodaLogic context and apply that class ngenie_prompt.
- If a suitable declared index exists for the requested value, use findByIndex(class_name, index_name, value) or getByIndex(class_name, index_name, value). Never invent an index name.
- If the target class has no suitable declared index for this lookup, use the native fallback find(class_name, query). This is the only allowed non-index fallback.
- Do not switch from an existing strict article/code/barcode index to find merely because the indexed lookup returned no rows. In that case follow ngenie_prompt, usually returning not_found. A post-index find fallback is allowed only when the class ngenie_prompt explicitly requires it.
- GetAllNodes, Class.get_all(), GetNode, NewNode, find_by_index, manual loops over a whole catalog and direct index metadata access are forbidden in candidate_handler_code.
- A small try/except around lookup or payload preparation is allowed, but it must not hide a not_found result or replace index search with a full scan.
- Node id is node._data.get("_id"). For a single node: uid = node._data.get("_id").
- findByIndex(...) and find(...) return real Node objects or an empty list. getByIndex(...) returns one Node or None. Never read Class._indexes/Class.indexes/indexes_json from generated code.
- To pass candidate nodes to the next LLM step, serialize real nodes with ngenie_node_payload(node) or ngenie_node_payloads(nodes, limit=candidate_limit()). These helpers only prepare compact UI/LLM JSON; search itself must stay native nodes.py.
- Do NOT immediately ask the user to choose from a raw index result when the ngenie_prompt/class rules may allow ranking. For fuzzy/text/trigram/LIKE searches return status='review' with candidates, so the second LLM step can apply ngenie_prompt and choose/filter. Use status='ambiguous' only when the user really must decide (same score/same semantic match, or choice depends on user preference).
- If no candidate search is needed and the user asks to create/update data on Android, return one operation_handler_code with apply_operations(ctx), not a list of JSON operations.
- If the task concerns ALL existing nodes of a class and the provided samples are incomplete, return data_requests (class_name, fields, filters) instead of asking the user to confirm get_all/findAll. The server will read the full accessible set and call a second step. Never invent findAll and never ask permission for this internal read.
- Never put {'query': '...'} into operation_handler_code or into a Node field on Android. Textual Node references must first be resolved by candidate_handler_code on the server; the final mobile handler writes only UID strings.
- A numeric quantity means the quantity in ONE logical row, not the number of candidate searches or table rows. Example: "Добавь 2 болгарки в заказ" means ONE Goods lookup and then ONE row with qty=2. Create several candidate items only when the user explicitly names several different goods/lines.
- Candidate item ids are per logical product phrase, never per physical unit. Never create sku_line_1 and sku_line_2 merely because qty=2.
- candidate_handler_code is a tiny SEARCH resolver only. Python import/from statements are forbidden. Do not generate random values, dates, loops that create documents, or data mutations there. If the task needs arbitrary existing clients/goods or a full catalog subset, return data_requests; creation/random distribution belongs to the second finalization step.

Example candidate_handler_code for Android:
"candidate_handler_code": "def resolve_candidates(ctx):\n    rows = findByIndex('Goods', 'barcode', '123') or []\n    items = []\n    if len(rows) == 1:\n        sku = rows[0]\n        items.append({'id':'sku_line_1','status':'resolved','uid':sku._data.get('_id'),'class_name':'Goods','context':{'target_field':'sku','qty':2}})\n    elif rows:\n        items.append({'id':'sku_line_1','status':'review','question':'Подобрать товар','class_name':'Goods','candidates':ngenie_node_payloads(rows, candidate_limit()),'context':{'target_field':'sku','qty':2}})\n    else:\n        items.append({'id':'sku_line_1','status':'not_found','message':'Товар не найден'})\n    return {'items': items}"
""".strip()


MOBILE_FINALIZE_RUNTIME_INSTRUCTION = """
Mobile/Android finalization rules:
- For mobile ALWAYS prefer one operation_handler_code over JSON operations for every data-changing action. For mass creation/update (2 goods, 100x100x10 warehouse cells, many rows) generate ONE apply_operations(ctx) handler with loops/lists, not many JSON operations.
- When a prior server step supplied data_results, use their exact _id values and update existing nodes with GetNode(uid), node._data[...] and node._save(). Do not call get_all/findAll and do not ask for another confirmation.
- Node fields must contain UID strings only. Never save a dict like {'query': '...'} on Android.
- operation_handler_code will be executed on the device inside the same generated handlers module <config_uid>.py.
- It must define def apply_operations(ctx): ...
- Use native mobile Node API exactly like normal handlers: generated classes, ContainerLine.create(data={...}), getByIndex/findByIndex, GetNode, node._data, node._save(), etc.
- Do not call UI/layout/navigation methods from operation_handler_code: node.Open(), node._open(), node.Show(), node.Dialog(), node.UpdateView(), PlugIn(), AddView(), RefreshTab(), UpdateView(). Android will refresh the host screen centrally after successful operations by running onShow/run_on_show.
- Current node is available as ctx.get('current_node') and ctx.get('node'). Its id/data are also in ctx['node_context'].
- Do not create a parallel runtime or use backend storage.
- Return a small JSON-serializable dict, e.g. {'ok': True, 'notes': ['Строка добавлена'], 'node_data': node._data}.
- Do not return operations:[{'tool':'create_node', ...}] for mobile when operation_handler_code is possible; create nodes inside apply_operations using GeneratedClass.create(data={...}).
- If there is nothing to change, return {'ok': True, 'notes': []}.
- Preserve quantities from candidate item context. A request such as "Добавь 2 болгарки" must produce one appended row with qty=2, never two identical rows with qty=1.
- Partial edits of an existing inline table must MERGE into every existing row. For example, "сделай цену в строках 100" changes only row['price']; it must not replace doc._data['lines'] with partial {'price': 100} dictionaries and must preserve product, product_view, qty, row ids and parent metadata. Recalculate sum when price or qty changes and both fields exist.

Example operation_handler_code:
"operation_handler_code": "def apply_operations(ctx):\n    doc = ctx.get('current_node')\n    if doc is None:\n        return {'ok': False, 'notes':['Не найден текущий узел']}\n    line = ContainerLine.create(data={'parent_doc': doc._data.get('_id'), 'sku': 'Goods$123', 'qty': 2})\n    line._save()\n    return {'ok': True, 'notes':['Строка добавлена'], 'node_data': doc._data}"
""".strip()


def _handler_code_from_answer(answer: Dict[str, Any]) -> str:
    return str(
        answer.get("candidate_handler_code")
        or answer.get("candidateHandlerCode")
        or answer.get("resolve_handler_code")
        or answer.get("resolveHandlerCode")
        or ""
    )


def _strip_python_code(text: Any) -> str:
    text = str(text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:python|py)?\s*", "", text, flags=re.I).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    return text


def _operation_handler_code_from_answer(answer: Dict[str, Any]) -> str:
    return _strip_python_code(
        answer.get("operation_handler_code")
        or answer.get("operationHandlerCode")
        or answer.get("apply_handler_code")
        or answer.get("applyHandlerCode")
        or ""
    )


def _mobile_operation_handler_has_unresolved_query(answer: Dict[str, Any]) -> bool:
    code = _operation_handler_code_from_answer(answer if isinstance(answer, dict) else {})
    if not code:
        return False
    return bool(re.search(r"[\"']query[\"']\s*:", code, flags=re.I))


def _sanitize_mobile_operation_handler_code(src: str) -> str:
    """Keep generated data mutations separate from Android UI refresh.

    The BottomSheet executes operation_handler_code on a background thread and
    then refreshes the host screen centrally via NodeTaskActivity.run_on_show()
    or MainActivity.updateRecyclerView(). Generated code should not call
    node.Open()/Show()/UpdateView() itself: those are UI actions and can run on
    the wrong thread or duplicate rendering. We remove such one-line calls as a
    last safety net; the prompt also tells the LLM not to generate them.
    """
    src = str(src or "")
    if not src.strip():
        return ""
    ui_call = re.compile(
        r"^\s*(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\.)?"
        r"(?:Open|_open|Show|Dialog|UpdateView|PlugIn|AddView|RefreshTab)\s*\(.*\)\s*;?\s*(?:#.*)?$"
    )
    out = []
    for line in src.splitlines():
        if ui_call.match(line):
            indent = line[:len(line) - len(line.lstrip())]
            out.append(indent + "# nGenie mobile: UI refresh is performed by Android host after operations")
            continue
        out.append(line)
    return "\n".join(out).strip()


def _py_literal(value: Any) -> str:
    """repr() wrapper for code snippets executed in the mobile handlers module."""
    return repr(value)


def _operation_handler_from_json_operations(operations: Any, message: Any = "") -> str:
    """Compatibility bridge for old LLM answers which still return JSON operations.

    Mobile nGenie should normally receive one operation_handler_code. If the LLM
    slips back to backend-style operations, synthesize a single handler instead
    of making Android print one "create_node requires operation_handler_code" per
    operation. This is intentionally generic and conservative; complex document
    row logic is still better generated directly by the LLM as Python code.
    """
    if not isinstance(operations, list) or not operations:
        return ""
    supported = {"create_node", "update_current_node", "patch_current_node", "bulk_update_nodes", "update_nodes", "bulkUpdateNodes", "append_table_rows", "none", ""}
    for op in operations:
        if not isinstance(op, dict):
            return ""
        tool = str(op.get("tool") or op.get("action") or "").strip()
        if tool not in supported:
            return ""
    ops_literal = _py_literal(operations)
    requested_qty = _message_requested_unit_count(message) or 1
    lines = [
        "def apply_operations(ctx):",
        "    ops = " + ops_literal,
        "    notes = []",
        "    created = []",
        "    current_node = ctx.get('current_node') or ctx.get('node')",
        "    for op in ops:",
        "        if not isinstance(op, dict):",
        "            continue",
        "        tool = str(op.get('tool') or op.get('action') or '')",
        "        if tool in ('', 'none'):",
        "            continue",
        "        if tool in ('update_current_node', 'patch_current_node'):",
        "            if current_node is None:",
        "                notes.append('Не найден текущий узел')",
        "                continue",
        "            data = op.get('data') or op.get('patch') or {}",
        "            for k, v in data.items():",
        "                if k not in ('_id', '_class'):",
        "                    current_node._data[k] = v",
        "            current_node._save()",
        "            notes.append('Узел изменён')",
        "            continue",
        "        if tool in ('bulk_update_nodes', 'update_nodes', 'bulkUpdateNodes'):",
        "            updates = op.get('updates') if isinstance(op.get('updates'), list) else op.get('items')",
        "            updates = updates if isinstance(updates, list) else []",
        "            getter = globals().get('GetNode')",
        "            changed_count = 0",
        "            for item in updates:",
        "                if not isinstance(item, dict):",
        "                    continue",
        "                uid = str(item.get('uid') or item.get('node_uid') or item.get('_id') or item.get('id') or '')",
        "                patch = item.get('data') if isinstance(item.get('data'), dict) else item.get('patch')",
        "                if not uid or not isinstance(patch, dict) or not callable(getter):",
        "                    continue",
        "                node = getter(uid)",
        "                if node is None:",
        "                    short_uid = '$'.join(uid.split('$')[-2:]) if uid.count('$') >= 2 else uid",
        "                    node = getter(short_uid) if short_uid != uid else None",
        "                if node is None:",
        "                    continue",
        "                for k, v in patch.items():",
        "                    if k not in ('_id', '_class', '_created_date', '_created_user'):",
        "                        node._data[k] = v",
        "                node._save()",
        "                changed_count += 1",
        "            notes.append('Изменено узлов: ' + str(changed_count))",
        "            continue",
        "        if tool == 'append_table_rows':",
        "            if current_node is None:",
        "                notes.append('Не найден текущий узел')",
        "                continue",
        "            field = str(op.get('field') or op.get('name') or '')",
        "            rows = op.get('rows') if isinstance(op.get('rows'), list) else []",
        "            requested_qty = " + str(requested_qty),
        "            if requested_qty > 1 and len(rows) == requested_qty:",
        "                qty_keys = ('qty', 'quantity', 'count')",
        "                normalized = []",
        "                for one in rows:",
        "                    if not isinstance(one, dict):",
        "                        normalized.append(one)",
        "                        continue",
        "                    normalized.append({k: v for k, v in one.items() if str(k).lower() not in qty_keys and not str(k).startswith('_') and not str(k).endswith('_view')})",
        "                if normalized and all(x == normalized[0] for x in normalized):",
        "                    first = dict(rows[0]) if isinstance(rows[0], dict) else {'name': str(rows[0])}",
        "                    qk = next((k for k in first.keys() if str(k).lower() in qty_keys), 'qty')",
        "                    first[qk] = requested_qty",
        "                    rows = [first]",
        "            elif requested_qty > 1 and len(rows) == 1 and isinstance(rows[0], dict):",
        "                first = dict(rows[0])",
        "                qty_keys = ('qty', 'quantity', 'count')",
        "                qk = next((k for k in first.keys() if str(k).lower() in qty_keys), 'qty')",
        "                try:",
        "                    current_qty = float(first.get(qk) or 0)",
        "                except Exception:",
        "                    current_qty = 0",
        "                if current_qty <= 1:",
        "                    first[qk] = requested_qty",
        "                rows = [first]",
        "            if not field:",
        "                notes.append('Не указано поле табличной части')",
        "                continue",
        "            existing = current_node._data.get(field)",
        "            existing = list(existing) if isinstance(existing, list) else []",
        "            row_class_name = str(op.get('row_class') or op.get('rowClass') or '')",
        "            parent_field = str(op.get('parent_field') or op.get('parentField') or '')",
        "            child_rows = bool(op.get('child_rows') or op.get('childRows'))",
        "            if row_class_name:",
        "                row_cls = globals().get(row_class_name)",
        "                if row_cls is None:",
        "                    notes.append('Класс строки ' + row_class_name + ' не найден')",
        "                    continue",
        "                parent_uid = current_node._data.get('_id')",
        "                for raw in rows:",
        "                    row_data = dict(raw) if isinstance(raw, dict) else {'name': str(raw)}",
        "                    if parent_field and parent_uid:",
        "                        row_data.setdefault(parent_field, parent_uid)",
        "                    if child_rows and parent_uid:",
        "                        row_data.setdefault('_parent', parent_uid)",
        "                    row_node = row_cls.create(data=row_data)",
        "                    try:",
        "                        row_node._save()",
        "                    except Exception:",
        "                        pass",
        "                    row_uid = (getattr(row_node, '_data', {}) or {}).get('_id')",
        "                    if row_uid:",
        "                        existing.append(row_uid)",
        "            else:",
        "                parent_uid = str((getattr(current_node, '_data', {}) or {}).get('_id') or '')",
        "                for raw in rows:",
        "                    row_data = dict(raw) if isinstance(raw, dict) else {'name': str(raw)}",
        "                    row_data.setdefault('_parent_node', parent_uid)",
        "                    row_data.setdefault('_parent_table', field)",
        "                    row_data.setdefault('_id', str(__import__('uuid').uuid4()))",
        "                    existing.append(row_data)",
        "            current_node._data[field] = existing",
        "            current_node._save()",
        "            notes.append('Добавлены строки в ' + field + ': ' + str(len(rows)))",
        "            continue",
        "        if tool == 'create_node':",
        "            class_name = str(op.get('class_name') or op.get('class') or '')",
        "            data = op.get('data') or {}",
        "            cls = globals().get(class_name)",
        "            if cls is None:",
        "                notes.append('Класс ' + class_name + ' не найден')",
        "                continue",
        "            node = cls.create(data=dict(data))",
        "            try:",
        "                node._save()",
        "            except Exception:",
        "                pass",
        "            created.append(dict(getattr(node, '_data', {}) or {}))",
        "            notes.append('Создан ' + class_name)",
        "    return {'ok': True, 'notes': notes, 'created_objects': created, 'node_data': dict(getattr(current_node, '_data', {}) or {}) if current_node is not None else None}",
    ]
    return "\n".join(lines).strip()


def _message_requested_unit_count(message: Any) -> int:
    """Extract a small leading item quantity used only for duplicate-plan repair."""
    text = str(message or "").strip().lower()
    word_counts = {
        "два": 2,
        "две": 2,
        "три": 3,
        "четыре": 4,
        "пять": 5,
    }
    for word, value in word_counts.items():
        if re.search(r"\b" + re.escape(word) + r"\b", text):
            return value
    match = re.search(r"\b(\d{1,2})\s+[a-zа-яё]", text, flags=re.I)
    if not match:
        return 0
    try:
        value = int(match.group(1))
    except Exception:
        return 0
    return value if 2 <= value <= 20 else 0


def _candidate_signature(item: Dict[str, Any]) -> tuple:
    resolved_uid = str(item.get("uid") or item.get("selected_uid") or "").strip()
    if resolved_uid:
        return ("resolved", resolved_uid)
    ids = []
    for candidate in item.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        uid = candidate.get("uid") or candidate.get("id") or candidate.get("node_id") or candidate.get("_id")
        if uid:
            ids.append(str(uid))
    return ("candidates", tuple(sorted(set(ids))))


def _collapse_duplicate_unit_candidate_items(handler_result: Any, message: Any) -> Dict[str, Any]:
    """Repair the common mobile plan error "qty N" -> N identical lookups.

    The repair is deliberately conservative: it only collapses exactly N items
    with the same class, candidate set/resolved UID and operation context, where
    every generated item has no quantity or quantity 1.
    """
    if not isinstance(handler_result, dict):
        return handler_result if isinstance(handler_result, dict) else {}
    requested = _message_requested_unit_count(message)
    items = handler_result.get("items") if isinstance(handler_result.get("items"), list) else []
    if not requested or len(items) < requested:
        return handler_result

    groups: Dict[tuple, list] = {}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        context = item.get("context") if isinstance(item.get("context"), dict) else {}
        qty = context.get("qty", context.get("quantity"))
        try:
            qty_num = float(qty) if qty not in (None, "") else 1.0
        except Exception:
            qty_num = 1.0
        if qty_num != 1.0:
            continue
        context_key = tuple(sorted(
            (str(k), json.dumps(v, ensure_ascii=False, sort_keys=True, default=str))
            for k, v in context.items()
            if str(k) not in {"row_index", "line_no", "line_index", "qty", "quantity"}
        ))
        key = (
            str(item.get("class_name") or item.get("class") or ""),
            str(item.get("status") or "").lower(),
            _candidate_signature(item),
            context_key,
        )
        groups.setdefault(key, []).append((index, item))

    remove_indexes = set()
    replacements: Dict[int, Dict[str, Any]] = {}
    for group in groups.values():
        if len(group) != requested:
            continue
        first_index, first_item = group[0]
        merged = dict(first_item)
        merged_context = dict(merged.get("context") or {})
        merged_context["qty"] = requested
        merged_context.pop("quantity", None)
        merged_context["row_index"] = 0
        merged["context"] = merged_context
        question = str(merged.get("question") or "")
        question = re.sub(r"\s+(?:для\s+)?строк[иуы]?\s*\d+\b", "", question, flags=re.I).strip()
        if question:
            merged["question"] = question
        replacements[first_index] = merged
        remove_indexes.update(index for index, _item in group[1:])

    if not remove_indexes:
        return handler_result
    out = dict(handler_result)
    out["items"] = [
        replacements.get(index, item)
        for index, item in enumerate(items)
        if index not in remove_indexes
    ]
    return out


def _mobile_context_class_name(node_context: Any) -> str:
    if not isinstance(node_context, dict):
        return ""
    for key in ("class_name", "class", "_class"):
        raw = str(node_context.get(key) or "").strip()
        if raw:
            parts = raw.split("$")
            return parts[-1] if len(parts) >= 2 else raw
    data = node_context.get("data") if isinstance(node_context.get("data"), dict) else {}
    raw = str(data.get("_class") or data.get("_id") or node_context.get("node_id") or "").strip()
    parts = raw.split("$")
    if len(parts) >= 3:
        return parts[-2]
    if len(parts) == 2:
        return parts[-1]
    return ""


def _mobile_table_row_fields(classes: Any, config_uid: str, table_def: Any) -> list:
    """Return row fields for inline tables and Node-backed virtual tables."""
    if not isinstance(table_def, dict):
        return []
    direct = [f for f in (table_def.get("fields") or []) if isinstance(f, dict)]
    if direct:
        return direct
    row_class = str(
        table_def.get("row_class")
        or table_def.get("target")
        or ((table_def.get("item_type") or {}).get("target") if isinstance(table_def.get("item_type"), dict) else "")
        or ""
    ).strip()
    if not row_class:
        return []
    row_class = row_class.split("$")[-1]
    for item in classes or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("config_uid") or "") != str(config_uid or ""):
            continue
        if str(item.get("class_name") or "").split("$")[-1] != row_class:
            continue
        return [f for f in (item.get("fields") or []) if isinstance(f, dict)]
    return []


def _mobile_selected_candidate(item: Dict[str, Any], uid: str) -> Dict[str, Any]:
    for key in ("selected_candidate", "selectedCandidate", "candidate"):
        value = item.get(key)
        if isinstance(value, dict):
            return value
    for candidate in item.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        candidate_uid = str(candidate.get("uid") or candidate.get("id") or candidate.get("node_id") or candidate.get("_id") or "").strip()
        if candidate_uid and candidate_uid == uid:
            return candidate
    return {}


def _mobile_candidate_title(candidate: Dict[str, Any]) -> str:
    if not isinstance(candidate, dict):
        return ""
    view = candidate.get("view") if isinstance(candidate.get("view"), dict) else {}
    data = candidate.get("data") if isinstance(candidate.get("data"), dict) else {}
    return str(
        candidate.get("title")
        or candidate.get("caption")
        or view.get("title")
        or view.get("caption")
        or data.get("name")
        or data.get("title")
        or data.get("caption")
        or ""
    ).strip()


def _mobile_full_node_uid(uid: Any, config_uid: Any = "", target_class: Any = "") -> str:
    """Return the canonical NodaLogic UID used by both web and Android.

    A selected candidate normally already contains
    ``<config_uid>$<class_name>$<id>``.  Preserve it verbatim.  Older mobile
    handlers may still emit ``Class$id`` or just ``id``; only those legacy
    values are expanded using the current configuration and declared Node
    target.  Never truncate a full UID: doing so loses tenant/configuration
    identity and makes the row differ from the web branch.
    """
    raw = str(uid or "").strip()
    if not raw:
        return ""
    parts = raw.split("$")
    if len(parts) >= 3:
        return raw
    cu = str(config_uid or "").strip()
    target = str(target_class or "").strip().split("$")[-1]
    if len(parts) == 2:
        return f"{cu}${raw}" if cu else raw
    if cu and target:
        return f"{cu}${target}${raw}"
    return raw


def _mobile_short_node_uid(uid: Any) -> str:
    """Legacy lookup alias only; storage must use _mobile_full_node_uid()."""
    raw = str(uid or "").strip()
    parts = raw.split("$")
    if len(parts) >= 3:
        return "$".join(parts[-2:])
    return raw


def _mobile_selection_entries(source: Any) -> list:
    if not isinstance(source, dict):
        return []
    if isinstance(source.get("requests"), list):
        return [x for x in source.get("requests") or [] if isinstance(x, dict)]
    if isinstance(source.get("items"), list):
        return [x for x in source.get("items") or [] if isinstance(x, dict)]
    return []


def _mobile_virtual_row_handler_from_selection(
    rt: Any,
    config_uid: str,
    node_context: Optional[Dict[str, Any]],
    selection_source: Any,
    message: Any,
) -> str:
    """Build a deterministic Android handler for inline/virtual table rows.

    The LLM is still responsible for candidate lookup and semantic choice, but
    once a UID is selected this bridge writes the row in the same shape as the
    Android virtual-node editor: canonical full Node UID, row id, parent metadata, view,
    quantity and simple price/sum defaults.  This prevents a follow-up LLM pass
    from losing qty=2 or appending a raw incomplete dictionary.
    """
    if not isinstance(node_context, dict):
        return ""
    entries = _mobile_selection_entries(selection_source)
    if not entries:
        return ""
    if len(entries) > 1:
        collapsed = _collapse_duplicate_unit_candidate_items({"items": entries}, message)
        entries = [x for x in (collapsed.get("items") or []) if isinstance(x, dict)]

    class_name = _mobile_context_class_name(node_context)
    if not class_name:
        return ""
    try:
        classes, lookup = rt._ngenie_collect_context(config_uid, include_samples=False)
    except Exception:
        return ""
    class_ctx = next((x for x in classes if isinstance(x, dict) and str(x.get("config_uid") or "") == str(config_uid or "") and str(x.get("class_name") or "") == class_name), None)
    if not isinstance(class_ctx, dict):
        return ""
    tables = {str(t.get("name") or ""): t for t in (class_ctx.get("tables") or []) if isinstance(t, dict)}

    requested_qty = _message_requested_unit_count(message) or 1
    rows_by_field: Dict[str, list] = {}
    for item in entries:
        context = item.get("context") if isinstance(item.get("context"), dict) else {}
        tool = str(context.get("tool") or item.get("tool") or "").strip()
        field = str(context.get("field") or context.get("table_field") or context.get("table") or "").strip()
        if tool and tool != "append_table_rows":
            continue
        if not field or field not in tables:
            continue
        table_def = tables[field]
        row_fields = _mobile_table_row_fields(classes, config_uid, table_def)
        if not row_fields:
            continue

        uid = str(
            item.get("selected_uid")
            or item.get("uid")
            or ((item.get("selected_candidate") or {}).get("uid") if isinstance(item.get("selected_candidate"), dict) else "")
            or ""
        ).strip()
        if not uid:
            continue
        candidate = _mobile_selected_candidate(item, uid)
        node_fields = [f for f in row_fields if str(f.get("kind") or "").lower() == "node"]
        target_field = str(context.get("target_field") or context.get("targetField") or "").strip()
        if not target_field and len(node_fields) == 1:
            target_field = str(node_fields[0].get("name") or "").strip()
        if not target_field:
            continue

        row = {}
        for key in ("row", "row_data", "rowData", "data"):
            value = context.get(key)
            if isinstance(value, dict):
                row.update(value)
                break
        target_def = next((f for f in node_fields if str(f.get("name") or "") == target_field), {})
        target_class = str(target_def.get("target") or item.get("class_name") or item.get("class") or "").strip()
        row[target_field] = _mobile_full_node_uid(uid, config_uid, target_class)

        qty_field = str(context.get("qty_field") or context.get("quantity_field") or context.get("qtyField") or "").strip()
        if not qty_field:
            for fld in row_fields:
                name = str(fld.get("name") or "").strip()
                low = name.lower()
                if low in {"qty", "quantity", "count", "amount_qty"} or "колич" in low:
                    qty_field = name
                    break
        if not qty_field:
            qty_field = "qty"
        qty = context.get("qty", context.get("quantity", requested_qty))
        try:
            qty = int(qty) if float(qty).is_integer() else float(qty)
        except Exception:
            qty = requested_qty
        if not qty or float(qty) <= 0:
            qty = requested_qty
        if len(entries) == 1 and requested_qty > 1:
            try:
                if float(qty) <= 1:
                    qty = requested_qty
            except Exception:
                qty = requested_qty
        row[qty_field] = qty

        title = _mobile_candidate_title(candidate)
        if not title:
            try:
                target_repo, _target_cfg = lookup.get((config_uid, target_class), (None, None))
                if target_repo is not None:
                    title = str(rt._ngenie_node_ref_view(target_repo, uid, default="") or "").strip()
            except Exception:
                title = ""
        if title:
            row.setdefault(target_field + "_view", title)

        candidate_data = candidate.get("data") if isinstance(candidate.get("data"), dict) else {}
        if not candidate_data:
            try:
                target_repo, _target_cfg = lookup.get((config_uid, target_class), (None, None))
                if target_repo is not None:
                    internal_id = str(uid).split("$")[-1]
                    loaded = rt._fetch_node_data_for_repo(target_repo, target_class, internal_id)
                    if isinstance(loaded, dict):
                        candidate_data = loaded
            except Exception:
                candidate_data = {}

        field_names = {str(f.get("name") or "") for f in row_fields}
        if "price" in field_names and row.get("price") in (None, ""):
            price = candidate_data.get("price")
            if price not in (None, ""):
                row["price"] = price
        if "sum" in field_names and row.get("sum") in (None, "") and row.get("price") not in (None, ""):
            try:
                row["sum"] = float(row.get("price")) * float(qty)
                if float(row["sum"]).is_integer():
                    row["sum"] = int(row["sum"])
            except Exception:
                pass
        rows_by_field.setdefault(field, []).append(row)

    if not rows_by_field:
        return ""

    plan_literal = _py_literal(rows_by_field)
    return "\n".join([
        "def apply_operations(ctx):",
        "    doc = ctx.get('current_node') or ctx.get('node')",
        "    if doc is None:",
        "        return {'ok': False, 'notes': ['Не найден текущий узел']}",
        "    plan = " + plan_literal,
        "    parent_uid = str((getattr(doc, '_data', {}) or {}).get('_id') or '')",
        "    added = 0",
        "    for field, new_rows in plan.items():",
        "        existing = doc._data.get(field)",
        "        existing = list(existing) if isinstance(existing, list) else []",
        "        for raw in new_rows:",
        "            row = dict(raw) if isinstance(raw, dict) else {'name': str(raw)}",
        "            row.setdefault('_parent_node', parent_uid)",
        "            row.setdefault('_parent_table', field)",
        "            row.setdefault('_id', str(__import__('uuid').uuid4()))",
        "            existing.append(row)",
        "            added += 1",
        "        doc._data[field] = existing",
        "    doc._save()",
        "    return {'ok': True, 'notes': ['Добавлено строк: ' + str(added)], 'node_data': doc._data}",
    ])


def _mobile_virtual_row_handler_from_operations(
    rt: Any,
    config_uid: str,
    node_context: Optional[Dict[str, Any]],
    operations: Any,
    message: Any,
) -> str:
    """Normalize final Android append_table_rows operations deterministically.

    This is the last reliable point of the mobile flow: depending on the Android
    client version the selected candidate may return through /plan or /finalize,
    but both routes eventually contain a concrete append_table_rows operation.
    Build the virtual-row payload from that operation instead of trusting a
    second LLM pass to preserve quantity and Android row metadata.
    """
    if not isinstance(node_context, dict) or not isinstance(operations, list):
        return ""
    append_ops = [
        op for op in operations
        if isinstance(op, dict)
        and str(op.get("tool") or op.get("action") or "").strip() == "append_table_rows"
    ]
    if not append_ops:
        return ""

    class_name = _mobile_context_class_name(node_context)
    if not class_name:
        return ""
    try:
        classes, lookup = rt._ngenie_collect_context(config_uid, include_samples=False)
    except Exception:
        classes, lookup = [], {}
    class_ctx = next((
        x for x in classes
        if isinstance(x, dict)
        and str(x.get("config_uid") or "") == str(config_uid or "")
        and str(x.get("class_name") or "") == class_name
    ), None)
    tables = {
        str(t.get("name") or ""): t
        for t in ((class_ctx or {}).get("tables") or [])
        if isinstance(t, dict)
    }

    requested_qty = _message_requested_unit_count(message) or 1
    rows_by_field: Dict[str, list] = {}
    for op in append_ops:
        field = str(op.get("field") or op.get("name") or "").strip()
        raw_rows = op.get("rows") if isinstance(op.get("rows"), list) else []
        if not field or not raw_rows:
            continue
        table_def = tables.get(field) or {}
        row_fields = _mobile_table_row_fields(classes, config_uid, table_def)
        node_defs = {
            str(f.get("name") or ""): f
            for f in row_fields
            if str(f.get("kind") or "").strip().lower() == "node"
        }
        declared_names = {str(f.get("name") or "") for f in row_fields}

        rows = [dict(x) if isinstance(x, dict) else {"name": str(x)} for x in raw_rows]
        qty_names = {"qty", "quantity", "count", "amount_qty"}

        def qty_key_for(row: Dict[str, Any]) -> str:
            for f in row_fields:
                name = str(f.get("name") or "").strip()
                low = name.lower()
                if low in qty_names or "колич" in low:
                    return name
            for key in row.keys():
                low = str(key).lower()
                if low in qty_names or "колич" in low:
                    return str(key)
            return "qty"

        # Repair the two known LLM mistakes:
        # 1) N physical units were emitted as N duplicate rows with qty=1;
        # 2) one selected row lost the original requested quantity and became 1.
        if requested_qty > 1 and len(rows) == requested_qty:
            signatures = []
            for row in rows:
                qk = qty_key_for(row)
                signatures.append({
                    k: v for k, v in row.items()
                    if k != qk and not str(k).startswith("_") and not str(k).endswith("_view")
                })
            if signatures and all(sig == signatures[0] for sig in signatures):
                first = dict(rows[0])
                first[qty_key_for(first)] = requested_qty
                rows = [first]
        elif requested_qty > 1 and len(rows) == 1:
            qk = qty_key_for(rows[0])
            try:
                current_qty = float(rows[0].get(qk) or 0)
            except Exception:
                current_qty = 0
            if current_qty <= 1:
                rows[0][qk] = requested_qty

        normalized_rows = []
        for row in rows:
            qk = qty_key_for(row)
            try:
                qty = float(row.get(qk) or 0)
            except Exception:
                qty = 0
            if qty <= 0:
                qty = requested_qty
            row[qk] = int(qty) if float(qty).is_integer() else qty

            # Use declared Node fields when available. For older context payloads
            # infer them from a NodaLogic UID so the normalization still applies.
            node_field_names = set(node_defs.keys())
            if not node_field_names:
                node_field_names = {
                    str(k) for k, v in row.items()
                    if isinstance(v, str) and v.count("$") >= 1
                }
            for node_field in node_field_names:
                raw_uid = row.get(node_field)
                if isinstance(raw_uid, dict):
                    raw_uid = (
                        raw_uid.get("uid") or raw_uid.get("id")
                        or raw_uid.get("node_id") or raw_uid.get("_id") or ""
                    )
                original_uid = str(raw_uid or "").strip()
                if not original_uid:
                    continue
                target_class = str((node_defs.get(node_field) or {}).get("target") or "").strip()
                if not target_class:
                    parts = original_uid.split("$")
                    if len(parts) >= 2:
                        target_class = parts[-2]
                row[node_field] = _mobile_full_node_uid(original_uid, config_uid, target_class)
                target_repo = None
                if target_class:
                    target_repo, _target_cfg = lookup.get((config_uid, target_class), (None, None))
                candidate_data: Dict[str, Any] = {}
                title = ""
                if target_repo is not None:
                    try:
                        title = str(rt._ngenie_node_ref_view(target_repo, original_uid, default="") or "").strip()
                    except Exception:
                        title = ""
                    try:
                        loaded = rt._fetch_node_data_for_repo(target_repo, target_class, original_uid.split("$")[-1])
                        if isinstance(loaded, dict):
                            candidate_data = loaded
                    except Exception:
                        candidate_data = {}
                if not title:
                    title = str(candidate_data.get("name") or candidate_data.get("title") or "").strip()
                if title:
                    row.setdefault(node_field + "_view", title)

                if ("price" in declared_names or "price" in row) and row.get("price") in (None, ""):
                    price = candidate_data.get("price")
                    if price not in (None, ""):
                        row["price"] = price

            if ("sum" in declared_names or "sum" in row) and row.get("sum") in (None, "") and row.get("price") not in (None, ""):
                try:
                    total = float(row.get("price")) * float(row.get(qk) or 0)
                    row["sum"] = int(total) if total.is_integer() else total
                except Exception:
                    pass
            normalized_rows.append(row)
        rows_by_field.setdefault(field, []).extend(normalized_rows)

    if not rows_by_field:
        return ""
    plan_literal = _py_literal(rows_by_field)
    return "\n".join([
        "def apply_operations(ctx):",
        "    doc = ctx.get('current_node') or ctx.get('node')",
        "    if doc is None:",
        "        return {'ok': False, 'notes': ['Не найден текущий узел']}",
        "    plan = " + plan_literal,
        "    parent_uid = str((getattr(doc, '_data', {}) or {}).get('_id') or '')",
        "    added = 0",
        "    for field, new_rows in plan.items():",
        "        existing = doc._data.get(field)",
        "        existing = list(existing) if isinstance(existing, list) else []",
        "        for raw in new_rows:",
        "            row = dict(raw) if isinstance(raw, dict) else {'name': str(raw)}",
        "            row.setdefault('_parent_node', parent_uid)",
        "            row.setdefault('_parent_table', field)",
        "            row.setdefault('_id', str(__import__('uuid').uuid4()))",
        "            existing.append(row)",
        "            added += 1",
        "        doc._data[field] = existing",
        "    doc._save()",
        "    return {'ok': True, 'notes': ['Добавлено строк: ' + str(added)], 'node_data': doc._data}",
    ])



def _mobile_inline_row_scalar_patch_handler(
    rt: Any,
    config_uid: str,
    node_context: Optional[Dict[str, Any]],
    message: Any,
) -> str:
    """Build a deterministic merge-only handler for commands such as
    ``сделай цену в строках 100``.

    A generic LLM handler may accidentally replace the whole inline table with
    partial dictionaries containing only the changed field.  This helper is
    intentionally narrow: it activates only for an explicit all-rows/table-row
    scalar assignment, identifies a declared row field by id/label, and mutates
    that one key in copies of the existing rows.  All references, quantities,
    views and virtual-row metadata are preserved.
    """
    if not isinstance(node_context, dict):
        return ""
    text = str(message or "").strip()
    low = text.lower().replace("ё", "е")
    if not low or not re.search(r"\b(строк|строки|строках|позици|табличн)", low):
        return ""
    if not re.search(r"\b(сделай|установи|поставь|задай|проставь|заполни|измени|замени)\b", low):
        return ""
    # Adding/removing rows is handled by the existing deterministic append path.
    if re.search(r"\b(добавь|создай|удали|очисти)\b", low):
        return ""

    class_name = _mobile_context_class_name(node_context)
    if not class_name:
        return ""
    try:
        classes, _lookup = rt._ngenie_collect_context(config_uid, include_samples=False)
    except Exception:
        return ""
    class_ctx = next((
        x for x in classes
        if isinstance(x, dict)
        and str(x.get("config_uid") or "") == str(config_uid or "")
        and str(x.get("class_name") or "") == class_name
    ), None)
    if not isinstance(class_ctx, dict):
        return ""

    tables = [dict(t) for t in (class_ctx.get("tables") or []) if isinstance(t, dict)]
    # Legacy unnamed inline table: bind it only when the current node has one
    # obvious list field (normally lines/rows/items/positions).
    if not tables:
        virtual = [dict(t) for t in (class_ctx.get("virtual_tables") or []) if isinstance(t, dict)]
        data = node_context.get("data") if isinstance(node_context.get("data"), dict) else {}
        candidates = [str(k) for k, v in data.items() if isinstance(v, list) and not str(k).startswith("_")]
        common = [k for k in candidates if k.lower() in {"lines", "rows", "items", "positions"}]
        if len(virtual) == 1 and len(candidates) == 1:
            virtual[0]["name"] = candidates[0]
            tables = virtual
        elif len(virtual) == 1 and len(common) == 1:
            virtual[0]["name"] = common[0]
            tables = virtual
    if not tables:
        return ""

    def norm(value: Any) -> str:
        value = str(value or "").lower().replace("ё", "е")
        return " ".join(re.findall(r"[a-zа-я0-9_]+", value, flags=re.I))

    aliases = {
        "price": ("цена", "цену", "цены", "ценой", "стоимость"),
        "sum": ("сумма", "сумму", "суммы", "итого", "стоимость строки"),
        "qty": ("количество", "количества", "количеству", "кол во", "кол-во"),
        "quantity": ("количество", "количества", "количеству", "кол во", "кол-во"),
        "count": ("количество", "количества", "количеству", "кол во", "кол-во"),
    }
    candidates = []
    for table in tables:
        field = str(table.get("name") or "").strip()
        if not field:
            continue
        row_fields = _mobile_table_row_fields(classes, config_uid, table)
        for fld in row_fields:
            if not isinstance(fld, dict):
                continue
            name = str(fld.get("name") or "").strip()
            if not name:
                continue
            labels = {norm(name), norm(fld.get("label") or "")}
            labels.update(norm(x) for x in aliases.get(name.lower(), ()))
            labels = {x for x in labels if x}
            message_norm = norm(low)
            score = max((len(label) for label in labels if re.search(r"(?<![a-zа-я0-9_])" + re.escape(label) + r"(?![a-zа-я0-9_])", message_norm)), default=0)
            if score:
                candidates.append((score, field, name, str(fld.get("kind") or "").lower(), row_fields))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score = candidates[0][0]
    selected = [x for x in candidates if x[0] == best_score]

    # The common chat command supplies a scalar at the end. Numbers are kept as
    # numeric values; quoted text is accepted for string fields.
    number_matches = re.findall(r"(?<![\w])[-+]?\d+(?:[\.,]\d+)?(?![\w])", low)
    quoted = re.findall(r"[\"«](.*?)[\"»]", text)
    bool_value = None
    if re.search(r"\b(истина|да|true|включено|включить)\b", low):
        bool_value = True
    elif re.search(r"\b(ложь|нет|false|выключено|выключить)\b", low):
        bool_value = False

    plans = []
    for _score, table_field, target_field, kind, row_fields in selected:
        if kind == "boolean" and bool_value is not None:
            value = bool_value
        elif kind in {"number", "int", "integer", "float", "double", "decimal"}:
            if not number_matches:
                continue
            raw = number_matches[-1].replace(",", ".")
            try:
                value = float(raw)
                if value.is_integer():
                    value = int(value)
            except Exception:
                continue
        else:
            if quoted:
                value = quoted[-1]
            elif number_matches:
                value = number_matches[-1]
            else:
                continue
        field_names = {str(f.get("name") or "") for f in row_fields if isinstance(f, dict)}
        qty_field = next((n for n in field_names if n.lower() in {"qty", "quantity", "count", "amount_qty"}), "qty")
        plans.append({
            "table": table_field,
            "field": target_field,
            "value": value,
            "qty_field": qty_field,
            "recalc_sum": "sum" in field_names and target_field in {"price", qty_field},
        })
    if not plans:
        return ""

    return "\n".join([
        "def apply_operations(ctx):",
        "    doc = ctx.get('current_node') or ctx.get('node')",
        "    if doc is None:",
        "        return {'ok': False, 'notes': ['Не найден текущий узел']}",
        "    plans = " + _py_literal(plans),
        "    changed = 0",
        "    for spec in plans:",
        "        table = str(spec.get('table') or '')",
        "        field = str(spec.get('field') or '')",
        "        rows = doc._data.get(table)",
        "        if not isinstance(rows, list):",
        "            continue",
        "        merged_rows = []",
        "        for raw in rows:",
        "            if not isinstance(raw, dict):",
        "                merged_rows.append(raw)",
        "                continue",
        "            row = dict(raw)",
        "            row[field] = spec.get('value')",
        "            if spec.get('recalc_sum') and row.get('price') not in (None, ''):",
        "                try:",
        "                    total = float(row.get('price')) * float(row.get(spec.get('qty_field') or 'qty') or 0)",
        "                    row['sum'] = int(total) if total.is_integer() else total",
        "                except Exception:",
        "                    pass",
        "            merged_rows.append(row)",
        "            changed += 1",
        "        doc._data[table] = merged_rows",
        "    if changed:",
        "        doc._save()",
        "    return {'ok': True, 'notes': ['Изменено строк: ' + str(changed)], 'node_data': doc._data}",
    ])


def _wrap_mobile_operation_handler_with_inline_row_normalizer(
    rt: Any,
    config_uid: str,
    node_context: Optional[Dict[str, Any]],
    code: Any,
    message: Any,
) -> str:
    """Post-normalize rows appended by arbitrary LLM mobile handlers.

    Some model answers bypass JSON operations and directly append a raw dict in
    operation_handler_code.  Wrap that handler, remember table lengths before it
    runs and normalize only newly appended inline rows afterwards.
    """
    src = _strip_python_code(code)
    if not src or not re.search(r"(?m)^\s*def\s+apply_operations\s*\(", src):
        return src
    class_name = _mobile_context_class_name(node_context)
    if not class_name:
        return src
    try:
        classes, lookup = rt._ngenie_collect_context(config_uid, include_samples=False)
    except Exception:
        return src
    class_ctx = next((
        x for x in classes
        if isinstance(x, dict)
        and str(x.get("config_uid") or "") == str(config_uid or "")
        and str(x.get("class_name") or "") == class_name
    ), None)
    if not isinstance(class_ctx, dict):
        return src

    specs: Dict[str, Dict[str, Any]] = {}
    target_classes = set()
    for table in class_ctx.get("tables") or []:
        if not isinstance(table, dict):
            continue
        field = str(table.get("name") or "").strip()
        if not field:
            continue
        row_fields = _mobile_table_row_fields(classes, config_uid, table)
        if not row_fields:
            continue
        node_fields = {}
        qty_field = "qty"
        names = set()
        for fld in row_fields:
            name = str(fld.get("name") or "").strip()
            if not name:
                continue
            names.add(name)
            kind = str(fld.get("kind") or "").strip().lower()
            if kind == "node":
                target = str(fld.get("target") or "").strip()
                node_fields[name] = target
                if target:
                    target_classes.add(target)
            low = name.lower()
            if low in {"qty", "quantity", "count", "amount_qty"} or "колич" in low:
                qty_field = name
        specs[field] = {
            "node_fields": node_fields,
            "qty_field": qty_field,
            "has_price": "price" in names,
            "has_sum": "sum" in names,
        }
    if not specs:
        return src

    # Build a compact view/price map for UID literals already present in the
    # generated code. It is optional; metadata/quantity normalization works even
    # when no literal can be extracted.
    ref_meta: Dict[str, Dict[str, Any]] = {}
    uid_tokens = set(re.findall(r"[A-Za-z0-9_-]+\$[A-Za-z_][A-Za-z0-9_]*\$[A-Za-z0-9_-]+", src))
    uid_tokens.update(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\$[A-Za-z0-9_-]+\b", src))
    for uid in uid_tokens:
        parts = str(uid).split("$")
        target_class = parts[-2] if len(parts) >= 2 else ""
        if target_classes and target_class not in target_classes:
            continue
        target_repo, _target_cfg = lookup.get((config_uid, target_class), (None, None))
        if target_repo is None:
            continue
        data: Dict[str, Any] = {}
        title = ""
        try:
            title = str(rt._ngenie_node_ref_view(target_repo, uid, default="") or "").strip()
        except Exception:
            title = ""
        try:
            loaded = rt._fetch_node_data_for_repo(target_repo, target_class, parts[-1])
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            data = {}
        if not title:
            title = str(data.get("name") or data.get("title") or "").strip()
        meta = {}
        if title:
            meta["view"] = title
        if data.get("price") not in (None, ""):
            meta["price"] = data.get("price")
        if meta:
            ref_meta[uid] = meta
            ref_meta[_mobile_short_node_uid(uid)] = meta

    renamed = re.sub(
        r"(?m)^(\s*)def\s+apply_operations\s*\(",
        r"\1def _ngenie_original_apply_operations(",
        src,
        count=1,
    )
    requested_qty = _message_requested_unit_count(message) or 1
    wrapper = [
        "",
        "def apply_operations(ctx):",
        "    doc = ctx.get('current_node') or ctx.get('node')",
        "    specs = " + _py_literal(specs),
        "    ref_meta = " + _py_literal(ref_meta),
        "    requested_qty = " + str(requested_qty),
        "    before = {}",
        "    if doc is not None:",
        "        for field in specs.keys():",
        "            value = doc._data.get(field)",
        "            before[field] = len(value) if isinstance(value, list) else 0",
        "    result = _ngenie_original_apply_operations(ctx)",
        "    if doc is None:",
        "        return result",
        "    changed = False",
        "    parent_uid = str((getattr(doc, '_data', {}) or {}).get('_id') or '')",
        "    for field, spec in specs.items():",
        "        existing = doc._data.get(field)",
        "        if not isinstance(existing, list):",
        "            continue",
        "        start = min(before.get(field, 0), len(existing))",
        "        old_rows = list(existing[:start])",
        "        new_rows = [dict(x) if isinstance(x, dict) else {'name': str(x)} for x in existing[start:]]",
        "        qk = str(spec.get('qty_field') or 'qty')",
        "        if requested_qty > 1 and len(new_rows) == requested_qty:",
        "            signatures = [{k: v for k, v in row.items() if k != qk and not str(k).startswith('_') and not str(k).endswith('_view')} for row in new_rows]",
        "            if signatures and all(sig == signatures[0] for sig in signatures):",
        "                new_rows = [dict(new_rows[0])]",
        "                new_rows[0][qk] = requested_qty",
        "                changed = True",
        "        elif requested_qty > 1 and len(new_rows) == 1:",
        "            try:",
        "                current_qty = float(new_rows[0].get(qk) or 0)",
        "            except Exception:",
        "                current_qty = 0",
        "            if current_qty <= 1:",
        "                new_rows[0][qk] = requested_qty",
        "                changed = True",
        "        for row in new_rows:",
        "            for node_field in (spec.get('node_fields') or {}).keys():",
        "                value = row.get(node_field)",
        "                if isinstance(value, dict):",
        "                    value = value.get('uid') or value.get('id') or value.get('node_id') or value.get('_id') or ''",
        "                raw_uid = str(value or '').strip()",
        "                target_class = str((spec.get('node_fields') or {}).get(node_field) or '').strip().split('$')[-1]",
        "                parts = raw_uid.split('$') if raw_uid else []",
        "                if len(parts) >= 3:",
        "                    full_uid = raw_uid",
        "                elif len(parts) == 2:",
        "                    full_uid = " + repr(str(config_uid or "")) + " + '$' + raw_uid if " + repr(bool(str(config_uid or "").strip())) + " else raw_uid",
        "                elif raw_uid and target_class:",
        "                    full_uid = " + repr(str(config_uid or "")) + " + '$' + target_class + '$' + raw_uid if " + repr(bool(str(config_uid or "").strip())) + " else raw_uid",
        "                else:",
        "                    full_uid = raw_uid",
        "                if full_uid and full_uid != raw_uid:",
        "                    row[node_field] = full_uid",
        "                    changed = True",
        "                short_uid = '$'.join(full_uid.split('$')[-2:]) if full_uid.count('$') >= 2 else full_uid",
        "                meta = ref_meta.get(full_uid) or ref_meta.get(raw_uid) or ref_meta.get(short_uid) or {}",
        "                if not meta and short_uid:",
        "                    try:",
        "                        getter = globals().get('GetNode')",
        "                        ref_node = getter(full_uid) if callable(getter) else None",
        "                        if ref_node is None and callable(getter) and short_uid != full_uid:",
        "                            ref_node = getter(short_uid)",
        "                        ref_data = dict(getattr(ref_node, '_data', {}) or {}) if ref_node is not None else {}",
        "                        dynamic_meta = {}",
        "                        dynamic_view = ref_data.get('name') or ref_data.get('title') or ref_data.get('caption') or ''",
        "                        if dynamic_view:",
        "                            dynamic_meta['view'] = str(dynamic_view)",
        "                        if ref_data.get('price') not in (None, ''):",
        "                            dynamic_meta['price'] = ref_data.get('price')",
        "                        meta = dynamic_meta",
        "                    except Exception:",
        "                        meta = {}",
        "                if meta.get('view') and not row.get(node_field + '_view'):",
        "                    row[node_field + '_view'] = meta.get('view')",
        "                    changed = True",
        "                if spec.get('has_price') and row.get('price') in (None, '') and meta.get('price') not in (None, ''):",
        "                    row['price'] = meta.get('price')",
        "                    changed = True",
        "            row.setdefault('_parent_node', parent_uid)",
        "            row.setdefault('_parent_table', field)",
        "            row.setdefault('_id', str(__import__('uuid').uuid4()))",
        "            if spec.get('has_sum') and row.get('sum') in (None, '') and row.get('price') not in (None, ''):",
        "                try:",
        "                    total = float(row.get('price')) * float(row.get(qk) or 0)",
        "                    row['sum'] = int(total) if total.is_integer() else total",
        "                except Exception:",
        "                    pass",
        "        if new_rows:",
        "            doc._data[field] = old_rows + new_rows",
        "            changed = True",
        "    if changed:",
        "        doc._save()",
        "    if isinstance(result, dict):",
        "        result['node_data'] = doc._data",
        "    return result",
    ]
    return (renamed.rstrip() + "\n" + "\n".join(wrapper)).strip()


def _mobile_operation_handler_code(answer: Dict[str, Any], message: Any = "") -> str:
    """Return sanitized mobile operation_handler_code, synthesizing it from
    legacy JSON operations when needed."""
    code = _sanitize_mobile_operation_handler_code(_operation_handler_code_from_answer(answer))
    if code:
        return code
    return _sanitize_mobile_operation_handler_code(
        _operation_handler_from_json_operations(answer.get("operations") or [], message=message)
    )


def _mobile_candidate(candidate: Any) -> Dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    view = candidate.get("view") if isinstance(candidate.get("view"), dict) else {}
    title = candidate.get("title") or candidate.get("caption") or view.get("title") or candidate.get("uid") or candidate.get("id") or ""
    out = dict(candidate)
    out.setdefault("id", candidate.get("uid") or candidate.get("node_id") or candidate.get("_id") or candidate.get("id"))
    out.setdefault("uid", out.get("id"))
    out.setdefault("view", {"title": str(title or "")})
    if candidate.get("note") is not None:
        out["note"] = candidate.get("note")
    return out


def _mobile_prepare_clarifications(rt: Any, raw_requests: Any, config_uid: str, lookup: Dict[Any, Any]) -> list:
    """Prepare clarification/display groups for Android without re-querying server
    when the LLM already supplied device-local candidates from handler_result.
    """
    requests_in = raw_requests if isinstance(raw_requests, list) else []
    prepared = []
    backend_raw = []
    for idx, req in enumerate(requests_in, start=1):
        if not isinstance(req, dict):
            continue
        raw_candidates = req.get("candidates") if isinstance(req.get("candidates"), list) else []
        local_candidates = [_mobile_candidate(c) for c in raw_candidates if isinstance(c, dict)]
        local_candidates = [c for c in local_candidates if c.get("id") or c.get("uid")]
        if local_candidates:
            prepared.append({
                "id": str(req.get("id") or req.get("key") or f"clarify_{idx}"),
                "question": str(req.get("question") or req.get("title") or "Уточните вариант"),
                "reason": str(req.get("reason") or ""),
                "config_uid": str(req.get("config_uid") or req.get("configUid") or config_uid or ""),
                "class_name": str(req.get("class_name") or req.get("class") or req.get("target_class") or ""),
                "query": str(req.get("query") or req.get("search") or req.get("value") or ""),
                "context": req.get("context") if isinstance(req.get("context"), dict) else {},
                "required": bool(req.get("required", True)),
                "display_only": bool(req.get("display_only") or req.get("displayOnly") or req.get("mode") == "display"),
                "candidates": local_candidates,
                "source": "mobile_candidate_handler",
            })
        else:
            backend_raw.append(req)
    if backend_raw:
        try:
            prepared.extend(rt._ngenie_prepare_clarifications(backend_raw, config_uid, lookup))
        except Exception:
            pass
    return prepared


def mobile_ambiguities_to_clarifications(rt: Any, handler_result: Dict[str, Any]) -> list:
    """Convert local Android handler_result to UI clarification groups.

    Do not re-query backend repositories here: the candidates came from the
    device storage/indexes and should remain device-local. This is the main
    difference from the web clarification path.
    """
    result = []
    if not isinstance(handler_result, dict):
        return result
    for idx, item in enumerate(handler_result.get("items") or []):
        if not isinstance(item, dict):
            continue
        status = rt._ngenie_handler_item_status(item)
        if status not in {"ambiguous", "review", "need_user", "clarify"}:
            continue
        candidates = [_mobile_candidate(c) for c in (item.get("candidates") or []) if isinstance(c, dict)]
        candidates = [c for c in candidates if c.get("id") or c.get("uid")]
        result.append({
            "id": item.get("id") or item.get("request_id") or f"handler_{idx}",
            "question": item.get("question") or item.get("message") or "Уточните вариант.",
            "class_name": item.get("class_name") or item.get("class") or "",
            "field": item.get("field") or "",
            "context": item.get("context") or {},
            "candidates": candidates,
            "display_only": bool(item.get("display_only")),
            "source": "mobile_candidate_handler",
        })
    return result


def _build_mobile_messages(rt: Any, message: str, config_uid: str, node_context: Optional[Dict[str, Any]], allow_catalog_create: bool, clarification_response: Optional[Dict[str, Any]], scope: str):
    messages = rt._ngenie_build_messages(
        message,
        config_uid,
        node_context,
        allow_catalog_create=allow_catalog_create,
        clarification_response=clarification_response,
        scope=scope,
    )
    messages.append({"role": "user", "content": MOBILE_CANDIDATE_RUNTIME_INSTRUCTION})
    return messages


def build_mobile_plan(
    rt: Any,
    message: str,
    config_uid: str,
    node_context: Optional[Dict[str, Any]] = None,
    allow_catalog_create: bool = False,
    clarification_response: Optional[Dict[str, Any]] = None,
    scope: str = "",
    debug: bool = False,
    client_capabilities: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a backend plan for Android.

    The server owns prompts/skills and candidate resolution, including semantic
    and server-only indexes. Android owns only the final data-changing handler.
    This keeps candidate search identical to web while preserving local/offline
    application of the chosen operation on the device.
    """
    messages = _build_mobile_messages(
        rt,
        message,
        config_uid,
        node_context,
        allow_catalog_create=allow_catalog_create,
        clarification_response=clarification_response,
        scope=scope,
    )
    answer = rt._ngenie_call_deepseek(messages)
    try:
        selected_ids = list(getattr(rt.g, "ngenie_selected_skill_ids", []) or [])
        answer["_ngenie_selected_skill_ids"] = selected_ids
    except Exception:
        selected_ids = []

    if not clarification_response:
        try:
            answer = rt._ngenie_repair_bulk_data_confirmation(messages, answer, message, config_uid)
        except Exception:
            pass
        try:
            data_final = rt._ngenie_finalize_data_requests_with_llm(
                message,
                config_uid,
                node_context,
                allow_catalog_create,
                scope,
                answer,
                attachments=None,
                mobile_mode=True,
            )
            if isinstance(data_final, dict) and data_final:
                answer = data_final
        except Exception:
            pass
        if selected_ids and isinstance(answer, dict):
            answer["_ngenie_selected_skill_ids"] = selected_ids

        try:
            answer = rt._ngenie_repair_invalid_candidate_handler(
                messages, answer, user_message=message
            )
        except Exception:
            pass
        # Repair may replace an unsafe import-based candidate handler with the
        # correct full-set data_requests plan. Run that standard second step.
        try:
            if isinstance(answer, dict) and (answer.get('data_requests') or answer.get('dataRequests')):
                repaired_data_final = rt._ngenie_finalize_data_requests_with_llm(
                    message,
                    config_uid,
                    node_context,
                    allow_catalog_create,
                    scope,
                    answer,
                    attachments=None,
                    mobile_mode=True,
                )
                if isinstance(repaired_data_final, dict) and repaired_data_final:
                    answer = repaired_data_final
        except Exception:
            pass
        if selected_ids and isinstance(answer, dict):
            answer["_ngenie_selected_skill_ids"] = selected_ids

    _classes, lookup = rt._ngenie_collect_context(config_uid, include_samples=False)

    # A raw {'query': ...} inside mobile mutation code would be persisted as a
    # dictionary because Android has no backend reference resolver. Force one
    # repair pass that moves textual Node lookup into candidate_handler_code.
    if not clarification_response and _mobile_operation_handler_has_unresolved_query(answer):
        repair_messages = list(messages) + [
            {"role": "assistant", "content": json.dumps(answer, ensure_ascii=False, default=str)},
            {
                "role": "user",
                "content": (
                    "Ошибка мобильного плана: operation_handler_code содержит неразрешённое значение "
                    "{'query': ...} для Node-поля. Android не умеет сохранять поисковый объект как ссылку. "
                    "Верни полный исправленный JSON для исходной задачи: сначала candidate_handler_code должен "
                    "найти каждый ссылочный узел на сервере по индексам/ngenie_prompt, а operation_handler_code "
                    "появится только после получения UID. Не оставляй query-словари в данных."
                ),
            },
        ]
        repaired = rt._ngenie_call_deepseek(repair_messages)
        if isinstance(repaired, dict) and repaired:
            answer = repaired
            if selected_ids:
                answer["_ngenie_selected_skill_ids"] = selected_ids

    handler_code = "" if clarification_response else _handler_code_from_answer(answer)

    # Backward-compatible repair for JSON operations containing textual Node
    # references. Web already has this second resolve step; mobile now uses the
    # same server candidate pipeline before synthesizing a local handler.
    if not clarification_response and not handler_code:
        try:
            op_resolve_raw = rt._ngenie_extract_resolve_requests_from_operations(
                answer.get("operations") or [], config_uid, node_context, lookup
            )
            if op_resolve_raw:
                resolved_answer = rt._ngenie_resolve_candidate_requests_with_llm(
                    message,
                    config_uid,
                    node_context,
                    allow_catalog_create,
                    scope,
                    answer,
                    op_resolve_raw,
                    lookup,
                    mode="mobile_operation_ref_resolve",
                )
                if isinstance(resolved_answer, dict) and resolved_answer:
                    answer = resolved_answer
                    if selected_ids:
                        answer["_ngenie_selected_skill_ids"] = selected_ids
                    handler_code = _handler_code_from_answer(answer)
        except Exception:
            pass

    if handler_code:
        handler_code = rt._ngenie_strip_candidate_handler_code(handler_code)
        try:
            rt._ngenie_validate_candidate_handler_code(handler_code)
        except ValueError as exc:
            # The model can occasionally copy a general nodes.py helper such as
            # GetAllNodes into a candidate resolver even though candidate lookup
            # must use declared indexes. Give it one constrained repair pass
            # instead of returning HTTP 500 to Android.
            repair_instruction = (
                "The candidate_handler_code was rejected by validation: " + str(exc) + "\n"
                "Return the complete corrected JSON answer for the original request. "
                "For candidate_handler_code use findByIndex/getByIndex with a real index name "
                "when a suitable index exists in the target class context. If no suitable index "
                "is declared, use find(class_name, query). Follow that class ngenie_prompt. "
                "Do not fall back from a strict index merely because it returned no rows. "
                "GetAllNodes, get_all, GetNode, NewNode, find_by_index, full-catalog loops and "
                "invented index names are forbidden. "
                "Keep all quantities, target fields and document-line context from the original request."
            )
            repair_messages = list(messages)
            repair_messages.append({
                "role": "assistant",
                "content": json.dumps(answer, ensure_ascii=False, default=str),
            })
            repair_messages.append({"role": "user", "content": repair_instruction})
            repaired = rt._ngenie_call_deepseek(repair_messages)
            if isinstance(repaired, dict) and repaired:
                answer = repaired
                if selected_ids:
                    answer["_ngenie_selected_skill_ids"] = selected_ids
            handler_code = _handler_code_from_answer(answer)
            if handler_code:
                handler_code = rt._ngenie_strip_candidate_handler_code(handler_code)
                try:
                    rt._ngenie_validate_candidate_handler_code(handler_code)
                except ValueError as repair_exc:
                    return {
                        "ok": False,
                        "phase": "candidate_handler_error",
                        "reply": "nGenie не смог сформировать безопасный поиск кандидатов. Проверьте индексы и ngenie_prompt целевого класса.",
                        "error": str(repair_exc),
                        "candidate_handler_code": "",
                        "operation_handler_code": "",
                        "operations": [],
                        "clarifications": [],
                        "raw": answer if debug else None,
                    }

    if handler_code:
        # Candidate lookup for Android must run on the backend, exactly like web.
        # In particular, semantic_index/server_only/ngenie_remote indexes do not
        # exist in the Android Python runtime. Returning this handler to Android
        # made the same AI answer behave differently between web and mobile.
        try:
            handler_result = rt._ngenie_execute_candidate_handler(
                handler_code,
                message,
                config_uid,
                node_context,
                allow_catalog_create,
                scope,
                answer,
                lookup,
            )
            handler_result = _collapse_duplicate_unit_candidate_items(handler_result, message)
        except Exception as exc:
            return {
                "ok": False,
                "phase": "candidate_handler_error",
                "reply": "Не удалось выполнить серверный поиск кандидатов.",
                "error": str(exc),
                "candidate_handler_code": "",
                "operation_handler_code": "",
                "operations": [],
                "clarifications": [],
                "raw": answer if debug else None,
            }

        state = {
            "version": 3,
            "mode": "mobile_server_candidates_local_operations",
            "message": message,
            "config_uid": config_uid,
            "scope": scope,
            "node_context": node_context or None,
            "allow_catalog_create": bool(allow_catalog_create),
            "initial_answer": answer,
            "client_capabilities": client_capabilities or {},
        }

        blocking_messages = rt._ngenie_handler_blocking_messages(handler_result)
        if blocking_messages:
            return {
                "ok": True,
                "phase": "blocked",
                "reply": "\n".join(blocking_messages),
                "candidate_handler_code": "",
                "operation_handler_code": "",
                "state": state,
                "operations": [],
                "clarifications": [],
                "raw": {**answer, "_candidate_handler_result": rt._ngenie_compact_handler_result_for_llm(handler_result)} if debug else None,
            }

        # Preserve the web behavior for explicit ambiguity: show those candidates
        # immediately. status=review still goes through the second LLM pass so the
        # class ngenie_prompt can rank/filter candidates before asking the user.
        direct_clarifications = rt._ngenie_handler_ambiguities_to_clarifications(
            handler_result, config_uid, lookup
        )
        if direct_clarifications:
            return {
                "ok": True,
                "phase": "show_clarifications",
                "reply": str(answer.get("reply") or "").strip() or "Нужно уточнение выбора.",
                "candidate_handler_code": "",
                "operation_handler_code": "",
                "state": state,
                "operations": [],
                "clarifications": direct_clarifications,
                "raw": {**answer, "_candidate_handler_result": rt._ngenie_compact_handler_result_for_llm(handler_result)} if debug else None,
            }

        finalized = _finalize_mobile_with_llm(
            rt,
            message,
            config_uid,
            node_context,
            allow_catalog_create,
            scope,
            answer,
            handler_result,
        ) or answer

        clarifications = _mobile_prepare_clarifications(
            rt,
            finalized.get("clarification_requests") or finalized.get("clarifications") or [],
            config_uid,
            lookup,
        )
        display_raw = (
            finalized.get("display_requests")
            or finalized.get("displayRequests")
            or finalized.get("candidate_requests")
            or finalized.get("candidateRequests")
            or []
        )
        if isinstance(display_raw, list):
            display_raw = [
                dict(x, display_only=True) if isinstance(x, dict) else x
                for x in display_raw
            ]
        clarifications += _mobile_prepare_clarifications(rt, display_raw, config_uid, lookup)
        operation_handler_code = ""
        if not clarifications:
            deterministic_handler = _mobile_inline_row_scalar_patch_handler(
                rt, config_uid, node_context, message
            )
            if not deterministic_handler:
                deterministic_handler = _mobile_virtual_row_handler_from_selection(
                    rt, config_uid, node_context, handler_result, message
                )
            if not deterministic_handler:
                deterministic_handler = _mobile_virtual_row_handler_from_operations(
                    rt, config_uid, node_context, finalized.get("operations") or [], message
                )
            if deterministic_handler:
                operation_handler_code = _sanitize_mobile_operation_handler_code(deterministic_handler)
        if not operation_handler_code:
            operation_handler_code = _mobile_operation_handler_code(finalized, message=message)
        if operation_handler_code:
            operation_handler_code = _sanitize_mobile_operation_handler_code(
                _wrap_mobile_operation_handler_with_inline_row_normalizer(
                    rt, config_uid, node_context, operation_handler_code, message
                )
            )
        has_selectable = any(
            not bool(group.get("display_only"))
            for group in clarifications
            if isinstance(group, dict)
        )
        return {
            "ok": True,
            "phase": "show_clarifications" if clarifications else "apply_operations",
            "reply": str(finalized.get("reply") or "").strip()
            or (("Нужно уточнение выбора." if has_selectable else "Показал найденные варианты.") if clarifications else "Готово."),
            "candidate_handler_code": "",
            "operation_handler_code": operation_handler_code,
            "state": state,
            "operations": [] if operation_handler_code else (finalized.get("operations") or []),
            "clarifications": clarifications,
            "projection_title": finalized.get("projection_title") or "",
            "projection_method_code": finalized.get("projection_method_code") or "",
            "analysis_html": finalized.get("analysis_html") or "",
            "raw": finalized if debug else None,
        }

    clarifications = _mobile_prepare_clarifications(rt, answer.get("clarification_requests") or answer.get("clarifications") or [], config_uid, lookup)
    display_raw = answer.get("display_requests") or answer.get("displayRequests") or answer.get("candidate_requests") or answer.get("candidateRequests") or []
    if isinstance(display_raw, list):
        display_raw = [dict(x, display_only=True) if isinstance(x, dict) else x for x in display_raw]
    clarifications = clarifications + _mobile_prepare_clarifications(rt, display_raw, config_uid, lookup)
    original_message = str((clarification_response or {}).get("original_message") or message or "")
    operation_handler_code = ""
    if not clarifications:
        deterministic_handler = _mobile_inline_row_scalar_patch_handler(
            rt, config_uid, node_context, original_message
        )
        if clarification_response and not deterministic_handler:
            deterministic_handler = _mobile_virtual_row_handler_from_selection(
                rt, config_uid, node_context, clarification_response, original_message
            )
        if not deterministic_handler:
            deterministic_handler = _mobile_virtual_row_handler_from_operations(
                rt, config_uid, node_context, answer.get("operations") or [], original_message
            )
        if deterministic_handler:
            operation_handler_code = _sanitize_mobile_operation_handler_code(deterministic_handler)
    if not operation_handler_code:
        operation_handler_code = _mobile_operation_handler_code(answer, message=original_message)
    if operation_handler_code:
        operation_handler_code = _sanitize_mobile_operation_handler_code(
            _wrap_mobile_operation_handler_with_inline_row_normalizer(
                rt, config_uid, node_context, operation_handler_code, original_message
            )
        )
    phase = "show_clarifications" if clarifications else "apply_operations"
    has_selectable_clarification = any(not bool(g.get("display_only")) for g in clarifications if isinstance(g, dict))
    return {
        "ok": True,
        "phase": phase,
        "reply": str(answer.get("reply") or "").strip() or (("Нужно уточнение выбора." if has_selectable_clarification else "Показал найденные варианты.") if clarifications else "Готово."),
        "state": {
            "version": 2,
            "mode": "mobile_local_handlers_module",
            "message": message,
            "config_uid": config_uid,
            "scope": scope,
            "node_context": node_context or None,
            "allow_catalog_create": bool(allow_catalog_create),
            "initial_answer": answer,
            "client_capabilities": client_capabilities or {},
        },
        "candidate_handler_code": "",
        "operation_handler_code": operation_handler_code,
        "operations": [] if operation_handler_code else (answer.get("operations") or []),
        "clarifications": clarifications,
        "projection_title": answer.get("projection_title") or "",
        "projection_method_code": answer.get("projection_method_code") or "",
        "analysis_html": answer.get("analysis_html") or "",
        "raw": answer if debug else None,
    }


def _finalize_mobile_with_llm(rt: Any, message: str, config_uid: str, node_context: Optional[Dict[str, Any]], allow_catalog_create: bool, scope: str, initial_answer: Dict[str, Any], handler_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        messages = rt._ngenie_build_candidate_handler_result_messages(
            message,
            config_uid,
            node_context,
            allow_catalog_create,
            scope,
            initial_answer,
            handler_result,
        )
        messages.append({"role": "user", "content": MOBILE_FINALIZE_RUNTIME_INSTRUCTION})
        answer = rt._ngenie_call_deepseek(messages)
        if isinstance(answer, dict) and answer:
            try:
                answer["_ngenie_selected_skill_ids"] = list(getattr(rt.g, "ngenie_selected_skill_ids", []) or [])
            except Exception:
                pass
            answer.setdefault("_candidate_handler_result", rt._ngenie_compact_handler_result_for_llm(handler_result or {}))
            return answer
    except Exception:
        import traceback
        traceback.print_exc()
    return None


def finalize_mobile_plan(
    rt: Any,
    state: Dict[str, Any],
    handler_result: Dict[str, Any],
    debug: bool = False,
) -> Dict[str, Any]:
    """Finalize a local Android candidate handler result into device code.

    Backend may run another LLM pass here, but it still only returns code/plan;
    Android applies it to local nodes in the generated handlers module.
    """
    if not isinstance(state, dict):
        state = {}
    message = str(state.get("message") or "")
    config_uid = str(state.get("config_uid") or "")
    scope = str(state.get("scope") or "")
    node_context = state.get("node_context") if isinstance(state.get("node_context"), dict) else None
    allow_catalog_create = bool(state.get("allow_catalog_create"))
    initial_answer = state.get("initial_answer") if isinstance(state.get("initial_answer"), dict) else {}
    handler_result = _collapse_duplicate_unit_candidate_items(handler_result, message)

    blocking_messages = rt._ngenie_handler_blocking_messages(handler_result or {})
    if blocking_messages:
        return {
            "ok": True,
            "phase": "blocked",
            "reply": "\n".join(blocking_messages),
            "operations": [],
            "operation_handler_code": "",
            "clarifications": [],
            "created_objects": [],
            "node_data": None,
            "layout_html": "",
            "analysis_html": "",
            "projection_title": initial_answer.get("projection_title") or "",
            "projection_method_code": initial_answer.get("projection_method_code") or "",
            "raw": {**initial_answer, "_candidate_handler_result": rt._ngenie_compact_handler_result_for_llm(handler_result or {})} if debug else None,
        }

    early_clarifications = mobile_ambiguities_to_clarifications(rt, handler_result or {})
    if early_clarifications and all(bool(g.get("display_only")) for g in early_clarifications if isinstance(g, dict)):
        return {
            "ok": True,
            "phase": "show_clarifications",
            "reply": str(initial_answer.get("reply") or "Показал найденные варианты."),
            "operations": [],
            "operation_handler_code": "",
            "clarifications": early_clarifications,
            "created_objects": [],
            "node_data": None,
            "layout_html": "",
            "analysis_html": "",
            "projection_title": initial_answer.get("projection_title") or "",
            "projection_method_code": initial_answer.get("projection_method_code") or "",
            "raw": {**initial_answer, "_candidate_handler_result": rt._ngenie_compact_handler_result_for_llm(handler_result or {})} if debug else None,
        }

    # Important: do not immediately show raw local index candidates to the user.
    # Send ambiguous/review candidates back to the LLM first; it has the class
    # ngenie_prompt and can rank/filter/select (e.g. cable 3*1.5 vs all cables).
    finalized = _finalize_mobile_with_llm(
        rt,
        message,
        config_uid,
        node_context,
        allow_catalog_create,
        scope,
        initial_answer,
        handler_result or {},
    ) or initial_answer

    _classes, lookup = rt._ngenie_collect_context(config_uid, include_samples=False)
    clarifications = _mobile_prepare_clarifications(rt, finalized.get("clarification_requests") or finalized.get("clarifications") or [], config_uid, lookup)
    display_raw = finalized.get("display_requests") or finalized.get("displayRequests") or finalized.get("candidate_requests") or finalized.get("candidateRequests") or []
    if isinstance(display_raw, list):
        display_raw = [dict(x, display_only=True) if isinstance(x, dict) else x for x in display_raw]
    clarifications = clarifications + _mobile_prepare_clarifications(rt, display_raw, config_uid, lookup)

    is_node_form_scope = scope == "node_form"
    method_code = finalized.get("projection_method_code") or ""
    analysis_html = finalized.get("analysis_html") or ""
    if is_node_form_scope and (method_code or analysis_html):
        method_code = ""
        analysis_html = ""

    operation_handler_code = ""
    if not clarifications:
        deterministic_handler = _mobile_inline_row_scalar_patch_handler(
            rt, config_uid, node_context, message
        )
        if not deterministic_handler:
            deterministic_handler = _mobile_virtual_row_handler_from_selection(
                rt, config_uid, node_context, handler_result, message
            )
        if not deterministic_handler:
            deterministic_handler = _mobile_virtual_row_handler_from_operations(
                rt, config_uid, node_context, finalized.get("operations") or [], message
            )
        if deterministic_handler:
            operation_handler_code = _sanitize_mobile_operation_handler_code(deterministic_handler)
    if not operation_handler_code:
        operation_handler_code = _mobile_operation_handler_code(finalized, message=message)
    if operation_handler_code:
        operation_handler_code = _sanitize_mobile_operation_handler_code(
            _wrap_mobile_operation_handler_with_inline_row_normalizer(
                rt, config_uid, node_context, operation_handler_code, message
            )
        )
    has_selectable_clarification = any(not bool(g.get("display_only")) for g in clarifications if isinstance(g, dict))
    return {
        "ok": True,
        "phase": "show_clarifications" if clarifications else "apply_operations",
        "reply": str(finalized.get("reply") or "").strip() or (("Нужно уточнение выбора." if has_selectable_clarification else "Показал найденные варианты.") if clarifications else "Готово."),
        "operation_handler_code": operation_handler_code,
        "operations": [] if operation_handler_code else (finalized.get("operations") or []),
        "clarifications": clarifications,
        "created_objects": [],
        "node_data": None,
        "layout_html": "",
        "analysis_html": analysis_html or "",
        "projection_title": finalized.get("projection_title") or "",
        "projection_method_code": method_code,
        "raw": finalized if debug else None,
    }
