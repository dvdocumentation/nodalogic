"""Helpers for the optional, non-owning class hierarchy fields."""

import json


def sync_hierarchy_class_ui(class_obj):
    """Add/remove the generated parent/group controls in a class web layout."""
    try:
        layout = json.loads(getattr(class_obj, "init_screen_layout_web", "") or "[]")
    except Exception:
        layout = []
    if not isinstance(layout, list):
        layout = []

    if not bool(getattr(class_obj, "hierarchy_enabled", False)):
        for row in layout:
            if isinstance(row, list):
                row[:] = [el for el in row if not (isinstance(el, dict) and el.get("id") in {"parent_ref", "is_group"})]
        layout[:] = [row for row in layout if row]
        class_obj.init_screen_layout_web = json.dumps(layout, ensure_ascii=False)
        return

    structure = str(getattr(class_obj, "data_structure", "") or "")
    known = {line.split(":", 1)[0].split("|")[-1].strip() for line in structure.splitlines() if ":" in line}
    additions = []
    if "parent_ref" not in known:
        additions.append(f'Родитель|parent_ref: Node("{class_obj.name}")')
    if "is_group" not in known:
        additions.append("Это группа|is_group: boolean")
    if additions:
        class_obj.data_structure = (structure.rstrip() + "\n" if structure.strip() else "") + "\n".join(additions)

    known_ids = set()

    def collect(value):
        if isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, dict):
            if value.get("id"):
                known_ids.add(str(value["id"]))
            collect(value.get("layout"))
            virtual = value.get("virtual_node")
            if isinstance(virtual, dict):
                collect(virtual.get("layout"))

    collect(layout)
    generated = []
    if "parent_ref" not in known_ids:
        generated.append({"type": "NodeInput", "id": "parent_ref", "caption": "Родительская группа",
                          "value": "@parent_ref", "dataset": class_obj.name, "groups_only": True})
    if "is_group" not in known_ids:
        generated.append({"type": "CheckBox", "id": "is_group", "caption": "Это группа", "value": "@is_group"})
    layout.extend([[element] for element in generated])
    class_obj.init_screen_layout_web = json.dumps(layout, ensure_ascii=False)
