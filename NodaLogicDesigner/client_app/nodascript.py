
import datetime
import requests

class NodaScriptError(Exception):
    pass


class NodaScriptEngine:

    def __init__(self):
        self.externals = {}
        self.max_instructions = 100000

    def register(self, name, fn):
        key = str(name or "")
        if not key:
            return self
        # Keep the exact spelling because NodaScript handlers commonly use
        # Message/Dialog with capital letters. Also expose a lower-case alias.
        self.externals[key] = fn
        self.externals[key.lower()] = fn
        return self

    # =============================
    # Execution
    # =============================

    def execute(self, code, data_root):
        ctx = self._make_context(data_root)
        exec(self._prepare(code), {}, ctx)
        return data_root

    def get(self, code, data_root):
        ctx = self._make_context(data_root)
        exec(self._prepare(code), {}, ctx)
        return ctx.get("_return")

    def _prepare(self, code):
        # Preserve indentation. The old implementation stripped every line, so
        # any if/for block became invalid Python and web fallback handlers appeared
        # to stop working. Only rewrite an actual return statement.
        lines = str(code or "").strip().splitlines()
        out = []
        for line in lines:
            stripped = line.lstrip()
            indent = line[:len(line) - len(stripped)]
            if stripped.startswith("return "):
                expr = stripped[len("return "):]
                out.append(f"{indent}_return = {expr}")
            elif stripped == "return":
                out.append(f"{indent}_return = None")
            else:
                out.append(line)
        return "\n".join(out)

    def _make_context(self, data_root):
        try:
            import nodes as server_nodes
            _system_user = server_nodes.system_user_node().to_dict()
        except Exception:
            _system_user = {"_id": "", "_class": "_User", "_data": {}}

        ctx = {
            "_data": data_root,
            "_system_user": _system_user,
            "SystemUser": lambda: _system_user,
            "True": True,
            "False": False,
            "None": None,
        }

        ctx.update(self._builtins())

        for k, v in self.externals.items():
            ctx[k] = v

        return ctx

    # =============================
    # Builtins
    # =============================

    def _builtins(self):
        return {
            "Now": lambda: datetime.datetime.utcnow().timestamp() * 1000,
            "ParseDate": self._parse_date,
            "FormatDate": self._format_date,
            "AddDays": lambda d, x: d + x * 86400000,
            "AddMonths": self._add_months,
            "NewArray": lambda: [],
            "NewObject": lambda: {},
            "NewStructure": self._new_structure,
            "Length": lambda x: len(x),
            "HasProperty": lambda o, k: k in o,
            "FindNodeIdsByIndex": self._find_node_ids_by_index,
            "FindByIndex": self._find_by_index,
            "GetByIndex": self._get_by_index,
            "FindByGlobalIndex": self._find_by_global_index,
            "GetByGlobalIndex": self._get_by_global_index,
            "RunProjection": self._run_projection,
            "run_projection": self._run_projection,
            "banner": self._banner,
            "banner_html": self._banner_html,
            "banner_layout": self._banner_layout,
            "close_banner": self._close_banner,
            "ngenie": self._ngenie,
        }

    def _run_projection(self):
        try:
            import nodes as server_nodes
            return server_nodes.RunProjection()
        except Exception as e:
            raise NodaScriptError(str(e))


    def _banner(self, banner_id, value, size=0.25, background=None):
        import nodes as server_nodes
        return server_nodes.banner(banner_id, value, size, background)

    def _banner_html(self, banner_id, value, size=0.25, background=None):
        import nodes as server_nodes
        return server_nodes.banner_html(banner_id, value, size, background)

    def _banner_layout(self, banner_id, value, size=0.25, background=None):
        import nodes as server_nodes
        return server_nodes.banner_layout(banner_id, value, size, background)

    def _close_banner(self, banner_id):
        import nodes as server_nodes
        return server_nodes.close_banner(banner_id)

    def _ngenie(self, prompt, file_path=None):
        import nodes as server_nodes
        return server_nodes.ngenie(prompt, file_path)

    def _parse_date(self, text, pattern=None):
        if pattern:
            dt = datetime.datetime.strptime(text, pattern)
        else:
            dt = datetime.datetime.fromisoformat(text)
        return dt.timestamp() * 1000

    def _format_date(self, millis, pattern):
        dt = datetime.datetime.utcfromtimestamp(millis / 1000)
        return dt.strftime(pattern)

    def _add_months(self, millis, months):
        dt = datetime.datetime.utcfromtimestamp(millis / 1000)
        month = dt.month - 1 + months
        year = dt.year + month // 12
        month = month % 12 + 1
        day = min(dt.day, 28)
        dt = dt.replace(year=year, month=month, day=day)
        return dt.timestamp() * 1000

    def _new_structure(self, *args):
        if len(args) % 2 != 0:
            raise NodaScriptError("NewStructure requires even arguments")
        d = {}
        for i in range(0, len(args), 2):
            d[str(args[i])] = args[i+1]
        return d


    def _find_node_ids_by_index(self, class_name, index_name, value, config_uid=None):
        try:
            import nodes as server_nodes
            return server_nodes.find_node_ids_by_index(class_name, index_name, value, config_uid)
        except Exception as e:
            raise NodaScriptError(str(e))


    def _find_by_index(self, class_name, index_name, value, config_uid=None):
        try:
            import nodes as server_nodes
            return server_nodes.findByIndex(class_name, index_name, value, config_uid)
        except Exception as e:
            raise NodaScriptError(str(e))

    def _get_by_index(self, class_name, index_name, value, config_uid=None):
        try:
            import nodes as server_nodes
            return server_nodes.getByIndex(class_name, index_name, value, config_uid)
        except Exception as e:
            raise NodaScriptError(str(e))

    def _find_by_global_index(self, index_name, value):
        try:
            import nodes as server_nodes
            return server_nodes.findByGlobalIndex(index_name, value)
        except Exception as e:
            raise NodaScriptError(str(e))

    def _get_by_global_index(self, index_name, value):
        try:
            import nodes as server_nodes
            return server_nodes.getByGlobalIndex(index_name, value)
        except Exception as e:
            raise NodaScriptError(str(e))
