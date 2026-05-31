
import datetime
import requests

class NodaScriptError(Exception):
    pass


class NodaScriptEngine:

    def __init__(self):
        self.externals = {}
        self.max_instructions = 100000

    def register(self, name, fn):
        self.externals[name.lower()] = fn

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
        lines = code.strip().splitlines()
        out = []
        for l in lines:
            l = l.strip()
            if l.startswith("return "):
                expr = l[len("return "):]
                out.append(f"_return = {expr}")
            else:
                out.append(l)
        return "\n".join(out)

    def _make_context(self, data_root):
        ctx = {
            "_data": data_root,
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
        }

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
