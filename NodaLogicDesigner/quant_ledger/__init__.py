# -*- coding: utf-8 -*-
"""Optional Quant Ledger package.

The implementation lives in :mod:`quant_ledger.api`.  Keeping the public API in
an explicit submodule also lets NodaLogic reload generated handlers against a
new ledger implementation without depending on an already cached package
``__init__`` module.
"""
from .api import *  # noqa: F401,F403
from .api import __all__  # noqa: F401

API_VERSION = 2
