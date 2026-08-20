# Quant Ledger

Optional server-side SQL ledger for NodaLogic. The package has no editor UI.
When the `quant_ledger` directory exists, `app.py` creates its SQL tables at startup.
Handlers opt in explicitly:

```python
from quant_ledger.api import quant, move, transaction, balance, balances, statement
```

See `ngenie_code/instructions/22_QUANT_LEDGER.md` and `demo/quant_ledger_demo.nod`.

`operation_id` uses reposting semantics: a repeated call atomically removes the
old movement from balances and replaces it with the new one. `MoveResult.reposted`
is `True` when a previous movement was replaced.
