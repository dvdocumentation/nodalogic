nGenie data-assistant skills
Each `*.py` file in this folder is a backend skill for the data nGenie assistant
(not `ngenie_code`). The assistant uses skills in two LLM steps:
The router receives only `SKILL_ID`, `NAME` and `DESCRIPTION` for all files.
After one or more skills are selected, the working LLM request receives only
the selected files' `PROMPT`, optional `FUNCTIONS_PROMPT`, and the result of
`prepare_context(base_context)`.
Minimal structure:
```python
SKILL_ID = "my_skill"
NAME = "Human title"
DESCRIPTION = "Short routing description. Keep it enough to decide when to use this skill."
PROMPT = "Long instruction used only after selection."

def prepare_context(base_context):
    return {}

def validate_answer(answer, skill_context=None, base_context=None):
    return []
```
Set `ENABLED = False` to disable a file without deleting it.
