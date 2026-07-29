SKILL_ID = "data_summary"
# Used explicitly by direct ngenie() planning. The regular chat keeps its existing
# report flow until data_requests are supported there as a first-class response.
ROUTER_VISIBLE = False
NAME = "Сводка по фактическим узлам"
DESCRIPTION = (
    "Смысловая сводка или анализ фактических узлов текущей конфигурации: "
    "'сделай сводку', 'чего хотят', 'проанализируй активные заказы', "
    "'что происходит'. Навык сначала выбирает реальный класс и фильтры, "
    "а итог строит только по прочитанным из базы узлам."
)

PROMPT = r'''
Навык: сводка по фактическим узлам текущей конфигурации.

На первом шаге НЕ составляй сводку и НЕ пиши projection_method_code.
Сначала верни data_requests — какие узлы backend должен реально прочитать:

"data_requests": [
  {
    "class_name": "Order",
    "filters": [{"field": "active", "op": "eq", "value": true}],
    "fields": ["title", "description", "active"]
  }
]

Правила планирования:
- selected_config_uid — единственная текущая конфигурация.
- Сопоставляй пользовательские слова с class_name/display_name, ngenie_description,
  ngenie_prompt и ngenie_role. Например русское «заказ» может означать class_name Order.
- Используй только настоящие class_name и поля из DataStructure.
- Для «активных» используй boolean-поле active == true, только если оно реально объявлено.
- fields — только данные, нужные для ответа; включай поля, смысл которых объяснён в
  ngenie_prompt. Если сказано «что хочет пользователь — в title», обязательно читай title.
- Не придумывай status, sum, amount, price и иные поля, которых нет в DataStructure.
- Не возвращай operations, проекцию, HTML или готовую summary на шаге планирования.

На втором шаге backend передаст data_results с реально найденными узлами.
Тогда верни только {"summary":"..."}. Сводка должна опираться исключительно на
переданные строки и метаданные класса. Не добавляй показатели и факты, которых нет
в data_results. Если строк нет, прямо сообщи, что подходящих узлов не найдено.
'''


def prepare_context(base_context):
    return {
        "selected_config_uid": (base_context or {}).get("selected_config_uid") or "",
        "classes": [
            {
                "class_name": c.get("class_name"),
                "display_name": c.get("display_name"),
                "ngenie_role": c.get("ngenie_role"),
                "ngenie_description": c.get("ngenie_description"),
                "ngenie_prompt": c.get("ngenie_prompt"),
                "fields": c.get("fields") or [],
            }
            for c in ((base_context or {}).get("classes") or [])
        ],
    }
