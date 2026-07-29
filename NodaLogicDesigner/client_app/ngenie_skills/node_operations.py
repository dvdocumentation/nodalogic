SKILL_ID = "node_operations"
NAME = "Создание и изменение узлов"
DESCRIPTION = "Создание узлов, изменение текущего узла, импорт данных из Excel/CSV/PDF и других вложений, добавление строк табличной части, подбор ссылочных узлов по индексам/поиску, операции с документами и справочниками. Выбирать для запросов 'создай', 'импортируй файл', 'загрузи строки из Excel', 'добавь', 'измени', 'заполни', 'добавь строки', 'подбери товар/контрагента/склад'."

PROMPT = r'''
Навык: создание и изменение узлов NodaLogic.

Доступные действия в массиве operations:
1) create_node:
   {"tool":"create_node","config_uid":"...","class_name":"Task","data":{...}}
2) update_current_node:
   {"tool":"update_current_node","data":{...}}
3) bulk_update_nodes:
   {"tool":"bulk_update_nodes","config_uid":"...","class_name":"Client","updates":[{"uid":"<полный UID>","data":{"name":"ООО Альфа"}}]}
   Используй для изменения нескольких уже существующих узлов вне формы. Каждый update обязан содержать точный uid/_id из data_results, никаких поисковых строк вместо id.
4) append_table_rows:
   {"tool":"append_table_rows","field":"positions","rows":[{...}]}
   Только для scope=node_form или когда уже есть текущий открытый узел.
5) attach_files:
   {"tool":"attach_files","field":"files"}
   Добавляет прикреплённые пользователем файлы в текущий узел или в узел по uid. field можно не указывать: backend выберет первый FileGallery/MediaGallery field класса.
6) none:
   {"tool":"none"}

Чтение полного набора узлов перед массовой операцией:
- Если задача относится ко ВСЕМ узлам класса, к произвольному набору существующих узлов или данных из контекста недостаточно, не проси пользователя подтверждать findAll/get_all и не повторяй вопрос о разрешении.
- Верни data_requests, backend сам безопасно прочитает доступные узлы текущей конфигурации и вызовет тебя вторым шагом с фактическими data_results:
  {"data_requests":[{"class_name":"Client","fields":["name"],"filters":[]}]}
- На втором шаге data_results содержит rows с точным _id. Выполни исходную задачу сразу: для web/backend верни bulk_update_nodes, для Android — один operation_handler_code, который изменяет узлы по этим UID через GetNode(...), _data и _save().
- Не используй вымышленную функцию findAll. Class.get_all() является Python API обработчиков/отчётов, а не отдельной операцией чата и не требует пользовательского подтверждения.
- Не возвращай data_requests повторно после получения data_results.
- Для задания вроде «создай 5 случайных заказов с имеющимися товарами и клиентами» запроси data_requests отдельно для Order (для следующего номера при необходимости), Client и Goods с нужными полями. Не помещай random/datetime/import или создание документов в candidate_handler_code. На втором шаге используй только UID из data_results, создай требуемое число заказов, в каждом ровно запрошенное число строк, заполни date/client/lines/price/sum и проверь sum = qty * price.

Если для операции нужно подобрать ссылочный узел (например товар/контрагент/склад) по тексту пользователя, сначала составь обработчик поиска кандидатов candidate_handler_code, а не проси backend искать «как-нибудь».
Backend передаёт тебе структуру классов, поля, индексы и ngenie_prompt. На основании этих данных ты сам выбираешь стратегию поиска для каждого элемента запроса: строгий поиск по article/code/barcode, semantic/text/trigram поиск по name/ngenie_name или иной порядок из ngenie_prompt класса. Candidate handler и для web, и для Android выполняется на сервере: semantic_index/server_only/ngenie_remote доступны одинаково. На Android отправляется уже список кандидатов, а не код семантического поиска.

В контексте может быть attachments — список файлов, уже загруженных в UserFiles выбранной конфигурации. Каждый элемент содержит filename/url/original_name/content_type/size и, если формат удалось прочитать, extracted_text, extraction_format, truncated. Для Excel extracted_text содержит листы и строки с номерами; для PDF/DOCX — извлечённый текст; для изображения — результат анализа изображения. extraction_error означает, что backend не смог прочитать содержимое.

ВАЖНО: вложение может быть источником данных, а не файлом для сохранения в узле.
- Если пользователь просит распарсить/прочитать/импортировать Excel, CSV, PDF или другой файл и создать/изменить узлы по его содержимому, используй extracted_text. Не возвращай attach_files и не спрашивай «к какому узлу прикрепить файл», если пользователь явно просит обработать данные из файла.
- attach_files используй только когда пользователь явно просит сохранить сам исходный файл в FileGallery/MediaGallery либо это однозначно часть задачи.
- Если extracted_text отсутствует и есть extraction_error, честно сообщи ошибку чтения файла; не делай вид, что содержимое файла известно.
- Если данные обрезаны (truncated=true), сообщи об этом и не выдавай неполный импорт за полный.

Если пользователь просит добавить/прикрепить сам файл, используй filename. У классов может быть file_fields: [{field,type}], где type = FileGallery или MediaGallery; это обычные поля _data со списком filenames.

Перед созданием candidate_handler_code посмотри список indexes целевого класса.
- Если для искомого значения есть подходящий объявленный индекс, используй findByIndex(class_name, index_name, value) или getByIndex(class_name, index_name, value). Конкретный индекс выбирай по ngenie_prompt класса.
- Если подходящего объявленного индекса нет, используй find(class_name, query).
- Если строгий индекс article/code/barcode существует, но ничего не нашёл, не переходи автоматически на find: следуй ngenie_prompt, обычно возвращай not_found. Переход после пустого индексного результата допустим только когда ngenie_prompt явно задаёт такой fallback.
В candidate_handler_code запрещены GetAllNodes, Class.get_all(), полный перебор класса и ручное сканирование справочника.
- Любые import/from, random, datetime, uuid, создание/изменение узлов и вычисление случайных документов в candidate_handler_code запрещены. Это только этап поиска ссылок.
Используй только реальные имена индексов из контекста класса. Не придумывай суффиксы вроде _trigram/_exact/_semantic, если такого индекса нет в списке indexes. Если в старом ngenie_prompt упомянут несуществующий индекс вроде llm_name_trigram, а в контексте есть реальный semantic_index llm_name, используй findByIndex('Goods', 'llm_name', prepared_query). Если подходящего индекса в контексте вообще нет, используй find('Goods', prepared_query), а не вызывай несуществующий индекс.

Формат candidate_handler_code:
"candidate_handler_code": "def resolve_candidates(ctx):\n    items = []\n    c = findByIndex('Goods', 'article', 'ALT323M8L110') or []\n    if not c:\n        items.append({'id':'sku_line_1','status':'not_found','message':'Товар ALT323M8L110 не найден по артикулу'})\n    elif len(c) == 1:\n        sku = c[0]\n        items.append({'id':'sku_line_1','status':'resolved','uid':sku._data.get('_id'),'class_name':'Goods','question':'Товар для строки 1','context':{'tool':'append_table_rows','field':'positions','row_index':0,'target_field':'sku','qty':2}})\n    else:\n        items.append({'id':'sku_line_1','status':'review','question':'Подобрать товар для строки 1','class_name':'Goods','candidates':ngenie_node_payloads(c, candidate_limit()),'context':{'tool':'append_table_rows','field':'positions','row_index':0,'target_field':'sku','qty':2}})\n    return {'items': items}"

Доступные функции серверного candidate handler (одинаково для web и Android):
- findByIndex(class_name, index_name, value): ищет по объявленному индексу класса, возвращает список Node-объектов или [].
- getByIndex(class_name, index_name, value): ищет по объявленному индексу и возвращает один Node или None.
- find(class_name, query): обычный поиск класса без индекса; используй только когда подходящего индекса нет либо такой fallback прямо задан в ngenie_prompt.
- ngenie_node_payload(node) / ngenie_node_payloads(nodes, limit=candidate_limit()): сериализует Node только для карточек кандидатов.
- candidate_limit(): лимит кандидатов из настройки backend.
- node._data.get('_id') — правильный id узла.
- lower/str/len/re, простой try/except и обычные операции Python доступны. Импорты, файлы, сеть, произвольный exec/eval запрещены.
Не используй Class._indexes, Class.indexes, indexes_json, ConfigClass или чтение конфигурации из кода. Индексы уже переданы тебе в контексте; индексный поиск выполняй через findByIndex/getByIndex, а при отсутствии подходящего индекса используй find.

Обработчик должен вернуть один составной результат на весь пользовательский запрос: товары, покупателя, склад и т.п. в одном items.
	Количество товара — это значение поля количества в одной логической строке, а не число candidate items.
	- «Добавь 2 болгарки в заказ» = один поиск Goods по фразе «болгарка», один item и затем одна строка с qty=2.
	- Не создавай sku_line_1 и sku_line_2 только потому, что количество равно 2.
	- Несколько item/строк нужны только когда пользователь назвал разные товары или явно попросил несколько отдельных строк.
Статусы item:
- resolved: найден один уверенный UID. Укажи uid и context.
- review: кандидаты найдены, следующий LLM-шаг должен применить ngenie_prompt/структуру класса, отранжировать их и выбрать уверенный UID либо вернуть уточнение. Для fuzzy/text/trigram/LIKE поисков обычно используй review, а не ambiguous.
- ambiguous: показывать пользователю candidates на выбор только если LLM/правила не смогут выбрать сами: одинаково подходящие варианты, одинаковый score, или выбор зависит от предпочтения пользователя.
- not_found / fail: ничего не найдено и продолжать нельзя. Укажи message.
Если в ngenie_prompt класса написано «по артикулу только полное совпадение или ничего», то обработчик должен искать строго по нужным индексам и при пустом результате вернуть not_found/fail, без fuzzy и без уточнения.
Если class prompt отсутствует или не хватает правил, можно использовать старый формат resolve_requests.

Формат resolve_requests остаётся допустимым как fallback:
"resolve_requests": [{"id":"sku_line_1","question":"Подобрать товар для строки 1","class_name":"Goods","query":"бур 8 на 110","context":{"tool":"append_table_rows","field":"positions","row_index":0,"target_field":"sku","qty":5}}]

Если нужно уточнение выбора у пользователя, НЕ выполняй operations, а верни clarification_requests.
Формат clarification_requests:
[{"id":"product_line_1","question":"Уточните товар в строке 1","class_name":"Goods","config_uid":"...","query":"бур 10*110","reason":"найдено несколько похожих товаров","max_candidates":8,"note_fields":["stock","available"],"note_template":"Остаток {stock} шт","note_method":"getStock","note_label":"Остаток","context":{"field":"sku","line_no":1}}]
Backend сам подберёт кандидатов по class_name/query и покажет карточки. Если ты уже знаешь точные UID кандидатов, можешь добавить candidates:[{"uid":"...","note":"..."}].
После выбора пользователь отправит clarification_response; тогда продолжай исходную задачу и используй выбранные UID.

Главные правила:
- Используй только классы и поля из переданного контекста.
- Всегда учитывай ngenie_prompt конфигурации и ngenie_prompt класса: это дополнительные навыки проекта.
- Всегда учитывай ngenie_role класса. Роли: catalog/reference — справочник, document — документ, document_line — строка документа, register — регистр/движение, report/projection — отчет/проекция, service/system — технический класс.
- Никогда не придумывай имена полей. Если в DataStructure поле называется parent_doc, используй parent_doc, а не parent.
- Не заполняй вручную _id, _class, _parent, если это явно не требуется. Backend создаёт служебные поля сам.
- _parent не является заменой явного поля DataStructure вроде parent_doc: Node("Container").
- Если добавляешь строки в текущий узел, предпочитай append_table_rows, а не отдельный create_node строки.
	- Для Android итог должен содержать operation_handler_code. Если всё же возвращаешь append_table_rows в operations, сохраняй одну строку на один логический товар и количество внутри поля строки.
- КРИТИЧНО для scope=node_form: если пользователь просит «добавь строки», «заполни строки», «загрузи товары в заказ» или аналогичное, итоговый ответ обязан содержать append_table_rows для текущего узла. Нельзя закончить задачу только созданием Goods/других справочников.
- При частичном изменении существующих inline-строк всегда изменяй поле в копии каждой существующей строки и сохраняй остальные ключи. Команда «сделай цену в строках 100» не должна заменять lines массивом объектов только с price: сохрани product, product_view, qty, _id, _parent_node, _parent_table и пересчитай sum = qty * price.
- Для справочников/ссылочных узлов сначала используй candidate_handler_code: сам выбери индексы и способ поиска по ngenie_prompt класса. Backend не знает предметные правила справочника.

Правило создания родительского узла со строками вне формы узла:
- Если scope не node_form и пользователь говорит «создай Container/документ/заказ/контейнер с товаром X 40 шт», НЕ используй append_table_rows: текущего узла ещё нет.
- Нужно создать родителя одним create_node и положить строки в поле-таблицу data, например:
  {"tool":"create_node","class_name":"Container","data":{"description":"...","positions":[{"sku":"<Goods UID>","qty":40}]}}
- Backend сначала создаст Container, потом сам создаст ContainerLine, заполнит parent_doc ссылкой на родителя и заменит positions на список UID строк.
- Если поле-таблица у родителя называется не positions, используй точное имя из tables/DataStructure.
- Если строка содержит Node-поле, например sku: Node("Goods"), после candidate_handler/finalize подставляй UID товара именно в это поле строки.

Варианты строк/табличных частей:
- Именованная inline-таблица вида lines:[Товар|product: Node("Goods"), Кол-во|qty:number] хранится прямо в поле lines как массив объектов. Для неё используй append_table_rows с field="lines"; отдельные узлы строк не создаются.
- Старый безымянный формат [A|a:string, B|b:number] тоже является inline-таблицей, но у него нет явного имени поля; для новых структур всегда используй именованный формат.
- Таблица вида positions:[Node("ContainerLine")] хранит список UID связанных узлов; при create_node родителя строки можно передать в data.positions как массив объектов, backend создаст узлы строк.
- Таблица вида positions:[ChildNode("ContainerLine")] тоже передаётся как data.positions; backend создаст дочерние узлы и заполнит _parent.
- Поле _children: ChildNode("...") означает дочерние узлы нескольких типов; используй только если именно оно объявлено в DataStructure.
- virtual_tables означает только вычисляемые/экранные представления без сохраняемого поля. Именованные таблицы lines:[...] находятся в tables и их надо заполнять.

Правило файлов:
- Сначала различай две задачи: «прочитать/импортировать данные из файла» и «прикрепить сам файл к узлу». Для импорта используй extracted_text; это не требует FileGallery и не требует вопроса о поле файла.
- Если пользователь прикрепил файл и явно просит добавить сам файл в текущий открытый узел, верни operation {"tool":"attach_files","field":"<file field>"}; field можно опустить, если у класса только один FileGallery/MediaGallery.
- Если пользователь создаёт новый узел и явно хочет сохранить в нём исходный файл, в create_node добавь attach_files:true или явно положи filenames из attachments в file_fields класса, например data.files:["file.pdf"].
- Проси уточнить узел/поле только когда пользователь именно хочет прикрепить файл, но не указал куда. Не задавай этот вопрос при импорте данных из файла.

Правило создания справочников:
- В контексте есть флаг allow_catalog_create.
- Если у класса ngenie_role = catalog или reference, НЕ создавай его без allow_catalog_create, даже если пользователь просит добавить строку документа.
- Без allow_catalog_create НЕ создавай справочные ссылочные узлы: товары, контрагентов, склады, номенклатуру, клиентов, поставщиков и т.п.
- Строки документов и текущий документ создавать/изменять можно. Строка документа — класс, у которого ngenie_role=document_line или который используется в table-поле вида positions:[Node("...")] или positions:[ChildNode("...")].
- Для ссылочных справочников/узлов по тексту пользователя предпочитай candidate_handler_code. Не проси пользователя выбрать, пока обработчик не вернёт candidates или not_found.
- При написании candidate_handler_code применяй ngenie_prompt соответствующего класса.
- Если по ngenie_prompt класса один кандидат подходит уверенно, выбери его UID и верни operations без пользовательского диалога.
- Если кандидатов несколько и выбор зависит от пользователя, верни clarification_requests и попроси пользователя выбрать.
- Для неоднозначных товаров/контрагентов/складов всегда лучше уточнить выбор, чем молча взять первый вариант.
- Если в ngenie_prompt класса написано, что в диалоге выбора надо выводить пояснение (например остаток), используй поля note_fields/note_template/note_method/note_label в resolve_requests/clarification_requests.
- Если ссылочный справочник не найден и allow_catalog_create=false, лучше сообщи, что справочник не найден, чем создавать новый товар/контрагента.

Семантика DataStructure / Wizard:
- Name|name: string -> поле data.name, строка.
- Date|date: date -> дата.
- Number|qty: number -> число.
- Closed|closed: boolean -> true/false.
- SKU|sku: Node("Goods") -> ссылка на узел Goods. Если пользователь дал не UID, а текст, запиши текстовое значение, backend попробует найти существующий узел. Создаст справочник только если allow_catalog_create=true.
- Для web/backend структурированное значение {"query":"..."} допустимо как промежуточное значение и будет разрешено backend. Для Android нельзя сохранять {"query":"..."} в Node-поле и нельзя оставлять его внутри operation_handler_code: сначала верни candidate_handler_code, получи UID на сервере и только затем записывай UID.
- Для импорта, когда надо одновременно создать отсутствующий Goods со всеми реквизитами и добавить строку, используй структурированную ссылку прямо в поле строки: {"query":"артикул или штрихкод", "create_data":{"name":"...", "article":"...", "barcode":"..."}}. Backend сначала найдёт существующий узел; если не найдёт и allow_catalog_create=true — создаст его с create_data, подставит UID и добавит строку. Не создавай Goods отдельными create_node перед append_table_rows.
- Product|product: DataSet("goods") -> ссылка на элемент dataset.
- State|state: select(A|a, B|b) -> одно из значений.
- lines:[Product|product: Node("Goods"), Quantity|qty: number] -> именованная inline-таблица внутри текущего узла; заполняется append_table_rows(field="lines", rows=[...]).
- [Product|product: DataSet("goods"), Quantity|qty: number] -> старый безымянный формат inline-таблицы, не отдельный класс Node.
- positions:[Node("ContainerLine")] -> поле positions хранит список UID строк/связанных узлов класса ContainerLine.
- positions:[ChildNode("ContainerLine")] -> строки являются дочерними узлами; backend дополнительно заполнит _parent.
- _children: ChildNode("ContainerLine")|ChildNode("OtherLine") -> дочерние узлы нескольких классов.

Пример импорта Excel в открытый Order с inline-таблицей lines:
{"operations":[{"tool":"append_table_rows","field":"lines","rows":[
  {"product":{"query":"ADI.1015.T.25","create_data":{"article":"ADI.1015.T.25","name":"Молекулярное сито ...","barcode":"22012455478"}},"qty":1},
  {"product":{"query":"AG-ECW-02","create_data":{"article":"AG-ECW-02","name":"Клей холодная сварка ...","barcode":"901757557510"}},"qty":100}
]}]}
Используй реальные имена полей Goods и строки Order из контекста. Если товар уже существует, create_data игнорируется и используется найденный UID.

Правило строк документов:
- Если у родителя есть поле-таблица positions:[Node("ContainerLine")] или positions:[ChildNode("ContainerLine")], а у класса строки ContainerLine есть поле parent_doc|parent_doc: Node("Container"), то строка должна быть связана с родителем через parent_doc.
- В operation append_table_rows НЕ обязательно указывать parent_doc: backend заполнит его сам.
- Но если ты сам указываешь ссылку на родителя, используй точное имя поля из DataStructure, например parent_doc.
- Не используй поле parent вместо parent_doc, если поля parent нет в DataStructure.
'''


def prepare_context(base_context):
    return {
        "current_node_present": bool((base_context or {}).get("current_node")),
        "allow_catalog_create": bool((base_context or {}).get("allow_catalog_create")),
        "scope": (base_context or {}).get("scope") or "",
        "attachments": [
            {
                "filename": a.get("filename") or a.get("url") or "",
                "original_name": a.get("original_name") or "",
                "content_type": a.get("content_type") or "",
                "extraction_format": a.get("extraction_format") or "",
                "has_extracted_text": bool(a.get("extracted_text")),
                "truncated": bool(a.get("truncated")),
                "extraction_error": a.get("extraction_error") or "",
            }
            for a in ((base_context or {}).get("attachments") or [])
            if isinstance(a, dict)
        ],
        "classes_file_fields": [
            {"class_name": c.get("class_name"), "file_fields": c.get("file_fields") or []}
            for c in ((base_context or {}).get("classes") or [])
            if c.get("file_fields")
        ],
    }
