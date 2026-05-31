import polib
from googletrans import Translator

def auto_translate_po_file(po_file_path, target_lang='ru'):
    """Автоматически переводит .po файл"""
    po = polib.pofile(po_file_path)
    translator = Translator()
    
    for entry in po:
        if not entry.msgstr and entry.msgid:  # если перевод пустой
            try:
                translation = translator.translate(entry.msgid, dest=target_lang).text
                entry.msgstr = translation
                print(f"✓ '{entry.msgid}' -> '{translation}'")
            except Exception as e:
                print(f"✗ Ошибка перевода '{entry.msgid}': {e}")
    
    po.save()
    print(f"Перевод завершен! Файл сохранен: {po_file_path}")

# Использование
#auto_translate_po_file('translations/ru/LC_MESSAGES/messages.po',"ru")
auto_translate_po_file('translations/de/LC_MESSAGES/messages.po',"en")