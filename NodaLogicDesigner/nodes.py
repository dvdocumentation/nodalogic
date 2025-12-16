import uuid
from datetime import datetime, timezone
from sqlitedict import SqliteDict
import os
import threading
import hashlib
import json
import hashlib
import copy


# Базовый путь для хранения
STORAGE_BASE_PATH = 'node_storage'
os.makedirs(STORAGE_BASE_PATH, exist_ok=True)
SCHEMES_DB_PATH = os.path.join(STORAGE_BASE_PATH, "node_schemes.sqlite")
try:
    _SCHEMES_STORAGE = SqliteDict(SCHEMES_DB_PATH, autocommit=True)
except Exception:
    # защищённый fallback на случай проблем с SqliteDict
    _SCHEMES_STORAGE = {}

class Node:
    """Базовый класс для всех узлов с хранением в SqliteDict"""
    _schemes = {}
    
    _class_storages = {}
    _storage_locks = {}  # Блокировки для каждого хранилища
    _instance_locks = {}  # Блокировки для каждого экземпляра
    
    def __init__(self, node_id=None, config_uid=None):
        self._id = node_id or str(uuid.uuid4())
        self._config_uid = config_uid
        self._storage = None
        self._data_cache = None  # Кэш для данных
        
        # Инициализируем блокировку для этого экземпляра
        if self._id not in Node._instance_locks:
            Node._instance_locks[self._id] = threading.RLock()
        
        self._lock = Node._instance_locks[self._id]
        
        with self._lock:
            self._init_storage()
            
            if self._id not in self._storage:
                # Создаем новую запись
                initial_data = {
                    '_id': self._id,
                    '_class': self.__class__.__name__
                }
                self._storage[self._id] = {
                    '_id': self._id,
                    '_class': self.__class__.__name__,
                    '_config_uid': config_uid,
                    '_data': initial_data,
                    '_created_at': datetime.now(timezone.utc).isoformat(),
                    '_updated_at': datetime.now(timezone.utc).isoformat()
                }
            else:
                # Обновляем время доступа и убеждаемся, что _id и _class есть в _data
                node_data = self._storage[self._id]
                if '_data' not in node_data:
                    node_data['_data'] = {}
                
                # Гарантируем, что _id и _class присутствуют в _data
                node_data['_data']['_id'] = self._id
                node_data['_data']['_class'] = self.__class__.__name__
                
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
    
    @property
    def _data(self):
        """Свойство для доступа к данным"""
        with self._lock:
            if self._data_cache is None:
                # Загружаем данные из хранилища в кэш
                if self._id in self._storage:
                    data = self._storage[self._id].get('_data', {})
                    # Гарантируем, что _id и _class всегда присутствуют
                    if '_id' not in data:
                        data['_id'] = self._id
                    if '_class' not in data:
                        data['_class'] = self.__class__.__name__
                    self._data_cache = data
                else:
                    self._data_cache = {}
            return self._data_cache
    
    @_data.setter
    def _data(self, value):
        """Сеттер для данных - сохраняет и обновляет хранилище"""
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                node_data['_data'] = value
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
                self._data_cache = value  # Обновляем кэш
    
    def _save(self):
        """Сохраняет данные из кэша в хранилище"""
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                node_data['_data'] = self._data_cache
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
                return True
            return False
    
    @classmethod
    def create(self, node_id=None, initial_data=None):
        """
        Создает новый узел того же класса и конфигурации.
        
        Args:
            node_id: ID нового узла (генерируется автоматически если не указан)
            initial_data: Начальные данные для нового узла
        
        Returns:
            Node: Новый экземпляр узла
        """
        # Создаем экземпляр узла с тем же config_uid
        new_node = self.__class__(node_id, self._config_uid)
        
        # Если переданы начальные данные - обновляем их
        if initial_data:
            with new_node._lock:
                if new_node._id in new_node._storage:
                    node_data = new_node._storage[new_node._id]
                    if '_data' not in node_data:
                        node_data['_data'] = {}
                    
                    # Обновляем данные, но защищаем _id и _class от перезаписи
                    protected_keys = {'_id', '_class'}
                    for key, value in initial_data.items():
                        if key not in protected_keys:
                            node_data['_data'][key] = value
                    
                    # Гарантируем, что _id и _class присутствуют
                    node_data['_data']['_id'] = new_node._id
                    node_data['_data']['_class'] = self.__class__.__name__
                    
                    node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                    new_node._storage[new_node._id] = node_data
                    
                    # Сбрасываем кэш
                    new_node._data_cache = None
        
        return new_node
    def _init_storage(self):
        class_name = self.__class__.__name__
        storage_key = f"{class_name}_{self._config_uid}" if self._config_uid else class_name
        
        if storage_key not in Node._class_storages:
            # Блокировка для создания нового хранилища
            if storage_key not in Node._storage_locks:
                Node._storage_locks[storage_key] = threading.RLock()
            
            with Node._storage_locks[storage_key]:
                # Двойная проверка после получения блокировки
                if storage_key not in Node._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    Node._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        
        self._storage = Node._class_storages[storage_key]
    
    def get_data(self):
        with self._lock:
            if self._id in self._storage:
                data = self._storage[self._id].get('_data', {})
                # Гарантируем, что _id и _class всегда присутствуют
                if '_id' not in data:
                    data['_id'] = self._id
                if '_class' not in data:
                    data['_class'] = self.__class__.__name__
                return data
            return {}
    
    def set_data(self, key, value):
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                if '_data' not in node_data:
                    node_data['_data'] = {}
                node_data['_data'][key] = value
                
                # Гарантируем, что _id и _class не перезаписываются
                node_data['_data']['_id'] = self._id
                node_data['_data']['_class'] = self.__class__.__name__
                
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
    
    def update_data(self, data_dict):
        with self._lock:
            if self._id in self._storage:
                node_data = self._storage[self._id]
                if '_data' not in node_data:
                    node_data['_data'] = {}
                
                # Обновляем данные, но защищаем _id и _class от перезаписи
                protected_keys = {'_id', '_class'}
                for key, value in data_dict.items():
                    if key not in protected_keys:
                        node_data['_data'][key] = value
                
                # Гарантируем, что _id и _class присутствуют
                node_data['_data']['_id'] = self._id
                node_data['_data']['_class'] = self.__class__.__name__
                
                node_data['_updated_at'] = datetime.now(timezone.utc).isoformat()
                self._storage[self._id] = node_data
    
    def delete(self):
        """Рекурсивно удалить узел и всех его потомков"""
        with self._lock:
            # Сначала рекурсивно удаляем всех потомков
            children = self.GetChildren()
            for child in children:
                child.delete()
            
            # Затем удаляем сам узел
            if self._id in self._storage:
                del self._storage[self._id]
                # Удаляем блокировку экземпляра
                if self._id in Node._instance_locks:
                    del Node._instance_locks[self._id]
            
            # Удаляем связь с родителем если есть
            parent_info = self._data.get('_parent')
            if parent_info:
                try:
                    parent_class_name = parent_info['class']
                    parent_id = parent_info['id']
                    parent_class = globals().get(parent_class_name)
                    if parent_class and issubclass(parent_class, Node):
                        parent_node = parent_class.get(parent_id, self._config_uid)
                        if parent_node:
                            parent_node.RemoveChild(self._id)
                except Exception as e:
                    print(f"Error removing from parent: {e}")
    
    @classmethod
    def get(cls, node_id, config_uid=None):
        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        
        # Убедимся, что хранилище инициализировано
        if storage_key not in cls._class_storages:
            # Блокировка для загрузки хранилища
            if storage_key not in cls._storage_locks:
                cls._storage_locks[storage_key] = threading.RLock()
            
            with cls._storage_locks[storage_key]:
                # Двойная проверка после получения блокировки
                if storage_key not in cls._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    
                    if not os.path.exists(db_path):
                        return None
                    
                    # Загружаем существующее хранилище
                    try:
                        cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
                    except Exception:
                        return None
        
        storage = cls._class_storages[storage_key]
        
        if node_id in storage:
            return cls(node_id, config_uid)
        else:
            return None
    
    @classmethod
    def get_all(cls, config_uid=None):
        storage_key = f"{cls.__name__}_{config_uid}" if config_uid else cls.__name__
        
        if storage_key not in cls._class_storages:
            # Блокировка для загрузки хранилища
            if storage_key not in cls._storage_locks:
                cls._storage_locks[storage_key] = threading.RLock()
            
            with cls._storage_locks[storage_key]:
                # Двойная проверка после получения блокировки
                if storage_key not in cls._class_storages:
                    db_path = os.path.join(STORAGE_BASE_PATH, f"{storage_key}.sqlite")
                    if not os.path.exists(db_path):
                        return {}
                    cls._class_storages[storage_key] = SqliteDict(db_path, autocommit=True)
        
        storage = cls._class_storages[storage_key]
        return {node_id: cls(node_id, config_uid) for node_id in storage.keys()}
    
    @classmethod
    def find(cls, condition_func, config_uid=None):
        results = {}
        for node_id, node in cls.get_all(config_uid).items():
            if condition_func(node):
                results[node_id] = node
        return results
    
    def to_dict(self):
        with self._lock:
            if self._id in self._storage:
                result = self._storage[self._id].copy()
                # Гарантируем, что _data содержит актуальные _id и _class
                if '_data' in result:
                    result['_data']['_id'] = self._id
                    result['_data']['_class'] = self.__class__.__name__
                return result
            return {}
    
    # --- менеджеры схем класса (персистентно через SqliteDict) ---
    @classmethod
    def _load_schemes_for_class(cls):
        """Гарантированно получить схемы для этого класса (из кеша или из Sqlite)."""
        # если уже загружено в атрибут — возвращаем
        if hasattr(cls, "_schemes") and cls._schemes is not None:
            return cls._schemes
        # пытаемся загрузить из персистентного хранилища
        try:
            stored = _SCHEMES_STORAGE.get(cls.__name__, None)
        except Exception:
            stored = None
        cls._schemes = stored or {}
        return cls._schemes

    @classmethod
    def _save_schemes_for_class(cls):
        """Сохранить схемы класса в персистентное хранилище."""
        try:
            _SCHEMES_STORAGE[cls.__name__] = cls._schemes or {}
            # SqliteDict с autocommit обычно не требует commit(), но на всякий случай:
            try:
                _SCHEMES_STORAGE.commit()
            except Exception:
                pass
        except Exception:
            pass

    @classmethod
    def _add_scheme(cls, name, key_types, value_types):
        schemes = cls._load_schemes_for_class()
        schemes[name] = {"keys": key_types, "values": value_types}
        cls._schemes = schemes
        cls._save_schemes_for_class()

    @classmethod
    def _remove_scheme(cls, name):
        schemes = cls._load_schemes_for_class()
        if name in schemes:
            del schemes[name]
            cls._schemes = schemes
            cls._save_schemes_for_class()

    @classmethod
    def _get_schemes(cls):
        return cls._load_schemes_for_class()
    # --- конец менеджеров схем ---

    def _sum_transaction(self, scheme_name, period=None, keys=None, values=None, meta=None):
        """Добавляет транзакцию в указанной схеме"""
        #schemes = self.__class__._get_schemes()
        #if scheme_name not in schemes:
        #    raise ValueError(f"Схема '{scheme_name}' не найдена для {self.__class__.__name__}. "
        #                     f"Зарегистрируй через {self.__class__.__name__}._add_scheme(...)")

        if keys is None:
            keys = []
        if values is None:
            values = []

        # Если период не задан → текущая дата
        if period is None:
            period = datetime.now().strftime("%Y-%m-%d")

        txs = self._data.setdefault("_transactions", {}).setdefault(scheme_name, [])
        last_tx = txs[-1] if txs else None
        parent_id = last_tx["uid"] if last_tx else None

        # Берем прошлые балансы
        balances = last_tx["balances"].copy() if last_tx else {}

        # Формируем ключ аналитики
        key_str = "::".join(str(k) for k in keys)
        if key_str not in balances:
            balances[key_str] = [0] * len(values)

        # Обновляем баланс
        balances[key_str] = [old + delta for old, delta in zip(balances[key_str], values)]

        # uid и hash
        uid = str(uuid.uuid4())
        prev_hash = last_tx["hash"] if last_tx else None
        tx_hash = hashlib.sha256(
            f"{uid}{parent_id}{balances}{period}".encode()
        ).hexdigest()

        tx = {
            "uid": uid,
            "parent": parent_id,
            "child": None,
            "period": period,
            "keys": keys,
            "values": values,
            "balances": balances,
            "hash": tx_hash,
            "prev_hash": prev_hash,
            "meta": meta or {}
        }

        # Закрываем child у предыдущей
        if last_tx:
            last_tx["child"] = uid

        txs.append(tx)
        self._data["_transactions"][scheme_name] = txs
        self._save()
        return uid

    def _get_balance(self, scheme_name):
        """Возвращает актуальные остатки по схеме"""
        txs = self._data.get("_transactions", {}).get(scheme_name, [])
        if not txs:
            return {}
        return txs[-1]["balances"]
    def _get_sum_transactions(self, scheme_name):
        """Возвращает полную цепочку транзакций по схеме"""
        return self._data.get("_transactions", {}).get(scheme_name, [])
    
    def _state_transaction(self, scheme_name, period=None, keys=None, values=None, meta=None):
        """Добавляет транзакцию состояния в указанной схеме (не суммирует, а устанавливает значения)"""
        if keys is None:
            keys = []
        if values is None:
            values = []

        # Если период не задан → текущая дата
        if period is None:
            period = datetime.now().strftime("%Y-%m-%d")

        txs = self._data.setdefault("_state_transactions", {}).setdefault(scheme_name, [])
        last_tx = txs[-1] if txs else None
        parent_id = last_tx["uid"] if last_tx else None

        # Для state транзакций НЕ наследуем balances - каждая транзакция независима
        # Формируем ключ аналитики
        key_str = "::".join(str(k) for k in keys)
        
        # Текущее состояние (значения этой транзакции)
        current_state = {key_str: values.copy()}

        # uid и hash
        uid = str(uuid.uuid4())
        prev_hash = last_tx["hash"] if last_tx else None
        tx_hash = hashlib.sha256(
            f"{uid}{parent_id}{current_state}{period}".encode()
        ).hexdigest()

        tx = {
            "uid": uid,
            "parent": parent_id,
            "child": None,
            "period": period,
            "keys": keys,
            "values": values,
            "state": current_state,  
            "hash": tx_hash,
            "prev_hash": prev_hash,
            "meta": meta or {}
        }

        # Закрываем child у предыдущей
        if last_tx:
            last_tx["child"] = uid

        txs.append(tx)
        self._data["_state_transactions"][scheme_name] = txs
        self._save()
        return uid

    def _get_state_balance(self, scheme_name):
        """Возвращает актуальное состояние по схеме (значения последней транзакции)"""
        txs = self._data.get("_state_transactions", {}).get(scheme_name, [])
        if not txs:
            return {}
        return txs[-1]["state"]  # ← берем state, а не balances

    def _get_state_transactions(self, scheme_name):
        """Возвращает полную цепочку транзакций состояния по схеме"""
        return self._data.get("_state_transactions", {}).get(scheme_name, [])
        
    def __str__(self):
        return f"{self.__class__.__name__}(id={self._id})"
    
    def __repr__(self):
        return self.__str__()
    
    def AddChild(self, child_class, child_id=None, child_data=None):
        """
        Добавить подчиненный узел
        
        Args:
            child_class: класс дочернего узла
            child_id: ID дочернего узла (генерируется автоматически если не указан)
            child_data: данные дочернего узла
        
        Returns:
            Node: созданный дочерний узел
        """
        with self._lock:
            # Создаем дочерний узел
            child_node = child_class(child_id)
            
            # Устанавливаем связь родитель-потомок
            children = self._data.setdefault('_children', [])
            children.append({
                'class': child_class.__name__,
                'id': child_node._id,
                'created_at': datetime.now(timezone.utc).isoformat()
            })
            
            # Устанавливаем связь потомок-родитель
            child_node._data['_parent'] = {
                'class': self.__class__.__name__,
                'id': self._id
            }
            
            # Если переданы данные - обновляем их
            if child_data:
                child_node.update_data(child_data)
            
            # Сохраняем изменения
            self._save()
            child_node._save()
            
            return child_node

    def RemoveChild(self, child_id):
        """
        Удалить подчиненный узел
        
        Args:
            child_id: ID дочернего узла
        """
        with self._lock:
            children = self._data.get('_children', [])
            
            # Ищем и удаляем дочерний узел из списка
            child_to_remove = None
            for child in children:
                if child['id'] == child_id:
                    child_to_remove = child
                    break
            
            if child_to_remove:
                children.remove(child_to_remove)
                self._data['_children'] = children
                
                # Удаляем сам дочерний узел
                child_class_name = child_to_remove['class']
                
                # Получаем класс дочернего узла (упрощенная реализация)
                # В реальной реализации нужно импортировать класс по имени
                try:
                    # Пытаемся найти класс в текущем модуле
                    child_class = globals().get(child_class_name)
                    if child_class and issubclass(child_class, Node):
                        child_node = child_class.get(child_id)
                        if child_node:
                            child_node.delete()
                except Exception:
                    # Если не удалось найти класс, просто удаляем узел из хранилища
                    storage_key = f"{child_class_name}_{self._config_uid}" if self._config_uid else child_class_name
                    if storage_key in Node._class_storages and child_id in Node._class_storages[storage_key]:
                        del Node._class_storages[storage_key][child_id]
                
                self._save()

    def GetChildren(self, level=None):
        """
        Получить всех подчиненных узлов
        
        Args:
            level: уровень вложенности (None - только прямые потомки, -1 - все уровни)
        
        Returns:
            list: список дочерних узлов
        """
        with self._lock:
            children_data = self._data.get('_children', [])
            children_nodes = []
            
            for child_info in children_data:
                child_class_name = child_info['class']
                child_id = child_info['id']
                
                # Пытаемся получить класс дочернего узла
                try:
                    child_class = globals().get(child_class_name)
                    if child_class and issubclass(child_class, Node):
                        child_node = child_class.get(child_id, self._config_uid)
                        if child_node:
                            children_nodes.append(child_node)
                            
                            # Рекурсивно получаем потомков если level = -1
                            if level == -1:
                                grandchildren = child_node.GetChildren(level)
                                children_nodes.extend(grandchildren)
                except Exception as e:
                    print(f"Error getting child node {child_id}: {e}")
                    continue
            
            return children_nodes
