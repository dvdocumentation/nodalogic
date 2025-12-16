from flask import Flask, render_template, redirect, url_for, request, flash, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import uuid
from sqlalchemy import select, text
import base64
import requests
from urllib.parse import urlparse
from flask import send_file
import io
from datetime import datetime, timezone
import json
from flask_sockets import Sockets
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket import WebSocketError
from gevent.pywsgi import WSGIServer
from sqlitedict import SqliteDict
import qrcode
from io import BytesIO
from collections import defaultdict
from flask import g
import pytz
from ast import parse, FunctionDef, fix_missing_locations
import ast
import inspect
import base64
from flask.json.provider import DefaultJSONProvider
import os
import time
import traceback
from flask import session
from functools import wraps



#Функции для работы с кодом обработчиков
import ast
import inspect


# Добавим в начало файла app.py, после импортов

NODE_CLASS_CODE = '''
from nodes import Node
'''

NODE_CLASS_CODE_ANDROID = '''
from nodes import Node
'''

ANDROID_IMPORTS_TEMPLATE = '''from nodesclient import RefreshTab,SetTitle,CloseNode,RunGPS,StopGPS,UpdateView,Dialog,ScanBarcode,GetLocation,AddTimer,StopTimer,ShowProgressButton,HideProgressButton,ShowProgressGlobal,HideProgressGlobal,Controls,SetCover,getBase64FromImageFile,convertImageFilesToBase64Array,saveBase64ToFile,convertBase64ArrayToFilePaths,UpdateMediaGallery
from android import *
from nodes import NewNode, DeleteNode, GetAllNodes, GetNode, GetAllNodesStr, GetRemoteClass, CreateDataSet, GetDataSet, DeleteDataSet,to_uid, from_uid
from com.dv.nodes import DataSet
from com.dv.nodes import DataSets
from com.dv.nodes import SimpleUtilites as su
from datasets import GetDataSetData

# Константы конфигурации
current_module_name="{uid}"
current_configuration_url="{config_url}"
_data_dir = su.get_data_dir(current_module_name)
_downloads_dir = su.get_downloads_dir(current_module_name)

'''

pending_responses = {}

pending_remote_requests = defaultdict(dict)


def api_auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_api_auth(auth.username, auth.password):
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function

def check_api_auth(username, password):
    # Проверка учетных данных для API
    user = db.session.execute(
        select(User).where(User.email == username)
    ).scalar_one_or_none()
    
    if user and check_password_hash(user.password, password):
        return True
    return False



#Функции сервера
from sqlitedict import SqliteDict

STORAGE_BASE_PATH = 'node_storage'

# Создаем базовую директорию
os.makedirs(STORAGE_BASE_PATH, exist_ok=True)

app = Flask(__name__)
sockets = Sockets(app)
class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault('ensure_ascii', False)
        kwargs.setdefault('indent', 4)
        return json.dumps(obj, **kwargs)

    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        return super().default(o)
app.json = CustomJSONProvider(app)
# Хранилище активных WebSocket соединений
active_connections = defaultdict(dict)

app.config['SECRET_KEY'] = 'ferret-key-6630'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024
app.config['USER_TIMEZONE'] = 'Europe/Moscow'
app.config['JSON_AS_ASCII'] = False  # Отключаем экранирование ASCII







# Вебсокет обработчики
def handle_websocket(ws, room_uid):
    """Обработчик WebSocket соединения"""
    print(f"New connection for room {room_uid}")
    user = None
    
    try:

        auth_header = ws.environ.get('HTTP_AUTHORIZATION')
        
        if auth_header and auth_header.startswith('Basic '):
            try:
                credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
                username, password = credentials.split(':', 1)
                
                # Проверяем учетные данные в базе данных
                with app.app_context():
                    user_obj = db.session.execute(
                        select(User).where(User.email == username)
                    ).scalar_one_or_none()
                    
                    if not user_obj or not check_password_hash(user_obj.password, password):
                        ws.close(code=4001)
                        return
                        
                    # Авторизация успешна
                    user = user_obj.email
                    print(f"Authenticated user: {user}")
                    
            except Exception as e:
                print(f"Auth error: {str(e)}")
                ws.close(code=4001)
                return
        else:
            # Если нет заголовка авторизации, закрываем соединение
            ws.close(code=4001)
            return    

        # Получаем инициализационное сообщение
        init_message = ws.receive()
        if not init_message:
            return
            
        try:
            data = json.loads(init_message)
            if data.get('type') != 'connection':
                raise ValueError("First message must be connection type")
                
            user = data.get('user')
            if not user:
                raise ValueError("User not specified")
                
            # Регистрируем соединение
            active_connections[room_uid][user] = ws
            print(f"User {user} connected to room {room_uid}")
            
            # Проверяем, является ли комната отладочной (в контексте приложения)
            is_debug_room = False
            room_name = ""
            with app.app_context():
                room = Room.query.filter_by(uid=room_uid).first()
                if room:
                    is_debug_room = ('отладк' in room.name.lower() or room.name == 'Комната для отладки')
                    room_name = room.name
            
            # Отправляем клиенту информацию о типе комнаты
            room_info = {
                'type': 'room_info',
                'is_debug_room': is_debug_room,
                'room_name': room_name,
                'room_uid': room_uid,
                'message': f'Подключение к комнате "{room_name}" установлено'
            }
            ws.send(json.dumps(room_info))
            
            # Если это отладочная комната, отправляем сообщение о подключении всем клиентам
            if is_debug_room:
                debug_message = {
                    'type': 'debug_connected',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'user': user,
                    'message': f'Пользователь {user} подключился к отладочной комнате'
                }
                
                # Отправляем всем подключенным клиентам этой комнаты
                for conn_user, conn_ws in list(active_connections[room_uid].items()):
                    try:
                        if not conn_ws.closed:
                            conn_ws.send(json.dumps(debug_message))
                    except WebSocketError:
                        active_connections[room_uid].pop(conn_user, None)
            
            # Отправляем текущие задачи (если комната не отладочная)
            if not is_debug_room:
                send_tasks_update(room_uid)
                send_nodes_update(room_uid, user)
            
            # Главный цикл обработки сообщений
            while True:
                message = ws.receive()
                if message is None:
                    break
                    
                try:
                    data = json.loads(message)
                    handle_ws_command(room_uid, user, data)
                except json.JSONDecodeError:
                    print(f"Invalid JSON from {user}")
                    ws.send(json.dumps({'error': 'Invalid JSON format'}))
                
                #time.sleep(0.1)    

        except (ValueError, json.JSONDecodeError) as e:
            print(f"Connection error: {str(e)}")
            ws.send(json.dumps({'error': str(e)}))
            
    except WebSocketError as e:
        print(f"WebSocket error: {str(e)}")
    finally:
        # Удаляем соединение при закрытии
        if user and room_uid in active_connections:
            active_connections[room_uid].pop(user, None)
        if not ws.closed:
            ws.close()
        print(f"Connection closed for {user} in room {room_uid}")


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
      
        

     # Создаем кастомный WSGI-сервер с поддержкой WebSocket
    def application(environ, start_response):
        path = environ.get('PATH_INFO', '')
        
        # Перехватываем WebSocket-запросы
        if path == '/ws' and 'wsgi.websocket' in environ:
            ws = environ['wsgi.websocket']
            room_uid = environ.get('QUERY_STRING', '').replace('room=', '')
            if room_uid:
                handle_websocket(ws, room_uid)
                return []
        
        # Все остальные запросы обрабатываем через Flask
        return app(environ, start_response)
    
    server = WSGIServer(
        ('0.0.0.0', 5000),
        application,
        handler_class=WebSocketHandler
    )

    server.serve_forever()