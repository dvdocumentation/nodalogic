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
from urllib.parse import parse_qs
import logging
from flask_babel import Babel, _,format_datetime,format_date
import re



logging.getLogger("geventwebsocket.handler").setLevel(logging.ERROR)



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

    user = db.session.execute(
        select(User).where(User.email == username)
    ).scalar_one_or_none()
    
    if user and check_password_hash(user.password, password):
        return True
    return False




#Server functions
from sqlitedict import SqliteDict

STORAGE_BASE_PATH = 'node_storage'


os.makedirs(STORAGE_BASE_PATH, exist_ok=True)

def get_locale():
    # if a user is logged in, use the locale from the user settings
    user = getattr(g, 'user', None)
    if user is not None:
        return user.locale
    # otherwise try to guess the language from the user accept
    # header the browser transmits.  We support de/fr/en in this
    # example.  The best match wins.
    return request.accept_languages.best_match(['de', 'en', 'ru'])

def get_timezone():
    user = getattr(g, 'user', None)
    if user is not None:
        return user.timezone

app = Flask(__name__)
app.config['BABEL_DEFAULT_LOCALE'] = 'en'


sockets = Sockets(app)


LANGUAGES = {
    'en': 'English', 
    'ru': 'Русский',
    'de': 'Deutsch'
}

def get_locale():
    
    lang = request.args.get('lang')
    if lang in LANGUAGES:
        session['current_language'] = lang
        return lang
    
    
    if 'current_language' in session and session['current_language'] in LANGUAGES:
        return session['current_language']
    
    
    lang_cookie = request.cookies.get('language')
    if lang_cookie in LANGUAGES:
        return lang_cookie
    
    
    if hasattr(g, 'user') and g.user is not None:
        return g.user.locale
    
   
    return request.accept_languages.best_match(LANGUAGES.keys())

def get_timezone():
    if hasattr(g, 'user') and g.user is not None:
        return g.user.timezone
    return 'UTC'


babel = Babel(app, locale_selector=get_locale, timezone_selector=get_timezone)


@app.context_processor
def utility_processor():
    return {
        'get_locale': get_locale,
        'LANGUAGES': LANGUAGES,
        'format_datetime': format_datetime,
        'format_date': format_date
    }

@app.route('/set_language/<lang>')
def set_language(lang):
    if lang in LANGUAGES:
        
        session['current_language'] = lang
        session.permanent = True  
        
        
        response = redirect(request.referrer or url_for('index'))
       
        response.set_cookie('language', lang, max_age=365*24*60*60)  # 1 год
        return response
    
    return redirect(request.referrer or url_for('index'))



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

active_connections = defaultdict(dict)

app.config['SECRET_KEY'] = 'ferret-key-6630'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024
app.config['USER_TIMEZONE'] = 'Europe/Moscow'
app.config['JSON_AS_ASCII'] = False  


TASKS_DB_PATH = 'tasks.db'

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'index'


class Dataset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(100))
    hash_indexes = db.Column(db.String(255))  
    text_indexes = db.Column(db.String(255))  
    view_template = db.Column(db.Text) 
    autoload = db.Column(db.Boolean, default=False)  
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id'))
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    
    items = db.relationship('DatasetItem', backref='dataset', cascade='all, delete-orphan')

class DatasetItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    item_id = db.Column(db.String(100))  
    data = db.Column(db.JSON)  
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

   
    __table_args__ = (
        db.Index('idx_dataset_item_id', 'dataset_id', 'item_id'),
    )


class Room(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(100))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
    config_display_name = db.Column(db.String(100), default="")

    configurations = db.relationship('Configuration', backref='user', lazy=True)

class UserDevice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    android_id = db.Column(db.String(100), nullable=False)
    device_model = db.Column(db.String(200))
    token = db.Column(db.String(200))
    last_connected = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    user = db.relationship('User', backref=db.backref('devices', lazy=True))  

class ConfigEvent(db.Model):
    __tablename__ = 'config_event'
    id = db.Column(db.Integer, primary_key=True)
    event = db.Column(db.String(100), nullable=False)  # onLaunch, onBarcode, etc.
    listener = db.Column(db.String(200), default="", nullable=False)
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    actions = db.relationship('ConfigEventAction', backref='event_obj', cascade='all, delete-orphan', order_by='ConfigEventAction.id')

    def actions_as_dicts(self):
        """Преобразует действия события в словари для JSON сериализации"""
        result = []
        for action in self.actions:
            action_dict = {
                "action": action.action,
                "method": action.method,
                "source": action.source,
                "server": action.server,
                "postExecuteMethod": action.post_execute_method
            }
            
            action_dict = {k: v for k, v in action_dict.items() if v is not None and v != ""}
            result.append(action_dict)
        return result

class ConfigEventAction(db.Model):
    __tablename__ = 'config_event_action'
    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(50), default='run', nullable=False)   # run, runprogress, runasync
    source = db.Column(db.String(50), default='internal', nullable=False)
    server = db.Column(db.String(255), default="")
    method = db.Column(db.String(200), default="")
    post_execute_method = db.Column(db.String(200), default="")
    order = db.Column(db.Integer, default=0)  

    event_id = db.Column(db.Integer, db.ForeignKey('config_event.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "action": self.action,
            "source": self.source,
            "server": self.server,
            "method": self.method,
            "postExecuteMethod": self.post_execute_method,
            "order": self.order,
        }
    
class Configuration(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))
    content_uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))
    vendor = db.Column(db.String(100))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    version = db.Column(db.String(20), default="00.00.01")
    server_name = db.Column(db.String(100), default="") 
    last_modified = db.Column(db.DateTime, nullable=False, 
                            default=datetime.now,
                            onupdate=datetime.now)
    nodes_handlers = db.Column(db.Text)  
    nodes_handlers_meta = db.Column(db.JSON)  
    nodes_server_handlers = db.Column(db.Text, nullable=True)  
    nodes_server_handlers_meta = db.Column(db.JSON)
    classes = db.relationship('ConfigClass', backref='config', cascade='all, delete-orphan')
    datasets = db.relationship('Dataset', backref='config', cascade='all, delete-orphan')
    sections = db.relationship('ConfigSection', backref='config', cascade='all, delete-orphan')
    servers = db.relationship('Server', backref='config', cascade='all, delete-orphan')
    config_events = db.relationship('ConfigEvent', backref='config', cascade='all, delete-orphan')
    
    def update_last_modified(self):
        self.last_modified = datetime.now()
        db.session.commit()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        if not hasattr(self, 'version'):
            self.version = "00.00.01"
        if not hasattr(self, 'last_modified'):
            self.last_modified = datetime.now(timezone.utc)
class ConfigSection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    code = db.Column(db.String(100))
    commands = db.Column(db.Text)
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id'))

class ConfigClass(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id'))
    has_storage = db.Column(db.Boolean, default=False)  
    class_type = db.Column(db.String(50))  
    display_name = db.Column(db.String(100))  
    cover_image = db.Column(db.Text)  
    section = db.Column(db.String(100))  
    section_code = db.Column(db.String(100)) 
    methods = db.relationship('ClassMethod', backref='class_obj', cascade='all, delete-orphan')
    events = db.Column(db.JSON, default={})
    hidden = db.Column(db.Boolean, default=False)
    event_objs = db.relationship('ClassEvent', backref='class_obj', cascade='all, delete-orphan')

class ClassMethod(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    source = db.Column(db.String(100), default='internal')
    server = db.Column(db.String(255)) 
    engine = db.Column(db.String(50))
    
    code = db.Column(db.Text)
    class_id = db.Column(db.Integer, db.ForeignKey('config_class.id'))



class ClassEvent(db.Model):
    __tablename__ = 'class_event'
    id = db.Column(db.Integer, primary_key=True)
    event = db.Column(db.String(100), nullable=False)           
    listener = db.Column(db.String(200), default="", nullable=False)
    class_id = db.Column(db.Integer, db.ForeignKey('config_class.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    
    actions = db.relationship('EventAction', backref='event_obj', cascade='all, delete-orphan', order_by='EventAction.id')

    def actions_as_dicts(self):
        """Преобразует действия события в словари для JSON сериализации"""
        result = []
        for action in self.actions:
            action_dict = {
                "action": action.action,
                "method": action.method,
                "source": action.source,
                "server": action.server,
                "postExecuteMethod": action.post_execute_method
            }
            
            action_dict = {k: v for k, v in action_dict.items() if v is not None and v != ""}
            result.append(action_dict)
        return result

class EventAction(db.Model):
    __tablename__ = 'event_action'
    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(50), default='run', nullable=False)   # run, runprogress, runasync
    source = db.Column(db.String(50), default='internal', nullable=False)
    server = db.Column(db.String(255), default="")
    method = db.Column(db.String(200), default="")
    post_execute_method = db.Column(db.String(200), default="")
    order = db.Column(db.Integer, default=0)  

    event_id = db.Column(db.Integer, db.ForeignKey('class_event.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "action": self.action,
            "source": self.source,
            "server": self.server,
            "method": self.method,
            "postExecuteMethod": self.post_execute_method,
            "order": self.order,
        }


class RoomObjects(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    room_uid = db.Column(db.String(36))
    config_uid = db.Column(db.String(36))
    class_name = db.Column(db.String(100))
    objects_data = db.Column(db.JSON) 
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    expires_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc) )

    acknowledged_by = db.Column(db.JSON, default=list) 
    
    __table_args__ = (
        db.Index('idx_room_objects', 'room_uid', 'config_uid', 'class_name'),
    )  

class Server(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    url = db.Column(db.String(255), nullable=False)
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id'))
    is_default = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

      

# Авторизация
@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))
@app.route('/update-device-token/<int:device_id>', methods=['POST'])
@login_required
def update_device_token(device_id):
    device = UserDevice.query.get_or_404(device_id)
    if device.user_id != current_user.id:
        abort(403)
    device.token = request.form.get('token')
    db.session.commit()
    flash('Token updated', 'success')
    return redirect(url_for('edit_profile'))

@app.route('/api/get-token', methods=['GET'])
@login_required
def get_token_by_android_id():
    android_id = request.args.get('android_id')
    if not android_id:
        return jsonify({'error': 'android_id is required'}), 400

    device = UserDevice.query.filter_by(user_id=current_user.id, android_id=android_id).first()
    if not device:
        return jsonify({'error': 'device not found'}), 404

    return jsonify({'token': device.token or ''})


@app.route('/edit-profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    if request.method == 'POST':
        current_user.email = request.form.get('email')
        if request.form.get('password'):
            current_user.password = generate_password_hash(request.form.get('password'))
        current_user.config_display_name = request.form.get('config_display_name')
        db.session.commit()
        flash(_('Profile updated successfully'), 'success')
        return redirect(url_for('dashboard'))
    
    devices = UserDevice.query.filter_by(user_id=current_user.id).all()
    return render_template('edit_profile.html', devices=devices)




@app.route('/admin')
@login_required
def admin_dashboard():
    if current_user.email != 'dv1555@hotmail.com':  
        abort(403)
    

    total_users = db.session.query(User).count()
    total_devices = db.session.query(UserDevice).count()
    
    
    active_users = set()
    for room_connections in active_connections.values():
        active_users.update(room_connections.keys())
    active_users_count = len(active_users)
    
    
    active_devices_count = sum(len(connections) for connections in active_connections.values())
    
    
    users_with_stats = db.session.query(
        User,
        db.func.count(UserDevice.id).label('device_count')
    ).outerjoin(UserDevice).group_by(User.id).all()
    
    return render_template('admin_dashboard.html',
                         total_users=total_users,
                         total_devices=total_devices,
                         active_users_count=active_users_count,
                         active_devices_count=active_devices_count,
                         users_with_stats=users_with_stats,active_connections=active_connections)


@app.route('/admin/user/<int:user_id>')
@login_required
def admin_user_detail(user_id):
    
    if current_user.email != 'dv1555@hotmail.com':
        abort(403)
    
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    
    
    devices = UserDevice.query.filter_by(user_id=user_id).all()
    
   
    configurations = Configuration.query.filter_by(user_id=user_id).all()
    
    
    rooms = Room.query.filter_by(user_id=user_id).all()
    
    
    is_active = any(user.email in connections for connections in active_connections.values())
    
    return render_template('admin_user_detail.html',
                         user=user,
                         devices=devices,
                         configurations=configurations,
                         rooms=rooms,
                         is_active=is_active)


@app.route('/admin/user/<int:user_id>/toggle-active', methods=['POST'])
@login_required
def admin_toggle_user_active(user_id):
    if current_user.email != 'dv1555@hotmail.com':
        abort(403)
    
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    
    flash(f'Статус пользователя {user.email} изменен', 'success')
    return redirect(url_for('admin_user_detail', user_id=user_id))


@app.template_filter('b64decode')
def b64decode_filter(s):
    if s:
        try:
            return base64.b64decode(s).decode('utf-8')
        except Exception as e:
            print(f"Decoding error: {str(e)}")
            return _("# Decoding error:")+ str(e)
    return ""

@app.route('/', methods=['GET', 'POST'])
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        form_type = request.form.get('form_type')
        
        if form_type == 'login':
            email = request.form.get('email')
            password = request.form.get('password')
            user = db.session.execute(
                select(User).where(User.email == email)
            ).scalar_one_or_none()
            
            if user and check_password_hash(user.password, password):
                login_user(user)
                return redirect(url_for('dashboard'))
            flash(_('Invalid email or password'), 'error')

        elif form_type == 'register':
            email = request.form.get('email')
            password = request.form.get('password')
            
            if db.session.execute(
                select(User).where(User.email == email)
            ).scalar_one_or_none():
                flash(_('Email already taken'), 'error')
            else:
                new_user = User(
                    email=email,
                    password=generate_password_hash(password)
                )
                db.session.add(new_user)
                db.session.commit()
                login_user(new_user)
                return redirect(url_for('dashboard'))
    
    return render_template('index.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

#Rooms

@app.route('/create-room', methods=['POST'])
@login_required
def create_room():
    name = request.form.get('name', 'Новая комната')
    new_room = Room(
        name=name,
        user_id=current_user.id
    )
    db.session.add(new_room)
    db.session.commit()
    return redirect(url_for('room_detail', room_uid=new_room.uid))

def generate_qr_code(data):
    import qrcode
    from io import BytesIO
    import base64
    
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill='black', back_color='white')
    
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def get_ws_scheme():
    # If Flask runs behind HTTPS (for example, via nginx with SSL)
    if request.is_secure or request.headers.get('X-Forwarded-Proto', '').lower() == 'https':
        return 'wss'
    return 'ws'

@app.route('/room/<room_uid>')
@login_required
def room_detail(room_uid):
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first_or_404()
    
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        tasks = tasks_db.get(room_uid, [])
        active_tasks = [t for t in tasks if not t.get('_done')]
    
    
    ws_scheme = get_ws_scheme()
    ws_url = f"{ws_scheme}://{request.host}/ws?room={room.uid}"

    qr_img = generate_qr_code(ws_url)

    
    
    return render_template('room_detail.html', 
                         room=room,
                         tasks=tasks,
                         active_tasks=active_tasks,
                         ws_url=ws_url,
                         qr_img=qr_img)




@app.route('/delete-room/<room_uid>')
@login_required
def delete_room(room_uid):
    room = Room.query.filter_by(uid=room_uid, user_id=current_user.id).first_or_404()
    db.session.delete(room)
    db.session.commit()
    
    
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        if room.uid in tasks_db:
            del tasks_db[room.uid]
        tasks_db.commit()
    
    return redirect(url_for('dashboard') + '#rooms')

def get_connected_users(room_uid):
    """Returns a list of all connected users in the room."""
    if room_uid not in active_connections:
        return []
    
    users = []
    for username, ws in active_connections[room_uid].items():
        users.append({
            'user': username,
            'email': username,  
            'connection_time': datetime.now(timezone.utc).isoformat(),
            'status': 'connected'
        })
    return users

# Websocket handlers
def handle_websocket(ws, room_uid):
    
    print(f"New connection for room {room_uid}")
    user = None
    
    try:

        auth_header = ws.environ.get('HTTP_AUTHORIZATION')
        active_connections[room_uid][user] = ws
        user_connected_message = {
            'type': 'user_connected',
            'user': user,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        for conn_user, conn_ws in list(active_connections[room_uid].items()):
            try:
                if not conn_ws.closed:
                    conn_ws.send(json.dumps(user_connected_message))
            except WebSocketError:
                active_connections[room_uid].pop(conn_user, None)

        auth_success=False
        if auth_header and auth_header.startswith('Basic '):
             try:
                 credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
                 username, password = credentials.split(':', 1)
                
                
                 with app.app_context():
                     user_obj = db.session.execute(
                         select(User).where(User.email == username)
                     ).scalar_one_or_none()
                    
                     if not user_obj or not check_password_hash(user_obj.password, password):
                         ws.close(code=4001)
                         return
                       
                     
                     user = user_obj.email
                     auth_success=True

                     #android_id = ws.environ.get('HTTP_ANDROID_ID')
                     #device_model = ws.environ.get('HTTP_DEVICE_MODEL')

                     query = parse_qs(ws.environ.get('QUERY_STRING', ''))
                     android_id = query.get('android_id', [None])[0]
                     device_model = query.get('device_model', [None])[0]

                     if android_id:
                        with app.app_context():
                            device = UserDevice.query.filter_by(user_id=user_obj.id, android_id=android_id).first()
                            if not device:
                                device = UserDevice(
                                    user_id=user_obj.id,
                                    android_id=android_id,
                                    device_model=device_model or "Unknown"
                                )
                                db.session.add(device)
                            else:
                                device.device_model = device_model or device.device_model
                                device.last_connected = datetime.now(timezone.utc)
                            db.session.commit()   

                     print(f"Authenticated user: {user}")
                   
             except Exception as e:
                 print(f"Auth error: {str(e)}")
                 ws.close(code=4001)
                 return
        else:
             
             #ws.close(code=4001)
             #return    
             pass

        
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
                
            
            active_connections[room_uid][user] = ws
            print(f"User {user} connected to room {room_uid}")

            # Minimal app: no debug-room behavior
            room_name = ""
            with app.app_context():
                room = Room.query.filter_by(uid=room_uid).first()
                if room:
                    room_name = room.name

            room_info = {
                'type': 'room_info',
                'is_debug_room': False,
                'room_name': room_name,
                'room_uid': room_uid,
                'message': f'Connection to the room  "{room_name}" has been established'
            }
            ws.send(json.dumps(room_info))

            if auth_success:

                send_tasks_update(room_uid)
                send_nodes_update(room_uid, user)
            
            
            while True:
                message = ws.receive()
                if message is None:
                    break
                    
                try:
                    data = json.loads(message)
                    handle_ws_command(room_uid, user, data, auth_success)
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
        
        if user and room_uid in active_connections:
            active_connections[room_uid].pop(user, None)
            
            user_disconnected_message = {
                'type': 'user_disconnected',
                'user': user,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            for conn_user, conn_ws in list(active_connections[room_uid].items()):
                try:
                    if not conn_ws.closed:
                        conn_ws.send(json.dumps(user_disconnected_message))
                except WebSocketError:
                    active_connections[room_uid].pop(conn_user, None)

        if not ws.closed:
            ws.close()
        print(f"Connection closed for {user} in room {room_uid}")

def send_nodes_update(room_uid, user_id=None):
    """Sends objects to room clients, excluding those already confirmed"""
    with app.app_context():
        query = RoomObjects.query.filter_by(room_uid=room_uid)
        
        objects = query.order_by(RoomObjects.created_at.desc()).all()
        
        if room_uid in active_connections:
            for user, ws in list(active_connections[room_uid].items()):
                
                if user_id and user != user_id:
                    continue
                    
                objects_data = []
                for obj in objects:
                    
                    if user in (obj.acknowledged_by or []):
                        continue
                        
                    objects_data.append({
                        'object_id': obj.id,  
                        'config_uid': obj.config_uid,
                        'class_name': obj.class_name,
                        'objects': obj.objects_data,
                        'created_at': obj.created_at.isoformat()
                    })
                
                if objects_data:  
                    try:
                        if not ws.closed:
                            ws.send(json.dumps({
                                'type': 'nodes_update',
                                'objects': objects_data
                            }))
                    except WebSocketError:
                        active_connections[room_uid].pop(user, None)
                        print(f"Removed dead connection for {user}")       

def send_tasks_update(room_uid):
    """Sends a task update to all clients in the room"""
    with app.app_context():  
        with SqliteDict(TASKS_DB_PATH) as tasks_db:
            tasks = tasks_db.get(room_uid, [])
            active_tasks = [t for t in tasks if not t.get('_done') and not t.get('_blocked')]
            
            if room_uid in active_connections:
                for user, ws in list(active_connections[room_uid].items()):
                    try:
                        if not ws.closed: 
                            ws.send(json.dumps({
                                'type': 'tasks_update',
                                'data': active_tasks
                            }))
                    except WebSocketError:
                        
                        active_connections[room_uid].pop(user, None)
                        print(f"Removed dead connection for {user}")


# API for working with tasks
@app.route('/api/room/<room_uid>/tasks', methods=['POST'])
@api_auth_required
def add_tasks(room_uid):
    if not request.is_json:
        abort(400, description="Request must be JSON")
    
    tasks = request.json
    if not isinstance(tasks, list):
        abort(400, description="Tasks should be an array")
    
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
       
        for task in tasks:
            if not isinstance(task, dict):
                continue
                
            if 'uid' not in task:
                task['uid'] = str(uuid.uuid4())
            task['_created'] = datetime.now(timezone.utc).isoformat()
            room_tasks.append(task)
        
        tasks_db[room_uid] = room_tasks
        tasks_db.commit()
        
        # Sending an update via websocket
        active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
        send_tasks_update(room_uid)
    
    return jsonify({"status": "success", "count": len(tasks)})

@app.route('/api/room/<room_uid>/tasks/available', methods=['GET'])
@api_auth_required
def get_available_task(room_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        # Find the first available task
        for i, task in enumerate(room_tasks):
            if not task.get('_done') and not task.get('_blocked'):
                # Mark as blocked
                room_tasks[i]['_blocked'] = True
                room_tasks[i]['_blocked_at'] = datetime.now(timezone.utc).isoformat()
                tasks_db[room_uid] = room_tasks
                tasks_db.commit()
                
                # Sending an update via websocket
                active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
                send_tasks_update(room_uid)
                
                return jsonify(task)
    
    return jsonify({"status": "no_tasks_available"}), 404

@app.route('/api/room/<room_uid>/tasks/<task_uid>/complete', methods=['POST'])
@api_auth_required
def complete_task(room_uid, task_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        for i, task in enumerate(room_tasks):
            if task.get('uid') == task_uid:
                room_tasks[i]['_done'] = True
                room_tasks[i]['_completed_at'] = datetime.now(timezone.utc).isoformat()
                tasks_db[room_uid] = room_tasks
                tasks_db.commit()
                
                # Sending an update via websocket
                active_tasks = [t for t in room_tasks if not t.get('_done') and not t.get('_blocked')]
                send_tasks_update(room_uid)
                
                return jsonify({"status": "success"})
    
    return jsonify({"status": "task_not_found"}), 404

@app.route('/api/room/<room_uid>/tasks/completed', methods=['DELETE'])
@api_auth_required
def clear_completed_tasks(room_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        room_tasks = tasks_db.get(room_uid, [])
        
        # We leave only unfinished tasks
        updated_tasks = [t for t in room_tasks if not t.get('_done')]
        tasks_db[room_uid] = updated_tasks
        tasks_db.commit()
        
        # Sending an update via websocket
        active_tasks = [t for t in updated_tasks if not t.get('_done') and not t.get('_blocked')]
        send_tasks_update(room_uid)
    
    return jsonify({"status": "success", "remaining": len(updated_tasks)})


#Personal account
@app.route('/dashboard')
@login_required
def dashboard():
    stmt = select(Configuration).where(Configuration.user_id == current_user.id)
    configs = db.session.execute(stmt).scalars().all()
    
    stmt = select(Room).where(Room.user_id == current_user.id)
    rooms = db.session.execute(stmt).scalars().all()
    
    return render_template('dashboard.html', configs=configs, rooms=rooms)

@app.route('/delete-config/<uid>')
@login_required
def delete_config(uid):
    # Replace the execute with scalar() or first()
    config = db.session.scalar(
        select(Configuration)
        .where(Configuration.uid == uid, Configuration.user_id == current_user.id)
    )
    
    if config:
        db.session.delete(config)
        db.session.commit()
        flash(_('Configuration deleted'), 'success')

    return redirect(url_for('dashboard'))




@app.route('/api/config/<config_uid>/node/<class_name>/<node_id>/<method_name>', methods=['POST'])
@api_auth_required
def execute_node_method(config_uid, class_name, node_id, method_name):
    """API for node execution"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        if config.nodes_server_handlers:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            # Create an isolated namespace
            isolated_globals = {}
            exec(handlers_code, isolated_globals)
            
            # Check that the class exists and is a subclass of Node
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # Get the node
                node = node_class.get(node_id, config_uid)
                if not node:
                    abort(404, description=f"Node {node_id} not found")
                
                # Check the existence of the method
                if not hasattr(node, method_name):
                    abort(404, description=f"Method {method_name} not found in class {class_name}")
                
                # Getting input data
                request_data = request.get_json() or {}
                
                # Determine the method type
                custom_methods = ['_sum_transaction', '_get_sum_balance', '_get_balance', '_get_sum_transactions',
            '_state_transaction', '_get_state_balance', '_get_state_transactions',
            '_add_scheme', '_remove_scheme']
                
                if method_name in custom_methods:
                    # Handling arbitrary methods
                    args = request_data.get('args', [])
                    kwargs = request_data.get('kwargs', {})
                    
                    try:
                        result = getattr(node, method_name)(*args, **kwargs)
                        return jsonify({
                            'status': True,
                            'result': result
                        })
                    except Exception as e:
                        return jsonify({
                            'status': False,
                            'error': str(e)
                        }), 500
                
                else:
                    # Processing standard methods
                    input_data = request_data
                    
                    try:
                        if method_name == "_save":
                            if input_data:
                                node._data_cache = input_data
                            result = node._save()
                            return jsonify({
                                'status': result,
                                'node': node.to_dict()
                            })
                        else:
                            result = getattr(node, method_name)(input_data)
                            if isinstance(result, tuple) and len(result) == 2:
                                success, data = result
                                return jsonify({
                                    'status': success,
                                    'data': data
                                })
                            else:
                                return jsonify(result)
                    except Exception as e:
                        return jsonify({
                            'status': False,
                            'error': str(e),
                            'node': node.to_dict()
                        }), 500
        
        abort(404, description=f"Class {class_name} not found")
        
    except Exception as e:
        return jsonify({'status': False, "error": str(e)}), 500

#API for calling remote nodes
@app.route('/api/<room_uid>/<target_user>/<config_uid>/remote_node/<class_name>/<node_id>/<method_name>', methods=['POST'])
def execute_remote_method(room_uid, target_user, config_uid, class_name, node_id, method_name):
    """Executing a method on a remote device via WebSocket"""
    
    # Check if the target user is active in the room
    if (room_uid not in active_connections or 
        target_user not in active_connections[room_uid]):
        return jsonify({
            'success': False,
            'error': f'A user {target_user} not in the room {room_uid}'
        }), 404
    
    # Get the target user's WebSocket connection
    target_ws = active_connections[room_uid][target_user]
    if target_ws.closed:
        return jsonify({
            'success': False,
            'error': f'Connection with {target_user} was closed'
        }), 404
    
    # Generate a unique request ID
    request_id = str(uuid.uuid4())
    
    # Getting input data
    input_data = request.get_json() or {}
    
    # Create a message to send
    message = {
        'type': 'remote_method',
        'request_id': request_id,
        'config_uid': config_uid,
        'class_name': class_name,
        'node_id': node_id,
        'method_name': method_name,
        'input_data': input_data,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Initialize the query in the waiting dictionary
    if room_uid not in pending_remote_requests:
        pending_remote_requests[room_uid] = {}
    
    pending_remote_requests[room_uid][request_id] = {
        'result': None,
        'error': None,
        'completed': False
    }
    
    
        
    request_id = str(uuid.uuid4())
    
    # Create a record about waiting for a response
    pending_responses[request_id] = {
        'room_uid': room_uid,
        'completed': False,
        'result': None,
        'error': None,
        'created_at': time.time()
    }
    
    # Add request_id to the message
    message['request_id'] = request_id
    
    try:
        target_ws.send(json.dumps(message))
        
        
        return jsonify({
            'success': True,
            'request_id': request_id,
            'status': 'pending',
            'message': 'The request has been sent. Use /api/check-response to check the status.'
        }), 202
        
    except WebSocketError as e:
        # Remove from pending on error
        if request_id in pending_responses:
            del pending_responses[request_id]
        return jsonify({
            'success': False,
            'error': f'WebSocket Error: {str(e)}'
        }), 500
        


@app.route('/api/check-response/<request_id>')
@api_auth_required
def check_response(request_id):
    """Checking the status of a remote request"""
    if request_id not in pending_responses:
        return jsonify({
            'status': 'not_found',
            'message': 'Request not found or expired'
        }), 404
    
    response_data = pending_responses[request_id]
    
    if response_data['completed']:
        # The request is complete, we return the result and clean up
        result = response_data
        del pending_responses[request_id]
        
        if result['error']:
            return jsonify({
                'status': 'error',
                'error': result['error']
            }), 500
        else:
            return jsonify({
                'status': 'completed',
                'data': result['result']
            })
    else:
        # Запрос еще в процессе
        elapsed = time.time() - response_data['created_at']
        return jsonify({
            'status': 'pending',
            'elapsed_seconds': round(elapsed, 1),
            'message': 'The request is still being processed.'
        })

@app.route('/api/config/<config_uid>/node/<class_name>/<node_id>', methods=['GET', 'PUT', 'DELETE'])
@api_auth_required
def node_api(config_uid, class_name, node_id):
    """API for working with a specific node"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        if config.nodes_server_handlers:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            # Create an isolated namespace
            isolated_globals = {}
            exec(handlers_code, isolated_globals)
            
            # We check that the class exists and is a subclass of Node from this space
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                if request.method == 'GET':
                    node = node_class.get(node_id, config_uid)
                    if node:
                        return jsonify(node.to_dict())
                    abort(404)
                
                elif request.method == 'PUT':
                    data = request.get_json()
                    node = node_class(node_id, config_uid)
                    if data:
                        node.update_data(data)
                    return jsonify(node.to_dict())
                
                elif request.method == 'DELETE':
                    node = node_class.get(node_id, config_uid)
                    if node:
                        node.delete()
                        return jsonify({"status": "deleted"})
                    abort(404)
        
        abort(404)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<config_uid>/node/<class_name>/register/<room_uid>', methods=['POST'])
@api_auth_required
def register_nodes(config_uid, class_name, room_uid):
    """Registers nodes of the specified class in the download room"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        # Safely retrieve JSON from the request body
        node_ids = []
        if request.data:  # Check if there is a request body
            try:
                #request_data = request.get_json() or {}
                node_ids = request.get_json() or []
            except Exception:
                # If the JSON is invalid, we assume that the body is empty.
                node_ids = []
        
        if config.nodes_server_handlers:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            # Create an isolated namespace
            isolated_globals = {}
            exec(handlers_code, isolated_globals)
            
            # Checking that the class exists
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # We receive nodes
                if node_ids:
                    # We register only selected nodes
                    nodes_data = []
                    for node_id in node_ids:
                        node = node_class.get(node_id, config_uid)
                        if node:
                            node_dict = node.to_dict()
                            node_dict['_id'] = node_id
                            nodes_data.append(node_dict)
                    
                    message = f"Registered {len(nodes_data)} selected nodes"
                else:
                    # Register all class nodes
                    nodes = node_class.get_all(config_uid)
                    nodes_data = []
                    for node_id, node in nodes.items():
                        node_dict = node.to_dict()
                        node_dict['_id'] = node_id
                        nodes_data.append(node_dict)
                    
                    message = f"Registered all {len(nodes_data)} nodes"
                
                # We register in the room
                return  handle_room_objects(config_uid, class_name, room_uid, nodes_data)
                
                
              
                
                
        
        abort(404, description=f"Class {class_name} not found")
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<config_uid>/node/<class_name>', methods=['GET', 'POST'])
@api_auth_required
def nodes_api(config_uid, class_name):
    """API for working with all class nodes"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)

    room_uid = request.args.get('room')  

    if room_uid and request.method == 'POST':
        # Processing through the room instead of direct creation
        data = request.get_json() or {}

        if config.nodes_server_handlers:
            try:
                handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
                isolated_globals = {}
                exec(handlers_code, isolated_globals)
                
                if (class_name in isolated_globals and 
                    hasattr(isolated_globals[class_name], '__bases__') and
                    any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                    
                    node_class = isolated_globals[class_name]
                    created_nodes = []
                    
                    # Processing both an array and a single object
                    objects_data = data if isinstance(data, list) else [data]
                    
                    for item_data in objects_data:
                        node_id = item_data.get('_id') or str(uuid.uuid4())
                        
                        # Removing system fields from user data
                        user_data = {k: v for k, v in item_data.items() if not k.startswith('_')}
                        
                        # CREATING A NODE ON THE SERVER
                        node = node_class(node_id, config_uid)
                        if user_data:
                            node.update_data(user_data)
                        
                        created_nodes.append(node.to_dict())

                        return handle_room_objects(config_uid, class_name, room_uid,data)  
            except Exception as e:
                return jsonify({"error": str(e)}), 500        
        
    
    try:
        if config.nodes_server_handlers:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            # Create an isolated namespace
            isolated_globals = {}
            exec(handlers_code, isolated_globals)
            
            # We check that the class exists and is a subclass of Node from this space
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                if request.method == 'GET':
                    nodes = node_class.get_all(config_uid)
                    result = {node_id: node.to_dict() for node_id, node in nodes.items()}
                    return jsonify(result)
                
                elif request.method == 'POST':
                    data = request.get_json() or {}
                    
                    # Supports both single object and array of objects
                    if isinstance(data, list):
                        # Processing an array of objects
                        created_nodes = []
                        for item_data in data:
                            node_id = item_data.get('_id') or str(uuid.uuid4())
                            
                            # Removing system fields from user data
                            user_data = {k: v for k, v in item_data.items() if not k.startswith('_')}
                            
                            node = node_class(node_id, config_uid)
                            if user_data:
                                node.update_data(user_data)
                            
                            created_nodes.append(node.to_dict())
                        
                        return jsonify(created_nodes), 201
                    
                    else:
                        # Processing a single object (old logic)
                        node_id = data.get('_id') or str(uuid.uuid4())
                        
                        # Removing system fields from user data
                        user_data = {k: v for k, v in data.items() if not k.startswith('_')}
                        
                        node = node_class(node_id, config_uid)
                        if user_data:
                            node.update_data(user_data)
                        
                        return jsonify(node.to_dict()), 201
        
        abort(404)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def handle_room_objects(config_uid, class_name, room_uid,data):
    """Processing objects across the room"""
    #data = request.get_json() or {}
    
    if not isinstance(data, list):
        data = [data]
    
    # Saving objects to the room database
    room_objects = RoomObjects(
        room_uid=room_uid,
        config_uid=config_uid,
        class_name=class_name,
        objects_data=data,
        expires_at=datetime.now(timezone.utc) ,
        acknowledged_by=[] 
    )
    db.session.add(room_objects)
    db.session.commit()
    
    # We send a message to all connected clients of the room
    send_nodes_update(room_uid)
    
    return jsonify({
        "status": "objects_queued",
        "count": len(data),
        "room_uid": room_uid,
        "object_id": room_objects.id,  # Return the ID of the created object
        "message": "Objects sent to room for client processing"
    }), 202 

def send_objects_update(room_uid, config_uid, class_name, objects_data):
    """Sends an object update to all clients of the room"""
    if room_uid in active_connections:
        message = {
            'type': 'objects_create',
            'config_uid': config_uid,
            'class_name': class_name,
            'objects': objects_data,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        for user, ws in list(active_connections[room_uid].items()):
            try:
                if not ws.closed:
                    ws.send(json.dumps(message))
            except WebSocketError:
                active_connections[room_uid].pop(user, None)
                print(f"Removed dead connection for {user}")

@app.route('/api/room/<room_uid>/objects', methods=['GET'])
@api_auth_required
def get_room_objects(room_uid):
    """Get objects for the room"""
    config_uid = request.args.get('config_uid')
    class_name = request.args.get('class_name')
    since = request.args.get('since')  # Optional: Get objects after the specified date
    
    query = RoomObjects.query.filter_by(room_uid=room_uid)
    
    if config_uid:
        query = query.filter_by(config_uid=config_uid)
    if class_name:
        query = query.filter_by(class_name=class_name)
    if since:
        try:
            since_date = datetime.fromisoformat(since.replace('Z', '+00:00'))
            query = query.filter(RoomObjects.created_at > since_date)
        except ValueError:
            pass
    
    objects = query.order_by(RoomObjects.created_at.desc()).all()
    
    result = []
    for obj in objects:
        result.append({
            'id': obj.id,
            'config_uid': obj.config_uid,
            'class_name': obj.class_name,
            'objects': obj.objects_data,
            'created_at': obj.created_at.isoformat(),
            'expires_at': obj.expires_at.isoformat()
        })
    
    return jsonify(result)

@app.route('/api/room/<room_uid>/objects', methods=['DELETE'])
@api_auth_required
def cleanup_room_objects(room_uid):
    """Очистить старые объекты в комнате"""
    older_than = request.args.get('older_than')
    
    if not older_than:
        return jsonify({"error": "Parameter 'older_than' is required"}), 400
    
    try:
        cutoff_date = datetime.fromisoformat(older_than.replace('Z', '+00:00'))
        
        # Delete objects older than the specified date
        deleted_count = RoomObjects.query.filter(
            RoomObjects.room_uid == room_uid,
            RoomObjects.created_at < cutoff_date
        ).delete()
        
        db.session.commit()
        
        return jsonify({
            "status": "success",
            "deleted_count": deleted_count,
            "cutoff_date": cutoff_date.isoformat()
        })
        
    except ValueError:
        return jsonify({"error": "Invalid date format. Use ISO format"}), 400                

@app.route('/api/config/<config_uid>/node/<class_name>/search', methods=['POST'])
@api_auth_required
def search_nodes(config_uid, class_name):
    """API for searching nodes by condition"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == config_uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    try:
        if config.nodes_server_handlers:
            handlers_code = base64.b64decode(config.nodes_server_handlers).decode('utf-8')
            
            # Create an isolated namespace
            isolated_globals = {}
            exec(handlers_code, isolated_globals)
            
            # We check that the class exists and is a subclass of Node from this space
            if (class_name in isolated_globals and 
                hasattr(isolated_globals[class_name], '__bases__') and
                any(base.__name__ == 'Node' for base in isolated_globals[class_name].__bases__)):
                
                node_class = isolated_globals[class_name]
                
                # We get the search condition from the request body
                search_condition = request.get_json() or {}
                
                def condition_func(node):
                    node_data = node.to_dict().get('_data', {})
                    for key, value in search_condition.items():
                        if key not in node_data or str(node_data[key]) != str(value):
                            return False
                    return True
                
                # We perform a search
                results = node_class.find(condition_func, config_uid)
                return jsonify({node_id: node.to_dict() for node_id, node in results.items()})
        
        abort(404)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/import-config-new', methods=['POST'])
@login_required
def import_config_new():
    """Import configuration from file - creates a new one or updates an existing one"""
    if 'config_file' not in request.files:
        flash(_('File not selected'), 'error')
        return redirect(url_for('dashboard'))
    
    file = request.files['config_file']
    if file.filename == '':
        flash(_('File not selected'), 'error')
        return redirect(url_for('dashboard'))
    
    if not file.filename.endswith('.nod'):
        flash(_('Only NOD files allowed'), 'error')
        return redirect(url_for('dashboard'))
    
    try:
        data = json.load(file.stream)
        
        print(f"Starting import of configuration")
        print(f"Data keys: {list(data.keys())}")
        
        
        imported_uid = data.get('uid')
        content_uid = data.get("content_uid")
        if not imported_uid:
            flash(_('Invalid configuration file: missing UID'), 'error')
            return redirect(url_for('dashboard'))
        
        # CHECKING IF A CONFIGURATION WITH THIS UID ALREADY EXISTS
        existing_config = db.session.execute(
            select(Configuration).where(Configuration.user_id==current_user.id, Configuration.content_uid==content_uid)
        ).scalar_one_or_none()
        
        if existing_config:
            # IF THE CONFIGURATION EXISTS, UPDATE IT
            print(f"Updating existing configuration: {existing_config.name}")
            
            # Checking access rights
            if existing_config.user_id != current_user.id:
                flash(_('You do not have permission to update this configuration'), 'error')
                return redirect(url_for('dashboard'))
            
            # UPDATE MAIN CONFIGURATION DATA
            existing_config.name = data.get('name', existing_config.name)
            existing_config.server_name = data.get('server_name', existing_config.server_name)
            existing_config.version = data.get('version', existing_config.version)
            existing_config.nodes_handlers = data.get('nodes_handlers', existing_config.nodes_handlers)
            existing_config.nodes_handlers_meta = data.get('nodes_handlers_meta', existing_config.nodes_handlers_meta)
            existing_config.nodes_server_handlers = data.get('nodes_server_handlers', existing_config.nodes_server_handlers)
            existing_config.nodes_server_handlers_meta = data.get('nodes_server_handlers_meta', existing_config.nodes_server_handlers_meta)
            
            # Delete all existing related data for a complete update
            print("Deleting existing related data...")
            for class_obj in existing_config.classes:
                db.session.delete(class_obj)
            for dataset in existing_config.datasets:
                db.session.delete(dataset)
            for section in existing_config.sections:
                db.session.delete(section)
            for server in existing_config.servers:
                db.session.delete(server)
            for event in existing_config.config_events:
                db.session.delete(event)    
            
            config_to_use = existing_config
            is_update = True
            
        else:
            # IF THERE IS NO CONFIGURATION - CREATE A NEW ONE
            print(f"Creating new configuration with UID: {imported_uid}")
            
            new_config = Configuration(
                name=data.get('name', _('Imported configuration')),
                server_name=data.get('server_name', ''),
                version=data.get('version', '00.00.01'),
                nodes_handlers=data.get('nodes_handlers', ''),
                nodes_handlers_meta=data.get('nodes_handlers_meta', {}),
                nodes_server_handlers=data.get('nodes_server_handlers', ''),
                nodes_server_handlers_meta=data.get('nodes_server_handlers_meta', {}),
                user_id=current_user.id,
                uid=str(uuid.uuid4()), 
                content_uid=content_uid,
                vendor=data.get("vendor")
            )
            
            db.session.add(new_config)
            db.session.flush()
            config_to_use = new_config
            is_update = False
        
        # IMPORT CLASSES (same for creation and update)
        classes_data = data.get('classes', [])
        print(f"Importing {len(classes_data)} classes...")
        
        for class_data in classes_data:
            new_class = ConfigClass(
                name=class_data['name'],
                section=class_data.get('section', ''),
                section_code=class_data.get('section_code', ''),
                has_storage=class_data.get('has_storage', False),
                display_name=class_data.get('display_name', class_data['name']),
                cover_image=class_data.get('cover_image', ''),
                class_type=class_data.get('class_type', ''),
                hidden=class_data.get('hidden', False),
                config_id=config_to_use.id
            )
            db.session.add(new_class)
            db.session.flush()
            
            # Import class methods
            methods_data = class_data.get('methods', [])
            print(f"  Importing {len(methods_data)} methods for class {class_data['name']}")
            
            for method_data in methods_data:
                new_method = ClassMethod(
                    name=method_data['name'],
                    source=method_data.get('source', 'internal'),
                    engine=method_data['engine'],
                    code=method_data['code'],
                    class_id=new_class.id
                )
                db.session.add(new_method)
            
            # Import class events
            events_data = class_data.get('events', [])
            print(f"  Importing {len(events_data)} events for class {class_data['name']}")
            
            for event_data in events_data:
                new_event = ClassEvent(
                    event=event_data['event'],
                    listener=event_data.get('listener', ''),
                    class_id=new_class.id
                )
                db.session.add(new_event)
                db.session.flush()
                
                # Import event actions
                actions_data = event_data.get('actions', [])
                print(f"    Importing {len(actions_data)} actions for event {event_data['event']}")
                
                for action_data in actions_data:
                    new_action = EventAction(
                        action=action_data.get('action', 'run'),
                        source=action_data.get('source', 'internal'),
                        server=action_data.get('server', ''),
                        method=action_data.get('method', ''),
                        post_execute_method=action_data.get('postExecuteMethod', ''),
                        order=action_data.get('order', 0),
                        event_id=new_event.id
                    )
                    db.session.add(new_action)
        
        # Import datasets
        datasets_data = data.get('datasets', [])
        print(f"Importing {len(datasets_data)} datasets...")
        
        for dataset_data in datasets_data:
            # Convert arrays back to strings for storage in the database
            hash_indexes = ','.join(dataset_data.get('hash_indexes', [])) if isinstance(dataset_data.get('hash_indexes'), list) else dataset_data.get('hash_indexes', '')
            text_indexes = ','.join(dataset_data.get('text_indexes', [])) if isinstance(dataset_data.get('text_indexes'), list) else dataset_data.get('text_indexes', '')
            
            new_dataset = Dataset(
                name=dataset_data['name'],
                hash_indexes=hash_indexes,
                text_indexes=text_indexes,
                view_template=dataset_data.get('view_template', ''),
                autoload=dataset_data.get('autoload', False),
                config_id=config_to_use.id
            )
            db.session.add(new_dataset)
        
        # Import sections
        sections_data = data.get('sections', [])
        print(f"Importing {len(sections_data)} sections...")
        
        for section_data in sections_data:
            new_section = ConfigSection(
                name=section_data['name'],
                code=section_data['code'],
                commands=section_data.get('commands', ''),
                config_id=config_to_use.id
            )
            db.session.add(new_section)
        
        # Import servers
        servers_data = data.get('servers', [])
        print(f"Importing {len(servers_data)} servers...")
        
        for server_data in servers_data:
            new_server = Server(
                alias=server_data['alias'],
                url=server_data['url'],
                is_default=server_data.get('is_default', False),
                config_id=config_to_use.id
            )
            db.session.add(new_server)

        common_events_data = data.get('CommonEvents', [])
        print(f"Importing {len(common_events_data)} common events.")

        for ev_data in common_events_data:
            new_event = ConfigEvent(
                event=ev_data['event'],
                listener=ev_data.get('listener', ''),
                config_id=config_to_use.id
            )
            db.session.add(new_event)

            for action_data in ev_data.get('actions', []):
                new_action = ConfigEventAction(
                    event_obj=new_event,
                    action=action_data.get('action', ''),
                    source=action_data.get('source', ''),
                    server=action_data.get('server', ''),
                    method=action_data.get('method', ''),
                    post_execute_method=action_data.get('postExecuteMethod', '')
                )
                db.session.add(new_action)    
        
        # CREATE/UPDATE THE SERVER HANDLERS FILE IF THERE ARE ANY
        if config_to_use.nodes_server_handlers:
            handlers_dir = os.path.join('Handlers', config_to_use.uid)
            os.makedirs(handlers_dir, exist_ok=True)
            handlers_file_path = os.path.join(handlers_dir, 'handlers.py')
            try:
                handlers_code = base64.b64decode(config_to_use.nodes_server_handlers).decode('utf-8')
                with open(handlers_file_path, 'w', encoding='utf-8') as f:
                    f.write(handlers_code)
                print(f"Created/updated server handlers file: {handlers_file_path}")
            except Exception as e:
                print(f"Error creating server handlers file: {str(e)}")
        
        # Updating the timestamp
        config_to_use.update_last_modified()
        
        db.session.commit()
        
        if is_update:
            print(f"Configuration updated successfully: {config_to_use.name}")
            flash(_('Configuration updated successfully'), 'success')
        else:
            print(f"Configuration imported successfully: {config_to_use.name}")
            flash(_('Configuration imported successfully'), 'success')
        
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        db.session.rollback()
        error_msg = f'Import error: {str(e)}'
        print(error_msg)
        traceback.print_exc()
        flash(_('Import error: {error}').format(error=str(e)), 'error')
        return redirect(url_for('dashboard'))



@app.route('/api/room/<room_uid>/task/<task_uid>', methods=['GET'])
@api_auth_required
def get_task(room_uid, task_uid):
    with SqliteDict(TASKS_DB_PATH) as tasks_db:
        tasks = tasks_db.get(room_uid, [])
        for i, task in enumerate(tasks):
            if task.get('uid') == task_uid:
                if task.get('_blocked'):
                    return jsonify({
                        'status': 'error',
                        'message': 'Task already blocked'
                    }), 400
                
                # Резервируем задачу
                tasks[i]['_blocked'] = True
                tasks[i]['_blocked_at'] = datetime.now().isoformat()
                tasks_db[room_uid] = tasks
                tasks_db.commit()
                
                # Отправляем обновление
                send_tasks_update(room_uid)
                
                return jsonify({
                    'status': 'success',
                    'task': task
                })
    
    return jsonify({'status': 'error', 'message': 'Task not found'}), 404

def handle_ws_command(room_uid, user, data, auth_success):
    command = data.get('type')
    if not auth_success:
        return

    if command == 'get_task':
        # Task reservation logic
        with app.app_context():  
            with SqliteDict(TASKS_DB_PATH) as tasks_db:
                tasks = tasks_db.get(room_uid, [])
                for task in tasks:
                    if task.get('uid') == data.get('task_uid') and not task.get('_blocked'):
                        task['_blocked'] = True
                        task['_blocked_by'] = user
                        task['_blocked_at'] = datetime.now().isoformat()
                        tasks_db[room_uid] = tasks
                        tasks_db.commit()
                        
                        ws = active_connections[room_uid].get(user)
                        if ws:
                            ws.send(json.dumps({
                                'type': 'task_assigned',
                                'task': task
                            }))
                        send_tasks_update(room_uid)
                        return
                        
                # If the task is not found
                ws = active_connections[room_uid].get(user)
                if ws:
                    ws.send(json.dumps({
                        'type': 'error',
                        'message': 'Task not available'
                    }))
    elif command == 'get_users':
        # Send a list of all connected users
        users_list = get_connected_users(room_uid)
        ws = active_connections[room_uid].get(user)
        if ws and not ws.closed:
            ws.send(json.dumps({
                'type': 'users_update',
                'users': users_list
            }))                
    elif command == 'acknowledge_objects':
        # The client confirms receipt of the objects
        object_ids = data.get('object_ids', [])
        
        with app.app_context():
            for obj_id in object_ids:
                room_object = db.session.get(RoomObjects, obj_id)
                if room_object and room_object.room_uid == room_uid:
                    # Add the user to the list of confirmed users
                    acknowledged = room_object.acknowledged_by or []
                    if user not in acknowledged:
                        acknowledged.append(user)
                        room_object.acknowledged_by = acknowledged
                        
                        # If all connected clients have confirmed, the object can be deleted
                        active_users = list(active_connections[room_uid].keys()) if room_uid in active_connections else []
                        if set(acknowledged) == set(active_users) and active_users:
                            db.session.delete(room_object)
            
            db.session.commit()
            
            # Send confirmation to the client
            ws = active_connections[room_uid].get(user)
            if ws:
                ws.send(json.dumps({
                    'type': 'acknowledgment_confirmed',
                    'object_ids': object_ids
                }))
    elif command == 'remote_method_response':
        # Processing the response from the remote method
        request_id = data.get('request_id')
        result_data = data.get('data', {})
        error = data.get('error')
        
        # Save the result for the corresponding query
        if request_id in pending_responses:
            pending_responses[request_id]['completed'] = True
            pending_responses[request_id]['result'] = result_data
            pending_responses[request_id]['error'] = error
    
    elif command == 'get_objects':
       # Client requests objects
        config_uid = data.get('config_uid')
        class_name = data.get('class_name')
        since = data.get('since')
        
        with app.app_context():
            query = RoomObjects.query.filter_by(room_uid=room_uid)
            

            
            objects = query.order_by(RoomObjects.created_at.desc()).all()
            
            ws = active_connections[room_uid].get(user)
            if ws:
                objects_data = []
                for obj in objects:
                    objects_data.append({
                        'config_uid': obj.config_uid,
                        'class_name': obj.class_name,
                        'objects': obj.objects_data,
                        'created_at': obj.created_at.isoformat()
                    })
                
                ws.send(json.dumps({
                    'type': 'objects_response',
                    'objects': objects_data
                }))
    # Minimal app: debug websocket messages disabled
@app.context_processor
def utility_processor():
    def safe_getattr(obj, attr, default=None):
        return getattr(obj, attr, default)
    return dict(safe_getattr=safe_getattr)

@app.before_request
def before_request():
    # Set the user's time zone (can be saved in the user settings)
    g.user_timezone = pytz.timezone('Europe/Moscow')  



@app.after_request
def update_config_timestamp(response):
    if request.endpoint in ['add_method', 'delete_method', 'edit_method', 
                          'add_event', 'edit_event', 'edit_class']:
        class_id = request.view_args.get('class_id')
        if class_id:
            class_obj = db.session.get(ConfigClass, class_id)
            if class_obj:
                class_obj.config.update_last_modified()
    return response    

def get_user_local_time():
    return datetime.now(g.user_timezone)

from ast import parse, FunctionDef, fix_missing_locations
import ast
import io


@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['GET'])
@api_auth_required
def get_dataset_items(uid, dataset_name):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    items = []
    for item in dataset.items:
        item_data = item.data.copy()  
        item_data['_id'] = item.item_id 
        items.append(item_data)
    return jsonify(items)

@app.route('/get-dataset-json')
@login_required
def get_dataset_json():
    dataset_id = request.args.get('dataset_id')
    dataset = db.session.get(Dataset, dataset_id)
    
    if not dataset or dataset.config.user_id != current_user.id:
        abort(404)
    
    return jsonify({
        'name': dataset.name,
        'hash_indexes': dataset.hash_indexes,
        'text_indexes': dataset.text_indexes,
        'view_template': dataset.view_template,
        'autoload': dataset.autoload
    })

@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['DELETE'])
@api_auth_required
def delete_all_dataset_items(uid, dataset_name):
    """Delete all records from the dataset"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    try:
        
        deleted_count = DatasetItem.query.filter_by(dataset_id=dataset.id).delete()
        db.session.commit()
        
        return jsonify({
            "status": "success", 
            "message": f"Deleted {deleted_count} items",
            "deleted_count": deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<uid>/dataset/<dataset_name>/items/<item_id>', methods=['DELETE'])
@api_auth_required
def delete_dataset_item(uid, dataset_name, item_id):
    """Delete a specific record from a dataset by ID"""
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    
    item = DatasetItem.query.filter_by(dataset_id=dataset.id, item_id=item_id).first()
    if not item:
        return jsonify({"error": "Item not found"}), 404
    
    try:
        db.session.delete(item)
        db.session.commit()
        
        return jsonify({
            "status": "success", 
            "message": f"Item {item_id} deleted"
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/<uid>/dataset/<dataset_name>/items', methods=['POST'])
@api_auth_required
def add_dataset_items(uid, dataset_name):
    config = db.session.execute(
        select(Configuration).where(Configuration.uid == uid)
    ).scalar_one_or_none()
    
    if not config:
        abort(404)
    
    dataset = next((d for d in config.datasets if d.name == dataset_name), None)
    if not dataset:
        abort(404)
    
    items = request.get_json()
    
    if not isinstance(items, list):
        abort(400, description="Request body must be a JSON array")
    
    for item in items:
        if not isinstance(item, dict):
            continue
            
        item_id = item.get('_id')
        if '_id' not in item:
            item['_id'] = str(uuid.uuid4())
            
        item_id = item['_id']
            
        # Check if item already exists
        existing_item = DatasetItem.query.filter_by(dataset_id=dataset.id, item_id=item_id).first()
        
        if existing_item:
            # Update existing item
            existing_item.data = item
            existing_item.updated_at = datetime.now(timezone.utc)
        else:
            # Create new item
            new_item = DatasetItem(
                dataset_id=dataset.id,
                item_id=item_id,
                data=item
            )
            db.session.add(new_item)
    
    db.session.commit()
    return jsonify({"status": "success", "count": len(items)})

if __name__ == '__main__':
    with app.app_context():
        db.create_all()


    # Create a custom WSGI server with WebSocket support
    def application(environ, start_response):
        path = environ.get('PATH_INFO', '')
        
        # Intercept WebSocket requests
        if path == '/ws' and 'wsgi.websocket' in environ:
            ws = environ['wsgi.websocket']
            query_string = environ.get('QUERY_STRING', '')
            parsed_params = parse_qs(query_string)

            room_uid = parsed_params.get('room', [''])[0]
            android_id = parsed_params.get('android_id', [''])[0]
            device_model = parsed_params.get('device_model', [''])[0]
            if room_uid:
                handle_websocket(ws, room_uid)
                return []
        
        # All other requests are processed through Flask
        return app(environ, start_response)
    
    server = WSGIServer(
        ('0.0.0.0', 5000),
        application,
        handler_class=WebSocketHandler
    )
    print("Server running on:")
    print("HTTP: http://0.0.0.0:5000")
    print("WebSocket: ws://0.0.0.0:5000/ws?room=ROOM_UID")

    server.serve_forever()