# -*- coding: utf-8 -*-
"""Database models for the Noda editor/server application."""

import uuid
from datetime import datetime, timezone

from flask_login import UserMixin
from sqlalchemy import text

from extensions import db

def _normalize_special_method_name_for_json(value):
    value = str(value or "").strip()
    return "HTTPRequest" if value == "HTTP Request" else value

class RawNode(db.Model):
    __tablename__ = 'raw_node'

    id = db.Column(db.Integer, primary_key=True)
    node_id = db.Column(db.String(255), unique=True, nullable=False, index=True)
    payload_json = db.Column(db.JSON, nullable=False, default=dict)
    content_type = db.Column(db.String(64), nullable=False, default='node')
    owner_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

class NodeDiscussionMessage(db.Model):
    """Permanent history for node-discussion messages only.

    Delivery/pending/FCM continue to use OutgoingMessageLog and existing flows.
    This table is used only by /api/node-discussion/by-node/<node_id>/messages.
    """
    __tablename__ = 'node_discussion_message'

    id = db.Column(db.Integer, primary_key=True)
    node_id = db.Column(db.String(255), nullable=False, index=True)
    client_message_id = db.Column(db.String(128), unique=True, nullable=False, index=True)

    sender_user = db.Column(db.String(255), nullable=True, index=True)
    sender_display_name = db.Column(db.String(255), default='')

    target_type = db.Column(db.String(32), nullable=False, default='user')
    target_id = db.Column(db.String(255), nullable=False, index=True)

    text = db.Column(db.Text, default='')
    image = db.Column(db.Text)
    image_url = db.Column(db.Text)
    payload_json = db.Column(db.JSON, default=dict)
    delivery_status = db.Column(db.String(32), default='accepted')
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

def ensure_node_discussion_message_table_runtime():
    """Ensure permanent node-discussion history table exists after the model is defined."""
    try:
        with db.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS node_discussion_message (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id VARCHAR(255) NOT NULL,
                    client_message_id VARCHAR(128) NOT NULL UNIQUE,
                    sender_user VARCHAR(255),
                    sender_display_name VARCHAR(255) DEFAULT '',
                    target_type VARCHAR(32) NOT NULL DEFAULT 'user',
                    target_id VARCHAR(255) NOT NULL,
                    text TEXT DEFAULT '',
                    image TEXT,
                    image_url TEXT,
                    payload_json JSON,
                    delivery_status VARCHAR(32) DEFAULT 'accepted',
                    created_at DATETIME
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_node_id ON node_discussion_message(node_id)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_node_discussion_message_client_message_id ON node_discussion_message(client_message_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_created_at ON node_discussion_message(created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_node_discussion_message_target ON node_discussion_message(target_type, target_id)"))
    except Exception as e:
        print('Could not ensure node_discussion_message table at runtime:', e)

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
    transport = db.Column(db.String(30), default='websocket')
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

class RoomDevice(db.Model):
    __tablename__ = 'room_device'

    id = db.Column(db.Integer, primary_key=True)
    room_uid = db.Column(db.String(36), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)
    device_uid = db.Column(db.String(120), nullable=False, index=True)
    user_key = db.Column(db.String(200), default='', nullable=False, index=True)
    push_channel = db.Column(db.String(30), default='websocket', nullable=False)
    fcm_token = db.Column(db.Text, default='')
    android_id = db.Column(db.String(100), default='')
    device_model = db.Column(db.String(200), default='')
    extra_json = db.Column(db.JSON, default=dict)
    last_seen = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('room_uid', 'device_uid', name='uq_room_device_room_uid_device_uid'),
        db.Index('idx_room_device_room_channel', 'room_uid', 'push_channel'),
    )

class RoomAlias(db.Model):
    """Room aliases bound to a configuration.

    Used by the web-client migration/registration commands.
    Stores mapping: alias -> Room.uid
    """

    __tablename__ = 'room_alias'

    id = db.Column(db.Integer, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    room_uid = db.Column(db.String(36), nullable=False, default="")

    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='CASCADE'), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('config_id', 'alias', name='uq_room_alias_config_alias'),
        db.Index('idx_room_alias_config', 'config_id'),
    )

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
    config_display_name = db.Column(db.String(100), default="")

    # Access flags
    can_designer = db.Column(db.Boolean, default=False)  # Configurator/Designer
    can_client = db.Column(db.Boolean, default=False)    # Web Client
    can_api = db.Column(db.Boolean, default=False)       # HTTP API (basic auth)

    # User who created/owns this account ("admin" scope)
    parent_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    parent_user = db.relationship('User', remote_side=[id], backref=db.backref('children', lazy=True))

    configurations = db.relationship('Configuration', backref='user', lazy=True)

class UserConfigAccess(db.Model):
    __tablename__ = 'user_config_access'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False, index=True)
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='CASCADE'), nullable=False, index=True)

    user = db.relationship('User', backref=db.backref('config_access', cascade='all, delete-orphan', lazy=True))
    config = db.relationship('Configuration', backref=db.backref('user_access', cascade='all, delete-orphan', lazy=True))

class UserDevice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    device_uid = db.Column(db.String(120), default='', nullable=False, index=True)
    android_id = db.Column(db.String(100), nullable=False, index=True)
    device_model = db.Column(db.String(200))
    token = db.Column(db.Text)
    extra_json = db.Column(db.JSON, default=dict)
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
        """Converts event actions into dictionaries for JSON serialization"""
        result = []
        for action in self.actions:
            action_dict = {
                "action": action.action,
                "method": _normalize_special_method_name_for_json(action.method),
                "source": action.source,
                "server": action.server,
                "postExecuteMethod": _normalize_special_method_name_for_json(action.post_execute_method),
                "methodText": action.method_text,
                "postExecuteMethodText": action.post_execute_text,
                "httpFunctionName": action.http_function_name,
                "postHttpFunctionName": action.post_http_function_name,
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
    # NodaScript stores script text here; PythonScript stores S3 URL here
    method_text = db.Column(db.Text, default="")
    post_execute_text = db.Column(db.Text, default="")
    http_function_name = db.Column(db.String(255), default="")
    post_http_function_name = db.Column(db.String(255), default="")
    order = db.Column(db.Integer, default=0)  

    event_id = db.Column(db.Integer, db.ForeignKey('config_event.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "action": self.action,
            "source": self.source,
            "server": self.server,
            "method": _normalize_special_method_name_for_json(self.method),
            "postExecuteMethod": _normalize_special_method_name_for_json(self.post_execute_method),
            "methodText": self.method_text,
            "postExecuteMethodText": self.post_execute_text,
            "httpFunctionName": self.http_function_name,
            "postHttpFunctionName": self.post_http_function_name,
            "order": self.order,
        }


class ConfigTimer(db.Model):
    __tablename__ = 'config_timer'
    id = db.Column(db.Integer, primary_key=True)
    timer_id = db.Column(db.String(100), nullable=False)  # user-visible ID
    period_seconds = db.Column(db.Integer, default=900, nullable=False)
    active = db.Column(db.Boolean, default=True, nullable=False)
    worker = db.Column(db.Boolean, default=False, nullable=False)
    runtime = db.Column(db.String(20), default='server', nullable=False)  # server/client
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id', ondelete='CASCADE'), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    actions = db.relationship('ConfigTimerAction', backref='timer_obj', cascade='all, delete-orphan', order_by='ConfigTimerAction.id')

    __table_args__ = (
        db.UniqueConstraint('config_id', 'timer_id', name='uq_config_timer_config_id_timer_id'),
        db.Index('idx_config_timer_config_active', 'config_id', 'active'),
    )

    def actions_as_dicts(self):
        result = []
        for action in self.actions:
            action_dict = {
                "action": action.action,
                "method": _normalize_special_method_name_for_json(action.method),
                "source": action.source,
                "server": action.server,
                "postExecuteMethod": _normalize_special_method_name_for_json(action.post_execute_method),
                "methodText": action.method_text,
                "postExecuteMethodText": action.post_execute_text,
                "httpFunctionName": action.http_function_name,
                "postHttpFunctionName": action.post_http_function_name,
                "order": action.order,
            }
            action_dict = {k: v for k, v in action_dict.items() if v is not None and v != ""}
            result.append(action_dict)
        return result

    def to_dict(self):
        runtime = str(getattr(self, "runtime", "") or "server").strip().lower()
        if runtime not in {"server", "client"}:
            runtime = "server"
        worker = bool(self.worker)
        try:
            period_seconds = int(getattr(self, "period_seconds", 0) or 0)
        except Exception:
            period_seconds = 0
        min_period_seconds = 900 if runtime == "server" or worker else 1
        period_seconds = max(min_period_seconds, period_seconds)
        return {
            "id": self.timer_id,
            "timer_id": self.timer_id,
            "period_seconds": period_seconds,
            "active": bool(self.active),
            "worker": worker,
            "runtime": runtime,
            "actions": self.actions_as_dicts(),
        }

class ConfigTimerAction(db.Model):
    __tablename__ = 'config_timer_action'
    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(50), default='run', nullable=False)   # run, runprogress, runasync
    source = db.Column(db.String(50), default='internal', nullable=False)
    server = db.Column(db.String(255), default="")
    method = db.Column(db.String(200), default="")
    post_execute_method = db.Column(db.String(200), default="")
    # NodaScript stores script text here; PythonScript stores S3 URL here
    method_text = db.Column(db.Text, default="")
    post_execute_text = db.Column(db.Text, default="")
    http_function_name = db.Column(db.String(255), default="")
    post_http_function_name = db.Column(db.String(255), default="")
    order = db.Column(db.Integer, default=0)

    timer_id = db.Column(db.Integer, db.ForeignKey('config_timer.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "action": self.action,
            "source": self.source,
            "server": self.server,
            "method": _normalize_special_method_name_for_json(self.method),
            "postExecuteMethod": _normalize_special_method_name_for_json(self.post_execute_method),
            "methodText": self.method_text,
            "postExecuteMethodText": self.post_execute_text,
            "httpFunctionName": self.http_function_name,
            "postHttpFunctionName": self.post_http_function_name,
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
    room_aliases = db.relationship('RoomAlias', backref='config', cascade='all, delete-orphan')
    config_events = db.relationship('ConfigEvent', backref='config', cascade='all, delete-orphan')
    config_timers = db.relationship('ConfigTimer', backref='config', cascade='all, delete-orphan', order_by='ConfigTimer.id')
    common_layouts = db.Column(db.JSON, default=list)
    
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
    # Projection settings. Projection classes are singleton UI processes in the web client.
    projection_type = db.Column(db.String(50), default="")
    projection_kanban_columns = db.Column(db.Text, default="")

    # PrintForm settings. PrintForm classes do not store their own persistent data;
    # web client creates an ephemeral runtime node, injects _basement_data,
    # runs onInputWeb(listener=onStartForm), then renders this template.
    print_template_type = db.Column(db.String(50), default="html_jinja")
    print_target_classes = db.Column(db.JSON, default=list)
    print_html_template = db.Column(db.Text, default="")
    display_name = db.Column(db.String(100))  
    record_view = db.Column(db.Text, default="")
    cover_image = db.Column(db.Text)  
    section = db.Column(db.String(100))  
    section_code = db.Column(db.String(100)) 
    methods = db.relationship('ClassMethod', backref='class_obj', cascade='all, delete-orphan')
    events = db.Column(db.JSON, default={})
    hidden = db.Column(db.Boolean, default=False)
    event_objs = db.relationship('ClassEvent', backref='class_obj', cascade='all, delete-orphan')
    # Display-related images / layouts
    display_image_web = db.Column(db.Text, default="")
    display_image_table = db.Column(db.Text, default="")
    init_screen_layout = db.Column(db.Text, default="")
    init_screen_layout_web = db.Column(db.Text, default="")
    data_structure = db.Column(db.Text, default="")
    show_tag_cloud = db.Column(db.Boolean, default=False)
    mobile_print_enabled = db.Column(db.Boolean, default=False)

    # PlugIn UI (mobile/web)
    plug_in = db.Column(db.Text, default="")
    plug_in_web = db.Column(db.Text, default="")

    # Commands UI (string formats described in UI hints)
    commands = db.Column(db.Text, default="")
    use_standard_commands = db.Column(db.Boolean, default=True)
    svg_commands = db.Column(db.Text, default="")

    # Migration / registration helpers (used by web-client)
    migration_register_command = db.Column(db.Boolean, default=False)
    migration_register_on_save = db.Column(db.Boolean, default=False)
    migration_send_via_queue = db.Column(db.Boolean, default=False)
    # Stores Room.uid (string)
    migration_default_room_uid = db.Column(db.String(36), default="")
    # Stores RoomAlias.alias (string)
    migration_default_room_alias = db.Column(db.String(100), default="")
    # How the class should be shared by link: share_link / package_class
    link_share_mode = db.Column(db.String(30), default="")
    indexes_json = db.Column(db.JSON, default=list)

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
        """Converts event actions into dictionaries for JSON serialization"""
        result = []
        for action in self.actions:
            action_dict = {
                "action": action.action,
                "method": _normalize_special_method_name_for_json(action.method),
                "source": action.source,
                "server": action.server,
                "postExecuteMethod": _normalize_special_method_name_for_json(action.post_execute_method),
                "methodText": action.method_text,
                "postExecuteMethodText": action.post_execute_text,
                "httpFunctionName": action.http_function_name,
                "postHttpFunctionName": action.post_http_function_name,
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
    # NodaScript stores script text here; PythonScript stores S3 URL here
    method_text = db.Column(db.Text, default="")
    post_execute_text = db.Column(db.Text, default="")
    http_function_name = db.Column(db.String(255), default="")
    post_http_function_name = db.Column(db.String(255), default="")
    order = db.Column(db.Integer, default=0)  

    event_id = db.Column(db.Integer, db.ForeignKey('class_event.id', ondelete='CASCADE'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "action": self.action,
            "source": self.source,
            "server": self.server,
            "method": _normalize_special_method_name_for_json(self.method),
            "postExecuteMethod": _normalize_special_method_name_for_json(self.post_execute_method),
            "methodText": self.method_text,
            "postExecuteMethodText": self.post_execute_text,
            "httpFunctionName": self.http_function_name,
            "postHttpFunctionName": self.post_http_function_name,
            "order": self.order,
        }

class Contract(db.Model):
    __tablename__ = 'contract'

    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()), unique=True, nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False, index=True)

    name = db.Column(db.String(100), nullable=False)
    display_name = db.Column(db.String(100), default="")

    # class / global_index / external_only
    source_type = db.Column(db.String(30), default='class', nullable=False)
    source_config_uid = db.Column(db.String(36), default="", index=True)
    class_name = db.Column(db.String(100), default="", index=True)
    # Multi-class contract source. List of {config_uid, class_name}.
    # source_config_uid/class_name are kept for backward compatibility and UI fallbacks.
    source_classes_json = db.Column(db.JSON, nullable=True)
    global_index_name = db.Column(db.String(100), default="")
    global_index_value = db.Column(db.String(255), default="")

    # Last externally provided class JSON, used by POST-only contracts and as an override when needed
    external_class_json = db.Column(db.JSON, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    owner = db.relationship('User', backref=db.backref('contracts', cascade='all, delete-orphan', lazy=True))
    pushed_objects = db.relationship('ContractObject', backref='contract', cascade='all, delete-orphan', lazy=True)
    acknowledgements = db.relationship('ContractAck', backref='contract', cascade='all, delete-orphan', lazy=True)

class ContractObject(db.Model):
    __tablename__ = 'contract_object'

    id = db.Column(db.Integer, primary_key=True)
    contract_id = db.Column(db.Integer, db.ForeignKey('contract.id', ondelete='CASCADE'), nullable=False, index=True)
    object_id = db.Column(db.String(255), nullable=False)
    payload_json = db.Column(db.JSON, nullable=False)
    object_version = db.Column(db.String(120), default="", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('contract_id', 'object_id', name='uq_contract_object_contract_object_id'),
        db.Index('idx_contract_object_contract', 'contract_id'),
    )

class ContractAck(db.Model):
    __tablename__ = 'contract_ack'

    id = db.Column(db.Integer, primary_key=True)
    contract_id = db.Column(db.Integer, db.ForeignKey('contract.id', ondelete='CASCADE'), nullable=False, index=True)
    device_id = db.Column(db.String(120), nullable=False, index=True)
    object_id = db.Column(db.String(255), nullable=False)
    object_version = db.Column(db.String(120), default="", nullable=False)
    acked_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('contract_id', 'device_id', 'object_id', name='uq_contract_ack_contract_device_object'),
        db.Index('idx_contract_ack_lookup', 'contract_id', 'device_id'),
    )

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

class OutgoingMessageLog(db.Model):
    __tablename__ = 'outgoing_message_log'

    id = db.Column(db.Integer, primary_key=True)
    client_message_id = db.Column(db.String(128), unique=True, nullable=False, index=True)
    sender_user = db.Column(db.String(255), nullable=True, index=True)
    target_type = db.Column(db.String(32), nullable=False, index=True)   # user / device
    target_id = db.Column(db.String(255), nullable=False, index=True)
    title = db.Column(db.String(255), nullable=True)
    body = db.Column(db.Text, nullable=True)
    payload_json = db.Column(db.JSON, nullable=True)
    status = db.Column(db.String(32), default='queued', nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    accepted_at = db.Column(db.DateTime, nullable=True)
    pushed_at = db.Column(db.DateTime, nullable=True)
    ack_at = db.Column(db.DateTime, nullable=True)
    ack_by = db.Column(db.String(255), nullable=True)
    ack_payload = db.Column(db.JSON, nullable=True)
    last_error = db.Column(db.Text, nullable=True)

class OutgoingMessageDeviceAck(db.Model):
    __tablename__ = 'outgoing_message_device_ack'

    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.Integer, db.ForeignKey('outgoing_message_log.id'), nullable=False, index=True)
    client_message_id = db.Column(db.String(128), nullable=True, index=True)
    user_key = db.Column(db.String(255), nullable=True, index=True)
    device_uid = db.Column(db.String(120), nullable=False, index=True)
    ack_at = db.Column(db.DateTime, nullable=True)
    ack_by = db.Column(db.String(255), nullable=True)
    ack_payload = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('message_id', 'device_uid', name='uq_outgoing_message_device_ack_message_device'),
        db.Index('idx_outgoing_message_device_ack_user_device', 'user_key', 'device_uid'),
    )

class MessageGroup(db.Model):
    __tablename__ = 'message_group'

    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.String(64), unique=True, nullable=False, index=True)
    title = db.Column(db.String(255), nullable=False)
    created_by = db.Column(db.String(255), nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

class MessageGroupMember(db.Model):
    __tablename__ = 'message_group_member'

    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.String(64), db.ForeignKey('message_group.group_id', ondelete='CASCADE'), nullable=False, index=True)
    user_key = db.Column(db.String(255), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    group = db.relationship(
        'MessageGroup',
        backref=db.backref('memberships', cascade='all, delete-orphan', lazy=True),
        primaryjoin='MessageGroupMember.group_id == MessageGroup.group_id',
    )

    __table_args__ = (
        db.UniqueConstraint('group_id', 'user_key', name='uq_message_group_member_group_user'),
        db.Index('ix_message_group_member_user_group', 'user_key', 'group_id'),
    )

class Server(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alias = db.Column(db.String(100), nullable=False)
    url = db.Column(db.String(255), nullable=False)
    config_id = db.Column(db.Integer, db.ForeignKey('configuration.id'))
    is_default = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

class ApiToken(db.Model):
    __tablename__ = "api_token"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    token = db.Column(db.String(128), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    revoked_at = db.Column(db.DateTime, nullable=True)

    user = db.relationship("User", backref="api_tokens")

__all__ = [
    'RawNode',
    'NodeDiscussionMessage',
    'Dataset',
    'DatasetItem',
    'Room',
    'RoomDevice',
    'RoomAlias',
    'User',
    'UserConfigAccess',
    'UserDevice',
    'ConfigEvent',
    'ConfigEventAction',
    'ConfigTimer',
    'ConfigTimerAction',
    'Configuration',
    'ConfigSection',
    'ConfigClass',
    'ClassMethod',
    'ClassEvent',
    'EventAction',
    'Contract',
    'ContractObject',
    'ContractAck',
    'RoomObjects',
    'OutgoingMessageLog',
    'OutgoingMessageDeviceAck',
    'MessageGroup',
    'MessageGroupMember',
    'Server',
    'ApiToken',
    'ensure_node_discussion_message_table_runtime',
]
