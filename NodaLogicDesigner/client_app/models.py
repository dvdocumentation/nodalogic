from __future__ import annotations

from datetime import datetime, timezone

from extensions import db

    
class Repo(db.Model):
    """User-selected configuration link + optional API overrides.

    Stored in a separate SQLite bind (client.sqlite) to keep client data isolated
    from the Designer DB.
    """

    __bind_key__ = "client"
    __tablename__ = "client_repo"

    id = db.Column(db.Integer, primary_key=True)

    # We intentionally do NOT add a foreign key to the main `user` table,
    # because it lives in a different bind.
    user_id = db.Column(db.Integer, nullable=False, index=True)

    config_url = db.Column(db.String(500), nullable=False)
    config_uid = db.Column(db.String(64), nullable=False, index=True)

    base_url = db.Column(db.String(300), default="")
    username = db.Column(db.String(100), default="")
    password = db.Column(db.String(100), default="")

    name = db.Column(db.String(200), default="")
    vendor = db.Column(db.String(200), default="")
    version = db.Column(db.String(50), default="")
    display_name = db.Column(db.String(200), default="")

    # Legacy cache (kept for compatibility)
    config_json = db.Column(db.Text, default="")
    config_cached_at = db.Column(db.DateTime, nullable=True)


class RepoConfig(db.Model):
    """Dedicated cache table used by the client for fast startup + mem-cache stamp."""

    __bind_key__ = "client"
    __tablename__ = "client_repo_config"

    id = db.Column(db.Integer, primary_key=True)
    repo_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    config_json = db.Column(db.Text, nullable=False)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class ClientSetting(db.Model):
    """Per-user client settings (stored in client.sqlite bind)."""

    __bind_key__ = "client"
    __tablename__ = "client_setting"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    key = db.Column(db.String(120), nullable=False, index=True)
    value = db.Column(db.Text, default="")

    __table_args__ = (
        db.UniqueConstraint("user_id", "key", name="uq_client_setting_user_key"),
    )