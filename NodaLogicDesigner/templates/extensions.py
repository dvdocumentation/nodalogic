from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

db = SQLAlchemy()
login_manager = LoginManager()
print("EXTENSIONS LOADED:", __name__, __file__, id(db))