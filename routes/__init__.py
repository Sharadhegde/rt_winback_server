import imp
import json
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, engine_from_config
from flask import Blueprint

from .config import config

routes = Blueprint('routes', __name__)

try:
    """Load the config details for SQL server
    Create the cursor and connection objects
    """
    params = config('./routes/config.ini', 'mysql') 
    url = URL.create(**params)
    engine = create_engine(url, echo=True)
    conn = engine.connect()

except Exception as e:
    print(str(e))

from .rt_winback import *