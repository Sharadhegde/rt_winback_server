from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, engine_from_config
from flask import Blueprint
import logging
from datetime import datetime

from .config import config

routes = Blueprint('routes', __name__)

try:
    """Load the config details for SQL server
    Create the cursor and connection objects
    """
    mysql_params = config('./routes/config.ini', 'mysql_prod') 
    mysql_url = URL.create(**mysql_params)
    mysql_engine = create_engine(mysql_url, echo=True)
    mysql_conn = mysql_engine.connect()

    postgresql_params = config('./routes/config.ini', 'postgresql') 
    postgresql_url = URL.create(**postgresql_params)
    postgresql_engine = create_engine(postgresql_url, echo=True)
    postgresql_conn = postgresql_engine.connect()

    today = datetime.today().date().strftime("%Y-%m-%d")
    logging.basicConfig(filename='routes/logs/whitepages-' + today + '.log', level=logging.WARNING)
    logger = logging.getLogger(__name__)
    VERSION = today
    
except Exception as e:
    print(str(e))

from .rt_winback import *