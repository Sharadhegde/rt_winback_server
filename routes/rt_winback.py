from crypt import methods
import imp
import json
from unittest import result
from . import routes, conn
from flask import jsonify, make_response, abort, request
from datetime import datetime
import logging
import pandas as pd

from .lec_check import *
from .helpers.db_manager import DBManager

today = datetime.today().date().strftime("%Y-%m-%d")
logging.basicConfig(filename='routes/logs/whitepages-' + today + '.log', level=logging.WARNING)
logger = logging.getLogger(__name__)
VERSION = today

@routes.route('/launchrtwinback/', methods=['GET'])
def launch_rt_winback():
    try:
        db_manager = DBManager()
        orderBtnList = db_manager.get_all_orders()
        start_time = datetime.now().replace(microsecond=0)
        logger.warning("VERSION:" + VERSION + " ---------------Starting new session--------------- on: " + str(start_time))

        # leads = Enhance_Carrier_Info(field=1)
        # leads.mt_process_data(orderBtnList)
        time_taken = datetime.now().replace(microsecond=0) - start_time

        logger.warning("---------------Ending  session--Time taken: " + str(time_taken))
        logger.warning("--------Time taken----: " + str(time_taken))
        json_result = {
            "Result": orderBtnList,
        }
        
    except Exception as e:
        logger.error(e)
        logger.error('Exception thrown:' + str(e))
        json_result = {
            "Result": "Failed!",
        }
    
    return json.dumps(json_result)