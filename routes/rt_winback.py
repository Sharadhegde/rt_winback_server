import imp
import json
from datetime import datetime

from .helpers.lec_check import *
from .helpers.db_manager import DBManager
from . import routes, VERSION, logger

@routes.route('/launchrtwinback/', methods=['GET'])
def launch_rt_winback():
    try:
        db_manager = DBManager()
        orderBtnList = db_manager.get_all_orders()
        start_time = datetime.now().replace(microsecond=0)
        logger.warning("VERSION:" + VERSION + " ---------------Starting new session--------------- on: " + str(start_time))

        leads = Enhance_Carrier_Info(field=1)
        leads.mt_process_data(orderBtnList)
        time_taken = datetime.now().replace(microsecond=0) - start_time

        logger.warning("---------------Ending  session--Time taken: " + str(time_taken))
        logger.warning("--------Time taken----: " + str(time_taken))
        json_result = {
            "Result": leads.carrier_info_list,
        }
        
    except Exception as e:
        logger.error(e)
        logger.error('Exception thrown:' + str(e))
        json_result = {
            "Result": "Failed!",
        }
    
    return json.dumps(json_result)
