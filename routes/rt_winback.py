import imp
import json
from datetime import datetime
from flask import request

from .helpers.lec_check import *
from .helpers.db_manager import DBManager
from . import routes, VERSION, logger

@routes.route('/launchrtwinback/', methods=['GET'])
def launch_rt_winback():
    try:
        start_time = datetime.now().replace(microsecond=0)
        logger.warning("VERSION:" + VERSION + " ---------------Starting new session--------------- on: " + str(start_time))
        
        client_ip = ""
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            client_ip = request.environ['REMOTE_ADDR']
        else:
            client_ip = request.environ['HTTP_X_FORWARDED_FOR']

        logger.info("Client IP: " + client_ip)
        db_manager = DBManager()
        orderBtnList, input_cb, new_cb = db_manager.get_all_orders()

        leads = Enhance_Carrier_Info(field=1)
        leads.mt_process_data(orderBtnList)

        db_manager.set_params(client_ip=client_ip, input_cb=input_cb, new_cb=new_cb)
        commit_to_db('routes/results/carrier_info_20220417-132149.csv', 'routes/results/disconnected_orders_20220417-132149.csv', db_manager)
        
        time_taken = datetime.now().replace(microsecond=0) - start_time

        logger.warning("---------------Ending  session--Time taken: " + str(time_taken))
        logger.warning("--------Time taken----: " + str(time_taken))
        json_result = {
            "Result": orderBtnList,
        }
        
    except Exception as e:
        logger.error('Exception thrown:' + str(e))
        json_result = {
            "Result": "Failed!",
        }
    
    return json.dumps(json_result)

def commit_to_db(results_filename1, results_filename2, db_manager):
    try:
        db_manager.write_to_db_carrier_info(results_filename1)
        db_manager.write_to_db_disconnected(results_filename2)
        
    except Exception as e:
        logger.error('Exception thrown:' + str(e))
