import requests
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

import sys
import time
import os
import json
import logging
from datetime import datetime
import threading
wplock = threading.Lock()

from .helpers.db_manager import DBManager
from .helpers.utils import Utils

today = datetime.today().date().strftime("%Y-%m-%d")
logging.basicConfig(filename='routes/logs/whitepages-' + today + '.log', level=logging.WARNING)
logger = logging.getLogger(__name__)
VERSION = today

class LecCheck:
    API_KEY="17ce13d95a3f159af64f6cdcdf99b4fa";
    TRIAL_KEY="1d75fe84defe49809ba135fe195d3b27";

    DLM=','

    def __init__(self):
        self.successful_requests = 0
        self.failed_requests = 0
        
    def get_carrier_info(self, phoneNumber):
        util = Utils()
        phoneNumber = util.unformat_phonenum(phoneNumber)
        if (util.validate_phonenum(phoneNumber) == False):
            self.failed_requests += 1
            logger.warning('Invalid Phone Number:' + str(phoneNumber))
            return "Invalid phone number" + str(phoneNumber)

        #uri="https://proapi.whitepages.com/3.0/phone.json?phone="+phoneNumber+"&api_key="+whitepages.API_KEY;
        uri = "https://api.ekata.com/3.0/phone.json?phone=" + phoneNumber + "&api_key=" + LecCheck.API_KEY

        response = requests.get(uri)
        self.successful_requests = self.successful_requests + 1
        parsed_response = self.parse_response(response.json())
        logger.debug(parsed_response)
        response.close()
        return parsed_response

    ''' parse the json response into a csv string '''
    def parse_response(self, json_str):
        results = json_str 
        csv_retval = results.get('phone_number') + LecCheck.DLM \
                    + str(json_str.get('carrier')).replace(',',' ')
        logger.info(csv_retval)
        return csv_retval

    #returns and prints the number of successful and failed requests.
    def get_stats(self):
        logger.warn("Successful Requests:" + str(self.successful_requests) )
        logger.warn("Failed Requests:"+str(self.failed_requests))
        return self.successful_requests, self.failed_requests

class Enhance_Carrier_Info:
    def __init__(self, field):
        self.field_num = field
        logger.info('The phone number field is configured as :' + str(field))
        self.whitepages_ref = LecCheck()
        self.db_manager_ref = DBManager()
        self.input_list = []

    def set_phonenum_field(self,  field_num):
        self.field_num = field_num

    #TODO only used for testing.
    def dummyworker(self, leads_list):
        for item in leads_list:
            self.db_manager_ref.write_filtered_leads(str(len(item)) + ":" + str(item))

    def worker(self, leads_list):
        logger.info('worker thread:')
        for item in leads_list:
            fields = item.split(',')
            try:
                wplock.acquire()
                phone_number = fields[self.field_num]
                if phone_number != 'phone_number' and phone_number != 'btn' and phone_number != 'Phone Number':
                    carrier_info = self.whitepages_ref.get_carrier_info(phone_number)
                    item = item.strip() + ',' + carrier_info
                    items = item.split(',')
                    carrier_info = carrier_info.split(',')
                    wplock.release()

            except Exception as e:
                logger.error(e)
                self.whitepages_ref.failed_requests = self.whitepages_ref.failed_requests + 1
                logger.error('Exception thrown:' + str(e))
                wplock.release()
                continue

            try:
                if(self.is_not_disconnected(items[2], carrier_info[1])):
                    item = items[0] + ',' + today + ',' + items[2] 
                    self.db_manager_ref.write_filtered_leads(item, False)
                else:
                    item = items[0] + ',' + today + ',' + items[4] 
                    self.db_manager_ref.write_filtered_leads(item, True)
            except:
                continue

    def is_not_disconnected(self, campaign, carrier):
        with open('./routes/helpers/carrier_mapping.json') as json_data:
            carrier_dict = json.load(json_data)
        for key, val in carrier_dict.items():
            if key.lower() == campaign.lower():
                if carrier in val:
                    return True
        return False

    def mt_process_data(self, results, statusFunc=None):
        util = Utils()
        self.input_list = results

        threads = []
        subset = util.chunks(self.input_list, 1000)
        for l in subset:
            t = threading.Thread(target=self.worker, args=([l]) )
            threads.append(t)
            t.start()

        for t in threads:
            logger.info ("joining threads")
            t.join()

        logger.info ('committing results')
        print("Committing")
        self.db_manager_ref.commit(self.input_list)
        if statusFunc != None:
            return statusFunc('Finished processing LEC check orders')

