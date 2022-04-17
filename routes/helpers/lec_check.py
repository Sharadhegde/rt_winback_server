import requests
import requests.packages.urllib3
import time
import json
import threading

requests.packages.urllib3.disable_warnings()
wplock = threading.Lock()

from .db_manager import DBManager
from .utils import Utils
from routes import logger, today

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

        # uri="https://proapi.whitepages.com/3.0/phone.json?phone="+phoneNumber+"&api_key="+whitepages.API_KEY;
        uri = "https://api.ekata.com/3.0/phone.json?phone=" + phoneNumber + "&api_key=" + LecCheck.API_KEY

        response = requests.get(uri)
        self.successful_requests = self.successful_requests + 1
        parsed_response = self.parse_response(response.json())
        logger.debug(parsed_response)
        response.close()
        return parsed_response

    def parse_response(self, json_str):
        results = json_str
        address_str = self.parse_address(json_str)
        csv_retval = results.get('phone_number') + LecCheck.DLM \
                    + address_str + LecCheck.DLM \
                    + str(json_str.get('carrier')).replace(',',' ') + LecCheck.DLM \
                    + json_str.get('line_type') + LecCheck.DLM \
                    + time.strftime("%x") + LecCheck.DLM + ""

        logger.info(csv_retval)
        return csv_retval

    def parse_address(self, json_str):
        address_str = ''
        address_list = json_str.get('current_addresses')
        address = address_list[0]

        address_str += str(address.get('street_line_1')) + str(address.get('street_line_2'))
        address_str += LecCheck.DLM + str(address.get('city'))
        address_str += LecCheck.DLM + str(address.get('state_code'))
        address_str += LecCheck.DLM + str(address.get('postal_code'))
        address_str += LecCheck.DLM + str(address.get('lat_long')['latitude'])
        address_str += LecCheck.DLM + str(address.get('lat_long')['longitude'])

        return address_str

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
        self.carrier_info_list = []

    def set_phonenum_field(self, field_num):
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
                phone_number = fields[self.field_num]
                if phone_number != 'phone_number' and phone_number != 'btn' and phone_number != 'Phone Number':
                    carrier_info = self.whitepages_ref.get_carrier_info(phone_number)
                    result = item.strip() + ',' + carrier_info
                    self.carrier_info_list.append(result)

            except Exception as e:
                logger.error(e)
                self.whitepages_ref.failed_requests = self.whitepages_ref.failed_requests + 1
                logger.error('Exception thrown:' + str(e))
                continue

    def is_not_disconnected(self, campaign, carrier):
        with open('./routes/helpers/carrier_mapping.json') as json_data:
            carrier_dict = json.load(json_data)
        for key, val in carrier_dict.items():
            if key.lower() == campaign.lower():
                if carrier in val:
                    return True
        return False
    
    def split_orders(self):
        for i in range(len(self.carrier_info_list)):
            try:
                row = self.carrier_info_list[i].split(',')
                if(self.is_not_disconnected(row[2], row[10])):
                    item = row[0] + ',' + today + ',' + row[2] 
                    self.db_manager_ref.write_filtered_leads(item, False)
                else:
                    item = row[0] + ',' + today + ',' + row[10] 
                    self.db_manager_ref.write_filtered_leads(item, True)
            except:
                continue

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
        
        self.split_orders()
        logger.info ('committing results')
        print("Committing")
        self.db_manager_ref.commit_carrier_info(self.carrier_info_list)
        self.db_manager_ref.commit_disconnected_orders(self.carrier_info_list)
        if statusFunc != None:
            return statusFunc('Finished processing LEC check orders')

