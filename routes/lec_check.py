import requests
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

import sys
import time
import os
import logging
from datetime import datetime
import threading
wplock = threading.Lock()

from db_manager import DBManager
from util import Utils

today = datetime.today().date().strftime("%Y%m%d")
logging.basicConfig(filename='logs/whitepages-' + today + '.log', level=logging.WARNING)
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
        uri = "https://api.ekata.com/3.0/phone.json?phone=" + phoneNumber + "&api_key=" + LecCheck.API_KEY;

        response = requests.get(uri)
        self.successful_requests = self.successful_requests+1
        parsed_response = self.parse_response(response.json())
        parsed_response = self.parse_business_info(response.json(),parsed_response)
        logger.debug(parsed_response)
        response.close()
        return parsed_response

    def parse_business_info(self, json_str, csv_str):
        # info_list=json_str.get('belongs_to')
        # business_info=info_list[0]
        # csv_str=csv_str+whitepages.DLM+str(business_info.get('name'))
        # if business_info.get('firstname') is not None :
        #     csv_str=csv_str+whitepages.DLM+str(business_info.get('firstname'))
        # if business_info.get('lastname') is not None :
        #     csv_str=csv_str+whitepages.DLM+str(business_info.get('lastname'))

        address_list = json_str.get('current_addresses')
        address = address_list[0]
        csv_str = csv_str + LecCheck.DLM + str(address.get('street_line_1'))
        csv_str = csv_str + LecCheck.DLM + str(address.get('street_line_2'))

        csv_str = csv_str + LecCheck.DLM + str(address.get('city'))
        csv_str = csv_str + LecCheck.DLM + str(address.get('state_code'))
        csv_str = csv_str + LecCheck.DLM + str(address.get('postal_code'))

        logger.info(csv_str)
        return csv_str

    ''' parse the json response into a csv string '''
    def parse_response(self, json_str):
        results = json_str 
        csv_retval = results.get('phone_number') + LecCheck.DLM \
                    + str(json_str.get('carrier')).replace(',',' ') + LecCheck.DLM \
                    + json_str.get('line_type') + LecCheck.DLM \
                    +'dnc=' + str(json_str.get('do_not_call')) + LecCheck.DLM \
                    + 'valid=' + str(json_str.get('is_valid')) + LecCheck.DLM \
                    +'connected=' + str(json_str.get('is_connected')) + LecCheck.DLM \
                    + "prepaid=" + str(json_str.get('is_prepaid')) + LecCheck.DLM \
                    + "is_commerical="+str(json_str.get('is_commerical'))+ LecCheck.DLM \
                    + "belongs_to="+str(json_str.get('belongs_to'))+ LecCheck.DLM \
                    + "current_addresses="+str(json_str.get('current_addresses'))+ LecCheck.DLM \
                    + "historical_addresses="+str(json_str.get('historical_addresses'))+ LecCheck.DLM \
                    + "associated_people="+str(json_str.get('associated_people'))+ LecCheck.DLM \
                    + "alternate_phones="+str(json_str.get('alternate_phones'))+ LecCheck.DLM \
                    + time.strftime("%x")

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

    '''
    partition the list into n partitions of l size each.
    '''
    def partition_list(l, n):
        for i in range(0, len(l), n):
            yield l[i : i+n]

    #TODO only used for testing.
    def dummyworker(self, leads_list):
        for item in leads_list:
            self.db_manager_ref.write_filtered_leads(str(len(item))+":"+ str(item) )

    def worker(self, leads_list):
        logger.info('worker thread:')
        for item in leads_list:
            fields = item.split(',')
            try:
                 wplock.acquire()
                 phone_number = fields[self.field_num]
                 if phone_number != 'phone_number' and phone_number != 'btn' and phone_number != 'Phone Number':
                    carrier_info = self.whitepages_ref.get_carrier_info(fields[self.field_num])
                    item = item.strip()+','+ carrier_info
                 wplock.release()

            except Exception as e:
                logger.error(e)
                self.whitepages_ref.failed_requests = self.whitepages_ref.failed_requests+1
                logger.error('Exception thrown:'+str(e))
                wplock.release()
                continue

            self.db_manager_ref.write_filtered_leads(item)


    def mt_process_file(self, filepath, statusFunc = None):
        util = Utils()
        header_columns = ',carrier,type,dnc,validity,connected,prepaid,processed_date,wp_business_name,wp_address,wp_suite,wp_city,wp_state,wp_zip' #These are the headers that Ekata API inserts
        header = True
        with open (filepath,'r') as f:
            for line in f:
                if header:
                    #line=line+header_columns
                    line = line.strip("\n") + header_columns
                    #newline = line.strip("\n") + " " + 'test2' + "\n"
                    header = False
                self.input_list.append(line)

        threads = []
        subset = util.chunks(self.input_list, 10000)
        for l in subset:
            t = threading.Thread(target=self.worker, args=([l]) )
            threads.append(t)
            t.start()

        for t in threads:
            logger.info ("joining threads")
            t.join()

        logger.info ('committing results')
        self.db_manager_ref.commit(filepath)
        if statusFunc != None:
            return statusFunc('Finished processing LEC check file: '+filepath)

def main(*args):
    start_time = datetime.now().replace(microsecond=0)
    logger.warning("VERSION:" + VERSION + " ---------------Starting new session-------- on: " + str(start_time))
        #first arg is the filename
    if (len(sys.argv) < 2):
        print('Version: ' + VERSION + os.linesep + ' Usage: leadsmanager <filename> <phone Column Number>\n e.g leadsmanager tests.csv 5')
        sys.exit(-1)
    leads = Enhance_Carrier_Info(int(sys.argv[2]) - 1)
    leads.mt_process_file(sys.argv[1])
    time_taken = datetime.now().replace(microsecond=0) - start_time

    logger.warning("---------------Ending  session--Time taken: " + str(time_taken))
    logger.warning("--------Time taken----: " + str(time_taken))


if __name__ == "__main__":
    main(sys.argv[1: ])

# import unittest
# class whitepages_test(unittest.TestCase):
#     def setUp(self):
#         pass

#     def test_fileQuestionCreatorFromFile(self):
#         wp = LecCheck()
#         val = wp.get_carrier_info('9493054125')
#         self.assertEqual('T-Mobile USA', val)
