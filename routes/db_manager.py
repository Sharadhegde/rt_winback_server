import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from . import routes, conn
from SphUtil import FileAndDBUtil, LEADS_DB_HOST, LEADS_DB_NAME, LEADS_DB_PWD, LEADS_DB_USER, WP_TABLE_NAME

today = datetime.today().date().strftime("%Y%m%d")
logging.basicConfig(filename='logs/whitepages-' + today + '.log',level=logging.WARNING)
logger = logging.getLogger(__name__)
VERSION = today


class DBManager:
    def __init__(self):
        self.result_list = []

    def write_leads_file(self, filename):
        pass

    def write_filtered_leads(self, record):
        self.result_list.append(record)

    #commit to both the file and the database.
    def commit(self, orig_filename):

        orig_filename = Path(orig_filename).stem
        results_filename = datetime.now().strftime("%Y%m%d-%H%M%S") + '.csv'
        results_filename = str(orig_filename)+'_'+str(results_filename)
        f=open(results_filename, 'w')
        logger.debug("Writing: " + str( len(self.result_list) ) + " records")

        for item in self.result_list:
            f.write(item+'\n')
        f.flush()
        f.close()

        self.write_to_db(results_filename)
        return results_filename

    #get the results dataframe into the database.
    def get_results_list(self, ):
        return self.result_list



    def write_to_db(self,filename):
        logger.info('writing to the database :'+str(len(self.result_list)) +" records ")
        #column names of WP Table
        #btn,address,city,state,zipcode,latitude,longitude,carrier,type,processed_date,sg_id
        try :
            csv_df=pd.read_csv(filename,index_col=False)
            csv_df.columns =csv_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.','').str.replace('%','')
            csv_df.rename(columns={'phone_number': 'btn', 'zipcode':'zip'}, inplace=True) #rename if different column headings
            csv_df.rename(columns={'Phone Number Combined': 'btn', 'wp_zip': 'zip','wp_city':'city','wp_state':'state','id':'sg_serial_number'},inplace=True)  # rename if different column headings

            csv_df.to_csv('csvdf.csv')
            wp_df=csv_df[['btn','city','state','latitude','longitude','carrier','type','processed_date','address','zip','sg_serial_number']]
            wp_df.to_csv('wp_df.csv')
            FileAndDBUtil.upload_records_into_db(wp_df, WP_TABLE_NAME, LEADS_DB_HOST, LEADS_DB_NAME, LEADS_DB_USER, LEADS_DB_PWD)
            logger.info('successfully written the records to the database :'+str(len(self.result_list)) +" records ")

        except Exception as e:
            logger.error(e)
