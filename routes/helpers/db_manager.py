import logging
from datetime import datetime
from pathlib import Path
from numpy import append

import pandas as pd
from routes import routes, conn

today = datetime.today().date().strftime("%Y-%m-%d")
logging.basicConfig(filename='routes/logs/whitepages-' + today + '.log', level=logging.WARNING)
logger = logging.getLogger(__name__)
VERSION = today


class DBManager:
    def __init__(self):
        self.existing_cb = []
        self.lost_cb = []
        self.input_cb = 0
        self.new_cb = 0

    def write_filtered_leads(self, record, lost):
        if(not lost):
            self.existing_cb.append(record)
        else:
            self.lost_cb.append(record)

    #commit to both the file and the database.
    def commit(self, results):
        results_filename = 'routes/results/' + datetime.now().strftime("%Y%m%d-%H%M%S") + '.csv'
        f = open(results_filename, 'w')
        logger.debug("Writing Existing CB: " + str(len(self.existing_cb)) + " records")
        for item in self.existing_cb:
            f.write(item + '\n')
        f.flush()
        f.close()
        f = open(results_filename, 'a')
        logger.debug("Writing Lost CB: " + str(len(self.lost_cb)) + " records")
        for item in self.lost_cb:
            f.write(item + '\n')
        f.flush()
        f.close()
        self.write_to_db(results_filename)
        return results_filename

    #get the results dataframe into the database.
    # def get_results_list(self):
    #     return self.result_list

    def write_to_db(self, results_filename):
        try :
            df = pd.read_csv(results_filename, header=None)
            df[1] = pd.to_datetime(pd.to_datetime(df[1]))

            existing_cb_df = df[(df[2].str.len() == 3) & (df[2] == df[2].str.upper())]
            existing_cb_df.columns = ['order_id', 'lec_check_date', 'carrier']
            lost_cb_df = df[df[2].str.len() != 3]
            lost_cb_df.columns = ['order_id', 'lec_check_date', 'changed_lec']
            
            existing_cb_df.to_sql('rt_winback_existing_cb', conn, if_exists='append', index=None)
            lost_cb_df.to_sql('winback_customers', conn, if_exists='append', index=None)

            conn.execute("""INSERT INTO sbmsprod.rt_winback_summary
                        (run_date, input_cb, new_cb, lost_cb)
                        VALUES('{}', '{}', '{}', '{}')""".format(today, self.input_cb, self.new_cb, lost_cb_df.shape[0]))

            logger.info('successfully written the records to the database')

        except Exception as e:
            logger.error(e)

    def get_all_orders(self):
        try:
            orderBtnList = []
            last_run, next_run = self.get_launch_dates()
            results_new = conn.execute("""SELECT id, btn, campaign
                                FROM orders_order 
                                WHERE date_installed >= '{}' 
                                AND date_installed <= '{}'""".format(last_run, next_run))
            for row in results_new:
                row_str = '{},{},{}'.format(row[0], row[1], row[2])
                orderBtnList.append(row_str)
            
            self.new_cb = len(orderBtnList)
            
            results_existing = conn.execute('''SELECT id, btn, campaign
                                FROM rt_winback_existing_cb rt, orders_order oo
                                WHERE oo.id = rt.order_id''')
            
            for row in results_existing:
                row_str = '{},{},{}'.format(row[0], row[1], row[2])
                orderBtnList.append(row_str)
                
            self.input_cb = len(orderBtnList)

        except Exception as e:
            logger.error(e)
            logger.error('Exception thrown:' + str(e))

        return orderBtnList

    def get_launch_dates(self):
        try:
            results = conn.execute('''SELECT *
                                FROM rt_winback_config
                                ORDER BY next_run_date LIMIT 1''')
            for row in results:
                last_run, next_run = row[1].strftime('%Y-%m-%d'), row[2].strftime('%Y-%m-%d')

        except Exception as e:
            print(e)
            logger.error(e)
            logger.error('Exception thrown:' + str(e))

        return last_run, next_run
