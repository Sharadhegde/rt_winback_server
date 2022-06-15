from datetime import datetime, timedelta
from hashlib import new
import pandas as pd

from routes import mysql_conn, postgresql_conn, logger, today

class DBManager:
    def __init__(self):
        self.existing_cb = []
        self.lost_cb = []
        self.input_cb = 0
        self.new_cb = 0
        self.client_ip = ""
        self.carrier_info_filename = 'routes/results/carrier_info_' + datetime.now().strftime("%Y%m%d") + '.csv'
        self.disconnected_orders_filename = 'routes/results/disconnected_orders_' + datetime.now().strftime("%Y%m%d") + '.csv'
    
    def set_params(self, client_ip, input_cb, new_cb):
        self.client_ip = client_ip
        self.input_cb = input_cb
        self.new_cb = new_cb

    def write_filtered_leads(self, record, lost):
        if(not lost):
            self.existing_cb.append(record)
        else:
            self.lost_cb.append(record)

    def commit_carrier_info(self, results):
        f = open(self.carrier_info_filename, 'w')
        logger.debug("Writing: " + str(len(results)) + " carrier info records")
        for item in results:
            f.write(item + '\n')
        f.flush()
        f.close()

    #commit existing and lost orders to both the file and the database.
    def commit_disconnected_orders(self, results):
        f = open(self.disconnected_orders_filename, 'w')
        logger.debug("Writing Existing CB: " + str(len(self.existing_cb)) + " records")
        for item in self.existing_cb:
            f.write(item + '\n')
        f.flush()
        f.close()
        f = open(self.disconnected_orders_filename, 'a')
        logger.debug("Writing Lost CB: " + str(len(self.lost_cb)) + " records")
        for item in self.lost_cb:
            f.write(item + '\n')
        f.flush()
        f.close()
    
    def write_to_db_carrier_info(self, results_filename):
        try:
            df = pd.read_csv(results_filename, header=None)
            df = df.iloc[:, 3:]
            df.columns = ['btn', 'address', 'city', 'state', 'zip', 'latitude', 'longitude',
            'carrier', 'carrier_type', 'processed_date', 'sg_serial_number']           
            df['sg_serial_number'] = df['sg_serial_number'].fillna("0")

            df.to_sql('wp_real', postgresql_conn, if_exists='append', index=None)
            logger.info('successfully written carrier info to the database')

        except Exception as e:
            logger.error(e)

    def write_to_db_disconnected(self, results_filename):
        try :
            df = pd.read_csv(results_filename, header=None)
            df[1] = pd.to_datetime(pd.to_datetime(df[1]))

            existing_cb_df = df[(df[2].str.len() == 3) & (df[2] == df[2].str.upper())]
            existing_cb_df.columns = ['order_id', 'lec_check_date', 'carrier']
            lost_cb_df = df[df[2].str.len() != 3]
            lost_cb_df.columns = ['order_id', 'lec_check_date', 'changed_lec']

            # existing_cb_df.to_sql('rt_winback_existing_cb', mysql_conn, if_exists='append', index=False)
            # lost_cb_df.to_sql('rt_winback_lost_cb', mysql_conn, if_exists='append', index=False)

            existing_cb_df.to_sql(name='temporary_table', con=mysql_conn, if_exists = 'replace', index=False)
            mysql_conn.execute("""INSERT IGNORE INTO rt_winback_existing_cb (SELECT * FROM temporary_table)""")
            
            lost_cb_df.to_sql(name='temporary_table', con=mysql_conn, if_exists = 'replace', index=False)
            mysql_conn.execute("""INSERT IGNORE INTO rt_winback_lost_cb (SELECT * FROM temporary_table)""")

            mysql_conn.execute("""INSERT INTO sbmsprod.rt_winback_summary
                        (run_date, input_cb, new_cb, lost_cb, existing_cb, client_ip)
                        VALUES('{}', '{}', '{}', '{}', '{}', '{}')""".format(today, self.input_cb, self.new_cb, lost_cb_df.shape[0], existing_cb_df.shape[0], self.client_ip))

            logger.info('successfully written the existing and lost records to the database')

        except Exception as e:
            logger.error(e)

    def get_all_orders(self):
        try:
            orderBtnList = []
            last_run, next_run = self.get_launch_dates()
            results_new = mysql_conn.execute("""SELECT id, btn, campaign
                                FROM orders_order 
                                WHERE date_installed >= '{}' 
                                AND date_installed <= '{}'
                                AND status = 'installed'""".format(last_run, next_run))

            # results_new = mysql_conn.execute("""SELECT id, btn, campaign
            #                     FROM orders_order 
            #                     WHERE status = 'installed'
            #                     AND date_installed IS NOT NULL""")
            for row in results_new:
                row_str = '{},{},{}'.format(row[0], row[1], row[2])
                if row_str not in orderBtnList:
                    orderBtnList.append(row_str)
            
            self.new_cb = len(orderBtnList)

            results_existing = mysql_conn.execute('''SELECT id, btn, campaign
                                FROM rt_winback_existing_cb rt, orders_order oo
                                WHERE oo.id = rt.order_id''')
            
            for row in results_existing:
                row_str = '{},{},{}'.format(row[0], row[1], row[2])
                orderBtnList.append(row_str)
                
            self.input_cb = len(orderBtnList)

        except Exception as e:
            logger.error('Exception thrown:' + str(e))

        return orderBtnList, self.input_cb, self.new_cb

    def get_launch_dates(self):
        try:
            results = mysql_conn.execute('''SELECT *
                                FROM rt_winback_config
                                ORDER BY next_run_date DESC
                                LIMIT 1''')
            for row in results:
                last_run, next_run = row[1].strftime('%Y-%m-%d'), row[2].strftime('%Y-%m-%d')

        except Exception as e:
            logger.error(e)
            logger.error('Exception thrown:' + str(e))

        return last_run, next_run
    
    def add_launch_dates(self):
        try:
            last_run = datetime.now().strftime('%Y-%m-%d 00:00:00')
            next_run = (datetime.strptime(last_run, '%Y-%m-%d 00:00:00') + timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
            mysql_conn.execute("""INSERT INTO rt_winback_config
                                VALUES ({}, '{}', '{}')""".format(7, last_run, next_run))
        except Exception as e:
            logger.error(e)
            logger.error('Exception thrown:' + str(e))
