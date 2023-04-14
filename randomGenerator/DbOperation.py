import psycopg2
import psycopg2.extras
import os
import json
import logging
from util import db_decode

# Set the logging level
logging.basicConfig(level=logging.DEBUG)


class DbOperation:
    def __init__(self, config_file='config.json', section='production'):
        with open(os.path.realpath(config_file), 'r') as f:
            data = json.loads(f.read())
            conn_data = data[section]
            self.conn = psycopg2.connect(
                user=db_decode(conn_data['username']),
                password=db_decode(conn_data['password']),
                host=conn_data['host'],
                port=conn_data['port'],
                database=conn_data['database']
            )
            self.cursor = self.conn.cursor()
            self.conn.autocommit = False

    def batch_update_with_tup(self, update_query, values):
        """
        Batch update

        The values have to be in list of tuples format, then can run one shot
        Should be the faster method
        :param update_query: the update query to run
        :param values: a list of tuples of the values
        :return:
        """
        cur = self.conn.cursor()
        psycopg2.extras.execute_batch(cur, update_query, values)
        self.conn.commit()

    def generic_execute_no_return(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()

    def generic_execute_no_return_with_param(self, query, param):
        cur = self.conn.cursor()
        psycopg2.extras.execute_batch(cur, query, param)
        self.conn.commit()

    def generic_execute_return(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return result
    
    def insert_cust_text(self, rand_str_list):
        cur = self.conn.cursor()
        for str in rand_str_list:
            cur.execute("insert into customer_info (cust_text) values (%s)", [str])
        self.conn.commit()

    def insert_customer_txn(self, cust_id_txn_list):
        cur = self.conn.cursor()
        for n in cust_id_txn_list:
            # print(n)
            cur.execute("insert into customer_txn (cust_id, txn_amt) values (%s, %s)", n)
        self.conn.commit()

    def close_conn(self):
        """
        Close the connection
        :return:
        """
        self.conn.close()
