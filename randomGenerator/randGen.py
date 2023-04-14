"""To generate random data in local PSQL server for 2 tables
customer_info & customer_txn"""
import pandas as pd
import numpy as np
import time
import random
import string
import logging.handlers
from DbOperation import DbOperation

LOG_FILENAME = "../logs/randomGenerator.log"

my_logger = logging.getLogger("rand_gen_logger")
my_logger.setLevel(logging.INFO)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=104857600, backupCount=10)
bf = logging.Formatter('{asctime} {name} {levelname:8s} {message}', style='{')

handler.setFormatter(bf)
my_logger.addHandler(handler)


def random_string(length):
    result = ''.join(random.choices(string.ascii_lowercase, k=length))

    return result


def generate_random_str_list(length):
    rand_str_list = []
    for i in range(0, length):
        rand_str_list.append(random_string(8))
    
    return rand_str_list


def main():
    start_time = time.time()
    db_op = DbOperation()

    # To generate random customers
    # rand_str_list = generate_random_str_list(1000000)
    # db_op.insert_cust_text(rand_str_list)

    # To generate random transactions
    cust_id_list = db_op.generic_execute_return("select cust_id from customer_info")
    cust_id_txn_list = []
    for cust_id in cust_id_list:
         for i in range(0, random.randint(1, 5)):
            cust_id_txn_list.append([cust_id[0], random.randint(-1000, 1000)])
    db_op.insert_customer_txn(cust_id_txn_list)

    my_logger.info("Finished processing in %.2f seconds" % (time.time() - start_time))


if __name__ == "__main__":
    main()