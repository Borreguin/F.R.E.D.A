""""
    Created by Roberto Sánchez A, based on the Master Thesis:
    "A proposed method for unsupervised anomaly detection for arg_from multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.arg_from@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33

    This script allows to handler several task to the mongo database
"""

import pymongo as pm
from pymongo.errors import ServerSelectionTimeoutError
from settings import initial_settings as init
from multiprocessing import Pool
import logging
import os


""" FREDA: General settings collection"""
db_name = "FREDA"
config_collection = "general_settings"
DB_test = "db_for_test_only"

""" Default logger: """
lg = init.LogDefaultConfig()

""" Default MongoClient"""
mClient = init.MongoClientDefaultConfig()


def read_settings():
    """
    Reads the general configurations about FREDA
    :return:
    """
    try:
        client = mClient.client
        db = client.get_database(db_name)
        collection = db[config_collection]
        result = collection.find_one({'_id': "config"})
        return True, result
    except Exception as e:
        lg.logger.error(e)
        return False, str(e)


def save_settings(key, value):
    """
    Save arg_from value setting in the general configuration
    :param key:
    :param value:
    :return:
    """
    try:
        client = mClient.client
        db = client.get_database(db_name)
        collection = db[config_collection]
        collection.update(
            {'_id': "config"},
            {"$set": {key: value}},
            upsert=True
        )
        msg = "Successful change in Settings: " + str(key) + ":" + str(value)
        lg.logger.debug(msg)
        return True, msg

    except Exception as e:
        lg.logger.error(e)
        return False, str(e)


def insert_test(ini, end):
    client = mClient.client
    db_test = client.get_database(DB_test)
    collection = db_test["for_testing_purpose"]
    exceptions = 0
    insertions = 0
    connect = True
    for i in range(ini, end):
        try:
            collection.update_one(
                {'id': i},
                {"$set": {"No_test": i}},
                upsert=True
            )
            lg.logger.debug("Inserting register No. " + str(i))
            insertions += 1
        except ServerSelectionTimeoutError:
            lg.logger.warning("MongoDb server is not running...")
            connect = False
            break
        except Exception as e:
            lg.logger.error(e)
            exceptions += 1
    client.close()
    return connect, insertions, exceptions


def check():

    """ Log configurations """
    lg = init.LogDefaultConfig()
    lg.logger.setLevel(logging.INFO)

    """ Starting the test_dict """
    lg.logger.info("Starting ")
    n_registers = 10000
    workers = 5
    n = int(n_registers/workers)
    to_run = [(x, x+n) for x in range(0, n_registers, n)]
    """ Insert 10000 registers"""
    with Pool(processes=workers) as pool:
        results = pool.starmap(insert_test, to_run)
    lg.logger.info(results)
    con, ins, exc = True, 0, 0
    for r in results:
        con = con&r[0]
        ins += r[1]
        exc += r[2]

    msg = "Connection: {0}, Exceptions: {1}, Insertions: {2}".format(con, exc, ins)
    lg.logger.info(msg)
    if con and n_registers==ins:
        client = mClient.client
        client.drop_database(DB_test)
        msg = "Successful test_dict"
        lg.logger.info(msg)
    else:
        msg = "Unsuccessful test_dict"
        lg.logger.error(msg)

    return True


if __name__ == '__main__':
    check()
