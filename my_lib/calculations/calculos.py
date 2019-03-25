""" coding: utf-8
Created by rsanchez on 07/05/2018
Este proyecto ha sido desarrollado en la Gerencia de Operaciones de CENACE
Mateo633
"""
import datetime
import os
import datetime as dt
import pandas as pd
import numpy as np
from my_lib.mongo_db_manager import mongo_handler as mh

script_path = os.path.dirname(os.path.abspath(__file__))
# ______________________________________________________________________________________________________________#
# ________________________________        GENERAL        VARIABLES       _______________________________________

delta_15 = 15  # minutes
factor_15 = delta_15 / 60  # factor for calculating Energy using P each 15 minutes
span_15 = "15m"

delta_30 = 30  # minutes
factor_30 = delta_30 / 60  # factor for calculating Energy using P each 15 minutes
span_30 = "30m"

yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss"
yyyy_MM_dd = "yyyy-MM-dd"


# ______________________________________________________________________________________________________________#



def test():
    cl = mh.configMonClient()
    db = cl.get_database("settings")
    col = db["mapping"]
    col.insert_one(dict(test="prueba", fecha=dt.datetime.now()))


if __name__ == "__main__":
    perform_test = True
    if perform_test:
        test()
