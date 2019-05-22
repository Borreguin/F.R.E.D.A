# Created by Roberto Sanchez at 5/12/2019
# -*- coding: utf-8 -*-
""""
    Created by Roberto Sánchez A, based on the Master Thesis:
    "A proposed method for unsupervised anomaly detection for a multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.a@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
import traceback
from my_lib.mongo_db_manager import RTDB_mongo_driver as drv
from settings import initial_settings as init
from datetime import datetime as dt
import os
from settings.initial_settings import DB_MONGO_NAME as rtdb

COLLECTION_SYSTEM = "SYSTEM|ADMIN"
INSERTION_TAG = "SYSTEM.NUM-INSERTIONS"
SIZE_DISK_TAG = "SYSTEM.DISK_SIZE"

def register_insertions(number_insertions, recursive=0):
    cnt = drv.RTContainer()
    if recursive > 2:   # try 3 times
        return False, "[{0}] Unable to update".format(INSERTION_TAG)
    try:
        tag_point = drv.TagPoint(cnt, INSERTION_TAG)
        assert(tag_point.tag_type is not None)
        register = dict(timestamp=dt.now().timestamp(), value=number_insertions)
        success, result = tag_point.insert_register(register, update_last=False)
        cnt.close()
        if success:
            return True, "[{0}] update".format(INSERTION_TAG)
        else:
            recursive += 1
            register_insertions(number_insertions, recursive)

    except Exception as e:
        tb = traceback.format_exc()
        msg = "[{0}] Unable to update".format(INSERTION_TAG)
        cnt.log.error(msg + "\n" + str(e) + "\n" + str(tb))
        cnt.create_tag_point(tag_name=INSERTION_TAG, tag_type=COLLECTION_SYSTEM)
        recursive +=1
        register_insertions(number_insertions, recursive)


def disk_size(path, unit="GB"):
    if not os.path.isdir(path):
        return False, "[{0}] does not exist".format(path)
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    if unit == "GB":
        return True, round(total_size/1000000000, 3)
    if unit == "MB":
        return True, round(total_size/1000000, 3)
    return total_size

