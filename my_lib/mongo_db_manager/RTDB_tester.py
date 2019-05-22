# Created by Roberto Sanchez at 4/22/2019
# -*- coding: utf-8 -*-
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
"""
from my_lib.mongo_db_manager import RTDB_mongo_driver as drv
from my_lib.mongo_db_manager import RTDBClasses as rtc
import logging
import datetime as dt
import settings.initial_settings as init
import time


def main():
    tag_name = "Test_only.aux"
    """ Init config for Logger """
    log = init.LogDefaultConfig("Test_driver.log")
    log.logger.setLevel(logging.INFO)

    """ Init container """
    container = drv.RTContainer(logger=log.logger)
    container.create_tag_point(tag_name, "Test")

    """ Creating register to insert"""
    registers = list()
    n_registers = 10000
    log.logger.info("Creating {0} registers".format(n_registers))
    for i in range(n_registers):
        r = dict(value=i, timestamp=dt.datetime.now() - dt.timedelta(seconds=n_registers-i))
        registers.append(r)

    """ Inserting n registers """
    log.logger.info("Inserting {0} registers".format(n_registers))
    t_start = time.time()
    tag_point = drv.TagPoint(container, tag_name, logger=log.logger)
    success, tag_id = container.find_tag_point_by_id(tag_point.tag_id)
    resp = tag_point.insert_many_registers(registers)

    """ Check if n registers were inserted correctly """
    t_end = time.time()
    if resp == n_registers:
        log.logger.info("Successful insertion test_dict. {0} registers were inserted in {1:.2f} seconds"
                        .format(n_registers, t_end - t_start))

    """ Reading n registers from table"""
    t_start = time.time()
    time_range = container.time_range(registers[0]["timestamp"], registers[-1]["timestamp"])
    df = tag_point.recorded_values(time_range)
    t_end = time.time()
    log.logger.info("{0} registers were read from {1} to {2} in {3:.2f} seconds"
                    .format(len(df.index), df.index[0], df.index[-1], t_end-t_start))

    time_range = container.time_range(registers[0]["timestamp"], registers[-1]["timestamp"], freq="4s")
    df = tag_point.interpolated(time_range)

    success, msg = container.delete_tag_point(tag_name)
    log.logger.info(msg)
    if success:
        log.logger.info("RTDB test_dict was successful")
        return True
    else:
        log.logger.info("RTDB test_dict was not sucessful")
        return False

if __name__ == '__main__':
    main()