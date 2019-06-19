# Created by Roberto Sanchez at 3/17/2019
import pymongo
from my_lib.RTDB.RTDA_BaseClass import RTDAcquisitionSource
from my_lib.RTDB.RTDA_BaseClass import RTDATagPoint
from my_lib.mongo_db_manager import RTDBClasses as h
import settings.initial_settings as init

import pandas as pd
import pymongo as pm
import re
import datetime as dt
import sys, traceback
import more_itertools as it
from multiprocessing import Pool
import logging
from my_lib.mongo_db_manager import RTDB_system as sys_h

DBTimeSeries = init.DB_MONGO_NAME
CollectionTagList = init.CL_TAG_LIST
CollectionLastValues = init.CL_LAST_VALUES


class RTContainer(RTDAcquisitionSource):
    def __init__(self, mongo_client_settings: dict = None, logger: logging.Logger=None):
        """
        Sets the Mongo DB container
        NOTE: This MongoClient must be tz_aware: Ex: pm.MongoClient(tz_aware=True)

        :param mongo_client_settings: MongoClient_settings (tz_aware=True)

        """
        if mongo_client_settings is None:
            self.container = init.MongoClientDefaultConfig().client
            self.settings = init.MONGOCLIENT_SETTINGS
        else:
            self.container = pm.MongoClient(**mongo_client_settings)
            self.settings = mongo_client_settings

        if logger is None:
            logger = init.LogDefaultConfig().logger
        self.log = logger

    @staticmethod
    def time_range(ini_time, end_time, **kwargs):
        if len(kwargs) != 0:
            return pd.date_range(ini_time, end_time, tz=dt.timezone.utc,**kwargs)
        else:
            return pd.date_range(ini_time, end_time, tz=dt.timezone.utc, periods=2)

    @staticmethod
    def time_range_for_today():
        return pd.date_range(dt.datetime.now().date(), dt.datetime.now(tz=dt.timezone.utc), periods=2)

    @staticmethod
    def start_and_time_of(time_range):
        return time_range[0], time_range[-1]

    @staticmethod
    def span(delta_time):
        return pd.Timedelta(**delta_time)

    def interpolated_of_tag_list(self, tag_list, time_range, numeric=False, **kwargs):
        if len(tag_list) == 0:
            return False, "tag_list is empty"

        id_dict, tag_dict, found, not_found = self.get_dicts_for_tag_list(tag_list)
        if len(found) == 0:
            return False, "{0} none of the tag_names exists in the Historian".format(tag_list)
        if len(not_found) > 0:
            self.log.warning("{0} do not exist in the Historian".format(not_found))
        try:
            tag_points = [TagPoint(self, tag_name) for tag_name in found]
            df_lst = [point.interpolated(time_range, numeric=numeric, **kwargs) for point in tag_points]
            result = dict()
            for tag, df in zip(found, df_lst):
                result[tag] = df
            return True, result

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def current_value_of_tag_list(self, tag_list: list, format_time):
        if len(tag_list) == 0:
            return False, "tag_list is empty"

        if format_time is None:
            format_time = init.DEFAULT_DATE_FORMAT

        id_dict, tag_dict, found, not_found = self.get_dicts_for_tag_list(tag_list)
        if len(found) == 0:
            return False, "{0} do not exist in the Historian".format(tag_list)
        if len(not_found) > 0:
            self.log.warning("{0} do not exist in the Historian".format(not_found))

        try:
            db = self.container[DBTimeSeries]
            # Get tag_id for the tag_list
            last_values_collection = db[CollectionLastValues]
            filter_dict = {"tag_id": {"$in": list(id_dict.keys())}}
            registers = list(last_values_collection.find(filter_dict))

            r = dict()
            found = list()
            for reg in registers:
                value = reg["series"]
                if format_time != "epoch":
                    value["timestamp"] = h.to_datetime(value["timestamp"])
                    value["timestamp"] = value["timestamp"].strftime(format_time)
                r[reg["tag_name"]] = value
                found.append(reg["tag_name"])

            result = dict(result=r, found=found, not_found=list(set(tag_list) - set(found)))
            return True, result

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def get_dicts_for_tag_list(self, tag_list):
        """
        Returns dictionaries id_dict: tag_id->tag_name, tag_dict: tag_name->tag_id
        Returns list of: found (tag_names that were found), not_found (not founded tags)
        :param tag_list:
        :return: id_dict, tag_dict, found, not_found
        """
        tag_registers = [self.find_tag_point_by_name(tag) for tag in tag_list]
        id_dict = dict()
        tag_dict = dict()
        not_found = list()
        found = list()
        for register, tag in zip(tag_registers, tag_list):
            if register[0]:
                id_dict[register[1]["tag_id"]] = tag
                tag_dict[tag] = register[1]["tag_id"]
                found.append(tag)
            else:
                not_found.append(tag)
        return id_dict, tag_dict, found, not_found

    def recorded_values_of_tag_list(self, tag_list, time_range, format_time):
        if len(tag_list) == 0:
            return False, "tag_list is empty"

        if format_time is None:
            format_time = init.DEFAULT_DATE_FORMAT

        id_dict, tag_dict, found, not_found = self.get_dicts_for_tag_list(tag_list)
        if len(found) == 0:
            return False, "{0} any of the tag_name exists in the Historian".format(tag_list)
        if len(not_found) > 0:
            self.log.warning("{0} do not exist in the Historian".format(not_found))

        result = dict(found=found, not_found=not_found, result=dict())
        try:
            point_list = [TagPoint(self, tag) for tag in found]
            df = [pt.recorded_values(time_range, numeric=False) for pt in point_list]
            for idx, tag in enumerate(found):
                df[idx]["timestamp"] = [h.to_datetime(x).strftime(format_time) for x in df[idx].index]
                result["result"][tag] = df[idx].to_dict(orient="register")
            return True, result

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def find_tag_point_by_name(self, TagName: str):
        TagName = TagName.upper().strip()
        try:
            db = self.container[DBTimeSeries]
            collection = db[CollectionTagList]
            result = collection.find_one({"tag_name": TagName})
            if result is None:
                return False, result
            else:
                return True, result
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def find_tag_point_by_id(self, tag_id:str):
        try:
            db = self.container[DBTimeSeries]
            collection = db[CollectionTagList]
            result = collection.find_one({"tag_id": tag_id})
            return True, result["tag_name"]
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def update_tag_name(self, tag_name:str, new_tag_name:str):

        tag_name = tag_name.upper().strip()
        new_tag_name = new_tag_name.strip().upper()
        success_old, result_old = self.find_tag_point_by_name(tag_name)
        if not success_old:
            return success_old, "[{0}] does not exist in the historian".format(tag_name)

        success_new, result_new = self.find_tag_point_by_name(new_tag_name)
        if success_new:
            return False, "[{0}] already exists in the historian. Use other new_tag_name"\
                .format(new_tag_name)

        to_update = [CollectionTagList, CollectionLastValues]
        if success_old and not success_new:
            try:
                filter_dict = dict(tag_id=result_old["tag_id"])
                db = self.container[DBTimeSeries]
                for collection in to_update:
                    collection = db[collection]
                    collection.update(filter_dict,
                                                   {"$set": {"tag_name": new_tag_name}
                    })
                return True, "[{0}] changed to [{1}]".format(tag_name, new_tag_name)
            except Exception as e:
                return False, str(e)

    def find_all_tags(self, filter_exp:str=None):
        try:
            db = self.container[DBTimeSeries]
            collection = db[CollectionTagList]

            if filter_exp is not None:
                filter_exp = filter_exp.replace("*", ".*")
                rgx = re.compile(filter_exp, re.IGNORECASE)  # compile the regex
                result = collection.find({"tag_name" : rgx}, projection={'_id': False})
            else:
                result = collection.find(projection={'_id': False})

            to_send = list()
            for d in result:
                to_send.append(d)
            df = pd.DataFrame(to_send).sort_values(by=["tag_name"])

            return True, df.to_dict(orient="records")
        except Exception as e:
            return False, list()

    def create_tag_point(self, tag_name: str, tag_type: str="generic"):

        success, tag_name, tag_type = self.validate_name(tag_name, tag_type)
        if not success:
            return False, "Incorrect name"

        db = self.container[DBTimeSeries]
        cl_tag_list = db[CollectionTagList]
        cl_last_value = db[CollectionLastValues]

        if cl_tag_list.count() == 0:
            cl_tag_list.create_index("tag_name", unique=True)

        if cl_last_value.count() == 0:
            cl_last_value.create_index("tag_id", unique=True)

        tagPoint = {"tag_name": tag_name, "tag_type": tag_type}

        try:
            result = cl_tag_list.insert_one(tagPoint)
            msg = "[{0}] was created successfully".format(tag_name)
            self.log.info(msg)
            inserted_id = str(tagPoint["_id"])
            cl_tag_list.update_one(tagPoint, {"$set": {"tag_id": inserted_id }}, upsert=True)
            return True, msg
        except Exception as e:
            msg = "[{0}] This tag already exists, duplicated-tag warning".format(tag_name)
            self.log.warning(msg + "\n" + str(e))
            return False, msg

    def validate_name(self, tag_name: str, tag_type:str ):
        tag_name = tag_name.upper().strip()
        tag_type = tag_type.upper().strip()

        if len(tag_type) <= 0:
            tag_type = "generic"
        if "|" not in tag_type:
            tag_type = "TSCL|" + tag_type
        if "$" in tag_type:
            tag_type.replace("$", "*")

        if len(tag_name) == 0:
            return False, tag_name, tag_type

        tag_name = re.sub(",", "*", tag_name)
        tag_name = re.sub(":", "*", tag_name)
        tag_name = re.sub(";", "*", tag_name)

        return True, tag_name, tag_type

    def delete_tag_point(self, tag_name: str, tag_type: str):
        tag_point = TagPoint(self, tag_name, self.log)
        tag_type = tag_type.strip().upper()

        msg = ""
        if tag_point.tag_name is None:
            msg = "[{0}] does not exists in the historian ".format(tag_name)
        elif tag_type not in tag_point.tag_type:
                msg += "[{0}] tagType does not match with [{1}]".format(tag_type, TagPoint.tag_type)

        if len(msg)>0:
            self.log.warning(msg)
            return False, msg

        collections_where_delete = [CollectionTagList, CollectionLastValues, tag_point.tag_type]
        msg = ""
        for collection in collections_where_delete:
            try:
                db = self.container[DBTimeSeries]
                filter_dict = dict(tag_id=tag_point.tag_id)
                cl = db[collection]
                cl.delete_many(filter_dict)
            except Exception as e:
                msg += "Unable to delete tag_point {0} in collection {1}".\
                    format(tag_name, collection)
                self.log.error(msg + "\n" + str(e))

        if len(msg) == 0:
            msg = "Tag point {0} was deleted".format(tag_name)
            self.log.info(msg)
            return True, msg
        else:
            return False, msg


    def close(self):
        self.container.close()


class TagPoint(RTDATagPoint):
    container = None
    tag_type = None
    tag_name = None
    tag_id = None
    log = None

    def __init__(self, container: RTContainer, tag_name: str, logger: logging.Logger=None):
        """
        Creates a TagPoint that allows to manage the corresponding time series.

        :param container: defines the container of the data
        :param tag_name: name of the tag
        :param logger: Python logger object

        """
        self.container = container
        self.tag_name = tag_name.upper()
        if logger is None:
            logger = init.LogDefaultConfig().logger
        self.log = logger

        success, search = container.find_tag_point_by_name(tag_name)
        if success and isinstance(search, dict):
            self._id = str(search["_id"])
            self.tag_id = str(search["tag_id"])
            self.tag_type = search["tag_type"]
            self.log.debug("[{0}] Tag was found".format(tag_name))

        else:
            self.log.warning("[{0}]: Tag was not found".format(tag_name))
            print("There is not Tag called: " + self.tag_name)

    def interpolated(self, time_range, numeric=True, **kwargs):
        df_series = self.recorded_values(time_range, border_type="Inclusive", numeric=numeric)
        # if df_series.empty:
        #    return df_series
        if numeric:
            df_series = df_series[["value"]].astype(float)

        df_result = pd.DataFrame(index=time_range)
        df_result = pd.concat([df_result, df_series], axis=1)
        df_result["value"].interpolate(inplace=True, **kwargs)
        df_result = df_result.loc[time_range]
        return df_result.loc[time_range]

    def n_values(self, time_range, n_samples, as_df=True, numeric=True):
        new_time_range = pd.date_range(time_range[0], time_range[-1], periods=n_samples)
        df = self.interpolated(new_time_range, as_df=as_df, numeric=numeric)
        return df

    def recorded_values(self, time_range, border_type="Inclusive", numeric=True):
        try:
            db = self.container.container[DBTimeSeries]
            collection = db[self.tag_type]
            d_ini, d_end = h.to_date(time_range[0]), h.to_date(time_range[-1])
            t_ini, t_fin = h.to_epoch(time_range[0]), h.to_epoch(time_range[-1])

            """
                f1          l1  f2          l2
                | *         |   |      *    |
                  t_ini               t_fin
                  t_ini < last
                  t_fin > first
            """
            filter_dict = {"tag_id": self.tag_id,
                           "date": {
                               "$gte": d_ini,
                               "$lte": d_end
                                },
                           "first": {"$lte": t_fin},
                           "last": {"$gte": t_ini}
                           }
            cursor = collection.find(filter_dict)
            series = list()
            for it in cursor:
                if isinstance(it["series"], list):
                    series += list(it["series"])
                if isinstance(it["series"], dict):
                    for k in it["series"].keys():
                        series.append(dict(timestamp=h.to_epoch(k), value=it["series"][k]))

            if len(series) == 0:
                return pd.DataFrame(columns=["value"], index=time_range)

            df_series = pd.DataFrame(series)
            df_series.set_index(["timestamp"], inplace=True)
            df_series.index = pd.to_datetime(df_series.index, unit='s', utc=True)
            df_series.sort_index(inplace=True)
            df_series = df_series.loc[~df_series.index.duplicated(keep='first')]
            # inclusive:
            mask = None
            if border_type == "Inclusive":
                """ Necessary to add microseconds to include Pandas uses nanoseconds but python datetime not."""
                mask = (df_series.index >= h.to_datetime(time_range[0]) - dt.timedelta(milliseconds=1)) & \
                       (df_series.index <= (h.to_datetime(time_range[-1] + dt.timedelta(milliseconds=1))))

            if border_type == "Exclusive":
                mask = (df_series.index > (h.to_datetime(time_range[0]) - dt.timedelta(milliseconds=1))) & \
                       (df_series.index < (h.to_datetime(time_range[-1] + dt.timedelta(milliseconds=1))))

            if border_type == "Right":
                mask = (df_series.index > (h.to_datetime(time_range[0]) - dt.timedelta(milliseconds=1))) & \
                       (df_series.index <= (h.to_datetime(time_range[-1] + dt.timedelta(milliseconds=1))))

            if border_type == "Left":
                mask = (df_series.index >= (h.to_datetime(time_range[0]) - dt.timedelta(milliseconds=1))) & \
                       (df_series.index < (h.to_datetime(time_range[-1] + dt.timedelta(milliseconds=1))))

            if mask is not None:
                df_series = df_series[mask]
            return df_series

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + "\n" + str(tb))
            return pd.DataFrame()


    def summaries(self, time_range, span, summary_type_list, calculation_type, timestamp_calculation):
        pass

    def value(self, timestamp, how="interpolated"):
        pass

    def snapshot(self):
        pass

    def current_value(self, fmt: str=None):
        if self.tag_id is None:
            return False, "[{0}] does not exists. ".format(self.tag_name)

        db = self.container.container[DBTimeSeries]
        collection = db[CollectionLastValues]
        try:
            filter_dict = dict(tag_id=self.tag_id)
            result = collection.find_one(filter_dict, {'_id': False})
            reg = result["series"]

            if fmt is None:
                fmt = init.DEFAULT_DATE_FORMAT
            if fmt == "epoch":
                return True, reg

            reg["timestamp"] = h.to_datetime(reg["timestamp"])
            reg["timestamp"] = reg["timestamp"].strftime(fmt)

            return True, reg
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(tb + "\n" + str(e))
            return False, str(e)

    def sampled_series(self, time_range, span, how="average"):
        pass

    def insert_register(self, register: dict, update_last=True, mongo_client: pm.MongoClient=None, reg_sys=False):
        """
        Insert a new register in the RTDB. Note: update a register must not be done with this function.
        Function "insert_register" inserts a register without checking the timestamp value (for fast writing).
        register dictionaries are saved in the key-value record.

        "series":[ {"value": 1234.567, "timestamp": 1552852232.05, "quality": "Normal"},
                   {"value": 1234.567, "timestamp": 1552852232.05, "quality": "TE.ERROR"}, ... ]

        The inserted register is saved in "tag_type" collection. Possible collections are: "analogs",
        "status", "events", "generics"


        :param mongo_client: if is needed (useful for parallel insertion)
        :param update_last: By default must be True, it update the last value inserted in the data base
        :param register: dictionary with attributes for: (timestamp is UNIX UTC value)
        :param reg_sys: saves number of insertions
        Ex. register = dict(value=1234.567, timestamp=1552852232.053721)

        Note: The internal value of "tag_name" is only for human reference checking, to query correctly
        the "tag_id" should be used
        :return:
        """
        if "timestamp" not in register.keys():
            msg = "Register is not correct. Correct format, \n " \
                  "Ex: dict(value=1234.567, timestamp=1552852232.053721)"
            self.log.error(msg)
            return False, msg

        if self.tag_type is None:
            return False, "[{0}] does not exist in the historian".format(self.tag_name)

        if mongo_client is None:
            cl = pm.MongoClient(**self.container.settings)
        else:
            cl = mongo_client

        try:
            db = cl[DBTimeSeries]
            collection = db[self.tag_type]
            """ Make sure that timestamp is in UNIX UTC format"""
            timestamp = h.to_epoch(register["timestamp"])
            register["timestamp"] = timestamp
            date = h.to_date(timestamp)
            filter_dict = dict(tag_id=self.tag_id)
            filter_dict["n_samples"] = {"$lt": h.n_samples_max}
            filter_dict["date"] = date
            result = collection.update_one(filter_dict,
                                           {
                                                "$push": {"series": register},
                                                "$min": {"first": timestamp},
                                                "$max": {"last": timestamp},
                                                "$inc": {"n_samples": 1},
                                                "$set": {"tag_name": self.tag_name}
                                                # note: tag_name is only for human reference checking
                                                # to query correctly the tag_id should be used
                                           }, upsert=True)
            if update_last:
                # In a batch mode is not necessary to save each register
                # update and number of insertions are done at the end of the operation
                self.update_last_register(register)
            if reg_sys:
                sys_h.register_insertions(1)

            msg = "[{0}] One register was successfully inserted ".format(self.tag_name)
            self.log.debug(msg)
            if mongo_client is None: # close if is not running in parallel fashion
                cl.close()
            return True, msg

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(tb + "\n" + str(e))
            if mongo_client is None: # close if is not running in parallel fashion
                cl.close()
            return False, "[{0}] register does not have a correct format".format(register)

    def update_last_register(self, register):
        """
        Auxiliary function:
        Construct a table with the last values in the RTDB
        :param register:
        Ex. register = dict(value=1234.567, timestamp=1552852232.053721)
        :return:
        """
        db = self.container.container[DBTimeSeries]
        collection = db[CollectionLastValues]
        timestamp = h.to_epoch(register["timestamp"])
        register["timestamp"] = timestamp
        try:
            filter_dict = dict(tag_id=self.tag_id)
            d = h.to_date(timestamp)
            filter_dict["timestamp"] = {"$lte": timestamp}
            result = None
            for it in range(3):
                try:
                    result = collection.update_one(filter_dict,
                                                   {
                                                       "$set": {"timestamp": timestamp,
                                                                "series": register,
                                                                "date": d,
                                                                "tag_name": self.tag_name},
                                                       "$min": {"first": timestamp},
                                                       "$max": {"last": timestamp}
                                                   }, upsert=True)
                    self.log.debug("[{0}] Update last value".format(self.tag_name))
                    break
                except Exception as e:
                    if it == 3:
                        self.log.error(str(e))
                    pass
            return True, result
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(tb + "\n" + str(e))
            return False, str(tb)

    @staticmethod
    def insert_register_as_batch(mongo_client_settings, tag_name, sub_list):
        """
        Auxiliary function: Inserts a list of register using a unique client.
        It´s used for inserting registers in a parallel fashion.
        :param tag_name: This parameters allows to create a TagPoint
        :param mongo_client_settings: Dictionary client configuration: MONGOCLIENT_SETTINGS = {"host":"localhost", "port": 2717,
         "tz_aware": true, ...}
        :param sub_list: list of registers. Ex. [ {"value": 1234.567, "timestamp": 1552852232.05, "quality": "Normal"},
        {"value": 12.567, "timestamp": 1552852235.25, "quality": "TE.ERROR"}, ... ]
        :return:
        """
        rtcontainer = RTContainer()
        tag_point = TagPoint(rtcontainer, tag_name)
        mongo_client = pm.MongoClient(**mongo_client_settings)
        last_register = sub_list[0]
        insertions = 0
        for register in sub_list:
            success, msg = tag_point.insert_register(register, update_last=False, reg_sys=False, mongo_client=mongo_client)
            if register["timestamp"] > last_register["timestamp"]:
                last_register = register
            if success:
                insertions +=1
        tag_point.update_last_register(last_register)
        return insertions

    def insert_many_registers(self, register_list):
        """
        Insert new registers in the RTDB. Note: update registers must not be done with this function
        "insert_many_registers" inserts many register without checking the timestamp value (for fast writing).
        register dictionaries are saved in the key-value record.

        "series":[ {"value": 1234.567, "timestamp": 1552852232.05, "quality": "Normal"},
                   {"value": 12.567, "timestamp": 1552852252.09, "quality": "TE.ERROR"}, ... ]

        The inserted register is saved in "tag_type" collection. Possible collections are: "analogs",
        "status", "events", "generics"


        :param register_list: dictionary or list with attributes for the measurement:
        Ex. register = [{"value":1.23, "timestamp":1552852232.053721, "quality": "Normal"},
        {"value":12.3, "timestamp":1552852282.08, "quality": "Normal"}, ... ]

        Note: The internal value of "tag_name" is only for human reference checking, to query correctly
        the "tag_id" should be used
        :return: {Success (True, False)}, {if success -> # insertions, else -> error msg}
        """
        if not isinstance(register_list, list):
            msg = "register_list is not a list of dictionaries"
            self.log.warning(msg)
            return False, msg

        """ Split register_list in max_workers (to_run) to run in parallel fashion"""
        """ if number of register is lesser than 2 times number of workers """
        max_workers = 5
        if len(register_list) <= max_workers * 2:
            insertions = self.insert_register_as_batch(self.container.settings, tag_name=self.tag_name, sub_list=register_list)
            sys_h.register_insertions(insertions)
            self.log.info(self.tag_name + ": Insertions: " + str(insertions))
            return True, insertions

        """ Split register_list in max_workers (to_run) to run in parallel fashion"""
        workers = min(max_workers, len(register_list)//max_workers + 1)
        sub_lists = it.divide(max_workers, register_list)
        to_run = [(self.container.settings, self.tag_name, list(l)) for l in sub_lists]

        with Pool(processes=workers) as pool:
            # to_run = [(container_settings, tag_name, {"value": 123.56, "timestamp": 121345.343}), (), ...]
            results = pool.starmap(self.insert_register_as_batch, to_run)

        insertions = sum(results)
        self.log.info("Insertions: " + str(results) + ":" + str(insertions))
        sys_h.register_insertions(insertions)
        return True, insertions



    def __str__(self):
        d = self.to_dict()
        return str(d)

    def to_dict(self):
        return dict(container=self.container.container.address,
                    tag_id=self.tag_id,
                    tag_type=self.tag_type,
                    tag_name=self.tag_name)



