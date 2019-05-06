# Created by Roberto Sanchez at 3/17/2019
import pymongo
from my_lib.RTDB.RTDA_BaseClass import RTDAcquisitionSource
from my_lib.RTDB.RTDA_BaseClass import RTDATagPoint
from my_lib.mongo_db_manager import RTDBClasses as h
import settings.initial_settings as init

import pandas as pd
import pymongo as pm
import datetime as dt
import sys, traceback
import more_itertools as it
from multiprocessing import Pool
import logging


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
        return pd.date_range(dt.datetime.now().date(), dt.datetime.now(tz=dt.timezone.utc))

    @staticmethod
    def start_and_time_of(time_range):
        return time_range[0], time_range[-1]

    @staticmethod
    def span(delta_time):
        return pd.Timedelta(**delta_time)

    def interpolated_of_tag_list(self, tag_list, time_range, span, numeric=False):
        pass

    def snapshot_of_tag_list(self, tag_list, time):
        pass

    def find_idTagSeries_for(self, TagName: str):
        try:
            db = self.container[DBTimeSeries]
            collection = db[CollectionTagList]
            result = collection.find_one({"tag_name": TagName})
            return True, result
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)

    def find_tagName(self, tag_id:str):
        try:
            db = self.container[DBTimeSeries]
            collection = db[CollectionTagList]
            result = collection.find_one({"tag_id": tag_id})
            return True, result["tag_name"]
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(str(e) + str(tb))
            return False, str(e)


    def create_tag_point(self, tag_name: str, tag_type: str="generic"):
        tag_type = "TSCL_" + tag_type

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
            msg = "[{0}] Tag created successfully".format(tag_name)
            self.log.info(msg)
            inserted_id = str(tagPoint["_id"])
            cl_tag_list.update_one(tagPoint, {"$set": {"tag_id": inserted_id }}, upsert=True)
            return result, msg
        except Exception as e:
            msg = "[{0}] Duplicated Tag".format(tag_name)
            self.log.warning(msg + "\n" + str(e))
            return None, msg

    def delete_tag_point(self, tag_name: str):
        tag_point = TagPoint(self, tag_name, self.log)
        if tag_point.tag_id is None:
            msg = "There is not tag named " + tag_name
            self.log.warning(msg)
            return False, msg
        collections_to_delete = [CollectionTagList, CollectionLastValues, tag_point.tag_type]
        try:
            db = self.container[DBTimeSeries]
            filter_dict = dict(tag_id=tag_point.tag_id)
            for collection in collections_to_delete:
                cl = db[collection]
                cl.delete_many(filter_dict)
            return True, "Tag point {0} was deleted".format(tag_name)
        except Exception as e:
            msg = "Unable to delete tag_point {0}".format(tag_name)
            self.log.error(msg + "\n" + str(e))
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
        self.tag_name = tag_name
        if logger is None:
            logger = init.LogDefaultConfig().logger
        self.log = logger

        success, search = container.find_idTagSeries_for(tag_name)
        if success and isinstance(search, dict):
            self._id = str(search["_id"])
            self.tag_id = str(search["tag_id"])
            self.tag_type = search["tag_type"]
            self.log.debug("[{0}] Tag was found".format(tag_name))

        else:
            self.log.warning("[{0}]: Tag was not found".format(tag_name))
            print("There is not Tag called: " + self.tag_name)

    def interpolated(self, time_range, span=None, as_df=True, numeric=True, **kwargs):
        df_series = self.recorded_values(time_range, border_type="Inclusive", as_df=as_df, numeric=numeric)
        if numeric:
            df_series = df_series[["value"]].astype(float)
        if span is not None:
            # TODO: Make this case
            pass
        df_result = pd.DataFrame(index=time_range)
        df_result = pd.concat([df_result, df_series], axis=1)
        df_result["value"].interpolate(inplace=True, **kwargs)
        df_result = df_result.loc[time_range]
        return df_result

    def n_values(self, time_range, n_samples, as_df=True, numeric=True):
        new_time_range = pd.date_range(time_range[0], time_range[-1], periods=n_samples)
        df = self.interpolated(new_time_range, as_df=as_df, numeric=numeric)
        return df

    def recorded_values(self, time_range, border_type="Inclusive", as_df=True, numeric=True):
        db = self.container.container[DBTimeSeries]
        collection = db[self.tag_type]
        d_ini, d_end = h.to_date(time_range[0]), h.to_date(time_range[-1])

        """
            f1          l1  f2          l2
            | *         |   |      *    |
              t_ini               t_fin
              t_ini < last
              t_fin > first
        """
        # TODO: add first and last (cases) for inclusive, exclusive and interpolate
        filter_dict = {"tag_id": self.tag_id,
                       "date": {
                           "$gte": d_ini,
                           "$lte": d_end
                       }
                       # "first": {""}
                       }
        cursor = collection.find(filter_dict)
        series = list()
        for it in cursor:
            series += list(it["series"])

        if len(series) == 0:
            return pd.DataFrame()

        df_series = pd.DataFrame(series)
        df_series.set_index(["timestamp"], inplace=True)
        df_series.index = pd.to_datetime(df_series.index, unit='s', utc=True)
        df_series.sort_index(inplace=True)
        # inclusive:
        if border_type == "Inclusive":
            """ Necessary to add microseconds to include 
                Pandas uses nanoseconds but python datetime not.
            """
            mask = (df_series.index >= h.to_datetime(time_range[0]) - dt.timedelta(milliseconds=1)) & \
                   (df_series.index <= (h.to_datetime(time_range[-1] + dt.timedelta(milliseconds=1))))
            df_series=df_series[mask]

        return df_series

    def summaries(self, time_range, span, summary_type_list, calculation_type, timestamp_calculation):
        pass

    def value(self, timestamp, how="interpolated"):
        pass

    def snapshot(self):
        pass

    def current_value(self):
        db = self.container.container[DBTimeSeries]
        collection = db[CollectionLastValues]
        try:
            filter_dict = dict(tag_id=self.tag_id)
            result = collection.find_one(filter_dict, )
            return True, result
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(tb + "\n" + str(e))
            return False, str(e)


    def sampled_series(self, time_range, span, how="average"):
        pass

    def insert_register(self, register: dict, update_last=True, mongo_client: pm.MongoClient=None):
        """
        Insert a new register in the RTDB. Note: update a register must not be done with this function
        "insert_register" insert a register without checking the timestamp value (for fast writing).
        register dictionaries are saved in the key-value record.

        "series":[ {"value": 1234.567, "timestamp": 1552852232.05, "quality": "Normal"},
                   {"value": 1234.567, "timestamp": 1552852232.05, "quality": "TE.ERROR"}, ... ]

        The inserted register is saved in "tag_type" collection. Possible collections are: "analogs",
        "status", "events", "generics"
        :param mongo_client: if is needed (useful for parallel insertion)
        :param update_last: By default must be True, it update the last value inserted in the data base
        :param register: dictionary with attributes for: (timestamp is UNIX UTC value)
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
                self.update_last_register(register)
            self.log.debug("[{0}] One register was successfully inserted ".format(self.tag_name))
            if mongo_client is None:
                cl.close()
            return True, result

        except Exception as e:
            tb = traceback.format_exc()
            self.log.error(tb + "\n" + str(e))
            if mongo_client is None:
                cl.close()
            return False, str(tb)

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
            success, msg = tag_point.insert_register(register, update_last=False, mongo_client=mongo_client)
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
        :return:
        """
        if not isinstance(register_list, list):
            msg = "register_list is not a list of dictionaries"
            self.log.warning(msg)
            return False, msg

        """ Split register_list in max_workers (to_run) to run in parallel fashion"""
        max_workers = 5
        workers = min(max_workers, len(register_list)//max_workers + 1)
        sub_lists = it.divide(max_workers, register_list)
        to_run = [(self.container.settings, self.tag_name, list(l)) for l in sub_lists]

        with Pool(processes=workers) as pool:
            # to_run = [(container_settings, tag_name, {"value": 123.56, "timestamp": 121345.343}), (), ...]
            results = pool.starmap(self.insert_register_as_batch, to_run)

        insertions = sum(results)
        self.log.info("Insertions: " + str(results) + ":" + str(insertions))
        return insertions



    def __str__(self):
        d = self.to_dict()
        return str(d)

    def to_dict(self):
        return dict(container=self.container.container.address,
                    tag_id=self.tag_id,
                    tag_type=self.tag_type,
                    tag_name=self.tag_name)



