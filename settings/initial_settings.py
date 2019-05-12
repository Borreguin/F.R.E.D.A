# Created by Roberto Sanchez at 3/29/2019
# -*- coding: utf-8 -*-
""" Set the initial settings of this application"""
import sys

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
import pymongo as pm
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import logging
import subprocess
import os
import json

""" script path"""
script_path = os.path.dirname(os.path.abspath(__file__))

""" read config.json configurations """

with open(os.path.join(script_path, "config.json")) as json_file:
    cfg_json = json.load(json_file)

""" Log file settings: """
file_name = cfg_json["ROTATING_FILE_HANDLER"]["filename"]
log_path = script_path.replace("settings", "logs")
log_file_name = os.path.join(log_path, file_name)
cfg_json["ROTATING_FILE_HANDLER"]["filename"] = log_file_name
ROTATING_FILE_HANDLER = cfg_json["ROTATING_FILE_HANDLER"]
ROTATING_FILE_HANDLER_LOG_LEVEL = cfg_json["ROTATING_FILE_HANDLER_LOG_LEVEL"]

""" Settings for Mongo Client"""
MONGOCLIENT_SETTINGS = cfg_json["MONGOCLIENT_SETTINGS"]
MONGO_LOG_LEVEL = cfg_json["MONGO_LOG_LEVEL"]
MONGO_LOG_LEVEL_OPTIONS = cfg_json["MONGO_LOG_LEVEL_OPTIONS"]
MONGOEXE = None

""" Windows version: """
if os.name == "nt":
    MONGOPATH = os.path.join(cfg_json["MONGOPATH_WINDOWS"])
    MONGOEXE = os.path.join(cfg_json["MONGOEXE_WINDOWS"])

""" Posix version: """
if os.name == "posix":
    # TODO: CHECK FOR POSIX SYSTEMS
    MONGOPATH = os.path.join(cfg_json["MONGOPATH_POSIX"])
    MONGOEXE = os.path.join(cfg_json["MONGOEXE_POSIX"])


""" MONGODB databases """
DB_MONGO_NAME = cfg_json["DB_NAME"]
DB_TIME_SERIES = cfg_json["DB_TIME_SERIES"]
CL_TAG_LIST = cfg_json["CL_TAG_LIST"]
CL_LAST_VALUES = cfg_json["CL_LAST_VALUES"]

""" SUPPORTED DATES """
SUPPORTED_FORMAT_DATES = cfg_json["SUPPORTED_FORMAT_DATES"]
DEFAULT_DATE_FORMAT = cfg_json["DEFAULT_DATE_FORMAT"]


"""" FLASK CONFIGURATION """
FLASK_SERVER_NAME = cfg_json["FLASK_SERVER_NAME"]
FLASK_DEBUG = cfg_json["FLASK_DEBUG"]

RESTPLUS_SWAGGER_UI_DOC_EXPANSION = cfg_json["RESTPLUS_SWAGGER_UI_DOC_EXPANSION"]
RESTPLUS_VALIDATE = cfg_json["RESTPLUS_VALIDATE"]
RESTPLUS_MASK_SWAGGER = cfg_json["RESTPLUS_MASK_SWAGGER"]
RESTPLUS_ERROR_404_HELP = cfg_json["RESTPLUS_ERROR_404_HELP"]

SQLALCHEMY_DATABASE_URI = cfg_json["SQLALCHEMY_DATABASE_URI"]
SQLALCHEMY_TRACK_MODIFICATIONS = cfg_json["SQLALCHEMY_TRACK_MODIFICATIONS"]

class MongoClientDefaultConfig():
    """
    Default configuration for the MongoClient to use in the entire application:
    """
    def __init__(self, mongo_client_settings: dict=None):
        self.lg = LogDefaultConfig()
        if mongo_client_settings is None:
            self.client = pm.MongoClient(**MONGOCLIENT_SETTINGS)
        else:
            self.client = pm.MongoClient(**mongo_client_settings)
        self.mongo_path = MONGOPATH
        self.check_mongo_db()

    def exists_mongo_path(self):
        """
        :return: Check if the DB directory exists
        """
        return os.path.isdir(self.mongo_path)

    def default_client(self) -> pm.mongo_client:
        """
        :return: Get acess to the default client
        """
        return self.client

    def setup_mongo_db(self):
        """
        :return: if the FREDA DB directory does not exist then create one
        """
        if not self.exists_mongo_path():
            try:
                os.makedirs(self.mongo_path)
            except Exception as e:
                self.lg.logger.warning("Unable to find the path for the default data base. Check: initial_settings.py")
                self.lg.logger.error(e)
                return False
        return True

    def is_db_running_win_os(self):
        """
        :return: Running MongoDb for windows
        """
        try:
            self.lg.logger.info("Starting MongoDb!")
            TO_RUN = MONGOEXE + " --dbpath " + MONGOPATH + " --port " + str(MONGOCLIENT_SETTINGS["port"])
            """ Write Logs from MongoDb if is needed """
            otp = None
            if MONGO_LOG_LEVEL in MONGO_LOG_LEVEL_OPTIONS:
                if MONGO_LOG_LEVEL == "ON":
                    otp = open(os.path.join(log_path, "mongoDb.log"), "w")
                else:
                    otp = None
            subprocess.Popen(TO_RUN, stdout=otp)
            self.lg.logger.info("MongoDb is running now!")
        except Exception as e:
            self.lg.logger.error(e)

    # TODO: create function for posix OS
    def is_db_running_posix_os(self):
        pass

    def check_mongo_db(self):
        """
        :return: Check if an instance of the MongoDb is running
        """
        try:
            self.client.server_info()
        except Exception as e:
            self.lg.logger.warning(e)
            self.lg.logger.info("The MongoDb is not running. Starting a new instance")
            if self.setup_mongo_db():
                if os.name == "nt":
                    self.is_db_running_win_os()
                if os.name == "posix":
                    self.is_db_running_win_os()


class LogDefaultConfig():
    """
    Default configuration for the logger file:
    """
    def __init__(self, log_name: str=None):
        if log_name is not None:
            log_file_name = os.path.join(log_path, log_name)
            ROTATING_FILE_HANDLER["filename"] = log_file_name
        logging.basicConfig(
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[
                                RotatingFileHandler(**ROTATING_FILE_HANDLER),
                                StreamHandler(sys.stdout)
                            ])

        self.logger = logging.getLogger(log_name)
        level = ROTATING_FILE_HANDLER_LOG_LEVEL["value"]
        options = ROTATING_FILE_HANDLER_LOG_LEVEL["options"]
        if level in options:
            if level == "error":
                self.logger.setLevel(logging.ERROR)
            if level == "warning":
                self.logger.setLevel(logging.WARNING)
            if level == "debug":
                self.logger.setLevel(logging.DEBUG)
            if level == "info":
                self.logger.setLevel(logging.INFO)
            if level == "off":
                self.logger.setLevel(logging.NOTSET)
        else:
            self.logger.setLevel(logging.ERROR)


def main():
    pass


if __name__ == '__main__':
    main()
