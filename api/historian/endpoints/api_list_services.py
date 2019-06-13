# Created by Roberto Sanchez at 4/16/2019
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
import traceback

from flask_restplus import Resource
from flask import request
from api.historian.endpoints import api_functions as apf
from my_lib.mongo_db_manager import RTDB_mongo_driver as dr
from settings import initial_settings as init
from api.restplus import api
from pandas import notnull
import json

log = init.LogDefaultConfig("web_service.log").logger

from api.historian import parsers as arg_from
from api.historian import serializers as ser_from
from my_lib.mongo_db_manager import RTDB_system as sys_h

ns = api.namespace('list', description='Historian list operations')


@ns.route('/snapshoot')
class SnapShoot(Resource):

    @api.expect(arg_from.tag_list_w_time_format)
    def get(self):
        """
            Returns SnapShoot for a list of tags {tag_list} at current time (last_value) with {time_format}
        """
        args = arg_from.tag_list_w_time_format.parse_args(request)
        tag_list = args.get("tag_list", None)
        tag_list = ''.join(tag_list).split(',')
        fmt = args.get("format_time", None)

        # init container where time series are saved
        cntr = dr.RTContainer()
        success, result = cntr.current_value_of_tag_list(tag_list, fmt)
        cntr.close()
        if success:
            result["success"] = True
            return result
        else:
            log.error(result)
            return dict(success=False, error=result)


@ns.route('/recorded_values')
class RecordedValues(Resource):

    @api.expect(arg_from.tag_list_time_range_w_time_format)
    def get(self):
        """
        Returns the recorded values between {start_time} and {end_time} for each tag_name specified in {tag_list}
        """
        args = arg_from.tag_list_time_range_w_time_format.parse_args(request)
        tag_list = args.get("tag_list", None)
        tag_list = ''.join(tag_list).split(',')
        start_time = args.get("start_time", None)
        end_time = args.get("end_time", None)
        format_time = args.get("format_time", None)
        start_time, end_time, format_time = apf.time_range_validation_w_format_time(start_time, end_time, format_time)

        if start_time is None and end_time is None:
            return dict(success=False, result="Unable to convert dates. Try the following formats"
                                              + str(init.SUPPORTED_FORMAT_DATES))

        cntr = dr.RTContainer()
        time_range = cntr.time_range(start_time, end_time)
        success, df = cntr.recorded_values_of_tag_list(tag_list, time_range, format_time)

        cntr.close()

        return dict(success=success, result=df)

    @api.expect(ser_from.register_for_tag_list)
    def post(self):
        """
        Inserts new registers for a {tag_list}

        Post a list of register for each {tag_name} where {list} is the list of dictionaries: {tag_name, registers}
        """
        request_data = request.data
        try:
            _list = json.loads(request_data)["list"]
            cntr = dr.RTContainer()
            for item in _list:
                tag_point = dr.TagPoint(cntr, item["tag_name"])
                if tag_point.tag_id is None:
                    continue
                registers = item["registers"]
                success, result = tag_point.insert_many_registers(registers)
            cntr.close()
            return dict(success=success, result=result)
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e) + "\n" + str(tb))
            return dict(success=False, result="tag_name was not found or register is not correct")


@ns.route('/interpolated_values/<string:tag_list>')
class InterpolatedValues(Resource):

    @api.expect(arg_from.range_time_with_span)
    def get(self, tag_list):
        """ Returns a interpolated time series for {tag_list} from {start_time} to {end_time} in {span} intervals"""
        args = arg_from.range_time_with_span.parse_args(request)
        start_time = args.get("start_time", None)
        end_time = args.get("end_time", None)
        format_time = args.get("format_time", None)
        span = args.get("span", "15 min")
        start_time, end_time, format_time = apf.time_range_validation_w_format_time(start_time, end_time, format_time)
        if start_time is None and end_time is None:
            return dict(success=False, result="Unable to recognize {start_time}, {end_time}")

        try:
            cntr = dr.RTContainer()
            time_range = cntr.time_range(start_time,end_time, freq=span)
            tag_point = dr.TagPoint(cntr, tag_list)
            # numeric=False implies data is obtained as it was saved in DB (without change)
            # numeric=True force values to be numeric
            result = tag_point.interpolated(time_range)
            result = result.where((notnull(result)), None)
            result["timestamp"] = [x.strftime(format_time) for x in result.index]
            result = result.to_dict(orient='register')
            cntr.close()
            return dict(success=True, result=result)
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e) + "\n" + str(tb))
            return dict(success=False, result="Unable to recognize span parameter. Help: \n" + str(apf.freq_options))


@ns.route('/registers')
class RegistersForTags(Resource):

    @api.expect(arg_from.tag_list)
    def get(self):
        """ Returns the last registers values for a {list of tag_names} """
        args = arg_from.tag_list.parse_args()["tag_list"]
        tag_names = ''.join(args).split(',')
        if tag_names is None:
            return dict(success=False, result=dict(), error="Unable to recognize list of tags")
        list_reg = list()
        cntr = dr.RTContainer()
        err = str()
        success_acc = True
        for tag_name in tag_names:
            tag_point = dr.TagPoint(cntr, tag_name)
            success, reg = tag_point.current_value()
            if success:
                success_acc = (success and True)
                reg["tag_name"] = tag_name
                list_reg.append(reg)
            else:
                success_acc = (success and False)
                err += reg
        cntr.close()
        return dict(succes=success_acc, result=list_reg, error=err)

    @api.expect(ser_from.register_tag_name_list)
    def post(self):
        """ Post registers for several TagPoints.
            The expected structure should have the {tag_name}, {timestamp} and {value}, others attributes can be added
            Ex: [{"tag_name": "dev1.voltaje", "timestamp": "2019-01-01",
            "value": 12.5, "quality": "normal", ...}, {...}]
        """
        request_data = request.data
        err = str()
        insertions = 0
        cntr = dr.RTContainer()
        try:
            register_list = json.loads(request_data)["registers"]
            success_acc = True
            for register in register_list:
                tag_name = register.pop("tag_name")
                tag_point = dr.TagPoint(cntr, tag_name)
                if tag_point.tag_id is None:
                    err += "[{0}] does not exist. ".format(tag_name)
                    success_acc = (success_acc and False)
                    continue
                success, result = tag_point.insert_register(register, update_last=True, reg_sys=False)
                if success:
                    success_acc = (success and True)
                    insertions += 1
                else:
                    success_acc = (success and True)
                    err += result

            sys_h.register_insertions(insertions)
            cntr.close()
            return dict(success=success_acc, result=insertions, error=err)
        except Exception as e:
            log.error(str(e))
            cntr.close()
            return dict(success=False, result=None, error=str(e))
