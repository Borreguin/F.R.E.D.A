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

ns = api.namespace('single', description='Historian single operations')


@ns.route('/snapshoot/<string:tag_name>')
class SnapShoot(Resource):

    @api.expect(arg_from.format_time)
    def get(self, tag_name):
        """
            Returns a SnapShoot for an Analog TagPoint {tag_name} at current time (last_value) with {time_format}
        """
        args = arg_from.format_time.parse_args(request)
        fmt = args.get("format_time", None)
        # init container where time series are saved
        cntr = dr.RTContainer()
        # define arg_from tag point (entity for time series)
        tag_point = dr.TagPoint(cntr, tag_name)

        success, result = tag_point.current_value(fmt)
        cntr.close()
        if success:
            return dict(sucess=True, result=result)
        else:
            log.error(result)
            return dict(success=False, error="Tag not found or there is no register for this tag")

    @api.response(400, 'Incorrect format of the register')
    @api.expect(ser_from.register)
    def post(self, tag_name):
        """
        Creates a new snapshoot in the historian
        """
        register = request.data
        try:
            register = json.loads(register)
            # init container where time series are saved
            cntr = dr.RTContainer()
            # define arg_from tag point (entity for time series)
            tag_point = dr.TagPoint(cntr, tag_name)
            success, result = tag_point.insert_register(register)
            cntr.close()
            return dict(success=success, result=str(result))
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e) + "\n" + str(tb))
            return dict(success=False, error="tag_name was not found or register is not correct")


@ns.route('/recorded_values/<string:tag_name>')
class RecordedValues(Resource):

    @api.expect(arg_from.range_time)
    def get(self, tag_name):
        """
        Returns a list of registers between {start_time} and {end_time} for a specific {tag_name}
        """
        args = arg_from.range_time.parse_args(request)
        start_time = args.get("start_time", None)
        end_time = args.get("end_time", None)
        format_time = args.get("format_time", None)
        start_time, end_time, format_time = apf.time_range_validation_w_format_time(start_time, end_time, format_time)

        if start_time is None and end_time is None:
            return dict(success=False, result="Unable to convert dates. Try the following formats"
                                              + str(init.SUPPORTED_FORMAT_DATES))

        cntr = dr.RTContainer()
        time_range = cntr.time_range(start_time, end_time)
        tag_point = dr.TagPoint(cntr, tag_name)
        # numeric=False implies data is obtained as it was saved in DB (without change)
        # numeric=True force values to be numeric
        result = tag_point.recorded_values(time_range, numeric=False)
        cntr.close()
        if len(result.index) > 0:
            result["timestamp"] = [x.strftime(format_time) for x in result.index]
        return dict(success=True, result=result.to_dict(orient='register'))

    @api.expect(ser_from.register_list)
    def post(self, tag_name):
        """
        Inserts a list of new registers for a TagPoint {tag_name}
        """
        request_data = request.data
        try:
            register_list = json.loads(request_data)["registers"]
            cntr = dr.RTContainer()
            tag_point = dr.TagPoint(cntr, tag_name)
            success, result = tag_point.insert_many_registers(register_list)
            cntr.close()
            return dict(success=success, result=result)
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e) + "\n" + str(tb))
            return dict(success=False, result="tag_name was not found or register is not correct")


@ns.route('/interpolated_values/<string:tag_name>')
class InterpolatedValues(Resource):

    @api.expect(arg_from.range_time_with_span_and_method)
    def get(self, tag_name):
        """ Returns a interpolated time series for {tag_name} from {start_time} to {end_time} in {span} intervals"""
        args = arg_from.range_time_with_span_and_method.parse_args(request)
        start_time = args.get("start_time", None)
        end_time = args.get("end_time", None)
        format_time = args.get("format_time", None)
        span = args.get("span", "15 min")
        method = args.get("method", None)
        start_time, end_time, format_time = apf.time_range_validation_w_format_time(start_time, end_time, format_time)
        valid, v_method = apf.validate_method(method)

        if start_time is None and end_time is None:
            return dict(success=False, result="Unable to recognize {start_time}, {end_time}")
        if not valid:
            return dict(success=False, result="[{0}] is not a valid method."
                                              " Review ./help/interpolated/method".format(method))

        try:
            cntr = dr.RTContainer()
            time_range = cntr.time_range(start_time,end_time, freq=span)
            tag_point = dr.TagPoint(cntr, tag_name)
            # numeric=False implies data is obtained as it was saved in DB (without change)
            # numeric=True force values to be numeric
            result = tag_point.interpolated(time_range, limit=1, limit_area="inside", method=v_method)
            result = result.where((notnull(result)), None)
            result["timestamp"] = [x.strftime(format_time) for x in result.index]
            result = result.to_dict(orient='register')
            cntr.close()
            return dict(success=True, result=result)
        except Exception as e:
            tb = traceback.format_exc()
            log.error(str(e) + "\n" + str(tb))
            return dict(success=False, result="Unable to recognize span parameter. Help: \n" + str(apf.freq_options))



