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

# from api.historian.parsers import test_arguments
from api.historian import parsers as arg_from
from api.historian import serializers as ser_from

ns = api.namespace('hist', description='Historian operations for saving time series data')

''' Test dictionary '''
test_dict = dict(a='This a class for testing')


@ns.route('/test_dict')
class SimpleClass(Resource):

    @api.expect(arg_from.test)
    def get(self):
        """
            Returns a stored value for a {test_id} index
        """
        args = arg_from.test.parse_args(request)
        test_id = args.get("test_id", "arg_from")
        if test_id in test_dict.keys():
            return {test_id: test_dict[test_id]}
        else:
            return {'error': '{0} was not found'.format(test_id)}


@ns.route('/test_dict/<string:test_id>')
class SimpleClassWithID(Resource):

    @api.marshal_with(ser_from.test_out) # expected output
    @api.expect(ser_from.test)  # expected entry
    def put(self, test_id='a'):
        """
        Update a stored value in the {test_id} register
        """
        data = request.json
        test_dict[test_id] = data["value"]
        return dict(test_id=test_id, value=test_dict[test_id], success=True)


@ns.route('/tag/<string:tag_name>')
class Tag_name(Resource):

    def get(self, tag_name:str="TagNameToSearch"):
        """
        Check whether a TagPoint exists or not in the historian
        :param tag_name:
        :return:
        """
        cntr = dr.RTContainer()
        try:
            success, result = cntr.find_tag_point_by_name(tag_name)
            if success:
                result.pop("_id")
            return dict(success=success, result=result)
        except Exception as e:
            return dict(success=False, errors=str(e))

    @api.expect(ser_from.tag_update)
    def put(self, tag_name:str="TagNameToSearch"):
        """
        Updates the name of a TagPoint
        """
        request_data = request.json
        new_tag_name = request_data["new_tag_name"]
        cntr = dr.RTContainer()
        success, result = cntr.update_tag_name(tag_name, new_tag_name)
        return dict(success=success, result=result)


@ns.route('/tag')
class Tag(Resource):
    @api.response(400, 'Unable to create a new TagPoint')
    @api.expect(ser_from.tag)
    def post(self):
        """
        Creates a new TagPoint
        """
        request_data = request.json
        tag_name = request_data.pop("tag_name")
        cntr = dr.RTContainer()
        try:
            if "tag_type" in request_data.keys():
                if len(request_data["tag_type"]) <= 0:
                    request_data["tag_type"] = "generic"
                success, result = cntr.create_tag_point(tag_name, request_data["tag_type"])
            else:
                success, result = cntr.create_tag_point(tag_name)
            cntr.close()
            return dict(success=success, result=result)
        except Exception as e:
            cntr.log.error("Unable to create a TagPoint \n" + str(e))
            cntr.close()
            return None, 400

    @api.response(400, 'Unable to delete a TagPoint')
    @api.expect(ser_from.tag_delete)
    def delete(self):
        """
        Deletes a TagPoint
        For security reasons the tag_type should match, otherwise the TagPoint cannot be deleted
        """
        request_data = request.json
        cntr = dr.RTContainer()
        try:
            tag_name = request_data["tag_name"]
            tag_type = request_data["tag_type"]
            success, result = cntr.delete_tag_point(tag_name, tag_type)
            cntr.close()
            return dict(success=success, result=result)
        except Exception as e:
            log.error(str(e))
            cntr.log.error("Unable to delete a TagPoint \n")
            cntr.close()
            return None, 400


@ns.route('/tags')
@ns.route('/tags/<string:filter_exp>')
class NewUpdateTag(Resource):

    def get(self, filter_exp=None):
        """
        Returns all existing TagPoints
        filter_exp: Expression for filter TagNames. * can be used as wildcard
        """
        cntr = dr.RTContainer()
        success, result = cntr.find_all_tags(filter_exp)
        return dict(success=success, result=result)


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
            log.error(str(e))
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
            log(str(e))
            return dict(success=False, result="tag_name was not found or register is not correct")


@ns.route('/interpolated_values/<string:tag_name>')
class InterpolatedValues(Resource):

    @api.expect(arg_from.range_time_with_span)
    def get(self, tag_name):
        """ Returns a interpolated time series for {tag_name} from {start_time} to {end_time} in {span} intervals"""
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
            tag_point = dr.TagPoint(cntr, tag_name)
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
        args = arg_from.tag_list.parse_args()["tag_names"]
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
                success, result = tag_point.insert_register(register)
                if success:
                    success_acc = (success and True)
                    insertions += 1
                else:
                    success_acc = (success and True)
                    err += result

            cntr.close()
            return dict(success=success_acc, result=insertions, error=err)
        except Exception as e:
            log(str(e))
            cntr.close()
            return dict(success=False, result=None, error=str(e))
