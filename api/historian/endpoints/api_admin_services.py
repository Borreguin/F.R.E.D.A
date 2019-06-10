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
from my_lib.mongo_db_manager import RTDB_mongo_driver as dr
from settings import initial_settings as init
from api.restplus import api
from pandas import notnull
import json

log = init.LogDefaultConfig("web_service.log").logger

# from api.historian.parsers import test_arguments
from api.historian import parsers as arg_from
from api.historian import serializers as ser_from
from my_lib.mongo_db_manager import RTDB_system as sys_h

ns = api.namespace('admin', description='Historian admin operations')


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


