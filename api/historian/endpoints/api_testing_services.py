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

ns = api.namespace('test', description='Historian testing operations')

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


