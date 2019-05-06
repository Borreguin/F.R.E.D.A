# Created by Roberto Sanchez at 4/16/2019
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
from flask_restful import Resource
from my_lib.mongo_db_manager import RTDB_mongo_driver as dr
from flask import request
import json

''' Test dictionary '''
test = dict(a='This a class for testing')


class SimpleClass(Resource):

    def get(self, test_id='a'):
        if test_id in test.keys():
            return {test_id: test[test_id]}
        else:
            return {'error': '{0} was not found'.format(test_id)}

    def put(self, test_id='a'):
        test[test_id] = request.form['data']
        return {test_id: test[test_id]}


class SnapShoot(Resource):

    def get(self, tag_name):
        cntr = dr.RTContainer()
        tag_point = dr.TagPoint(cntr, tag_name)
        success, result = tag_point.current_value()
        cntr.close()
        if success:
            return result
        else:
            return dict(error= "Tag not found")

    def put(self, tag_name):
        register = request.data
        try:
            register = json.loads(register)
            cntr = dr.RTContainer()
            tag_point = dr.TagPoint(cntr, tag_name)
            success, result = tag_point.insert_register(register)
            cntr.close()
            return dict(success=success)
        except Exception as e:
            return dict(error="tag_name or register is not correct")
