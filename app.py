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

    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""
from flask import Flask
from flask import Blueprint
from flask_restful import Api
from resources import api_services as api_sr

app = Flask(__name__)
api_bp = Blueprint('api', __name__)
api = Api(api_bp)


def add_api_resources():
    """
    The following resources can be accessed as: http://hostname:port/api/resources/parameters
    :return:
    """
    # Specify the api prefix to use
    app.register_blueprint(api_bp, url_prefix='/api')
    # Test Class, only for testing purposes:
    api.add_resource(api_sr.SimpleClass, '/test', '/test/<string:test_id>')

    """ Tag Point Snap shoot """
    api.add_resource(api_sr.SnapShoot, '/snapshoot/<string:tag_name>')


""" Adding the api resources"""
add_api_resources()


if __name__ == '__main__':
    # TODO: remove debug=True for production environment
    app.run(debug=True)
