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
from flask import Flask, Blueprint
from settings import initial_settings as init
from api.restplus import api
from api.database import db

# namespaces:
# from api.blog.endpoints.posts import ns as blog_posts_namespace
from api.historian.endpoints.api_services import ns as historian_namespace

app = Flask(__name__)
log = init.LogDefaultConfig().logger


def configure_app(flask_app):
    flask_app.config['SERVER_NAME'] = init.FLASK_SERVER_NAME
    flask_app.config['SQLALCHEMY_DATABASE_URI'] = init.SQLALCHEMY_DATABASE_URI
    flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = init.SQLALCHEMY_TRACK_MODIFICATIONS
    flask_app.config['SWAGGER_UI_DOC_EXPANSION'] = init.RESTPLUS_SWAGGER_UI_DOC_EXPANSION
    flask_app.config['RESTPLUS_VALIDATE'] = init.RESTPLUS_VALIDATE
    flask_app.config['RESTPLUS_MASK_SWAGGER'] = init.RESTPLUS_MASK_SWAGGER
    flask_app.config['ERROR_404_HELP'] = init.RESTPLUS_ERROR_404_HELP


def initialize_app(flask_app):
    configure_app(flask_app)

    blueprint = Blueprint('api', __name__, url_prefix='/api')
    api.init_app(blueprint)
    api.add_namespace(historian_namespace)
    flask_app.register_blueprint(blueprint)

    db.init_app(flask_app)

#################################################

#def add_api_resources():
    """
    The following historian can be accessed as: http://hostname:port/api/historian/parameters
    :return:
    """
    # Specify the api prefix to use
#    app.register_blueprint(api_bp, url_prefix='/api')
    # Test Class, only for testing purposes:
#    api.add_resource(api_sr.SimpleClass, '/test_dict', '/test_dict/<string:test_id>')

    """ Tag Point Snap shoot """
#    api.add_resource(api_sr.SnapShoot, '/snapshoot/<string:tag_name>')

    """ Tag TimeSeries (recorded values, as they were recorded in DB)"""
#    api.add_resource(api_sr.RecordedValues,'/recorded-values/<string:tag_name>',
#                     '/recorded-values/<string:tag_name>/<string:start_time>/<string:end_time>')

    """ Tag TimeSeries (interpolated values using a specific span between start_time and end_time """
#    api.add_resource(api_sr.InterpolatedValues, '/interpolated-values/<string:tag_name>',
#                     '/interpolated-values/<string:tag_name>/<string:start_time>/<string:end_time>/<string:span>')

############################################

""" Adding the api historian"""
# add_api_resources()


def main():
    initialize_app(app)
    log.info('>>>>> Starting development server at http://{}/api/ <<<<<'.format(app.config['SERVER_NAME']))
    app.run(debug=init.FLASK_DEBUG)


if __name__ == '__main__':
    # TODO: remove debug=True for production environment
    main()
