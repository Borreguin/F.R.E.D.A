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
from settings import initial_settings as init
from api.restplus import api as api_p
# from api.database import db

# namespaces:
# from api.blog.endpoints.posts import ns as blog_posts_namespace
from api.historian.endpoints.api_admin_services import ns as historian_namespace_admin
from api.historian.endpoints.api_single_services import ns as historian_namespace_single
from api.historian.endpoints.api_list_services import ns as historian_namespace_list

app = Flask(__name__)
log = init.LogDefaultConfig().logger


def configure_app(flask_app):
    # flask_app.config['SERVER_NAME'] = init.FLASK_SERVER_NAME
    # flask_app.config['SQLALCHEMY_DATABASE_URI'] = init.SQLALCHEMY_DATABASE_URI
    # flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = init.SQLALCHEMY_TRACK_MODIFICATIONS
    flask_app.config['SWAGGER_UI_DOC_EXPANSION'] = init.RESTPLUS_SWAGGER_UI_DOC_EXPANSION
    flask_app.config['RESTPLUS_VALIDATE'] = init.RESTPLUS_VALIDATE
    flask_app.config['RESTPLUS_MASK_SWAGGER'] = init.RESTPLUS_MASK_SWAGGER
    flask_app.config['ERROR_404_HELP'] = init.RESTPLUS_ERROR_404_HELP


blueprint = Blueprint('api', __name__, url_prefix='/api')


@blueprint.route("/test")
def b_test():
    return "This is a test. Blueprint is working correctly."


api_p.init_app(blueprint)
api_p.add_namespace(historian_namespace_admin)
api_p.add_namespace(historian_namespace_single)
api_p.add_namespace(historian_namespace_list)
app.register_blueprint(blueprint)
configure_app(app)


@app.route("/")
def main_page():
    """ Adding initial page """
    return "Hello from FastCGI via IIS! Testing an empty web Page 20"


def main():
    log.info('>>>>> Starting development server at http://{}/api/ <<<<<'.format(app.config['SERVER_NAME']))
    app.run(debug=init.FLASK_DEBUG)


if __name__ == "__main__":
    main()
