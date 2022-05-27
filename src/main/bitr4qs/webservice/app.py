from flask import Flask, make_response
from src.main.bitr4qs.configuration import initialise


def create_app(arguments):
    app = Flask(__name__)

    register_app(app, arguments)
    register_extensions(app)
    register_blueprints(app)
    register_error_handlers(app)

    return app


def register_app(app, arguments):

    config = initialise(arguments)
    app.config['BiTR4QsConfiguration'] = config


def register_extensions(app):
    """Register Flask extensions."""
    pass


def register_blueprints(app):
    """Register Flask blueprints."""

    from src.main.bitr4qs.webservice.modules.SPARQLEndpoint import SPARQLEndpoint
    from src.main.bitr4qs.webservice.modules.VersioningEndpoint import versioningEndpoint
    from src.main.bitr4qs.webservice.modules.ApplicationEndpoint import ApplicationEndpoint

    # origins = app.config.get('CORS_ORIGIN_WHITELIST', '*')
    # cors.init_app(endpoint, origins=CORS_ORIGIN_WHITELIST)
    app.register_blueprint(SPARQLEndpoint)
    app.register_blueprint(versioningEndpoint)
    app.register_blueprint(ApplicationEndpoint)

    @app.route("/")
    def index():
        return make_response('Unsupported Query', 400)


def register_error_handlers(app):
    pass