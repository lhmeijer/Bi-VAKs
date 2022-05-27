from src.main.bitr4qs.core.BiTR4Qs import BiTR4QsSingleton
from src.main.bitr4qs.query.UpdateQuery import UpdateQuery
from src.main.bitr4qs.namespace import BITR4QS
from flask import Blueprint, request, make_response, current_app
from src.main.bitr4qs.exception import UnsupportedQuery, NonAbsoluteBaseError, SparqlProtocolError
import src.main.bitr4qs.query as queries
from src.main.bitr4qs.request.UpdateRequest import UpdateQueryRequest
from .VersioningEndpoint import versioning_operation

ApplicationEndpoint = Blueprint('application_endpoint', __name__)


@ApplicationEndpoint.route("/quads", methods=['GET'])
def number_of_quads():
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

    try:
        numberOfQuads = BiTR4QsCore.get_number_of_quads_in_revision_store()
        response = make_response(numberOfQuads, 200)
        return response
    except Exception as e:
        return make_response('Error after executing number of quads in revision store.', 400)


@ApplicationEndpoint.route("/revision/<path:revisionID>", methods=['GET'])
def revision(revisionID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

    try:
        numberOfQuads = BiTR4QsCore.get_number_of_quads_in_revision_store()
        response = make_response(numberOfQuads, 200)
        return response
    except Exception as e:
        return make_response('Error after executing number of quads in revision store.', 400)