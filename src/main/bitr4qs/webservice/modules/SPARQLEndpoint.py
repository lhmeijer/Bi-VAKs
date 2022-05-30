from src.main.bitr4qs.core.BiTR4Qs import BiTR4QsSingleton
from src.main.bitr4qs.query.UpdateQuery import UpdateQuery
from src.main.bitr4qs.namespace import BITR4QS
from flask import Blueprint, request, make_response, current_app
from src.main.bitr4qs.exception import UnsupportedQuery, NonAbsoluteBaseError, SparqlProtocolError
import src.main.bitr4qs.query as queries
from src.main.bitr4qs.request.UpdateRequest import UpdateQueryRequest
from .VersioningEndpoint import versioning_operation

SPARQLEndpoint = Blueprint('sparql_endpoint', __name__)


@SPARQLEndpoint.route("/query", methods=['GET'])
def sparql_query():
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

    queryAtomType = request.values.get('queryAtomType', None) or None

    if queryAtomType == 'VM':
        query = queries.VMQuery(request=request, base=str(BITR4QS))
    elif queryAtomType == 'DM':
        query = queries.DMQuery(request=request, base=str(BITR4QS))
    elif queryAtomType == 'VQ':
        query = queries.VQuery(request=request, base=str(BITR4QS))
    else:
        query = queries.Query(request=request, base=str(BITR4QS))

    try:
        query.translate_query()
    except UnsupportedQuery:
        return make_response('Unsupported Query', 400)
    except NonAbsoluteBaseError:
        return make_response('Non absolute Base URI given', 400)
    except SparqlProtocolError:
        return make_response('Sparql Protocol Error', 400)

    # queryType = 'SelectQuery'
    # # get query type
    # mimetype = _get_best_matching_mime_type(request, queryType)
    # print("mimetype ", mimetype)
    # if not mimetype:
    #     return make_response("Mimetype: {} not acceptable".format(mimetype), 406)
    # query.return_format = mimetype

    try:
        queryResponse = BiTR4QsCore.apply_query(query)
        response = make_response(queryResponse, 200)
        if query.return_format:
            response.headers['Content-Type'] = query.return_format
        if query.number_of_processed_quads:
            response.headers['N-ProcessedQuads'] = query.number_of_processed_quads
        return response
    except Exception as e:
        return make_response('Error after executing the SPARQL query.', 400)


@SPARQLEndpoint.route("/update", methods=['POST'])
def sparql_update():
    try:
        updateQuery = UpdateQuery(request=request, base=str(BITR4QS))
        updateQuery.translate_query()
    except UnsupportedQuery:
        return make_response('Unsupported Query', 400)
    except NonAbsoluteBaseError:
        return make_response('Non absolute Base URI given', 400)
    except SparqlProtocolError:
        return make_response('Sparql Protocol Error', 400)

    updateRequest = UpdateQueryRequest(updateQuery=updateQuery)
    return versioning_operation(revisionRequest=updateRequest)
