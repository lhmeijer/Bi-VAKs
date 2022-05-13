from src.main.bitr4qs.core.BiTR4Qs import BiTR4QsSingleton
from src.main.bitr4qs.query.UpdateQuery import UpdateQuery
from src.main.bitr4qs.namespace import BITR4QS
from flask import Blueprint, request, make_response, current_app
from src.main.bitr4qs.exception import UnsupportedQuery, NonAbsoluteBaseError, SparqlProtocolError
import src.main.bitr4qs.query as queries
from src.main.bitr4qs.request.UpdateRequest import UpdateQueryRequest

SPARQLEndpoint = Blueprint('sparql_endpoint', __name__)
resultSetMimetypesDefault = 'application/sparql-results+json'
askMimetypesDefault = 'application/sparql-results+json'
rdfMimetypesDefault = 'text/turtle'

resultSetMimetypes = ['application/sparql-results+xml', 'application/xml',
                      'application/sparql-results+json', 'application/json', 'text/csv',
                      'text/html', 'application/xhtml+xml']
askMimetypes = ['application/sparql-results+xml', 'application/xml',
                'application/sparql-results+json', 'application/json', 'text/html',
                'application/xhtml+xml']
rdfMimetypes = ['text/turtle', 'application/x-turtle', 'application/rdf+xml', 'application/xml',
                'application/n-triples', 'application/trig', 'application/ld+json',
                'application/json']


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

    queryType = 'SelectQuery'
    # get query type
    mimetype = _get_best_matching_mime_type(request, queryType)
    print("mimetype ", mimetype)
    if not mimetype:
        return make_response("Mimetype: {} not acceptable".format(mimetype), 406)
    query.return_format = mimetype

    try:
        queryResponse = BiTR4QsCore.apply_query(query)
        print("queryResponse ", queryResponse)
        response = make_response(queryResponse, 200)
        response.headers['Content-Type'] = mimetype
        return response
    except Exception as e:
        return make_response('Error after executing the SPARQL query.', 400)


@SPARQLEndpoint.route("/update", methods=['POST'])
def sparql_update():
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

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
    try:
        update = BiTR4QsCore.apply_versioning_operation(updateRequest, 'update')
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the update query.', 400)


def _get_best_matching_mime_type(req, queryType):
    if queryType == 'SelectQuery':
        mimetype_default = resultSetMimetypesDefault
        mimetype_list = resultSetMimetypes
    elif queryType == 'AskQuery':
        mimetype_default = askMimetypesDefault
        mimetype_list = askMimetypes
    elif queryType in ['ConstructQuery', 'DescribeQuery']:
        mimetype_default = rdfMimetypesDefault
        mimetype_list = rdfMimetypes
    else:
        mimetype_default = ''
        mimetype_list = []

    match_list = [mimetype_default] + mimetype_list
    if 'Accept' in req.headers:
        mimetype = req.accept_mimetypes.best_match(match_list, None)
    else:
        mimetype = mimetype_default

    return mimetype
