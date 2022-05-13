from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError
import json
from src.main.bitr4qs.exception import SPARQLConnectorException


_response_mime_types = {
    "xml": "application/sparql-results+xml, application/rdf+xml",
    "json": "application/sparql-results+json, application/json",
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "application/rdf+xml": "application/rdf+xml",
    "html": "text/html, application/xhtml+xml",
    "trig": "application/trig",
    "turtle": "text/turtle, application/x-turtle",
    "ntriples": "application/n-triples",
    "trix": "application/trix",
    "nquads": "application/n-quads"
}


class HttpQuadStore(object):

    def __init__(self, queryEndpoint, updateEndpoint):
        self._queryEndpoint = queryEndpoint
        self._updateEndpoint = updateEndpoint

    def _execute_query(self, queryString, returnFormat):
        request = self._create_query_request(queryString, returnFormat)
        try:
            response = urlopen(request)
        except HTTPError as e:
            print(e)
            raise HTTPError
        return response

    def _create_query_request(self, query, returnFormat):

        headers = {"Accept": returnFormat}
        params = {}
        args = {}
        # merge params/headers dicts
        args.setdefault("params", {})

        args.setdefault("headers", {})
        args["headers"].update(headers)

        method = 'GET'
        if method == 'GET':
            params['query'] = query
            args["params"].update(params)
            qsa = "?" + urlencode(args["params"])
            request = Request(self._queryEndpoint + qsa, headers=args["headers"])
        elif method == 'POST':
            args["headers"].update({"Content-Type": "application/sparql-query"})
            qsa = "?" + urlencode(params)
            print(qsa)
            request = Request(self._queryEndpoint + qsa, data=query.encode(), headers=args["headers"])
        elif method == 'POST_FORM':
            params['query'] = query
            args["params"].update(params)
            request = Request(self._queryEndpoint, data=urlencode(args["params"]).encode(), headers=args["headers"])
        else:
            raise SPARQLConnectorException("Unknown method %s" % method)

        return request

    def execute_update_query(self, updateQueryString):
        request = self._create_update_request(updateQueryString, 'application/sparql-results+json')
        print("request ", request.data)
        try:
            response = urlopen(request)
        except HTTPError as e:
            raise HTTPError
        return response

    def _create_update_request(self, query, returnFormat):
        params = {}
        headers = {
            "Accept": returnFormat,
            "Content-Type": "application/sparql-update",
        }
        args = {}  # other QSAs

        args.setdefault("params", {})
        args["params"].update(params)
        args.setdefault("headers", {})
        args["headers"].update(headers)

        qsa = "?" + urlencode(args["params"])
        print(self._updateEndpoint + qsa)
        return Request(self._updateEndpoint + qsa, data=query.encode('utf-8'), headers=args["headers"])

    def _execute_select_query_json(self, selectQueryString):
        print("selectQueryString ", selectQueryString)
        result = self._execute_query(selectQueryString, 'application/sparql-results+json')
        return json.loads(result.read().decode("utf-8"))

    def execute_select_query(self, selectQueryString, returnFormat):
        returnFormat = 'json'
        if returnFormat == 'json':
            return self._execute_select_query_json(selectQueryString)
        return None

    def _execute_construct_query(self, constructQueryString, returnFormat):
        result = self._execute_query(constructQueryString, returnFormat)
        return result.read().decode("utf-8")

    def execute_construct_query(self, constructQueryString, returnFormat):
        return self._execute_construct_query(constructQueryString, _response_mime_types[returnFormat])

    def execute_describe_query(self, describeQuery, returnFormat=None):
        return self._execute_describe_query(describeQuery, _response_mime_types[returnFormat])

    def _execute_describe_query(self, describeQueryString, returnFormat):
        result = self._execute_query(describeQueryString, returnFormat)
        return result.read().decode("utf-8")

    def execute_ask_query(self, askQueryString):
        result = self._execute_query(askQueryString, 'application/sparql-results+json')
        answer = json.loads(result.read().decode("utf-8"))
        if 'boolean' in answer:
            if answer['boolean'] is True:
                return True
            return False
        return result

