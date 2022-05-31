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

    def __init__(self, nameDataset, urlDataset):
        self._nameDataset = nameDataset
        self._urlDataset = urlDataset

    def _execute_query(self, queryString, returnFormat):
        request = self._create_query_request(queryString, returnFormat)
        try:
            response = urlopen(request)
        except HTTPError as e:
            print(e)
            raise HTTPError
        return response

    def _create_query_request(self, query, returnFormat):

        queryEndpoint = "{0}/{1}/query".format(self._urlDataset, self._nameDataset)

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
            request = Request(queryEndpoint + qsa, headers=args["headers"], method='GET')
        elif method == 'POST':
            args["headers"].update({"Content-Type": "application/sparql-query"})
            qsa = "?" + urlencode(params)
            # print(qsa)
            request = Request(queryEndpoint + qsa, data=query.encode(), headers=args["headers"], method='POST')
        elif method == 'POST_FORM':
            params['query'] = query
            args["params"].update(params)
            request = Request(queryEndpoint, data=urlencode(args["params"]).encode(), headers=args["headers"])
        else:
            raise SPARQLConnectorException("Unknown method %s" % method)

        return request

    def execute_update_query(self, updateQueryString):
        request = self._create_update_request(updateQueryString, 'application/sparql-results+json')
        # print("request ", request.data)
        try:
            response = urlopen(request)
        except HTTPError as e:
            raise HTTPError
        return response

    def _create_update_request(self, query, returnFormat):

        updateEndpoint = "{0}/{1}/update".format(self._urlDataset, self._nameDataset)
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
        return Request(updateEndpoint + qsa, data=query.encode('utf-8'), headers=args["headers"], method='POST')

    def _execute_select_query_json(self, selectQueryString):
        # print("selectQueryString ", selectQueryString)
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

    def n_quads_to_store(self, nquads):
        dataEndpoint = "{0}/{1}/data".format(self._urlDataset, self._nameDataset)
        headers = {'Content-Type': 'application/n-quads'}
        nquads = nquads.encode(encoding='utf-8', errors='strict')
        # print("nquads ", nquads)
        # splittedNquads = nquads.split('\n')
        # print("splittedNquads ", splittedNquads)
        # print("self._dataEndpoint ", self._dataEndpoint)
        # index = 0
        # for nquad in splittedNquads:
        #     if index in list(range(100)):
        #         print("{0} {1}".format(index, nquad))
        #     index += 1
        request = Request(dataEndpoint, data=nquads, headers=headers, method='POST')
        try:
            response = urlopen(request)
        except HTTPError as e:
            print(e)
            raise HTTPError
        return response

    def data_of_store(self, returnFormat):
        dataEndpoint = "{0}/{1}/data".format(self._urlDataset, self._nameDataset)
        headers = {'Content-Type': returnFormat}
        request = Request(dataEndpoint, headers=headers, method='GET')
        try:
            response = urlopen(request)
            result = response.read().decode("utf-8")
        except HTTPError as e:
            print(e)
            raise HTTPError
        return result

    @classmethod
    def create_fuseki_dataset(cls, nameDataset, urlDataset):
        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        data = 'dbName={0}&dbType=mem'.format(nameDataset)
        endpoint = '{0}/$/datasets'.format(urlDataset)
        request = Request(endpoint, data=data.encode('utf-8'), headers=headers, method='POST')
        try:
            response = urlopen(request)
            print("response ", response)
        except HTTPError as e:
            print(e)
            raise HTTPError
        return cls(nameDataset=nameDataset, urlDataset=urlDataset)

    def delete_fuseki_dataset(self):
        # headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        endpoint = '{0}/$/datasets/{1}'.format(self._urlDataset, self._nameDataset)
        request = Request(endpoint, method='DELETE')
        try:
            response = urlopen(request)
            print("response ", response.read().decode("utf-8"))
        except HTTPError as e:
            print(e)
            raise HTTPError


