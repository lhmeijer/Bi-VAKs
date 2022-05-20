from .HttpQuadStore import HttpQuadStore
from rdflib.term import URIRef, Literal
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError
import json
from src.main.bitr4qs.exception import SPARQLConnectorException


class TemporalQuadStore(HttpQuadStore):

    def __init__(self, queryEndpoint, updateEndpoint, dataEndpoint, effectiveDate: Literal = None,
                 transactionRevision: URIRef = None):
        super().__init__(queryEndpoint, updateEndpoint, dataEndpoint)
        self.effective_date = effectiveDate
        self.transaction_revision = transactionRevision

    def reset_store(self):
        SPARQLQuery = "DROP ALL"
        self.execute_update_query(SPARQLQuery)

    def add_modifications_to_store(self, modifications):
        deleteString = ""
        insertString = ""

        for modification in modifications:
            if modification.deletion:
                deleteString += modification.value.to_sparql()
            else:
                insertString += modification.value.to_sparql()

        SPARQLQuery = """DELETE DATA {{ {0} }};
        INSERT DATA {{ {1} }}""".format(deleteString, insertString)
        self.execute_update_query(SPARQLQuery)

    def execute_query(self, queryString, returnFormat):
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