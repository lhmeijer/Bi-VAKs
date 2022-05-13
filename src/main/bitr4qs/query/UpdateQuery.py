from .Query import Query
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue, plist
from rdflib.term import URIRef
from src.main.bitr4qs.tools.algebra import translate_update
from pyparsing import ParseException
from werkzeug.http import parse_options_header
from src.main.bitr4qs.exception import UnsupportedQuery, NonAbsoluteBaseError, SparqlProtocolError


class UpdateQuery(Query):

    def __init__(self, request, base=None):
        super().__init__(request, base)

    def translate_query(self):
        self._parse_query_request()

        if self._query is None:
            # changes only start, or end date -> self._translated_update = [] -> obtain the updates from previous revision
            pass
        else:
            try:
                self._parsedQuery = parseUpdate(self._query)
                self._configure_query_dataset()
                self._translatedQuery = translate_update(self._parsedQuery, base=self._base)
            except ParseException:
                print("ParseException")
                raise UnsupportedQuery()

        if self._base is not None and not self._is_absolute_uri(self._base):
            raise NonAbsoluteBaseError()

        if not self._is_valid_query_base(self._parsedQuery):
            raise NonAbsoluteBaseError()

    def _configure_query_dataset(self):
        if not isinstance(self._defaultGraph, list) or not isinstance(self._namedGraph, list):
            return

        if len(self._defaultGraph) == 0 and len(self._namedGraph) == 0:
            return

        if self._parsedQuery.request[0].withClause is not None:
            raise SparqlProtocolError

        if self._parsedQuery.request[0].using is not None:
            raise SparqlProtocolError

        self._parsedQuery.request[0]['using'] = plist()

        for uri in self._defaultGraph:
            self._parsedQuery.request[0]['using'].append(CompValue('UsingClause', default=URIRef(uri)))
        for uri in self._namedGraph:
            self._parsedQuery.request[0]['using'].append(CompValue('UsingClause', named=URIRef(uri)))

    def _parse_query_request(self):

        self._defaultGraph = []
        self._namedGraph = []

        if 'Content-Type' in self._request.headers:
            content_mimetype, options = parse_options_header(self._request.headers['Content-Type'])

            if content_mimetype == "application/x-www-form-urlencoded":
                self._defaultGraph = self._request.form.getlist('using-graph-uri')
                self._namedGraph = self._request.form.getlist('using-named-graph-uri')
                self._query = self._request.form.get('update', None)
            elif content_mimetype == "application/sparql-update":
                self._defaultGraph = self._request.args.getlist('using-graph-uri')
                self._namedGraph = self._request.args.getlist('using-named-graph-uri')
                self._query = self._request.data.decode("utf-8")