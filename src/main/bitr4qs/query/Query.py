import os
from uritools import urisplit
from pyparsing import ParseException
from werkzeug.http import parse_options_header
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue, plist
from src.main.bitr4qs.tools.algebra import translate_query
from rdflib.term import URIRef
from src.main.bitr4qs.term.TriplePattern import TriplePattern
from src.main.bitr4qs.term.QuadPattern import QuadPattern
from src.main.bitr4qs.exception import UnsupportedQuery, NonAbsoluteBaseError


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


class Query(object):

    def __init__(self, request, base=None):
        self._request = request
        self._base = base

        self._query = None
        self._defaultGraph = None
        self._namedGraph = None
        self._parsedQuery = None
        self._translatedQuery = None
        self._quadPattern = None
        self._queryType = None
        self._returnFormat = None

    @property
    def request(self):
        return self._request

    @property
    def base(self):
        return self._base

    @property
    def return_format(self):
        return self._queryType

    @property
    def translated_query(self):
        return self._translatedQuery

    def _extract_quad_pattern(self, groupGraphPattern, graph=None):
        """

        :param groupGraphPattern:
        :param graph:
        :return:
        """
        for part in groupGraphPattern.part:
            if part.name == 'TriplesBlock':
                for triple in part.triples:
                    pattern = TriplePattern((triple[0], triple[1], triple[2])) if graph is None \
                        else QuadPattern((triple[0], triple[1], triple[2]), graph)
                    self._quadPattern = pattern
            elif part.name == 'GraphGraphPattern':
                self._extract_quad_pattern(part.graph, part.term)

    def evaluate_query(self, revisionStore):
        self._returnFormat = self._get_best_matching_mime_type()
        # Extract the quad pattern from SPARQL query
        self._extract_quad_pattern(self.translated_query.where)

    @staticmethod
    def _is_absolute_uri(uri):
        try:
            parsed = urisplit(uri)
        except ValueError:
            return False
        if parsed[0] and parsed[0] in ['http', 'https'] and parsed[1] and not parsed[4] and (
                parsed[2] == '/' or os.path.isabs(parsed[2])):
            return True
        else:
            return False

    def _is_valid_query_base(self, parsedQuery):
        """Check if a query contains an absolute base if base is given.
        Args: parsed_query: the parsed query
        Returns: True - if Base URI is given and absolute or if no Base is given
                 False - if Base URI is given an not absolute
        """
        for value in parsedQuery[0]:
            if value.name == 'Base' and not self._is_absolute_uri(value.iri):
                return False

        return True

    def translate_query(self):

        self._parse_query_request()
        # Check whether a SPARQL query is defined
        if self._query is None:
            pass
        else:
            try:
                self._parsedQuery = parseQuery(self._query)
                print("self._parsedQuery ", self._parsedQuery)
                self._configure_query_dataset()
                self._translatedQuery = translate_query(self._parsedQuery, base=self._base)
                self._queryType = self._translatedQuery.name
                print("self._translatedQuery ", self._translatedQuery.name)
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

        # clean existing named (FROM NAMED) and default (FROM) DatasetClauses
        self._parsedQuery[1]['datasetClause'] = plist()

        # add new named (default-graph-uri) and default (named-graph-uri)
        # DatasetClauses from Protocol
        for uri in self._defaultGraph:
            self._parsedQuery[1]['datasetClause'].append(CompValue('DatasetClause', default=URIRef(uri)))
        for uri in self._namedGraph:
            self._parsedQuery[1]['datasetClause'].append(CompValue('DatasetClause', named=URIRef(uri)))

    def _parse_query_request(self):

        self._defaultGraph = []
        self._namedGraph = []

        if self._request.method == 'GET':
            self._defaultGraph = self._request.args.getlist('default-graph-uri')
            self._namedGraph = self._request.args.getlist('named-graph-uri')
            self._query = self._request.args.get('query', None)
        elif self._request.method == 'POST':
            if 'Content-Type' in self._request.headers:
                content_mimetype, options = parse_options_header(self._request.headers['Content-Type'])
                if content_mimetype == "application/x-www-form-urlencoded":
                    self._defaultGraph = self._request.form.getlist('default-graph-uri')
                    self._namedGraph = self._request.form.getlist('named-graph-uri')
                    self._query = self._request.form.get('query', None)
                elif content_mimetype == "application/sparql-query":
                    self._defaultGraph = self._request.args.getlist('default-graph-uri')
                    self._namedGraph = self._request.args.getlist('named-graph-uri')
                    self._query = self._request.data.decode("utf-8")

    def _get_best_matching_mime_type(self):
        if self._queryType == 'SelectQuery':
            mimetype_default = resultSetMimetypesDefault
            mimetype_list = resultSetMimetypes
        elif self._queryType == 'AskQuery':
            mimetype_default = askMimetypesDefault
            mimetype_list = askMimetypes
        elif self._queryType in ['ConstructQuery', 'DescribeQuery']:
            mimetype_default = rdfMimetypesDefault
            mimetype_list = rdfMimetypes
        else:
            mimetype_default = ''
            mimetype_list = []

        match_list = [mimetype_default] + mimetype_list
        if 'Accept' in self._request.headers:
            mimetype = self._request.accept_mimetypes.best_match(match_list, None)
        else:
            mimetype = mimetype_default

        return mimetype