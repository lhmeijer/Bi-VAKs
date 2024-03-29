from rdflib.term import URIRef, Variable
from .TriplePattern import TriplePattern


class QuadPattern(TriplePattern):

    def __init__(self, value, graph):

        super().__init__(value)
        self.graph = graph

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, graph):
        assert isinstance(graph, URIRef) or isinstance(graph, Variable)
        self._graph = graph

    def quad(self):
        return self._subject, self._predicate, self._object, self._graph

    def variables(self):
        variables = super().variables()
        if isinstance(self._graph, Variable):
            variables.append((self._graph.n3(), 3))
        return variables

    def query_via_insert_update(self, construct=True, subjectName='?revision'):
        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {2} :inserts {1} }}\n{2} :inserts {1} .".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        elif isinstance(self._graph, Variable) and not construct:
            queryString = """{{ GRAPH {0} {{ {2} :inserts {1} }} }} UNION {{ {2} :inserts {1} }}""".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        else:
            queryString = "GRAPH {0} {{ {2} :inserts {1} }}".format(self._graph.n3(), self.rdf_star(), subjectName)
        return queryString

    def query_via_delete_update(self, construct=True, subjectName='?revision'):
        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {2} :deletes {1} }}\n{2} :deletes {1} .".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        elif isinstance(self._graph, Variable) and not construct:
            queryString = """{{ GRAPH {0} {{ {2} :deletes {1} }} }} UNION {{ {2} :deletes {1} }}""".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        else:
            queryString = "GRAPH {0} {{ {2} :deletes {1} }}".format(self._graph.n3(), self.rdf_star(), subjectName)
        return queryString

    def query_via_unknown_update(self, construct=True, subjectName='?revision'):

        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {1} ?p1 {2} }}\n?{1} ?p2 {2} .".format(self._graph.n3(), subjectName,
                                                                               self.rdf_star())
        elif isinstance(self._graph, Variable) and not construct:
            # queryString = """OPTIONAL {{ GRAPH {0} {{ {3} {1}1 {2} }} }}
            # OPTIONAL {{ ?update {1}2 {2} }}""".format(self._graph.n3(), predicate, self.rdf_star(), subjectName)
            queryString = """{{ GRAPH {0} {{ {1} ?p1 {2} }} }} UNION {{ {1} ?p2 {2} }}""".format(
                self._graph.n3(), subjectName, self.rdf_star())
        else:
            queryString = "GRAPH {0} {{ {1} ?p {2} }}".format(self._graph.n3(), subjectName, self.rdf_star())
        return queryString

    def sparql(self):
        return "GRAPH {0} {{ {1} }}".format(self._graph.n3(), ' '.join(self.represent_term(term) for term in self.triple()))

    def __eq__(self, other):
        equals = super().__eq__(other)
        if not equals:
            return False
        if self.represent_term(self._graph) != self.represent_term(other.graph):
            return False
        return True

    def __str__(self):
        return '({0})'.format(','.join(self.represent_term(term) for term in self.quad()))


