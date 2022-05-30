from .Triple import Triple
from rdflib.term import URIRef, Literal


class Quad(Triple):

    def __init__(self, value, graph):
        super().__init__(value)
        self._graph = graph

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, graph):
        assert isinstance(graph, URIRef), "Context information %s must be an rdflib term" % (s,)
        self._graph = graph

    def quad(self):
        return self._subject, self._predicate, self._object, self._graph

    def n_quad(self):
        return ' '.join(self.represent_term(term) for term in self.quad()) + ' .\n'

    def __hash__(self):
        return hash((self.represent_term(self._subject), self.represent_term(self._predicate),
                     self.represent_term(self._object), self.represent_term(self._graph)))

    def query_via_unknown_update(self, construct=True, subjectName='?update'):
        queryString = "GRAPH {0} {{ {1} ?p {2} }}".format(self._graph.n3(), subjectName, self.rdf_star())
        return queryString

    def sparql(self):
        return "GRAPH {0} {{ {1} }}".format(self._graph.n3(), ' '.join(element.n3() for element in self.triple()))

    def __eq__(self, other):
        equals = super().__eq__(other)
        if not equals:
            return False
        if self._graph.n3() != other.graph.n3():
            return False
        return True

    def __str__(self):
        return '({0})'.format(','.join(self.represent_term(term) for term in self.quad()))

