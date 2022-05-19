from .Triple import Triple
from rdflib import URIRef


class Quad(Triple):

    def __init__(self, value, graph):
        super().__init__(value)
        s, p, o = value

        assert isinstance(graph, URIRef), "Context information %s must be an rdflib term" % (s,)

        self._graph = graph

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, graph):
        self._graph = graph

    def n_quad(self):
        NQuad = ' '.join((self._subject.n3(), self._predicate.n3(), self._object.n3(), self._graph.n3())) + ' .\n'
        return NQuad

    def __hash__(self):
        return hash((self._subject, self._predicate, self._object, self._graph))

    def to_query_via_update(self, predicate, construct=True, subjectName='?update'):
        queryString = "GRAPH {0} {{ {3} {1} {2} }}".format(self._graph.n3(), predicate, self.rdf_star(), subjectName)
        return queryString

    def to_sparql(self):
        return "GRAPH {0} {{ {1} }}".format(self._graph.n3(), ' '.join(element.n3() for element in self.get_triple()))

