from rdflib.term import URIRef, Literal, Variable
from .TriplePattern import TriplePattern


class QuadPattern(TriplePattern):

    def __init__(self, value, graph):

        super().__init__(value)
        s, p, o = value

        assert isinstance(graph, URIRef) or isinstance(graph, Variable), "Context information %s must be an " \
                                                                         "rdflib term or variable" % (s,)

        self._graph = graph

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, graph):
        self._graph = graph

    def get_variables(self):
        variables = super().get_variables()
        if isinstance(self._graph, Variable):
            variables.append((self._graph.n3(), 3))
        return variables

    def to_query_via_update(self, predicate, construct=True, subjectName='?update'):

        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {3} {1}1 {2} }}\n?update {1}2 {2} .".format(self._graph.n3(), predicate,
                                                                                      self.rdf_star(), subjectName)
        elif isinstance(self._graph, Variable) and not construct:
            # queryString = """OPTIONAL {{ GRAPH {0} {{ {3} {1}1 {2} }} }}
            # OPTIONAL {{ ?update {1}2 {2} }}""".format(self._graph.n3(), predicate, self.rdf_star(), subjectName)
            queryString = """{{ GRAPH {0} {{ {3} {1}1 {2} }} }} UNION {{ ?update {1}2 {2} }}""".format(
                self._graph.n3(), predicate, self.rdf_star(), subjectName)
        else:
            queryString = "GRAPH {0} {{ {3} {1} {2} }}".format(self._graph.n3(), predicate, self.rdf_star(), subjectName)
        return queryString

    def to_sparql(self):
        return "GRAPH {0} {{ {1} }}".format(self._graph.n3(), ' '.join(element.n3() for element in self.get_triple()))
