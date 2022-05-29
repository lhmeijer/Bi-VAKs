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

    def to_query_via_insert_update(self, construct=True, subjectName='?update'):
        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {2} :inserts {1} }}\n?update :inserts {1} .".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        elif isinstance(self._graph, Variable) and not construct:
            queryString = """{{ GRAPH {0} {{ {2} :inserts {1} }} }} UNION {{ ?update :inserts {1} }}""".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        else:
            queryString = "GRAPH {0} {{ {2} :inserts {1} }}".format(self._graph.n3(), self.rdf_star(), subjectName)
        return queryString

    def to_query_via_delete_update(self, construct=True, subjectName='?update'):
        if isinstance(self._graph, Variable) and construct:
            queryString = "GRAPH {0} {{ {2} :deletes {1} }}\n?update :deletes {1} .".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        elif isinstance(self._graph, Variable) and not construct:
            queryString = """{{ GRAPH {0} {{ {2} :deletes {1} }} }} UNION {{ ?update :deletes {1} }}""".format(
                self._graph.n3(), self.rdf_star(), subjectName)
        else:
            queryString = "GRAPH {0} {{ {2} :deletes {1} }}".format(self._graph.n3(), self.rdf_star(), subjectName)
        return queryString

    def to_query_via_unknown_update(self, construct=True, subjectName='?update'):

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

    def to_sparql(self):
        return "GRAPH {0} {{ {1} }}".format(self._graph.n3(), ' '.join(element.n3() for element in self.get_triple()))

    def __eq__(self, other):
        equals = super().__eq__(other)
        if not equals:
            return False
        if self._graph.n3() != other._graph.n3():
            return False
        return True

    def __str__(self):
        return '({0})'.format(','.join((self._subject.n3(), self._predicate.n3(), self._object.n3(), self._graph.n3())))
