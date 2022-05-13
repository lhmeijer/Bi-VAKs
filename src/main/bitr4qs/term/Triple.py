from .TriplePattern import TriplePattern
from rdflib.term import URIRef, Literal


class Triple(TriplePattern):

    def __init__(self, value):
        s, p, o = value
        assert isinstance(s, URIRef), "Subject %s must be an rdflib term" % (s,)
        assert isinstance(p, URIRef), "Predicate %s must be an rdflib term" % (p,)
        assert isinstance(o, URIRef) or isinstance(o, Literal), "Object %s must be an rdflib term" % (o,)
        super().__init__(value)

    def n_quad(self):
        return ' '.join(element.n3() for element in self.get_triple()) + ' .\n'

    def __hash__(self):
        return hash((self._subject, self._predicate, self._object))

