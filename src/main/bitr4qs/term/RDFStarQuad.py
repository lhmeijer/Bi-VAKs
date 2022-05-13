from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Quad import Quad


class RDFStarQuad(object):

    def __init__(self, value):
        s, p, o = value
        assert isinstance(s, URIRef) or isinstance(s, Quad), "Subject %s must be an rdflib term" % (s,)
        assert isinstance(p, URIRef), "Predicate %s must be an rdflib term" % (p,)
        assert isinstance(o, URIRef) or isinstance(o, Literal) or isinstance(o, Quad), "Object %s must be an rdflib term" % (o,)

        self._subject = s
        self._predicate = p
        self._object = o

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        assert isinstance(subject, URIRef) or isinstance(subject, Quad)
        self._subject = subject

    def to_sparql(self):
        if isinstance(self._subject, Quad) and not isinstance(self._object, Quad):
            return "GRAPH {0} {{ {1} }}".format(
                self._subject.graph.n3(), ' '.join((self._subject.rdf_star(), self._predicate.n3(), self._object.n3())))
        elif isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            pass
        elif not isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            return "GRAPH {0} {{ {1} }}".format(
                self._object.graph.n3(), ' '.join((self._subject.n3(), self._predicate.n3(), self._object.rdf_star())))
        else:
            return ' '.join((self._subject.n3(), self._predicate.n3(), self._object.n3())) + ' .'

    def n_quad(self):
        if isinstance(self._subject, Quad) and not isinstance(self._object, Quad):
            return ' '.join((self._subject.rdf_star(), self._predicate.n3(), self._object.n3(),
                             self._subject.graph.n3())) + ' .\n'
        elif isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            pass
        elif not isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            return ' '.join((self._subject.n3(), self._predicate.n3(), self._object.rdf_star(),
                             self._object.graph.n3())) + ' .\n'
        else:
            return ' '.join((self._subject.n3(), self._predicate.n3(), self._object.n3())) + ' .\n'
