from rdflib.term import URIRef, Literal
from .Triple import Triple


class RDFStarTriple(object):

    def __init__(self, value):
        s, p, o = value
        assert isinstance(s, URIRef) or isinstance(s, Triple), "Subject %s must be an rdflib term" % (s,)
        assert isinstance(p, URIRef), "Predicate %s must be an rdflib term" % (p,)
        assert isinstance(o, URIRef) or isinstance(o, Literal) or isinstance(o, Triple), "Object %s must be an rdflib term" % (o,)

        self._subject = s
        self._predicate = p
        self._object = o

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        assert isinstance(subject, URIRef) or isinstance(subject, Triple)
        self._subject = subject

    def to_sparql(self):
        if isinstance(self._subject, Triple) and not isinstance(self._object, Triple):
            return ' '.join((self._subject.rdf_star(), self._predicate.n3(), self._object.n3())) + ' .'
        elif isinstance(self._subject, Triple) and isinstance(self._object, Triple):
            return ' '.join((self._subject.rdf_star(), self._predicate.n3(), self._object.rdf_star())) + ' .'
        elif not isinstance(self._subject, Triple) and isinstance(self._object, Triple):
            return ' '.join((self._subject.n3(), self._predicate.n3(), self._object.rdf_star())) + ' .'
        else:
            return ' '.join((self._subject.n3(), self._predicate.n3(), self._object.n3())) + ' .'

    def n_quad(self):
        return self.to_sparql() + '\n'
