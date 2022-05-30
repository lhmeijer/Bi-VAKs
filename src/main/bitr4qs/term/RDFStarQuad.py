from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Quad import Quad


class RDFStarQuad(object):

    def __init__(self, value):
        s, p, o = value

        self.subject = s
        self.predicate = p
        self.object = o

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        assert isinstance(subject, URIRef) or isinstance(subject, Quad)
        self._subject = subject

    @property
    def predicate(self):
        return self._predicate

    @predicate.setter
    def predicate(self, predicate):
        assert isinstance(predicate, URIRef)
        self._predicate = predicate

    @property
    def object(self):
        return self._object

    @object.setter
    def object(self, newObject):
        assert isinstance(newObject, URIRef) or isinstance(newObject, Quad)
        self._object = newObject

    def triple(self):
        return self._subject, self._predicate, self._object

    def quad(self):
        if isinstance(self._subject, Quad) and not isinstance(self._object, Quad):
            return self._subject, self._predicate, self._object, self._subject.graph
        if not isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            return self._subject, self._predicate, self._object, self._object.graph
        else:
            return self._subject, self._predicate, self._object

    def sparql(self):
        if isinstance(self._subject, Quad) and not isinstance(self._object, Quad):
            return "GRAPH {0} {{ {1} }}".format(
                self._subject.graph.n3(), ' '.join(self.represent_term(term) for term in self.triple()))
        elif isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            pass
        elif not isinstance(self._subject, Quad) and isinstance(self._object, Quad):
            return "GRAPH {0} {{ {1} }}".format(
                self._object.graph.n3(), ' '.join(self.represent_term(term) for term in self.triple()))
        else:
            return ' '.join(self.represent_term(term) for term in self.triple()) + ' .'

    def n_quad(self):
        return ' '.join(self.represent_term(term) for term in self.quad()) + ' .\n'

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.represent_term(self._subject) != self.represent_term(other.subject):
                return False
            if self.represent_term(self._predicate) != self.represent_term(other.predicate):
                return False
            if self.represent_term(self._object) != self.represent_term(other.object):
                return False
            if isinstance(self._subject, Quad) and not isinstance(self._object, Quad):
                if self.represent_term(self._subject.graph) != self.represent_term(other.subject.graph):
                    return False
            if not isinstance(self._subject, Quad) and isinstance(self._object, Quad):
                if self.represent_term(self._object.graph) != self.represent_term(other.object.graph):
                    return False
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return '({0})'.format(','.join(self.represent_term(term) for term in self.quad()))

    def represent_term(self, term):
        if isinstance(term, Literal):
            return self._quote_literal(term)
        elif isinstance(term, Quad):
            return term.rdf_star()
        else:
            return term.n3()

    def _quote_literal(self, l_):
        """
        a simpler version of term.Literal.n3()
        """

        encoded = self._quote_encode(l_)

        if l_.language:
            if l_.datatype:
                raise Exception("Literal has datatype AND language!")
            return "%s@%s" % (encoded, l_.language)
        elif l_.datatype:
            return "%s^^<%s>" % (encoded, l_.datatype)
        else:
            return "%s" % encoded

    @staticmethod
    def _quote_encode(l_):
        return '"%s"' % l_.replace("\\", "\\\\").replace("\n", "\\n").replace(
            '"', '\\"').replace("\r", "\\r")


