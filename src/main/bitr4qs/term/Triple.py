from .TriplePattern import TriplePattern
from rdflib.term import URIRef, Literal
from typing import IO, Optional
from io import BytesIO


class Triple(TriplePattern):

    def __init__(self, value):
        s, p, o = value
        assert isinstance(s, URIRef), "Subject %s must be an rdflib term" % (s,)
        assert isinstance(p, URIRef), "Predicate %s must be an rdflib term" % (p,)
        assert isinstance(o, URIRef) or isinstance(o, Literal), "Object %s must be an rdflib term" % (o,)
        super().__init__(value)

    def n_quad(self):
        return self.serialize_to_nquads()

    def __hash__(self):
        return hash((self._subject, self._predicate, self._object))

    def serialize_to_nquads(self, encoding: Optional[str] = None, **args):
        stream = BytesIO()
        if encoding is None:
            stream.write(self._nq_row().encode("utf-8", "replace"))
            return stream.getvalue().decode("utf-8")
        else:
            stream.write(self._nq_row().encode(encoding, "replace"))
            return stream.getvalue().decode("utf-8")

    def _nq_row(self):
        if isinstance(self._object, Literal):
            return "%s %s %s .\n" % (
                self._subject.n3(),
                self._predicate.n3(),
                self._quoteLiteral(self._object)
            )
        else:
            return "%s %s %s .\n" % (
                self._subject.n3(),
                self._predicate.n3(),
                self._object.n3(),
            )

    def _quoteLiteral(self, l_):
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

    def _quote_encode(self,l_):
        return '"%s"' % l_.replace("\\", "\\\\").replace("\n", "\\n").replace(
            '"', '\\"'
        ).replace("\r", "\\r")
