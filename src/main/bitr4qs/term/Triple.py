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
        return ' '.join(self.represent_term(term) for term in self.triple()) + ' .\n'

    def __hash__(self):
        return hash((self.represent_term(self._subject), self.represent_term(self._predicate),
                     self.represent_term(self._object)))

    @classmethod
    def triple_from_json(cls, triple):
        nameTerms = ['subject', 'predicate', 'object']
        terms = []
        for term in nameTerms:
            if triple[term]['type'] == 'uri':
                terms.append(URIRef(triple[term]['value']))
            elif triple[term]['type'] == 'literal':
                lang = None
                datatype = 'http://www.w3.org/2001/XMLSchema#string'
                if 'xml:lang' in triple[term]:
                    lang = triple[term]['xml:lang']
                    datatype = None
                elif 'datatype' in triple[term]:
                    datatype = triple[term]['datatype']
                terms.append(Literal(triple[term]['value'], lang=lang, datatype=datatype))

        return cls(tuple(terms))
