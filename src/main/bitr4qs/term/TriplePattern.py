from rdflib.term import URIRef, Literal, Variable


class TriplePattern(object):

    def __init__(self, value):

        s, p, o = value

        assert isinstance(s, URIRef) or isinstance(s, Variable), "Subject %s must be an rdflib term or variable" % (s,)
        assert isinstance(p, URIRef) or isinstance(p, Variable), "Predicate %s must be an rdflib term or variable" % (p,)
        assert isinstance(o, URIRef) or isinstance(o, Literal) or isinstance(o, Variable), \
            "Object %s must be an rdflib term, literal or a variable" % (o,)

        self._subject = s
        self._predicate = p
        self._object = o

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        assert isinstance(subject, URIRef) or isinstance(subject, Variable)
        self._subject = subject

    @property
    def predicate(self):
        return self._predicate

    @predicate.setter
    def predicate(self, predicate):
        assert isinstance(predicate, URIRef) or isinstance(predicate, Variable)
        self._predicate = predicate

    @property
    def object(self):
        return self._object

    @object.setter
    def object(self, object):
        assert isinstance(object, URIRef) or isinstance(object, Literal) or isinstance(object, Variable)
        self._object = object

    def get_variables(self):
        variables = []
        if isinstance(self._subject, Variable):
            variables.append((self._subject.n3(), 0))
        if isinstance(self._predicate, Variable):
            variables.append((self._predicate.n3(), 1))
        if isinstance(self._object, Variable):
            variables.append((self._object.n3(), 2))
        return variables

    def get_triple(self):
        return self._subject, self._predicate, self._object

    def to_sparql(self):
        return ' '.join(element.n3() for element in self.get_triple()) + ' .'

    def to_query_via_insert_update(self, construct=True, subjectName='?update'):
        return self.to_query_via_update(':inserts', construct, subjectName)

    def to_query_via_delete_update(self, construct=True, subjectName='?update'):
        return self.to_query_via_update(':deletes', construct, subjectName)

    def to_query_via_update(self, predicate, construct=True, subjectName='?update'):
        queryString = "{2} {0} {1} .".format(predicate, self.rdf_star(), subjectName)
        return queryString

    def n3(self):
        pass

    def rdf_star(self):
        return "<< {0} >>".format(' '.join(element.n3() for element in self.get_triple()))


