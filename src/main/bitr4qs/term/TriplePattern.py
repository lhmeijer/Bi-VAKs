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
        return "{0} :inserts {1} .".format(subjectName, self.rdf_star())

    def to_query_via_delete_update(self, construct=True, subjectName='?update'):
        return "{0} :deletes {1} .".format(subjectName, self.rdf_star())

    def to_query_via_unknown_update(self, construct=True, subjectName='?update'):
        return "{0} ?p {1} .".format(subjectName, self.rdf_star())

    def n3(self):
        return ' '.join(element.n3() for element in self.get_triple()) + ' .'

    def rdf_star(self):
        return "<< {0} >>".format(' '.join(element.n3() for element in self.get_triple()))

    def to_select_query(self):
        SPARQLQuery = """SELECT {0}
        WHERE {{ {1} }}""".format(' '.join(variable[0] for variable in self.get_variables()), self.to_sparql())
        return SPARQLQuery

    def matches(self, triple):

        if not isinstance(self._subject, Variable):
            if self._subject.n3() != triple.subject.n3():
                return False
        if not isinstance(self._predicate, Variable):
            if self._predicate.n3() != triple.predicate.n3():
                return False
        if not isinstance(self._object, Variable):
            if self._object.n3() != triple.object.n3():
                return False
        return True

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self._subject.n3() != other.subject.n3():
                return False
            if self._predicate.n3() != other.predicate.n3():
                return False
            if self._object.n3() != other.object.n3():
                return False
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return '({0})'.format(','.join((self._subject.n3(), self._predicate.n3(), self._object.n3())))
