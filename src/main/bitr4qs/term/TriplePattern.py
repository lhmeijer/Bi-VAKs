from rdflib.term import URIRef, Literal, Variable


class TriplePattern(object):

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
        assert isinstance(subject, URIRef) or isinstance(subject, Variable), "Subject %s must be an rdflib term or variable" % (subject,)
        self._subject = subject

    @property
    def predicate(self):
        return self._predicate

    @predicate.setter
    def predicate(self, predicate):
        assert isinstance(predicate, URIRef) or isinstance(predicate, Variable), "Predicate %s must be an rdflib term or variable" % (predicate,)
        self._predicate = predicate

    @property
    def object(self):
        return self._object

    @object.setter
    def object(self, newObject):
        assert isinstance(newObject, URIRef) or isinstance(newObject, Literal) or isinstance(newObject, Variable), "Object %s must be an rdflib term, literal or a variable" % (newObject,)
        self._object = newObject

    def variables(self):
        variables = []
        if isinstance(self._subject, Variable):
            variables.append((self._subject.n3(), 0))
        if isinstance(self._predicate, Variable):
            variables.append((self._predicate.n3(), 1))
        if isinstance(self._object, Variable):
            variables.append((self._object.n3(), 2))
        return variables

    def triple(self):
        return self._subject, self._predicate, self._object

    def sparql(self):
        return ' '.join(self.represent_term(term) for term in self.triple()) + ' .'

    def query_via_insert_update(self, construct=True, subjectName='?update'):
        return "{0} :inserts {1} .".format(subjectName, self.rdf_star())

    def query_via_delete_update(self, construct=True, subjectName='?update'):
        return "{0} :deletes {1} .".format(subjectName, self.rdf_star())

    def query_via_unknown_update(self, construct=True, subjectName='?update'):
        return "{0} ?p {1} .".format(subjectName, self.rdf_star())

    def rdf_star(self):
        return "<< {0} >>".format(' '.join(self.represent_term(term) for term in self.triple()))

    def select_query(self):
        SPARQLQuery = """SELECT {0}
        WHERE {{ {1} }}""".format(' '.join(variable[0] for variable in self.variables()), self.sparql())
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
        if self.represent_term(self._subject) != self.represent_term(other.subject):
            return False
        if self.represent_term(self._predicate) != self.represent_term(other.predicate):
            return False
        if self.represent_term(self._object) != self.represent_term(other.object):
            return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return '({0})'.format(','.join(self.represent_term(term) for term in self.triple()))

    def represent_term(self, term):
        if isinstance(term, Literal):
            return self._quote_literal(term)
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

