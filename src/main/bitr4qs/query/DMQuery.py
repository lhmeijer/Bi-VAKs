from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD
from src.main.bitr4qs.tools.parser.UpdateParser import UpdateParser


class DMQuery(Query):

    def __init__(self, request, base=None):
        super().__init__(request, base)

        self._transactionTimeA = None
        self._validTimeA = None

        self._transactionTimeB = None
        self._validTimeB = None

    @property
    def transaction_time_a(self) -> URIRef:
        return self._transactionTimeA

    @transaction_time_a.setter
    def transaction_time_a(self, transactionTimeA: URIRef):
        self._transactionTimeA = transactionTimeA

    @property
    def transaction_time_b(self) -> URIRef:
        return self._transactionTimeB

    @transaction_time_b.setter
    def transaction_time_b(self, transactionTimeB: URIRef):
        self._transactionTimeB = transactionTimeB

    @property
    def valid_time_a(self) -> Literal:
        return self._validTimeA

    @valid_time_a.setter
    def valid_time_a(self, validTimeA: Literal):
        self._validTimeA = validTimeA

    @property
    def valid_time_b(self) -> Literal:
        return self._validTimeB

    @valid_time_b.setter
    def valid_time_b(self, validTimeB: Literal):
        self._validTimeB = validTimeB

    def evaluate_query(self, revisionStore):
        super().evaluate_query(revisionStore)

        revisionA = self._request.view_args.get('revisionA', None) or None
        if revisionA is not None:
            self.transaction_time_a = URIRef(revisionA)

        revisionB = self._request.view_args.get('revisionB', None) or None
        if revisionB is not None:
            self.transaction_time_b = URIRef(revisionB)

        validDateA = self._request.view_args.get('dateA', None) or None
        if validDateA is not None:
            self.valid_time_a = Literal(validDateA, datatype=XSD.dateTimeStamp)

        validDateB = self._request.view_args.get('dateB', None) or None
        if validDateB is not None:
            self.valid_time_b = Literal(validDateB, datatype=XSD.dateTimeStamp)

    def apply_query(self, revisionStore):
        updateParser = UpdateParser()
        Version.modifications_between_two_states(transactionA=self._transactionTimeA, validA=self._validTimeA,
                                                 transactionB=self._transactionTimeB, validB=self._validTimeB,
                                                 revisionStore=revisionStore, updateParser=updateParser,
                                                 quadPattern=self._quadPattern)
        modifications = updateParser.get_list_of_modifications()
        # What do I return for this update
        # Check the variables in the SPARQL query, and returns these and separate them based on insertions and deletions
        variables = self._quadPattern.get_variables()
        results = {'head': {'vars': [var for var, _ in variables]}, 'result': {'insertions': [], 'deletions': []}}
        for modification in modifications:
            result = {}
            for variable, index in variables:
                if index == 0:
                    value = modification.value.subject
                elif index == 1:
                    value = modification.value.predicate
                elif index == 2:
                    value = modification.value.object
                else:
                    value = modification.value.graph

                if isinstance(value, URIRef):
                    result[variable] = {'type': 'uri', 'value': str(value)}
                elif isinstance(value, Literal):
                    result[variable] = {'type': 'literal', 'value': str(value)}

            if modification.deletion:
                results['result']['deletions'].append(result)
            else:
                results['result']['insertions'].append(result)

        return results