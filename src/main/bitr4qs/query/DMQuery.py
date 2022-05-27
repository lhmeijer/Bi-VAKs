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
    def return_format(self):
        return 'application/json'

    def evaluate_query(self, revisionStore):
        super().evaluate_query(revisionStore)

        tagNameA = self._request.values.get('tagA', None) or None
        if tagNameA:
            try:
                tagA = revisionStore.tag_from_name(Literal(tagNameA))
                self._transactionTimeA = tagA.transaction_revision
                self._validTimeA = tagA.effective_date
            except Exception as e:
                raise e
            # TODO Tag does not exist

        tagNameB = self._request.values.get('tagB', None) or None
        if tagNameB:
            try:
                tagB = revisionStore.tag_from_name(Literal(tagNameA))
                self._transactionTimeB = tagB.transaction_revision
                self._validTimeB = tagB.effective_date
            except Exception as e:
                raise e
            # TODO Tag does not exist

        revisionIDB = self._request.values.get('revisionB', None) or None
        # TODO RevisionB does not exist or is not given.
        if revisionIDB:
            self._transactionTimeB = URIRef(revisionIDB)

        revisionIDA = self._request.values.get('revisionA', None) or None
        # TODO RevisionA does not exist or is not given.
        if revisionIDA:
            try:
                revisionA = revisionStore.revision(revisionID=URIRef(revisionIDA), isValidRevision=False,
                                                   transactionRevisionA=self._transactionTimeB)
                self._transactionTimeA = revisionA.identifier
            except Exception as e:
                raise e

        validDateA = self._request.values.get('dateA', None) or None
        # TODO no valid date A is given.
        if validDateA:
            self._validTimeA = Literal(validDateA, datatype=XSD.dateTimeStamp)

        validDateB = self._request.values.get('dateB', None) or None
        # TODO no valid date B is given.
        if validDateB:
            self._validTimeB = Literal(validDateB, datatype=XSD.dateTimeStamp)

    def apply_query(self, revisionStore):
        """

        :param revisionStore:
        :return:
        """
        version = Version(validTime=None, transactionTime=None, revisionStore=revisionStore,
                          quadPattern=self._quadPattern)
        version.modifications_between_two_states(transactionA=self._transactionTimeA, validA=self._validTimeA,
                                                 transactionB=self._transactionTimeB, validB=self._validTimeB)

        # Set the number of processed quads to construct the version
        self._numberOfProcessedQuads = version.number_of_processed_quads()

        modifications = version.update_parser.get_list_of_modifications()
        print("modifications ", modifications)

        # TODO check which queryType -> return a result for each queryType
        if self._queryType == 'SelectQuery':
            return self._apply_select_query(modifications)
        elif self._queryType == 'ConstructQuery':
            pass
        elif self._queryType == 'AskQuery':
            pass
        elif self._queryType == 'DescribeQuery':
            pass
        else:
            pass

    def _apply_select_query(self, modifications):
        """

        :param modifications:
        :return:
        """
        # Check the variables in the SPARQL query, and returns these and separate them based on insertions and deletions
        variables = self._quadPattern.get_variables()
        print("variables ", variables)
        results = {'head': {'vars': [var for var, _ in variables]}, 'results': {'insertions': [], 'deletions': []}}
        print("results ", results)
        for modification in modifications:
            print("modification ", modification)
            result = {}
            for variable, index in variables:
                print("variable ", variable)
                print('index ', index)
                if index == 0:
                    value = modification.value.subject
                    print('modification.value.subject ', modification.value.subject)
                elif index == 1:
                    value = modification.value.predicate
                elif index == 2:
                    value = modification.value.object
                else:
                    value = modification.value.graph

                if isinstance(value, URIRef):
                    result[variable] = {'type': 'uri', 'value': str(value)}
                    print("result ", result)
                elif isinstance(value, Literal):
                    print(value.datatype)
                    print(value.language)
                    result[variable] = {'type': 'literal', 'value': str(value)}

            if modification.deletion:
                results['results']['deletions'].append(result)
            else:
                results['results']['insertions'].append(result)

        return results
