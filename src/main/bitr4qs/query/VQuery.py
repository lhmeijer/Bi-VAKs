from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD
import json


class VQuery(Query):

    def __init__(self, request, base=None):
        super().__init__(request, base)

        self._tags = None
        self._headRevision = None

    @property
    def return_format(self):
        return 'application/json'

    def evaluate_query(self, revisionStore):
        super().evaluate_query(revisionStore)

        # Obtain the branch based on the branch name
        branchName = self._request.values.get('branch', None) or None
        if branchName is not None:
            try:
                branch = revisionStore.branch_from_name(Literal(branchName)).identifier
            except Exception as e:
                raise e
        else:
            branch = None

        # Obtain the head of the transaction revisions
        try:
            self._headRevision = revisionStore.head_revision(branch)
        except Exception as e:
            raise e

        # Get all tags from the revision graph also specified from a branch (ordered on transaction time)
        self._tags = revisionStore.tags_in_revision_graph(revisionA=self._headRevision.preceding_revision)
        print("self._tags ", [tag.__dict__() for i, tag in self._tags.items()])

    def apply_query(self, revisionStore):
        """

        :param revisionStore:
        :return:
        """
        results = {"head": {"vars": ["tagName", "response"]}, "results": {"bindings": []}}
        # Initialise the version
        version = Version(validTime=None, transactionTime=None, revisionStore=revisionStore,
                          quadPattern=self._quadPattern)

        previousTransactionTime = None
        previousValidTime = None
        for i in range(len(self._tags)):

            print(self._tags[i])
            version.transaction_time = self._tags[i].transaction_revision
            version.valid_time = self._tags[i].effective_date

            version.retrieve_version(previousTransactionTime=previousTransactionTime,
                                     previousValidTime=previousValidTime,
                                     headRevision=self._headRevision.preceding_revision)
            response = version.query_version(self._query, self._returnFormat)
            print("response ", response)
            if self._has_version_response(response):
                result = {"tagName": {"type": "literal", "value": self._tags[i].tag_name.value},
                          "response": {"type": self._returnFormat, "value": response}}
                results["results"]["bindings"].append(result)

            previousTransactionTime = version.transaction_time
            previousValidTime = version.valid_time

        # Set the number of processed quads to construct all versions
        self._numberOfProcessedQuads = version.number_of_processed_quads()
        print("results ", results)
        return results

    def _has_version_response(self, response):
        if self._queryType == 'SelectQuery':
            if self._returnFormat == 'application/sparql-results+json':
                jsonResponse = json.loads(response)
                if len(jsonResponse['results']['bindings']) == 0:
                    return False
                else:
                    return True

        return True
