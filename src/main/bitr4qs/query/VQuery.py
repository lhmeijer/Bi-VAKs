from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD


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
        self._tags = revisionStore.tags_in_revision_line(revisionA=self._headRevision.preceding_revision)
        # TODO check whether we obtain a list of tags

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

        for tag in self._tags:

            version.transaction_time = tag.transaction_revision
            version.valid_time = tag.valid_time

            version.retrieve_version(previousTransactionTime=previousTransactionTime,
                                     previousValidTime=previousValidTime,
                                     headRevision=self._headRevision.preceding_revision)
            response = version.query_version(self._query, self._returnFormat)

            result = {"tagName": {"type": "literal", "value": tag.tag_name.value},
                      "response": {"type": self._returnFormat, "value": response}}
            results["results"]["bindings"].append(result)

            previousTransactionTime = version.transaction_time
            previousValidTime = version.valid_time

        # Set the number of processed quads to construct all versions
        self._numberOfProcessedQuads = version.number_of_processed_quads()

        return results
