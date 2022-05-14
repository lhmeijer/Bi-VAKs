from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD


class VQuery(Query):

    def __init__(self, request, base=None):
        super().__init__(request, base)

        self._tags = None
        self._validTime = None

    @property
    def valid_time(self) -> Literal:
        return self._validTime

    @valid_time.setter
    def valid_time(self, validTime: Literal):
        self._validTime = validTime

    @property
    def tags(self) -> Literal:
        return self._tags

    @tags.setter
    def tags(self, tags: Literal):
        self._tags = tags

    def evaluate_query(self, revisionStore):
        super().evaluate_query(revisionStore)

        # Obtain the branch based on the branch name
        branchName = self._request.values.get('branch', None) or None
        branchIdentifier = None
        if branchName is not None:
            branch = revisionStore.branch_from_name(Literal(branchName))
            # TODO Branch does not exist
            branchIdentifier = branch.identifier

        # Obtain the head of the transaction revisions
        headRevision, revisionNumber = revisionStore.head_revision(branchIdentifier)
        # TODO HEAD Revision does not exist.
        if headRevision is not None:
            headRevision = URIRef(headRevision)

        # Get all tags from the revision graph also specified from a branch (ordered on transaction time)
        self._tags = revisionStore.tags_in_revision_line(revisionA=headRevision)
        # TODO check whether we obtain a list of tags

        # Set a specific effective date for all tags
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self.valid_time = Literal(str(effectiveDate), datatype=XSD.dateTimeStamp)

    def apply_query(self, revisionStore):
        # effective date of query is leading compare to tag effective date
        responses = {}
        # Initialise the version
        version = Version(None, None)

        previousTransactionTime = None
        previousValidTime = None

        for tag in self._tags:

            version.transaction_time = tag.transaction_revision

            if self._validTime is None:
                version.valid_time = tag.valid_time
            else:
                version.valid_time = self._validTime

            version.retrieve_version(revisionStore=revisionStore, previousTransactionTime=previousTransactionTime,
                                     previousValidTime=previousValidTime, quadPattern=self._quadPattern)
            response = version.query_version(self._query, self._queryType, self._returnFormat)
            responses[tag.tag_name] = response

            previousTransactionTime = version.transaction_time
            previousValidTime = version.valid_time

        return responses
