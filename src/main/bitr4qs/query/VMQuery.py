from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD


class VMQuery(Query):

    def __init__(self, request, base=None):
        super().__init__(request, base)

        self._transactionTime = None
        self._validTime = None
        self._headRevision = None

    @property
    def head_revision(self) -> URIRef:
        return self._headRevision

    @head_revision.setter
    def head_revision(self, headRevision: URIRef):
        self._headRevision = headRevision

    @property
    def transaction_time(self) -> URIRef:
        return self._transactionTime

    @transaction_time.setter
    def transaction_time(self, transactionTime: URIRef):
        self._transactionTime = transactionTime

    @property
    def valid_time(self) -> Literal:
        return self._validTime

    @valid_time.setter
    def valid_time(self, validTime: Literal):
        self._validTime = validTime

    def evaluate_query(self, revisionStore):
        super().evaluate_query(revisionStore)

        revision = self._request.view_args.get('revision', None) or None
        if revision is not None:
            # TODO check existence of revision
            self.transaction_time = URIRef(revision)

        tagName = self._request.view_args.get('tag', None) or None
        if tagName is not None:
            tag = revisionStore.tag_from_name(Literal(tagName))
            self.transaction_time = tag.transaction_revision
            self.valid_time = tag.effective_date

        # Obtain the branch based on the branch name
        branchName = self._request.values.get('branch', None) or None
        branchIdentifier = None
        if branchName is not None:
            branch = revisionStore.branch_from_name(Literal(branchName))
            branchIdentifier = branch.identifier

        # Obtain the valid time given by the user
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self.valid_time = Literal(str(effectiveDate), datatype=XSD.dateTimeStamp)

        # Obtain the head of the transaction revisions
        latestTransactionRevision, revisionNumber = revisionStore.head_revision(branchIdentifier)
        if latestTransactionRevision is not None:
            self.head_revision = URIRef(latestTransactionRevision)

        if self._transactionTime is None:
            self.transaction_time = self._headRevision

        print("self._headRevision ", self._headRevision)
        print("self._transactionTime ", self._transactionTime)
        print("self._validTime ", self._validTime)

    def apply_query(self, revisionStore):
        # Initialise the state or version of the RDF dataset
        version = Version(validTime=self._validTime, transactionTime=self._transactionTime)
        # Construct the version or state of the RDF dataset
        version.retrieve_version(headRevision=self._headRevision, revisionStore=revisionStore,
                                 quadPattern=self._quadPattern)
        # Apply the query to the version
        response = version.query_version(self._query, self._queryType, self._returnFormat)
        print("response ", response)
        # Reset the temporal quad store
        version.clear_version()
        return response
