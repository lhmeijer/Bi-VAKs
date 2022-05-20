from .Query import Query
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.core.Version import Version
from rdflib.namespace import XSD
import datetime


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

        revisionID = self._request.values.get('revision', None) or None
        if revisionID is not None:
            try:
                revision = revisionStore.revision(revisionID=URIRef(revisionID), isValidRevision=False,
                                                  transactionRevisionA=self._headRevision.preceding_revision)
                self._transactionTime = revision.identifier
            except Exception as e:
                raise e

        tagName = self._request.values.get('tag', None) or None
        if tagName is not None:
            try:
                tag = revisionStore.tag_from_name(Literal(tagName))
                self._transactionTime = tag.transaction_revision
                self._validTime = tag.effective_date
            except Exception as e:
                raise e
            # TODO Tag does not exist

        # Obtain the valid time given by the user
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self._validTime = Literal(str(effectiveDate), datatype=XSD.dateTimeStamp)

        # Set the transaction time to the HEAD of the transaction revisions if no other transaction time is given.
        if self._transactionTime is None:
            self._transactionTime = self._headRevision.preceding_revision

        # Set the valid time to the current time if no other valid time is given.
        if self._validTime is None:
            time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02.00")
            self._validTime = Literal(str(time), datatype=XSD.dateTimeStamp)

        print("self._headRevision ", self._headRevision)
        print("self._transactionTime ", self._transactionTime)
        print("self._validTime ", self._validTime)

    def apply_query(self, revisionStore):
        # Initialise the state or version of the RDF dataset
        version = Version(validTime=self._validTime, transactionTime=self._transactionTime)
        # Construct the version or state of the RDF dataset
        version.retrieve_version(headRevision=self._headRevision.preceding_revision, revisionStore=revisionStore,
                                 quadPattern=self._quadPattern)
        # Apply the query to the version
        response = version.query_version(self._query, self._returnFormat)
        print("response ", response)
        # Reset the temporal quad store
        version.clear_version()
        return response
