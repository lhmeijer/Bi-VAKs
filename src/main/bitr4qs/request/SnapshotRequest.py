from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.Snapshot import Snapshot, SnapshotRevision


class SnapshotRequest(Request):

    type = 'snapshot'

    def __init__(self, request):
        super().__init__(request)

        self._effectiveDate = None
        self._transactionRevision = None
        self._nameDataset = None
        self._urlDataset = None

    def evaluate_request(self, revisionStore):

        self.evaluate_request_to_modify(revisionStore)

        if self._effectiveDate is None:
            self._effectiveDate = self._creationDate

        # TODO throw error if not all variables have a value

    def evaluate_request_to_modify(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain effective date of the Snapshot
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self._effectiveDate = Literal(effectiveDate, datatype=XSD.dateTimeStamp)

        # Obtain the transaction time based on a given transaction revision
        revisionID = self._request.values.get('revision', None) or None
        if revisionID is not None:
            if revisionID == 'HEAD':
                self._transactionRevision = 'HEAD'
            else:
                revision = revisionStore.revision(revisionID=URIRef(revisionID), isValidRevision=False,
                                                  transactionRevisionA=self._precedingTransactionRevision)
                self._transactionRevision = revision.identifier

        # Obtain the name of the dataset
        nameDataset = self._request.values.get('nameDataset', None) or None
        if nameDataset is not None:
            self._nameDataset = Literal(nameDataset)

        # Obtain the url of the dataset
        urlDataset = self._request.values.get('urlDataset', None) or None
        if urlDataset is not None:
            self._urlDataset = Literal(urlDataset)

    def transaction_revision_from_request(self):
        revision = SnapshotRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branchIndex=self._branchIndex, revisionNumber=self._revisionNumber)

        self._currentTransactionRevision = revision.identifier

        return revision

    def valid_revisions_from_request(self):

        if self._transactionRevision == 'HEAD':
            self._transactionRevision = self._currentTransactionRevision

        revision = Snapshot.revision_from_data(
            nameDataset=self._nameDataset, revisionNumber=self._revisionNumberValidRevision,
            effectiveDate=self._effectiveDate, transactionRevision=self._transactionRevision,
            branchIndex=self._branchIndexValidRevision, urlDataset=self._urlDataset)
        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Snapshot), "Valid Revision should be a Snapshot"
        # AssertionError

        modifiedRevision = revision.modify(
            otherNameDataset=self._nameDataset, otherUrlDataset=self._urlDataset, revisionStore=revisionStore,
            branchIndex=self._branchIndexValidRevision, otherEffectiveDate=self._effectiveDate,
            otherTransactionRevision=self._transactionRevision, revisionNumber=self._revisionNumberValidRevision)
        return [modifiedRevision]
