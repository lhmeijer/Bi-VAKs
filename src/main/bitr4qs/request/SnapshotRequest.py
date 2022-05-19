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

    @property
    def effective_date(self) -> Literal:
        return self._effectiveDate

    @effective_date.setter
    def effective_date(self, effectiveDate: Literal):
        self._effectiveDate = effectiveDate

    @property
    def transaction_revision(self) -> Literal:
        return self._transactionRevision

    @transaction_revision.setter
    def transaction_revision(self, transactionRevision: URIRef):
        self._transactionRevision = transactionRevision

    @property
    def preceding_snapshot_identifier(self) -> URIRef:
        return self._precedingSnapshotIdentifier

    @preceding_snapshot_identifier.setter
    def preceding_snapshot_identifier(self, precedingSnapshotIdentifier: URIRef):
        self._precedingSnapshotIdentifier = precedingSnapshotIdentifier

    @property
    def name_dataset(self) -> Literal:
        return self._nameDataset

    @name_dataset.setter
    def name_dataset(self, nameDataset: Literal):
        self._nameDataset = nameDataset

    @property
    def url_dataset(self) -> Literal:
        return self._urlDataset

    @url_dataset.setter
    def url_dataset(self, urlDataset: Literal):
        self._urlDataset = urlDataset

    def evaluate_request(self, revisionStore):

        self.evaluate_request_to_modify(revisionStore)

        if self._effectiveDate is None:
            self._effectiveDate = self.creation_date

        # TODO throw error if not all variables have a value

    def evaluate_request_to_modify(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain effective date of the Snapshot
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate, datatype=XSD.dateTimeStamp)

        # Obtain the transaction time based on a given transaction revision
        revisionID = self._request.values.get('revision', None) or None
        print("revisionID ", revisionID)
        if revisionID is not None:
            if revisionID == 'HEAD':
                self.transaction_revision = 'HEAD'
            else:
                # TODO Check existence
                self.transaction_revision = URIRef(revisionID)

        # Obtain the name of the dataset
        nameDataset = self._request.values.get('nameDataset', None) or None
        if nameDataset is not None:
            self.name_dataset = Literal(nameDataset)

        # Obtain the url of the dataset
        urlDataset = self._request.values.get('urlDataset', None) or None
        if urlDataset is not None:
            self.url_dataset = Literal(urlDataset)

    def transaction_revision_from_request(self):
        revision = SnapshotRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)
        print("self._transactionRevision ", self._transactionRevision)
        if self._transactionRevision == 'HEAD':
            self.transaction_revision = revision.identifier

        return revision

    def valid_revisions_from_request(self):
        print("self._transactionRevision , ", self._transactionRevision)
        revision = Snapshot.revision_from_data(
            nameDataset=self._nameDataset, revisionNumber=self._revisionNumber, effectiveDate=self._effectiveDate,
            transactionRevision=self._transactionRevision, branchIndex=self._branchIndex, urlDataset=self._urlDataset)
        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Snapshot), "Valid Revision should be a Snapshot"
        # AssertionError

        modifiedRevision = revision.modify(
            otherNameDataset=self._nameDataset, otherUrlDataset=self._urlDataset, branchIndex=self._branchIndex,
            otherEffectiveDate=self._effectiveDate, otherTransactionRevision=self._transactionRevision,
            revisionNumber=self._revisionNumber, revisionStore=revisionStore)
        return [modifiedRevision]
