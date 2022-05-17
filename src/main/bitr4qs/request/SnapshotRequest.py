from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD


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

        super().evaluate_request(revisionStore)

        # Obtain the preceding Snapshot
        precedingSnapshotID = self._request.view_args.get('snapshotID', None) or None
        precedingSnapshot = None
        if precedingSnapshotID is not None:
            precedingSnapshots = revisionStore.revision(URIRef(precedingSnapshotID), 'snapshot', validRevision=True)
            precedingSnapshot = precedingSnapshots[precedingSnapshotID]
            self.preceding_valid_revision = precedingSnapshot.identifier
            self.branch_index = precedingSnapshot.branch_index

        # Obtain effective date of the Snapshot
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate, datatype=XSD.dateTimeStamp)
        elif precedingSnapshot is not None:
            self.effective_date = precedingSnapshot.effective_date
        else:
            self.effective_date = self.creation_date

        # Obtain the transaction time based on a given transaction revision
        revisionID = self._request.values.get('revision', None) or None
        transactionRevision = None
        if revisionID is not None:
            transactionRevision = revisionStore.revision(revisionID=URIRef(revisionID), transactionRevision=True)

        if transactionRevision is not None:
            self.transaction_revision = transactionRevision.identifier
        elif precedingSnapshot is not None:
            self.transaction_revision = precedingSnapshot.transaction_revision

        # Obtain the name of the dataset
        nameDataset = self._request.values.get('nameDataset', None) or None
        if nameDataset is not None:
            self.name_dataset = Literal(nameDataset)
        elif precedingSnapshot is not None:
            self.name_dataset = precedingSnapshot.name_dataset
        else:
            # TODO no name of the dataset is known, return an error
            pass

        # Obtain the url of the dataset
        urlDataset = self._request.values.get('urlDataset', None) or None
        if urlDataset is not None:
            self.url_dataset = Literal(urlDataset)
        elif precedingSnapshot is not None:
            self.url_dataset = precedingSnapshot.url_dataset
        else:
            # TODO no URL of the dataset is known, return an error
            pass