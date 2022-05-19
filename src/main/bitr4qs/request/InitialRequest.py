from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.InitialRevision import InitialRevision
from src.main.bitr4qs.revision.Snapshot import Snapshot


class InitialRequest(Request):

    type = 'initial'

    def __init__(self, request):
        super().__init__(request)

        self._nameDataset = None
        self._urlDataset = None
        self._effectiveDate = None
        self._transactionRevision = None

    @property
    def effective_date(self) -> Literal:
        return self._effectiveDate

    @effective_date.setter
    def effective_date(self, effectiveDate: Literal):
        self._effectiveDate = effectiveDate

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

        # Check whether revision store is empty
        # if it is not empty return an error because it should be the first transaction revision

        # Obtain the name of an already existing dataset
        nameDataset = self._request.values.get('nameDataset', None) or None
        if nameDataset is not None:
            self.name_dataset = Literal(nameDataset)

        # Obtain the url of an already existing dataset
        urlDataset = self._request.values.get('urlDataset', None) or None
        if urlDataset is not None:
            self.url_dataset = Literal(urlDataset)

        # Obtain effective date of the Snapshot
        effectiveDate = self._request.values.get('effectiveDate', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate, datatype=XSD.dateTimeStamp)
        else:
            self.effective_date = self.creation_date

        self.revision_number = revisionStore.get_new_revision_number()
        self.branch_index = revisionStore.get_new_branch_index()

    def transaction_revision_from_request(self):
        revision = InitialRevision.revision_from_data(creationDate=self._creationDate, author=self._author,
                                                      description=self._description, branch=self._branch,
                                                      revisionNumber=self._revisionNumber)
        self._transactionRevision = revision.identifier

        return revision

    def valid_revisions_from_request(self):
        # Check whether the user already uses an existing dataset, and create a snapshot and update from it.
        if self._nameDataset is not None and self._urlDataset is not None:
            snapshot = Snapshot.revision_from_data(
                revisionNumber=self._revisionNumber, branchIndex=self._branchIndex, nameDataset=self._nameDataset,
                urlDataset=self._urlDataset, effectiveDate=self._effectiveDate,
                transactionRevision=self._transactionRevision)
            update = snapshot.update_from_snapshot()
            return [snapshot, update]
        else:
            return []
