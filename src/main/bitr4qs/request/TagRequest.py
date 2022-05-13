from .Request import Request
from rdflib.term import URIRef, Literal


class TagRequest(Request):

    def __init__(self, request):
        super().__init__(request)

        self._effectiveDate = None
        self._transactionRevision = None
        self._name = None

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
    def name(self) -> Literal:
        return self._name

    @name.setter
    def name(self, name: Literal):
        self._name = name

    def evaluate_request(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain effective date
        effectiveDate = self._request.values.get('effectiveDate', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate)

        # Obtain the transaction revision
        revision = self._request.view_args.get('revision', None) or None
        if revision is not None:
            self.transaction_revision = URIRef(revision)

        # Obtain effective date
        name = self._request.values.get('name', None) or None
        if name is not None:
            self.name = Literal(name)