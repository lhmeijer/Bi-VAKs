from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD


class RevertRequest(Request):

    type = 'revert'

    def __init__(self, request):
        super().__init__(request)

        self._transactionRevisions = None

    @property
    def transaction_revisions(self) :
        return self._transactionRevisions

    @transaction_revisions.setter
    def transaction_revisions(self, transactionRevisions):
        self._transactionRevisions = transactionRevisions

    def evaluate_request(self, revisionStore):

        super().evaluate_request(revisionStore)

        # Obtain the preceding Revert
        precedingRevertID = self._request.view_args.get('revertID', None) or None
        precedingRevert = None
        if precedingRevertID is not None:
            precedingReverts = revisionStore.revision(URIRef(precedingRevertID), 'revert', validRevision=True)
            precedingRevert = precedingReverts[precedingRevertID]
            self.preceding_valid_revision = precedingRevert.identifier
            self.branch_index = precedingRevert.branch_index

        revision = revisionStore.revision(self._headRevision.preceding_revision, transactionRevision=True)