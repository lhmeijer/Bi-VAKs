from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.Revert import Revert, RevertRevision



class RevertRequest(Request):

    type = 'revert'

    def __init__(self, request):
        super().__init__(request)

        self._transactionRevision = None

    def evaluate_request(self, revisionStore):

        super().evaluate_request(revisionStore)
        self._transactionRevision = self._request.view_args.get('revisionID', None) or None

    def transaction_revision_from_request(self):
        revision = TagRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)
        self._currentTransactionRevision = revision.identifier
        return revision

    def valid_revisions_from_request(self):
        revision = Revert.revision_from_data(
            transactionRevision=self._transactionRevision, revisionNumber=self._revisionNumber,
            branchIndex=self._branchIndex)
        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Revert), "Valid Revision should be a Revert"
        # AssertionError

        modifiedRevision = revision.modify(
            otherTransactionRevision=self._transactionRevision, branchIndex=self._branchIndex,
            revisionNumber=self._revisionNumber, revisionStore=revisionStore)
        return [modifiedRevision]