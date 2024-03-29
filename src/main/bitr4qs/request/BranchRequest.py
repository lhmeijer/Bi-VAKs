from .Request import Request
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.revision.Branch import Branch, BranchRevision


class BranchRequest(Request):

    type = 'branch'

    def __init__(self, request):
        super().__init__(request)

        self._branchName = None
        self._branchedOffRevision = None

    def evaluate_request(self, revisionStore):

        self.evaluate_request_to_modify(revisionStore)

        if self._branchedOffRevision is None:
            self._branchedOffRevision = self._precedingTransactionRevision

        self._branchIndex, self._branchIndexValidRevision = revisionStore.new_branch_index()
        self._headRevision = None

        # TODO throw error if not all variables have a value

    def evaluate_request_to_modify(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain the name of the branch
        branchName = self._request.values.get('name', None) or None
        if branchName:
            self._branchName = Literal(branchName)

        # Obtain the branch from which the branch branches off.
        revisionID = self._request.values.get('revision', None) or None
        if revisionID is not None:
            revision = revisionStore.revision(revisionID=URIRef(revisionID), isValidRevision=False,
                                              transactionRevisionA=self._precedingTransactionRevision)
            self._branchedOffRevision = revision.identifier
            self._precedingTransactionRevision = revision.identifier
            self._revisionNumber, self._revisionNumberValidRevision = revisionStore.new_revision_number(
                revision.revision_number)

        self._branch = None

    def transaction_revision_from_request(self):
        revision = BranchRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branchIndex=self._branchIndex, revisionNumber=self._revisionNumber)
        self._currentTransactionRevision = revision.identifier
        return revision

    def valid_revisions_from_request(self):

        revision = Branch.revision_from_data(
            branchName=self._branchName, revisionNumber=self._revisionNumberValidRevision,
            branchedOffRevision=self._branchedOffRevision, branchIndex=self._branchIndex)
        self._branch = revision.identifier

        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Branch), "Valid Revision should be a Branch"
        # AssertionError

        modifiedRevisions = revision.modify(
            otherBranchName=self._branchName, otherBranchedOffRevision=self._branchedOffRevision,
            branchIndex=self._branchIndex, revisionNumber=self._revisionNumberValidRevision, revisionStore=revisionStore)

        self._branch = modifiedRevisions.identifier
        return modifiedRevisions
