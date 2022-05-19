from .Request import Request
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.revision.Branch import Branch, BranchRevision


class BranchRequest(Request):

    type = 'branch'

    def __init__(self, request):
        super().__init__(request)

        self.branch_name = None
        self.branched_off_revision = None

    @property
    def branch_name(self) -> Literal:
        return self._branchName

    @branch_name.setter
    def branch_name(self, branchName: Literal):
        self._branchName = branchName

    @property
    def branched_off_revision(self) -> URIRef:
        return self._branchedOffRevision

    @branched_off_revision.setter
    def branched_off_revision(self, branchedOffRevision: URIRef):
        self._branchedOffRevision = branchedOffRevision

    def evaluate_request(self, revisionStore):

        super().evaluate_request(revisionStore)

        # Obtain the name of the branch
        branchName = self._request.values.get('branchName', None) or None
        print("branchName ", branchName)
        if branchName is not None:
            self.branch_name = Literal(branchName)
        else:
            # TODO return an error
            pass

        # Obtain the branch from which the branch branches off.
        revisionID = self._request.values.get('revision', None) or None
        print("revisionID ", revisionID)
        if revisionID is not None:
            # TODO check existence branch and revision
            transactionRevision = revisionStore.revision(revisionID=URIRef(revisionID), transactionRevision=True)
            print("transactionRevision ", transactionRevision)
            self.branched_off_revision = transactionRevision.identifier
            self.preceding_transaction_revision = transactionRevision.identifier
            self.revision_number = revisionStore.get_new_revision_number(transactionRevision.revision_number)
        else:
            self.branched_off_revision = self._precedingTransactionRevision

        self.branch_index = revisionStore.get_new_branch_index()
        self.head_revision = None
        self.branch = None

    def evaluate_request_to_modify(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain the name of the branch
        branchName = self._request.values.get('branchName', None) or None
        print("branchName ", branchName)
        if branchName is not None:
            self.branch_name = Literal(branchName)

        # Obtain the branch from which the branch branches off.
        revisionID = self._request.values.get('revision', None) or None
        if revisionID is not None:
            transactionRevision = revisionStore.revision(revisionID=URIRef(revisionID), transactionRevision=True)
            self.branched_off_revision = transactionRevision.identifier
            self.preceding_transaction_revision = transactionRevision.identifier
            self.revision_number = revisionStore.get_new_revision_number(transactionRevision.revision_number)

    def transaction_revision_from_request(self):
        revision = BranchRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)

        return revision

    def valid_revisions_from_request(self):
        print("self._branchName ", self._branchName)
        print("self._branchedOffRevision ", self._branchedOffRevision)
        print('self._branchIndex ', self._branchIndex)
        revision = Branch.revision_from_data(branchName=self._branchName, revisionNumber=self._revisionNumber,
                                             branchedOffRevision=self._branchedOffRevision, branchIndex=self._branchIndex)
        print("revision ", revision)
        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Branch), "Valid Revision should be a Branch"
        # AssertionError

        modifiedRevisions = revision.modify(
            otherBranchName=self._branchName, otherBranchedOffRevision=self._branchedOffRevision,
            branchIndex=self._branchIndex, revisionNumber=self._revisionNumber, revisionStore=revisionStore)
        return modifiedRevisions
