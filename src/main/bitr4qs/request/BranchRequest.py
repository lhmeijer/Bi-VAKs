from .Request import Request
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.revision.Branch import Branch


class BranchRequest(Request):

    def __init__(self, request):
        super().__init__(request)
        self.branch_name = None
        self.preceding_branch = None

    @property
    def branch_name(self) -> Literal:
        return self._branchName

    @branch_name.setter
    def branch_name(self, branchName: Literal):
        self._branchName = branchName

    @property
    def preceding_branch(self) -> Branch:
        return self._precedingBranch

    @preceding_branch.setter
    def preceding_branch(self, precedingBranch: Branch):
        self._precedingBranch = precedingBranch

    def evaluate_request(self, revisionStore):
        super().evaluate_request(revisionStore)

        # Obtain the preceding branch
        precedingBranchID = self._request.view_args.get('branchID', None) or None
        precedingBranch = None
        if precedingBranchID is not None:
            precedingBranch = revisionStore.branch(precedingBranchID)
            self.preceding_valid_revision = precedingBranchID

        # Obtain the name of the branch
        branchName = self._request.values.get('branchName', None) or None
        if branchName is not None:
            self.branch_name = Literal(branchName)
        elif precedingBranch is not None:
            self.branch_name = precedingBranch.branch_name

        # Obtain the head of the transaction revisions
        transactionRevision = self._request.values.get('transaction_revision', None) or None
        if transactionRevision is None and precedingBranch is None:
            precedingTransactionRevision, revisionNumber = revisionStore.head_revision()
        elif transactionRevision is not None:
            self.preceding_transaction_revision = URIRef(transactionRevision)
            # TODO we moeten nu andere updates krijgen
        elif precedingBranch is not None:
            self.preceding_transaction_revision = precedingBranch.transaction_revision

