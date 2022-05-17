from .Request import Request
from rdflib.term import URIRef, Literal


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

        # Obtain the preceding branch
        precedingBranchID = self._request.view_args.get('branchID', None) or None
        precedingBranch = None
        if precedingBranchID is not None:
            precedingBranches = revisionStore.revision(URIRef(precedingBranchID), 'branch', validRevision=True)
            precedingBranch = precedingBranches[precedingBranchID]
            self.preceding_valid_revision = precedingBranch.identifier
            self.branch_index = precedingBranch.branch_index

        # Obtain the name of the branch
        branchName = self._request.values.get('branchName', None) or None
        if branchName is not None:
            self.branch_name = Literal(branchName)
        elif precedingBranch is not None:
            self.branch_name = precedingBranch.branch_name
        else:
            # TODO no branch name is known, return an error
            pass

        # Obtain the branch from which the branch branches off.
        revisionID = self._request.values.get('revision', None) or None
        transactionRevision = None
        if revisionID is not None:
            transactionRevision = revisionStore.revision(revisionID=URIRef(revisionID), transactionRevision=True)

        # Both the transaction revision as the preceding branch is not None, so the user would like to change the
        # transaction revision it branches off from.
        if transactionRevision is not None and precedingBranch is not None:
            # Check whether new transaction revision is older than the transaction revision of the preceding branch.
            # Otherwise it is not allowed to make this change
            # TODO check whether the updates can be moved to another place in the revision graph
            # Obtain all updates in the current branch.
            updates = ...
            # Check whether we can add these updates to the new place where the branch branches off.
            # Create these new updates and add them to the branch request
            self.branched_off_revision = transactionRevision.identifier
        elif transactionRevision is not None and precedingBranch is None:
            self.branched_off_revision = transactionRevision.identifier
            self.preceding_transaction_revision = transactionRevision.identifier
            self.revision_number = revisionStore.get_new_revision_number(transactionRevision.revision_number)
            self.head_revision = None
        elif precedingBranch is not None and transactionRevision is None:
            self.branched_off_revision = precedingBranch.transaction_revision
        elif precedingBranch is None and self._precedingTransactionRevision is not None:
            self.branched_off_revision = self._precedingTransactionRevision
        else:
            # TODO no transaction revision is known, return an error
            pass

        if precedingBranch is None:
            self.branch_index = revisionStore.get_new_branch_index()
