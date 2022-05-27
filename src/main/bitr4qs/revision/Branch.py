from .ValidRevision import ValidRevision
from .TransactionRevision import TransactionRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class BranchRevision(TransactionRevision):

    typeOfRevision = BITR4QS.BranchRevision
    nameOfRevision = 'BranchRevision'


class Branch(ValidRevision):

    typeOfRevision = BITR4QS.Branch
    nameOfRevision = 'Branch'
    predicateOfPrecedingRevision = BITR4QS.precedingBranch

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 branchName: Literal = None,
                 branchedOffRevision: URIRef = None,
                 revisionNumber: Literal = None,
                 branchIndex: Literal = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.branch_name = branchName
        self.branched_off_revision = branchedOffRevision

    @property
    def branch_name(self):
        return self._branchName

    @branch_name.setter
    def branch_name(self, branchName):
        if branchName is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branchName, branchName)))
        self._branchName = branchName

    @property
    def branched_off_revision(self):
        return self._branchedOffRevision

    @branched_off_revision.setter
    def branched_off_revision(self, branchedOffRevision: URIRef):
        if branchedOffRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branchedOffAt, branchedOffRevision)))
        self._branchedOffRevision = branchedOffRevision

    def modify(self, revisionStore, otherBranchName=None, otherBranchedOffRevision=None, revisionNumber=None, branchIndex=None):

        branchName = otherBranchName if otherBranchName is not None else self._branchName

        if otherBranchedOffRevision is not None:
            # Get all updates which corresponds to
            updates = ...
            # Add and delete these updates from the snapshot
            branchedOffRevision = otherBranchedOffRevision
        else:
            branchedOffRevision = self._branchedOffRevision

        modifiedBranch = Branch.revision_from_data(
            branchName=branchName, branchIndex=branchIndex, branchedOffRevision=branchedOffRevision,
            revisionNumber=revisionNumber, precedingRevision=self._identifier)

        return modifiedBranch

    def revert(self, revisionStore, revisionNumber=None, branchIndex=None):
        # Check whether there exists a preceding snapshot
        if self._precedingRevision is not None:
            # Get the preceding snapshot
            otherBranch = revisionStore.revision(self._precedingRevision, revisionType='branch', validRevision=True)
            revertedBranch = self.modify(
                revisionNumber=revisionNumber, branchIndex=branchIndex, otherBranchName=otherBranch.branch_name,
                otherBranchedOffRevision=otherBranch.branched_off_revision, revisionStore=revisionStore)
        else:
            revertedBranch = Branch.revision_from_data(revisionNumber=revisionNumber, branchIndex=branchIndex,
                                                       branchName=None, branchedOffRevision=None,
                                                       precedingRevision=self._identifier)
        return revertedBranch

    @classmethod
    def _revision_from_request(cls, request):
        return cls(branchName=request.branch_name, branchedOffRevision=request.branched_off_revision,
                   precedingRevision=request.preceding_valid_revision, revisionNumber=request.revision_number,
                   branchIndex=request.branch_index)

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'branchName' in data, "branchName should be in the data of the revision"
        assert 'branchedOffRevision' in data, "branchedOffRevision should be in the data of the revision"
        assert 'branchIndex' in data, "branchIndex should be in the data of the revision"

        return cls(**data)

    def __dict__(self):
        result = super().__dict__()
        result['branchName'] = str(self._branchName)
        result['branchedOffRevision'] = str(self._branchedOffRevision)
        return result