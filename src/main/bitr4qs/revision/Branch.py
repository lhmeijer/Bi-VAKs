from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


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
    def branch_name(self) -> Literal:
        return self._branchName

    @branch_name.setter
    def branch_name(self, branchName: Literal):
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

    @classmethod
    def _revision_from_request(cls, request):
        return cls(branchName=request.branch_name, branchedOffRevision=request.branched_off_revision,
                   precedingRevision=request.preceding_valid_revision, revisionNumber=request.revision_number,
                   branchIndex=request.branch_index)