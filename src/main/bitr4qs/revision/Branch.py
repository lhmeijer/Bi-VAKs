from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class Branch(ValidRevision):

    typeOfRevision = BITR4QS.Branch
    nameOfRevision = 'Branch'

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 name: Literal = None,
                 transactionRevision: URIRef = None,
                 revisionNumber: Literal = None,
                 branchIndex: Literal = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.name = name
        self.transaction_revision = transactionRevision

    @property
    def name(self) -> Literal:
        return self._name

    @name.setter
    def name(self, name: Literal):
        if name is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.name, name)))
        self._name = name

    @property
    def transaction_revision(self):
        return self._transactionRevision

    @transaction_revision.setter
    def transaction_revision(self, transactionRevision: URIRef):
        if transactionRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.transactedAt, transactionRevision)))
        self._transactionRevision = transactionRevision

    @classmethod
    def _revision_from_request(cls, request):
        return cls(name=request.branch_name, transactionRevision=request.preceding_transaction_revision,
                   precedingRevision=request.preceding_valid_revision, revisionNumber=request.revision_number,
                   branchIndex=request.branch_index)