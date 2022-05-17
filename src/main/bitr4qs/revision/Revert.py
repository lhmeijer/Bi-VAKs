from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class Revert(ValidRevision):

    typeOfRevision = BITR4QS.Revert
    nameOfRevision = 'Revert'
    predicateOfPrecedingRevision = BITR4QS.precedingRevert

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 transactionRevisions: list = None,
                 revisionNumber: Literal = None,
                 branchIndex: Literal = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.transaction_revisions = transactionRevisions