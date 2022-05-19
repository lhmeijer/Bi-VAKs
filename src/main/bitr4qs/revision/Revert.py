from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from .TransactionRevision import TransactionRevision


class RevertRevision(TransactionRevision):

    typeOfRevision = BITR4QS.RevertRevision
    nameOfRevision = 'RevertRevision'


class Revert(ValidRevision):

    typeOfRevision = BITR4QS.Revert
    nameOfRevision = 'Revert'
    predicateOfPrecedingRevision = BITR4QS.precedingRevert

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 transactionRevision: URIRef = None,
                 revisionNumber: Literal = None,
                 branchIndex: Literal = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.transaction_revision = transactionRevision