from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple


class BranchRevision(TransactionRevision):

    typeOfRevision = BITR4QS.BranchRevision
