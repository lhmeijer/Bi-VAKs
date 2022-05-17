from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple


class TagRevision(TransactionRevision):

    typeOfRevision = BITR4QS.TagRevision

