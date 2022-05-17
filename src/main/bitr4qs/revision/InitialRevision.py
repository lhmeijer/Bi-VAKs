from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple


class InitialRevision(TransactionRevision):

    typeOfRevision = BITR4QS.InitialRevision
