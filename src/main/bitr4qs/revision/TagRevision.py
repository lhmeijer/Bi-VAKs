from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple


class TagRevision(TransactionRevision):

    typeOfRevision = BITR4QS.TagRevision

    @property
    def valid_revision(self):
        return self._validRevision

    @valid_revision.setter
    def valid_revision(self, validRevision: URIRef):
        if validRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.tag, validRevision)))
        self._validRevision = validRevision

