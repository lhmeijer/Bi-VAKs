from .Revision import Revision
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class HeadRevision(Revision):

    typeOfRevision = BITR4QS.HeadRevision

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, revisionNumber=None,
                 branch=None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber)
        self.branch = branch

    @property
    def branch(self) -> URIRef:
        return self._branch

    @branch.setter
    def branch(self, branch: URIRef):
        if branch is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branch, branch)))
        self._branch = branch