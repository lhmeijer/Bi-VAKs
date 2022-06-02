from .Revision import Revision
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class ValidRevision(Revision):

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, revisionNumber=None,
                 branchIndex=None):
        super().__init__(identifier=identifier, precedingRevision=precedingRevision,
                         hexadecimalOfHash=hexadecimalOfHash, revisionNumber=revisionNumber)
        self.branch_index = branchIndex

    @property
    def branch_index(self):
        return self._branchIndex

    @branch_index.setter
    def branch_index(self, branchIndex):
        if branchIndex is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branchIndex, branchIndex)))
        self._branchIndex = branchIndex

    def __dict__(self):
        result = super().__dict__()
        if self._branchIndex is not None:
            result['branchIndex'] = str(self._branchIndex)
        return result