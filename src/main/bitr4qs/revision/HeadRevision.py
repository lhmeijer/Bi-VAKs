from .Revision import Revision
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class HeadRevision(Revision):

    typeOfRevision = BITR4QS.HeadRevision
    nameOfRevision = 'HeadRevision'

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

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'precedingRevision' in data, "precedingRevision should be in the data of the revision"
        assert 'branch' in data, "branch should be in the data of the revision"

        return cls(**data)

    def __dict__(self):
        result = super().__dict__()
        if self._branch is not None:
            result['branch'] = str(self._branch)
        return result