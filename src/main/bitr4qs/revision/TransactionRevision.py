from .Revision import Revision
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.namespace import RDFS


class TransactionRevision(Revision):

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, creationDate=None, author=None,
                 description=None, branch=None, revisionNumber=None, validRevision: URIRef = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber)
        self.creation_date = creationDate
        self.author = author
        self.description = description
        self.branch = branch
        self.valid_revision = validRevision

    @property
    def creation_date(self):
        return self._creationDate

    @creation_date.setter
    def creation_date(self, creationDate):
        if creationDate is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.createdAt, creationDate)))
        self._creationDate = creationDate

    @property
    def author(self):
        return self._author

    @author.setter
    def author(self, author):
        if author is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.author, author)))
        self._author = author

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        if description is not None:
            self._RDFPatterns.append(Triple((self._identifier, RDFS.comment, description)))
        self._description = description

    @property
    def branch(self):
        return self._branch

    @branch.setter
    def branch(self, branch):
        if branch is not None:
            self._RDFPatterns.append(Triple((self._identifier, RDFS.branch, branch)))
        self._branch = branch

    @property
    def valid_revision(self):
        return self._validRevision

    @valid_revision.setter
    def valid_revision(self, validRevision: URIRef):
        self._validRevision = validRevision

    @classmethod
    def _revision_from_request(cls, request):
        return cls(creationDate=request.creation_date, author=request.author, description=request.description,
                   branch=request.branch, revisionNumber=request.revision_number, validRevision=request.valid_revision,
                   precedingRevision=request.preceding_transaction_revision)
