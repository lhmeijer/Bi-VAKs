from .Revision import Revision
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.namespace import RDFS


class TransactionRevision(Revision):

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, creationDate=None, author=None,
                 description=None, branch=None, revisionNumber=None, validRevisions: list = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber)
        self.creation_date = creationDate
        self.author = author
        self.description = description
        self.branch = branch
        self.valid_revisions = validRevisions

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
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branch, branch)))
        self._branch = branch

    @property
    def valid_revisions(self):
        return self._validRevisions

    @valid_revisions.setter
    def valid_revisions(self, validRevisions: list):
        if validRevisions is not None:
            for validRevision in validRevisions:
                if 'Snapshot' in str(validRevision):
                    self._RDFPatterns.append(Triple((self._identifier, BITR4QS.snapshot, validRevision)))
                elif 'Tag' in str(validRevision):
                    self._RDFPatterns.append(Triple((self._identifier, BITR4QS.tag, validRevision)))
                elif 'Update' in str(validRevision):
                    self._RDFPatterns.append(Triple((self._identifier, BITR4QS.update, validRevision)))
        self._validRevisions = validRevisions

    @classmethod
    def _revision_from_request(cls, request):
        return cls(creationDate=request.creation_date, author=request.author, description=request.description,
                   branch=request.branch, revisionNumber=request.revision_number,
                   precedingRevision=request.preceding_transaction_revision)
