from .Revision import Revision
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.namespace import RDFS


class TransactionRevision(Revision):

    typeOfRevision = BITR4QS.TransactionRevision
    nameOfRevision = 'Revision'

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, creationDate=None, author=None,
                 description=None, branchIndex=None, revisionNumber=None, validRevisions: list = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.creation_date = creationDate
        self.author = author
        self.description = description
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
    def valid_revisions(self):
        return self._validRevisions

    @valid_revisions.setter
    def valid_revisions(self, validRevisions: list):

        if validRevisions is not None:
            self._validRevisions = []
            for validRevisionID in validRevisions:
                self._add_triple_to_valid_revisions(validRevisionID)
        else:
            self._validRevisions = validRevisions

    def add_valid_revision(self, validRevisionID: URIRef):

        if validRevisionID is not None:

            if self._validRevisions is None:
                self._validRevisions = []

            self._add_triple_to_valid_revisions(validRevisionID)

    def _add_triple_to_valid_revisions(self, validRevisionID):
        if 'Snapshot' in str(validRevisionID):
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.snapshot, validRevisionID)))
            self._validRevisions.append(('snapshot', validRevisionID))
        elif 'Tag' in str(validRevisionID):
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.tag, validRevisionID)))
            self._validRevisions.append(('tag', validRevisionID))
        elif 'Update' in str(validRevisionID):
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.update, validRevisionID)))
            self._validRevisions.append(('update', validRevisionID))
        elif 'Branch' in str(validRevisionID):
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branch, validRevisionID)))
            self._validRevisions.append(('branch', validRevisionID))

    @classmethod
    def _revision_from_request(cls, request):
        return cls(creationDate=request.creation_date, author=request.author, description=request.description,
                   branchIndex=request.branch_index, revisionNumber=request.revision_number,
                   precedingRevision=request.preceding_transaction_revision)

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'creationDate' in data, "creationDate should be in the data of the revision"
        assert 'author' in data, "author should be in the data of the revision"
        assert 'description' in data, "description should be in the data of the revision"
        assert 'branchIndex' in data, "branchIndex should be in the data of the revision"
        assert 'precedingRevision' in data, "precedingRevision should be in the data of the revision"

        return cls(**data)

    def __dict__(self):
        result = super().__dict__()
        result['author'] = str(self._author)
        result['description'] = str(self._description)
        result['creationDate'] = str(self._creationDate)
        if self._validRevisions:
            result['validRevisions'] = [(revision[0], str(revision[1])) for revision in self._validRevisions]
        return result
