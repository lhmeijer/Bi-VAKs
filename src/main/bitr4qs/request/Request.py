from rdflib.term import Literal, URIRef
import datetime
from rdflib.namespace import XSD


class Request(object):

    def __init__(self, request):
        self._request = request
        self._branch = None
        self._creationDate = None
        self._author = None
        self._description = None

        self._precedingTransactionRevision = None
        self._precedingValidRevision = None

        self._validRevision = None

        self._revisionNumber = None
        self._branchIndex = None

    @property
    def branch(self) -> URIRef:
        return self._branch

    @branch.setter
    def branch(self, branch: URIRef):
        self._branch = branch

    @property
    def author(self) -> Literal:
        return self._author

    @author.setter
    def author(self, author: Literal):
        self._author = author

    @property
    def creation_date(self) -> Literal:
        return self._creationDate

    @creation_date.setter
    def creation_date(self, creationDate: Literal):
        self._creationDate = creationDate

    @property
    def description(self) -> Literal:
        return self._description

    @description.setter
    def description(self, description: Literal):
        self._description = description

    @property
    def preceding_transaction_revision(self) -> URIRef:
        return self._precedingTransactionRevision

    @preceding_transaction_revision.setter
    def preceding_transaction_revision(self, precedingTransactionRevision: URIRef):
        self._precedingTransactionRevision = precedingTransactionRevision

    @property
    def preceding_valid_revision(self) -> URIRef:
        return self._precedingValidRevision

    @preceding_valid_revision.setter
    def preceding_valid_revision(self, precedingValidRevision: URIRef):
        self._precedingValidRevision = precedingValidRevision

    @property
    def valid_revision(self) -> URIRef:
        return self._validRevision

    @valid_revision.setter
    def valid_revision(self, validRevision: URIRef):
        self._validRevision = validRevision

    @property
    def revision_number(self) -> Literal:
        return self._revisionNumber

    @revision_number.setter
    def revision_number(self, revisionNumber: Literal):
        self._revisionNumber = revisionNumber

    @property
    def branch_index(self) -> Literal:
        return self._branchIndex

    @branch_index.setter
    def branch_index(self, branchIndex: Literal):
        self._branchIndex = branchIndex

    def evaluate_request(self, revisionStore):
        # Obtain the author
        author = self._request.values.get('author', None) or None
        if author is not None:
            self.author = Literal(author)
            print("author ", self._author)

        # Obtain the description
        description = self._request.values.get('description', None) or None
        if description is not None:
            self.description = Literal(description)
            print("description ", self._description)

        # Obtain the branch based on the branch name
        branchName = self._request.values.get('branch', None) or None
        print("branchName ", branchName)
        if branchName is not None:
            branch = revisionStore.branch_from_name(Literal(branchName))
            self.branch = branch.identifier
            self.branch_index = branch.branch_index

        # Obtain the head of the transaction revisions and its revision number
        precedingTransactionRevision, revisionNumber = revisionStore.head_revision(self._branch)
        print("precedingTransactionRevision ", precedingTransactionRevision)
        if precedingTransactionRevision is not None:
            self.preceding_transaction_revision = precedingTransactionRevision

        if revisionNumber is not None:
            self.revision_number = revisionNumber

        # Obtain the creation date of the transaction revision
        time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02.00")
        self.creation_date = Literal(time, datatype=XSD.dateTimeStamp)
