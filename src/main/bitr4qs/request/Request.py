from rdflib.term import Literal, URIRef
import datetime
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.TransactionRevision import TransactionRevision
from src.main.bitr4qs.exception import MissingInformationError


class Request(object):

    type = 'request'

    def __init__(self, request):
        self._request = request
        self._branch = None
        self._creationDate = None
        self._author = None
        self._description = None
        self._precedingTransactionRevision = None
        self._currentTransactionRevision = None

        self._revisionNumber = None
        self._revisionNumberValidRevision = None
        self._branchIndex = None

        self._headRevision = None

    @property
    def branch(self):
        return self._branch

    @property
    def current_transaction_revision(self):
        return self._currentTransactionRevision

    @property
    def revision_number(self):
        return self._revisionNumber

    @property
    def head_revision(self):
        return self._headRevision

    def evaluate_request(self, revisionStore):
        # Obtain the author
        author = self._request.values.get('author', None) or None
        if author is not None:
            self._author = Literal(author)
        else:
            raise MissingInformationError("The author of a transaction revision is not given.")

        # Obtain the description
        description = self._request.values.get('description', None) or None
        if description is not None:
            self._description = Literal(description)
        else:
            raise MissingInformationError("The description of a transaction revision is not given.")

        # Obtain the branch based on the branch name
        branchName = self._request.values.get('branch', None) or None
        if branchName:
            try:
                branch = revisionStore.branch_from_name(Literal(branchName))
                self._branch = branch.identifier
                self._branchIndex = branch.branch_index
            except Exception as e:
                raise e
        else:
            self._branchIndex = revisionStore.main_branch_index()

        # Obtain the head of the transaction revisions and its revision number
        try:
            self._headRevision = revisionStore.head_revision(self._branch)
            self._precedingTransactionRevision = self._headRevision.preceding_revision
            self._revisionNumber, self._revisionNumberValidRevision = revisionStore.new_revision_number(
                self._headRevision.revision_number)
        except Exception as e:
            raise e

        # Obtain the creation date of the transaction revision
        time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00")
        self._creationDate = Literal(str(time), datatype=XSD.dateTimeStamp)

    def evaluate_request_to_modify(self, revisionStore):
        pass

    def transaction_revision_from_request(self):
        revision = TransactionRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)
        self._currentTransactionRevision = revision.identifier
        return revision

    def valid_revisions_from_request(self):
        pass

    def modifications_from_request(self, revision, revisionStore):
        pass

    def reversions_from_request(self, revision, revisionStore):
        revertedRevision = revision.revert(revisionStore=revisionStore, branchIndex=self._branchIndex,
                                           revisionNumber=self._revisionNumberValidRevision)
        return revertedRevision
