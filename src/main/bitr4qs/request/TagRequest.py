from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.Tag import Tag, TagRevision


class TagRequest(Request):

    type = 'tag'

    def __init__(self, request):
        super().__init__(request)

        self._effectiveDate = None
        self._transactionRevision = None
        self._tagName = None

    @property
    def effective_date(self) -> Literal:
        return self._effectiveDate

    @effective_date.setter
    def effective_date(self, effectiveDate: Literal):
        self._effectiveDate = effectiveDate

    @property
    def transaction_revision(self) -> Literal:
        return self._transactionRevision

    @transaction_revision.setter
    def transaction_revision(self, transactionRevision: URIRef):
        self._transactionRevision = transactionRevision

    @property
    def tag_name(self) -> Literal:
        return self._tagName

    @tag_name.setter
    def tag_name(self, tagName: Literal):
        self._tagName = tagName

    def evaluate_request(self, revisionStore):

        super().evaluate_request(revisionStore)

        # Obtain effective date
        effectiveDate = self._request.values.get('date', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate, datatype=XSD.dateTimeStamp)
        else:
            self.effective_date = self.creation_date

        # Obtain the transaction time based on a given transaction revision
        revisionID = self._request.values.get('revision', None) or None
        if revisionID is not None:
            transactionRevision = revisionStore.revision(revisionID=URIRef(revisionID), transactionRevision=True)
            self.transaction_revision = transactionRevision.identifier

        # Obtain the name of the tag
        name = self._request.values.get('name', None) or None
        if name is not None:
            self.tag_name = Literal(name)

    def transaction_revision_from_request(self):
        revision = TagRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)

        if self._transactionRevision is None:
            self.transaction_revision = revision.identifier

        return revision

    def valid_revisions_from_request(self):
        revision = Tag.revision_from_data(
            tagName=self._tagName, revisionNumber=self._revisionNumber, effectiveDate=self._effectiveDate,
            transactionRevision=self._transactionRevision, branchIndex=self._branchIndex)
        return [revision]

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Tag), "Valid Revision should be a Tag"
        # AssertionError

        modifiedRevision = revision.modify(
            otherTagName=self._tagName, branchIndex=self._branchIndex, otherEffectiveDate=self._effectiveDate,
            otherTransactionRevision=self._transactionRevision, revisionNumber=self._revisionNumber,
            revisionStore=revisionStore)
        return [modifiedRevision]