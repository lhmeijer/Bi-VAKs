from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD


class TagRequest(Request):

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

        # Obtain the preceding Tag
        precedingTagID = self._request.view_args.get('tagID', None) or None
        precedingTag = None
        if precedingTagID is not None:
            precedingTags = revisionStore.valid_revision(URIRef(precedingTagID), 'tag')
            precedingTag = precedingTags[precedingTagID]
            self.preceding_valid_revision = precedingTag.identifier
            self.branch_index = precedingTag.branch_index

        # Obtain effective date
        effectiveDate = self._request.values.get('effectiveDate', None) or None
        if effectiveDate is not None:
            self.effective_date = Literal(effectiveDate, datatype=XSD.dateTimeStamp)
        elif precedingTag is not None:
            self.effective_date = precedingTag.effective_date
        else:
            # TODO no effective date is known, return an error
            pass

        # Obtain the transaction revision
        transactionRevision = self._request.view_args.get('transactionRevision', None) or None
        if transactionRevision is not None:
            # TODO check existence
            self.transaction_revision = URIRef(transactionRevision)
        elif precedingTag is not None:
            self.transaction_revision = precedingTag.transaction_revision
        elif self._precedingTransactionRevision is not None:
            self.transaction_revision = self._precedingTransactionRevision
        else:
            # TODO no transaction revision is known, return an error
            pass

        # Obtain the name of the tag
        name = self._request.values.get('name', None) or None
        if name is not None:
            self.tag_name = Literal(name)
        elif precedingTag is not None:
            self.tag_name = precedingTag.tag_name
        else:
            # TODO no tag name is known, return an error
            pass