from .ValidRevision import ValidRevision
from .TransactionRevision import TransactionRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS


class TagRevision(TransactionRevision):

    typeOfRevision = BITR4QS.TagRevision
    nameOfRevision = 'TagRevision'


class Tag(ValidRevision):

    typeOfRevision = BITR4QS.Tag
    nameOfRevision = 'Tag'
    predicateOfPrecedingRevision = BITR4QS.precedingTag

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 tagName: Literal = None,
                 effectiveDate: Literal = None,
                 transactionRevision: URIRef = None,
                 revisionNumber: Literal = None,
                 branchIndex: Literal = None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.tag_name = tagName
        self.effective_date = effectiveDate
        self.transaction_revision = transactionRevision

    @property
    def tag_name(self):
        return self._tagName

    @tag_name.setter
    def tag_name(self, tagName: Literal):
        if tagName is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.tagName, tagName)))
        self._tagName = tagName

    @property
    def effective_date(self):
        return self._effectiveDate

    @effective_date.setter
    def effective_date(self, effectiveDate: Literal):
        if effectiveDate is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.validAt, effectiveDate)))
        self._effectiveDate = effectiveDate

    @property
    def transaction_revision(self):
        return self._transactionRevision

    @transaction_revision.setter
    def transaction_revision(self, transactionRevision: URIRef):
        if transactionRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.transactedAt, transactionRevision)))
        self._transactionRevision = transactionRevision

    def modify(self, revisionStore, otherTagName=None, otherEffectiveDate=None, otherTransactionRevision=None,
               revisionNumber=None, branchIndex=None):

        tagName = otherTagName if otherTagName is not None else self._tagName
        effectiveDate = otherEffectiveDate if otherEffectiveDate is not None else self._effectiveDate
        transactionRevision = otherTransactionRevision if otherTransactionRevision is not None else self._transactionRevision

        modifiedTag = Tag.revision_from_data(
            effectiveDate=effectiveDate, branchIndex=branchIndex, tagName=tagName, revisionNumber=revisionNumber,
            transactionRevision=transactionRevision, precedingRevision=self._identifier)
        return modifiedTag

    def revert(self, revisionStore, revisionNumber=None, branchIndex=None):
        # Check whether there exists a preceding snapshot
        if self._precedingRevision is not None:
            # Get the preceding tag
            otherTag = ...
            revertedTag = self.modify()
        else:
            # Remove this snapshot from Jena
            revertedTag = Tag.revision_from_data(revisionNumber=revisionNumber, branchIndex=branchIndex, tagName=None,
                                                 transactionRevision=None, effectiveDate=None,
                                                 precedingRevision=self._identifier)
        return revertedTag

    @classmethod
    def _revision_from_request(cls, request):
        return cls(tagName=request.tag_name, effectiveDate=request.effective_date, transactionRevision=request.transaction_revision,
                   precedingRevision=request.preceding_revision)

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'branchIndex' in data, "branchIndex should be in the data of the revision"
        assert 'tagName' in data, "tagName should be in the data of the revision"
        assert 'effectiveDate' in data, "effectiveDate should be in the data of the revision"
        assert 'transactionRevision' in data, "transactionRevision should be in the data of the revision"

        return cls(**data)

    def __dict__(self):
        result = super().__dict__()
        result['tagName'] = str(self._tagName)
        result['effectiveDate'] = str(self._effectiveDate)
        result['transactionRevision'] = str(self._transactionRevision)
        return result


