from .Request import Request
from src.main.bitr4qs.term.Triple import Triple
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Quad import Quad
from src.main.bitr4qs.term.Modification import Modification
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.Update import Update, UpdateRevision


class UpdateRequest(Request):

    type = 'update'

    def __init__(self, request):
        super().__init__(request)
        self._startDate = None
        self._endDate = None
        self._modifications = None

    def evaluate_request(self, revisionStore):
        self.evaluate_request_to_modify(revisionStore)

    def evaluate_request_to_modify(self, revisionStore):

        super().evaluate_request(revisionStore)

        # Obtain start date
        startDate = self._request.values.get('startDate', None) or None
        if startDate is not None:
            self._startDate = Literal(str(startDate), datatype=XSD.dateTimeStamp)

        # Obtain end date
        endDate = self._request.values.get('endDate', None) or None
        if endDate is not None:
            self._endDate = Literal(str(endDate), datatype=XSD.dateTimeStamp)

        # Obtain the modifications
        self._modifications = self._request.values.get('modifications', None) or None

    def transaction_revision_from_request(self):
        revision = UpdateRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)

        return revision

    def valid_revisions_from_request(self):
        revision = Update.revision_from_data(
            startDate=self._startDate, revisionNumber=self._revisionNumber, endDate=self._endDate,
            modifications=self._modifications, branchIndex=self._branchIndex)
        return [revision]


class UpdateQueryRequest(UpdateRequest):

    def __init__(self, updateQuery):
        super().__init__(updateQuery.request)
        self._updateQuery = updateQuery

    def _modifications_fom_query(self, revisionStore):
        self._modifications = []
        for update in self._updateQuery.translated_query:
            print("update ", update)
            if update.name == 'InsertData':
                node = self._evaluate_insert_or_delete_data(update, revisionStore)
                if node is not None:
                    return node
            elif update.name == 'DeleteData':
                node = self._evaluate_insert_or_delete_data(update, revisionStore, deletion=True)
                if node is not None:
                    return node
            else:
                pass
        return None

    def _can_quad_be_added_to_update(self, quad, revisionStore, deletion=False):
        """

        :param quad:
        :param revisionStore:
        :param deletion:
        :return:
        """
        canBeAdded = revisionStore.can_quad_be_added_or_deleted(
            quad=quad, deletion=deletion, endDate=self._endDate, headRevision=self._precedingTransactionRevision,
            startDate=self._startDate)
        return canBeAdded

    def _evaluate_insert_or_delete_data(self, update, revisionStore, deletion=False):
        """

        :param update:
        :param revisionStore:
        :param deletion:
        :return:
        """
        # Check whether a deleted or inserted triple can be added to the update
        for triple in update.triples:
            print("triple ", triple)
            tripleNode = Triple(triple)
            if self._can_quad_be_added_to_update(tripleNode, revisionStore, deletion):
                self._modifications.append(Modification(tripleNode, deletion))
            else:
                return tripleNode

        for graph in update.quads:
            for triple in update.quads[graph]:
                quadNode = Quad(triple, graph)
                print("triple ", triple)
                print("graph ", graph)
                print("quadNode ", quadNode)
                if self._can_quad_be_added_to_update(quadNode, revisionStore, deletion):
                    self._modifications.append(Modification(quadNode, deletion))
                else:
                    return quadNode
        return None

    def evaluate_request(self, revisionStore):
        self.evaluate_request_to_modify(revisionStore)

        node = self._modifications_fom_query(revisionStore)
        if node is not None:
            raise Exception("Quad can not be added or deleted to revision store.")


class ModifiedRepeatedUpdateRequest(UpdateRequest):

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"
        # AssertionError

        modifiedRevision = revision.modify(
            otherStartDate=self._startDate, otherEndDate=self._endDate, branchIndex=self._branchIndex,
            otherModifications=self._modifications, revisionNumber=self._revisionNumber, revisionStore=revisionStore,
            relatedContent=False, headRevision=self._headRevision.preceding_revision)
        return [modifiedRevision]

    def reversions_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"

        revertedRevision = revision.revert(revisionStore=revisionStore, revisionNumber=self._revisionNumber,
                                           branchIndex=self._branchIndex, relatedContent=False,
                                           headRevision=self._headRevision.preceding_revision)
        return revertedRevision


class ModifiedRelatedUpdateRequest(UpdateRequest):

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"
        # AssertionError

        modifiedRevision = revision.modify(
            otherStartDate=self._startDate, otherEndDate=self._endDate, branchIndex=self._branchIndex,
            otherModifications=self._modifications, revisionNumber=self._revisionNumber, revisionStore=revisionStore,
            relatedContent=True, headRevision=self._headRevision.preceding_revision)
        return [modifiedRevision]

    def reversions_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"

        revertedRevision = revision.revert(revisionStore=revisionStore, revisionNumber=self._revisionNumber,
                                           branchIndex=self._branchIndex, relatedContent=True,
                                           headRevision=self._headRevision.preceding_revision)
        return revertedRevision

