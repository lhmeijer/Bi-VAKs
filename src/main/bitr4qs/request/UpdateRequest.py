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

        self._shouldBeTested = None

    def evaluate_request(self, revisionStore):
        """
        Function to evaluate an update request.
        :param revisionStore:
        :return:
        """
        self.evaluate_request_to_modify(revisionStore)

    def evaluate_request_to_modify(self, revisionStore):
        """
        Function to evaluate an update request when we would like to modify an existing update.
        :param revisionStore:
        :return:
        """

        super().evaluate_request(revisionStore)

        # Obtain the start date from the user's request. If unknown -> startDate = None
        startDate = self._request.values.get('startDate', None) or None
        if startDate is not None:
            if startDate == 'unknown':
                self._startDate = None
            else:
                self._startDate = Literal(str(startDate), datatype=XSD.dateTimeStamp)

        # Obtain end date from the user's request. If unknown -> endDate = None
        endDate = self._request.values.get('endDate', None) or None
        if endDate is not None:
            if endDate == 'unknown':
                self._endDate = None
            else:
                self._endDate = Literal(str(endDate), datatype=XSD.dateTimeStamp)

        shouldBeTested = self._request.values.get('test', None) or None
        if shouldBeTested:
            if shouldBeTested == 'yes' or shouldBeTested == 'no':
                self._shouldBeTested = shouldBeTested

        # Obtain the modifications
        self._modifications = self._request.values.get('modifications', None) or None

    def transaction_revision_from_request(self):
        """
        Function to get an UpdateRevision from the data extracted from the user's request.
        :return:
        """
        revision = UpdateRevision.revision_from_data(
            precedingRevision=self._precedingTransactionRevision, creationDate=self._creationDate, author=self._author,
            description=self._description, branch=self._branch, revisionNumber=self._revisionNumber)
        self._currentTransactionRevision = revision.identifier
        return revision

    def valid_revisions_from_request(self):
        """
        Function to get an Update from the data extracted from the user's request.
        :return:
        """
        revision = Update.revision_from_data(
            startDate=self._startDate, revisionNumber=self._revisionNumberValidRevision, endDate=self._endDate,
            modifications=self._modifications, branchIndex=self._branchIndex)
        return [revision]


class UpdateQueryRequest(UpdateRequest):

    def __init__(self, updateQuery):
        super().__init__(updateQuery.request)
        self._updateQuery = updateQuery

    def _modifications_fom_query(self, revisionStore):
        self._modifications = []
        for update in self._updateQuery.translated_query:
            if update.name == 'InsertData':
                self._evaluate_insert_or_delete_data(update, revisionStore)
            elif update.name == 'DeleteData':
                self._evaluate_insert_or_delete_data(update, revisionStore, deletion=True)
            else:
                pass

        # Determine whether we should test that the modifications can be added or deleted from the revision store.
        if self._shouldBeTested is not None:
            try:
                canBeAdded = revisionStore.can_modifications_be_added_or_deleted(
                    modifications=self._modifications, headRevision=self._precedingTransactionRevision,
                    startDate=self._startDate, endDate=self._endDate)
            except Exception as e:
                raise e
            # If shouldBeTest == 'no' we only run the test, but we do not mind the answer. -> Compute ingestion time
            if not canBeAdded and self._shouldBeTested == 'yes':
                raise Exception('We cannot insert or delete some quads or triples.')

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
            tripleNode = Triple(triple)
            self._modifications.append(Modification(tripleNode, deletion))
            # canBeAdded = self._can_quad_be_added_to_update(tripleNode, revisionStore, deletion)
            # if canBeAdded:
            #     self._modifications.append(Modification(tripleNode, deletion))
            # elif not canBeAdded and not self._shouldBeTested:
            #     self._modifications.append(Modification(tripleNode, deletion))
            # else:
            #     raise Exception('We cannot insert or delete triple {0}.'.format(str(triple)))

        for graph in update.quads:
            for triple in update.quads[graph]:
                quadNode = Quad(triple, graph)
                self._modifications.append(Modification(quadNode, deletion))
                # canBeAdded = self._can_quad_be_added_to_update(quadNode, revisionStore, deletion)
                # if canBeAdded:
                #     self._modifications.append(Modification(quadNode, deletion))
                # elif not canBeAdded and not self._shouldBeTested:
                #     self._modifications.append(Modification(quadNode, deletion))
                # else:
                #     raise Exception('We cannot insert or delete quad {0}.'.format(str(quadNode)))

    def evaluate_request(self, revisionStore):
        self.evaluate_request_to_modify(revisionStore)

        self._modifications_fom_query(revisionStore)


class ModifiedRepeatedUpdateRequest(UpdateRequest):

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"
        # AssertionError

        modifiedRevision = revision.modify(
            otherStartDate=self._startDate, otherEndDate=self._endDate, branchIndex=self._branchIndex,
            otherModifications=self._modifications, revisionNumber=self._revisionNumberValidRevision,
            relatedContent=False, headRevision=self._headRevision.preceding_revision, revisionStore=revisionStore,
            shouldBeTested=self._shouldBeTested)
        return [modifiedRevision]

    def reversions_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"

        revertedRevision = revision.revert(
            revisionStore=revisionStore, revisionNumber=self._revisionNumberValidRevision,
            branchIndex=self._branchIndex, relatedContent=False, headRevision=self._headRevision.preceding_revision)
        return revertedRevision


class ModifiedRelatedUpdateRequest(UpdateRequest):

    def modifications_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"
        # AssertionError

        modifiedRevision = revision.modify(
            otherStartDate=self._startDate, otherEndDate=self._endDate, branchIndex=self._branchIndex,
            otherModifications=self._modifications, revisionNumber=self._revisionNumberValidRevision,
            relatedContent=True, headRevision=self._headRevision.preceding_revision, revisionStore=revisionStore,
            shouldBeTested=self._shouldBeTested)
        return [modifiedRevision]

    def reversions_from_request(self, revision, revisionStore):

        assert isinstance(revision, Update), "Valid Revision should be a Update"

        revertedRevision = revision.revert(
            revisionStore=revisionStore, revisionNumber=self._revisionNumberValidRevision,
            branchIndex=self._branchIndex, relatedContent=True, headRevision=self._headRevision.preceding_revision)
        return revertedRevision

