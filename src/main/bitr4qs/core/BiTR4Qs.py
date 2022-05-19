from .RevisionStore import RevisionStore
from src.main.bitr4qs.namespace import BITR4QS
from .RevisionStoreExplicit import RevisionStoreExplicit
from .RevisionStoreImplicit import RevisionStoreImplicit
from rdflib.term import Literal
from src.main.bitr4qs.revision.HeadRevision import HeadRevision


class BiTR4QsSingleton(object):

    BiTR4QsCore = None

    @classmethod
    def get(cls, config):
        if cls.BiTR4QsCore is not None:
            return cls.BiTR4QsCore
        else:
            if config.implicit_reference():
                cls.BiTR4QsCore = BiTR4QsImplicit(config)
            elif config.explicit_reference():
                cls.BiTR4QsCore = BiTR4QsExplicit(config)
            else:
                cls.BiTR4QsCore = BiTR4Qs(config)
            return cls.BiTR4QsCore


class BiTR4Qs(object):

    def __init__(self, config):
        self._config = config
        self._revisionStore = self.revision_store()

    def revision_store(self):
        return RevisionStore(self._config)

    @staticmethod
    def _valid_revisions_to_transaction_revision(transactionRevision, validRevisions):
        pass

    def _to_revision_store(self, revisions):
        """

        :param revisions:
        :return:
        """
        for revision in revisions:
            revision.add_to_revision_store(self._revisionStore)

    def _head_revision(self, precedingHeadRevision, transactionRevision):
        """

        :param precedingHeadRevision:
        :param transactionRevision:
        :return:
        """
        # Check whether there is a head revision. If yes -> remove entirely from the revision store.
        if precedingHeadRevision is not None:
            precedingHeadRevision.delete_to_revision_store(self._revisionStore)

        # Create new head revision and add it to the revision store.
        headRevision = HeadRevision.revision_from_data(branch=transactionRevision.branch,
                                                       precedingRevision=transactionRevision.identifier,
                                                       revisionNumber=transactionRevision.revision_number)
        headRevision.add_to_revision_store(self._revisionStore)

    def modify_versioning_operation(self, revisionID, request):
        """

        :param revisionID:
        :param request:
        :return:
        """
        request.evaluate_request(self._revisionStore)

        transactionRevision = request.transaction_revision_from_request()

        revision = self._revisionStore.revision(revisionID, revisionType=request.type, validRevision=True)
        modifiedRevisions = request.modifications_from_request(revision, self._revisionStore)

        self._valid_revisions_to_transaction_revision(transactionRevision, modifiedRevisions)
        self._to_revision_store(modifiedRevisions.append(transactionRevision))
        self._head_revision(request.head_revision, transactionRevision)

    def revert_versioning_operation(self, revisionID, request):
        """

        :param revisionID:
        :param request:
        :return:
        """
        # Evaluate request
        request.evaluate_request(self._revisionStore)

        # Create a RevertRevision
        transactionRevision = request.transaction_revision_from_request()

        validRevisions = []
        revisions = self._revisionStore.valid_revisions_from_transaction_revision(revisionID, revisionType=None)
        for revision in revisions:
            revertedRevisions = request.reversions_from_request(revision, revisionStore=self._revisionStore)
            validRevisions.extend(revertedRevisions)

        validRevision = request.valid_revisions_from_request()
        validRevisions.append(validRevision)

        self._valid_revisions_to_transaction_revision(transactionRevision, validRevisions)
        self._to_revision_store(validRevisions + transactionRevision)
        self._head_revision(request.head_revision, transactionRevision)

    def apply_versioning_operation(self, request):
        """

        :param request:
        :return:
        """
        try:
            request.evaluate_request(self._revisionStore)
        except Exception as e:
            print("e ", e)
            raise e

        try:
            transactionRevision = request.transaction_revision_from_request()
            validRevisions = request.valid_revisions_from_request()
        except AssertionError:
            raise Exception

        self._valid_revisions_to_transaction_revision(transactionRevision, validRevisions)
        self._to_revision_store([transactionRevision] + validRevisions)
        self._head_revision(request.head_revision, transactionRevision)

    def apply_query(self, query):
        query.evaluate_query(self._revisionStore)
        print("Query evaluated")
        # if node is not None:
        #     return node
        return query.apply_query(self._revisionStore)


class BiTR4QsImplicit(BiTR4Qs):

    def revision_store(self):
        return RevisionStoreImplicit(self._config)


class BiTR4QsExplicit(BiTR4Qs):

    def revision_store(self):
        return RevisionStoreExplicit(self._config)

    @staticmethod
    def _valid_revisions_to_transaction_revision(transactionRevision, validRevisions):
        for validRevision in validRevisions:
            transactionRevision.add_valid_revision(validRevision.identifier)

