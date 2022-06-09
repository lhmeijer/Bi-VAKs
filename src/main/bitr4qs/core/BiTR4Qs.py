from .RevisionStore import RevisionStore
from .RevisionStoreExplicit import RevisionStoreExplicit
from .RevisionStoreImplicit import RevisionStoreImplicit
from .RevisionStoreCombined import RevisionStoreCombined
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
            elif config.combined_reference():
                cls.BiTR4QsCore = BiTR4QsCombined(config)
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
            try:
                revision.add_to_revision_store(self._revisionStore)
            except Exception as e:
                raise e

    def _head_revision(self, precedingHeadRevision, revision):
        """

        :param precedingHeadRevision:
        :param revision:
        :return:
        """
        # Check whether there exists a head revision. If yes -> remove entirely from the revision store.
        if precedingHeadRevision:
            try:
                precedingHeadRevision.delete_to_revision_store(self._revisionStore)
            except Exception as e:
                raise e

        # Create new head revision and add it to the revision store.
        try:
            headRevision = HeadRevision.revision_from_data(
                branch=revision.branch, precedingRevision=revision.identifier, revisionNumber=revision.revision_number)
            headRevision.add_to_revision_store(self._revisionStore)
        except Exception as e:
            raise e

    def modify_versioning_operation(self, revisionID, request):
        """

        :param revisionID:
        :param request:
        :return:
        """
        try:
            request.evaluate_request_to_modify(self._revisionStore)
            transactionRevision = request.transaction_revision_from_request()

            revision = self._revisionStore.revision(
                revisionID=revisionID, revisionType=request.type, isValidRevision=True,
                transactionRevisionA=transactionRevision.preceding_revision)

            modifiedRevisions = request.modifications_from_request(revision=revision, revisionStore=self._revisionStore)
        except Exception as e:
            print("exception ", e)
            raise e

        # Attach the created valid revisions to the transaction revisions for explicit reference.
        self._valid_revisions_to_transaction_revision(transactionRevision, modifiedRevisions)
        try:
            # Insert the transaction revision and the valid revisions to the revision store.
            self._to_revision_store([transactionRevision] + modifiedRevisions)

            # Delete the old HEAD revision and add a new HEAD revision.
            self._head_revision(request.head_revision, transactionRevision)
        except Exception as e:
            print("exception ", e)
            raise e

        # Return the modified revisions.
        return [modifiedRevision.__dict__() for modifiedRevision in modifiedRevisions]

    def revert_versioning_operation(self, revisionID, request):
        """

        :param revisionID:
        :param request:
        :return:
        """
        # Evaluate revert request
        request.evaluate_request(self._revisionStore)

        # Create a RevertRevision
        transactionRevision = request.transaction_revision_from_request()

        validRevisions = []
        revisions = self._revisionStore.transaction_from_valid_and_valid_from_transaction(
            revisionID, revisionType=None, transactionFromValid=False)

        for revision in revisions:
            revertedRevisions = request.reversions_from_request(revision, revisionStore=self._revisionStore)
            validRevisions.extend(revertedRevisions)

        validRevision = request.valid_revisions_from_request()
        validRevisions.append(validRevision)

        self._valid_revisions_to_transaction_revision(transactionRevision, validRevisions)
        self._to_revision_store([transactionRevision] + validRevisions)
        self._head_revision(request.head_revision, transactionRevision)

        # Return the valid revisions.
        return [validRevision.__dict__() for validRevision in validRevisions]

    def apply_versioning_operation(self, request):
        """

        :param request:
        :return:
        """
        try:
            # Extract all information needed for the revisions from the request.
            request.evaluate_request(self._revisionStore)

            # Create a transaction revision
            transactionRevision = request.transaction_revision_from_request()

            # Create valid revision(s)
            validRevisions = request.valid_revisions_from_request()
        except AssertionError:
            raise Exception
        except Exception as e:
            print("exception ", e)
            raise e

        # Attach the created valid revisions to the transaction revisions for explicit reference.
        self._valid_revisions_to_transaction_revision(transactionRevision, validRevisions)

        try:
            # Insert the transaction revision and the valid revisions to the revision store.
            self._to_revision_store([transactionRevision] + validRevisions)

            # Delete the old HEAD revision and add a new HEAD revision.
            self._head_revision(request.head_revision, transactionRevision)
        except Exception as e:
            print("exception ", e)
            raise e

        # Return the valid revisions.
        return [validRevision.__dict__() for validRevision in validRevisions]

    def apply_query(self, query):
        try:
            query.evaluate_query(self._revisionStore)
            response = query.apply_query(self._revisionStore)
        except Exception as e:
            print("e ", e)
            raise e

        return response

    def get_number_of_quads_in_revision_store(self):
        try:
            numberOfQuads = self._revisionStore.number_of_quads_in_revision_store()
        except Exception as e:
            raise e
        return numberOfQuads

    def reset_revision_store(self):
        try:
            snapshots = self._revisionStore.all_revisions('snapshot', isValidRevision=True)
            if len(snapshots) > 0:
                for _, snapshot in snapshots.items():
                    snapshot.delete_dataset()
            self._revisionStore.revision_store.empty_dataset()
            numberOfQuads = self.get_number_of_quads_in_revision_store()
        except Exception as e:
            raise Exception

        if numberOfQuads > 0:
            raise Exception("Revision Store should be empty.")

    def upload_revision_store(self, data, returnFormat):
        try:
            self._revisionStore.revision_store.upload_to_dataset(data, returnFormat=returnFormat, encoded=True)
            snapshots = self._revisionStore.all_revisions('snapshot', isValidRevision=True)
            if len(snapshots) > 0:
                for _, snapshot in snapshots.items():
                    snapshot.create_dataset(self._revisionStore)
        except Exception as e:
            print("e ", e)
            raise e

    def get_revision_store(self, returnFormat):
        return self._revisionStore.revision_store.get_dataset(returnFormat, decoded=True)


class BiTR4QsImplicit(BiTR4Qs):

    def revision_store(self):
        return RevisionStoreImplicit(self._config)

    @staticmethod
    def _valid_revisions_to_transaction_revision(transactionRevision, validRevisions):
        for validRevision in validRevisions:
            if 'Branch' in str(validRevision.identifier):
                transactionRevision.add_valid_revision(validRevision.identifier)


class BiTR4QsExplicit(BiTR4Qs):

    def revision_store(self):
        return RevisionStoreExplicit(self._config)

    @staticmethod
    def _valid_revisions_to_transaction_revision(transactionRevision, validRevisions):
        for validRevision in validRevisions:
            transactionRevision.add_valid_revision(validRevision.identifier)


class BiTR4QsCombined(BiTR4Qs):

    def revision_store(self):
        return RevisionStoreCombined(self._config)

    @staticmethod
    def _valid_revisions_to_transaction_revision(transactionRevision, validRevisions):
        for validRevision in validRevisions:
            transactionRevision.add_valid_revision(validRevision.identifier)

