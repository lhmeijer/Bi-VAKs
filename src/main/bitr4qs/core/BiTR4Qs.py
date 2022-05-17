import src.main.bitr4qs.revision as revisions
from .RevisionStore import RevisionStore
from src.main.bitr4qs.namespace import BITR4QS
from .RevisionStoreExplicit import RevisionStoreExplicit
from .RevisionStoreImplicit import RevisionStoreImplicit
from rdflib.term import Literal


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

    def apply_versioning_operation(self, request):
        """
        Function that applies a versioning operations based on a request such as update, snapshot, tag, and branch
        :param request: BranchRequest, UpdateRequest, TagRequest, or SnapshotRequest
        :return:
        """
        request.evaluate_request(self._revisionStore)

        functionName = '_' + request.type + '_revision'
        func = getattr(self, functionName)

        # Extract transaction revision from the given request.
        transactionRevision = func(request)
        self._valid_revisions_to_transaction_revision(transactionRevision, request.valid_revisions)

        # Add the valid and transaction revisions to the revision store.
        if request.valid_revisions is not None:
            for validRevision in request.valid_revisions:
                validRevision.add_to_revision_store(self._revisionStore)

        if transactionRevision is not None:
            transactionRevision.add_to_revision_store(self._revisionStore)

        # Check whether there is a head revision. If yes -> remove entirely from the revision store.
        if request.head_revision is not None:
            request.head_revision.delete_to_revision_store(self._revisionStore)

        # Create new head revision and add it to the revision store.
        headRevision = revisions.HeadRevision.revision_from_data(branch=transactionRevision.branch,
                                                                 precedingRevision=transactionRevision.identifier,
                                                                 revisionNumber=transactionRevision.revision_number)
        headRevision.add_to_revision_store(self._revisionStore)

    @staticmethod
    def _initial_revision(initialRequest):
        initialRevision = revisions.BranchRevision.revision_from_request(initialRequest)

        # Check whether the user already uses an existing dataset, and create a snapshot and update from it.
        if initialRequest.name_dataset is not None and initialRequest.url_dataset is not None:
            snapshot = revisions.Snapshot.revision_from_data(
                revisionNumber=initialRequest.revision_number, branchIndex=initialRequest.branch_index,
                nameDataset=initialRequest.name_dataset, urlDataset=initialRequest.url_dataset,
                effectiveDate=initialRequest.effective_date, transactionRevision=initialRevision.identifier)
            update = snapshot.update_from_snapshot()

            initialRequest.add_valid_revision(snapshot)
            initialRequest.add_valid_revision(update)
        return None, initialRevision

    @staticmethod
    def _branch_revision(branchRequest):
        branchRevision = revisions.BranchRevision.revision_from_request(branchRequest)
        branch = revisions.Branch.revision_from_request(branchRequest)
        branchRequest.add_valid_revision(branch)
        return branch, branchRevision

    @staticmethod
    def _update_revision(updateRequest):
        updateRevision = revisions.UpdateRevision.revision_from_request(updateRequest)
        update = revisions.Update.revision_from_request(updateRequest)
        updateRequest.add_valid_revision(update)
        return update, updateRevision

    @staticmethod
    def _snapshot_revision(snapshotRequest):
        snapshotRevision = revisions.SnapshotRevision.revision_from_request(snapshotRequest)

        if snapshotRequest.transaction_revision is None:
            snapshotRequest.transaction_revision = snapshotRevision.identifier

        snapshot = revisions.Snapshot.revision_from_request(snapshotRequest)
        snapshotRequest.add_valid_revision(snapshot)
        return snapshot, snapshotRevision

    @staticmethod
    def _tag_revision(tagRequest):
        tagRevision = revisions.TagRevision.revision_from_request(tagRequest)

        if tagRequest.transaction_revision is None:
            tagRequest.transaction_revision = tagRevision.identifier

        tag = revisions.Tag.revision_from_request(tagRequest)
        tagRequest.add_valid_revision(tag)
        return tag, tagRevision

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
        transactionRevision.valid_revisions([validRevision.identifier for validRevision in validRevisions])

