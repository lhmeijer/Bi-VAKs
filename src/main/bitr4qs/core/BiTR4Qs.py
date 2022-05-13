import src.main.bitr4qs.revision as revisions
from .RevisionStore import RevisionStore
from src.main.bitr4qs.namespace import BITR4QS
from .RevisionStoreExplicit import RevisionStoreExplicit
from .RevisionStoreImplicit import RevisionStoreImplicit


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

    def apply_versioning_operation(self, request, requestType):
        request.evaluate_request(self._revisionStore)

        functionName = '_' + requestType + '_revision'
        func = getattr(self, functionName)

        validRevision, transactionRevision = func(request)

        validRevision.add_to_revision_store(self._revisionStore)
        transactionRevision.add_to_revision_store(self._revisionStore)

    @staticmethod
    def _branch_revision(branchRequest):
        branch = revisions.Branch.revision_from_request(branchRequest)
        branchRevision = revisions.BranchRevision.revision_from_request(branchRequest)
        return branch, branchRevision

    @staticmethod
    def _update_revision(updateRequest):
        update = revisions.Update.revision_from_request(updateRequest)
        updateRevision = revisions.UpdateRevision.revision_from_request(updateRequest)
        return update, updateRevision

    @staticmethod
    def _snapshot_revision(snapshotRequest):
        snapshot = revisions.Snapshot.revision_from_request(snapshotRequest)
        snapshotRevision = revisions.SnapshotRevision.revision_from_request(snapshotRequest)
        return snapshot, snapshotRevision

    @staticmethod
    def _tag_revision(tagRequest):
        tag = revisions.Tag.revision_from_request(tagRequest)
        tagRevision = revisions.TagRevision.revision_from_request(tagRequest)
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
    def _branch_revision(branchRequest):
        branch = revisions.Branch.revision_from_request(branchRequest)
        branchRevision = revisions.BranchRevision.revision_from_request(branchRequest)
        return branch, branchRevision

    @staticmethod
    def _update_revision(updateRequest):
        update = revisions.Update.revision_from_request(updateRequest)
        updateRequest.valid_revision = update.identifier
        updateRevision = revisions.UpdateRevision.revision_from_request(updateRequest)
        return update, updateRevision

    @staticmethod
    def _snapshot_revision(snapshotRequest):
        snapshot = revisions.Snapshot.revision_from_request(snapshotRequest)
        snapshotRequest.valid_revision = snapshot.identifier
        snapshotRevision = revisions.SnapshotRevision.revision_from_request(snapshotRequest)
        return snapshot, snapshotRevision

    @staticmethod
    def _tag_revision(tagRequest):
        tag = revisions.Tag.revision_from_request(tagRequest)
        tagRequest.valid_revision = tag.identifier
        tagRevision = revisions.TagRevision.revision_from_request(tagRequest)
        return tag, tagRevision
