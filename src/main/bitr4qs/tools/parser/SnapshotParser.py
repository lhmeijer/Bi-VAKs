from .Parser import Parser
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import URIRef


class SnapshotParser(Parser):

    @staticmethod
    def _get_valid_revision(identifier):
        from src.main.bitr4qs.revision.Tag import Tag
        return Tag(URIRef(identifier))

    @staticmethod
    def _parse_valid_revision(revision, p, o):

        if str(p) == str(BITR4QS.transactedAt):
            revision.transaction_revision = o

        elif str(p) == str(BITR4QS.validAt):
            revision.effective_date = o

        elif str(p) == str(BITR4QS.nameDataset):
            revision.name_dataset = o

        elif str(p) == str(BITR4QS.urlDataset):
            revision.url_dataset = o

        elif str(p) == str(BITR4QS.precedingSnapshot):
            revision.preceding_revision = o

    @staticmethod
    def _get_transaction_revision(identifier):
        from src.main.bitr4qs.revision.SnapshotRevision import SnapshotRevision
        return SnapshotRevision(URIRef(identifier))

    @staticmethod
    def _parse_transaction_revision(revision, p, o):

        if str(p) == str(BITR4QS.snapshot):
            revision.valid_revision = o