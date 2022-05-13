from .Parser import Parser, TripleSink
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from rdflib.term import URIRef


class SnapshotParser(Parser):

    @staticmethod
    def _parse_valid_revision(identifier, NTriples, index, revision=None):
        """

        :param identifier:
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        from src.main.bitr4qs.revision.Snapshot import Snapshot
        if revision is None:
            revision = Snapshot(URIRef(identifier))

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            NTriplesParser.parsestring(NTriple)

            if identifier != str(sink.subject):
                return revision, index

            index += 1

            if str(sink.predicate) == str(BITR4QS.transactedAt):
                revision.transaction_revision = sink.object

            elif str(sink.predicate) == str(BITR4QS.validAt):
                revision.effective_date = sink.object

            elif str(sink.predicate) == str(BITR4QS.nameDataset):
                revision.name_dataset = sink.object

            elif str(sink.predicate) == str(BITR4QS.urlDataset):
                revision.url_dataset = sink.object

            elif str(sink.predicate) == str(BITR4QS.precedingSnapshot):
                revision.preceding_identifier = sink.object

            elif str(sink.predicate) == str(BITR4QS.branchIndex):
                revision.branch_index = sink.object

            elif str(sink.predicate) == str(BITR4QS.revisionNumber):
                revision.revision_number = sink.object

            elif str(sink.predicate) == str(BITR4QS.hash):
                revision.hexadecimal_of_hash = sink.object

        return revision, index