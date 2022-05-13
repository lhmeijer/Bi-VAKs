from .Parser import Parser, TripleSink
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser


class BranchParser(Parser):

    @staticmethod
    def _parse(identifier, NTriples, index, revision=None):
        """

        :param identifier:
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        from src.main.bitr4qs.revision.Branch import Branch
        if revision is None:
            revision = Branch(identifier)

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            NTriplesParser.parsestring(NTriple)
            index += 1

            if identifier != sink.subject:
                return revision, index

            if sink.predicate == str(BITR4QS.inserts):
                revision.preceding_identifier(sink.object)

            elif sink.predicate == str(BITR4QS.branchedOffAt):
                revision.branched_off_at(sink.object)

            elif sink.predicate == str(BITR4QS.precedingRevision):
                revision.preceding_identifier(sink.object)

            elif sink.predicate == str(BITR4QS.branchIndex):
                revision.branch_index(sink.object)

            elif sink.predicate == str(BITR4QS.revisionNumber):
                revision.revision_number(sink.object)

        return revision, index