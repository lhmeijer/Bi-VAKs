from .Parser import Parser, TripleSink
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from rdflib.term import URIRef


class TagParser(Parser):

    @staticmethod
    def _parse_valid_revision(identifier, NTriples, index, revision=None):
        """

        :param identifier:
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        from src.main.bitr4qs.revision.Tag import Tag
        if revision is None:
            revision = Tag(URIRef(identifier))

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

            elif str(sink.predicate) == str(BITR4QS.tagName):
                revision.tag_name = sink.object

            elif str(sink.predicate) == str(BITR4QS.precedingTag):
                revision.preceding_identifier = sink.object

            elif str(sink.predicate) == str(BITR4QS.branchIndex):
                revision.branch_index = sink.object

            elif str(sink.predicate) == str(BITR4QS.revisionNumber):
                revision.revision_number = sink.object

        return revision, index

    @staticmethod
    def _parse_transaction_revision(identifier, NTriples, index, revision=None):
        """

        :param identifier:
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        from src.main.bitr4qs.revision.TagRevision import TagRevision
        if revision is None:
            revision = TagRevision(URIRef(identifier))

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            NTriplesParser.parsestring(NTriple)

            if identifier != str(sink.subject):
                return revision, index

            index += 1

            if str(sink.predicate) == str(BITR4QS.precedingRevision):
                revision.preceding_identifier = sink.object

            elif str(sink.predicate) == str(BITR4QS.tag):
                revision.valid_revision = sink.object

            elif str(sink.predicate) == str(BITR4QS.branchIndex):
                revision.branch_index = sink.object

            elif str(sink.predicate) == str(BITR4QS.revisionNumber):
                revision.revision_number = sink.object

        return revision, index