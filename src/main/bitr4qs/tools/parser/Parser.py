import re
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.term.Quad import Quad
from src.main.bitr4qs.term.Modification import Modification
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from rdflib.term import URIRef
from rdflib.namespace import RDF, RDFS


class TripleSink(object):

    def __init__(self):
        self._subject = None
        self._predicate = None
        self._object = None

        self._modification = None

    def add_modification(self, graph=None, deletion=False):
        if graph is None:
            self._modification = Modification(Triple((self._subject, self._predicate, self._object)), deletion)
        else:
            self._modification = Modification(Quad((self._subject, self._predicate, self._object), graph), deletion)
        return self._modification

    def triple(self, s, p, o):
        self._subject = s
        self._predicate = p
        self._object = o

    @property
    def subject(self):
        return self._subject

    @property
    def predicate(self):
        return self._predicate

    @property
    def object(self):
        return self._object

    @property
    def modification(self):
        return self._modification


class Parser(object):

    @classmethod
    def parse_sorted_combined(cls, stringOfValidRevisions, stringOfTransactionRevisions, forward=True):
        """

        :param stringOfValidRevisions:
        :param stringOfTransactionRevisions:
        :param forward:
        :return:
        """
        transactionRevisions = cls.parse_revisions(stringOfTransactionRevisions, 'transaction')
        validRevisions = cls.parse_revisions(stringOfValidRevisions, 'valid')

        listOfTransactionRevisions = list(transactionRevisions.values())
        listOfTransactionRevisions.sort(key=lambda x: x.revision_number, reverse=not forward)

        orderedValidRevisions = {}
        i = 0

        for transactionRevision in listOfTransactionRevisions:
            validRevisionIDs = transactionRevision.valid_revisions
            if validRevisionIDs:
                for _, validRevisionID in validRevisionIDs:
                    if str(validRevisionID) in validRevisions:
                        validRevision = validRevisions[str(validRevisionID)]
                        orderedValidRevisions[i] = validRevision
                        i += 1
        return orderedValidRevisions

    @classmethod
    def parse_sorted_implicit(cls, stringOfValidRevisions, forward=True):
        """

        :param stringOfValidRevisions:
        :param forward:
        :return:
        """
        validRevisions = cls.parse_revisions(stringOfValidRevisions, 'valid')
        listOfValidRevisions = list(validRevisions.values())
        listOfValidRevisions.sort(key=lambda x: x.revision_number, reverse=not forward)
        orderedValidRevisions = dict(zip(list(range(len(listOfValidRevisions))), listOfValidRevisions))
        return orderedValidRevisions

    @classmethod
    def parse_sorted_explicit(cls, stringOfValidRevisions, stringOfTransactionRevisions, endRevision: URIRef,
                              forward=True):
        """

        :param stringOfValidRevisions:
        :param stringOfTransactionRevisions:
        :param endRevision:
        :param forward:
        :return:
        """
        transactionRevisions = cls.parse_revisions(stringOfTransactionRevisions, 'transaction')
        validRevisions = cls.parse_revisions(stringOfValidRevisions, 'valid')

        orderedValidRevisions = {}
        nOfRevisions = len(validRevisions)
        i = 0
        while i < nOfRevisions:
            if str(endRevision) in transactionRevisions:
                revision = transactionRevisions[str(endRevision)]
                endRevision = revision.preceding_revision
                validRevisionIDs = revision.valid_revisions
                if validRevisionIDs:
                    for _, validRevisionID in validRevisionIDs:
                        if str(validRevisionID) in validRevisions:
                            validRevision = validRevisions[str(validRevisionID)]

                            if forward:
                                j = nOfRevisions - i - 1
                                orderedValidRevisions[j] = validRevision
                            else:
                                orderedValidRevisions[i] = validRevision

                            i += 1
        print("orderedValidRevisions ", orderedValidRevisions)
        return orderedValidRevisions

    @classmethod
    def parse_revisions(cls, stringOfRevisions, revisionName=None):
        """

        :param stringOfRevisions:
        :param revisionName:
        :return:
        """
        revisions = {}

        functionName = 'parse_' + revisionName + '_revision'
        func = getattr(cls, functionName)

        NQuads = stringOfRevisions.split('\n')[:-1]
        index = 0

        while index != len(NQuads):
            revisionID = re.findall(r'<(.*?)>', NQuads[index])[0]

            if revisionID in revisions:
                revision, index = func(revisionID, NQuads[index:], index, revisions[revisionID])
            else:
                revision, index = func(revisionID, NQuads[index:], index)

            revisions[str(revision.identifier)] = revision
        # print("revisions ", revisions)
        return revisions

    @staticmethod
    def _get_valid_revision(identifier):
        from src.main.bitr4qs.revision.ValidRevision import ValidRevision
        return ValidRevision(URIRef(identifier))

    @staticmethod
    def _parse_valid_revision(revision, p, o):
        pass

    @classmethod
    def parse_valid_revision(cls, identifier, NTriples, index, revision=None):
        """

        :param identifier:
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        if revision is None:
            revision = cls._get_valid_revision(identifier)

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            NTriplesParser.parsestring(NTriple)

            if identifier != str(sink.subject):
                return revision, index

            index += 1

            if str(sink.predicate) == str(BITR4QS.hash):
                revision.hexadecimal_of_hash = sink.object

            elif str(sink.predicate) == str(BITR4QS.branchIndex):
                revision.branch_index = sink.object

            elif str(sink.predicate) == str(BITR4QS.revisionNumber):
                revision.revision_number = sink.object

            cls._parse_valid_revision(revision, sink.predicate, sink.object)

        return revision, index

    @staticmethod
    def _get_transaction_revision(identifier):
        from src.main.bitr4qs.revision.TransactionRevision import TransactionRevision
        return TransactionRevision(URIRef(identifier))

    @staticmethod
    def _parse_transaction_revision(revision, p, o):
        predicates = [str(BITR4QS.snapshot), str(BITR4QS.tag), str(BITR4QS.update)]
        if str(p) in predicates:
            revision.add_valid_revision(o)

    @classmethod
    def parse_transaction_revision(cls, identifier, NTriples, index, revision=None):
        """
        Function that parses a general transaction revision
        :param identifier: The identifier of the transaction revision
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        if revision is None:
            revision = cls._get_transaction_revision(identifier)

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            NTriplesParser.parsestring(NTriple)
            if identifier != str(sink.subject):
                return revision, index

            index += 1

            if str(sink.predicate) == str(BITR4QS.hash):
                revision.hexadecimal_of_hash = sink.object

            elif str(sink.predicate) == str(BITR4QS.precedingRevision):
                revision.preceding_revision = sink.object

            elif str(sink.predicate) == str(BITR4QS.revisionNumber):
                revision.revision_number = sink.object

            elif str(sink.predicate) == str(BITR4QS.branch):
                revision.branch = sink.object

            elif str(sink.predicate) == str(BITR4QS.createdAt):
                revision.creation_date = sink.object

            elif str(sink.predicate) == str(BITR4QS.author):
                revision.author = sink.object

            elif str(sink.predicate) == str(RDFS.comment):
                revision.description = sink.object

            cls._parse_transaction_revision(revision, sink.predicate, sink.object)

        return revision, index


class HeadParser(Parser):

    @staticmethod
    def _get_transaction_revision(identifier):
        from src.main.bitr4qs.revision.HeadRevision import HeadRevision
        return HeadRevision(URIRef(identifier))

    @staticmethod
    def _parse_transaction_revision(revision, p, o):
        pass