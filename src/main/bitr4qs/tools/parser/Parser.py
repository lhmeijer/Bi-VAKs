import re
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.term.Quad import Quad
from src.main.bitr4qs.term.Modification import Modification
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from rdflib.term import URIRef


class TripleSink(object):

    def __init__(self):
        self._subject = None
        self._predicate = None
        self._object = None

    def add_modification(self, graph=None, deletion=False):
        if graph is None:
            return Modification(Triple((self._subject, self._predicate, self._object)), deletion)
        else:
            return Modification(Quad((self._subject, self._predicate, self._object), graph), deletion)

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


class Parser(object):

    @classmethod
    def parse_sorted_implicit(cls, stringOfValidRevisions, forwards=True):
        """

        :param stringOfValidRevisions:
        :param forwards:
        :return:
        """
        validRevisions = cls.parse_revisions(stringOfValidRevisions, 'valid')
        listOfValidRevisions = list(validRevisions.values())
        reverse = False if forwards else True
        listOfValidRevisions.sort(key=lambda x: x.revision_number, reverse=reverse)
        orderedValidRevisions = dict(zip(list(range(len(listOfValidRevisions))), listOfValidRevisions))
        return orderedValidRevisions

    @classmethod
    def parse_sorted_explicit(cls, stringOfValidRevisions, stringOfTransactionRevisions, endRevision: URIRef,
                              forwards=True):
        """

        :param stringOfValidRevisions:
        :param stringOfTransactionRevisions:
        :param endRevision:
        :param forwards:
        :return:
        """
        transactionRevisions = cls.parse_revisions(stringOfTransactionRevisions, 'transaction')
        validRevisions = cls.parse_revisions(stringOfValidRevisions, 'valid')

        orderedValidRevisions = {}
        nOfRevisions = len(validRevisions)
        i = 0
        while i == nOfRevisions:
            if str(endRevision) in transactionRevisions:
                revision = transactionRevisions[str(endRevision)]
                endRevision = revision.preceding_revision

                if revision.valid_revision is not None:
                    validRevision = validRevisions[str(revision.valid_revision)]

                    if forwards:
                        j = nOfRevisions - i
                        orderedValidRevisions[j] = validRevision
                    else:
                        orderedValidRevisions[i] = validRevision

                    i += 1
        return orderedValidRevisions

    @classmethod
    def parse_revisions(cls, stringOfRevisions, revisionName):
        """

        :param stringOfRevisions:
        :param revisionName:
        :return:
        """
        revisions = {}

        functionName = '_' + "parse_" + revisionName + '_revision'
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

        return revisions

    @staticmethod
    def _parse_valid_revision(identifier, NTriples, index, revision=None):
        return revision, index

    @staticmethod
    def _parse_transaction_revision(identifier, NTriples, index, revision=None):
        return revision, index