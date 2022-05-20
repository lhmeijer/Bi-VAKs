from src.main.bitr4qs.namespace import BITR4QS
import re
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from .Parser import Parser, TripleSink
from src.main.bitr4qs.term.Modification import Modification
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD


class UpdateParser(Parser):

    def __init__(self):
        self._modifications = {}

    @staticmethod
    def _graph_name(NQuad, StringOfTriple):
        """

        :param NQuad:
        :param StringOfTriple:
        :return:
        """
        # Todo program differently
        withoutRDFStar = NQuad.replace('<< ' + StringOfTriple + ' >>', '')
        splitWithoutRDFStar = re.findall(r'<(.*?)>', withoutRDFStar)
        if len(splitWithoutRDFStar) > 2:
            graph = URIRef(splitWithoutRDFStar[2])
            return graph
        return None

    @staticmethod
    def parse_inserts_or_deletes(sink, NQuad, deletion=False, forward=True):

        NTriplesParser = W3CNTriplesParser(sink=sink)

        stringOfTriple = re.search(r'<<\s(.*?)\s>>', NQuad)
        if stringOfTriple is None:
            pass

        stringOfTriple = stringOfTriple.group(1)
        NTriplesParser.parsestring(stringOfTriple + " .")
        graph = UpdateParser._graph_name(NQuad, stringOfTriple)

        if not forward:
            deletion = False if deletion else True

        modification = sink.add_modification(graph=graph, deletion=deletion)

        return modification

    @classmethod
    def parse_valid_revision(cls, identifier, NQuads, index, revision=None):
        """

        :param identifier:
        :param NQuads:
        :param index:
        :param revision:
        :return:
        """
        from src.main.bitr4qs.revision.Update import Update
        if revision is None:
            revision = Update(URIRef(identifier))

        RevisionNTriplesParser = W3CNTriplesParser(sink=revision)

        sink = TripleSink()
        NTriplesParser= W3CNTriplesParser(sink=sink)
        for NQuad in NQuads:

            splitQuad = re.findall(r'<(.*?)>', NQuad)
            updateID = splitQuad[0]
            index += 1

            if identifier != updateID:
                return revision, index

            if splitQuad[1] == str(BITR4QS.inserts):
                _ = cls.parse_inserts_or_deletes(sink=revision, NQuad=NQuad)

            elif splitQuad[1] == str(BITR4QS.deletes):
                _ = cls.parse_inserts_or_deletes(sink=revision, NQuad=NQuad, deletion=True)

            elif splitQuad[1] == str(BITR4QS.precedingUpdate):
                NTriplesParser.parsestring(NQuad)
                revision.preceding_identifier = sink.object

            elif splitQuad[1] == str(BITR4QS.startedAt):
                NTriplesParser.parsestring(NQuad)
                revision.start_date = sink.object

            elif splitQuad[1] == str(BITR4QS.endedAt):
                NTriplesParser.parsestring(NQuad)
                revision.end_date = sink.object

            elif splitQuad[1] == str(BITR4QS.hash):
                NTriplesParser.parsestring(NQuad)
                revision.hexadecimal_of_hash = sink.object

            elif splitQuad[1] == str(BITR4QS.branchIndex):
                NTriplesParser.parsestring(NQuad)
                revision.branch_index = sink.object

            elif splitQuad[1] == str(BITR4QS.revisionNumber):
                NTriplesParser.parsestring(NQuad)
                revision.revision_number = sink.object

        return revision, index

    @staticmethod
    def _get_transaction_revision(identifier):
        from src.main.bitr4qs.revision.Update import UpdateRevision
        return UpdateRevision(URIRef(identifier))

    @staticmethod
    def _parse_transaction_revision(revision, p, o):

        if str(p) == str(BITR4QS.update):
            revision.add_valid_revision(o)

    def parse_aggregate(self, stringOfRevisions, forward=True):
        """

        :param stringOfRevisions:
        :param forward:
        :return:
        """
        NQuads = stringOfRevisions.split(' .\n')[:-1]

        sink = TripleSink()
        for NQuad in NQuads:

            splitQuad = re.findall(r'<(.*?)>', NQuad)

            if splitQuad[1] == str(BITR4QS.inserts):
                modification = self.parse_inserts_or_deletes(sink=sink, NQuad=NQuad, forward=forward)
            elif splitQuad[1] == str(BITR4QS.deletes):
                modification = self.parse_inserts_or_deletes(sink=sink, NQuad=NQuad, deletion=True, forward=forward)
            else:
                continue

            hashOfModification = modification.value.__hash__()
            if hashOfModification in self._modifications:
                self._modifications[hashOfModification]['counter'] += -1 if modification.deletion else 1
            else:
                self._modifications[hashOfModification] = {'counter': -1, 'modification': modification.value} \
                    if modification.deletion else {'counter': 1, 'modification': modification.value}

    def get_list_of_modifications(self):
        modificationsInList = []
        for key, value in self._modifications.items():
            if value['counter'] > 0:
                modificationsInList.append(Modification(value['modification']))
            elif value['counter'] < 0:
                modificationsInList.append(Modification(value['modification'], deletion=True))

        return modificationsInList

    def n_quads_to_modifications(self, stringOfNQuads):
        pass

    def modifications_to_n_quads(self):
        n_quads = ''.join(v['modification'].n_quad() if v['counter'] > 0 else "" for _, v in self._modifications.items())
        return n_quads

    def modifications_to_sparql_update_query(self):
        deleteString, insertString = "", ""

        for _, v in self._modifications.items():
            if v['counter'] > 0:
                insertString += v['modification'].to_sparql() + '\n'
            elif v['counter'] < 0:
                deleteString += v['modification'].to_sparql() + '\n'

        lengthDeleteString = len(deleteString)
        lengthInsertString = len(insertString)

        if lengthInsertString == 0 and lengthDeleteString == 0:
            SPARQLQuery = ""
        if len(deleteString) == 0:
            SPARQLQuery = "INSERT DATA {{ {0} }}".format(insertString)
        elif len(insertString) == 0:
            SPARQLQuery = "DELETE DATA {{ {0} }}".format(deleteString)
        else:
            SPARQLQuery = """DELETE DATA {{ {0} }};
            INSERT DATA {{ {1} }}""".format(deleteString, insertString)
        print("SPARQLQuery ", SPARQLQuery)

        return SPARQLQuery


