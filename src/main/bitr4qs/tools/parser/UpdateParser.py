from src.main.bitr4qs.namespace import BITR4QS
import re
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from .Parser import Parser, TripleSink
from src.main.bitr4qs.term.Modification import Modification
from rdflib.term import URIRef
from rdflib.namespace import RDFS
from src.main.bitr4qs.tools.parser.UpdateNQuadParser import UpdateNQuadParser


class UpdateParser(Parser):

    def __init__(self):
        self._modifications = {}
        self._numberOfProcessedQuads = 0

    def reset_modifications(self):
        self._modifications = {}

    @property
    def number_of_processed_quads(self):
        return self._numberOfProcessedQuads

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

    def n_quads_to_modifications(self, NQuads, deletion=False):
        sink = TripleSink()
        nQuadParser = UpdateNQuadParser(sink)

        for NQuad in NQuads:
            nQuadParser.parsestring(NQuad)
            self._numberOfProcessedQuads += 1

            hashOfModification = sink.modification.value.__hash__()
            if hashOfModification in self._modifications:
                self._modifications[hashOfModification]['counter'] += -1 if deletion else 1
            else:
                self._modifications[hashOfModification] = {'counter': -1, 'modification': sink.modification.value} \
                    if deletion else {'counter': 1, 'modification': sink.modification.value}

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
            deletion = not deletion

        modification = sink.add_modification(graph=graph, deletion=deletion)

        return modification

    def parse_revisions(self, stringOfRevisions, revisionName=None):
        """

        :param stringOfRevisions:
        :param revisionName:
        :return:
        """
        validRevisions = {}
        transactionRevisions = {}

        NQuads = stringOfRevisions.split('\n')[:-1]
        index = 0

        while index != len(NQuads):
            revisionID = re.findall(r'<(.*?)>', NQuads[index])[0]

            if revisionName is None:
                if 'Revision' in revisionID:
                    finalRevisionName = 'transaction'
                else:
                    finalRevisionName = 'valid'
            else:
                finalRevisionName = revisionName

            if finalRevisionName == 'valid':
                func = getattr(self, 'parse_valid_revision')
                if revisionID in validRevisions:
                    revision, index = func(revisionID, NQuads[index:], index, validRevisions[revisionID])
                else:
                    revision, index = func(revisionID, NQuads[index:], index)

                validRevisions[str(revision.identifier)] = revision
            elif finalRevisionName == 'transaction':
                func = getattr(self, 'parse_transaction_revision')
                if revisionID in transactionRevisions:
                    revision, index = func(revisionID, NQuads[index:], index, transactionRevisions[revisionID])
                else:
                    revision, index = func(revisionID, NQuads[index:], index)

                transactionRevisions[str(revision.identifier)] = revision
        # print("revisions ", revisions)
        return validRevisions, transactionRevisions

    def parse_valid_revision(self, identifier, NQuads, index, revision=None):
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

        sink = TripleSink()
        NTriplesParser= W3CNTriplesParser(sink=sink)
        for NQuad in NQuads:

            splitQuad = re.findall(r'<(.*?)>', NQuad)
            updateID = splitQuad[0]

            if identifier != updateID:
                return revision, index

            index += 1
            self._numberOfProcessedQuads += 1

            if splitQuad[1] == str(BITR4QS.inserts):
                _ = self.parse_inserts_or_deletes(sink=revision, NQuad=NQuad)

            elif splitQuad[1] == str(BITR4QS.deletes):
                _ = self.parse_inserts_or_deletes(sink=revision, NQuad=NQuad, deletion=True)

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

    def parse_transaction_revision(self, identifier, NTriples, index, revision=None):
        """
        Function that parses a general transaction revision
        :param identifier: The identifier of the transaction revision
        :param NTriples:
        :param index:
        :param revision:
        :return:
        """
        if revision is None:
            from src.main.bitr4qs.revision.Update import UpdateRevision
            revision = UpdateRevision(URIRef(identifier))

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for NTriple in NTriples:

            splitQuad = re.findall(r'<(.*?)>', NTriple)
            updateRevisionID = splitQuad[0]

            if identifier != updateRevisionID:
                return revision, index

            NTriplesParser.parsestring(NTriple)
            index += 1
            self._numberOfProcessedQuads += 1

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

            if str(sink.predicate) == str(BITR4QS.update):
                revision.add_valid_revision(sink.object)

        return revision, index

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
            self._numberOfProcessedQuads += 1

            if splitQuad[1] == str(BITR4QS.inserts):
                modification = self.parse_inserts_or_deletes(sink=sink, NQuad=NQuad, forward=forward)

            elif splitQuad[1] == str(BITR4QS.deletes):
                modification = self.parse_inserts_or_deletes(sink=sink, NQuad=NQuad, deletion=True, forward=forward)
            else:
                continue

            self._add_modification_to_modifications(modification)

    def _add_modification_to_modifications(self, modification):
        """

        :param modification:
        :return:
        """
        hashOfModification = modification.value.__hash__()
        if hashOfModification in self._modifications:
            self._modifications[hashOfModification]['counter'] += -1 if modification.deletion else 1
        else:
            self._modifications[hashOfModification] = {'counter': -1, 'modification': modification.value} \
                if modification.deletion else {'counter': 1, 'modification': modification.value}

    def parse_sorted_combined(self, stringOfRevisions, StringOfTransactionRevisions=None, forward=True):
        """

        :param stringOfRevisions:
        :param forward:
        :return:
        """
        updates, updatesRevisions = self.parse_revisions(stringOfRevisions)
        listOfUpdateRevisions = list(updatesRevisions.values())
        listOfUpdateRevisions.sort(key=lambda x: x.revision_number, reverse=not forward)

        for updateRevision in listOfUpdateRevisions:
            updateIDs = updateRevision.valid_revisions
            if updateIDs:
                for _, updateID in updateIDs:
                    if str(updateID) in updates:
                        update = updates[str(updateID)]
                        for modification in update.modifications:
                            if not forward:
                                modification.invert()
                            self._add_modification_to_modifications(modification)

    def parse_sorted_implicit(self, stringOfValidRevisions, forward=True):
        """

        :param stringOfValidRevisions:
        :param forward:
        :return:
        """
        updates, _ = self.parse_revisions(stringOfValidRevisions, 'valid')
        listOfUpdates = list(updates.values())
        listOfUpdates.sort(key=lambda x: x.revision_number, reverse=not forward)

        for update in listOfUpdates:
            for modification in update.modifications:
                if not forward:
                    modification.invert()
                self._add_modification_to_modifications(modification)

    def parse_sorted_explicit(self, stringOfValidRevisions, stringOfTransactionRevisions, endRevision: URIRef,
                              forward=True):
        """

        :param stringOfValidRevisions:
        :param stringOfTransactionRevisions:
        :param endRevision:
        :param forward:
        :return:
        """
        _, updatesRevisions = self.parse_revisions(stringOfTransactionRevisions, 'transaction')
        updates, _ = self.parse_revisions(stringOfValidRevisions, 'valid')

        orderedUpdates = {}
        nOfRevisions = len(updates)
        i = 0
        while i < nOfRevisions:
            if str(endRevision) in updatesRevisions:
                updateRevision = updatesRevisions[str(endRevision)]
                endRevision = updateRevision.preceding_revision
                updateIDs = updateRevision.valid_revisions
                if updateIDs:
                    for _, updateID in updateIDs:
                        if str(updateID) in updates:
                            update = updates[str(updateID)]

                            if forward:
                                j = nOfRevisions - i - 1
                                orderedUpdates[j] = update
                            else:
                                orderedUpdates[i] = update

                            i += 1
        for i in range(len(orderedUpdates)):
            # print("orderedUpdates[i] ", orderedUpdates[i].identifier)
            for modification in orderedUpdates[i].modifications:
                if not forward:
                    modification.invert()
                self._add_modification_to_modifications(modification)

    def get_list_of_modifications(self):
        modificationsInList = []
        for key, value in self._modifications.items():
            if value['counter'] > 0:
                modificationsInList.append(Modification(value['modification']))
            elif value['counter'] < 0:
                modificationsInList.append(Modification(value['modification'], deletion=True))

        return modificationsInList

    def modifications_to_n_quads(self):
        n_quads = ''.join(v['modification'].n_quad() if v['counter'] > 0 else "" for _, v in self._modifications.items())
        return n_quads

    def modifications_to_sparql_update_query(self):
        deleteString, insertString = "", ""

        for _, v in self._modifications.items():
            if v['counter'] > 0:
                insertString += v['modification'].sparql() + '\n'
            elif v['counter'] < 0:
                deleteString += v['modification'].sparql() + '\n'

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
        # print("SPARQLQuery ", SPARQLQuery)

        return SPARQLQuery


