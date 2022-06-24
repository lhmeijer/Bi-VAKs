from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from datetime import datetime, timedelta
from src.main.bitr4qs.term.TriplePattern import TriplePattern
from rdflib.term import Variable
from timeit import default_timer as timer
from src.main.bitr4qs.term.Triple import Triple
from src.evaluation.configuration import BearBConfiguration
from rdflib.exceptions import ParserError
import re


def get_queries_from_nt_file(queryFileName):

    sink = TripleSink()
    NTriplesParser = W3CNTriplesParser(sink=sink)
    nOfQuery = 1
    queries = {}
    replacement = '<http://dbpedia.org/ontology/Replacement>'

    with open(queryFileName, "r") as file:
        for line in file:
            triple = [None, None, None]

            if '?s' in line:
                line = line.replace('?s', replacement)
                triple[0] = Variable('?s')

            if '?p' in line:
                line = line.replace('?p', replacement)
                triple[1] = Variable('?p')

            if '?o' in line:
                line = line.replace('?o', replacement)
                triple[2] = Variable('?o')

            NTriplesParser.parsestring(line)
            parsedTriple = [sink.subject, sink.predicate, sink.object]

            triple = [parsedTriple[i] if triple[i] is None else triple[i] for i in range(len(triple))]

            triplePattern = TriplePattern(tuple(triple))
            queries[nOfQuery] = triplePattern

            nOfQuery += 1

    return queries


class ChangeComputer(object):

    def __init__(self, config, inputFolder, exportFolder):
        self._config = config
        self._inputFolder = inputFolder
        self._exportFolder = exportFolder

        self._queries = get_queries_from_nt_file(self._config.bear_queries_file_name)

    def _parse_line(self, line, NTriplesParser, sink):
        try:
            NTriplesParser.parsestring(line)
            triple = Triple((sink.subject, sink.predicate, sink.object))
            return triple
        except ParserError:
            return None
            # match = re.search(r'<node(.*)>', line, re.IGNORECASE)
            # if match:
            #     found = match.group(1)
            #     line = line.replace('node{0}'.format(found), 'http://example.org/bnode/{0}'.format(found))
            #     return self._parse_line(line, NTriplesParser, sink)

    def compute_changes(self):

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        result = {}
        for j in range(1, len(self._queries) + 1):
            result[j] = []

        triplesA = []
        print('{0}{1}.nt.gz'.format(self._inputFolder, "{:06d}".format(1)))
        index = 0
        with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, "{:06d}".format(1)), 'rt') as file:
            for line in file:

                triple = self._parse_line(line.strip(), NTriplesParser, sink)
                if triple:
                    triplesA.append(triple.sparql())
                    queryNumber = self._contains_query(triple)
                    if queryNumber is not None:
                        result[queryNumber].append(triple)

                    index += 1

                    if index % 1000000 == 0:
                        print("We have processed {0} triples.".format(index))

                    if index % 10000000 == 0:
                        break

        for number, setOfTriples in result.items():
            with open('{0}start.nt'.format(self._inputFolder), 'a') as file:
                file.write(''.join([triple.n_quad() for triple in setOfTriples]))

            with open('{0}-{1}.txt'.format(self._config.bear_results_dir, number), 'a') as file:
                for triple in setOfTriples:
                    tripleResult = triple.result_based_on_query_type(self._config.QUERY_TYPE)
                    file.write('[Solution in {0}]{1}\n'.format(0, tripleResult))

        print("saved results for the first version.")

        setOfTriplesA = set(triplesA)

        for i in range(1, self._config.NUMBER_OF_VERSIONS):
            fileNameB = "{:06d}".format(i+1)
            triplesB = []

            result = {}
            for j in range(1, len(self._queries) + 1):
                result[j] = []

            index = 0
            with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, fileNameB), 'rt') as file:
                for line in file:

                    triple = self._parse_line(line.strip(), NTriplesParser, sink)
                    if triple:
                        triplesB.append(triple.sparql())
                        queryNumber = self._contains_query(triple)
                        if queryNumber is not None:
                            result[queryNumber].append(triple)

                        index += 1
                        if index % 1000000 == 0:
                            print("We have processed {0} triples.".format(index))

                        if index % 10000000 == 0:
                            break

            setOfTriplesB = set(triplesB)

            overlapTriples = setOfTriplesA.intersection(setOfTriplesB)
            print("number of overlapTriples ", len(overlapTriples))
            addedTriples = setOfTriplesB - overlapTriples
            print("number of addedTriples ", len(addedTriples))
            deletedTriples = setOfTriplesA - overlapTriples
            print("number of deletedTriples ", len(deletedTriples))

            exportFileNameAdded = 'data-added_{0}-{1}.nt.gz'.format(i, i+1)
            with gzip.open('{0}{1}'.format(self._exportFolder, exportFileNameAdded), 'wb') as file:
                file.write('\n'.join(addedTriples).encode('utf-8'))

            exportFileNameDeleted = 'data-deleted_{0}-{1}.nt.gz'.format(i, i+1)
            with gzip.open('{0}{1}'.format(self._exportFolder, exportFileNameDeleted), 'wb') as file:
                file.write('\n'.join(deletedTriples).encode('utf-8'))

            setOfTriplesA = setOfTriplesB

            for number, setOfTriples in result.items():
                with open('{0}-{1}.txt'.format(self._config.bear_results_dir, number), 'a') as file:
                    for triple in setOfTriples:
                        tripleResult = triple.result_based_on_query_type(self._config.QUERY_TYPE)
                        file.write('[Solution in {0}]{1}\n'.format(i, tripleResult))

    def _contains_query(self, triple):
        """

        :param triple:
        :return:
        """
        for queryNumber, query in self._queries.items():
            if query.matches(triple):
                return queryNumber
        return None

    def compute_change_results(self):

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        jumps = list(range(0, self._config.NUMBER_OF_VERSIONS, 5)) + [self._config.NUMBER_OF_VERSIONS]

        triplesA = {}
        for j in range(1, len(self._queries) + 1):
            triplesA[j] = set()

        with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, "{:06d}".format(1)), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line.strip())
                triple = Triple((sink.subject, sink.predicate, sink.object))
                queryNumber = self._contains_query(triple)
                if queryNumber is not None:
                    triplesA[queryNumber].add(triple.sparql())

        for i in jumps[1:]:
            fileNameB = "{:06d}".format(i)
            print("fileNameB ", fileNameB)

            result = {}
            for j in range(1, len(self._queries) + 1):
                result[j] = set()

            with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, fileNameB), 'rt') as file:
                for line in file:
                    NTriplesParser.parsestring(line.strip())
                    triple = Triple((sink.subject, sink.predicate, sink.object))
                    queryNumber = self._contains_query(triple)
                    if queryNumber is not None:
                        result[queryNumber].add(triple.sparql())

            for number, setOfTriples in result.items():
                print("queryNumber ", number)
                overlapTriples = triplesA[number].intersection(setOfTriples)
                print("number of overlapTriples ", len(overlapTriples))
                addedTriples = setOfTriples - overlapTriples
                print("number of addedTriples ", len(addedTriples))
                deletedTriples = triplesA[number] - overlapTriples
                print("number of deletedTriples ", len(deletedTriples))

                with open('{0}-{1}.txt'.format(self._config.bear_results_dir, number), 'a') as file:
                    for addition in addedTriples:
                        NTriplesParser.parsestring(addition)
                        triple = Triple((sink.subject, sink.predicate, sink.object))
                        tripleResult = triple.result_based_on_query_type(self._config.QUERY_TYPE)
                        file.write('[ADD in jump {0}]{1}\n'.format(i, tripleResult))

                    for deletion in deletedTriples:
                        NTriplesParser.parsestring(deletion)
                        triple = Triple((sink.subject, sink.predicate, sink.object))
                        tripleResult = triple.result_based_on_query_type(self._config.QUERY_TYPE)
                        file.write('[DEL in jump {0}]{1}\n'.format(i, tripleResult))

    def compute_version_results(self):

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        for i in range(0, self._config.NUMBER_OF_VERSIONS):
            fileNameB = "{:06d}".format(i+1)
            print("fileNameB ", fileNameB)

            result = {}
            for j in range(1, len(self._queries) + 1):
                result[j] = []

            with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, fileNameB), 'rt') as file:
                for line in file:
                    NTriplesParser.parsestring(line.strip())
                    triple = Triple((sink.subject, sink.predicate, sink.object))
                    # print("triple ", triple)
                    queryNumber = self._contains_query(triple)
                    if queryNumber is not None:
                        result[queryNumber].append(triple)

            for number, setOfTriples in result.items():
                print("queryNumber ", number)

                with open('{0}-{1}.txt'.format(self._config.bear_results_dir, number), 'a') as file:
                    for triple in setOfTriples:
                        tripleResult = triple.result_based_on_query_type(self._config.QUERY_TYPE)
                        file.write('[Solution in {0}]{1}\n'.format(i, tripleResult))


if __name__ == "__main__":
    config = BearBConfiguration()
    changeComputer = ChangeComputer(config, inputFolder=config.raw_version_data_dir,
                                    exportFolder=config.raw_change_data_dir)
    changeComputer.compute_version_results()