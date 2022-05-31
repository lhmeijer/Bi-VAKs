from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from datetime import datetime, timedelta
from src.main.bitr4qs.term.TriplePattern import TriplePattern
from rdflib.term import Variable
from timeit import default_timer as timer
from src.main.bitr4qs.term.Triple import Triple


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

    def compute_changes(self):

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)

        triplesA = set()
        with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, "{:06d}".format(1)), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line.strip())
                triple = Triple((sink.subject, sink.predicate, sink.object))
                triplesA.add(triple.sparql())

        for i in range(1, self._config.NUMBER_OF_VERSIONS):
            fileNameB = "{:06d}".format(i+1)
            triplesB = set()

            with gzip.open('{0}{1}.nt.gz'.format(self._inputFolder, fileNameB), 'rt') as file:
                for line in file:
                    NTriplesParser.parsestring(line.strip())
                    triple = Triple((sink.subject, sink.predicate, sink.object))
                    # print("triple ", triple)
                    triplesB.add(triple.sparql())

            overlapTriples = triplesA.intersection(triplesB)
            print("number of overlapTriples ", len(overlapTriples))
            addedTriples = triplesB - overlapTriples
            print("number of addedTriples ", len(addedTriples))
            deletedTriples = triplesA - overlapTriples
            print("number of deletedTriples ", len(deletedTriples))

            exportFileNameAdded = 'data-added_{0}-{1}.nt.gz'.format(i, i+1)
            with gzip.open('{0}{1}'.format(self._exportFolder, exportFileNameAdded), 'wb') as file:
                file.write('\n'.join(addedTriples).encode('utf-8'))

            exportFileNameDeleted = 'data-deleted_{0}-{1}.nt.gz'.format(i, i+1)
            with gzip.open('{0}{1}'.format(self._exportFolder, exportFileNameDeleted), 'wb') as file:
                file.write('\n'.join(deletedTriples).encode('utf-8'))

            triplesA = triplesB
