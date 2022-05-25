from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from src.evaluation.queries import Queries
from datetime import datetime, timedelta
from src.evaluation.configuration import BearBConfiguration as config
from src.main.bitr4qs.term.TriplePattern import TriplePattern
from rdflib.term import Variable
from timeit import default_timer as timer


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


def _get_query_numbers(predicate):
    queryNumbers = []
    for queryNumber, query in Queries.items():
        if predicate == query['p']:
            queryNumbers.append(str(queryNumber))
    return queryNumbers


def triples_by_date(inputFolderName, nOfInputFiles, exportFolderName):

    sink = TripleSink()
    NTriplesParser = W3CNTriplesParser(sink=sink)

    days = {}
    totalTriples = 0

    start = timer()
    previousTimestamp = None

    for i in range(nOfInputFiles):

        if i % 1000 == 0:
            print("Processed file number ", i)

        addedFileName = 'data-added_{0}-{1}'.format(str(i+1), str(i+2))
        deletedFileName = 'data-deleted_{0}-{1}'.format(str(i+1), str(i+2))
        index = 0
        timestamp = None
        timestampUnknown = []
        data = []

        # print("timestamp ", timestamp)
        with gzip.open('{0}{1}.nt.gz'.format(inputFolderName, addedFileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)
                # print("line ", line)
                # print("s ", sink.subject)
                # print("p ", sink.predicate)
                # print("o ", sink.object)

                if sink.predicate.n3() == '<http://dbpedia.org/ontology/wikiPageExtracted>':
                    timestamp = str(sink.object)
                    previousTimestamp = str(sink.object)

                queryNumbers = _get_query_numbers(sink.predicate.n3())
                if timestamp is None:
                    timestampUnknown.append(index)

                triple = [str(totalTriples), str(i+1), 'added', str(index), timestamp, '-'.join(queryNumbers), line]
                print("triple ", triple)
                data.append(triple)
                index += 1
                totalTriples += 1

        with gzip.open('{0}{1}.nt.gz'.format(inputFolderName, deletedFileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)
                # print("line ", line)
                # print("s ", sink.subject)
                # print("p ", sink.predicate)
                # print("o ", sink.object)

                queryNumbers = _get_query_numbers(sink.predicate.n3())
                if timestamp is None:
                    timestampUnknown.append(index)

                triple = [str(totalTriples), str(i+1), 'deleted', str(index), timestamp, '-'.join(queryNumbers), line]
                print("triple ", triple)

                data.append(triple)
                index += 1
                totalTriples += 1

        if timestamp is None:
            timestamp = previousTimestamp

        for k in timestampUnknown:
            data[k][4] = timestamp

        timestampDateTime = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S+00:00")
        day = timestampDateTime.strftime("%d-%m-%Y")

        if day not in days:
            days[day] = index
        else:
            days[day] += index

        with open("{0}{1}.txt".format(exportFolderName, day), "a+") as file:
            file.write(''.join(','.join(info) for info in data))

    end = timer()

    print("runtime ", timedelta(seconds=end - start))
    print("days ", days)
    print("number of days ", len(days))
    print("total number of triples ", totalTriples)


if __name__ == "__main__":

    nOfInstances = 21045
    triples_by_date(config.raw_data_dir, nOfInstances, config.processed_data_dir)