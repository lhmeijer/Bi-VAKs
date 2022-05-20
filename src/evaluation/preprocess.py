from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from src.evaluation.queries import Queries
import os
from datetime import datetime


def _get_query_numbers(predicate):
    queryNumbers = []
    for queryNumber, query in Queries.items():
        if predicate == query['p']:
            queryNumbers.append(str(queryNumber))
    return queryNumbers


def preprocess(inputFolderName, nOfInputFiles, exportFolderName):

    sink = TripleSink()
    NTriplesParser = W3CNTriplesParser(sink=sink)

    days = {}
    totalTriples = 0

    for i in range(nOfInputFiles):

        if i % 1000 == 0:
            print("Processed file ", i)

        addedFileName = 'data-added_{0}-{1}'.format(str(i+1), str(i+2))
        deletedFileName = 'data-deleted_{0}-{1}'.format(str(i+1), str(i+2))
        index = 0
        timestamp = None
        noTimestampKnown = []
        data = []

        # print("timestamp ", timestamp)
        with gzip.open('{0}{1}.nt.gz'.format(inputFolderName, addedFileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)

                # print("s ", sink.subject)
                # print("p ", sink.predicate)
                # print("o ", sink.object)

                if sink.predicate.n3() == '<http://dbpedia.org/ontology/wikiPageExtracted>':
                    timestamp = str(sink.object)

                queryNumbers = _get_query_numbers(sink.predicate.n3())
                if timestamp is None:
                    noTimestampKnown.append(index)

                triple = [str(totalTriples), '{0}-{1}'.format(addedFileName, str(index)), timestamp,
                          '-'.join(queryNumbers), line]
                data.append(triple)
                index += 1
                totalTriples += 1

        with gzip.open('{0}{1}.nt.gz'.format(inputFolderName, deletedFileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)

                # print("s ", sink.subject)
                # print("p ", sink.predicate)
                # print("o ", sink.object)

                queryNumbers = _get_query_numbers(sink.predicate.n3())

                triple = [str(totalTriples), '{0}-{1}'.format(deletedFileName, str(index)), timestamp,
                          '-'.join(queryNumbers), line]

                data.append(triple)
                index += 1
                totalTriples += 1

        for k in noTimestampKnown:
            data[k][1] = timestamp

        timestampDateTime = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S+00:00")
        day = timestampDateTime.strftime("%d-%m-%Y")

        if day not in days:
            days[day] = index
        else:
            days[day] += index

        with open("{0}{1}.txt".format(exportFolderName, day), "a+") as file:

            file.write(''.join(','.join(info) for info in data))

    print("days ", days)
    print("number of days ", len(days))
    print("total number of triples ", totalTriples)


if __name__ == "__main__":
    raw_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'alldata.CB.nt', '')
    preprocessed_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'preprocessed', 'by_date', '')
    nOfInstances = '21045'
    preprocess(raw_data_dir, 2000, preprocessed_data_dir)