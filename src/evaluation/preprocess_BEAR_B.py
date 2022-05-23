from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from src.evaluation.queries import Queries
import os
from datetime import datetime, timedelta
from src.evaluation.configuration_BEAR_B import BearBConfiguration as config
import numpy as np


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

                # print("s ", sink.subject)
                # print("p ", sink.predicate)
                # print("o ", sink.object)

                if sink.predicate.n3() == '<http://dbpedia.org/ontology/wikiPageExtracted>':
                    timestamp = str(sink.object)

                queryNumbers = _get_query_numbers(sink.predicate.n3())
                if timestamp is None:
                    timestampUnknown.append(index)

                triple = [str(totalTriples), str(i+1), 'added', str(index), timestamp, '-'.join(queryNumbers), line]
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

                triple = [str(totalTriples), str(i+1), 'deleted', str(index), timestamp, '-'.join(queryNumbers), line]

                data.append(triple)
                index += 1
                totalTriples += 1

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

    print("days ", days)
    print("number of days ", len(days))
    print("total number of triples ", totalTriples)


def _generate_start_and_end_date(queryDate, distribution):
    startDateOfYear = datetime.strptime(config.START_DATE, "%Y-%m-%dT%H:%M:%S+00:00")

    startDate = None
    startDateIsKnown = True
    endDate = None
    endDateIsKnown = True

    leftBoundInSeconds = 0
    rightBoundInSeconds = 0

    while startDate is None and endDate is None:

        middleOfInterval = int(np.random.normal(loc=config.MEAN_closeness, scale=config.STANDARD_DEVIATION_closeness))
        widthOfInterval = int(np.random.normal(loc=config.MEAN_width, scale=config.STANDARD_DEVIATION_width))

        leftBoundInSeconds = middleOfInterval - widthOfInterval
        rightBoundInSeconds = middleOfInterval + widthOfInterval

        leftBound = startDateOfYear + timedelta(seconds=leftBoundInSeconds)
        rightBound = startDateOfYear + timedelta(seconds=rightBoundInSeconds)

        if leftBoundInSeconds < 0 or leftBoundInSeconds > config.MAX_SECONDS:
            startDateIsKnown = False
            leftBoundInSeconds = 0

        if rightBoundInSeconds < 0 or rightBoundInSeconds > config.MAX_SECONDS:
            endDateIsKnown = False
            rightBoundInSeconds = config.MAX_SECONDS

        if queryDate is not None and (leftBound > queryDate or rightBound < queryDate):
            continue
        else:
            startDate = leftBound.strftime("%Y-%m-%dT%H:%M:%S+00:00") if startDateIsKnown else 'unknown'
            endDate = rightBound.strftime("%Y-%m-%dT%H:%M:%S+00:00") if endDateIsKnown else 'unknown'

    distribution[leftBoundInSeconds:rightBoundInSeconds] += 1

    return startDate, endDate


def create_updates(inputFolderName, date, exportFolderName):

    np.random.seed(config.SEED)

    distribution = np.zeros(config.MAX_SECONDS)

    updates = []
    nOfTriples = 0
    nOfUpdates = 0

    while date != '18-08-2015':

        fileName = '{0}{1}.txt'.format(inputFolderName, date)
        if os.path.isfile(fileName):

            update = [str(nOfUpdates), date, str(nOfTriples+1)]
            print("update ", update)
            nOfInstancesInUpdate = config.NUMBER_OF_INSTANCES
            instance = None
            queryTime = None

            with open(fileName, "r") as file:

                for line in file:
                    inputData = line.split(',')
                    nOfTriples += 1

                    instanceFromFile = int(inputData[1])
                    if len(inputData[5]) > 0:
                        queryTime = datetime.strptime(config.QUERY_TIME, "%Y-%m-%dT%H:%M:%S+00:00")

                    if instance != instanceFromFile:

                        if nOfInstancesInUpdate == 0:
                            startDate, endDate = _generate_start_and_end_date(queryTime, distribution)
                            update.extend([str(nOfTriples), startDate, endDate])
                            print("update ", update)
                            updates.append(update)

                            nOfUpdates += 1
                            update = [str(nOfUpdates), date, str(nOfTriples+1)]
                            nOfInstancesInUpdate = config.NUMBER_OF_INSTANCES

                        nOfInstancesInUpdate -= 1
                        instance = instanceFromFile

            startDate, endDate = _generate_start_and_end_date(queryTime, distribution)
            update.extend([str(nOfTriples), startDate, endDate])
            print("update ", update)
            nOfUpdates += 1
            updates.append(update)

        timestamp = datetime.strptime(date, "%d-%m-%Y")
        newDate = timestamp + timedelta(days=1)
        date = newDate.strftime("%d-%m-%Y")

    with open("{0}{1}.txt".format(exportFolderName, config.updates_file_name), "w") as file:
        file.write('\n'.join(','.join(update) for update in updates))


if __name__ == "__main__":

    nOfInstances = '21045'
    # triples_by_date(config.raw_data_dir, 100, config.processed_data_dir)
    create_updates(config.processed_data_dir, '01-08-2015', config.updates_data_dir)