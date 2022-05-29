from datetime import datetime, timedelta
import numpy as np
import json
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import gzip
from src.main.bitr4qs.term.Triple import Triple
from .preprocess_BEAR_B import get_queries_from_nt_file


class UpdateGenerator(object):

    def __init__(self, config, inputFolder, exportFileName, statisticsFileName):
        self._config = config
        self._inputFolder = inputFolder
        self._exportFileName = exportFileName
        self._statisticsFileName = statisticsFileName

        self._distribution = np.zeros(self._config.MAX_SECONDS)
        self._queries = get_queries_from_nt_file(self._config.bear_queries_file_name)
        self._startDateOfYear = datetime.strptime(self._config.START_EFFECTIVE_DATE, "%Y-%m-%dT%H:%M:%S+00:00")
        self._queryTime = datetime.strptime(self._config.QUERY_TIME, "%Y-%m-%dT%H:%M:%S+00:00")

    def _generate_start_and_end_date(self, containQuery=False):
        """

        :param containQuery:
        :return:
        """
        startDate = None
        startDateIsKnown = True
        endDate = None
        endDateIsKnown = True

        leftBoundInSeconds = 0
        rightBoundInSeconds = 0

        while startDate is None and endDate is None:

            middleOfInterval = int(
                np.random.normal(loc=self._config.MEAN_closeness, scale=self._config.STANDARD_DEVIATION_closeness))
            widthOfInterval = int(
                np.random.normal(loc=self._config.MEAN_width, scale=self._config.STANDARD_DEVIATION_width))

            leftBoundInSeconds = middleOfInterval - widthOfInterval
            rightBoundInSeconds = middleOfInterval + widthOfInterval

            leftBound = self._startDateOfYear + timedelta(seconds=leftBoundInSeconds)
            rightBound = self._startDateOfYear + timedelta(seconds=rightBoundInSeconds)

            if leftBoundInSeconds < 0 or leftBoundInSeconds > self._config.MAX_SECONDS:
                startDateIsKnown = False
                leftBoundInSeconds = 0

            if rightBoundInSeconds < 0 or rightBoundInSeconds > self._config.MAX_SECONDS:
                endDateIsKnown = False
                rightBoundInSeconds = self._config.MAX_SECONDS

            if containQuery and (leftBound > self._queryTime or rightBound < self._queryTime):
                continue
            else:
                startDate = leftBound.strftime("%Y-%m-%dT%H:%M:%S+00:00") if startDateIsKnown else 'unknown'
                endDate = rightBound.strftime("%Y-%m-%dT%H:%M:%S+00:00") if endDateIsKnown else 'unknown'

        self._distribution[leftBoundInSeconds:rightBoundInSeconds] += 1

        return startDate, endDate

    def _contains_query(self, triple):
        """

        :param triple:
        :return:
        """
        for queryNumber, query in self._queries.items():
            if query.matches(triple):
                return True
        return False

    @staticmethod
    def _does_overlap(triple, doesOverlap, otherTriples=None):
        if otherTriples:
            overlap = False
            for i in range(len(otherTriples)):

                if triple == otherTriples[i]:
                    doesOverlap[i] = True
                    overlap = True
            if overlap:
                doesOverlap.append(True)
            else:
                doesOverlap.append(False)
        else:
            doesOverlap.append(False)

    def _read_modifications_file(self, fileName, containsQueries, doesOverlap, otherTriples=None):
        """

        :param fileName:
        :param containsQueries:
        :return:
        """
        counter = 0
        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)
        triples = []

        with gzip.open('{0}{1}'.format(self._inputFolder, fileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)
                triple = Triple((sink.subject, sink.predicate, sink.object))
                self._does_overlap(triple, doesOverlap, otherTriples)
                triples.append(triple)
                containsQueries.append(self._contains_query(triple))
                counter += 1
        return triples, counter

    def generate_updates(self):
        """

        :return:
        """
        np.random.seed(self._config.SEED)

        updates = []
        nOfTriples = 0
        nOfUpdates = 0

        for i in range(1, self._config.NUMBER_OF_VERSIONS):

            if i % 10 == 0:
                print("Processed files up to file number {0}-{1}".format(str(i-1), str(i)))

            addedFileName = 'data-added_{0}-{1}.nt.gz'.format(str(i), str(i+1))
            deletedFileName = 'data-deleted_{0}-{1}.nt.gz'.format(str(i), str(i+1))

            containsQueries = []
            doesOverlap = []
            insertions, nOfInsertions = self._read_modifications_file(addedFileName, containsQueries, doesOverlap)
            _, nOfDeletions = self._read_modifications_file(deletedFileName, containsQueries, doesOverlap, insertions)
            totalNOfTriples = nOfInsertions + nOfDeletions

            indices = np.arange(totalNOfTriples)
            np.random.shuffle(indices)

            index = 0

            while index < totalNOfTriples:

                inserted = []
                deleted = []
                doesContainQuery = False

                until = self._config.TRIPLES_PER_UPDATE if totalNOfTriples - index > self._config.TRIPLES_PER_UPDATE \
                    else totalNOfTriples - index

                for j in range(until):
                    tripleIndex = indices[index]
                    if tripleIndex < nOfInsertions:
                        if not doesOverlap[tripleIndex]:
                            inserted.append(str(tripleIndex))
                    else:
                        if not doesOverlap[tripleIndex]:
                            deleted.append(str(tripleIndex - nOfInsertions))
                    index += 1
                    if containsQueries[tripleIndex]:
                        doesContainQuery = True

                nInserted = len(inserted)
                nDeleted = len(deleted)

                if nInserted + nDeleted > 0:
                    nOfTriples += nInserted + nDeleted

                    startDate, endDate = self._generate_start_and_end_date(doesContainQuery)
                    update = [str(nOfUpdates), '{0}-{1}'.format(str(i), str(i + 1)), '-'.join(inserted),
                              '-'.join(deleted), startDate, endDate]
                    updates.append(update)
                    nOfUpdates += 1

        with open(self._exportFileName, "w") as file:
            file.write('\n'.join(','.join(update) for update in updates))

        minimumSeconds = np.argmin(self._distribution)
        maximumSeconds = np.argmax(self._distribution)
        minimumDate = (self._startDateOfYear + timedelta(seconds=float(minimumSeconds))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        maximumDate = (self._startDateOfYear + timedelta(seconds=float(maximumSeconds))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        distributionPerDay = np.sum(self._distribution.reshape((365, 86400)), axis=1)

        statistics = {'distributionPerDay': distributionPerDay.tolist(), 'minimumDate': minimumDate,
                      'maximumDate': maximumDate, 'numberOfUpdates': nOfUpdates, 'numberOfTriples': nOfTriples}

        with open(self._config.statistics_updates_file_name, 'w') as file:
            json.dump(statistics, file)



    # def generate_updates(self):
        #
        # np.random.seed(self._config.SEED)
        #
        # updates = []
        # nOfTriples = 0
        # nOfUpdates = 0
        # index = 0
        #
        # for i in range(self._config.NUMBER_OF_VERSIONS):
        #     fileName = self._get_name_of_data_file(i + index)
        #
        #     while not os.path.isfile('{0}{1}'.format(self._inputFolder, fileName)):
        #         index += 1
        #         fileName = self._get_name_of_data_file(i + index)
        #
        #     update = [str(nOfUpdates), fileName, str(nOfTriples + 1)]
        #     nOfInstancesInUpdate = self._config.NUMBER_OF_INSTANCES
        #     instance = None
        #     queryTime = None
        #
        #     with open('{0}{1}'.format(self._inputFolder, fileName), "r") as file:
        #
        #         for line in file:
        #             nOfTriples += 1
        #             inputData = line.split(',')
        #
        #             instanceFromFile = int(inputData[1])
        #             if len(inputData[5]) > 0:
        #                 queryTime = datetime.strptime(self._config.QUERY_TIME, "%Y-%m-%dT%H:%M:%S+00:00")
        #
        #             if instance != instanceFromFile:
        #
        #                 if nOfInstancesInUpdate == 0:
        #                     startDate, endDate = self._generate_start_and_end_date(queryTime)
        #                     update.extend([str(nOfTriples), startDate, endDate])
        #                     print("update ", update)
        #                     updates.append(update)
        #
        #                     update = [str(nOfUpdates), fileName, str(nOfTriples + 1)]
        #                     nOfUpdates += 1
        #                     nOfInstancesInUpdate = self._config.NUMBER_OF_INSTANCES
        #                     queryTime = None
        #
        #                 nOfInstancesInUpdate -= 1
        #                 instance = instanceFromFile
        #
        #         startDate, endDate = self._generate_start_and_end_date(queryTime)
        #         update.extend([str(nOfTriples), startDate, endDate])
        #         print("update ", update)
        #         nOfUpdates += 1
        #         updates.append(update)
        #
        # with open(self._exportFileName, "w") as file:
        #     file.write('\n'.join(','.join(update) for update in updates))
