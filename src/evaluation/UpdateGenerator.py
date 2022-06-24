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
        queryTime = datetime.strptime(self._config.QUERY_TIME, "%Y-%m-%dT%H:%M:%S+00:00")
        self._startTimeQueryTime = queryTime - timedelta(days=3)
        self._endTimeQueryTime = queryTime + timedelta(days=3)

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
            if widthOfInterval < 0:
                continue

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

            if containQuery and (leftBound > self._startTimeQueryTime or rightBound < self._endTimeQueryTime):
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

    def _read_modifications_file(self, fileName, containsQueries):
        """

        :param fileName:
        :param containsQueries:
        :return:
        """
        counter = 0
        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)
        with gzip.open('{0}{1}'.format(self._inputFolder, fileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line.strip())
                triple = Triple((sink.subject, sink.predicate, sink.object))
                containsQueries.append(self._contains_query(triple))
                counter += 1
        return counter

    def generate_updates(self):
        """

        :return:
        """
        np.random.seed(self._config.SEED)

        updates = []
        nOfTriples = 0
        nOfUpdates = 0

        for i in range(1, self._config.NUMBER_OF_VERSIONS):

            if i % 1 == 0:
                print("Processed files up to file number {0}-{1}".format(str(i), str(i+1)))

            addedFileName = 'data-added_{0}-{1}.nt.gz'.format(str(i), str(i+1))
            deletedFileName = 'data-deleted_{0}-{1}.nt.gz'.format(str(i), str(i+1))

            containsQueries = []
            # doesOverlap = []
            nOfInsertions = self._read_modifications_file(addedFileName, containsQueries)
            nOfDeletions = self._read_modifications_file(deletedFileName, containsQueries)
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
                        inserted.append(str(tripleIndex))
                    else:
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
                    nOfUpdates += 1

                    with open(self._exportFileName, "a") as file:
                        file.write(','.join(update) + '\n')
                if nOfUpdates % 100 == 0:
                    print(nOfUpdates)

        minimumSeconds = np.argmin(self._distribution)
        maximumSeconds = np.argmax(self._distribution)
        minimumDate = (self._startDateOfYear + timedelta(seconds=float(minimumSeconds))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        maximumDate = (self._startDateOfYear + timedelta(seconds=float(maximumSeconds))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        distributionPerDay = np.sum(self._distribution.reshape((365, 86400)), axis=1)
        time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02.00")

        statistics = {'creationDate': str(time), 'distributionPerDay': distributionPerDay.tolist(),
                      'minimumDate': minimumDate, 'maximumDate': maximumDate, 'numberOfUpdates': nOfUpdates,
                      'numberOfTriples': nOfTriples}

        with open(self._statisticsFileName, 'w') as file:
            json.dump(statistics, file)
