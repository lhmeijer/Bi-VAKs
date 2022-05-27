from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import numpy as np
import gzip
from datetime import datetime, timedelta
from timeit import default_timer as timer
import json


class StoreCreator(object):

    def __init__(self, application, modificationsFolder, updateDataFile, config):
        self._application = application
        self._modificationsFolder = modificationsFolder
        self._updateDataFile = updateDataFile
        self._config = config

        self._updateIndex = 0
        self._updateIDs = []
        self._branch = None
        self._versionNumber = 0

        self._runtimeUpdates = []
        self._runtimeModifiedUpdates = []
        self._runtimeSnapshots = []
        self._runtimeBranches = []
        self._runtimeTags = []

        np.random.seed(self._config.SEED)

    def set_up_revision_store(self):

        # Initialise revision store

        with open(self._updateDataFile, "r") as file:
            updateData = file.readlines()

        while self._updateIndex != len(updateData):

            start = timer()
            # Create a Tag
            tagID = self._application.post('/tag', data=dict(
                name='version {0}'.format(str(self._versionNumber)), author='Jeroen Klein',
                date=self._config.QUERY_TIME, description='Add a new tag.', revision='HEAD', branch=self._branch))
            end = timer()
            self._runtimeTags.append(str(timedelta(seconds=end - start)))

            # Create Updates
            self._send_updates(updateData)

            if self._config.NUMBER_UPDATES_TO_SNAPSHOT is not None \
                    and self._updateIndex % self._config.NUMBER_UPDATES_TO_SNAPSHOT == 0:
                start = timer()
                # Create a Snapshot
                snapshotID = self._application.post('/snapshot', data=dict(
                    nameDataset='test-snapshot', urlDataset='http://localhost:3030', author='Vincent Koster',
                    date=self._config.SNAPSHOT_EFFECTIVE_DATE, description='Add a new snapshot.', revision='HEAD',
                    branch=self._branch))
                end = timer()
                self._runtimeSnapshots.append(str(timedelta(seconds=end - start)))

            if self._config.NUMBER_UPDATES_TO_BRANCH is not None \
                    and self._updateIndex % self._config.NUMBER_UPDATES_TO_BRANCH == 0:
                start = timer()
                # Create a Branch
                self._branch = 'branch {0}'.format(str(self._versionNumber))
                branchID = self._application.post('/branch', data=dict(
                    name=self._branch, author='Yvette Post', description='Add a new branch.'))
                end = timer()
                self._runtimeBranches.append(str(timedelta(seconds=end - start)))

            self._versionNumber += 1

        start = timer()
        # Create a Tag
        tagID = self._application.post('/tag', data=dict(
            name='version {0}'.format(str(self._versionNumber)), author='Jeroen Klein',
            date=self._config.QUERY_TIME, description='Add a new tag.', revision='HEAD', branch=self._branch))
        end = timer()
        self._runtimeTags.append(str(timedelta(seconds=end - start)))

        numberOfQuads = self._application.get('/quads')

        ingestionResults = {'TotalNumberOfQuads': numberOfQuads, 'IngestionTimeUpdates': self._runtimeUpdates,
                            'MEAN_IngestionTimeUpdates': np.mean(np.array(self._runtimeUpdates)),
                            'STANDARD_DEVIATION_IngestionTimeUpdates': np.std(np.array(self._runtimeUpdates)),
                            'IngestionTimeTags': self._runtimeTags,
                            'MEAN_IngestionTimeTags': np.mean(np.array(self._runtimeTags)),
                            'STANDARD_DEVIATION_IngestionTimeTags': np.std(np.array(self._runtimeTags)),
                            'IngestionTimeModifiedUpdates': self._runtimeModifiedUpdates,
                            'MEAN_IngestionTimeModifiedUpdates': np.mean(np.array(self._runtimeModifiedUpdates)),
                            'STANDARD_DEVIATION_IngestionTimeModifiedUpdates': np.std(np.array(self._runtimeModifiedUpdates)),
                            'IngestionTimeSnapshots': self._runtimeSnapshots,
                            'MEAN_IngestionTimeSnapshots': np.mean(np.array(self._runtimeSnapshots)),
                            'STANDARD_DEVIATION_IngestionTimeSnapshots': np.std(np.array(self._runtimeSnapshots)),
                            'IngestionTimeBranches': self._runtimeBranches,
                            'MEAN_IngestionTimeBranches': np.mean(np.array(self._runtimeBranches)),
                            'STANDARD_DEVIATION_IngestionTimeBranches': np.std(np.array(self._runtimeBranches)),
                            }

        with open(self._config.ingestion_results_file_name, 'w') as file:
            json.dump(ingestionResults, file)

    def _read_modifications_file(self, fileName, deletion=False):

        sink = TripleSink()
        NTriplesParser = W3CNTriplesParser(sink=sink)
        modifications = []

        with gzip.open('{0}{1}'.format(self._modificationsFolder, fileName), 'rt') as file:
            for line in file:
                NTriplesParser.parsestring(line)
                modification = sink.add_modification(deletion=deletion)
                modifications.append(modification)
        return modifications

    def _send_updates(self, updateData):
        addedFileName = 'data-added_{0}.nt.gz'.format(updateData[self._updateIndex][1])
        deletedFileName = 'data-deleted_{0}.nt.gz'.format(updateData[self._updateIndex][1])

        insertions = self._read_modifications_file(addedFileName)
        deletions = self._read_modifications_file(deletedFileName, deletion=True)
        totalTriples = len(insertions) + len(deletions)

        index = 0
        while index <= totalTriples:

            updateInsertions = [insertions[int(i)] for i in updateData[self._updateIndex][2].split('-')]
            updateDeletions = [deletions[int(i)] for i in updateData[self._updateIndex][3].split('-')]
            index += len(updateDeletions) + len(updateInsertions)

            SPARQLUpdateQuery = self._update_sparql_from_modifications(updateInsertions + updateDeletions)

            start = timer()
            updateID = self._application.post('/update', data=dict(
                update=SPARQLUpdateQuery, author='Tom de Vries', startDate=updateData[self._updateIndex][4],
                endDate=updateData[self._updateIndex][5], description='Add new update.', branch=self._branch))
            end = timer()
            self._runtimeUpdates.append(str(timedelta(seconds=end - start)))

            self._updateIDs.append(updateID)

            self._updateIndex += 1
            if self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE is not None \
                    and self._updateIndex % self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE == 0:

                if self._config.FROM_LAST_X_UPDATES is not None:
                    randomInt = np.random.randint(self._updateIndex - self._config.FROM_LAST_X_UPDATES,
                                                  self._updateIndex)
                else:
                    randomInt = np.random.randint(1, self._updateIndex)

                startDate = ''
                if updateData[randomInt][4] != 'unknown':
                    startTimestamp = datetime.strptime(updateData[randomInt][4], "%Y-%m-%dT%H:%M:%S+00:00")
                    startDate = (startTimestamp + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

                endDate = ''
                if updateData[randomInt][5] != 'unknown':
                    endTimestamp = datetime.strptime(updateData[randomInt][5], "%Y-%m-%dT%H:%M:%S+00:00")
                    endDate = (endTimestamp + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

                start = timer()
                updateID = self._application.post('/update/{0}'.format(self._updateIDs[randomInt]), data=dict(
                    author='Tom de Vries', description='Modify update.', branch=self._branch, startDate=startDate,
                    endDate=endDate))
                end = timer()
                self._runtimeModifiedUpdates.append(str(timedelta(seconds=end - start)))

    @staticmethod
    def _update_sparql_from_modifications(modifications):
        deleteString, insertString = "", ""

        for modification in modifications:
            if modification.deletion:
                deleteString += modification.value.to_sparql()
            else:
                insertString += modification.value.to_sparql()

        SPARQLQuery = """DELETE DATA {{ {0} }};
        INSERT DATA {{ {1} }}""".format(deleteString, insertString)
        return SPARQLQuery
