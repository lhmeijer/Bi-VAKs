from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import numpy as np
import gzip
from datetime import datetime, timedelta
from timeit import default_timer as timer
import json
from src.main.bitr4qs.namespace import BITR4QS
import os


class StoreCreator(object):

    def __init__(self, application, modificationsFolder, updateDataFile, config, revisionStoreFileName,
                 ingestionResultsFileName):
        self._application = application
        self._modificationsFolder = modificationsFolder
        self._updateDataFile = updateDataFile
        self._config = config
        self._revisionStoreFileName = revisionStoreFileName
        self._ingestionResultsFileName = ingestionResultsFileName

        self._updateIndex = 0
        self._updateIDs = []
        self._branch = None
        self._versionNumber = 1
        self._totalNumberOfUpdates = 0

        self._runtimeUpdates = []
        self._numberOfProcessedQuads = []
        self._runtimeModifiedUpdates = []
        self._runtimeSnapshots = []
        self._runtimeBranches = []
        self._runtimeTags = []
        self._totalNumberOfSeconds = 0

        np.random.seed(self._config.SEED)

    def reset_revision_store(self):
        try:
            self._application.delete('/reset')
        except Exception:
            raise Exception

    def _create_tag(self):
        start = timer()
        tag = self._application.post('/tag', data=dict(
            name='version {0}'.format(str(self._versionNumber)), author='Jeroen Klein',
            date=self._config.QUERY_TIME, description='Add a new tag {0}.'.format(str(self._versionNumber)),
            revision='HEAD', branch=self._branch))
        end = timer()
        seconds = timedelta(seconds=end - start).total_seconds()
        self._runtimeTags.append(seconds)
        self._totalNumberOfSeconds += seconds

    def _create_snapshot(self):
        start = timer()
        snapshotID = self._application.post('/snapshot', data=dict(
            nameDataset='snapshot-{0}'.format(self._versionNumber), urlDataset='http://localhost:3030',
            author='Vincent Koster', date=self._config.SNAPSHOT_EFFECTIVE_DATE,
            description='Add a new snapshot.', revision='HEAD', branch=self._branch))
        end = timer()
        seconds = timedelta(seconds=end - start).total_seconds()
        self._runtimeSnapshots.append(seconds)
        self._totalNumberOfSeconds += seconds

    def _create_branch(self):
        start = timer()
        # Create a Branch
        oldBranch = self._branch
        self._branch = 'branch {0}'.format(str(self._versionNumber))
        branchID = self._application.post('/branch', data=dict(
            name=self._branch, author='Yvette Post', branch=oldBranch,
            description='Add a new branch {0}.'.format(self._versionNumber)))
        end = timer()
        seconds = timedelta(seconds=end - start).total_seconds()
        self._runtimeBranches.append(seconds)
        self._totalNumberOfSeconds += seconds

    def set_up_revision_store(self):

        print("Obtain the updates.")
        updateData = []
        with open(self._updateDataFile, "r") as file:
            for line in file:
                updateData.append(line.strip().split(','))
        self._totalNumberOfUpdates = len(updateData)
        print("self._totalNumberOfUpdates ", self._totalNumberOfUpdates)

        # Initialise revision store
        print("Initialise Revision Store!")
        self._application.post('/initialise', data=dict(
            author='Yvette Post', nameDataset='BEAR-B', urlDataset='http://localhost:3030',
            description='Initialise BiTR4Qs.', startDate='unknown', endDate='unknown'))
        print("Revision Store is initialised!")

        while self._versionNumber < self._config.NUMBER_OF_VERSIONS:
            print("Create a Tag with version number ", self._versionNumber)
            self._create_tag()

            # Create Updates
            self._send_updates(updateData)

            if self._config.VERSIONS_TO_SNAPSHOT and self._versionNumber % self._config.VERSIONS_TO_SNAPSHOT == 0:
                print("Create a Snapshot with name snapshot-{0}".format(self._versionNumber))
                self._create_snapshot()

            if self._config.VERSIONS_TO_BRANCH and self._versionNumber % self._config.VERSIONS_TO_BRANCH == 0:
                print("Create a Branch with name branch ", self._versionNumber)
                self._create_branch()

            self._versionNumber += 1

        # Create the last Tag
        print("Create a Tag with version number ", self._versionNumber)
        self._create_tag()

        numberOfQuadsResponse = self._application.get('/quads')
        numberOfQuads = numberOfQuadsResponse.data.decode("utf-8")
        print("numberOfQuads ", numberOfQuads)

        dataResponse = self._application.get('/data', headers=dict(accept="application/n-triples"))
        with open(self._revisionStoreFileName, 'w') as file:
            file.write(dataResponse.data.decode("utf-8"))

        size = os.path.getsize(self._config.revision_store_file_name)
        time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00")

        ingestionResults = {'creationDate':str(time), 'NUMBER_quads': numberOfQuads, 'FILE_SIZE_MB': size/1000000,
                            'TOTAL_IngestionTime': str(timedelta(seconds=self._totalNumberOfSeconds)),
                            'MEAN_IngestionTimeUpdates': np.mean(np.array(self._runtimeUpdates)),
                            'STANDARD_DEVIATION_IngestionTimeUpdates': np.std(np.array(self._runtimeUpdates)),
                            'MEAN_IngestionTimeTags': np.mean(np.array(self._runtimeTags)),
                            'STANDARD_DEVIATION_IngestionTimeTags': np.std(np.array(self._runtimeTags)),
                            'IngestionTimeUpdates': self._runtimeUpdates,
                            'IngestionTimeTags': self._runtimeTags,
                            'NumberOfProcessedQuads': self._numberOfProcessedQuads
                            }

        if self._config.VERSIONS_TO_SNAPSHOT:
            ingestionResults['IngestionTimeSnapshots'] = self._runtimeSnapshots
            ingestionResults['MEAN_IngestionTimeSnapshots'] = np.mean(np.array(self._runtimeSnapshots))
            ingestionResults['STANDARD_DEVIATION_IngestionTimeSnapshots'] = np.std(np.array(self._runtimeSnapshots))

        if self._config.VERSIONS_TO_BRANCH:
            ingestionResults['IngestionTimeBranches'] = self._runtimeBranches
            ingestionResults['MEAN_IngestionTimeBranches'] = np.mean(np.array(self._runtimeBranches))
            ingestionResults['STANDARD_DEVIATION_IngestionTimeBranches'] = np.std(np.array(self._runtimeBranches))

        if self._config.UPDATES_TO_MODIFIED_UPDATE:
            ingestionResults['IngestionTimeModifiedUpdates'] = self._runtimeModifiedUpdates
            ingestionResults['MEAN_IngestionTimeModifiedUpdates'] = np.mean(np.array(self._runtimeModifiedUpdates))
            ingestionResults['STANDARD_DEVIATION_IngestionTimeModifiedUpdates'] = np.std(np.array(self._runtimeModifiedUpdates))

        with open(self._ingestionResultsFileName, 'w') as file:
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
        fileName = updateData[self._updateIndex][1]
        addedFileName = 'data-added_{0}.nt.gz'.format(updateData[self._updateIndex][1])
        deletedFileName = 'data-deleted_{0}.nt.gz'.format(updateData[self._updateIndex][1])

        insertions = self._read_modifications_file(addedFileName)
        deletions = self._read_modifications_file(deletedFileName, deletion=True)

        while self._updateIndex < self._totalNumberOfUpdates and fileName == updateData[self._updateIndex][1]:

            updateInsertions = []
            if len(updateData[self._updateIndex][2]) > 0:
                updateInsertions.extend([insertions[int(i)] for i in updateData[self._updateIndex][2].split('-')])

            updateDeletions = []
            if len(updateData[self._updateIndex][3]) > 0:
                updateDeletions.extend([deletions[int(i)] for i in updateData[self._updateIndex][3].split('-')])

            SPARQLUpdateQuery = self._update_sparql_from_modifications(updateInsertions + updateDeletions)

            numberOfQuadsResponse = self._application.get('/quads')
            self._numberOfProcessedQuads.append(int(numberOfQuadsResponse.data.decode("utf-8")))

            start = timer()
            updateResponse = self._application.post('/update', data=dict(
                update=SPARQLUpdateQuery, author='Tom de Vries', startDate=updateData[self._updateIndex][4],
                branch=self._branch, description='Add update {0}.'.format(str(self._updateIndex+1)),
                endDate=updateData[self._updateIndex][5], test=''))
            end = timer()
            seconds = timedelta(seconds=end - start).total_seconds()
            self._runtimeUpdates.append(seconds)
            self._totalNumberOfSeconds += seconds

            statuscode = updateResponse.status_code
            if statuscode > 300:
                raise Exception("Something is not correct.")

            update = json.loads(updateResponse.data.decode("utf-8"))
            self._updateIDs.append(update["identifier"])

            if self._config.UPDATES_TO_MODIFIED_UPDATE \
                    and (self._updateIndex + 1) % self._config.UPDATES_TO_MODIFIED_UPDATE == 0:

                if self._config.FROM_LAST_X_UPDATES:
                    randomInt = np.random.randint(self._updateIndex - self._config.FROM_LAST_X_UPDATES,
                                                  self._updateIndex)
                else:
                    randomInt = np.random.randint(0, self._updateIndex)

                startDate = ''
                if updateData[randomInt][4] != 'unknown':
                    startTimestamp = datetime.strptime(updateData[randomInt][4], "%Y-%m-%dT%H:%M:%S+00:00")
                    startDate = (startTimestamp + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

                endDate = ''
                if updateData[randomInt][5] != 'unknown':
                    endTimestamp = datetime.strptime(updateData[randomInt][5], "%Y-%m-%dT%H:%M:%S+00:00")
                    endDate = (endTimestamp + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")


                updateID = self._updateIDs.pop(randomInt)
                updateID = updateID.replace(str(BITR4QS), '')
                start = timer()
                updateResponse = self._application.post('/update/{0}'.format(updateID), data=dict(
                    author='Tom de Vries', description='Modify update {0}.'.format(updateID), branch=self._branch,
                    startDate=startDate, endDate=endDate, test=''))
                end = timer()
                seconds = timedelta(seconds=end - start).total_seconds()
                self._runtimeModifiedUpdates.append(seconds)
                self._totalNumberOfSeconds += seconds

                update = json.loads(updateResponse.data.decode("utf-8"))
                if update["identifier"] in self._updateIDs:
                    print("update ", update)
                    raise Exception
                self._updateIDs.append(update["identifier"])

            self._updateIndex += 1
            if self._updateIndex % 100 == 0:
                print("We have created update ", self._updateIndex)

    @staticmethod
    def _update_sparql_from_modifications(modifications):
        deleteString, insertString = "", ""
        insert = False
        delete = False

        for modification in modifications:
            if modification.deletion:
                deleteString += modification.value.sparql() + '\n'
                delete = True
            else:
                insertString += modification.value.sparql() + '\n'
                insert = True

        if delete and insert:
            SPARQLQuery = 'DELETE DATA {{ {0} }};\nINSERT DATA {{ {1} }}'.format(deleteString, insertString)
        elif delete and not insert:
            SPARQLQuery = 'DELETE DATA {{ {0} }}'.format(deleteString)
        elif insert and not delete:
            SPARQLQuery = 'INSERT DATA {{ {0} }}'.format(insertString)
        else:
            raise Exception("This update does not contain any modifications.")

        return SPARQLQuery
