from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from src.main.bitr4qs.tools.parser.Parser import TripleSink
import numpy as np
import gzip


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

        np.random.seed(self._config.SEED)

    def set_up_revision_store(self):

        # Initialise revision store

        with open(self._updateDataFile, "r") as file:
            updateData = file.readlines()

            while self._updateIndex != len(updateData):
                # Create a Tag
                tagID = self._application.post('/tag', data=dict(
                    name='version {0}'.format(str(self._versionNumber)), author='Jeroen Klein',
                    date=self._config.QUERY_TIME, description='Add a new tag.', revision='HEAD', branch=branch))

                # Create Updates
                self._send_updates(updateData)

                if self._config.NUMBER_UPDATES_TO_SNAPSHOT is not None \
                        and self._updateIndex % self._config.NUMBER_UPDATES_TO_SNAPSHOT == 0:
                    # Create a Snapshot
                    snapshotID = self._application.post('/snapshot', data=dict(
                        nameDataset='test-snapshot', urlDataset='http://localhost:3030', author='Vincent Koster',
                        date=self._config.SNAPSHOT_EFFECTIVE_DATE, description='Add a new snapshot.', revision='HEAD',
                        branch=branch))

                if self._config.NUMBER_UPDATES_TO_BRANCH is not None \
                        and self._updateIndex % self._config.NUMBER_UPDATES_TO_BRANCH == 0:
                    # Create a Branch
                    branch = 'branch {0}'.format(str(self._versionNumber))
                    branchID = self._application.post('/branch', data=dict(
                        name=branch, author='Yvette Post', description='Add a new branch.'))

                self._versionNumber += 1

            tagID = self._application.post('/tag', data=dict(
                name='version {0}'.format(str(self._versionNumber)), author='Jeroen Klein',
                date=self._config.QUERY_TIME, description='Add a new tag.', revision='HEAD', branch=branch))

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

            startDate = '' if updateData[self._updateIndex][4] == 'unknown' else updateData[self._updateIndex][4]
            endDate = '' if updateData[self._updateIndex][5] == 'unknown' else updateData[self._updateIndex][5]
            SPARQLUpdateQuery = self._update_sparql_from_modifications(updateInsertions + updateDeletions)

            updateID = self._application.post('/update', data=dict(
                update=SPARQLUpdateQuery, author='Tom de Vries', startDate=startDate, endDate=endDate,
                description='Add new update.', branch=self._branch))
            self._updateIDs.append(updateID)

            self._updateIndex += 1
            if self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE is not None \
                    and self._updateIndex % self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE == 0:

                if self._config.FROM_LAST_X_UPDATES is not None:
                    randomInt = np.random.randint(self._updateIndex - self._config.FROM_LAST_X_UPDATES,
                                                  self._updateIndex)
                else:
                    randomInt = np.random.randint(1, self._updateIndex)
                updateID = self._application.post('/update/{0}'.format(self._updateIDs[randomInt]), data=dict(
                    author='Tom de Vries', description='Modify update.', branch=self._branch))


    # def _send_updates(self, updateData):
    #
    #     fileName = '{0}{1}'.format(self._modificationsFolder, updateData[self._updateIndex][1])
    #     startIndex = 1
    #     endIndex = updateData[self._updateIndex][3]
    #
    #     modifications = []
    #     sink = TripleSink()
    #     NTriplesParser = W3CNTriplesParser(sink=sink)
    #
    #     with open(fileName, 'r') as file:
    #         for line in file:
    #
    #             tripleString = ''.join(line[6:])
    #             deletion = True if line[2] == 'deleted' else False
    #             NTriplesParser.parsestring(tripleString)
    #             modification = sink.add_modification(deletion=deletion)
    #             modifications.append(modification)
    #
    #             if startIndex == endIndex:
    #                 startDate = '' if updateData[self._updateIndex][4] == 'unknown' else updateData[self._updateIndex][4]
    #                 endDate = '' if updateData[self._updateIndex][5] == 'unknown' else updateData[self._updateIndex][5]
    #                 SPARQLUpdateQuery = self._update_sparql_from_modifications(modifications)
    #                 updateID = self._application.post('/update', data=dict(
    #                     update=SPARQLUpdateQuery, author='Tom de Vries', startDate=startDate, endDate=endDate,
    #                     description='Add new update.', branch=self._branch))
    #                 self._updateIDs.append(updateID)
    #
    #                 endIndex = updateData[self._updateIndex][3]
    #                 self._updateIndex += 1
    #
    #                 if self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE is not None \
    #                         and self._updateIndex % self._config.NUMBER_UPDATES_TO_MODIFIED_UPDATE == 0:
    #
    #                     if self._config.FROM_LAST_X_UPDATES is not None:
    #                         randomInt = np.random.randint(self._updateIndex - self._config.FROM_LAST_X_UPDATES,
    #                                                       self._updateIndex)
    #                     else:
    #                         randomInt = np.random.randint(1, self._updateIndex)
    #                     updateID = self._application.post('/update/{0}'.format(self._updateIDs[randomInt]), data=dict(
    #                         author='Tom de Vries', description='Modify update.', branch=self._branch))
    #
    #             startIndex += 1

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