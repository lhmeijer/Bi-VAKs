from src.evaluation.configuration import BearBConfiguration
import os
from src.evaluation.UpdateGenerator import UpdateGenerator
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
from src.evaluation.StoreCreator import StoreCreator
from src.evaluation.Evaluator import Evaluator
import itertools
from src.evaluation.preprocess_BEAR_B import ChangeComputer
from datetime import datetime

if __name__ == "__main__":

    computeChangeDataset = False
    generateUpdates = False
    createStore = False
    evaluateQueries = True

    if computeChangeDataset:
        config = BearBConfiguration()
        changeComputer = ChangeComputer(config, inputFolder=config.raw_version_data_dir,
                                        exportFolder=config.raw_change_data_dir)
        changeComputer.compute_changes()

    if generateUpdates:

        possibleIndices = [[0], [50, 100], [(1000000, 4320000), (5000000, 432000)]]  # ->  2 x 2 = 4
        permutationsIndices = list(itertools.product(*possibleIndices))

        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seed=indices[0], closeness=indices[2][0], width=indices[2][1],
                                        triplesPerUpdate=indices[1])
            if not os.path.isfile(config.updates_file_name):
                updateGenerator = UpdateGenerator(config=config, inputFolder=config.raw_change_data_dir,
                                                  exportFileName=config.updates_file_name,
                                                  statisticsFileName=config.statistics_updates_file_name)
                updateGenerator.generate_updates()

    if createStore:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        generalIndices = [[0], [50, 100], [(1000000, 4320000), (5000000, 432000)], [(None, None)], [None],
                          [(None, None)], ['combined'], ['repeated']]   # -> 2 x 2 x 3 = 12
        permutationsGeneralIndices = list(itertools.product(*generalIndices))
        # snapshotModifiedIndices = [[0], [50], [(1000000, 4320000)], [(None, None), ('N', 45)], [None], [(None, 5)],
        #                    ['explicit'], ['repeated']]  # -> 2 x 3 x 2 = 12
        snapshotModifiedIndices = [[0], [50], [(1000000, 4320000)], [(None, None)], [None], [(None, 5)],
                           ['combined'], ['related']]  # -> 2 x 3 x 2 = 12
        permutationsSnapshotModifiedIndices = list(itertools.product(*snapshotModifiedIndices))
        branchIndices = [[0], [50], [(1000000, 4320000)], [(None, None)], [3], [(None, None)],
                         ['combined'], ['repeated']]  # -> 3
        permutationsBranchIndices = list(itertools.product(*branchIndices))

        permutationsIndices = permutationsSnapshotModifiedIndices

        permutationsIndices = [(0, 50, (1000000, 4320000), ('N', 45), 3, (None, 5), 'implicit', 'related')]
        # TODO shutdown server such that we can run explicit and implicit at the same time.
        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seed=indices[0], closeness=indices[2][0], width=indices[2][1], snapshot=indices[3],
                                        triplesPerUpdate=indices[1], branch=indices[4], modifiedUpdate=indices[5],
                                        reference=indices[6], content=indices[7])

            args = get_default_configuration()
            args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                         'implicit': config.REFERENCE_IMPLICIT,
                                         'combined': config.REFERENCE_COMBINED}
            args['updateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

            application = create_app(args).test_client()
            for i in range(5):
                print("Round ", i)
                print("Current time ", datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00"))
                createFile = not os.path.isfile(config.revision_store_file_name)
                store = StoreCreator(application=application, config=config, createRevisionStoreFile=createFile,
                                     modificationsFolder=config.raw_change_data_dir,
                                     updateDataFile=config.updates_file_name,
                                     revisionStoreFileName=config.revision_store_file_name,
                                     ingestionResultsFileName=config.ingestion_results_file_name)
                store.set_up_revision_store()
                store.reset_revision_store()

    if evaluateQueries:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        generalIndicesVM = [[0], [50, 100], [(1000000, 4320000), (5000000, 432000)], [(None, None)], [None],
                           [(None, None)], ['combined'], ['repeated'], [('specific', 20)], ['aggregated'], ['between']]
        # generalIndicesVM = [[0], [50, 100], [(1000000, 4320000)], [(None, None)], [None],
        #                    [(None, None)], ['combined'], ['repeated'], [('specific', 20)], ['aggregated'], ['between']]
        permutationsGeneralIndicesVM = list(itertools.product(*generalIndicesVM))
        snapshotModifiedIndicesVM = [[0], [50], [(1000000, 4320000)], [('N', 45)], [None], [(None, 5)],
                                    ['implicit'], ['repeated'], [('specific', 20)], ['aggregated'], ['between']]  # -> 3 x 2 x 2 = 12
        # snapshotModifiedIndicesVM = [[0], [50], [(1000000, 4320000)], [(None, None)], [None], [(None, 5)],
        #                             ['implicit'], ['related'], [('specific', 20)], ['aggregated']]  # -> 3 x 2 x 2 = 12
        permutationsSnapshotModifiedIndicesVM = list(itertools.product(*snapshotModifiedIndicesVM))
        branchIndicesVM = [[0], [50], [(1000000, 4320000)], [(None, None)], [3], [(None, None)], ['implicit'],
                           ['repeated'], [('specific', 20)], ['aggregated'], ['between']]    # -> 3
        permutationsBranchIndicesVM = list(itertools.product(*branchIndicesVM))
        permutationsIndicesVM = permutationsGeneralIndicesVM + permutationsSnapshotModifiedIndicesVM + \
                                permutationsBranchIndicesVM

        # -> 2 x 2 x 3 x 2 = 24
        generalIndicesDM = [[0], [50, 100], [(1000000, 4320000), (5000000, 432000)], [(None, None)], [None],
                           [(None, None)], ['explicit'], ['repeated'], [('specific', 20)], ['aggregated'], ['initial']]
        # generalIndicesDM = [[0], [50, 100], [(1000000, 4320000)], [(None, None)], [None], [(None, None)],
        #                    ['implicit'], ['repeated'], [('specific', 20)], ['sorted'], ['initial']]
        permutationsGeneralIndicesDM = list(itertools.product(*generalIndicesDM))
        modifiedIndicesDM = [[0], [50], [(1000000, 4320000)], [(None, None)], [None], [(None, 5)],
                             ['explicit'], ['related'], [('specific', 20)], ['aggregated'], ['between']]  # -> 3 x 2 = 6
        permutationsModifiedIndicesDM = list(itertools.product(*modifiedIndicesDM))
        branchIndicesDM = [[0], [50], [(1000000, 4320000)], [(None, None)], [3], [(None, None)], ['implicit'],
                           ['repeated'], [('specific', 20)], ['aggregated'], ['between']]    # -> 3
        permutationsBranchIndicesDM = list(itertools.product(*branchIndicesDM))
        permutationsIndicesDM = permutationsGeneralIndicesDM + permutationsModifiedIndicesDM + \
                                permutationsBranchIndicesDM

        # -> 2 x 2 x 3 x 2 x 2 = 48
        # generalIndicesVQ = [[0], [50, 100], [(1000000, 4320000), (5000000, 432000)], [(None, None)], [None],
        #                    [(None, None)], ['explicit'], ['repeated'], [('specific', 20)], ['sorted'], ['initial']]
        generalIndicesVQ = [[0], [100], [(1000000, 4320000)], [(None, None)], [None],
                           [(None, None)], ['explicit'], ['repeated'], [('specific', 20)], ['sorted'], ['initial']]
        permutationsGeneralIndicesVQ = list(itertools.product(*generalIndicesVQ))
        modifiedIndicesVQ = [[0], [50], [(1000000, 4320000)], [(None, None)], [None], [(None, 5)],
                             ['explicit'], ['repeated'], [('specific', 20)], ['sorted'], ['initial']]  # -> 3 x 2 = 6
        permutationsModifiedIndicesVQ = list(itertools.product(*modifiedIndicesVQ))
        branchIndicesVQ = [[0], [50], [(1000000, 4320000)], [(None, None)], [3], [(None, None)], ['implicit'],
                           ['repeated'], [('specific', 20)], ['sorted'], ['initial']]    # -> 3
        permutationsBranchIndicesVQ = list(itertools.product(*branchIndicesVQ))
        permutationsIndicesVQ = permutationsGeneralIndicesVQ + permutationsModifiedIndicesVQ + \
                                permutationsBranchIndicesVQ
        # permutationsIndices = [(0, 50, 1000000, 4320000, ('N', 30), None, (None, None), 'explicit', 'repeated',
        #                         'specific')]
        for indices in permutationsGeneralIndicesVM:
            print("indices ", indices)

            config = BearBConfiguration(seed=indices[0], closeness=indices[2][0], width=indices[2][1], snapshot=indices[3],
                                        triplesPerUpdate=indices[1], branch=indices[4], modifiedUpdate=indices[5],
                                        reference=indices[6], fetching=indices[8][0], content=indices[7],
                                        numberOfQueries=indices[8][1], modifications=indices[9], retrieve=indices[10])

            print("config.query_results_file_name ", config.query_results_file_name)
            # if os.path.isfile(config.revision_store_file_name) and not os.path.isfile(config.query_results_file_name):
            if os.path.isfile(config.revision_store_file_name):
                args = get_default_configuration()
                args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                             'implicit': config.REFERENCE_IMPLICIT,
                                             'combined': config.REFERENCE_COMBINED}
                args['fetchingStrategy'] = {'queryAllUpdates': config.FETCHING_ALL,
                                            'querySpecificUpdates': config.FETCHING_SPECIFIC}
                args['updateContentStrategy'] = {'repeated': config.CONTENT_REPEATED,
                                                 'related': config.CONTENT_RELATED}
                args['modificationsStrategy'] = {'aggregated': config.MODIFICATIONS_AGGREGATED,
                                                 'sorted': config.MODIFICATIONS_SORTED}
                args['retrievingStrategy'] = {'betweenUpdates': config.RETRIEVING_BETWEEN,
                                              'fromInitialUpdate': config.RETRIEVING_INITIAL}

                application = create_app(args).test_client()

                for i in range(5):
                    print("Round ", i)
                    print("Current time ", datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00"))
                    evaluator = Evaluator(config=config, application=application,
                                          revisionStoreFileName=config.revision_store_file_name,
                                          queryResultsFileName=config.query_results_file_name)
                    evaluator.set_up_revision_store()
                    evaluator.evaluate()
                    evaluator.reset_revision_store()
