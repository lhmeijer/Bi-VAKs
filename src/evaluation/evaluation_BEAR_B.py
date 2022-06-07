from src.evaluation.configuration import BearBConfiguration
import os
from src.evaluation.UpdateGenerator import UpdateGenerator
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
from src.evaluation.StoreCreator import StoreCreator
from src.evaluation.Evaluator import Evaluator
import itertools
from src.evaluation.preprocess_BEAR_B import ChangeComputer


if __name__ == "__main__":

    computeChanges = False
    generateUpdates = False
    createStore = False
    evaluateQueries = True

    if computeChanges:
        config = BearBConfiguration()
        changeComputer = ChangeComputer(config, inputFolder=config.raw_version_data_dir,
                                        exportFolder=config.raw_change_data_dir)
        changeComputer.compute_changes()

    if generateUpdates:

        possibleIndices = [[0], [10, 100], [1000000, 10000000], [432000, 4320000]]
        permutationsIndices = list(itertools.product(*possibleIndices))

        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seed=indices[0], closeness=indices[2], width=indices[3],
                                        triplesPerUpdate=indices[1])
            if not os.path.isfile(config.updates_file_name):
                updateGenerator = UpdateGenerator(config=config, inputFolder=config.raw_change_data_dir,
                                                  exportFileName=config.updates_file_name,
                                                  statisticsFileName=config.statistics_updates_file_name)
                updateGenerator.generate_updates()

    if createStore:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        snapshotIndices = [[0], [100], [1000000, 10000000], [432000, 4320000], [(None, None), ('F', 30), ('N', 30)],
                           [None], [(None, None)], ['explicit', 'implicit'], ['repeated']]       # -> 48
        permutationsSnapshotIndices = list(itertools.product(*snapshotIndices))
        branchIndices = [[0], [100], [1000000], [4320000], [(None, None)], [3], [(None, None)],
                         ['explicit', 'implicit'], ['repeated']]     # -> 4
        permutationsBranchIndices = list(itertools.product(*branchIndices))
        modifiedIndices = [[0], [100], [1000000], [4320000], [(None, None)], [None], [(None, 5), (5, 5)],
                           ['explicit', 'implicit'], ['repeated', 'related']]     # -> 16
        permutationsModifiedIndices = list(itertools.product(*modifiedIndices))

        # permutationsIndices = permutationsSnapshotIndices + permutationsBranchIndices + permutationsModifiedIndices

        permutationsIndices = [(0, 100, 1000000, 4320000, (None, None), None, (None, None), 'implicit', 'repeated')]
        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seed=indices[0], closeness=indices[2], width=indices[3], snapshot=indices[4],
                                        triplesPerUpdate=indices[1], branch=indices[5], modifiedUpdate=indices[6],
                                        reference=indices[7], content=indices[8])
            print("config.REFERENCE_EXPLICIT ", config.REFERENCE_EXPLICIT)
            print("config.REFERENCE_IMPLICIT ", config.REFERENCE_IMPLICIT)

            if not os.path.isfile(config.revision_store_file_name):
                args = get_default_configuration()
                args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                             'implicit': config.REFERENCE_IMPLICIT}
                args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

                application = create_app(args).test_client()

                store = StoreCreator(application=application, config=config,
                                     modificationsFolder=config.raw_change_data_dir,
                                     updateDataFile=config.updates_file_name,
                                     revisionStoreFileName=config.revision_store_file_name,
                                     ingestionResultsFileName=config.ingestion_results_file_name)
                store.set_up_revision_store()
                store.reset_revision_store()

    if evaluateQueries:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        snapshotIndices = [[0], [10, 100], [1000000, 10000000], [432000, 4320000], [(None, None), ('F', 30), ('N', 30)],
                           [None], [(None, None)], ['explicit', 'implicit'], ['repeated'], ['all', 'specific']]  # -> 96
        permutationsSnapshotIndices = list(itertools.product(*snapshotIndices))
        branchIndices = [[0], [10, 100], [1000000], [4320000], [(None, None)], [3], [(None, None)],
                         ['explicit', 'implicit'], ['repeated'], ['specific']]     # -> 4
        permutationsBranchIndices = list(itertools.product(*branchIndices))
        modifiedIndices = [[0], [10, 100], [1000000], [4320000], [(None, None)], [None], [(None, 5), (5, 5)],
                           ['explicit', 'implicit'], ['repeated', 'related'], ['all', 'specific']]     # -> 32
        permutationsModifiedIndices = list(itertools.product(*modifiedIndices))

        # permutationsIndices = permutationsSnapshotIndices + permutationsBranchIndices + permutationsModifiedIndices
        permutationsIndices = [(0, 100, 1000000, 432000, (None, None), None, (None, None), 'explicit', 'repeated',
                                'specific')]
        for indices in permutationsIndices:
            print("indices ", indices)

            if indices[9] == 'all':
                nOfQueries = 5
            else:
                nOfQueries = None

            config = BearBConfiguration(seed=indices[0], closeness=indices[2], width=indices[3], snapshot=indices[4],
                                        triplesPerUpdate=indices[1], branch=indices[5], modifiedUpdate=indices[6],
                                        reference=indices[7], fetching=indices[9], content=indices[8],
                                        numberOfQueries=nOfQueries)

            if os.path.isfile(config.revision_store_file_name):
                args = get_default_configuration()
                args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                             'implicit': config.REFERENCE_IMPLICIT}
                args['fetchingStrategy'] = {'queryAllUpdates': config.FETCHING_ALL,
                                            'querySpecificUpdates': config.FETCHING_SPECIFIC}
                args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

                application = create_app(args).test_client()

                evaluator = Evaluator(config=config, application=application,
                                      revisionStoreFileName=config.revision_store_file_name,
                                      queryResultsFileName=config.query_results_file_name)
                evaluator.set_up_revision_store()
                evaluator.evaluate()
                evaluator.reset_revision_store()
