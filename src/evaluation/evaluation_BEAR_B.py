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
    generateUpdates = True
    createStore = False
    evaluateQueries = False

    if computeChanges:
        config = BearBConfiguration()
        changeComputer = ChangeComputer(config, inputFolder=config.raw_version_data_dir,
                                        exportFolder=config.raw_change_data_dir)
        changeComputer.compute_changes()

    if generateUpdates:

        possibleIndices = [[0], [0, 1], [0, 1], [0, 1]]
        permutationsIndices = list(itertools.product(*possibleIndices))

        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seedIndex=indices[0], indexCloseness=indices[2], indexWidth=indices[3],
                                        triplesPerUpdateIndex=indices[1])
            if not os.path.isfile(config.updates_file_name):
                updateGenerator = UpdateGenerator(config=config, inputFolder=config.raw_change_data_dir,
                                                  exportFileName=config.updates_file_name,
                                                  statisticsFileName=config.statistics_updates_file_name)
                updateGenerator.generate_updates()

    if createStore:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        snapshotIndices = [[0], [10, 100], [0, 1], [0, 1], [None, 'N', 'F'], [None], [0], [0, 1], [0]]       # -> 48
        permutationsSnapshotIndices = list(itertools.product(*snapshotIndices))
        branchIndices = [[0], [0, 1], [0], [1], [0], [1], [0], [0, 1], [0]]     # -> 4
        permutationsBranchIndices = list(itertools.product(*branchIndices))
        modifiedIndices = [[0], [0, 1], [0], [1], [0], [0], [1, 2], [0, 1], [0, 1]]     # -> 16
        permutationsModifiedIndices = list(itertools.product(*modifiedIndices))

        permutationsIndices = snapshotIndices + branchIndices + modifiedIndices
        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seedIndex=indices[0], indexCloseness=indices[2], indexWidth=indices[3],
                                        triplesPerUpdateIndex=indices[1], snapshotIndex=indices[4],
                                        branchIndex=indices[5], modifiedUpdateIndex=indices[6],
                                        referenceIndex=indices[7], contentIndex=indices[8])

            if not os.path.isfile(config.revision_store_file_name):
                args = get_default_configuration()
                args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                             'implicit': config.REFERENCE_IMPLICIT}
                args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

                application = create_app(args).test_client()

                store = StoreCreator(application=application, config=config,
                                     modificationsFolder=config.raw_change_data_dir,
                                     updateDataFile=config.updates_file_name)
                store.reset_revision_store()

    if evaluateQueries:
        # seed, numberUpdates, indexCloseness, indexWidth, snapshots, branches, modifiedUpdates, reference, content
        snapshotIndices = [[0], [0, 1], [0, 1], [0, 1], [0, 1, 2], [0], [0], [0, 1], [0]]       # -> 48
        permutationsSnapshotIndices = list(itertools.product(*snapshotIndices))
        branchIndices = [[0], [0, 1], [0], [1], [0], [1], [0], [0, 1], [0]]     # -> 4
        permutationsBranchIndices = list(itertools.product(*branchIndices))
        modifiedIndices = [[0], [0, 1], [0], [1], [0], [0], [1, 2], [0, 1], [0, 1]]     # -> 16
        permutationsModifiedIndices = list(itertools.product(*modifiedIndices))

        permutationsIndices = snapshotIndices + branchIndices + modifiedIndices
        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seedIndex=indices[0], indexCloseness=indices[2], indexWidth=indices[3],
                                        triplesPerUpdateIndex=indices[1], snapshotIndex=indices[4],
                                        branchIndex=indices[5], modifiedUpdateIndex=indices[6],
                                        referenceIndex=indices[7], contentIndex=indices[8])
                # TODO set up de revision store from a
            if os.path.isfile(config.revision_store_file_name):
                args = get_default_configuration()
                args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT,
                                             'implicit': config.REFERENCE_IMPLICIT}
                args['fetchingStrategy'] = {'queryAllUpdates': config.FETCHING_ALL,
                                            'querySpecificUpdates': config.FETCHING_SPECIFIC}
                args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

                application = create_app(args).test_client()

                evaluator = Evaluator(config=config, application=application,
                                      revisionStoreFileName=config.revision_store_file_name)
                evaluator.set_up_revision_store()
                evaluator.evaluate()
                evaluator.reset_revision_store()
