from src.evaluation.configuration import BearBConfiguration
import os
from src.evaluation.UpdateGenerator import UpdateGenerator
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
from src.evaluation.StoreCreator import StoreCreator
from src.evaluation.Evaluator import Evaluator
import itertools


if __name__ == "__main__":

    generateUpdates = False
    createStore = False
    evaluateQueries = True

    possibleIndices = [[0], [0, 1, 2, 3], [0, 1], [0, 1]]
    permutationsIndices = list(itertools.product(*possibleIndices))

    if generateUpdates:
        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearBConfiguration(seedIndex=indices[0], indexCloseness=indices[2], indexWidth=indices[3],
                                        triplesPerUpdateIndex=indices[1])
            if not os.path.isfile(config.updates_file_name):
                updateGenerator = UpdateGenerator(config=config, inputFolder=config.raw_data_dir,
                                                  exportFileName=config.updates_file_name,
                                                  statisticsFileName=config.statistics_updates_file_name)
                updateGenerator.generate_updates()

    if createStore or evaluateQueries:

        config = BearBConfiguration(triplesPerUpdateIndex=3, indexCloseness=1, indexWidth=1)

        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT, 'implicit': config.REFERENCE_IMPLICIT}
        args['fetchingStrategy'] = {'queryAllUpdates': config.FETCHING_ALL, 'querySpecificUpdates': config.FETCHING_SPECIFIC}
        args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}

        application = create_app(args).test_client()

        # Determine the number of triples in the revision store.
        nOfTriples = 0

        if nOfTriples == 0 and createStore:
            store = StoreCreator(application=application, config=config, modificationsFolder=config.raw_data_dir,
                                 updateDataFile=config.updates_file_name)
            store.set_up_revision_store()

        if evaluateQueries:
            evaluator = Evaluator(config=config, application=application)
            evaluator.evaluate()
