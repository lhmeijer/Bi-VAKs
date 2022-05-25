from src.evaluation.configuration import BearBConfiguration
import os
from src.evaluation.UpdateGenerator import UpdateGenerator
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
from src.evaluation.StoreCreator import StoreCreator
from src.evaluation.Evaluator import Evaluator


if __name__ == "__main__":

    generateUpdates = True
    createStore = False
    evaluateQueries = False

    config = BearBConfiguration

    if generateUpdates and not os.path.isfile(config.updates_file_name):
        updateGenerator = UpdateGenerator(
            config=config, inputFolder=config.raw_data_dir, exportFileName=config.updates_file_name,
            statisticsFileName=config.statistics_updates_file_name)
        updateGenerator.generate_updates()

    # args = get_default_configuration()
    # args['referenceStrategy'] = {'explicit': config.REFERENCE_EXPLICIT, 'implicit': config.REFERENCE_IMPLICIT}
    # args['fetchingStrategy'] = {'queryAllUpdates': config.FETCHING_ALL, 'querySpecificUpdates': config.FETCHING_SPECIFIC}
    # args['UpdateContentStrategy'] = {'repeated': config.CONTENT_REPEATED, 'related': config.CONTENT_RELATED}
    #
    # application = create_app(args).run(host=args['host'], port=args['port'])
    #
    # # Determine the number of triples in the revision store.
    # nOfTriples = ...
    #
    # if nOfTriples == 0 and createStore:
    #     store = StoreCreator(application=application, config=config, modificationsFolder=config.processed_data_dir,
    #                          updateDataFile=config.updates_file_name)
    #     store.set_up_revision_store()
    #
    # if evaluateQueries:
    #     evaluator = Evaluator(config=config, application=application)
    #     evaluator.evaluate()
