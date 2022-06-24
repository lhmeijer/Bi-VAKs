from src.main.bitr4qs.configuration import get_default_configuration
from src.evaluation.StoreCreator import StoreCreator
from src.evaluation.Evaluator import Evaluator
import itertools
from src.evaluation.preprocess_BEAR_B import ChangeComputer
from datetime import datetime
from src.evaluation.BearAConfiguration import BearAConfiguration
import os
from src.evaluation.UpdateGenerator import UpdateGenerator
from src.main.bitr4qs.webservice.app import create_app
import subprocess

if __name__ == "__main__":

    computeChangeDataset = False
    generateUpdates = False
    createStore = True
    evaluateQueries = False

    if computeChangeDataset:
        config = BearAConfiguration()
        changeComputer = ChangeComputer(config, inputFolder=config.raw_version_data_dir,
                                        exportFolder=config.raw_change_data_dir)
        changeComputer.compute_changes()

    if generateUpdates:

        possibleIndices = [[0], [100, 1000], [(1000000, 4320000), (5000000, 432000)]]  # ->  2 x 2 = 4
        permutationsIndices = list(itertools.product(*possibleIndices))

        for indices in permutationsIndices:
            print("indices ", indices)
            config = BearAConfiguration(seed=indices[0], closeness=indices[2][0], width=indices[2][1],
                                        triplesPerUpdate=indices[1])
            if not os.path.isfile(config.updates_file_name):
                updateGenerator = UpdateGenerator(config=config, inputFolder=config.raw_change_data_dir,
                                                  exportFileName=config.updates_file_name,
                                                  statisticsFileName=config.statistics_updates_file_name)
                updateGenerator.generate_updates()

    if createStore:
        subprocess.run(['python', 'StoreCreator.py', '0'])