import numpy as np
from matplotlib import pyplot as plt
from src.evaluation.configuration import BearBConfiguration
import itertools
import json


def load_query_time(fileName, results, computeTimeGrowth=True):
    resultsPerQuery = {}
    minimumNumberOfTriples = None
    with open(fileName, 'r') as file:
        for line in file:
            data = line.strip().split(',')
            metaData = data[1].split('-')
            queryNumber = metaData[1]

            if queryNumber not in resultsPerQuery:
                resultsPerQuery[queryNumber] = {'time': [], 'triples': []}

            if metaData[2] == 'time':
                resultsPerQuery[queryNumber]['time'].append([float(point) for point in data[2:]])
            elif metaData[2] == 'triples' and len(resultsPerQuery[queryNumber]['triples']) == 0:
                resultsPerQuery[queryNumber]['triples'].extend([int(point) for point in data[2:]])

                minimumInTriples = np.min(resultsPerQuery[queryNumber]['triples'])
                if minimumNumberOfTriples is None or minimumInTriples < minimumNumberOfTriples:
                    minimumNumberOfTriples = minimumInTriples

    timeWithMinimumTriples = []
    timePerVersion = []
    for queryNumber, value in resultsPerQuery.items():
        indices = (np.argwhere(np.array(value['triples']) == minimumNumberOfTriples)).flatten()
        lengthOfIndices = len(indices)
        minLengthOfIndices = lengthOfIndices if lengthOfIndices < 10 else 10
        timeWithMinimumTriples.extend(list(
            np.take(np.array(value['time']), indices[:minLengthOfIndices], axis=1).flatten()))

        timePerVersion.append(np.mean(np.array(value['time']), axis=0))

        if not computeTimeGrowth:
            results['processedTriples'] = value['triples']

    results['meanTime'] = np.mean(np.array(timePerVersion), axis=0)
    results['stdTime'] = np.std(np.array(timePerVersion), axis=0)

    if computeTimeGrowth:
        meanTimeWithMinimumTriples = np.mean(np.array(timeWithMinimumTriples))
        timeGrowthPerQuery = []
        for queryNumber, value in resultsPerQuery.items():
            timeGrowth = np.zeros(len(value['triples']))
            indices = np.argwhere(np.array(value['triples']) > minimumNumberOfTriples).flatten()
            timeGrowth[indices] = ((np.mean(np.array(value['time']), axis=0)[indices] - meanTimeWithMinimumTriples) /
                                   (np.array(value['triples'])[indices])) * 10
            timeGrowthPerQuery.append(timeGrowth)

        results['meanTimeGrowth'] = np.mean(np.array(timeGrowthPerQuery), axis=0)
        results['stdTimeGrowth'] = np.std(np.array(timeGrowthPerQuery), axis=0)


def load_ingestion_time(fileName, results):

    with open(fileName, 'r') as file:
        jsonResults = json.load(file)

    ingestionTimes = []
    processedTriples = []
    for jsonResult in jsonResults:
        ingestionTimes.append(jsonResult['IngestionTimeUpdates'])
        processedTriples.append(jsonResult['NumberOfProcessedQuads'])

    results['meanIngestionTime'] = np.mean(np.array(ingestionTimes), axis=0)
    results['stdIngestionTime'] = np.std(np.array(ingestionTimes), axis=0)
    results['processedTriples'] = np.std(np.array(processedTriples), axis=0)


def plot_query_time(data, fileName, numberOfTriples=False):

    fig = plt.figure()
    ax = fig.add_subplot(111)

    if numberOfTriples:
        ax2 = ax.twinx()
        ax2.set_ylabel('Processed triples')
        ax2.set_ylim([0, 140000])

    for values in data:

        y = values['meanTime']
        z = values['stdTime']
        x = list(range(1, len(y) + 1))

        ax.plot(x, y, label=values['name'], color=values['color'])
        ax.fill_between(x, y-z, y+z, color=values['color'], alpha=0.1)

        if numberOfTriples:
            y = values['processedTriples']
            ax2.plot(x, y, color=values['color'], linestyle='dashed')

    ax.set_ylabel('Lookup time (sec)', fontsize=14)
    plt.xlabel('Version', fontsize=14)
    plt.xlim([1, 89])
    plt.legend(loc='upper left', fontsize='large', ncol=2)
    plt.tight_layout()
    plt.savefig(fileName)
    plt.close()


def plot_time_growth(data, fileName):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    for values in data:

        y = values['meanTimeGrowth']
        z = values['stdTimeGrowth']
        x = list(range(1, len(y) + 1))

        ax.plot(x, y, label=values['name'], color=values['color'])
        ax.fill_between(x, y-z, y+z, color=values['color'], alpha=0.1)

    ax.set_ylabel('Growth in time (per 10 triples)', fontsize=14)
    plt.xlabel('Version', fontsize=14)
    plt.xlim([1, 89])
    plt.legend(loc='upper left', fontsize='large', ncol=2)
    plt.tight_layout()
    plt.savefig(fileName)
    plt.close()


def plot_ingestion_time(data, fileName):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    for values in data:
        y = values['meanIngestionTime']
        z = values['stdIngestionTime']
        x = values['processedTriples']

        ax.plot(x, y, label=values['name'], color=values['color'])
        ax.fill_between(x, y - z, y + z, color=values['color'], alpha=0.1)

        jumps1 = list(range(1, len(x), 2))
        for i in jumps1:
            ax.axvspan(x[i], ax[i+1], color=values['color'], alpha=0.1)

    ax.set_ylabel('Cumulative ingestion time (sec)', fontsize=14)
    plt.xlabel('Processed triples', fontsize=14)
    plt.legend(loc='upper left', fontsize='large', ncol=2)
    plt.tight_layout()
    plt.savefig(fileName)
    plt.close()


def plot_version_query_time(data, fileName):

    versionQueryMeansExplicit, versionQueryMeansImplicit = [], []
    versionQueryStdsExplicit, versionQueryStdsImplicit= [], []
    versionQueryNames = []
    for values in data:
        versionQueryMeansExplicit.append(values['explicit']['meanTime'])
        versionQueryStdsExplicit.append(values['explicit']['std'])
        versionQueryMeansImplicit.append(values['implicit']['meanTime'])
        versionQueryStdsImplicit.append(values['implicit']['std'])
        versionQueryNames.append(values['name'])

    x = np.arange(len(data))
    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.bar(x, versionQueryMeansExplicit, yerr=versionQueryStdsExplicit, label='explicit', color='blue')
    ax.bar(x, versionQueryMeansImplicit, yerr=versionQueryStdsImplicit, label='implicit', color='red')

    ax.set_ylabel('Lookup time (sec)', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(versionQueryNames)

    plt.legend(loc='upper left', fontsize='large', ncol=2)
    plt.tight_layout()
    plt.savefig(fileName)
    plt.close()


if __name__ == "__main__":
    dataPoints = [('explicit', 'blue', 'explicit'), ('implicit', 'red', 'implicit')]

    allResults = []
    for points in dataPoints:
        result = {'name': points[0], 'color': points[1]}

        config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None), reference=points[2],
                                    triplesPerUpdate=50, branch=None, modifiedUpdate=(None, 5), fetching='specific',
                                    content='related', modifications='aggregated', retrieve='between')

        print(config.query_results_file_name)
        load_query_time(config.query_results_file_name, result)
        allResults.append(result)

    plot_query_time(allResults, config.figures_query_results, numberOfTriples=False)

