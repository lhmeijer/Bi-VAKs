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

        timePerVersion.extend(value['time'])

        results['processedTriples'] = value['triples']

    print(len(timePerVersion))
    print(len(timePerVersion[0]))

    # results['meanTime'] = np.median(np.array(timePerVersion), axis=0)
    results['meanTime'] = np.percentile(np.array(timePerVersion), 50, axis=0)
    results['stdTime'] = np.std(np.array(timePerVersion), axis=0)
    results['10quantileTime'] = np.percentile(np.array(timePerVersion), 25, axis=0)
    results['90quantileTime'] = np.percentile(np.array(timePerVersion), 75, axis=0)

    if computeTimeGrowth:
        meanTimeWithMinimumTriples = np.mean(np.array(timeWithMinimumTriples))
        timeGrowthPerQuery = []
        for queryNumber, value in resultsPerQuery.items():
            timeGrowth = np.zeros(len(value['triples']))
            indices = np.argwhere(np.array(value['triples']) > minimumNumberOfTriples).flatten()
            timeGrowth[indices] = ((np.mean(np.array(value['time']), axis=0)[indices] - meanTimeWithMinimumTriples) /
                                   (np.array(value['triples'])[indices])) * 100
            timeGrowthPerQuery.append(timeGrowth)

        results['meanTimeGrowth'] = np.median(np.array(timeGrowthPerQuery), axis=0)
        results['stdTimeGrowth'] = np.std(np.array(timeGrowthPerQuery), axis=0)
        results['10quantileTimeGrowth'] = np.percentile(np.array(timeGrowthPerQuery), 25, axis=0)
        results['90quantileTimeGrowth'] = np.percentile(np.array(timeGrowthPerQuery), 75, axis=0)


def load_ingestion_time(fileName, results):
    with open(fileName, 'r') as file:
        jsonResults = json.load(file)

    ingestionTimes = []
    cumulativeIngestionTime = []
    processedTriples = []
    for jsonResult in jsonResults:
        ingestionTimes.append(jsonResult['IngestionTimeUpdates'])
        cumulativeIngestionTime.append(np.cumsum(jsonResult['IngestionTimeUpdates']))
        processedTriples.append(jsonResult['NumberOfProcessedQuads'])

    results['meanIngestionTime'] = np.median(np.array(ingestionTimes), axis=0)
    results['stdIngestionTime'] = np.std(np.array(ingestionTimes), axis=0)
    results['25quantileIngestionTime'] = np.quantile(np.array(ingestionTimes), 0.25, axis=0)
    results['75quantileIngestionTime'] = np.quantile(np.array(ingestionTimes), 0.75, axis=0)
    results['meanCumulativeIngestionTime'] = np.mean(np.array(cumulativeIngestionTime), axis=0)
    results['stdCumulativeIngestionTime'] = np.std(np.array(cumulativeIngestionTime), axis=0)
    results['processedTriples'] = np.mean(np.array(processedTriples), axis=0)


def plot_query_time(data, fileName, numberOfTriples=False):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    if numberOfTriples:
        ax2 = ax.twinx()
        ax2.set_ylabel('Processed triples', fontsize=14)
        ax2.set_ylim([0, 140000])

    for values in data:

        y = values['meanTime']
        print(y)
        z = values['stdTime']
        x = list(range(1, len(y) + 1))
        # x = list(range(5, 89, 5)) + [89]
        # print(x)

        ax.plot(x, y, label=values['name'], color=values['color'])
        # ax.fill_between(x, y-z, y+z, color=values['color'], alpha=0.1)
        ax.fill_between(x, values['10quantileTime'], values['90quantileTime'], color=values['color'], alpha=0.1)

        if numberOfTriples:
            y = values['processedTriples']
            ax2.fill_between(x, y, color=values['color'], label='_nolegend_', alpha=0.1)
            # ax2.plot(x, y, color=values['color'], linestyle='dashed')

    ax.set_ylabel('Lookup time (sec)', fontsize=14)
    plt.xlabel('Version', fontsize=14)
    plt.xlim([1, 89])
    # plt.xlim([5, 89])
    # plt.ylim([0.1, 0.4])
    # plt.legend(loc='upper left', fontsize='large', ncol=3)
    ax.legend(loc='upper left', fontsize='large', ncol=3)
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
        ax.fill_between(x, values['10quantileTimeGrowth'], values['90quantileTimeGrowth'], color=values['color'],
                        alpha=0.1)
        # ax.fill_between(x, y-z, y+z, color=values['color'], alpha=0.1)

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
        print(y)
        z = values['stdIngestionTime']
        print(z)
        x = values['processedTriples']

        ax.plot(x, y, label=values['name'], color=values['color'])
        # ax.fill_between(x, values['25quantileIngestionTime'], values['75quantileIngestionTime'], color=values['color'],
        #                 alpha=0.25)
        ax.fill_between(x, y - z, y + z, color=values['color'], alpha=0.25)

        indices = [26, 298, 461, 744, 1017, 1252, 1508, 1727]
        jumps = [0, 2, 4, 6]
        for i in jumps:
            ax.axvspan(x[indices[i]], x[indices[i + 1]], color=values['color'], alpha=0.1)
            # ax.axvline(x=x[i])

    # ax.set_ylabel('Cumulative ingestion time (sec)', fontsize=14)
    ax.set_ylabel('Ingestion time (sec)', fontsize=14)
    plt.xlabel('Processed triples', fontsize=14)
    plt.legend(loc='upper left', fontsize='large', ncol=3)
    plt.tight_layout()
    plt.xlim([30000, 163500])
    plt.savefig(fileName)
    plt.close()


def plot_version_query_time(data, fileName):
    # versionQueryMeansExplicit, versionQueryMeansImplicit = [], []
    # versionQueryStdsExplicit, versionQueryStdsImplicit= [], []
    # versionQueryNames = []
    # for values in data:
    #     versionQueryMeansExplicit.append(values['explicit']['meanTime'])
    #     versionQueryStdsExplicit.append(values['explicit']['std'])
    #     versionQueryMeansImplicit.append(values['implicit']['meanTime'])
    #     versionQueryStdsImplicit.append(values['implicit']['std'])
    #     versionQueryNames.append(values['name'])


    fig = plt.figure()
    ax = fig.add_subplot(111)

    versionQueryNames = []
    index = 0
    for values in data:
        x = np.arange(index, 9, 3)
        print(x)
        queryMeans = []
        queryStds = []
        for i in range(1, 4):
            queryMeans.append(float(values[i]['meanTime']))
            queryStds.append(float(values[i]['stdTime']))
            if len(versionQueryNames) < 3:
                versionQueryNames.append(values[i]['name'])
        print("queryMeans ", queryMeans)
        print("queryStds ", queryStds)

        ax.bar(x, queryMeans, yerr=queryStds, label=values['name'], color=values['color'])
        index += 1

        print(values)
    print("versionQueryNames ", versionQueryNames)

    # ax.bar(x, versionQueryMeansExplicit, yerr=versionQueryStdsExplicit, label='explicit', color='blue')
    # ax.bar(x, versionQueryMeansImplicit, yerr=versionQueryStdsImplicit, label='implicit', color='red')

    ax.set_ylabel('Lookup time (sec)', fontsize=14)
    x = np.arange(1, 9, 3)
    ax.set_xticks(x)
    ax.set_xticklabels(versionQueryNames)

    plt.legend(loc='upper left', fontsize='large', ncol=3)
    plt.tight_layout()
    plt.savefig(fileName)
    plt.close()


if __name__ == "__main__":
    # dataPoints = [('explicit', 'blue', 'explicit'), ('implicit', 'red', 'implicit'), ('combined', 'green', 'combined')]
    #
    # allResults = []
    # for points in dataPoints:
    #     result = {'name': points[0], 'color': points[1]}
    #
    #     config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None), reference=points[2],
    #                                 triplesPerUpdate=50, branch=None, modifiedUpdate=(None, None), fetching='all',
    #                                 content='repeated', modifications='aggregated', retrieve='between')
    #
    #     print(config.query_results_file_name)
    #     load_query_time(config.query_results_file_name, result)
    #     allResults.append(result)
    #
    # plot_query_time(allResults, config.figures_query_results, numberOfTriples=True)

    # dataPoints = [('explicit', 'blue', 'explicit'), ('implicit', 'red', 'implicit'), ('combined', 'green', 'combined')]
    #
    # allResults = []
    # for points in dataPoints:
    #     result = {'name': points[0], 'color': points[1]}
    #
    #     config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=('N', 45), reference=points[2],
    #                                 triplesPerUpdate=50, branch=3, modifiedUpdate=(None, 5), content='related')
    #
    #     print(config.ingestion_results_file_name)
    #     load_ingestion_time(config.ingestion_results_file_name, result)
    #     allResults.append(result)
    #
    # plot_ingestion_time(allResults, config.figures_ingestion_results)

    dataPoints = [('explicit', 'blue', 'explicit'), ('implicit', 'red', 'implicit'), ('combined', 'green', 'combined')]

    allResults = []
    for points in dataPoints:
        # result = {'name': points[0], 'color': points[1], 1: {'name': '50-LC-HW-FS-CP-AG'},
        #           2: {'name': '50-HC-LW-FS-CP-AG'}, 3: {'name': '100-LC-HW-FS-CP-AG'}, 6: {'name': '50-LC-HW-FS-CP-SO'},
        #           4: {'name': '50-LC-HW-FS-CP-M5-AG'}, 5: {'name': '50-LC-HW-FS-CP-B3-AG'}}

        result = {'name': points[0], 'color': points[1], 1: {'name': '50-LC-HW-FS-CP-AG'},
                  2: {'name': '50-LC-HW-FS-CP-SO'}, 3: {'name': '50-HC-LW-FS-CP-SO'}}
        # 4: {'name': '50-LC-HW-FS-CP-M5-SO'}, 5: {'name': '50-LC-HW-FS-CP-B3-SO'}

        config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None),
                                    reference=points[2],
                                    triplesPerUpdate=50, branch=None, modifiedUpdate=(None, None), fetching='specific',
                                    content='repeated', modifications='aggregated', retrieve='between')

        print(config.query_results_file_name)
        load_query_time(config.query_results_file_name, result[1])

        config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None),
                                    reference=points[2],
                                    triplesPerUpdate=50, branch=None, modifiedUpdate=(None, None), fetching='specific',
                                    content='repeated', modifications='sorted', retrieve='initial')

        print(config.query_results_file_name)
        load_query_time(config.query_results_file_name, result[2])

        config = BearBConfiguration(seed=0, closeness=5000000, width=432000, snapshot=(None, None), reference=points[2],
                                    triplesPerUpdate=50, branch=None, modifiedUpdate=(None, None), fetching='specific',
                                    content='repeated', modifications='sorted', retrieve='initial')

        print(config.query_results_file_name)
        load_query_time(config.query_results_file_name, result[3])

        # config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None),
        #                             reference=points[2],
        #                             triplesPerUpdate=100, branch=None, modifiedUpdate=(None, None), fetching='specific',
        #                             content='repeated', modifications='aggregated', retrieve='between')
        #
        # print(config.query_results_file_name)
        # load_query_time(config.query_results_file_name, result[3])

        # config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None),
        #                             reference=points[2],
        #                             triplesPerUpdate=50, branch=None, modifiedUpdate=(None, 5), fetching='specific',
        #                             content='repeated', modifications='sorted', retrieve='initial')
        #
        # print(config.query_results_file_name)
        # load_query_time(config.query_results_file_name, result[4])
        #
        # config = BearBConfiguration(seed=0, closeness=1000000, width=4320000, snapshot=(None, None),
        #                             reference=points[2],
        #                             triplesPerUpdate=50, branch=3, modifiedUpdate=(None, None), fetching='specific',
        #                             content='repeated', modifications='sorted', retrieve='initial')
        #
        # print(config.query_results_file_name)
        # load_query_time(config.query_results_file_name, result[5])

        allResults.append(result)

    plot_version_query_time(allResults, config.figures_query_results)
