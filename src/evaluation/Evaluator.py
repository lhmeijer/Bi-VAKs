import json
import re
from rdflib import URIRef, Literal
import numpy as np
from .preprocess_BEAR_B import get_queries_from_nt_file


class Evaluator(object):

    def __init__(self, application, config):
        self._application = application
        self._config = config

        self._queries = get_queries_from_nt_file(self._config.bear_queries_file_name)

    def evaluate(self):
        """

        :return:
        """
        timePerQuery = []
        triplesPerQuery = []

        results = {}

        for queryIndex, query in self._queries.items():
            func = getattr(self, '_evaluate_{0}_query'.format(self._config.QUERY_ATOM.lower()))
            time, triples = func(queryIndex, query)
            timePerQuery.append(time)
            triplesPerQuery.append(triples)
            results['query-{0}'.format(queryIndex)] = {'time': time, 'triples': triples}

        numpyTimePerQuery = np.array(timePerQuery)
        numpyTriplesPerQuery = np.array(triplesPerQuery)

        meanTimePerVersion = np.mean(numpyTimePerQuery, axis=0)
        results['MeanTimePerVersion'] = meanTimePerVersion.tolist()
        standardDeviationTimePerVersion = np.std(numpyTimePerQuery, axis=0)
        results['StandardDeviationTimePerVersion'] = standardDeviationTimePerVersion.tolist()

        meanTriplesPerQuery = np.mean(numpyTriplesPerQuery, axis=0)
        results['MeanTriplesPerVersion'] = meanTriplesPerQuery.tolist()
        standardDeviationTriplesPerQuery = np.std(numpyTriplesPerQuery, axis=0)
        results['StandardDeviationTriplesPerVersion'] = standardDeviationTriplesPerQuery.tolist()

        with open(self._config.query_results_file_name, 'w') as file:
            json.dump(results, file)

    def _evaluate_vm_query(self, queryIndex, query):
        trueResults = self._extract_vm_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))

        time = []
        totalNumberOfTriples = []

        for i in range(self._config.NUMBER_OF_VERSIONS + 1):
            results = self._application.get('/query', query_string=dict(
                query=query.to_select_query(), queryAtomType='VM', tag='version {0}'.format(i)),
                                            headers=dict(accept="application/sparql-results+json"))

            # Obtain the number of triples it needed to construct the given version.
            numberOfTriples = results.headers['N-ProcessedQuads']
            totalNumberOfTriples.append(numberOfTriples)

            jsonResults = json.loads(results.read().decode("utf-8"))
            for jsonResult in jsonResults['results']['bindings']:
                try:
                    self._compare_results(jsonResult, trueResults[i])
                except Exception:
                    raise Exception

        return time, totalNumberOfTriples

    def _evaluate_dm_query(self, queryIndex, query):

        trueResults = self._extract_dm_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))
        jumps = list(range(0, self._config.NUMBER_OF_VERSIONS, 5)) + [self._config.NUMBER_OF_VERSIONS]

        time = []
        totalNumberOfTriples = []

        for i in jumps:
            results = self._application.get('/query', query_string=dict(
                query=query.to_select_query(), queryAtomType='DM', tagA='version 0', tagB='version {0}'.format(i)),
                               headers=dict(accept="application/sparql-results+json"))

            # Obtain the number of triples it needed to obtain the insertions and deletions between to versions.
            numberOfTriples = results.headers['N-ProcessedQuads']
            totalNumberOfTriples.append(numberOfTriples)

            jsonResults = json.loads(results.read().decode("utf-8"))

            for jsonResult in jsonResults['results']['insertions']:
                try:
                    self._compare_results(jsonResult, trueResults[i]['insertions'])
                except Exception:
                    raise Exception

            for jsonResult in jsonResults['results']['deletions']:
                try:
                    self._compare_results(jsonResult, trueResults[i]['deletions'])
                except Exception:
                    raise Exception

        return time, totalNumberOfTriples

    def _evaluate_vq_query(self, queryIndex, query):
        realResults = self._extract_vq_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))

        time = []

        results = self._application.get('/query', query_string=dict(query=query.to_select_query(), queryAtomType='VQ'),
                                        headers=dict(accept="application/sparql-results+json"))

        # Obtain the number of triples it needed to obtain all versions which give an answer for query x.
        numberOfTriples = results.headers['N-ProcessedQuads']

        jsonResults = json.loads(results.read().decode("utf-8"))

        for variableName, variableResult in jsonResults.items():
            if variableResult['value'] not in realResults:
                raise Exception

        return time, numberOfTriples

    def _compare_results(self, jsonResult, trueResults):
        result = []

        for variableName, variableResult in jsonResult.items():
            if variableResult['type'] == 'uri':
                result.append(URIRef(variableResult['value']))
            elif variableResult['type'] == 'literal':
                result.append(Literal(variableResult['value']))

        stringResult = ' '.join(term.n3() for term in result)

        try:
            trueResults.remove(stringResult)
        except ValueError:
            raise Exception  # or scream: thing not in some_list!

        if len(trueResults) > 0:
            raise Exception

    @staticmethod
    def _extract_vm_results_from_file(fileName):

        results = {}
        with open(fileName, "r") as file:
            for line in file:
                stringWithinBrackets = re.search(r"\[.*?]", line).group(0)
                versionNumber = int(re.findall(r'\d', stringWithinBrackets)[0])
                resultString = line.replace(stringWithinBrackets, '')

                if versionNumber not in results:
                    results[versionNumber] = [resultString]
                else:
                    results[versionNumber].append(resultString)
        return results

    @staticmethod
    def _extract_dm_results_from_file(fileName):
        results = {}
        with open(fileName, "r") as file:
            for line in file:

                stringWithinBrackets = re.search(r"\[.*?]", line).group(0)
                versionNumber = int(re.findall(r'\d', stringWithinBrackets)[0])
                resultString = line.replace(stringWithinBrackets, '')

                if versionNumber not in results:
                    results[versionNumber] = {'insertions': [], 'deletions': []}

                if 'ADD' in stringWithinBrackets:
                    results[versionNumber]['insertions'].append(resultString)
                elif 'DEL' in stringWithinBrackets:
                    results[versionNumber]['deletions'].append(resultString)

        return results

    @staticmethod
    def _extract_vq_results_from_file(fileName):
        return Evaluator._extract_vm_results_from_file(fileName)




