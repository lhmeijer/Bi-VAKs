import json
import re
from rdflib import URIRef, Literal
import numpy as np
from .preprocess_BEAR_B import get_queries_from_nt_file
from datetime import datetime, timedelta
from timeit import default_timer as timer
from src.main.bitr4qs.term.TriplePattern import TriplePattern


class Evaluator(object):

    def __init__(self, application, config, revisionStoreFileName, queryResultsFileName):
        self._application = application
        self._config = config

        self._revisionStoreFileName = revisionStoreFileName
        self._queryResultsFileName = queryResultsFileName

        self._queries = get_queries_from_nt_file(self._config.bear_queries_file_name)
        self._nOfQueries = len(self._queries) if not self._config.NUMBER_OF_QUERIES else self._config.NUMBER_OF_QUERIES

    def set_up_revision_store(self):
        with open(self._revisionStoreFileName) as file:
            response = self._application.post('/upload', data=file.read(), headers=dict(accept="application/n-triples"))

    def reset_revision_store(self):
        try:
            self._application.delete('/reset')
        except Exception:
            raise Exception

    def evaluate(self):
        """

        :return:
        """
        timePerQuery = []
        triplesPerQuery = []

        for i in range(1, self._nOfQueries + 1):
        # for i in range(26, self._nOfQueries + 1):
            func = getattr(self, '_evaluate_{0}_query'.format(self._config.QUERY_ATOM.lower()))
            # time, triples = func(queryIndex, query)
            time, triples = func(i, self._queries[i])
            timePerQuery.append(time)
            triplesPerQuery.append(triples)

            with open(self._config.query_results_file_name, 'a') as file:
                file.write('query-{0}-time,{1}\n'.format(i, ','.join(str(t) for t in time)))
                file.write('query-{0}-triples,{1}\n'.format(i, ','.join(str(t) for t in triples)))
        print("timePerQuery ", timePerQuery)
        print("timePerQuery n ", len(timePerQuery))
        print("timePerQuery m ", len(timePerQuery[0]))
        numpyTimePerQuery = np.array(timePerQuery)
        numpyTriplesPerQuery = np.array(triplesPerQuery)

        meanTimePerVersion = np.mean(numpyTimePerQuery, axis=0)
        standardDeviationTimePerVersion = np.std(numpyTimePerQuery, axis=0)

        meanTriplesPerQuery = np.mean(numpyTriplesPerQuery, axis=0)
        standardDeviationTriplesPerQuery = np.std(numpyTriplesPerQuery, axis=0)

        with open(self._queryResultsFileName, 'a') as file:
            file.write('MEAN_TimePerVersion,{0}\n'.format(','.join(str(m) for m in meanTimePerVersion)))
            file.write('STANDARD_DEVIATION_TimePerVersion,{0}\n'.format(
                ','.join(str(s) for s in standardDeviationTimePerVersion)))
            file.write('MEAN_TriplesPerVersion,{0}\n'.format(','.join(str(m) for m in meanTriplesPerQuery)))
            file.write('STANDARD_DEVIATION_TriplesPerVersion,{0}\n'.format(
                ','.join(str(s) for s in standardDeviationTriplesPerQuery)))

    def _evaluate_vm_query(self, queryIndex, query):
        trueResults = self._extract_vm_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))

        time = []
        totalNumberOfTriples = []

        print("Query\n", query.select_query())
        for i in range(self._config.NUMBER_OF_VERSIONS):
            print("We query now version ", i+1)

            start = timer()
            results = self._application.get('/query', query_string=dict(
                query=query.select_query(), queryAtomType='VM', tag='version {0}'.format(i+1)),
                                            headers=dict(accept="application/sparql-results+json"))
            end = timer()
            time.append(float(timedelta(seconds=end - start).total_seconds()))
            # Obtain the number of triples it needed to construct the given version.
            numberOfTriples = results.headers['N-ProcessedQuads']
            print("numberOfTriples ", numberOfTriples)
            totalNumberOfTriples.append(int(numberOfTriples))

            jsonResults = json.loads(results.data.decode("utf-8"))
            print("jsonResults ", jsonResults)
            print("trueResults[i] ", trueResults[i])
            # for jsonResult in jsonResults['results']['bindings']:
            try:
                self._compare_results(jsonResults['results']['bindings'], trueResults[i])
            except Exception:
                raise Exception

        return time, totalNumberOfTriples

    def _evaluate_dm_query(self, queryIndex, query):

        trueResults = self._extract_dm_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))
        jumps = list(range(0, self._config.NUMBER_OF_VERSIONS, 5)) + [self._config.NUMBER_OF_VERSIONS]
        print("jumps ", jumps)
        time = []
        totalNumberOfTriples = []

        for i in jumps[1:]:
            print("We query now version 1 and version ", i)
            start = timer()
            results = self._application.get('/query', query_string=dict(
                query=query.select_query(), queryAtomType='DM', tagA='version 1', tagB='version {0}'.format(i)),
                               headers=dict(accept="application/sparql-results+json"))
            end = timer()
            time.append(float(timedelta(seconds=end - start).total_seconds()))

            # Obtain the number of triples it needed to obtain the insertions and deletions between to versions.
            numberOfTriples = results.headers['N-ProcessedQuads']
            totalNumberOfTriples.append(int(numberOfTriples))

            jsonResults = json.loads(results.data.decode("utf-8"))
            print("jsonResults ", jsonResults)
            print("trueResults[i] ", trueResults[i])
            try:
                self._compare_results(jsonResults['results']['insertions'], trueResults[i]['insertions'])
            except Exception:
                raise Exception

            try:
                self._compare_results(jsonResults['results']['deletions'], trueResults[i]['deletions'])
            except Exception:
                raise Exception

        return time, totalNumberOfTriples

    def _evaluate_vq_query(self, queryIndex, query):
        realResults = self._extract_vq_results_from_file('{0}-{1}.txt'.format(self._config.bear_results_dir, queryIndex))
        nOfVersionsInRealResults = np.count_nonzero(realResults == 1)
        print("nOfVersionsInRealResults ", nOfVersionsInRealResults)

        if self._config.VERSIONS_TO_BRANCH:
            branch = self._config.BRANCH
        else:
            branch = None

        time = []
        totalNumberOfTriples = []

        start = timer()
        results = self._application.get('/query', query_string=dict(
            query=query.select_query(), queryAtomType='VQ', branch=branch),
                                        headers=dict(accept="application/sparql-results+json"))
        end = timer()
        time.append(float(timedelta(seconds=end - start).total_seconds()))

        # Obtain the number of triples it needed to obtain all versions which give an answer for query x.
        numberOfTriples = results.headers['N-ProcessedQuads']
        totalNumberOfTriples.append(int(numberOfTriples))

        jsonResults = json.loads(results.data.decode("utf-8"))
        nOfVersions = 0

        for result in jsonResults['results']['bindings']:
            versionNumber = int(re.findall(r'\d+', result['tagName']['value'])[0]) - 1
            if realResults[versionNumber] == 1:
                nOfVersions += 1
            else:
                raise Exception("Version {0} should not contain any response.".format(versionNumber))
        print("nOfVersions ", nOfVersions)
        if nOfVersionsInRealResults != nOfVersions:
            raise Exception

        return time, totalNumberOfTriples

    @staticmethod
    def _compare_results(jsonResults, trueResults):

        results = set()
        for jsonResult in jsonResults:
            result = [None, None, None]
            for variableName, variableResult in jsonResult.items():
                index = 0 if variableName == '?s' else (1 if variableName == '?p' else 2)

                if variableResult['type'] == 'uri':
                    result[index] = URIRef(variableResult['value']).n3()
                elif variableResult['type'] == 'literal':
                    lang = None
                    datatype = 'http://www.w3.org/2001/XMLSchema#string'
                    if 'xml:lang' in variableResult:
                        lang = variableResult['xml:lang']
                        datatype = None
                    elif 'datatype' in variableResult:
                        datatype = variableResult['datatype']
                    result[index] = TriplePattern.quote_literal(
                        Literal(variableResult['value'], lang=lang, datatype=datatype))
            result = list(filter(None, result))
            # s = ' '.join(result).encode('utf-8').decode('us-ascii', errors='ignore').replace('?', '')
            s = ' '.join(result)
            results.add(s)

        differenceResults = results - trueResults
        print("results ", results)
        if len(differenceResults) > 0:
            print('differenceResults', differenceResults)
            print("List is not empty ", len(differenceResults))
            # raise Exception

    def _extract_vm_results_from_file(self, fileName):
        results = {}
        for i in range(self._config.NUMBER_OF_VERSIONS+1):
            results[i] = set()

        with open(fileName, "r") as file:
            lines = file.read().split('[Solution')
            for line in lines[1:]:
                line = '[Solution' + line
                stringWithinBrackets = re.search(r"\[Solution.*?]", line).group(0)
                versionNumber = int(re.findall(r'\d+', stringWithinBrackets)[0])
                resultString = line.strip().replace(stringWithinBrackets, '').encode('utf-8')\
                    .decode('us-ascii', errors='ignore').replace('?', '')

                results[versionNumber].add(resultString)
        return results

    def _extract_dm_results_from_file(self, fileName):
        results = {}
        for i in range(self._config.NUMBER_OF_VERSIONS + 1):
            results[i] = {'insertions': set(), 'deletions': set()}

        with open(fileName, "r") as file:
            for line in file:

                stringWithinBrackets = re.search(r"\[.*?]", line).group(0)
                versionNumber = int(re.findall(r'\d+', stringWithinBrackets)[0])
                resultString = line.strip().replace(stringWithinBrackets, '')

                if 'ADD' in stringWithinBrackets:
                    results[versionNumber]['insertions'].add(resultString)
                elif 'DEL' in stringWithinBrackets:
                    results[versionNumber]['deletions'].add(resultString)

        return results

    def _extract_vq_results_from_file(self, fileName):
        results = np.zeros(self._config.NUMBER_OF_VERSIONS)
        with open(fileName, "r") as file:
            lines = file.read().split('[Solution')
            for line in lines[1:]:
                line = '[Solution' + line
                stringWithinBrackets = re.search(r"\[Solution.*?]", line).group(0)
                versionNumber = int(re.findall(r'\d+', stringWithinBrackets)[0])
                if versionNumber < self._config.NUMBER_OF_VERSIONS:
                    results[versionNumber] = 1
        print("results ", results)
        return results




