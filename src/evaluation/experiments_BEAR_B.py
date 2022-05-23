# Create Updates and Update Revisions from the BEAR B Benchmark -> instant data
# These Updates have different time interval -> wide, small, much overlap, less overlap -> normal distribution
# retrospective or future -> nearby to its creation date or far away from it
# Different amount of Updates -> [21046, 10523, 5261, 2630, 1315]
# Create after a day a Tag and Tag Revision to indicate the new version -> which we should query
# Single snapshot middle transaction time, and much overlap in valid time and not a lot of overlap in valid time
# Branches
# Modifications in valid time
from src.main.bitr4qs.term.TriplePattern import TriplePattern
from src.evaluation.queries import Queries
import re
import json
from rdflib import URIRef, Literal

# CHECK Whether revision store already exists

def extract_vm_results_from_file(fileName):

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


def extract_dm_results_from_file(fileName):
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


def extract_vq_results_from_file(fileName):
    results = {}
    with open(fileName, "r") as file:
        for line in file:

            stringWithinBrackets = re.search(r"\[.*?]", line).group(0)
            versionNumber = int(re.findall(r'\d', stringWithinBrackets)[0])


    return results


def delta_materialisation():

    for queryIndex, query in Queries.items():
        triplePattern = TriplePattern((query['s'], query['p'], query['o']))
        realResults = extract_dm_results_from_file(...)

        jumps = list(range(0, 89, 5)) + [89]
        for i in jumps:
            results = app.get('/query', query_string=dict(query=triplePattern.to_select_query(), queryAtomType='DM',
                                                          tagA='version 0', tagB='version {0}'.format(i)),
                               headers=dict(accept="application/sparql-results+json"))
            jsonResults = json.loads(results.read().decode("utf-8"))
            for jsonResult in jsonResults['results']['insertions']:
                result = []
                for variableName, variableResult in jsonResult.items():
                    if variableResult['type'] == 'uri':
                        result.append(URIRef(variableResult['value']))
                    elif variableResult['type'] == 'literal':
                        result.append(Literal(variableResult['value']))
                stringResult = ' '.join(term.n3() for term in result)

def version_materialisation():

    for queryIndex, query in Queries.items():
        triplePattern = TriplePattern((query['s'], query['p'], query['o']))
        realResults = extract_vm_results_from_file(...)

        for i in range(nOfVersions + 1):
            results = app.get('/query', query_string=dict(query=triplePattern.to_select_query(), queryAtomType='VM',
                                                           tag='version {0}'.format(i)),
                               headers=dict(accept="application/sparql-results+json"))
            jsonResults = json.loads(results.read().decode("utf-8"))
            for jsonResult in jsonResults['results']['bindings']:
                result = []
                for variableName, variableResult in jsonResult.items():
                    if variableResult['type'] == 'uri':
                        result.append(URIRef(variableResult['value']))
                    elif variableResult['type'] == 'literal':
                        result.append(Literal(variableResult['value']))
                stringResult = ' '.join(term.n3() for term in result)

                try:
                    realResults[i].remove(stringResult)
                except ValueError:
                    raise Exception  # or scream: thing not in some_list!

            if len(realResults[i]) > 0:
                raise Exception




def experiment1():
    pass

if __name__ == '__main__':
    file_name = '/Users/lisameijer/Universiteit/ThesisComputerScience/data/alldata.CB.nt-2/data-added_1-2.nt'
    with open(file_name) as f:
        lines = f.readlines()

    count = 0
    for line in lines:
        count += 1
        print(f'line {count}: {line}')

    #data = open('test.ttl').read()
    # headers = {'Content-Type': 'text/turtle;charset=utf-8'}
    # r = requests.post('http://localhost:3030/mydataset/data?default', data=data, headers=headers)