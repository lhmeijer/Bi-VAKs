import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json


class DMQueryTest(unittest.TestCase):

    def test_select_query_single_variable_explicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['fetchingStrategy'] = {'queryAllUpdates': False, 'querySpecificUpdates': True}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        query = """PREFIX recipes:  <http://recipehub.nl/recipes#>
        SELECT ?recipe
        WHERE { GRAPH recipes:ChickenAndEgg { ?recipe recipes:produces recipes:Shakshuka . } }
        """
        response = app.get('/query', query_string=dict(
            query=query, queryAtomType='DM', revisionA='http://bi-tr4qs.org/vocab/Revision_sl2vb01',
            revisionB='http://bi-tr4qs.org/vocab/Revision_axz0pb4', dateA="2021-07-10T00:00:00+00:00",
            dateB="2021-07-20T00:00:00+00:00"), headers=dict(accept="application/sparql-results+json"))
        self.assertEqual(response.status_code, 200)

        obj = json.loads(response.data.decode("utf-8"))
        self.assertDictEqual(obj["results"]["bindings"][0], {
            "cuisine": {'type': 'uri', 'value': 'http://recipehub.nl/recipes#ChineseCuisine'}})

    def test_select_query_multiple_variables_explicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['fetchingStrategy'] = {'queryAllUpdates': False, 'querySpecificUpdates': True}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        query = """PREFIX recipes:  <http://recipehub.nl/recipes#>
        SELECT ?recipe ?food
        WHERE { GRAPH recipes:ChickenAndEgg { ?recipe recipes:produces ?food . } }
        """
        response = app.get('/query', query_string=dict(
            query=query, queryAtomType='DM', revisionA='http://bi-tr4qs.org/vocab/Revision_sl2vb01',
            revisionB='http://bi-tr4qs.org/vocab/Revision_axz0pb4', dateA="2021-07-10T00:00:00+00:00",
            dateB="2021-07-20T00:00:00+00:00"), headers=dict(accept="application/sparql-results+json"))
        self.assertEqual(response.status_code, 200)

        obj = json.loads(response.data.decode("utf-8"))
        self.assertDictEqual(obj["results"]["bindings"][0], {
            "cuisine": {'type': 'uri', 'value': 'http://recipehub.nl/recipes#ChineseCuisine'}})

    def test_select_query_implicit_repeated_head(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['fetchingStrategy'] = {'queryAllUpdates': True, 'querySpecificUpdates': False}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        query = """
        PREFIX :  <http://recipehub.nl/recipes#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?cuisine
        WHERE { ?cuisine rdfs:label "Chinese"@en-gb . }
        """
        response = app.get('/query', query_string=dict(query=query, queryAtomType='VM',
                                                       date="2021-07-01T00:00:00+02:00",
                                                       revision='http://bi-tr4qs.org/vocab/Revision_dk290vw'),
                           headers=dict(accept="application/sparql-results+json"))
        self.assertEqual(response.status_code, 200)

        obj = json.loads(response.data.decode("utf-8"))
        self.assertDictEqual(obj["results"]["bindings"][0], {
            "cuisine": {'type': 'uri', 'value': 'http://recipehub.nl/recipes#ChineseCuisine'}})

