import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json


class InitialTest(unittest.TestCase):

    def test_initialisation_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/initialise', data=dict(
            author='Yvette Post', nameDataset='BEAR-B', urlDataset='http://localhost:3030',
            description='Initialise BiTR4Qs.', date='2015-01-01T00:00:00+00:00'))
        self.assertEqual(response.status_code, 200)
        # obj = json.loads(response.data.decode("utf-8"))
        # self.assertEqual(obj['branchName'], "ChocolateRecipes")