import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json


class BranchTest(unittest.TestCase):

    def test_branch_from_main_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Yvette Post',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")

    def test_branch_from_another_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Veerle Groot', branch='SweetRecipes',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")

    def test_branch_from_specific_revision_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Marijn Bosch',
                                                 revision='http://bi-tr4qs.org/vocab/Revision_49bvls3',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")
        self.assertEqual(obj['branchedOffRevision'], "http://bi-tr4qs.org/vocab/Revision_49bvls3")

    def test_branch_from_main_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Yvette Post',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_branch_from_another_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Veerle Groot',
                                                 branch='SweetRecipes',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_branch_from_specific_revision_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/branch', data=dict(name='ChocolateRecipes', author='Marijn Bosch',
                                                 revision='http://bi-tr4qs.org/vocab/Revision_49bvls3',
                                                 description='Add a new branch called ChocolateRecipes.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['branchName'], "ChocolateRecipes")
        self.assertEqual(obj['branchedOffRevision'], "http://bi-tr4qs.org/vocab/Revision_49bvls3")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])
