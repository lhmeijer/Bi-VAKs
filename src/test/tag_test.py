import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json


class TagTest(unittest.TestCase):

    def test_tag_from_main_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False, 'combined': False}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Jeroen Schipper',
                                              date='2021-07-01T00:00:00+00:00',
                                              description='Add a new tag to the Bi-TR4Qs.', revision='HEAD'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], response.headers['X-CurrentRevision'])

    def test_tag_from_another_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False, 'combined': False}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Sabine Jonker',
                                              date='2021-07-01T00:00:00+00:00', branch='SweetRecipes',
                                              description='Add a new tag to the Bi-TR4Qs.', revision='HEAD'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], response.headers['X-CurrentRevision'])

    def test_tag_from_specific_revision_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Maaike Stam', date='2021-07-01T00:00:00+00:00',
                                              description='Add a new tag to the Bi-TR4Qs.', branch='FishRecipes',
                                              revision='http://bi-tr4qs.org/vocab/Revision_30fhp34'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], "http://bi-tr4qs.org/vocab/Revision_30fhp34")

    def test_tag_from_main_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Jeroen Schipper',
                                              date='2021-07-01T00:00:00+00:00',
                                              description='Add a new tag to the Bi-TR4Qs.', revision='HEAD'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], response.headers['X-CurrentRevision'])
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_tag_from_another_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Sabine Jonker', revision='HEAD',
                                              date='2021-07-01T00:00:00+00:00', branch='SweetRecipes',
                                              description='Add a new tag to the Bi-TR4Qs.'))
        self.assertEqual(response.status_code, 200)

        obj = json.loads(response.data.decode("utf-8"))

        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], response.headers['X-CurrentRevision'])
        self.assertEqual(obj['branchIndex'], "1")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_tag_from_specific_revision_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/tag', data=dict(name='version 1', author='Maaike Stam', date='2021-07-01T00:00:00+00:00',
                                              description='Add a new tag to the Bi-TR4Qs.', branch='FishRecipes',
                                              revision='http://bi-tr4qs.org/vocab/Revision_30fhp34'))
        self.assertEqual(response.status_code, 200)

        obj = json.loads(response.data.decode("utf-8"))

        self.assertEqual(obj['effectiveDate'], "2021-07-01T00:00:00+00:00")
        self.assertEqual(obj['tagName'], "version 1")
        self.assertEqual(obj['transactionRevision'], "http://bi-tr4qs.org/vocab/Revision_30fhp34")
        self.assertEqual(obj['branchIndex'], "2")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])