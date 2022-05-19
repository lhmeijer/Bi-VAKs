import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration


class SnapshotTest(unittest.TestCase):

    def test_snapshot_from_main_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.', revision='HEAD'))
        self.assertEqual(response.status_code, 200)

    def test_snapshot_from_another_branch_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.',
                                                   branch='SweetRecipes', revision='HEAD'))
        self.assertEqual(response.status_code, 200)

    def test_snapshot_from_specific_revision_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.',
                                                   revision='http://bi-tr4qs.org/vocab/Revision_93jgc0p'))
        self.assertEqual(response.status_code, 200)

    def test_snapshot_from_main_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.', revision='HEAD'))
        self.assertEqual(response.status_code, 200)

    def test_snapshot_from_another_branch_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.',
                                                   branch='SweetRecipes', revision='HEAD'))
        self.assertEqual(response.status_code, 200)

    def test_snapshot_from_specific_revision_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/snapshot', data=dict(nameDataset='test-snapshot', urlDataset='http://localhost:3030',
                                                   author='Vincent Koster', date='2021-06-16T00:00:00+02:00',
                                                   description='Add a new snapshot to the Bi-TR4Qs.',
                                                   revision='http://bi-tr4qs.org/vocab/Revision_dk290vw'))
        self.assertEqual(response.status_code, 200)