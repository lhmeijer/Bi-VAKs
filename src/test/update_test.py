import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json


class UpdateTest(unittest.TestCase):

    def test_modify_update_start_date_explicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_ljs29c', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_ljs29c.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['endDate'], "2021-10-08T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-07-02T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_ljs29c")

    def test_modify_modified_update_end_date_explicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_45hgxa', data=dict(author='Edith Vonk', branch='SweetRecipes',
                                                               endDate='2021-10-10T00:00:00+00:00',
                                                               description='Modify update Update_45hgxa.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        print("obj ", obj)
        self.assertEqual(obj['endDate'], "2021-10-10T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-06-25T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_45hgxa")
        self.assertEqual(len(obj['modifications']), 16)

    def test_modify_update_start_date_implicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_ljs29c', data=dict(author='Erik Doerr',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_ljs29c.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['endDate'], "2021-10-08T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-07-02T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_ljs29c")
        self.assertEqual(obj['branchIndex'], "0")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_modify_modified_update_end_date_implicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_45hgxa', data=dict(author='Edith Vonk', branch='SweetRecipes',
                                                               endDate='2021-10-10T00:00:00+00:00',
                                                               description='Modify update Update_45hgxa.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['endDate'], "2021-10-10T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-06-25T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_45hgxa")
        self.assertEqual(len(obj['modifications']), 16)
        self.assertEqual(obj['branchIndex'], "1")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_modify_update_start_date_explicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_ljs29c', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_ljs29c.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['endDate'], "2021-10-08T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-07-02T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_ljs29c")
        self.assertEqual(len(obj['modifications']), 0)

    def test_modify_modified_update_end_date_explicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_45hgxa', data=dict(author='Edith Vonk', branch='SweetRecipes',
                                                               endDate='2021-10-10T00:00:00+00:00',
                                                               description='Modify update Update_45hgxa.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        self.assertEqual(obj['endDate'], "2021-10-10T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-06-25T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_45hgxa")
        self.assertEqual(len(obj['modifications']), 0)

    def test_modify_update_start_date_implicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_ljs29c', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_ljs29c.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        print("obj ", obj)
        self.assertEqual(obj['endDate'], "2021-10-08T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-07-02T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_ljs29c")
        self.assertEqual(len(obj['modifications']), 0)
        self.assertEqual(obj['branchIndex'], "0")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])

    def test_modify_modified_update_end_date_implicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_45hgxa', data=dict(author='Edith Vonk', branch='SweetRecipes',
                                                               endDate='2021-10-10T00:00:00+00:00',
                                                               description='Modify update Update_45hgxa.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        print("obj ", obj)
        self.assertEqual(obj['endDate'], "2021-10-10T00:00:00+00:00")
        self.assertEqual(obj['startDate'], "2021-06-25T00:00:00+00:00")
        self.assertEqual(obj['precedingRevision'], "http://bi-tr4qs.org/vocab/Update_45hgxa")
        self.assertEqual(len(obj['modifications']), 0)
        self.assertEqual(obj['branchIndex'], "1")
        self.assertEqual(obj['revisionNumber'], response.headers['X-CurrentRevisionNumber'])