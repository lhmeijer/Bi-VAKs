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
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)
        obj = json.loads(response.data.decode("utf-8"))
        print(obj)

    def test_modify_update_start_date_implicit_repeated(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': True, 'related': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)

    def test_modify_update_start_date_explicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)

    def test_modify_update_start_date_implicit_related(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        args['UpdateContentStrategy'] = {'repeated': False, 'related': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+00:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)