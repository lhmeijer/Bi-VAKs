import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration


class UpdateTest(unittest.TestCase):

    def test_modify_update_end_date_explicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': True, 'implicit': False}
        app = create_app(args).test_client()
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+02:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)

    def test_modify_update_end_date_implicit(self):
        args = get_default_configuration()
        args['referenceStrategy'] = {'explicit': False, 'implicit': True}
        app = create_app(args).test_client()
        response = app.post('/update/Update_dk2b0o', data=dict(author='Edith Vonk',
                                                               startDate='2021-07-02T00:00:00+02:00',
                                                               description='Modify update Update_mv83lv.'))
        self.assertEqual(response.status_code, 200)