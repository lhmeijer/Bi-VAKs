import unittest
from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration
import json
import os


class ApplicationTest(unittest.TestCase):

    def test_upload_data_to_revision_store(self):
        args = get_default_configuration()
        app = create_app(args).test_client()
        fileName = os.path.join(os.path.dirname(__file__), '..', '..', 'testdata', 'RevisionsExplicit.trig')
        with open(fileName) as file:
            data = file.read()
            response = app.post('/upload', data=data, headers=dict(accept="application/trig"))
            self.assertEqual(response.status_code, 200)
            # response = app.post('/upload', data=data, headers=dict(accept="application/n-triples"))

    def test_get_data_from_revision_store(self):
        args = get_default_configuration()
        app = create_app(args).test_client()
        response = app.get('/data', headers=dict(accept="application/trig"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], "application/trig")

    def test_reset_revision_store(self):
        args = get_default_configuration()
        app = create_app(args).test_client()
        response = app.delete('/reset')
        self.assertEqual(response.status_code, 200)