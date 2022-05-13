import sys
from src.main.bitr4qs.exception import InvalidConfigurationError


def get_default_configuration():
    return {
        'port': 5000,
        'host': '::',
        'referenceStrategy': {'implicit': False, 'explicit': True},
        'fetchingStrategy': {'queryAllUpdates': True, 'querySpecificUpdates': False},
        'UpdateContentStrategy': {'repeated': True, 'related': False},
        'revisionStore': {'name': 'revision-store', 'url': 'http://localhost:3030'},
        'temporalStore': {'name': 'temporal-store', 'url': 'http://localhost:3030'}
    }


def initialise(args):

    try:
        config = BiTR4QsConfiguration(**args)
    except InvalidConfigurationError as e:
        print(e)
        sys.exit('Exiting triple version control system')

    return config


class BiTR4QsConfiguration:

    def __init__(self, port, host, referenceStrategy, fetchingStrategy, UpdateContentStrategy, revisionStore,
                 temporalStore):
        self._port = port
        self._host = host
        self._referenceStrategy = referenceStrategy
        self._fetchingStrategy = fetchingStrategy
        self._UpdateContentStrategy = UpdateContentStrategy
        self._revisionStore = revisionStore
        self._temporalStore = temporalStore

    def repeated_update_content(self):
        return self._UpdateContentStrategy['repeated']

    def related_update_content(self):
        return self._UpdateContentStrategy['related']

    def query_all_updates(self):
        return self._fetchingStrategy['queryAllUpdates']

    def query_specific_updates(self):
        return self._fetchingStrategy['querySpecificUpdates']

    def implicit_reference(self):
        return self._referenceStrategy['implicit']

    def explicit_reference(self):
        return self._referenceStrategy['explicit']

    @property
    def revision_store(self):
        return self._revisionStore

    @property
    def temporal_store(self):
        return self._temporalStore


