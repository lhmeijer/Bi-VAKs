import sys
from src.main.bitr4qs.exception import InvalidConfigurationError


def get_default_configuration():
    return {
        'port': 5000,
        'host': '::',
        'referenceStrategy': {'implicit': False, 'explicit': True, 'combined': False},
        'fetchingStrategy': {'queryAllUpdates': True, 'querySpecificUpdates': False},
        'updateContentStrategy': {'repeated': True, 'related': False},
        'modificationsStrategy': {'aggregated': True, 'sorted': False},
        'retrievingStrategy': {'betweenUpdates': True, 'fromInitialUpdate': False},
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

    def __init__(self, port, host, referenceStrategy, fetchingStrategy, updateContentStrategy, modificationsStrategy,
                 retrievingStrategy, revisionStore, temporalStore):
        self._port = port
        self._host = host
        self._referenceStrategy = referenceStrategy
        self._fetchingStrategy = fetchingStrategy
        self._updateContentStrategy = updateContentStrategy
        self._modificationsStrategy = modificationsStrategy
        self._retrievingStrategy = retrievingStrategy
        self._revisionStore = revisionStore
        self._temporalStore = temporalStore

    def repeated_update_content(self):
        return self._updateContentStrategy['repeated']

    def related_update_content(self):
        return self._updateContentStrategy['related']

    def query_all_updates(self):
        return self._fetchingStrategy['queryAllUpdates']

    def query_specific_updates(self):
        return self._fetchingStrategy['querySpecificUpdates']

    def implicit_reference(self):
        return self._referenceStrategy['implicit']

    def explicit_reference(self):
        return self._referenceStrategy['explicit']

    def combined_reference(self):
        return self._referenceStrategy['combined']

    def aggregated_modifications(self):
        return self._modificationsStrategy['aggregated']

    def sorted_modifications(self):
        return self._modificationsStrategy['sorted']

    def retrieve_between_updates(self):
        return self._retrievingStrategy['betweenUpdates']

    def retrieve_from_initial_update(self):
        return self._retrievingStrategy['fromInitialUpdate']

    @property
    def revision_store(self):
        return self._revisionStore

    @property
    def temporal_store(self):
        return self._temporalStore


