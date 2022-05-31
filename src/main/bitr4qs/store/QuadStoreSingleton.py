from .HttpQuadStore import HttpQuadStore
from .TemporalQuadStore import TemporalQuadStore


class HttpRevisionStoreSingleton(object):

    store = None

    @classmethod
    def get(cls, config):
        if cls.store is not None:
            return cls.store
        else:
            revisionStoreConfig = config.revision_store
            cls.store = HttpQuadStore(urlDataset=revisionStoreConfig['url'], nameDataset=revisionStoreConfig['name'])
            return cls.store

    @classmethod
    def close(cls):
        if cls.store is not None:
            cls.store.close()
            cls.store = None


class HttpTemporalStoreSingleton(HttpRevisionStoreSingleton):

    store = None

    @classmethod
    def get(cls, config):
        if cls.store is not None:
            return cls.store
        else:
            temporalStoreConfig = config.temporal_store
            cls.store = TemporalQuadStore(nameDataset=temporalStoreConfig['name'], urlDataset=temporalStoreConfig['url'])
            return cls.store
