from .HttpQuadStore import HttpQuadStore
from .TemporalQuadStore import TemporalQuadStore


class HttpRevisionStoreSingleton(object):

    store = None

    @classmethod
    def get(cls, config):
        print("cls.store ", cls.store)
        if cls.store is not None:
            return cls.store
        else:
            revisionStoreConfig = config.revision_store
            cls.store = HttpQuadStore("{0}/{1}/query".format(revisionStoreConfig['url'], revisionStoreConfig['name']),
                                      "{0}/{1}/update".format(revisionStoreConfig['url'], revisionStoreConfig['name']),
                                      "{0}/{1}/data".format(revisionStoreConfig['url'], revisionStoreConfig['name']))
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
            cls.store = TemporalQuadStore(
                "{0}/{1}/query".format(temporalStoreConfig['url'], temporalStoreConfig['name']),
                "{0}/{1}/update".format(temporalStoreConfig['url'], temporalStoreConfig['name']),
                "{0}/{1}/data".format(temporalStoreConfig['url'], temporalStoreConfig['name']))
            return cls.store


class HttpDataStoreSingleton(object):

    store = None

    @classmethod
    def get_data_store(cls, dbName, dbURL):
        if cls.store is not None:
            return cls.store
        else:
            cls.store = HttpQuadStore("{0}/{1}/query".format(dbURL, dbName), "{0}/{1}/update".format(dbURL, dbName),
                                      "{0}/{1}/data".format(dbURL, dbName))
            return cls.store

    @classmethod
    def close(cls):
        if cls.store is not None:
            cls.store.close()
            cls.store = None
