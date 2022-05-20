from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import Literal, URIRef
from src.main.bitr4qs.term.Triple import Triple


class InitialRevision(TransactionRevision):

    typeOfRevision = BITR4QS.InitialRevision
    nameOfRevision = 'InitialRevision'

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'creationDate' in data, "creationDate should be in the data of the revision"
        assert 'author' in data, "author should be in the data of the revision"
        assert 'description' in data, "description should be in the data of the revision"

        return cls(**data)
