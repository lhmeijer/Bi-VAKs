from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from src.main.bitr4qs.store.QuadStoreSingleton import HttpDataStoreSingleton
from src.main.bitr4qs.revision.Update import Update
from src.main.bitr4qs.term.Modification import Modification


class Snapshot(ValidRevision):

    typeOfRevision = BITR4QS.Snapshot
    nameOfRevision = 'Snapshot'
    predicateOfPrecedingRevision = BITR4QS.precedingSnapshot

    def __init__(self, identifier: URIRef = None,
                 precedingRevision: URIRef = None,
                 hexadecimalOfHash: Literal = None,
                 nameDataset: Literal = None,
                 urlDataset: Literal = None,
                 effectiveDate: Literal = None,
                 transactionRevision: URIRef = None,
                 revisionNumber=None,
                 branchIndex=None):
        super().__init__(identifier, precedingRevision, hexadecimalOfHash, revisionNumber, branchIndex)
        self.name_dataset = nameDataset
        self.url_dataset = urlDataset
        self.effective_date = effectiveDate
        self.transaction_revision = transactionRevision

    @property
    def name_dataset(self):
        return self._nameDataset

    @name_dataset.setter
    def name_dataset(self, nameDataset: Literal):
        if nameDataset is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.nameDataset, nameDataset)))
        self._nameDataset = nameDataset

    @property
    def url_dataset(self):
        return self._urlDataset

    @url_dataset.setter
    def url_dataset(self, urlDataset: Literal):
        if urlDataset is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.urlDataset, urlDataset)))
        self._urlDataset = urlDataset

    @property
    def effective_date(self):
        return self._effectiveDate

    @effective_date.setter
    def effective_date(self, effectiveDate: Literal):
        if effectiveDate is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.validAt, effectiveDate)))
        self._effectiveDate = effectiveDate

    @property
    def transaction_revision(self):
        return self._transactionRevision

    @transaction_revision.setter
    def transaction_revision(self, transactionRevision: URIRef):
        if transactionRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.transactedAt, transactionRevision)))
        self._transactionRevision = transactionRevision

    def query(self, SPARQLQuery, queryType, returnFormat):
        # create a quad store from name dataset and url
        datastore = HttpDataStoreSingleton.get_data_store(self._nameDataset, self._urlDataset)
        # query the quad store, which returns an RDF Graph/Dataset
        if queryType == 'ConstructQuery':
            response = datastore.execute_construct_query(SPARQLQuery, returnFormat)

        return response

    def update_from_snapshot(self):
        SPARQLQuery = "CONSTRUCT WHERE { ?s ?p ?o }"
        datastore = HttpDataStoreSingleton.get_data_store(self._nameDataset, self._urlDataset)
        response = datastore.execute_construct_query(SPARQLQuery, 'nquads')
        # Parse the N-Quads to a list of triples or quads
        quads = ...
        update = Update.revision_from_data(startDate=self.effective_date, endDate=None, branchIndex=self.branch_index,
                                           revisionNumber=self.revision_number,
                                           modifications=[Modification(quad) for quad in quads])
        return update

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'branchIndex' in data, "branchIndex should be in the data of the revision"
        assert 'nameDataset' in data, "nameDataset should be in the data of the revision"
        assert 'urlDataset' in data, "urlDataset should be in the data of the revision"
        assert 'effectiveDate' in data, "effectiveDate should be in the data of the revision"
        assert 'transactionRevision' in data, "transactionRevision should be in the data of the revision"

        return cls(**data)

    @classmethod
    def _revision_from_request(cls, request):
        return cls(nameDataset=request.name_dataset, urlDataset=request.url_dataset,
                   effectiveDate=request.effective_date, transactionRevision=request.transaction_revision,
                   precedingRevision=request.preceding_snapshot.identifier)
