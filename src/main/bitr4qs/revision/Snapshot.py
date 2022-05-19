from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.namespace import BITR4QS
from src.main.bitr4qs.store.QuadStoreSingleton import HttpDataStoreSingleton
from src.main.bitr4qs.revision.Update import Update
from src.main.bitr4qs.term.Modification import Modification
from .TransactionRevision import TransactionRevision
from src.main.bitr4qs.tools.parser.UpdateParser import UpdateParser


class SnapshotRevision(TransactionRevision):

    typeOfRevision = BITR4QS.SnapshotRevision
    nameOfRevision = 'SnapshotRevision'


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

    def add_to_revision_store(self, revisionStore):
        super().add_to_revision_store(revisionStore)

        # Get all updates for the snapshot
        updateParser = UpdateParser()
        datastore = HttpDataStoreSingleton.get_data_store(self._nameDataset.value, self._urlDataset.value)
        print("datastore ", datastore)
        revisionStore.get_updates_in_revision_graph(revisionA=self._transactionRevision, date=self._effectiveDate,
                                                    updateParser=updateParser)
        modifications_in_n_quad = updateParser.modifications_to_n_quads()
        datastore.n_quads_to_store(modifications_in_n_quad)

    def query(self, SPARQLQuery, queryType, returnFormat):
        # create a quad store from name dataset and url
        datastore = HttpDataStoreSingleton.get_data_store(self._nameDataset.value, self._urlDataset.value)
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

    def modify(self, revisionStore, otherNameDataset=None, otherUrlDataset=None, otherEffectiveDate=None,
               otherTransactionRevision=None, revisionNumber=None, branchIndex=None):

        nameDataset = otherNameDataset if otherNameDataset is not None else self._nameDataset
        urlDataset = otherUrlDataset if otherUrlDataset is not None else self._urlDataset

        if otherEffectiveDate is not None:
            # Get all updates which corresponds to otherEffectiveDate and not existing effective date
            updates = ...
            # Add and delete these updates from the snapshot
            effectiveDate = otherEffectiveDate
        else:
            effectiveDate = self._effectiveDate

        if otherTransactionRevision is not None:
            # Get all updates between the existing transaction revision and otherTransactionRevision
            transactionRevision = otherTransactionRevision
        else:
            transactionRevision = self._transactionRevision

        modifiedSnapshot = Snapshot.revision_from_data(
            effectiveDate=effectiveDate, branchIndex=branchIndex, nameDataset=nameDataset, urlDataset=urlDataset,
            revisionNumber=revisionNumber, transactionRevision=transactionRevision, precedingRevision=self._identifier)
        return modifiedSnapshot

    def revert(self, revisionStore, revisionNumber=None, branchIndex=None):
        # Check whether there exists a preceding snapshot
        if self._precedingRevision is not None:
            # Get the preceding snapshot
            otherSnapshot = ...
            revertedSnapshot = self.modify()
        else:
            # Remove this snapshot from Jena
            revertedSnapshot = Snapshot.revision_from_data(revisionNumber=revisionNumber, branchIndex=branchIndex,
                                                           nameDataset=None, urlDataset=None, transactionRevision=None,
                                                           effectiveDate=None, precedingRevision=self._identifier)
        return revertedSnapshot

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
