from .HttpQuadStore import HttpQuadStore
from rdflib.term import URIRef, Literal
from urllib.request import urlopen
from urllib.error import HTTPError


class TemporalQuadStore(HttpQuadStore):

    def __init__(self, nameDataset, urlDataset, effectiveDate: Literal = None, transactionRevision: URIRef = None):
        super().__init__(nameDataset, urlDataset)
        self.effective_date = effectiveDate
        self.transaction_revision = transactionRevision

    def reset_store(self):
        SPARQLQuery = "DROP ALL"
        self.execute_update_query(SPARQLQuery)

    def add_modifications_to_store(self, modifications):
        deleteString, insertString = "", ""
        insert = False
        delete = False

        for modification in modifications:
            if modification.deletion:
                deleteString += modification.value.sparql() + '\n'
                delete = True
            else:
                insertString += modification.value.sparql() + '\n'
                insert = True

        if delete and insert:
            SPARQLQuery = 'DELETE DATA {{ {0} }};\nINSERT DATA {{ {1} }}'.format(deleteString, insertString)
            self.execute_update_query(SPARQLQuery)
        elif delete and not insert:
            SPARQLQuery = 'DELETE DATA {{ {0} }}'.format(deleteString)
            self.execute_update_query(SPARQLQuery)
        elif insert and not delete:
            SPARQLQuery = 'INSERT DATA {{ {0} }}'.format(insertString)
            self.execute_update_query(SPARQLQuery)

    def execute_query(self, queryString, returnFormat):
        request = self._create_query_request(queryString, returnFormat)
        try:
            response = urlopen(request)
            result = response.read().decode("utf-8")
        except HTTPError as e:
            print(e)
            raise HTTPError
        return result