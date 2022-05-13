from .HttpQuadStore import HttpQuadStore
from rdflib.term import URIRef, Literal
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError


class TemporalQuadStore(HttpQuadStore):

    def __init__(self, queryEndpoint, updateEndpoint, dataEndpoint, effectiveDate: Literal = None,
                 transactionRevision: URIRef = None):
        super().__init__(queryEndpoint, updateEndpoint)
        self._dataEndpoint = dataEndpoint
        self.effective_date = effectiveDate
        self.transaction_revision = transactionRevision

    def reset_store(self):
        SPARQLQuery = "DROP ALL"
        self.execute_update_query(SPARQLQuery)

    def add_modifications_to_store(self, modifications):
        deleteString = ""
        insertString = ""

        for modification in modifications:
            if modification.deletion:
                deleteString += modification.value.to_sparql()
            else:
                insertString += modification.value.to_sparql()

        SPARQLQuery = """DELETE DATA {{ {0} }};
        INSERT DATA {{ {1} }}""".format(deleteString, insertString)
        self.execute_update_query(SPARQLQuery)

    def n_quads_to_store(self, nquads):
        print('nquads ', nquads)
        headers = {'Content-Type': 'application/n-quads'}
        nquads = nquads.encode(encoding='utf-8', errors='strict')
        request = Request(self._dataEndpoint, data=nquads, headers=headers)
        try:
            response = urlopen(request)
            print("response ", response.read())
        except HTTPError as e:
            print(e)
            raise HTTPError
        return response