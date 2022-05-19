from .HttpQuadStore import HttpQuadStore
from rdflib.term import URIRef, Literal


class TemporalQuadStore(HttpQuadStore):

    def __init__(self, queryEndpoint, updateEndpoint, dataEndpoint, effectiveDate: Literal = None,
                 transactionRevision: URIRef = None):
        super().__init__(queryEndpoint, updateEndpoint, dataEndpoint)
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