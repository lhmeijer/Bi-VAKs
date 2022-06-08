from .RevisionStore import RevisionStore
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.namespace import BITR4QS
import src.main.bitr4qs.tools.parser as parser
from rdflib.namespace import XSD


class RevisionStoreImplicit(RevisionStore):

    @staticmethod
    def main_branch_index():
        return Literal(0, datatype=XSD.nonNegativeInteger)

    @staticmethod
    def new_revision_number(revisionNumber=None):
        """
        Function to obtain a new revision number based on the existing revision number.
        :param revisionNumber:
        :return:
        """
        if revisionNumber is not None:
            assert isinstance(revisionNumber, Literal)
            newRevisionNumber = Literal(revisionNumber.value + 1, datatype=revisionNumber.datatype)
        else:
            newRevisionNumber = Literal(1, datatype=XSD.nonNegativeInteger)
        return newRevisionNumber, newRevisionNumber

    def new_branch_index(self):
        """
        Function to obtain a new branch index based on the existing branches
        :return:
        """

        SPARQLQuery = """SELECT ?branchIndex
        WHERE {{
          ?branch rdf:type :Branch .
          ?branch :branchIndex ?branchIndex .
        }}
        ORDER BY DESC(?branchIndex)
        LIMIT 1
        """
        # Execute the SELECT query on the revision store
        result = self._revisionStore.execute_select_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'json')

        if 'branchIndex' in result['results']['bindings']:
            branchIndex = int(result['results']['bindings'][0]['branchIndex']['value']) + 1
        else:
            branchIndex = 0

        return Literal(branchIndex, datatype=XSD.nonNegativeInteger)

    def _get_pairs_of_revision_numbers_and_branch_indices(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        pairs = []
        while revisionA is not None:
            if revisionB:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchIndexA ?branchIndexB ?revision ?revisionNumberB
                WHERE {{
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {0} :branch ?branch .
                        ?branch :branchedOffAt ?revision .
                        ?branch :branchIndex ?branchIndexA. 
                    }}
                    {1} :revisionNumber ?revisionNumberB .
                    OPTIONAL {{ 
                        {1} :branch ?branchB .
                        ?branchB :branchIndex ?branchIndexB. 
                    }}
                }}""".format(revisionA.n3(), revisionB.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                if 'branchIndexB' in result['results']['bindings'][0]:
                    branchIndexB = int(result['results']['bindings'][0]['branchIndexB']['value'])
                else:
                    branchIndexB = 0

            else:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchIndexA ?revision
                WHERE {{ 
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {0} :branch ?branch .
                        ?branch :branchedOffAt ?revision .
                        ?branch :branchIndex ?branchIndexA . 
                    }}
                }}""".format(revisionA.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                branchIndexB = None

            revisionNumberA = int(result['results']['bindings'][0]['revisionNumberA']['value'])

            if 'revision' in result['results']['bindings'][0]:
                revisionA = URIRef(result['results']['bindings'][0]['revision']['value'])
                branchIndexA = int(result['results']['bindings'][0]['branchIndexA']['value'])
            else:
                revisionA = None
                branchIndexA = 0

            if branchIndexB is not None and branchIndexA == branchIndexB:
                revisionA = None
                revisionNumberB = int(result['results']['bindings'][0]['revisionNumberB']['value'])
                pairs.append((revisionNumberA, branchIndexA, revisionNumberB))
            else:
                pairs.append((revisionNumberA, branchIndexA))
        # print("pairs ", pairs)
        return pairs

    @staticmethod
    def _select_valid_revision(pair, revisionNumber='?revisionNumber', branchIndex='?branchIndex'):
        if len(pair) == 3:
            filterString = "( {3} = {0} && {4} <= {1} && {4} > {2} )".format(
                pair[1], pair[0], pair[2], branchIndex, revisionNumber)
        else:
            filterString = "( {2} = {0} && {3} <= {1} )".format(pair[1], pair[0], branchIndex, revisionNumber)
        return filterString

    @staticmethod
    def _select_transaction_revision(pair, revisionNumber='?revisionNumber', branchIndex='?branchIndex'):

        if pair[1] == 0:
            if len(pair) == 3:
                filterString = "( !bound({2}) && {3} <= {0} && {3} > {1} )".format(
                    pair[0], pair[2], branchIndex, revisionNumber)
            else:
                filterString = "( !bound({1}) && {2} <= {0} )".format(pair[0], branchIndex, revisionNumber)
        else:
            if len(pair) == 3:
                filterString = "( {3} = {2} && {4} <= {0} && {4} > {1} )".format(
                    pair[0], pair[2], pair[1], branchIndex, revisionNumber)
            else:
                filterString = "( {2} = {1} && {3} <= {0} )".format(pair[0], pair[1], branchIndex, revisionNumber)
        return filterString

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):

        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(transactionRevisionA, transactionRevisionB)
        revisionFilter = " || ".join(self._select_transaction_revision(pair) for pair in pairs)

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
        ?revision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?revision :branch ?branch . ?branch :branchIndex ?branchIndex . }}
        FILTER ( {0} )
        FILTER ( {1} = ?revision )
        ?revision ?p ?o . }}""".format(revisionFilter, transactionRevision.n3())
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))

    def _valid_revisions_in_graph(self, revisionA: URIRef, revisionType: str, queryType: str,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param revisionType:
        :param revisionB:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)

        revisionFilter = " || ".join(self._select_valid_revision(pair) for pair in pairs)
        otherFilter = " || ".join(self._select_valid_revision(pair, revisionNumber='?otherRevisionNumber',
                                                              branchIndex='?otherBranchIndex') for pair in pairs)

        queryString = "DESCRIBE" if queryType == 'DescribeQuery' else 'SELECT'
        prefixString = '\n'.join((self.prefixRDF, self.prefixBiTR4Qs)) if prefix else ""

        SPARQLQuery = """{0}
        {1} ?revision
        WHERE {{ 
            ?revision rdf:type :{2} .
            ?revision :revisionNumber ?revisionNumber .
            ?revision :branchIndex ?branchIndex .
            FILTER ( {3} ){4}
            MINUS {{
                ?other rdf:type :{2} .
                ?other :revisionNumber ?otherRevisionNumber .
                ?other :branchIndex ?otherBranchIndex .
                ?other :preceding{2} ?revision.
                FILTER ( {5} )
            }}
        }}""".format(prefixString, queryString, revisionType.title(), revisionFilter, timeConstrain, otherFilter)
        # print("SPARQLQuery ", SPARQLQuery)
        if prefix and queryType == 'DescribeQuery':
            stringOfValidRevisions = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
            return stringOfValidRevisions
        else:
            return SPARQLQuery

    def is_transaction_time_a_earlier(self, revisionA: URIRef, revisionB: URIRef) -> bool:
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        SPARQLQuery = """ASK {{ 
            {0} :revisionNumber ?revisionNumberA . 
            {1} :revisionNumber ?revisionNumberB .
            FILTER ( ?revisionNumberA < ?revisionNumberB )
        }}
        """.format(revisionA.n3(), revisionB.n3())
        result = self._revisionStore.execute_ask_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)))
        return result

    def tags_in_revision_graph(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        tags = self._valid_revisions_in_graph(revisionA=revisionA, revisionType='tag', revisionB=revisionB,
                                              queryType='DescribeQuery')
        tags = parser.TagParser.parse_sorted_implicit(tags)
        return tags

    def _get_sorted_updates(self, updateParser, stringOfUpdates, revisionA: URIRef, revisionB: URIRef = None,
                            forward=True):
        """

        :param updateParser:
        :param stringOfUpdates:
        :param revisionA:
        :param revisionB:
        :param forward:
        :return:
        """
        updateParser.parse_sorted_implicit(stringOfUpdates, forward=forward)

    def get_modifications_of_updates_between_revisions(self, revisionA, revisionB, date, updateParser, quadPattern,
                                                       forward=True):
        """

        :param revisionA:
        :param revisionB:
        :param date:
        :param updateParser:
        :param quadPattern:
        :param forward:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)
        revisionFilter = " || ".join(self._select_valid_revision(pair) for pair in pairs)

        otherPairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionB)

        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        updateTimeString = self._update_time_string(date=date)

        if self.config.related_update_content():
            otherFilter = " || ".join(self._select_valid_revision(
                pair, branchIndex='?precedingBranchIndex', revisionNumber='?precedingRevisionNumber') for pair in otherPairs)
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')
            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?revision rdf:type :Update .
                         ?revision :revisionNumber ?revisionNumber .
                         ?revision :branchIndex ?branchIndex .
                         FILTER ( {1} ){2}
                         ?revision :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                ?precedingUpdate :revisionNumber ?precedingRevisionNumber .
                ?precedingUpdate :branchIndex ?precedingBranchIndex .
                FILTER ( {3} ){4}
                ?precedingUpdate :precedingUpdate* ?revision .    
                {5} 
                }}""".format(construct, revisionFilter, updateTimeString, otherFilter,updatePrecedingTimeString, where)
        else:

            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?revision rdf:type :Update .
                         ?revision :revisionNumber ?revisionNumber .
                         ?revision :branchIndex ?branchIndex .
                         FILTER ( {1} ){2}
                         ?revision :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                BIND ( ?precedingUpdate AS ?revision )
                ?revision :revisionNumber ?revisionNumber .
                ?revision :branchIndex ?branchIndex .
                FILTER ( {1} ){2}
                {3} }}""".format(construct, revisionFilter, updateTimeString, where)
        # print('SPARQLQuery ', SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        # print("stringOfUpdates ", stringOfUpdates)
        if self._config.aggregated_modifications():
            updateParser.parse_aggregate(stringOfUpdates, forward)
        else:
            self._get_sorted_updates(updateParser, stringOfUpdates, revisionA, revisionB, forward)

    def _transaction_revision_from_valid_revision(self, validRevisionID, revisionType):
        """

        :param validRevisionID:
        :param revisionType:
        :return:
        """
        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
            {0} :branchIndex ?branchIndex .
            {0} :revisionNumber ?revisionNumber .
            ?revision :revisionNumber ?revisionNumber .
            OPTIONAL {{ ?branch rdf:type :Branch . ?revision :branch ?branch . ?branch :branchIndex ?branchIndex2 . }}
            FILTER ( !bound(?branch) || ?branchIndex2 = ?branchIndex )
            FILTER ( ( ?branchIndex = 0 && !bound(?branch) ) || ?branchIndex > 0 && bound(?branch) )
            FILTER NOT EXISTS {{ ?revision :branchIndex ?branchIndex1 }}
            ?revision ?p ?o .
        }}""".format(validRevisionID.n3())
        # print("SPARQLQuery ", SPARQLQuery)
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))

    def _valid_revisions_from_transaction_revision(self, transactionRevisionID, revisionType):
        """

        :param transactionRevisionID:
        :param revisionType:
        :return:
        """

        if revisionType == 'update':
            construct = 'GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2'
            where = '{ GRAPH ?g { ?revision ?p1 ?o1 } } UNION { ?revision ?p2 ?o2 }'
        else:
            construct = where = '?revision ?p ?o'

        SPARQLQuery = """
        CONSTRUCT {{ {0} }}
        WHERE {{ {1} :revisionNumber ?revisionNumber .
        OPTIONAL {{ {1} :branch ?branch . }}
        ?branch :branchIndex ?branchIndex .
        ?revision :revisionNumber ?revisionNumber .
        ?revision :branchIndex ?branchIndex .
        {2} }}""".format(construct, transactionRevisionID.n3(), where)
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))