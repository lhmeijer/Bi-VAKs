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
            return Literal(revisionNumber.value + 1, datatype=revisionNumber.datatype)
        else:
            return Literal(1, datatype=XSD.nonNegativeInteger)

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
        # print("result ", result)

        if 'branchIndex' in result['results']['bindings'][0]:
            branchIndex = int(result['results']['bindings'][0]['branchIndex']['value']) + 1
        else:
            branchIndex = int(1)

        return Literal(branchIndex, datatype=XSD.nonNegativeInteger)

    def _get_pairs_of_revision_numbers_and_branch_indices(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        pairs = []
        while revisionA is not None:
            if revisionB is not None:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchIndex ?revision ?stopRevisionNumber
                WHERE {{
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {1} :branch ?branch .
                        ?branch :branchedOffAt ?revision .
                        ?branch :branchIndex ?branchIndex. 
                    }}
                    {1} :revisionNumber ?revisionNumberB .
                    BIND(IF( ?revisionNumberA < ?revisionNumberB, ?revisionNumberB, ?revisionNumberA) AS ?stopRevisionNumber) 
                }}""".format(revisionA.n3(), revisionB.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                revisionNumber = Literal(result['results']['bindings'][0]['revisionNumberA']['value'], datatype=XSD.nonNegativeInteger)
                stopRevisionNumber = Literal(result['results']['bindings'][0]['stopRevisionNumber']['value'], datatype=XSD.nonNegativeInteger)

            else:
                SPARQLQuery = """SELECT ?revisionNumber ?branchIndex ?revision
                WHERE {{ 
                    {0} :revisionNumber ?revisionNumber .
                    OPTIONAL {{ 
                        {0} :branch ?branch .
                        ?branch :branchedOffAt ?revision .
                        ?branch :branchIndex ?branchIndex. 
                    }}
                }}""".format(revisionA.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                print("result ", result)
                revisionNumber = Literal(result['results']['bindings'][0]['revisionNumber']['value'], datatype=XSD.nonNegativeInteger)
                stopRevisionNumber = revisionNumber

            if 'revision' in result['results']['bindings'][0]:
                revisionA = URIRef(result['results']['bindings'][0]['revision']['value'])
                branchIndex = Literal(result['results']['bindings'][0]['branchIndex']['value'], datatype=XSD.nonNegativeInteger)
            else:
                revisionA = None
                branchIndex = Literal(0, datatype=XSD.nonNegativeInteger)

            if revisionNumber != stopRevisionNumber:
                revisionA = None
                pairs.append((revisionNumber, branchIndex, stopRevisionNumber))
            else:
                pairs.append((revisionNumber, branchIndex))
        print("pairs ", pairs)
        return pairs

    @staticmethod
    def _select_valid_revision(pair, variableName='?update'):
        if len(pair) == 3:
            filterString = "FILTER ( ?revisionNumber <= {0} && ?revisionNumber >= {1} )".format(pair[0].n3(), pair[2].n3())
        else:
            filterString = "FILTER ( ?revisionNumber <= {0} )".format(pair[0].n3())
        SPARQLString = """{2} :branchIndex {0} . 
        {2} :revisionNumber ?revisionNumber .
        {1}""".format(pair[1].n3(), filterString, variableName)
        return SPARQLString

    @staticmethod
    def _select_transaction_revision(pair, variableName='?update'):
        if len(pair) == 3:
            filterString = "FILTER ( ?revisionNumber <= {0} && ?revisionNumber >= {1} )".format(pair[0].n3(),
                                                                                                pair[2].n3())
        else:
            filterString = "FILTER ( ?revisionNumber <= {0} )".format(pair[0].n3())

        if pair[1].value == 0:
            SPARQLString = """{0} :revisionNumber ?revisionNumber .
            {1}
            FILTER NOT EXISTS {{ {0} :branch ?branch }}""".format(variableName, filterString)
        else:
            SPARQLString = """{2} :revisionNumber ?revisionNumber .
            {1}
            {2} :branch ?branch .
            ?branch :branchIndex {0} .""".format(pair[1].n3(), filterString, variableName)
        return SPARQLString

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):

        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(transactionRevisionA, transactionRevisionB)
        variable = '?revision'
        if len(pairs) == 1:
            unions = self._select_transaction_revision(pairs[0], variable)
        else:
            unions = "\nUNION\n".join("{{ {0} }}".format(self._select_transaction_revision(pair, variable)) for pair in pairs)

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ {0}
        FILTER ( {1} = ?revision )
        ?revision ?p ?o . }}""".format(unions, transactionRevision.n3())
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
        variable = '?{0}'.format(revisionType)

        if len(pairs) == 1:
            unions = self._select_valid_revision(pairs[0], variable)
            otherUnions = self._select_valid_revision(pairs[0], '?other')
        else:
            unions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair, variable)) for pair in pairs)
            otherUnions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair, '?other')) for pair in pairs)

        queryString = "DESCRIBE" if queryType == 'DescribeQuery' else 'SELECT'
        prefixString = "PREFIX : <{0}>\n".format(str(BITR4QS)) if prefix else ""

        SPARQLQuery = """{0}{1} {2}
        WHERE {{ 
            {2} rdf:type :{3} .
            {4}{5}
            MINUS {{
                ?other rdf:type :{3} .
                ?other :preceding{3} {2}.
                {6}
            }}
        }}""".format(prefixString, queryString, variable, revisionType.title(), unions, timeConstrain, otherUnions)
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

    def get_modifications_of_updates_between_revisions(self, revisionA, revisionB, date, updateParser, quadPattern,
                                                       forward=True):
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)
        unions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair)) for pair in pairs)
        otherPairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionB)

        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        updateTimeString = self._update_time_string(date=date)

        if self.config.related_update_content():
            otherUnions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(
                pair, variableName='?precedingUpdate')) for pair in otherPairs)
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')
            SPARQLQuery = """PREFIX : <{0}>
            CONSTRUCT {{ {1} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?update rdf:type :Update .
                         {2}{3}
                         ?update :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                {4}{5}
                ?precedingUpdate :precedingUpdate* ?update .    
                {6} }}""".format(str(BITR4QS), construct, unions, updateTimeString, otherUnions,
                                 updatePrecedingTimeString, where)
        else:
            otherUnions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair)) for pair in otherPairs)
            SPARQLQuery = """PREFIX : <{0}>
            CONSTRUCT {{ {1} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?update rdf:type :Update .
                         {2}{3}
                         ?update :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                BIND ( ?precedingUpdate AS ?update )
                {4}{3}
                {5} }}""".format(str(BITR4QS), construct, unions, updateTimeString, otherUnions, where)
        print('SPARQLQuery ', SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        print("stringOfUpdates ", stringOfUpdates)
        updateParser.parse_aggregate(stringOfUpdates, forward)

    def _transaction_revision_from_valid_revision(self, validRevisionID, revisionType):
        # TODO
        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
            {0} :branchIndex ?branchIndex .
            {0} :revisionNumber ?revisionNumber .
            OPTIONAL {{ ?branch :branchIndex ?branchIndex }}
            ?revision :revisionNumber ?revisionNumber .
            ?revision ?p ?o }}""".format(validRevisionID.n3())
        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))

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
        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))