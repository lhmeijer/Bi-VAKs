from .RevisionStore import RevisionStore
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.namespace import BITR4QS
import src.main.bitr4qs.tools.parser as parser


class RevisionStoreImplicit(RevisionStore):

    def _get_pairs_of_revision_numbers_and_branch_indices(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        pairs = []
        while revisionA is not None:
            if revisionB is not None:
                SPARQLQuery = """
                SELECT ?revisionNumberA ?branchIndex ?revision ?stopRevisionNumber
                WHERE {{
                    {1} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ {1} :branch ?branch }}
                    OPTIONAL {{ ?branch :branchedOffAt ?revision }}
                    OPTIONAL {{ ?branch :branchIndex ?branchIndex }}
                    {2} :revisionNumber ?revisionNumberB .
                    BIND(IF( ?revisionNumberA < ?revisionNumberB, ?revisionNumberB, ?revisionNumberA) AS ?stopRevisionNumber) 
                }}
                """
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query(SPARQLQuery, 'json')
                revisionNumber = result['results']['bindings'][0]['revisionNumberA']['value']
                stopRevisionNumber = result['results']['bindings'][0]['stopRevisionNumber']['value']

            else:
                SPARQLQuery = """
                SELECT ?revisionNumber ?branchIndex ?revision
                WHERE {{ 
                    {0} :revisionNumber ?revisionNumber .
                    OPTIONAL {{ {0} :branch ?branch }}
                    OPTIONAL {{ ?branch :branchOffAt ?revision }}
                    OPTIONAL {{ ?branch :branchIndex ?branchIndex }} 
                    }}
                """.format(revisionA.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query(SPARQLQuery, 'json')
                revisionNumber = result['results']['bindings'][0]['revisionNumber']['value']
                stopRevisionNumber = revisionNumber

            if 'revision' in result['results']['bindings'][0]:
                revisionA = URIRef(result['results']['bindings'][0]['revision']['value'])
                branchIndex = result['results']['bindings'][0]['branchIndex']['value']
            else:
                revisionA = None
                branchIndex = 0

            if revisionNumber != stopRevisionNumber:
                revisionA = None
                pairs.append((revisionNumber, branchIndex, stopRevisionNumber))
            else:
                pairs.append((revisionNumber, branchIndex))

        return pairs

    @staticmethod
    def _select_valid_revision(pair, variableName='?update'):
        if len(pair) == 3:
            filterString = "FILTER ( ?revisionNumber <= {0} && ?revisionNumber >= {1} )".format(pair[0], pair[2])
        else:
            filterString = "FILTER ( ?revisionNumber <= {0} )".format(pair[0])
        SPARQLString = """
        {2} :branchIndex {0} . 
        {2} :revisionNumber ?revisionNumber .
        {1}
        """.format(pair[1], filterString, variableName)
        return SPARQLString

    def _valid_revisions_in_graph(self, revisionA: URIRef, validRevisionType: str, queryType: str,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param validRevisionType:
        :param revisionB:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)
        variable = '?{0}'.format(validRevisionType.lower())
        unions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair, variable)) for pair in pairs)
        otherUnions = "UNION".join("{{ {0} }}".format(self._select_valid_revision(pair, '?other')) for pair in pairs)

        queryString = "DESCRIBE" if queryType == 'DescribeQuery' else 'SELECT'
        prefixString = "PREFIX : <{0}>".format(str(BITR4QS)) if prefix else ""

        SPARQLQuery = """{0}
        {1} ?{2}
        WHERE {{ 
            ?{2} rdf:type :{3} .
            {4}{5}
            MINUS {{
                ?other rdf:type :{3} .
                ?other :preceding{3} ?revision .
                {6}
            }}
        }}""".format(prefixString, queryString, validRevisionType.lower(), validRevisionType, unions, timeConstrain,
                     otherUnions)
        stringOfValidRevisions = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        return stringOfValidRevisions

    def is_transaction_time_a_earlier(self, revisionA: URIRef, revisionB: URIRef) -> bool:
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        SPARQLString = """PREFIX : <{0}>
        ASK {{ 
            {1} :revisionNumber ?revisionNumberA . 
            {2} :revisionNumber ?revisionNumberB .
            FILTER ( ?revisionNumberA < ?revisionNumberB )
        }}
        """.format(str(BITR4QS), revisionA.n3(), revisionB.n3())
        result = self._revisionStore.execute_ask_query(SPARQLString)
        return result

    def tags_in_revision_graph(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        tags = self._valid_revisions_in_graph(revisionA=revisionA, validRevisionType='Tag', revisionB=revisionB,
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