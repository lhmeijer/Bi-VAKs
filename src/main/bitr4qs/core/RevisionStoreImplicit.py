from .RevisionStore import RevisionStore
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.namespace import BITR4QS
import src.main.bitr4qs.tools.parser as parser
from rdflib.namespace import XSD


class RevisionStoreImplicit(RevisionStore):

    typeStore = 'implicit'

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

    def new_branch_index(self, branch=None):
        """
        Function to obtain a new branch index based on the existing branches
        :return:
        """

        if branch is not None:
            return branch.branch_index, branch.branch_index

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
            branchIndex = Literal(int(result['results']['bindings'][0]['branchIndex']['value']) + 1,
                                  datatype=XSD.nonNegativeInteger)
        else:
            branchIndex = Literal(1, datatype=XSD.nonNegativeInteger)

        return branchIndex, branchIndex

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
                        {0} :branchIndex ?branchIndexA .
                        ?branch rdf:type :Branch .
                        ?branch :branchIndex ?branchIndexA. 
                        MINUS {{
                            ?otherBranch rdf:type :Branch .
                            ?otherBranch :branchIndex branchIndexA .
                            ?otherBranch :precedingBranch ?branch .
                        }}
                        ?branch :branchedOffAt ?revision .
                    }}
                    {1} :revisionNumber ?revisionNumberB .
                    OPTIONAL {{ 
                        {1} :branchIndex ?branchIndexB .
                    }}
                }}""".format(revisionA.n3(), revisionB.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                revisionNumberB = int(result['results']['bindings'][0]['revisionNumberB']['value'])
                if 'branchIndexB' in result['results']['bindings'][0]:
                    branchIndexB = int(result['results']['bindings'][0]['branchIndexB']['value'])
                else:
                    branchIndexB = None

            else:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchIndexA ?revision
                WHERE {{ 
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {0} :branchIndex ?branchIndexA .
                        ?branch rdf:type :Branch .
                        ?branch :branchIndex ?branchIndexA . 
                        MINUS {{
                            ?otherBranch rdf:type :Branch .
                            ?otherBranch :branchIndex branchIndexA .
                            ?otherBranch :precedingBranch ?branch .
                        }}
                        ?branch :branchedOffAt ?revision .
                    }}
                }}""".format(revisionA.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                revisionNumberB, branchIndexB = None, None

            revisionNumberA = int(result['results']['bindings'][0]['revisionNumberA']['value'])

            if 'revision' in result['results']['bindings'][0]:
                revisionA = URIRef(result['results']['bindings'][0]['revision']['value'])
                branchIndexA = int(result['results']['bindings'][0]['branchIndexA']['value'])
            else:
                revisionA = None
                branchIndexA = None

            if revisionNumberB is not None and branchIndexA == branchIndexB:
                revisionA = None
                pairs.append((revisionNumberA, branchIndexA, revisionNumberB))
            else:
                pairs.append((revisionNumberA, branchIndexA))
        # print("pairs ", pairs)
        return pairs

    @staticmethod
    def _select_revision(pair, revisionNumber='?revisionNumber', branchIndex='?branchIndex'):
        if pair[1] is None:
            if len(pair) == 3:
                filterString = "( !bound({2}) && {3} <= {0} && {3} > {1} )".format(pair[0], pair[2], branchIndex,
                                                                                   revisionNumber)
            else:
                filterString = "( !bound({1}) && {2} <= {0} )".format(pair[0], branchIndex, revisionNumber)
        else:
            if len(pair) == 3:
                filterString = "( {3} = {0} && {4} <= {1} && {4} > {2} )".format(
                    pair[1], pair[0], pair[2], branchIndex, revisionNumber)
            else:
                filterString = "( {2} = {0} && {3} <= {1} )".format(pair[1], pair[0], branchIndex, revisionNumber)
        return filterString

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):

        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(transactionRevisionA, transactionRevisionB)
        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
        ?revision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?revision :branchIndex ?branchIndex . }}
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

        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)
        otherFilter = " || ".join(self._select_revision(pair, revisionNumber='?otherRevisionNumber',
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
                FILTER ( {5} )
                ?other :preceding{2} ?revision.
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

    def _sorted_snapshots(self, stringOfSnapshots, revisionA, revisionB=None, forward=True):
        snapshots = parser.SnapshotParser.parse_sorted_implicit(stringOfSnapshots, forward=forward)
        return snapshots

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
        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)

        otherPairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionB)

        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        updateTimeString = self._update_time_string(date=date)

        revisionNumbersConstruct, revisionNumbersWhere = "", ""

        if self.config.related_update_content():
            otherFilter = " || ".join(self._select_revision(pair, branchIndex='?precedingBranchIndex',
                                                            revisionNumber='?precedingRevisionNumber')
                                      for pair in otherPairs)
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')

            if self._config.sorted_modifications():
                revisionNumbersConstruct = "\n?revision :revisionNumber ?revisionNumber ."
                revisionNumbersWhere = "\nOPTIONAL { ?revision :revisionNumber ?revisionNumber }"

            SPARQLQuery = """CONSTRUCT {{ {0}{6} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?revision rdf:type :Update .
                         ?revision :precedingUpdate ?precedingUpdate .
                         ?revision :revisionNumber ?revisionNumber .
                         ?revision :branchIndex ?branchIndex .
                         FILTER ( {1} ){2}
                        }}
                    }}
                ?precedingUpdate :revisionNumber ?precedingRevisionNumber .
                ?precedingUpdate :branchIndex ?precedingBranchIndex .
                FILTER ( {3} ){4}
                OPTIONAL {{ ?precedingUpdate :precedingUpdate* ?revision }}
                {5}{7}
                }}""".format(construct, revisionFilter, updateTimeString, otherFilter, updatePrecedingTimeString, where,
                             revisionNumbersConstruct, revisionNumbersWhere)
        else:
            otherFilter = " || ".join(self._select_revision(
                pair, branchIndex='?otherBranchIndex', revisionNumber='?otherRevisionNumber') for pair in otherPairs)
            updateSucceedingTimeString = self._update_time_string(date=date, variableName='?succeedingRevision')

            if self._config.sorted_modifications():
                revisionNumbersConstruct = "\n?revision :revisionNumber ?revisionNumber ."

            SPARQLQuery = """CONSTRUCT {{ {0}{6} }}
            WHERE {{
                {{
                    SELECT ?revision 
                    WHERE {{
                        ?succeedingRevision rdf:type :Update .
                        ?succeedingRevision :precedingUpdate ?revision .
                        ?succeedingRevision :revisionNumber ?revisionNumber .
                        ?succeedingRevision :branchIndex ?branchIndex .
                        FILTER ( {1} ){2}
                    }}
                }}
                ?revision :revisionNumber ?otherRevisionNumber .
                ?revision :branchIndex ?otherBranchIndex .
                FILTER ( {3} ){4}    
                {5} }}""".format(construct, revisionFilter, updateSucceedingTimeString, otherFilter, updateTimeString,
                                 where, revisionNumbersConstruct)
        # print('SPARQLQuery ', SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        # print("stringOfUpdates ", stringOfUpdates)
        updateParser.parse_aggregate(stringOfUpdates, forward)

    def _transaction_revision_from_valid_revision(self, validRevisionID, revisionType):
        """

        :param validRevisionID:
        :param revisionType:
        :return:
        """
        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
            {0} :revisionNumber ?revisionNumber .
            OPTIONAL {{ {0} :branchIndex ?branchIndex }}
            ?revision :revisionNumber ?otherRevisionNumber .
            OPTIONAL {{ ?revision :branchIndex ?otherBranchIndex }}
            FILTER ( ?revisionNumber = ?otherRevisionNumber && ( !bound(?otherBranchIndex) && !bound(?branchIndex) || ?otherBranchIndex = ?branchIndex ))
            FILTER ( ?revision != {0} )
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
        WHERE {{ 
            {1} :revisionNumber ?revisionNumber .
            OPTIONAL {{ {1} :branchIndex ?branchIndex }}
            ?revision :revisionNumber ?otherRevisionNumber .
            OPTIONAL {{ ?revision :branchIndex ?otherBranchIndex }}
            FILTER ( ?revisionNumber = ?otherRevisionNumber && ( !bound(?otherBranchIndex) && !bound(?branchIndex) || ?otherBranchIndex = ?branchIndex ))
            FILTER ( ?revision != {1} )
        {2} }}""".format(construct, transactionRevisionID.n3(), where)
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))