from .RevisionStore import RevisionStore
from rdflib.term import URIRef, Literal
import src.main.bitr4qs.tools.parser as parser
from rdflib.namespace import XSD


class RevisionStoreCombined(RevisionStore):

    typeStore = 'combined'

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
        return newRevisionNumber, None

    def _get_pairs_of_revision_numbers_and_branch(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        pairs = []
        while revisionA is not None:
            if revisionB:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchA ?branchB ?revision ?revisionNumberB
                WHERE {{
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {0} :branch ?branch .
                        ?branch :branchedOffAt ?revision .
                    }}
                    {1} :revisionNumber ?revisionNumberB .
                    OPTIONAL {{ 
                        {1} :branch ?branchB .
                    }}
                }}""".format(revisionA.n3(), revisionB.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                revisionNumberB = int(result['results']['bindings'][0]['revisionNumberB']['value'])
                if 'branchB' in result['results']['bindings'][0]:
                    branchB = URIRef(result['results']['bindings'][0]['branchB']['value'])
                else:
                    branchB = None

            else:
                SPARQLQuery = """SELECT ?revisionNumberA ?branchA ?revision
                WHERE {{ 
                    {0} :revisionNumber ?revisionNumberA .
                    OPTIONAL {{ 
                        {0} :branch ?branchA .
                        ?branchA :branchedOffAt ?revision . 
                    }}
                }}""".format(revisionA.n3())
                # Execute the SELECT query on the revision store
                result = self._revisionStore.execute_select_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'json')
                revisionNumberB, branchB = None, None

            revisionNumberA = int(result['results']['bindings'][0]['revisionNumberA']['value'])

            if 'revision' in result['results']['bindings'][0]:
                revisionA = URIRef(result['results']['bindings'][0]['revision']['value'])
                branchA = URIRef(result['results']['bindings'][0]['branchA']['value'])
            else:
                revisionA = None
                branchA = None

            if revisionNumberB is not None and branchA == branchB:
                revisionA = None
                pairs.append((revisionNumberA, branchA, revisionNumberB))
            else:
                pairs.append((revisionNumberA, branchA))
        # print("pairs ", pairs)
        return pairs

    @staticmethod
    def _select_transaction_revision(pair, revisionNumber='?revisionNumber', branch='?branch'):
        if pair[1] is None:
            if len(pair) == 3:
                filterString = "( !bound({2}) && {3} <= {0} && {3} > {1} )".format(pair[0], pair[2], branch,
                                                                                   revisionNumber)
            else:
                filterString = "( !bound({1}) && {2} <= {0} )".format(pair[0], branch, revisionNumber)
        else:
            if len(pair) == 3:
                filterString = "( {3} = {2} && {4} <= {0} && {4} > {1} )".format(
                    pair[0], pair[2], pair[1], branch, revisionNumber)
            else:
                filterString = "( {2} = {1} && {3} <= {0} )".format(pair[0], pair[1], branch, revisionNumber)
        return filterString

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):

        pairs = self._get_pairs_of_revision_numbers_and_branch(transactionRevisionA, transactionRevisionB)
        revisionFilter = " || ".join(self._select_transaction_revision(pair) for pair in pairs)

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
        ?revision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?revision :branch ?branch .}}
        FILTER ( {0} )
        FILTER ( {1} = ?revision )
        ?revision ?p ?o . }}""".format(revisionFilter, transactionRevision.n3())
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))

    def _valid_revisions_in_graph(self, revisionA: URIRef, revisionType: str, queryType: str = None,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param revisionType:
        :param revisionB:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch(revisionA, revisionB)

        revisionFilter = " || ".join(self._select_transaction_revision(pair) for pair in pairs)
        otherFilter = " || ".join(self._select_transaction_revision(pair, revisionNumber='?otherRevisionNumber',
                                                                    branch='?otherBranch') for pair in pairs)

        subString = """
        ?transactionRevision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?transactionRevision :branch ?branch . }}
        FILTER ( {1} )
        ?transactionRevision :{0} ?revision.{2}
        MINUS {{
            ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
            OPTIONAL {{ ?otherTransactionRevision :branch ?otherBranch . }}
            FILTER ( {3} )
            ?otherTransactionRevision :{0} ?other.
            ?other :preceding{4} ?revision.
        }}""".format(revisionType, revisionFilter, timeConstrain, otherFilter, revisionType.title())

        if queryType is None:
            return subString

        queryString = "\nDESCRIBE ?revision" if queryType == 'DescribeQuery' else '\nSELECT ?revision'
        prefixString = '\n'.join((self.prefixRDF, self.prefixBiTR4Qs)) if prefix else ""

        SPARQLQuery = """{0}{1}
        WHERE {{ 
            {2}
        }}""".format(prefixString, queryString, subString)
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

    def _transaction_revisions_in_revision_graph(self, revisionA: URIRef, validRevisionTypes: list,
                                                 revisionB: URIRef = None):
        """

        :param revisionA:
        :param validRevisionTypes:
        :param revisionB:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch(revisionA, revisionB)
        revisionFilter = " || ".join(self._select_transaction_revision(pair) for pair in pairs)

        construct = '\n'.join("?revision :{0} ?{0} .".format(revisionType) for revisionType in validRevisionTypes)
        if len(validRevisionTypes) == 1:
            where = "?revision :{0} ?{0} .".format(validRevisionTypes[0])
        else:
            where = ' UNION '.join(
                "{{ ?revision :{0} ?{0} }}\n".format(revisionType) for revisionType in validRevisionTypes)

        SPARQLQuery = """CONSTRUCT {{ ?revision :revisionNumber ?revisionNumber .\n{0} }}
        WHERE {{
            ?revision :revisionNumber ?revisionNumber .
            OPTIONAL {{ ?revision :branch ?branch }}
            FILTER ( {1} )
            {2}
        }}""".format(construct, revisionFilter, where)
        stringOfRevisions = self._revisionStore.execute_construct_query(
                '\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')

        return stringOfRevisions

    def tags_in_revision_graph(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        tags = self._valid_revisions_in_graph(revisionA=revisionA, revisionType='tag', revisionB=revisionB,
                                              queryType='DescribeQuery')
        tagRevisions = self._transaction_revisions_in_revision_graph(revisionA, ['tag'], revisionB)
        tags = parser.TagParser.parse_sorted_combined(tags, tagRevisions)
        return tags

    def _sorted_snapshots(self, stringOfSnapshots, revisionA, revisionB=None, forward=True):
        snapshotRevisions = self._transaction_revisions_in_revision_graph(revisionA, ['snapshot'], revisionB)
        snapshots = parser.SnapshotParser.parse_sorted_combined(stringOfSnapshots, snapshotRevisions, forward=forward)
        return snapshots

    def _get_sorted_updates(self, updateParser, stringOfUpdates, revisionA: URIRef, revisionB: URIRef = None,
                            forward=True, quadPattern=None):
        """

        :param updateParser:
        :param stringOfUpdates:
        :param revisionA:
        :param revisionB:
        :param forward:
        :return:
        """
        updateParser.parse_sorted_combined(stringOfUpdates, None, forward=forward)

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
        pairs = self._get_pairs_of_revision_numbers_and_branch(revisionA, revisionB)
        revisionFilter = " || ".join(self._select_transaction_revision(pair) for pair in pairs)

        otherPairs = self._get_pairs_of_revision_numbers_and_branch(revisionB)

        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        updateTimeString = self._update_time_string(date=date)

        otherFilter = " || ".join(self._select_transaction_revision(
            pair, branch='?otherBranch', revisionNumber='?otherRevisionNumber') for pair in otherPairs)

        if self.config.related_update_content():
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')

            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?transactionRevision :revisionNumber ?revisionNumber .
                         OPTIONAL {{ ?transactionRevision :branch ?branch . }}
                         FILTER ( {1} )
                         ?transactionRevision :update ?revision.{2}
                         ?revision :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
                OPTIONAL {{ ?otherTransactionRevision :branch ?otherBranch . }}
                FILTER ( {3} )
                ?otherTransactionRevision :update ?precedingUpdate .{4}
                OPTIONAL {{ ?precedingUpdate :precedingUpdate* ?revision }}  
                {5} 
                }}""".format(construct, revisionFilter, updateTimeString, otherFilter, updatePrecedingTimeString, where)
        else:
            validRevisionTimeString = self._update_time_string(date=date, variableName='?validRevision')

            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?revision
                    WHERE {{
                         ?transactionRevision :revisionNumber ?revisionNumber .
                         OPTIONAL {{ ?transactionRevision :branch ?branch . }}
                         FILTER ( {1} )
                         ?transactionRevision :update ?validRevision.{2}
                         ?validRevision :precedingUpdate ?revision .
                        }}
                    }}
                ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
                OPTIONAL {{ ?otherTransactionRevision :branch ?otherBranch . }}
                FILTER ( {3} )
                ?otherTransactionRevision :update ?revision .{4}
                {5} }}
                """.format(construct, revisionFilter, validRevisionTimeString, otherFilter, updateTimeString, where)
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
        SPARQLQuery = """
        CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
            ?revision :{0} {1} .
            ?revision ?p ?o .
        }}""".format(revisionType, validRevisionID.n3())
        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))

    def _valid_revisions_from_transaction_revision(self, transactionRevisionID, revisionType):
        """

        :param transactionRevisionID:
        :param revisionType:
        :return:
        """
        if revisionType == 'update':
            where = """{0} :update ?revision .
            {{ GRAPH ?g {{ ?revision ?p1 ?o1 }} }} UNION {{ ?revision ?p2 ?o2 }}""".format(transactionRevisionID.n3())
        elif revisionType == 'snapshot':
            where = "{0} :snapshot ?revision .\n?revision ?p ?o .".format(transactionRevisionID.n3())
        elif revisionType == 'tag':
            where = "{0} :tag ?revision\n?revision ?p ?o .".format(transactionRevisionID.n3())
        elif revisionType == 'branch':
            where = """{0} :branch ?revision .
            OPTIONAL {{ {0} :update ?revision }}
            ?revision ?p ?o .""".format(transactionRevisionID.n3())
        elif revisionType == 'revert':
            where = """
            {0} :revert ?revision .
            OPTIONAL {{ {0} :update ?revision }}
            OPTIONAL {{ {0} :tag ?revision }}
            OPTIONAL {{ {0} :snapshot ?revision }}
            OPTIONAL {{ {0} :branch ?revision }}
            {{ GRAPH ?g {{ ?revision ?p1 ?o1 }} }} UNION {{ ?revision ?p2 ?o2 }}""".format(transactionRevisionID.n3())
        else:
            where = ""

        if revisionType == 'update' or revisionType == 'revert' or revisionType == 'branch':
            construct = 'GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2'
        else:
            construct = '?revision ?p ?o'

        SPARQLQuery = """CONSTRUCT {{ {0} }}
        WHERE {{ {1} }}""".format(construct, where)

        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))

    def get_updates_in_revision_graph(self, revisionA: URIRef, updateParser, quadPattern=None, revisionB: URIRef = None,
                                      forward=True, date: Literal = None, leftOfInterval: Literal = None,
                                      rightOfInterval: Literal = None, startTimeInBetween=False,
                                      endTimeInBetween=False):
        """

        :param revisionA:
        :param quadPattern:
        :param updateParser:
        :param revisionB:
        :param forward:
        :param date:
        :param leftOfInterval:
        :param rightOfInterval:
        :param startTimeInBetween:
        :param endTimeInBetween:
        :return:
        """
        timeConstrain = self._update_time_string(date, leftOfInterval, rightOfInterval, startTimeInBetween,
                                                 endTimeInBetween)

        updateWhere = self._valid_revisions_in_graph(revisionA=revisionA, revisionB=revisionB, queryType=None,
                                                     revisionType='update', prefix=False, timeConstrain=timeConstrain)
        if self._config.related_update_content():
            content = "\nOPTIONAL { ?revision :precedingUpdate* ?allRevisions }\n"
            construct, where = self._construct_where_for_update(quadPattern=quadPattern, subjectName='?allRevisions')
        else:
            content = ""
            construct, where = self._construct_where_for_update(quadPattern=quadPattern)

        constructRevisions = ""
        if self._config.sorted_modifications():
            constructRevisions = """\n?transactionRevision :revisionNumber ?revisionNumber .
            ?transactionRevision :update ?revision .
            """

        SPARQLQuery = """CONSTRUCT {{ {0}{4} }}
        WHERE {{ 
            {1} 
            {3}{2}
        }}""".format(construct, updateWhere, where, content, constructRevisions)

        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')

        if self._config.aggregated_modifications():
            updateParser.parse_aggregate(stringOfUpdates, forward)
        else:
            self._get_sorted_updates(updateParser, stringOfUpdates, revisionA, revisionB, forward,
                                     quadPattern=quadPattern)