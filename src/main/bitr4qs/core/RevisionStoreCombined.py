from .RevisionStore import RevisionStore
from rdflib.term import URIRef, Literal
import src.main.bitr4qs.tools.parser as parser
from rdflib.namespace import XSD
from datetime import datetime, timedelta
from timeit import default_timer as timer
from .RevisionStoreImplicit import RevisionStoreImplicit


class RevisionStoreCombined(RevisionStoreImplicit):

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

    def new_branch_index(self, branch=None):
        """
        Function to obtain a new branch index based on the existing branches
        :return:
        """

        if branch is not None:
            return branch.branch_index, None

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

        return branchIndex, None

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):

        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(transactionRevisionA, transactionRevisionB)
        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        WHERE {{ 
        ?revision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?revision :branchIndex ?branchIndex .}}
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
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)

        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)
        otherFilter = " || ".join(self._select_revision(pair, revisionNumber='?otherRevisionNumber',
                                                        branchIndex='?otherBranchIndex') for pair in pairs)

        subString = """
        ?transactionRevision :revisionNumber ?revisionNumber .
        OPTIONAL {{ ?transactionRevision :branchIndex ?branchIndex . }}
        FILTER ( {1} )
        ?transactionRevision :{0} ?revision.{2}
        MINUS {{
            ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
            OPTIONAL {{ ?otherTransactionRevision :branchIndex ?otherBranchIndex . }}
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

    def _transaction_revisions_in_revision_graph(self, revisionA: URIRef, validRevisionTypes: list,
                                                 revisionB: URIRef = None):
        """

        :param revisionA:
        :param validRevisionTypes:
        :param revisionB:
        :return:
        """
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)
        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)

        construct = '\n'.join("?revision :{0} ?{0} .".format(revisionType) for revisionType in validRevisionTypes)
        if len(validRevisionTypes) == 1:
            where = "?revision :{0} ?{0} .".format(validRevisionTypes[0])
        else:
            where = ' UNION '.join(
                "{{ ?revision :{0} ?{0} }}\n".format(revisionType) for revisionType in validRevisionTypes)

        SPARQLQuery = """CONSTRUCT {{ ?revision :revisionNumber ?revisionNumber .\n{0} }}
        WHERE {{
            ?revision :revisionNumber ?revisionNumber .
            OPTIONAL {{ ?revision :branchIndex ?branchIndex }}
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
                            forward=True):
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
        pairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionA, revisionB)
        revisionFilter = " || ".join(self._select_revision(pair) for pair in pairs)

        otherPairs = self._get_pairs_of_revision_numbers_and_branch_indices(revisionB)

        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        updateTimeString = self._update_time_string(date=date)

        otherFilter = " || ".join(self._select_revision(
            pair, branchIndex='?otherBranchIndex', revisionNumber='?otherRevisionNumber') for pair in otherPairs)

        if self.config.related_update_content():
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')

            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         ?transactionRevision :revisionNumber ?revisionNumber .
                         OPTIONAL {{ ?transactionRevision :branchIndex ?branchIndex . }}
                         FILTER ( {1} )
                         ?transactionRevision :update ?revision.{2}
                         ?revision :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                    {{
                    SELECT ?revision
                    WHERE {{
                        ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
                        OPTIONAL {{ ?otherTransactionRevision :branchIndex ?otherBranchIndex . }}
                        FILTER ( {3} )
                        ?otherTransactionRevision :update ?precedingUpdate .{4}
                        OPTIONAL {{ ?precedingUpdate :precedingUpdate* ?revision }}  
                        }}
                    }}
                {5}
                FILTER ( ?revision = ?precedingUpdate )
                }}""".format(construct, revisionFilter, updateTimeString, otherFilter, updatePrecedingTimeString, where)
        else:
            validRevisionTimeString = self._update_time_string(date=date, variableName='?validRevision')
            SPARQLQuery = """CONSTRUCT {{ {0} }}
            WHERE {{
                    {{
                    SELECT ?other
                    WHERE {{
                         ?transactionRevision :revisionNumber ?revisionNumber .
                         OPTIONAL {{ ?transactionRevision :branchIndex ?branchIndex . }}
                         FILTER ( {1} )
                         ?transactionRevision :update ?validRevision.{2}
                         ?validRevision :precedingUpdate ?other .
                        }}
                    }}
                    {{
                    SELECT ?revision
                    WHERE {{
                        ?otherTransactionRevision :revisionNumber ?otherRevisionNumber .
                        OPTIONAL {{ ?otherTransactionRevision :branchIndex ?otherBranchIndex . }}
                        FILTER ( {3} )
                        ?otherTransactionRevision :update ?revision .{4}
                        }}
                    }}
                {5}
                FILTER ( ?revision = ?other )
                }}
                """.format(construct, revisionFilter, validRevisionTimeString, otherFilter, updateTimeString, where)
        # print('SPARQLQuery ', SPARQLQuery)
        # start = timer()
        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        # end = timer()
        # print("get modified updates ", timedelta(seconds=end - start).total_seconds())
        # print("stringOfUpdates ", stringOfUpdates)
        # start = timer()
        updateParser.parse_aggregate(stringOfUpdates, forward)
        # end = timer()
        # print("aggregate updates ", timedelta(seconds=end - start).total_seconds())

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
            if self._config.related_update_content():
                constructRevisions = """\n?transactionRevision :revisionNumber ?revisionNumber .
                ?transactionRevision :update ?allRevisions ."""
            else:
                constructRevisions = """\n?transactionRevision :revisionNumber ?revisionNumber .
                ?transactionRevision :update ?revision ."""

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
            self._get_sorted_updates(updateParser, stringOfUpdates, revisionA, revisionB, forward)