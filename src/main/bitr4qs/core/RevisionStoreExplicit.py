from .RevisionStore import RevisionStore
from rdflib.term import Literal, URIRef
import src.main.bitr4qs.tools.parser as parser
from src.main.bitr4qs.namespace import BITR4QS


class RevisionStoreExplicit(RevisionStore):

    def get_modifications_of_updates_between_revisions(self, revisionA, revisionB, date, updateParser, quadPattern,
                                                       forward=True):
        updateSucceedingTimeString = self._update_time_string(date=date, variableName='?succeedingUpdate')
        construct, where = self._construct_where_for_update(quadPattern=quadPattern)
        if self.config.related_update_content():
            updatePrecedingTimeString = self._update_time_string(date=date, variableName='?precedingUpdate')
            SPARQLQuery = """PREFIX : <{0}>
            CONSTRUCT {{ {1} }}
            WHERE {{
                    {{
                    SELECT ?precedingUpdate
                    WHERE {{
                         {2} :precedingRevision* ?revision .
                         ?revision :precedingRevision+ {3} .
                         ?revision :update ?succeedingUpdate .{4}
                         ?succeedingUpdate :precedingUpdate ?precedingUpdate .
                        }}
                    }}
                {3} :precedingRevision* ?revision .
                ?revision :update ?precedingUpdate .{5}
                ?precedingUpdate :precedingUpdate* ?revision .
                {6} }}""".format(str(BITR4QS), construct, revisionA.n3(), revisionB.n3(), updateSucceedingTimeString,
                                 updatePrecedingTimeString, where)
        else:
            updateTimeString = self._update_time_string(date=date, variableName='?revision')
            SPARQLQuery = """PREFIX : <{0}>
            CONSTRUCT {{ {1} }}
            WHERE {{
                    {{
                    SELECT ?revision
                    WHERE {{
                         {2} :precedingRevision* ?revision .
                         ?revision :precedingRevision+ {3} .
                         ?revision :update ?succeedingUpdate .{4}
                         ?succeedingUpdate :precedingUpdate ?revision .
                        }}
                    }}
                {3} :precedingRevision* ?revision .
                ?revision :update ?revision .{5}
                {6} }}""".format(str(BITR4QS), construct, revisionA.n3(), revisionB.n3(), updateSucceedingTimeString,
                                 updateTimeString, where)

        # print('SPARQLQuery ', SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        # print("stringOfUpdates ", stringOfUpdates)
        updateParser.parse_aggregate(stringOfUpdates, forward)

    def _tag_revisions_in_revision_graph(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        revisionEndConstrain = ""
        if revisionB is not None:
            revisionEndConstrain = "\n?revision :precedingRevision+ {0} .".format(revisionB.n3())

        SPARQLQuery = """PREFIX : <{0}>
        CONSTRUCT {{ ?revision :precedingRevision ?precedingRevision . 
        ?revision :tag ?tag . }}
        WHERE {{ {1} :precedingRevision* ?revision .{2}
        ?revision :precedingRevision ?precedingRevision .
        ?revision :tag ?tag . 
        }}""".format(str(BITR4QS), revisionA.n3(), revisionEndConstrain)
        stringOfTagRevisions = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        return stringOfTagRevisions

    def tags_in_revision_graph(self, revisionA: URIRef, revisionB: URIRef = None):
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        tags = self._valid_revisions_in_graph(revisionA=revisionA, revisionType='tag', revisionB=revisionB,
                                              queryType='DescribeQuery')
        tagRevisions = self._tag_revisions_in_revision_graph(revisionA, revisionB)
        tags = parser.TagParser.parse_sorted_explicit(tags, tagRevisions, endRevision=revisionA)
        return tags

    def is_transaction_time_a_earlier(self, revisionA, revisionB) -> bool:
        """

        :param revisionA:
        :param revisionB:
        :return:
        """
        SPARQLQuery = "ASK {{ {0} :precedingRevision+ {1} }}".format(revisionB.n3(), revisionA.n3())
        result = self._revisionStore.execute_ask_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)))
        return result

    def _transaction_revision(self, transactionRevisionA: URIRef, transactionRevisionID: URIRef,
                              transactionRevisionB: URIRef = None):
        """

        :param transactionRevisionA:
        :param transactionRevisionID:
        :param transactionRevisionB:
        :return:
        """
        earlyStop = ""
        if transactionRevisionB is not None:
            earlyStop = "\n?revision :precedingRevision+ {0} .".format(transactionRevisionB.n3())

        SPARQLQuery = """CONSTRUCT {{ ?revision ?p ?o }}
        {{ {0} :precedingRevision* ?revision .{1}
        FILTER ( {2} = ?revision ) 
        ?revision ?p ?o . }}""".format(transactionRevisionA.n3(), earlyStop, transactionRevisionID.n3())
        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))

    def _valid_revisions_in_graph(self, revisionA: URIRef, revisionType: str, queryType: str,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param revisionType:
        :param revisionB:
        :return:
        """
        earlyStopA = earlyStopB = ""
        if revisionB is not None:
            earlyStopA = "\n?transactionRevision :precedingRevision+ {0} .".format(revisionB.n3())
            earlyStopB = "\n?otherTransactionRevision :precedingRevision+ {0} .".format(revisionB.n3())

        queryString = "DESCRIBE" if queryType == 'DescribeQuery' else 'SELECT'
        prefixString = "PREFIX : <{0}>".format(str(BITR4QS)) if prefix else ""

        SPARQLQuery = """{0}
        {6} ?revision
        WHERE {{ {2} :precedingRevision* ?transactionRevision .{3}
            ?transactionRevision :{1} ?revision .{7}
            MINUS {{
                {2} :precedingRevision* ?otherTransactionRevision .{4}
                ?otherTransactionRevision :{1} ?otherValidRevision .
                ?otherValidRevision :preceding{5} ?revision . 
            }}
        }}""".format(prefixString, revisionType, revisionA.n3(), earlyStopA, earlyStopB, revisionType.title(),
                     queryString, timeConstrain)

        if prefix and queryType == 'DescribeQuery':
            stringOfValidRevisions = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
            return stringOfValidRevisions
        else:
            return SPARQLQuery

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

        if revisionType == 'update' or revisionType == 'revert':
            construct = 'GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2'
        else:
            construct = '?revision ?p ?o'

        SPARQLQuery = """CONSTRUCT {{ {0} }}
        WHERE {{ {1} }}""".format(construct, where)

        return '\n'.join((self.prefixBiTR4Qs, SPARQLQuery))

