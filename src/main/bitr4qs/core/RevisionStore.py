from rdflib.term import URIRef, Literal
from src.main.bitr4qs.store.QuadStoreSingleton import HttpRevisionStoreSingleton
import src.main.bitr4qs.tools.parser as parser
from src.main.bitr4qs.namespace import BITR4QS
from datetime import datetime
from src.main.bitr4qs.exception import RevisionConstructionError


class RevisionStore(object):

    prefixBiTR4Qs = 'PREFIX : <{0}>'.format(str(BITR4QS))
    prefixRDF = 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>'

    def __init__(self, config):
        self._config = config
        self._revisionStore = HttpRevisionStoreSingleton.get(config)

    @property
    def config(self):
        return self._config

    @property
    def revision_store(self):
        return self._revisionStore

    def number_of_quads_in_revision_store(self):
        """
        Function to obtain the number of quads and triples in the revision store.
        :return:
        """
        SPARQLQuery = """SELECT (Count(*) AS ?numQuads) 
        WHERE { { GRAPH ?g { ?s ?p ?o } } UNION { ?s ?p ?o } }"""
        results = self._revisionStore.execute_select_query(SPARQLQuery, 'json')
        if 'numQuads' in results['results']['bindings'][0]:
            return int(results['results']['bindings'][0]['numQuads']['value'])
        else:
            raise Exception

    def empty_revision_store(self):
        """
        Function to empty the revision store
        :return:
        """
        SPARQLQuery = "DROP ALL"
        self._revisionStore.execute_update_query(SPARQLQuery)

    def data_of_revision_store(self, returnFormat):
        return self._revisionStore.data_of_store(returnFormat)

    def revision_from_identifier(self, revisionID):
        """

        :param revisionID:
        :return:
        """
        isValidRevision = True
        if 'Revision' in revisionID:
            isValidRevision = False

        revisionType = self._get_revision_type(revisionID)
        if revisionType == 'update':
            construct = 'GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2'
            where = '{ GRAPH ?g { ?revision ?p1 ?o1 } } UNION { ?revision ?p2 ?o2 }'
        else:
            construct = where = '?revision ?p ?o'

        SPARQLQuery = "CONSTRUCT {{ {0} }}\nWHERE {{ {1} }}".format(construct, where)

        stringOfRevision = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        func = getattr(self, '_' + revisionType)
        revisions = func(stringOfRevision=stringOfRevision, isValidRevision=isValidRevision)
        return self._fetch_revision(revisions)

    @staticmethod
    def _fetch_revision(revisions):
        if len(revisions) == 1:
            for revisionID, revision in revisions.items():
                return revision
        else:
            return RevisionConstructionError('We could either construct no revision or multiple revisions.')

    @staticmethod
    def _get_revision_type(revisionID):
        if 'Update' in revisionID:
            revisionType = 'update'
        elif 'Snapshot' in revisionID:
            revisionType = 'snapshot'
        elif 'Tag' in revisionID:
            revisionType = 'tag'
        elif 'Branch' in revisionID:
            revisionType = 'branch'
        elif 'Revert' in revisionID:
            revisionType = 'revert'
        else:
            return Exception
        return revisionType

    def head_revision(self, branch: URIRef = None):
        """
        Function that returns the HEAD of the transaction revisions based on the given branch.
        :param branch: the given branch for which one would like to have the HEAD Revision.
        :return: an HEAD revision object containing all data of the HEAD Revision.
        """
        branchString = "?revision :branch {0} .".format(branch.n3()) if branch else \
            "FILTER NOT EXISTS { ?revision :branch ?branch . }"

        SPARQLQuery = """DESCRIBE ?revision
        WHERE {{ 
            ?revision rdf:type :HeadRevision . 
            {1} 
        }}""".format(str(BITR4QS), branchString)

        result = self._revisionStore.execute_describe_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        headRevisions = parser.HeadParser.parse_revisions(result, 'transaction')
        return self._fetch_revision(headRevisions)

    def new_branch_index(self):
        return None

    @staticmethod
    def new_revision_number(revisionNumber=None):
        return None

    @staticmethod
    def main_branch_index():
        return None

    def preceding_modifications(self, updateID: URIRef):
        SPARQLQuery = """CONSTRUCT {{
            GRAPH ?g {{ ?update ?p1 ?o1 }}
            ?update ?p2 ?o2 .
        }}
        WHERE {{
        {0} :precedingUpdate* ?update .
        {{ GRAPH ?g {{ ?update ?p1 ?o1 }} }} UNION {{  ?update ?p2 ?o2 . }}
        }}""".format(updateID.n3())
        print("SPARQLQuery ", SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        print("stringOfUpdates ", stringOfUpdates)
        updateParser = parser.UpdateParser()
        updateParser.parse_aggregate(stringOfUpdates, forward=True)
        modifications = updateParser.get_list_of_modifications()
        print('\n'.join(str(a) for a in modifications))
        return modifications

    def preceding_revisions(self, revisionID: URIRef, revisionType=None, isValidRevision=True, propertyPath=''):
        """
        Function to obtain the preceding revision(s) of the given valid or transaction revision.
        :param revisionID: The identifier of the given revision.
        :param revisionType: The type of the revision (update, revert, branch, merge, snapshot, tag)
        :param isValidRevision: True if revision is a valid revision, False if transaction revision
        :param propertyPath: ['', '+', '*']
        :return:
        """
        if revisionType is None:
            revisionType = self._get_revision_type(revisionID)
        func = getattr(self, '_' + revisionType)

        if not isValidRevision:
            revisionType = 'revision'

        if revisionType == 'update':
            construct = 'GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2'
            where = '{ GRAPH ?g { ?revision ?p1 ?o1 } } UNION { ?revision ?p2 ?o2 }'
        else:
            construct = where = '?revision ?p ?o'

        SPARQLQuery = """CONSTRUCT {{ {0} }}
        WHERE {{ {1} :preceding{2}{3} ?revision .
        {4} }}""".format(construct, revisionID.n3, revisionType.title(), propertyPath, where)
        stringOfRevisions = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        revisions = func(stringOfRevisions, isValidRevision=isValidRevision)
        if len(propertyPath) == 0:
            return self._fetch_revision(revisions)
        return revisions

    def transaction_from_valid_and_valid_from_transaction(self, revisionID, transactionFromValid=True,
                                                          revisionType=None):
        """
        Function to obtain the transaction revision from a valid revision identifier or to obtain the valid
        revision(s) from a transaction revision identifier.
        :param revisionID: The identifier of the given revision.
        :param transactionFromValid: True if transaction revision from a valid revision
        :param revisionType: The type of the revision (update, revert, branch, merge, snapshot, tag)
        :return:
        """
        if not revisionType:
            revisionType = self._get_revision_type(revisionID)

        if transactionFromValid:
            SPARQLQuery = self._transaction_revision_from_valid_revision(revisionID, revisionType)
        else:
            SPARQLQuery = self._valid_revisions_from_transaction_revision(revisionID, revisionType)
        stringOfRevision = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        func = getattr(self, '_' + revisionType)
        revisions = func(stringOfRevision=stringOfRevision, isValidRevision=not transactionFromValid)
        if transactionFromValid:
            return self._fetch_revision(revisions)
        return revisions

    def _transaction_revision_from_valid_revision(self, validRevisionID, revisionType):
        return ""

    def _valid_revisions_from_transaction_revision(self, transactionRevisionID, revisionType):
        return ""

    def revision(self, revisionID: URIRef, transactionRevisionA: URIRef, revisionType: str = 'unknown',
                 isValidRevision=True, transactionRevisionB: URIRef = None):
        """
        Function to obtain the valid or transaction revision from its identifier and check its existence in the given
        transaction revision graph.
        :param revisionID: The identifier of the given revision.
        :param revisionType: The type of the revision (update, revert, branch, merge, snapshot, tag)
        :param isValidRevision: True if revision is a valid revision, False if transaction revision
        :param transactionRevisionA:
        :param transactionRevisionB:
        :return:
        """
        if isValidRevision:
            SPARQLQuery = self._valid_revision(transactionRevisionA=transactionRevisionA, validRevision=revisionID,
                                               revisionType=revisionType, transactionRevisionB=transactionRevisionB)
        else:
            SPARQLQuery = self._transaction_revision(transactionRevisionA=transactionRevisionA,
                                                     transactionRevision=revisionID,
                                                     transactionRevisionB=transactionRevisionB)
        print("SPARQLQuery ", SPARQLQuery)
        stringOfRevision = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        func = getattr(self, '_' + revisionType)
        revisions = func(stringOfRevision=stringOfRevision, isValidRevision=isValidRevision)
        return self._fetch_revision(revisions)

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):
        return ""

    def _valid_revision(self, transactionRevisionA: URIRef, validRevision: URIRef, revisionType,
                        transactionRevisionB: URIRef = None):

        subQuery = self._valid_revisions_in_graph(transactionRevisionA, revisionType, queryType='SelectQuery',
                                                  revisionB=transactionRevisionB, prefix=False)

        if revisionType == 'update':
            construct = "GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2 ."
            where = "{ GRAPH ?g { ?revision ?p1 ?o1 } } UNION { ?revision ?p2 ?o2 }"
        else:
            construct = where = "?revision ?p ?o ."

        SPARQLQuery = """CONSTRUCT {{ {0} }}
        WHERE {{ {{ {1} }}
        FILTER ( {2} = ?revision ) 
        {3} }}""".format(construct, subQuery, validRevision.n3(), where)
        return '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery))

    @staticmethod
    def _unknown(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            revision = parser.Parser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            revision = parser.Parser.parse_revisions(stringOfRevision, revisionName='transaction')
        return revision

    @staticmethod
    def _branch(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            branch = parser.BranchParser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            branch = parser.BranchParser.parse_revisions(stringOfRevision, revisionName='transaction')
        return branch

    @staticmethod
    def _snapshot(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            snapshot = parser.SnapshotParser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            snapshot = parser.SnapshotParser.parse_revisions(stringOfRevision, revisionName='transaction')
        return snapshot

    @staticmethod
    def _tag(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            tag = parser.TagParser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            tag = parser.TagParser.parse_revisions(stringOfRevision, revisionName='transaction')
        return tag

    @staticmethod
    def _update(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            update = parser.UpdateParser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            update = parser.UpdateParser.parse_revisions(stringOfRevision, revisionName='transaction')
        return update

    def branch_from_name(self, branchName: Literal):
        """
        Function to obtain the Branch from its name.
        :param branchName: The name of the Branch
        :return:
        """
        SPARQLQuery = """DESCRIBE ?branch
        WHERE {{ ?branch :branchName {0} }}""".format(branchName.n3())
        strOfBranch = self._revisionStore.execute_describe_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        branches = parser.BranchParser.parse_revisions(strOfBranch, revisionName='valid')
        return self._fetch_revision(branches)

    def tag_from_name(self, tagName: Literal):
        """
        Function to obtain the Tag from its name.
        :param tagName:
        :return:
        """
        SPARQLQuery = """DESCRIBE ?tag
        WHERE {{ ?tag :tagName {0} }}""".format(tagName.n3())
        stringOfTag = self._revisionStore.execute_describe_query('\n'.join((self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        tags = parser.TagParser.parse_revisions(stringOfTag, revisionName='valid')
        return self._fetch_revision(tags)

    def can_quad_be_modified(self, quad, revisionA: URIRef, revisionB: URIRef, revisionC: URIRef = None,
                             startDate: Literal = None, endDate: Literal = None, deletion=False):

        if startDate:
            startDate = datetime.strptime(str(startDate), "%Y-%m-%dT%H:%M:%S+00:00")
        if endDate:
            endDate = datetime.strptime(str(endDate), "%Y-%m-%dT%H:%M:%S+00:00")

        if self.config.related_update_content():
            content = "\n?revision :precedingUpdate* ?allUpdate .\n"
            if deletion:
                where = quad.query_via_insert_update(construct=False, subjectName='?allUpdate')
            else:
                where = quad.query_via_delete_update(construct=False, subjectName='?allUpdate')
        else:
            content = ""
            if deletion:
                where = quad.query_via_insert_update(construct=False, subjectName='?revision')
            else:
                where = quad.query_via_delete_update(construct=False, subjectName='?revision')

        # Obtain subquery between two transaction revisions
        subQueryBetween = self._valid_revisions_in_graph(revisionA=revisionA, revisionB=revisionB, prefix=False,
                                                         queryType='SelectQuery', revisionType='update')

        SPARQLQuery = """SELECT ?revision ?startDate ?endDate
        WHERE {{ {{ {0} }}
        OPTIONAL {{ ?revision :startedAt ?startDate . }} 
        OPTIONAL {{ ?revision :endedAt ?endDate . }} 
        {1}{2} 
        }}""".format(subQueryBetween, content, where)
        print("SPARQLQuery ", SPARQLQuery)
        results = self._revisionStore.execute_select_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'json')

        for result in results['results']['bindings']:
            if not revisionC and 'revision' in result:
                return False

            if 'startDate' in result:
                otherStartDate = datetime.strptime(result['startDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")
                if startDate and startDate > otherStartDate:
                    return False
            else:
                if startDate:
                    return False

            if 'endDate' in result:
                otherEndDate = datetime.strptime(result['endDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")
                if endDate and endDate < otherEndDate:
                    return False
            else:
                if endDate:
                    return False

        # Obtain subquery before a transaction revisions
        subQueryBefore = self._valid_revisions_in_graph(revisionA=revisionC, queryType='SelectQuery',
                                                        revisionType='update', prefix=False)

        SPARQLQuery = """SELECT ?revision ?startDate ?endDate
        WHERE {{ {{ {0} }}
        OPTIONAL {{ ?revision :startedAt ?startDate . }} 
        OPTIONAL {{ ?revision :endedAt ?endDate . }} 
        {1}{2}
        }}""".format(subQueryBefore, content, where)
        print("SPARQLQuery ", SPARQLQuery)
        results = self._revisionStore.execute_select_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'json')

        for result in results['results']['bindings']:
            if 'startDate' in result:
                otherStartDate = datetime.strptime(result['startDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")
                if startDate and otherStartDate > startDate:
                    return False
                elif not startDate:
                    return False

            if 'endDate' in result:
                otherEndDate = datetime.strptime(result['endDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")
                if endDate and otherEndDate < endDate:
                    return False
                elif not endDate:
                    return False

        return True

    def can_quad_be_added_or_deleted(self, quad, headRevision: URIRef, startDate: Literal = None,
                                     endDate: Literal = None, deletion=False):
        if startDate:
            startDate = datetime.strptime(str(startDate), "%Y-%m-%dT%H:%M:%S+00:00")

        if endDate:
            endDate = datetime.strptime(str(endDate), "%Y-%m-%dT%H:%M:%S+00:00")

        if self.config.related_update_content():
            content = "\n?revision :precedingUpdate* ?allUpdate ."
            where = quad.query_via_unknown_update(construct=False, subjectName='?allUpdate')
        else:
            content = ""
            where = quad.query_via_unknown_update(construct=False, subjectName='?revision')

        # Obtain subquery before a transaction revisions
        subQuery = self._valid_revisions_in_graph(revisionA=headRevision, queryType='SelectQuery',
                                                  revisionType='update', prefix=False)
        SPARQLQuery = """SELECT ?p ?startDate ?endDate
        WHERE {{ {{ {0} }}
        OPTIONAL {{ ?revision :startedAt ?startDate . }}
        OPTIONAL {{ ?revision :endedAt ?endDate . }}
        {1}{2}
        }}""".format(subQuery, content, where)

        results = self._revisionStore.execute_select_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'json')
        numberInsertions = 0
        numberDeletions = 0

        for result in results['results']['bindings']:
            otherStartDate = None
            if 'startDate' in result:
                otherStartDate = datetime.strptime(result['startDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")
            otherEndDate = None
            if 'endDate' in result:
                otherEndDate = datetime.strptime(result['endDate']['value'], "%Y-%m-%dT%H:%M:%S+00:00")

            if deletion and 'http://bi-tr4qs.org/vocab/inserts' == result['p']['value']:
                if startDate and otherStartDate and startDate < otherStartDate:
                    continue
                elif not startDate and otherStartDate:
                    continue

                if endDate and otherEndDate and endDate > otherEndDate:
                    continue
                elif not endDate and otherEndDate:
                    continue

                numberInsertions += 1

            elif deletion and 'http://bi-tr4qs.org/vocab/deletes' == result['p']['value']:
                if endDate and otherStartDate and endDate < otherStartDate:
                    continue
                if startDate and otherEndDate and startDate > otherEndDate:
                    continue

                numberDeletions += 1

            elif not deletion and 'http://bi-tr4qs.org/vocab/inserts' == result['p']['value']:
                if endDate and otherStartDate and endDate < otherStartDate:
                    continue
                if startDate and otherEndDate and startDate > otherEndDate:
                    continue

                numberInsertions += 1

            elif not deletion and 'http://bi-tr4qs.org/vocab/deletes' == result['p']['value']:
                if startDate and otherStartDate and startDate < otherStartDate:
                    continue
                elif not startDate and otherStartDate:
                    continue

                if endDate and otherEndDate and endDate > otherEndDate:
                    continue
                elif not endDate and otherEndDate:
                    continue

                numberDeletions += 1

        if deletion and numberDeletions + 1 == numberInsertions:
            return True
        if not deletion and numberDeletions == numberInsertions:
            return True
        return False

    # def can_quad_be_added_or_deleted(self, quad, headRevision: URIRef, startDate: Literal = None,
    #                                  endDate: Literal = None, deletion=False):
    #     """
    #     - A quad Q can be inserted if N=M, where N stands for the number of updates that insert Q and somewhere overlap,
    #     and where M stands for the number of updates that deletes Q and fully overlap
    #     - A quad Q with valid time t can be deleted if N=M+1, where N stands for the number of updates that insert Q and
    #     fully overlap t and where M stands for the number of updates that deletes Q and somewhere overlap t
    #     :param quad:
    #     :param headRevision:
    #     :param startDate:
    #     :param endDate:
    #     :param deletion:
    #     :return:
    #     """
    #
    #     assert headRevision is not None, "The HEAD Revision is not allowed to be None."
    #
    #     updateWhere = self._valid_revisions_in_graph(revisionA=headRevision, queryType='SelectQuery',
    #                                                  revisionType='update', prefix=False)
    #
    #     if deletion:
    #         stringA = quad.query_via_delete_update(construct=False, subjectName='?revision')
    #         stringB = quad.query_via_insert_update(construct=False, subjectName='?revision')
    #     else:
    #         stringA = quad.query_via_insert_update(construct=False, subjectName='?revision')
    #         stringB = quad.query_via_delete_update(construct=False, subjectName='?revision')
    #
    #     if startDate is None and endDate is None:
    #         timeString = """{{ {0} }}
    #         UNION
    #         {{ {1}
    #         NOT EXISTS {{ ?revision :endedAt ?endDate }}
    #         NOT EXISTS {{ ?revision :startedAt ?startDate }} }}
    #         """.format(stringA, stringB)
    #     elif startDate is None:
    #         timeString = """{{ {0}
    #         OPTIONAL {{ ?revision :startedAt ?startDate . }}
    #         FILTER ( !bound(?startDate) || ?startDate <= {2} )
    #         }}
    #         UNION
    #         {{ {1}
    #         ?revision :endedAt ?endDate . FILTER ( {2} <= ?endDate )
    #         }}
    #         UNION
    #         {{ {1}
    #         NOT EXISTS {{ ?revision :endedAt ?endDate }}
    #         NOT EXISTS {{ ?revision :startedAt ?startDate }}
    #         }}
    #         """.format(stringA, stringB, endDate.n3())
    #     elif endDate is None:
    #         timeString = """{{ {0}
    #         OPTIONAL {{ ?revision :endedAt ?endDate . }}
    #         FILTER ( !bound(?endDate) || {2} <= ?endDate )
    #         }}
    #         UNION
    #         {{ {1}
    #         ?revision :startedAt ?startDate . FILTER ( ?startDate >= {2} )
    #         }}
    #         UNION
    #         {{ {1}
    #         NOT EXISTS {{ ?revision :endedAt ?endDate }}
    #         NOT EXISTS {{ ?revision :startedAt ?startDate }}
    #         }}
    #         """.format(stringA, stringB, startDate.n3())
    #     else:
    #         timeString = """{{
    #             {0}
    #             OPTIONAL {{ ?revision :startedAt ?startDate }}
    #             FILTER ( !bound(?startDate) || ?startDate <= {3} )
    #             OPTIONAL {{ ?revision :endedAt ?endDate }}
    #             FILTER ( !bound(?endDate) || {2} <= ?endDate )
    #         }} UNION {{
    #             {1}
    #             OPTIONAL {{ ?revision :startedAt ?startDate }}
    #             FILTER (!bound(?startDate) || ?startDate <= {2} )
    #             OPTIONAL {{ ?revision :endedAt ?endDate }}
    #             FILTER ( !bound(?endDate) || {3} <= ?endDate )
    #         }} """.format(stringA, stringB, startDate.n3(), endDate.n3())
    #
    #     if self.config.related_update_content():
    #         content = "\n?revision :precedingUpdate* ?allUpdate ."
    #         construct = quad.query_via_unknown_update(construct=True, subjectName='?allUpdate')
    #         where = quad.query_via_unknown_update(construct=False, subjectName='?allUpdate')
    #     else:
    #         content = ""
    #         construct = quad.query_via_unknown_update(construct=True, subjectName='?revision')
    #         where = quad.query_via_unknown_update(construct=False, subjectName='?revision')
    #
    #     SPARQLQuery = """CONSTRUCT {{ {0} }}
    #     WHERE {{
    #         {{ {1} }}
    #         {2}{3}
    #         {4}
    #     }}""".format(construct, updateWhere, timeString, content, where)
    #
    #     # print("SPARQLQuery ", SPARQLQuery)
    #     stringOfUpdates = self._revisionStore.execute_construct_query(
    #         '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
    #     # print("stringOfUpdates ", stringOfUpdates)
    #     updateParser = parser.UpdateParser()
    #     updateParser.parse_aggregate(stringOfUpdates, forward=True)
    #     modifications = updateParser.get_list_of_modifications()
    #
    #     if deletion:
    #         if len(modifications) == 1 and modifications[0].insertion:
    #             return True
    #         else:
    #             return False
    #     else:
    #         if len(modifications) == 0:
    #             return True
    #         else:
    #             return False

    @staticmethod
    def _update_time_string(date: Literal = None, leftOfInterval: Literal = None, rightOfInterval: Literal = None,
                            startTimeInBetween=False, endTimeInBetween=False, variableName='?revision'):
        timeConstrains = ""
        if date:
            timeConstrains = """
            OPTIONAL {{ {1} :startedAt ?startDate . }} 
            FILTER ( !bound(?startDate) || ?startDate <= {0} )
            OPTIONAL {{ {1} :endedAt ?endDate . }}
            FILTER ( !bound(?endDate) || ?endDate >= {0} )
            """.format(date.n3(), variableName)
        elif not date and startTimeInBetween and not endTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER (  ?startDate > {0} && ?startDate <= {1} && ?endDate > {1} )
            }} UNION {{
                {2} :startedAt ?startDate .
                NOT EXISTS {{ {2} :endedAt ?endDate . }}
                FILTER (  ?startDate > {0} && ?startDate <= {1} ) 
                }}""".format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        elif not date and endTimeInBetween and not startTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER ( ?endDate >= {0} && ?endDate < {1} && ?startDate < {0} )
            }} UNION {{
                {2} :endedAt ?endDate .
                NOT EXISTS {{ {2} :startedAt ?startDate . }}
                FILTER (  ?endDate >= {0} && ?endDate < {1} ) 
                }}""".format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        return timeConstrains

    def _construct_where_for_update(self, quadPattern):
        construct = where = ""
        if self.config.query_all_updates() or not quadPattern:
            construct = "GRAPH ?g { ?revision ?p1 ?o1 }\n?revision ?p2 ?o2 ."
            where = "{ GRAPH ?g { ?revision ?p1 ?o1 } } UNION { ?revision ?p2 ?o2 }"
        elif self.config.query_specific_updates():
            construct = quadPattern.query_via_unknown_update(construct=True)
            where = quadPattern.query_via_unknown_update(construct=False)
        else:
            # Raise an exception that one of the two update fetching strategies should be True
            pass
        return construct, where

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

        updateWhere = self._valid_revisions_in_graph(revisionA=revisionA, revisionB=revisionB, queryType='SelectQuery',
                                                     revisionType='update', prefix=False, timeConstrain=timeConstrain)
        construct, where = self._construct_where_for_update(quadPattern=quadPattern)

        content = ""
        if self.config.related_update_content():
            content = "\n?revision :precedingUpdate* ?revision ."

        SPARQLQuery = """CONSTRUCT {{ {0} }}
        WHERE {{ 
            {{ 
                {1} 
            }}
            {2}{3}
        }}""".format(construct, updateWhere, where, content)
        # print("SPARQLQuery ", SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(
            '\n'.join((self.prefixRDF, self.prefixBiTR4Qs, SPARQLQuery)), 'nquads')
        updateParser.parse_aggregate(stringOfUpdates, forward)

    def _valid_revisions_in_graph(self, revisionA: URIRef, revisionType: str, queryType: str,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param revisionType:
        :param queryType:
        :param revisionB:
        :param prefix:
        :param timeConstrain:
        :return:
        """
        return ""

    def closest_snapshot(self, validTime: Literal, headRevision: URIRef):
        """
        Function to return the closest Snapshot from the HEAD revision in the revision graph.
        :param validTime: The time to determine the closest Snapshot from.
        :param headRevision: The latest transaction revision in the revision graph.
        :return:
        """
        stringOfSnapshots = self._valid_revisions_in_graph(revisionA=headRevision, queryType='DescribeQuery',
                                                           revisionType='snapshot')

        snapshots = parser.SnapshotParser.parse_revisions(stringOfSnapshots, 'valid')
        if len(snapshots) == 0:
            return None

        referenceTime = datetime.strptime(str(validTime), "%Y-%m-%dT%H:%M:%S+02:00")

        minimumDifference = None
        minimumSnapshot = None
        for snapshotID, snapshot in snapshots.items():

            time = datetime.strptime(str(snapshot.effective_date), "%Y-%m-%dT%H:%M:%S+02:00")
            difference = referenceTime - time if referenceTime > time else time - referenceTime

            if minimumDifference is None:
                minimumDifference = difference
            elif difference < minimumDifference:
                minimumDifference = difference
                minimumSnapshot = snapshot

        return minimumSnapshot

    def is_transaction_time_a_earlier(self, revisionA: URIRef, revisionB: URIRef) -> bool:
        """
        Function to determine whether transaction revision A happens before transaction revision B.
        :param revisionA: Transaction revision A
        :param revisionB: Transaction revision B
        :return:
        """
        return True
