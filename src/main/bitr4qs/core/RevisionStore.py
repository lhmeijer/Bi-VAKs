from rdflib.term import URIRef, Literal
from src.main.bitr4qs.store.QuadStoreSingleton import HttpRevisionStoreSingleton
import src.main.bitr4qs.tools.parser as parser
from src.main.bitr4qs.namespace import BITR4QS
from datetime import datetime


class RevisionStore(object):

    def __init__(self, config):
        self._config = config
        self._revisionStore = HttpRevisionStoreSingleton.get(config)

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config

    @property
    def revision_store(self):
        return self._revisionStore

    def head_revision(self, branch: URIRef = None):
        """
        Function that returns the HEAD of the transaction revisions based on the given branch.
        :param branch: the given branch for which one would like to have the HEAD Revision.
        :return: an HEAD revision object containing all data of the HEAD Revision.
        """
        branchString = "?revision :branch {0} .".format(branch.n3()) if branch is not None else \
            "FILTER NOT EXISTS { ?revision :branch ?branch . }"

        SPARQLQuery = """
        PREFIX : <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        DESCRIBE ?revision
        WHERE {{ 
            ?revision rdf:type :HeadRevision . 
            {1} 
        }}""".format(str(BITR4QS), branchString)

        result = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        headRevisions = parser.HeadParser.parse_revisions(result, 'transaction')

        if len(headRevisions) == 1:
            for revisionID, revision in headRevisions.items():
                return revision
        else:
            return None

    def get_new_branch_index(self):
        return None

    @staticmethod
    def get_new_revision_number(revisionNumber=None):
        return None

    @staticmethod
    def main_branch_index():
        return None

    def valid_revisions_from_transaction_revision(self, transactionRevisionID, revisionType=None):
        if revisionType is None:
            if 'Update' in transactionRevisionID:
                revisionType = 'update'
            elif 'Snapshot' in transactionRevisionID:
                revisionType = 'snapshot'
            elif 'Tag' in transactionRevisionID:
                revisionType = 'tag'
            elif 'Branch' in transactionRevisionID:
                revisionType = 'branch'
            elif 'Revert' in transactionRevisionID:
                revisionType = 'revert'
            else:
                return Exception
        SPARQLQuery = self._valid_revisions_from_transaction_revision(transactionRevisionID, revisionType)
        stringOfValidRevision = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        func = getattr(self, '_' + revisionType)
        return func(stringOfValidRevision, validRevision=True)

    def _valid_revisions_from_transaction_revision(self, transactionRevisionID, revisionType):
        return ""

    def revision(self, revisionID: URIRef, transactionRevisionA, revisionType: str = 'unknown', isValidRevision=True,
                 transactionRevisionB=None):
        """

        :param revisionID:
        :param revisionType:
        :param isValidRevision:
        :param transactionRevisionA:
        :param transactionRevisionB:
        :return:
        """
        if revisionID == 'update' and isValidRevision and self._config.related_update_content:
            pass
            # SPARQLQuery = """PREFIX <{0}>
            # DESCRIBE ?update
            # WHERE {{ {1} :precedingUpdate* ?update }}""".format(str(BITR4QS), revisionID.n3())
            # stringOfUpdates = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
            # return self._update(stringOfUpdates, validRevision=True, transactionRevision=False)
        else:
            if isValidRevision:
                SPARQLQuery = self._valid_revision(transactionRevisionA, revisionID, revisionType, transactionRevisionB)
            else:
                SPARQLQuery = self._transaction_revision(transactionRevisionA, revisionID, transactionRevisionB)
            print("SPARQLQuery ", SPARQLQuery)
            stringOfRevision = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
            print("stringOfValidRevision ", stringOfRevision)
            func = getattr(self, '_' + revisionType)
            revisions = func(stringOfRevision, isValidRevision)
            if len(revisions) == 1:
                for revisionID, revision in revisions.items():
                    return revision
            else:
                return Exception('Not an unique identifier')

    def _transaction_revision(self, transactionRevisionA, transactionRevision, transactionRevisionB=None):
        return ""

    def _valid_revision(self, transactionRevisionA, validRevision, revisionType, transactionRevisionB=None):

        subQuery = self._valid_revisions_in_graph(transactionRevisionA, revisionType, queryType='SelectQuery',
                                                  revisionB=transactionRevisionB, prefix=False)

        if revisionType == 'update':
            startQuery = "CONSTRUCT { GRAPH ?g { ?update ?p1 ?o1 }\n?update ?p2 ?o2 }"
            extra = "{ GRAPH ?g { ?update ?p1 ?o1 } } UNION { ?update ?p2 ?o2 }"
        else:
            startQuery = "DESCRIBE ?{0}".format(revisionType)
            extra = ""

        SPARQLQuery = """PREFIX : <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        {1}
        WHERE {{ {{ {2} }}
        FILTER ( {3} = ?{4} ) 
        {5} }}""".format(str(BITR4QS), startQuery, subQuery, validRevision.n3(), revisionType, extra)
        return SPARQLQuery

    @staticmethod
    def _unknown(stringOfRevision: str, isValidRevision: bool):
        if isValidRevision:
            revision = parser.Parser.parse_revisions(stringOfRevision, revisionName='valid')
        else:
            revision = parser.Parser.parse_revisions(stringOfRevision, revisionName='transaction')
            print("revision ", revision)
        return revision

    @staticmethod
    def _branch(stringOfBranch: str, isValidRevision: bool):
        if isValidRevision:
            branch = parser.BranchParser.parse_revisions(stringOfBranch, revisionName='valid')
        else:
            branch = parser.BranchParser.parse_revisions(stringOfBranch, revisionName='transaction')
        return branch

    @staticmethod
    def _snapshot(stringOfSnapshot: str, isValidRevision: bool):
        if isValidRevision:
            snapshot = parser.SnapshotParser.parse_revisions(stringOfSnapshot, revisionName='valid')
        else:
            snapshot = parser.SnapshotParser.parse_revisions(stringOfSnapshot, revisionName='transaction')
        return snapshot

    @staticmethod
    def _tag(stringOfTag: str, isValidRevision: bool):
        if isValidRevision:
            tag = parser.TagParser.parse_revisions(stringOfTag, revisionName='valid')
        else:
            tag = parser.TagParser.parse_revisions(stringOfTag, revisionName='transaction')
        return tag

    @staticmethod
    def _update(stringOfUpdate: str, isValidRevision: bool):
        if isValidRevision:
            print("ik kom hier in ")
            update = parser.UpdateParser.parse_revisions(stringOfUpdate, revisionName='valid')
        else:
            update = parser.UpdateParser.parse_revisions(stringOfUpdate, revisionName='transaction')
        return update

    def check_existence(self, revisionIdentifier, revisionType):
        SPARQLQuery = "ASK {{ {0} rdf:type {1} }}".format(revisionIdentifier, revisionType)
        existence = self._revisionStore.execute_ask_query(SPARQLQuery)
        return existence

    def branch_from_name(self, branchName: Literal):
        SPARQLQuery = """PREFIX : <{0}>
        DESCRIBE ?branch
        WHERE {{ ?branch :branchName {1} }}""".format(str(BITR4QS), branchName.n3())
        stringOfBranch = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        branches = parser.BranchParser.parse_revisions(stringOfBranch, revisionName='valid')
        if len(branches) == 1:
            for _, branch in branches.items():
                return branch
        else:
            return Exception('Not an unique name')

    def tag_from_name(self, tagName: Literal):
        SPARQLQuery = """PREFIX : <{0}>
        DESCRIBE ?tag
        WHERE {{ ?tag :tagName {0} }}""".format(str(BITR4QS), tagName.n3())
        stringOfTag = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        tags = parser.TagParser.parse_revisions(stringOfTag, revisionName='valid')
        if len(tags) == 1:
            for _, tag in tags.items():
                return tag
        else:
            return Exception('Not an unique name')

    def can_quad_be_added_or_deleted(self, quad, headRevision: URIRef, startDate: Literal = None,
                                     endDate: Literal = None, deletion=False):
        """
        - A quad Q can be inserted if N=M, where N stands for the number of updates that insert Q and somewhere overlap,
        and where M stands for the number of updates that deletes Q and fully overlap
        - A quad Q with valid time t can be deleted if N=M+1, where N stands for the number of updates that insert Q and
        fully overlap t and where M stands for the number of updates that deletes Q and somewhere overlap t
        :param quad:
        :param headRevision:
        :param startDate:
        :param endDate:
        :param deletion:
        :return:
        """

        assert headRevision is not None, "The HEAD Revision is not allowed to be None."

        updateWhere = self._valid_revisions_in_graph(revisionA=headRevision, queryType='SelectQuery',
                                                     validRevisionType='Update', prefix=False)

        if deletion:
            stringA = quad.to_query_via_delete_update()
            stringB = quad.to_query_via_insert_update()
        else:
            stringA = quad.to_query_via_insert_update()
            stringB = quad.to_query_via_delete_update()

        if startDate is None and endDate is None:
            timeString = """{{ {0} }}
            UNION
            {{ {1}
            NOT EXISTS {{ ?update :endedAt ?endDate }}
            NOT EXISTS {{ ?update :startedAt ?startDate }} }}
            """.format(stringA, stringB)
            print("timeString", timeString)
        elif startDate is None:
            timeString = """{{ {0}
            OPTIONAL {{ ?update :startedAt ?startDate . }}
            FILTER ( !bound(?startDate) || ?startDate <= {2} )
            }}
            UNION
            {{ {1} 
            ?update :endedAt ?endDate . FILTER ( {2} <= ?endDate )
            }}
            UNION
            {{ {1} 
            NOT EXISTS {{ ?update :endedAt ?endDate }}
            NOT EXISTS {{ ?update :startedAt ?startDate }}
            }}
            """.format(stringA, stringB, endDate.n3())
            print("timeString", timeString)
        elif endDate is None:
            timeString = """{{ {0}
            OPTIONAL {{ ?update :endedAt ?endDate . }} 
            FILTER ( !bound(?endDate) || {2} <= ?endDate )
            }}
            UNION
            {{ {1} 
            ?update :startedAt ?startDate . FILTER ( ?startDate >= {2} )
            }}
            UNION
            {{ {1} 
            NOT EXISTS {{ ?update :endedAt ?endDate }}
            NOT EXISTS {{ ?update :startedAt ?startDate }}
            }}
            """.format(stringA, stringB, startDate.n3())
        else:
            timeString = """{{ 
                {0}
                OPTIONAL {{ ?update :startedAt ?startDate }}
                FILTER ( !bound(?startDate) || ?startDate <= {3} )
                OPTIONAL {{ ?update :endedAt ?endDate }} 
                FILTER ( !bound(?endDate) || {2} <= ?endDate ) 
            }} UNION {{ 
                {1} 
                OPTIONAL {{ ?update :startedAt ?startDate }}
                FILTER (!bound(?startDate) || ?startDate <= {2} ) 
                OPTIONAL {{ ?update :endedAt ?endDate }} 
                FILTER ( !bound(?endDate) || {3} <= ?endDate ) 
            }} """.format(stringA, stringB, startDate.n3(), endDate.n3())

        if self.config.related_update_content():
            content = "\n?update :precedingUpdate* ?allUpdate ."
            construct = quad.to_query_via_update('?p', construct=True, subjectName='allUpdate')
            where = quad.to_query_via_update('?p', construct=False, subjectName='allUpdate')
        else:
            content = ""
            construct = quad.to_query_via_update('?p', construct=True)
            where = quad.to_query_via_update('?p', construct=False)

        SPARQLQuery = """PREFIX : <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {{ {1} }}
        WHERE {{ 
            {{ {2} }}
            {3}{4}
            {5}
        }}""".format(str(BITR4QS), construct, updateWhere, timeString, content, where)

        print("SPARQLQuery ", SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        print("stringOfUpdates ", stringOfUpdates)
        updateParser = parser.UpdateParser()
        updateParser.parse_aggregate(stringOfUpdates, forward=True)
        modifications = updateParser.get_list_of_modifications()

        if deletion:
            if len(modifications) == 1 and modifications[0].insertion:
                return True
            else:
                return False
        else:
            if len(modifications) == 0:
                return True
            else:
                return False

    @staticmethod
    def _update_time_string(date: Literal = None, leftOfInterval: Literal = None, rightOfInterval: Literal = None,
                            startTimeInBetween=False, endTimeInBetween=False, variableName='?update'):
        timeConstrains = ""
        if date is not None:
            timeConstrains = """
            OPTIONAL {{ {1} :startedAt ?startDate . }} 
            FILTER ( !bound(?startDate) || ?startDate <= {0} )
            OPTIONAL {{ {1} :endedAt ?endDate . }}
            FILTER ( !bound(?endDate) || ?endDate >= {0} )
            """.format(date.n3(), variableName)
        elif date is None and startTimeInBetween and not endTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER (  ?startDate > {0} && ?startDate <= {1} && ?endDate > {1} )
            }} UNION {{
                {2} :startedAt ?startDate .
                NOT EXISTS {{ ?update :endedAt ?endDate . }}
                FILTER (  ?startDate > {0} && ?startDate <= {1} ) }}
                """.format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        elif date is None and endTimeInBetween and not startTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER ( ?endDate >= {0} && ?endDate < {1} && ?startDate < {0} )
            }} UNION {{
                {2} :endedAt ?endDate .
                NOT EXISTS {{ ?update :startedAt ?startDate . }}
                FILTER (  ?endDate >= {0} && ?endDate < {1} ) }}
                """.format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        return timeConstrains

    def _construct_where_for_update(self, quadPattern):
        construct = where = ""
        if self.config.query_all_updates() or quadPattern is None:
            construct = "GRAPH ?g { ?update ?p1 ?o1 }\n?update ?p2 ?o2 ."
            where = "{ GRAPH ?g { ?update ?p1 ?o1 } } UNION { ?update ?p2 ?o2 }"
        elif self.config.query_specific_updates():
            construct = quadPattern.to_query_via_update('?p', construct=True)
            where = quadPattern.to_query_via_update('?p', construct=False)
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
            content = "\n?update :precedingUpdate* ?update ."

        SPARQLQuery = """PREFIX : <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {{ {1} }}
        WHERE {{ 
            {{ 
                {2} 
            }}
            {3}{4}
        }}
        """.format(str(BITR4QS), construct, updateWhere, where, content)
        print("SPARQLQuery ", SPARQLQuery)
        stringOfUpdates = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
        print("stringOfUpdates ", stringOfUpdates)
        updateParser.parse_aggregate(stringOfUpdates, forward)

    def _valid_revisions_in_graph(self, revisionA: URIRef, revisionType: str, queryType: str,
                                  revisionB: URIRef = None, prefix=True, timeConstrain=""):
        """

        :param revisionA:
        :param validRevisionType:
        :param queryType:
        :param revisionB:
        :param prefix:
        :param timeConstrain:
        :return:
        """
        return ""

    def closest_snapshot(self, validTime: Literal, headRevision: URIRef):
        """

        :param validTime:
        :param headRevision:
        :return:
        """
        stringOfSnapshots = self._valid_revisions_in_graph(revisionA=headRevision, queryType='DescribeQuery',
                                                           revisionType='snapshot')

        snapshots = parser.SnapshotParser.parse_revisions(stringOfSnapshots, 'valid')

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

        :param revisionA:
        :param revisionB:
        :return:
        """
        return True
