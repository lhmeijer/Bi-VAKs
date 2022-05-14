from rdflib.term import URIRef, Literal
from src.main.bitr4qs.store.QuadStoreSingleton import HttpRevisionStoreSingleton
import src.main.bitr4qs.tools.parser as parser
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.namespace import XSD
from datetime import datetime
from src.main.bitr4qs.revision.HeadRevision import HeadRevision


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

        :param branch:
        :return:
        """
        if branch is not None:
            branchString = "?headRevision :branch {0} .".format(branch.n3())
        else:
            branchString = "NOT EXISTS { ?headRevision :branch ?branch . }"

        SPARQLQuery = """
        PREFIX : <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?revision ?revisionNumber
        WHERE {{ 
            ?headRevision rdf:type :HeadRevision . 
            ?headRevision :precedingRevision ?revision .
            OPTIONAL {{ ?headRevision :revisionNumber ?revisionNumber . }}
            {1} 
        }}""".format(str(BITR4QS), branchString)

        result = self._revisionStore.execute_select_query(SPARQLQuery, 'json')

        revision = None
        if 'revision' in result['results']['bindings'][0]:
            revision = URIRef(result['results']['bindings'][0]['revision']['value'])

        revisionNumber = None
        if 'revisionNumber' in result['results']['bindings'][0]:
            revisionNumber = Literal(result['results']['bindings'][0]['revisionNumber']['value'],
                                     datatype=XSD.nonNegativeInteger)
        return revision, revisionNumber

    def get_new_branch_index(self):
        return None

    def update_head_revision(self, precedingRevision, currentRevision, revisionNumber=None, branch=None):

        headRevision = self.head_revision(branch=branch)
        if headRevision is None:
            revision = HeadRevision.revision(branch=branch, precedingRevision=currentRevision,
                                             revisionNumber=revisionNumber)


        branchString = ""
        if branch is not None:
            branchString = '\n?headRevision :branch {0}'.format(branch.n3())

        revisionNumberString = ""
        if revisionNumber is not None:
            revisionNumberString = '\n?headRevision :revisionNumber {0}'.format(revisionNumber.n3())

        SPARQLQuery = """PREFIX <{0}>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        DELETE {{ 
            ?headRevision :precedingRevision {1} . 
            ?headRevision :revisionNumber ?revisionNumber .
            ?headRevision :branch ?branch . 
        }}
        INSERT {{ ?headRevision :precedingRevision {2} .{3}{4} }}
        WHERE {{ ?headRevision rdf:type :HeadRevision .
            ?headRevision :precedingRevision {1} .
            OPTIONAL {{ ?headRevision :revisionNumber ?revisionNumber }}
            OPTIONAL {{ ?headRevision :branch ?branch }}
        }}
        """.format(str(BITR4QS), precedingRevision.n3(), currentRevision.n3(), branchString, revisionNumberString)

        result = self._revisionStore.execute_update_query(SPARQLQuery, 'json')

    def valid_revision(self, validRevisionIdentifier: URIRef, validRevisionType: str):

        SPARQLQuery = "DESCRIBE {0}".format(validRevisionIdentifier)

        if validRevisionType == 'update':
            if self._config.related_update_content:
                SPARQLQuery = """PREFIX <{0}>
                DESCRIBE ?update
                WHERE {{ {1} :precedingUpdate* ?update }}""".format(str(BITR4QS), validRevisionIdentifier)

        stringOfValidRevision = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        func = getattr(self, '_' + validRevisionType)
        return func(stringOfValidRevision)

    @staticmethod
    def _branch(stringOfBranch: str):
        branch = parser.BranchParser.parse_revisions(stringOfBranch, revisionName='valid')
        return branch

    @staticmethod
    def _snapshot(stringOfSnapshot: str):
        snapshot = parser.SnapshotParser.parse_revisions(stringOfSnapshot, revisionName='valid')
        return snapshot

    @staticmethod
    def _tag(stringOfTag: str):
        tag = parser.TagParser.parse_revisions(stringOfTag, revisionName='valid')
        return tag

    @staticmethod
    def _update(stringOfUpdate: str):
        update = parser.UpdateParser.parse_revisions(stringOfUpdate, revisionName='valid')
        return update

    def check_existence(self, revisionIdentifier, revisionType):
        SPARQLQuery = "ASK {{ {0} rdf:type {1} }}".format(revisionIdentifier, revisionType)
        existence = self._revisionStore.execute_ask_query(SPARQLQuery)
        return existence

    def branch_from_name(self, branchName: Literal):
        SPARQLQuery = """DESCRIBE ?branch
        WHERE {{ ?branch :branchName {0} }}""".format(branchName)
        stringOfBranch = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        branch = parser.BranchParser.parse_revisions(stringOfBranch, revisionName='valid')
        return branch

    def tag_from_name(self, tagName: Literal):
        SPARQLQuery = """DESCRIBE ?tag
        WHERE {{ ?tag :tagName {0} }}""".format(tagName)
        stringOfTag = self._revisionStore.execute_describe_query(SPARQLQuery, 'nquads')
        branch = parser.TagParser.parse_revisions(stringOfTag, revisionName='valid')
        return branch

    def can_quad_be_added_or_deleted(self, quad, headRevision: URIRef, startDate: Literal = None,
                                      endDate: Literal = None, deletion=False):
        """
        - A quad Q can be inserted if N=M, where N stands for the number of updates that insert Q and somewhere overlap,
        and where M stands for the number of updates that deletes Q and fully overlap
        - A quad Q with valid time t can be deleted if N=M+1, where N stands for the number of updates that insert Q and
        fully overlap t and where M stands for the number of updates that deletes Q and somewhere overlap t
        :param quad:
        :param updateWhere:
        :param startDate:
        :param endDate:
        :param deletion:
        :return:
        """
        assert headRevision is not None, "The HEAD Revision is not allowed to be None."

        updateWhere = self._valid_revisions_in_graph(revisionA=headRevision, queryType='SelectQuery',
                                                     validRevisionType='Update', prefix=False)

        if deletion:
            stringA = quad.to_query_delete_update()
            stringB = quad.to_query_insert_update()
        else:
            stringA = quad.to_query_insert_update()
            stringB = quad.to_query_delete_update()

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
            OPTIONAL {{ ?update :startDate ?startDate . }}
            FILTER ( ?startDate <= {2} )
            }}
            UNION
            {{ {1} 
            ?update :endDate ?endDate . FILTER ( {2} <= ?endDate )
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
            OPTIONAL {{ ?update :endDate ?endDate . }} 
            FILTER ( {2} <= ?endDate )
            }}
            UNION
            {{ {1} 
            ?update :startDate ?startDate . FILTER ( ?startDate >= {2} )
            }}
            UNION
            {{ {1} 
            NOT EXISTS {{ ?update :endedAt ?endDate }}
            NOT EXISTS {{ ?update :startedAt ?startDate }}
            }}
            """.format(stringA, stringB, startDate.n3())
            print("timeString", timeString)
        else:
            timeString = """{{ {0}
            OPTIONAL {{ ?update :startDate ?startDate }}
            FILTER (?startDate <= {3} )
            OPTIONAL {{ ?update :endDate ?endDate }} 
            FILTER ( {2} <= ?endDate ) 
            }} UNION {{ 
            {1} 
            OPTIONAL {{ ?update :startDate ?startDate }}
            FILTER (?startDate >= {2} ) 
            OPTIONAL {{ ?update :endDate ?endDate }} 
            FILTER ( {3} <= ?endDate ) }} """.format(stringA, stringB, startDate.n3(), endDate.n3())

        if self.config.related_update_content():
            content = "\n?update :precedingUpdate* ?allUpdate ."
            construct = quad.to_query_update('?p', construct=True, subjectName='allUpdate')
            where = quad.to_query_update('?p', construct=False, subjectName='allUpdate')
        else:
            content = ""
            construct = quad.to_query_update('?p', construct=True)
            where = quad.to_query_update('?p', construct=False)

        SPARQLQuery = """PREFIX : <{0}>
        CONSTRUCT {{ {1} }}
        WHERE {{ 
            {{ {2} }}
            {3}{4}
            {5}
        }}""".format(str(BITR4QS), construct, updateWhere, timeString, content, where)

        stringOfUpdates = self._revisionStore.execute_construct_query(SPARQLQuery, 'nquads')
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
            FILTER ( !bound(?endDate) || ?endDate >= {0} )""".format(date.n3(), variableName)
        elif date is None and startTimeInBetween and not endTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER (  ?startDate > {0} && ?startDate <= {1} && ?endDate > {1} )
            }} UNION {{
                {2} :startedAt ?startDate .
                NOT EXISTS {{ ?update :endedAt ?endDate . }}
                FILTER (  ?startDate > {0} && ?startDate <= {1} )
            }}""".format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        elif date is None and endTimeInBetween and not startTimeInBetween:
            timeConstrains = """
            {{  {2} :startedAt ?startDate .
                {2} :endedAt ?endDate .
                FILTER ( ?endDate >= {0} && ?endDate < {1} && ?startDate < {0} )
            }} UNION {{
                {2} :endedAt ?endDate .
                NOT EXISTS {{ ?update :startedAt ?startDate . }}
                FILTER (  ?endDate >= {0} && ?endDate < {1} )
            }}""".format(leftOfInterval.n3(), rightOfInterval.n3(), variableName)
        return timeConstrains

    def _construct_where_for_update(self, quadPattern):
        construct = where = ""
        if self.config.query_all_updates():
            construct = where = "GRAPH ?g { ?update ?p1 ?o1 . }\n?update ?p2 ?o2 ."
        elif self.config.query_specific_updates():
            construct = quadPattern.to_query_update('?p', construct=True)
            where = quadPattern.to_query_update('?p', construct=False)
        else:
            # Raise an exception that one of the two update fetching strategies should be True
            pass
        return construct, where

    def get_updates_in_revision_graph(self, revisionA: URIRef, quadPattern, updateParser, revisionB: URIRef = None,
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
                                                     validRevisionType='Update', prefix=False,
                                                     timeConstrain=timeConstrain)
        construct, where = self._construct_where_for_update(quadPattern=quadPattern)

        content = ""
        if self.config.related_update_content():
            content = "\n?update :precedingUpdate* ?update ."

        SPARQLQuery = """PREFIX : <{0}> 
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

    def _valid_revisions_in_graph(self, revisionA: URIRef, validRevisionType: str, queryType: str,
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
                                                           validRevisionType='Snapshot')

        snapshots = parser.SnapshotParser.parse_revisions(stringOfSnapshots, 'valid')
        print(snapshots)

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
