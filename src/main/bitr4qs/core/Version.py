from src.main.bitr4qs.store.QuadStoreSingleton import HttpTemporalStoreSingleton
from src.main.bitr4qs.tools.parser.UpdateParser import UpdateParser
from datetime import datetime, timedelta
from timeit import default_timer as timer


class Version(object):

    def __init__(self, validTime, transactionTime, revisionStore, quadPattern):
        self._validTime = validTime
        self._transactionTime = transactionTime
        self._revisionStore = revisionStore
        self._quadPattern = quadPattern

        self._temporalStore = None
        self._updateParser = UpdateParser()

    def number_of_processed_quads(self):
        return self._updateParser.number_of_processed_quads

    @property
    def valid_time(self):
        return self._validTime

    @valid_time.setter
    def valid_time(self, validTime):
        self._validTime = validTime

    @property
    def transaction_time(self):
        return self._transactionTime

    @transaction_time.setter
    def transaction_time(self, transactionTime):
        self._transactionTime = transactionTime

    @property
    def update_parser(self):
        return self._updateParser

    def modifications_between_valid_states(self, validA, validB, transactionTime):
        """

        :param validA:
        :param validB:
        :param transactionTime:
        :return:
        """
        validTimeA = datetime.strptime(str(validA), "%Y-%m-%dT%H:%M:%S+00:00")
        validTimeB = datetime.strptime(str(validB), "%Y-%m-%dT%H:%M:%S+00:00")

        if validTimeA < validTimeB:
            # Get the modification to fastforward
            self._revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validA, rightOfInterval=validB, startTimeInBetween=True, quadPattern=self._quadPattern,
                updateParser=self._updateParser, revisionA=transactionTime)

            # Get the modification to rewind
            self._revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validA, rightOfInterval=validB, forward=False, endTimeInBetween=True,
                quadPattern=self._quadPattern, updateParser=self._updateParser, revisionA=transactionTime)

        elif validTimeA > validTimeB:
            # Get the modifications to rewind
            self._revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validB, rightOfInterval=validA, forward=False, startTimeInBetween=True,
                quadPattern=self._quadPattern, updateParser=self._updateParser, revisionA=transactionTime)

            # Get the modifications to fastforward
            self._revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validB, rightOfInterval=validA, endTimeInBetween=True, quadPattern=self._quadPattern,
                updateParser=self._updateParser, revisionA=transactionTime)

    def modifications_between_two_states_2(self, transactionA, validA, transactionB, validB):

        self._revisionStore.get_updates_in_revision_graph(
            revisionA=transactionA, date=validA, quadPattern=self._quadPattern, updateParser=self._updateParser)
        modificationsInNQuadA = set(self._updateParser.modifications_to_n_quads().split('\n'))
        self._updateParser.reset_modifications()

        self._revisionStore.get_updates_in_revision_graph(
            revisionA=transactionB, date=validB, quadPattern=self._quadPattern, updateParser=self._updateParser)
        modificationsInNQuadB = set(self._updateParser.modifications_to_n_quads().split('\n'))
        self._updateParser.reset_modifications()

        overlapTriples = modificationsInNQuadA.intersection(modificationsInNQuadB)

        addedTriples = modificationsInNQuadB - overlapTriples
        self._updateParser.n_quads_to_modifications(addedTriples, deletion=False)

        deletedTriples = modificationsInNQuadA - overlapTriples
        self._updateParser.n_quads_to_modifications(deletedTriples, deletion=True)

    def retrieve_version_2(self):
        """

        :return:
        """
        if self._temporalStore is None:
            self._temporalStore = HttpTemporalStoreSingleton.get(self._revisionStore.config)

        self._updateParser.reset_modifications()

        # get all updates from initial revision to given revision A
        self._revisionStore.get_updates_in_revision_graph(
            date=self._validTime, updateParser=self._updateParser, quadPattern=self._quadPattern,
            revisionA=self._transactionTime)
        modifications_in_n_quad = self._updateParser.modifications_to_n_quads()
        self._temporalStore.upload_to_dataset(modifications_in_n_quad, 'application/n-quads')

    def modifications_between_two_states(self, transactionA, transactionB, validA, validB):
        """
        Function to obtain the updates to bring state A into state B.
        :param transactionA:
        :param transactionB:
        :param validA:
        :param validB:
        :return:
        """
        # print('transactionA ', transactionA)
        # print('transactionB ', transactionB)
        # print("validA ", validA)
        # print("validB ", validB)

        if transactionA == transactionB:
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionA)

        elif self._revisionStore.is_transaction_time_a_earlier(revisionA=transactionA, revisionB=transactionB):  # t(a) < t(b)
            # [a] <- [] <- [] <- [] <- [b]
            # First bring state A into a new state with valid time B
            # start = timer()
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionA)
            # end = timer()
            # print("a into a new state with valid time b ", timedelta(seconds=end - start).total_seconds())

            # Rewind all the updates which are modifications of the updates within the transaction revisions
            # Otherwise some updates will be taken into account twice.
            # start = timer()
            self._revisionStore.get_modifications_of_updates_between_revisions(
                revisionA=transactionB, revisionB=transactionA, date=validB, updateParser=self._updateParser,
                quadPattern=self._quadPattern, forward=False)
            # end = timer()
            # print("modifications of updates ", timedelta(seconds=end - start).total_seconds())
            # Fastforward the updates within the transaction revisions
            # start = timer()
            self._revisionStore.get_updates_in_revision_graph(
                date=validB, forward=True, quadPattern=self._quadPattern, updateParser=self._updateParser,
                revisionA=transactionB, revisionB=transactionA)
            # end = timer()
            # print("updates within transaction revisions ", timedelta(seconds=end - start).total_seconds())

        elif self._revisionStore.is_transaction_time_a_earlier(revisionA=transactionB, revisionB=transactionA):  # t(a) > t(b)
            # [b] <- [] <- [] <- [] <- [a]
            # Rewind the updates within the transaction revisions
            # start = timer()
            self._revisionStore.get_updates_in_revision_graph(
                date=validA, forward=False, quadPattern=self._quadPattern, updateParser=self._updateParser,
                revisionA=transactionA, revisionB=transactionB)
            # end = timer()
            # print("rewind updates within transaction revisions ", timedelta(seconds=end - start).total_seconds())

            # Fastforward all the updates which are modifications of the updates within the transaction revisions
            # start = timer()
            self._revisionStore.get_modifications_of_updates_between_revisions(
                revisionA=transactionA, revisionB=transactionB, date=validA, updateParser=self._updateParser,
                quadPattern=self._quadPattern, forward=True)
            # end = timer()
            # print("modifications of updates ", timedelta(seconds=end - start).total_seconds())

            # Bring state A into a new state with valid time B
            # start = timer()
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionB)
            # end = timer()
            # print("a into a new state with valid time b ", timedelta(seconds=end - start).total_seconds())

        else:
            raise Exception

    def retrieve_version(self, headRevision=None, previousTransactionTime=None, previousValidTime=None):
        """

        :param headRevision:
        :param previousTransactionTime:
        :param previousValidTime:
        :return:
        """
        if self._temporalStore is None:
            self._temporalStore = HttpTemporalStoreSingleton.get(self._revisionStore.config)

        self._updateParser.reset_modifications()

        # Get the closest snapshot
        snapshot = None
        if previousTransactionTime is None or previousValidTime is None:
            snapshot = self._revisionStore.closest_snapshot(validTime=self._validTime, headRevision=headRevision)
        if previousTransactionTime and previousValidTime:
            self.modifications_between_two_states(transactionA=previousTransactionTime, validA=previousValidTime,
                                                  transactionB=self._transactionTime, validB=self._validTime)
            SPARQLUpdateQuery = self._updateParser.modifications_to_sparql_update_query()
            # print('SPARQLUpdateQuery ', SPARQLUpdateQuery)
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)
        elif snapshot is None and previousValidTime is None:
            # get all updates from initial revision to given revision A
            self._revisionStore.get_updates_in_revision_graph(
                revisionA=self._transactionTime, date=self._validTime, revisionB=previousTransactionTime,
                quadPattern=self._quadPattern, updateParser=self._updateParser)
            modifications_in_n_quad = self._updateParser.modifications_to_n_quads()
            self._temporalStore.upload_to_dataset(modifications_in_n_quad, 'application/n-quads')
        else:
            # query the snapshot using the quad pattern return a dictionary of modifications
            SPARQLConstructQuery = "CONSTRUCT {{ {0} }}\nWHERE {{ {0} }}".format(self._quadPattern.sparql())
            # start = timer()
            stringOfNQuads = snapshot.query_dataset(SPARQLConstructQuery, 'ConstructQuery', 'nquads')
            # end = timer()
            # print("query snapshot ", timedelta(seconds=end - start).total_seconds())
            # Add the n quads to temporal store
            # start = timer()
            # self._updateParser.n_quads_to_modifications(stringOfNQuads)
            # end = timer()
            # print("modifications of snapshots ", timedelta(seconds=end - start).total_seconds())
            response = self._temporalStore.upload_to_dataset(stringOfNQuads, 'application/n-quads')

            self.modifications_between_two_states(transactionA=snapshot.transaction_revision, validB=self._validTime,
                                                  transactionB=self._transactionTime, validA=snapshot.effective_date)
            # start = timer()
            SPARQLUpdateQuery = self._updateParser.modifications_to_sparql_update_query()
            # end = timer()
            # print("to modifications ", timedelta(seconds=end - start).total_seconds())
            # print('SPARQLUpdateQuery ', SPARQLUpdateQuery)
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)

    def clear_version(self):
        self._temporalStore.empty_dataset()

    def query_version(self, queryString, returnFormat):
        return self._temporalStore.execute_query(queryString, returnFormat)
