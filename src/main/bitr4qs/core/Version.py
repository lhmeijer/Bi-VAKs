from src.main.bitr4qs.store.QuadStoreSingleton import HttpTemporalStoreSingleton
from datetime import datetime
from src.main.bitr4qs.tools.parser.UpdateParser import UpdateParser


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

    def modifications_between_two_states(self, transactionA, transactionB, validA, validB):
        """
        Function to obtain the updates to bring state A into state B.
        :param transactionA:
        :param transactionB:
        :param validA:
        :param validB:
        :return:
        """
        print('transactionA ', transactionA)
        print('transactionB ', transactionB)
        print("validA ", validA)
        print("validB ", validB)
        if transactionA == transactionB:
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionA)

        elif self._revisionStore.is_transaction_time_a_earlier(revisionA=transactionA, revisionB=transactionB):  # t(a) < t(b)
            # [a] <- [] <- [] <- [] <- [b]
            print('[a] <- [] <- [] <- [] <- [b]')
            # First bring state A into a new state with valid time B
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionA)

            # Rewind all the updates which are modifications of the updates within the transaction revisions
            # Otherwise some updates will be taken into account twice.
            self._revisionStore.get_modifications_of_updates_between_revisions(
                revisionA=transactionB, revisionB=transactionA, date=validA, updateParser=self._updateParser,
                quadPattern=self._quadPattern, forward=False)
            # Fastforward the updates within the transaction revisions
            self._revisionStore.get_updates_in_revision_graph(
                date=validB, forward=True, quadPattern=self._quadPattern, updateParser=self._updateParser,
                revisionA=transactionB, revisionB=transactionA)

        elif self._revisionStore.is_transaction_time_a_earlier(revisionA=transactionB, revisionB=transactionA):  # t(a) > t(b)
            # [b] <- [] <- [] <- [] <- [a]
            print('[b] <- [] <- [] <- [] <- [a]')
            # Rewind the updates within the transaction revisions
            self._revisionStore.get_updates_in_revision_graph(
                date=validA, forward=False, quadPattern=self._quadPattern, updateParser=self._updateParser,
                revisionA=transactionA, revisionB=transactionB)

            # Fastforward all the updates which are modifications of the updates within the transaction revisions
            self._revisionStore.get_modifications_of_updates_between_revisions(
                revisionA=transactionA, revisionB=transactionB, date=validA, updateParser=self._updateParser,
                quadPattern=self._quadPattern, forward=True)

            # Bring state A into a new state with valid time B
            self.modifications_between_valid_states(validA=validA, validB=validB, transactionTime=transactionA)

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
            print("Initialise Temporal Store.")
            self._temporalStore = HttpTemporalStoreSingleton.get(self._revisionStore.config)

        # Get the closest snapshot
        snapshot = None
        if previousTransactionTime is None or previousValidTime is None:
            print("Determine the closest snapshot.")
            print("self._validTime ", self._validTime)
            snapshot = self._revisionStore.closest_snapshot(validTime=self._validTime, headRevision=headRevision)
            print("snapshot ", snapshot)
        snapshot = None
        if previousTransactionTime and previousValidTime:
            self.modifications_between_two_states(transactionA=self._transactionTime, validA=self._validTime,
                                                  transactionB=previousTransactionTime, validB=previousValidTime)
            SPARQLUpdateQuery = self._updateParser.modifications_to_sparql_update_query()
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)
        elif not snapshot and not previousValidTime:
            # get all updates from initial revision to given revision A
            self._revisionStore.get_updates_in_revision_graph(
                revisionA=self._transactionTime, date=self._validTime, revisionB=previousTransactionTime,
                quadPattern=self._quadPattern, updateParser=self._updateParser)
            modifications_in_n_quad = self._updateParser.modifications_to_n_quads()
            self._temporalStore.n_quads_to_store(modifications_in_n_quad)
        else:
            # query the snapshot using the quad pattern return a dictionary of modifications
            SPARQLConstructQuery = "CONSTRUCT WHERE {{ {0} }}".format(self._quadPattern.to_sparql())
            stringOfNQuads = snapshot.query(SPARQLConstructQuery, 'ConstructQuery', 'nquads')
            # Add the n quads to temporal store
            response = self._temporalStore.n_quads_to_store(stringOfNQuads)

            self.modifications_between_two_states(transactionA=snapshot.transaction_revision, validB=self._validTime,
                                                  transactionB=self._transactionTime, validA=snapshot.effective_date)
            SPARQLUpdateQuery = self._updateParser.modifications_to_sparql_update_query()
            print('SPARQLUpdateQuery ', SPARQLUpdateQuery)
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)

    def clear_version(self):
        self._temporalStore.reset_store()

    def query_version(self, queryString, returnFormat):
        return self._temporalStore.execute_query(queryString, returnFormat)
