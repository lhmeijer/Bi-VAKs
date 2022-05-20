from src.main.bitr4qs.store.QuadStoreSingleton import HttpTemporalStoreSingleton
from datetime import datetime
from src.main.bitr4qs.tools.parser.UpdateParser import UpdateParser


class Version(object):

    def __init__(self, validTime, transactionTime):
        self._validTime = validTime
        self._transactionTime = transactionTime
        self._temporalStore = None

    @property
    def transaction_time(self):
        return self._transactionTime

    @transaction_time.setter
    def transaction_time(self, transactionTime):
        self._transactionTime = transactionTime

    @property
    def valid_time(self):
        return self._validTime

    @valid_time.setter
    def valid_time(self, validTime):
        self._validTime = validTime

    @staticmethod
    def modifications_between_valid_states(validA, validB, revisionStore, updateParser, quadPattern, transactionTime):
        """

        :param validA:
        :param validB:
        :param revisionStore:
        :param updateParser:
        :param quadPattern:
        :param transactionTime:
        :return:
        """
        validTimeA = datetime.strptime(str(validA), "%Y-%m-%dT%H:%M:%S+02:00")
        validTimeB = datetime.strptime(str(validB), "%Y-%m-%dT%H:%M:%S+02:00")

        if validTimeA < validTimeB:
            # Get the modification to fastforward
            revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validA, rightOfInterval=validB, startTimeInBetween=True, quadPattern=quadPattern,
                updateParser=updateParser, revisionA=transactionTime)

            # Get the modification to rewind
            revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validA, rightOfInterval=validB, forward=False, endTimeInBetween=True,
                quadPattern=quadPattern, updateParser=updateParser, revisionA=transactionTime)

        elif validTimeA > validTimeB:
            # Get the modifications to rewind
            revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validB, rightOfInterval=validA, forward=False, startTimeInBetween=True,
                quadPattern=quadPattern, updateParser=updateParser, revisionA=transactionTime)

            # Get the modifications to fastforward
            revisionStore.get_updates_in_revision_graph(
                leftOfInterval=validB, rightOfInterval=validA, endTimeInBetween=True, quadPattern=quadPattern,
                updateParser=updateParser, revisionA=transactionTime)

    @staticmethod
    def modifications_between_two_states(transactionA, transactionB, validA, validB, revisionStore, quadPattern,
                                         updateParser):
        """
        Function to obtain the updates to bring state A into state B.
        :param transactionA:
        :param transactionB:
        :param validA:
        :param validB:
        :param revisionStore:
        :param quadPattern:
        :param updateParser:
        :return:
        """
        print('transactionA ', transactionA)
        print('transactionB ', transactionB)
        print("validA ", validA)
        print("validB ", validB)
        if transactionA == transactionB:
            Version.modifications_between_valid_states(validA, validB, revisionStore, updateParser, quadPattern,
                                                       transactionA)

        elif revisionStore.is_transaction_time_a_earlier(revisionA=transactionA, revisionB=transactionB):  # t(a) < t(b)
            # [a] <- [] <- [] <- [] <- [b]
            print('[a] <- [] <- [] <- [] <- [b]')
            # First bring state A into a new state with valid time B
            Version.modifications_between_valid_states(validA, validB, revisionStore, updateParser, quadPattern,
                                                       transactionA)

            # Rewind all the updates which are modifications of the updates within the transaction revisions
            # Otherwise some updates will be taken into account twice.
            revisionStore.get_modifications_of_updates_between_revisions(transactionB, transactionA, validA,
                                                                         updateParser, quadPattern, forward=False)
            # Fastforward the updates within the transaction revisions
            revisionStore.get_updates_in_revision_graph(
                date=validB, forward=True, quadPattern=quadPattern, updateParser=updateParser, revisionA=transactionB,
                revisionB=transactionA)

        elif revisionStore.is_transaction_time_a_earlier(revisionA=transactionB, revisionB=transactionA):  # t(a) > t(b)
            # [b] <- [] <- [] <- [] <- [a]
            print('[b] <- [] <- [] <- [] <- [a]')
            # Rewind the updates within the transaction revisions
            revisionStore.get_updates_in_revision_graph(
                date=validA, forward=False, quadPattern=quadPattern, updateParser=updateParser, revisionA=transactionA,
                revisionB=transactionB)

            # Fastforward all the updates which are modifications of the updates within the transaction revisions
            revisionStore.get_modifications_of_updates_between_revisions(transactionA, transactionB, validA,
                                                                         updateParser, quadPattern, forward=True)

            # Bring state A into a new state with valid time B
            Version.modifications_between_valid_states(validA, validB, revisionStore, updateParser, quadPattern,
                                                       transactionA)

        else:
            pass

    def retrieve_version(self, revisionStore, quadPattern, headRevision=None, previousTransactionTime=None,
                         previousValidTime=None):
        """

        :param headRevision:
        :param revisionStore:
        :param quadPattern:
        :param previousTransactionTime:
        :param previousValidTime:
        :return:
        """
        if self._temporalStore is None:
            print("Initialise temporal store.")
            self._temporalStore = HttpTemporalStoreSingleton.get(revisionStore.config)

        updateParser = UpdateParser()
        # Get the closest snapshot
        snapshot = None
        if previousTransactionTime is None or previousValidTime is None:
            print("Determine the closest snapshot.")
            snapshot = revisionStore.closest_snapshot(self._validTime, headRevision)
            print('snapshot ', snapshot)

        if previousTransactionTime is not None and previousValidTime is not None:
            self.modifications_between_two_states(transactionA=self._transactionTime, quadPattern=quadPattern,
                                                  transactionB=previousTransactionTime, validA=self._validTime,
                                                  validB=previousValidTime, revisionStore=revisionStore,
                                                  updateParser=updateParser)
            SPARQLUpdateQuery = updateParser.modifications_to_sparql_update_query()
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)
        elif snapshot is None:
            # get all updates from initial revision to given revision A
            revisionStore.get_updates_in_revision_graph(revisionA=self._transactionTime, date=self._validTime,
                                                        revisionB=previousTransactionTime, quadPattern=quadPattern,
                                                        updateParser=updateParser)
            modifications_in_n_quad = updateParser.modifications_to_n_quads()
            self._temporalStore.n_quads_to_store(modifications_in_n_quad)
        else:
            # query the snapshot using the quad pattern return a dictionary of modifications
            SPARQLConstructQuery = "CONSTRUCT WHERE {{ {0} }}".format(quadPattern.to_sparql())
            stringOfNQuads = snapshot.query(SPARQLConstructQuery, 'ConstructQuery', 'nquads')
            # Add the n quads to temporal store
            response = self._temporalStore.n_quads_to_store(stringOfNQuads)

            self.modifications_between_two_states(transactionA=snapshot.transaction_revision, quadPattern=quadPattern,
                                                  transactionB=self._transactionTime, validA=snapshot.effective_date,
                                                  validB=self._validTime, revisionStore=revisionStore,
                                                  updateParser=updateParser)
            SPARQLUpdateQuery = updateParser.modifications_to_sparql_update_query()
            print('SPARQLUpdateQuery ', SPARQLUpdateQuery)
            self._temporalStore.execute_update_query(SPARQLUpdateQuery)

    def clear_version(self):
        self._temporalStore.reset_store()

    def query_version(self, queryString, returnFormat):
        return self._temporalStore.execute_query(queryString, returnFormat)
