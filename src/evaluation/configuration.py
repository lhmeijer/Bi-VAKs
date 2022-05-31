import os


class BearBConfiguration(object):

    raw_version_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                        'BEAR-B-alldata.IC.nt', '')
    raw_change_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                       'BEAR-B-alldata.CB.nt', '')

    MAX_SECONDS = 31536000
    START_EFFECTIVE_DATE = '2015-01-01T00:00:00+00:00'
    QUERY_TIME = '2015-07-01T00:00:00+00:00'

    reference = ['explicit', 'implicit']
    fetching = ['all', 'specific']
    content = ['repeated', 'related']

    MEAN_closeness = 15768000
    standard_deviations_closeness = [1000000, 10000000]

    STANDARD_DEVIATION_width = 86400
    means_width = [432000, 4320000]

    triples_per_updates = [10, 100]

    seeds = [0, 1, 2, 3, 4]

    _snapshot_effective_dates = {'F': '2015-03-01T00:00:00+00:00', 'N': '2015-07-01T00:00:00+00:00'}
    versions_to_snapshot = [(None, None), ('F', 30), ('N', 30)]

    versions_to_branch = [None, 3]
    updates_to_modified_update = [(None, None), (None, 5), (5, 5)]

    QUERY_TYPE = 'p'    # po or p
    QUERY_ATOM = 'VM'   # VM, DM, VQ
    NUMBER_OF_VERSIONS = 89
    number_of_queries = [None, 3]

    _bear_b_results = {'VM': 'mat-{0}-queries'.format(QUERY_TYPE), 'DM': 'diff-{0}-queries'.format(QUERY_TYPE),
                       'VQ': 'ver-{0}-queries'.format(QUERY_TYPE)}

    bear_results_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                    'BEAR-B-{0}'.format(_bear_b_results[QUERY_ATOM]), _bear_b_results[QUERY_ATOM])

    bear_queries_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw', 'BEAR-B-queries',
                                          '{0}-queries.nt'.format(QUERY_TYPE))

    def __init__(self, indexCloseness=0, indexWidth=0, triplesPerUpdateIndex=0, seedIndex=0, snapshotIndex=0,
                 branchIndex=0, modifiedUpdateIndex=0, referenceIndex=0, fetchingIndex=0, contentIndex=0,
                 numberOfQueriesIndex=0):

        self.STANDARD_DEVIATION_closeness = self.standard_deviations_closeness[indexCloseness]
        _closeness = 'LC' if self.STANDARD_DEVIATION_closeness == 1000000 else 'HC'

        self.MEAN_width = self.means_width[indexWidth]
        _width = 'LW' if self.MEAN_width == 432000 else 'HW'

        self.TRIPLES_PER_UPDATE = self.triples_per_updates[triplesPerUpdateIndex]
        self.SEED = self.seeds[seedIndex]

        _updates = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width))
        self.updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-updates',
                                              _updates + '.txt')
        self.statistics_updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B',
                                                         'statistics', 'updates_stats_' + _updates + '.json')

        self.VERSIONS_TO_SNAPSHOT = self.versions_to_snapshot[snapshotIndex][1]
        _far_or_nearby = self.versions_to_snapshot[snapshotIndex][0]
        if self.VERSIONS_TO_SNAPSHOT:
            self.SNAPSHOT_EFFECTIVE_DATE = self._snapshot_effective_dates[_far_or_nearby]
        _snapshots = '' if not self.VERSIONS_TO_SNAPSHOT else 'S{0}'.format(_far_or_nearby)

        self.VERSIONS_TO_BRANCH = self.versions_to_branch[branchIndex]
        _branches = '' if not self.VERSIONS_TO_BRANCH else 'B'

        self.UPDATES_TO_MODIFIED_UPDATE = self.updates_to_modified_update[modifiedUpdateIndex][1]
        self.FROM_LAST_X_UPDATES = self.updates_to_modified_update[modifiedUpdateIndex][0]
        _modified_updates = '' if not self.UPDATES_TO_MODIFIED_UPDATE else (
            'ML' if self.updates_to_modified_update[modifiedUpdateIndex][0] else 'MA')

        # implicit or explicit reference
        self.REFERENCE_IMPLICIT = True if self.reference[referenceIndex] == 'implicit' else False
        self.REFERENCE_EXPLICIT = True if self.reference[referenceIndex] == 'explicit' else False
        _reference = 'RI' if self.REFERENCE_IMPLICIT else 'RE'

        # Fetching all updates or specific updates
        self.FETCHING_ALL = True if self.reference[fetchingIndex] == 'all' else False
        self.FETCHING_SPECIFIC = True if self.reference[fetchingIndex] == 'specific' else False
        _fetching = 'FA' if self.REFERENCE_IMPLICIT else 'FS'

        # Content of updates repeated or related
        self.CONTENT_RELATED = True if self.reference[contentIndex] == 'repeated' else False
        self.CONTENT_REPEATED = True if self.reference[contentIndex] == 'related' else False
        _repeated = 'CL' if self.CONTENT_RELATED else 'CP'

        _ingestion_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width,
                                       _reference, _repeated, _snapshots, _branches, _modified_updates))

        self.ingestion_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B',
                                                        'ingestion', _ingestion_results + '.json')

        self.revision_store_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-revisions',
                                                     _ingestion_results + '.nt')

        self.NUMBER_OF_QUERIES = self.number_of_queries[numberOfQueriesIndex]

        _query_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width, _reference,
                                   _fetching, _repeated, _snapshots, _branches, _modified_updates,
                                   self.QUERY_ATOM, self.QUERY_TYPE))
        self.query_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'query',
                                                    _query_results + '.txt')


