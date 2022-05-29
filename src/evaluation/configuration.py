import os


class BearBConfiguration(object):

    raw_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw', 'BEAR-B-alldata.CB.nt', '')

    MAX_SECONDS = 31536000
    START_EFFECTIVE_DATE = '2015-01-01T00:00:00+00:00'
    QUERY_TIME = '2015-07-01T00:00:00+00:00'

    REFERENCE_IMPLICIT = False  # implicit or explicit
    REFERENCE_EXPLICIT = True
    _reference = 'RI' if REFERENCE_IMPLICIT else 'RE'

    FETCHING_ALL = True  # implicit or explicit
    FETCHING_SPECIFIC = False
    _fetching = 'FA' if REFERENCE_IMPLICIT else 'FS'

    CONTENT_RELATED = False  # implicit or explicit
    CONTENT_REPEATED = True
    _repeated = 'CL' if REFERENCE_IMPLICIT else 'CP'

    MEAN_closeness = 15768000
    standard_deviations_closeness = [1000000, 10000000]

    STANDARD_DEVIATION_width = 86400
    means_width = [432000, 4320000]

    triples_per_updates = [5, 10, 20, 40]

    seeds = [0, 1, 2, 3, 4]

    _snapshot_effective_dates = {'F': {'LC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'},
                                       'HC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'}},
                                 'N': {'LC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'},
                                       'HC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'}}}
    n_updates_to_snapshot = {5: [(None, None), ('F', 20000), ('N', 20000)],
                             10: [(None, None), ('F', 20000), ('N', 20000)],
                             20: [(None, None), ('F', 20000), ('N', 20000)],
                             40: [(None, None), ('F', 20000), ('N', 20000)]}

    n_updates_to_branch = {5: [None, 100], 10: [None, 100], 20: [None, 100], 40: [None, 100]}

    n_updates_to_modified_update = {5: [None, 5], 10: [None, 5], 20: [None, 5], 40: [None, 5]}
    FROM_LAST_X_UPDATES = 10

    QUERY_TYPE = 'p'    # po or p
    QUERY_ATOM = 'VM'   # VM, DM, VQ
    NUMBER_OF_VERSIONS = 89

    _bear_b_results = {'VM': 'mat-{0}-queries'.format(QUERY_TYPE), 'DM': 'diff-{0}-queries'.format(QUERY_TYPE),
                       'VQ': 'ver-{0}-queries'.format(QUERY_TYPE)}

    bear_results_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                    'BEAR-B-{0}'.format(_bear_b_results[QUERY_ATOM]), _bear_b_results[QUERY_ATOM])

    bear_queries_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw', 'BEAR-B-queries',
                                          '{0}-queries.nt'.format(QUERY_TYPE))

    def __init__(self, indexCloseness=0, indexWidth=0, triplesPerUpdateIndex=0, seedIndex=0, snapshotIndex=0,
                 branchIndex=0, modifiedUpdateIndex=0):

        self.STANDARD_DEVIATION_closeness = self.standard_deviations_closeness[indexCloseness]
        _closeness = 'LC' if self.STANDARD_DEVIATION_closeness == 1000000 else 'HC'

        self.MEAN_width = self.means_width[indexWidth]
        _width = 'LW' if self.MEAN_width == 432000 else 'HW'

        self.TRIPLES_PER_UPDATE = self.triples_per_updates[triplesPerUpdateIndex]
        self.SEED = self.seeds[seedIndex]

        self._updates = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width))
        self.updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-updates',
                                              self._updates + '.txt')
        self.statistics_updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B',
                                                         'statistics', 'updates_stats_' + self._updates + '.txt')

        self.NUMBER_UPDATES_TO_SNAPSHOT = self.n_updates_to_snapshot[self.TRIPLES_PER_UPDATE][snapshotIndex][1]
        _far_or_nearby = self.n_updates_to_snapshot[self.TRIPLES_PER_UPDATE][snapshotIndex][0]
        if self.NUMBER_UPDATES_TO_SNAPSHOT:
            self.SNAPSHOT_EFFECTIVE_DATE = self._snapshot_effective_dates[_far_or_nearby][_closeness][_width]
        _snapshots = '' if not self.NUMBER_UPDATES_TO_SNAPSHOT else 'S{0}'.format(_far_or_nearby)

        self.NUMBER_UPDATES_TO_BRANCH = self.n_updates_to_branch[self.TRIPLES_PER_UPDATE][branchIndex]
        _branches = '' if not self.NUMBER_UPDATES_TO_BRANCH else 'B'

        self.NUMBER_UPDATES_TO_MODIFIED_UPDATE = \
            self.n_updates_to_modified_update[self.TRIPLES_PER_UPDATE][modifiedUpdateIndex]
        _modified_updates = '' if not self.NUMBER_UPDATES_TO_MODIFIED_UPDATE else 'MU'

        _ingestion_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width,
                                       self._reference, self._repeated,_snapshots, _branches, _modified_updates))

        self.ingestion_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B',
                                                        'ingestion', _ingestion_results + '.json')

        self.revision_store_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-revisions',
                                                     _ingestion_results + '.nt')

        _query_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width, self._reference,
                                   self._fetching, self._repeated, _snapshots, _branches, _modified_updates,
                                   self.QUERY_ATOM, self.QUERY_TYPE))
        self.query_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'query',
                                                    _query_results + '.json')


