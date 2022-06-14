import os
from datetime import datetime


class BearBConfiguration(object):

    raw_version_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                        'BEAR-B-alldata.IC.nt', '')
    raw_change_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                       'BEAR-B-alldata.CB.nt', '')

    MAX_SECONDS = 31536000
    START_EFFECTIVE_DATE = '2015-01-01T00:00:00+00:00'
    QUERY_TIME = '2015-07-01T00:00:00+00:00'

    MEAN_closeness = 15768000
    STANDARD_DEVIATION_width = 86400

    seeds = [0, 1, 2, 3, 4]

    _snapshot_effective_dates = {'F': '2015-03-01T00:00:00+00:00', 'N': '2015-07-01T00:00:00+00:00'}

    QUERY_TYPE = 'p'    # po or p
    QUERY_ATOM = 'VQ'   # VM, DM, VQ
    NUMBER_OF_VERSIONS = 89

    _bear_b_results = {'VM': 'mat-{0}-queries'.format(QUERY_TYPE), 'DM': 'diff-{0}-queries'.format(QUERY_TYPE),
                       'VQ': 'ver-{0}-queries'.format(QUERY_TYPE)}

    bear_results_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw',
                                    'BEAR-B-{0}'.format(_bear_b_results[QUERY_ATOM]), _bear_b_results[QUERY_ATOM])

    bear_queries_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-raw', 'BEAR-B-queries',
                                          '{0}-queries.nt'.format(QUERY_TYPE))

    BRANCH = "branch 87"

    def __init__(self, seed=0, closeness=1000000, width=4320000, triplesPerUpdate=10, snapshot=(None, None),
                 branch=None, modifiedUpdate=(None, None), reference='explicit', fetching='specific',
                 content='repeated', modifications="aggregated", numberOfQueries=None, retrieve='between'):

        # [1000000, 5000000]
        self.STANDARD_DEVIATION_closeness = closeness
        _closeness = 'LC' if self.STANDARD_DEVIATION_closeness == 1000000 else 'HC'

        # [432000, 4320000]
        self.MEAN_width = width
        _width = 'LW' if self.MEAN_width == 432000 else 'HW'

        # [50, 100]
        self.TRIPLES_PER_UPDATE = triplesPerUpdate
        self.SEED = seed

        _updates = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width))
        self.updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-updates',
                                              _updates + '.txt')
        self.statistics_updates_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B',
                                                         'statistics', 'updates_stats_' + _updates + '.json')

        _far_or_nearby, self.VERSIONS_TO_SNAPSHOT = snapshot
        if self.VERSIONS_TO_SNAPSHOT:
            self.SNAPSHOT_EFFECTIVE_DATE = self._snapshot_effective_dates[_far_or_nearby]
        _snapshots = '' if self.VERSIONS_TO_SNAPSHOT is None else 'S{0}'.format(_far_or_nearby)

        self.VERSIONS_TO_BRANCH = branch
        _branches = '' if self.VERSIONS_TO_BRANCH is None else 'B'

        self.FROM_LAST_X_UPDATES, self.UPDATES_TO_MODIFIED_UPDATE = modifiedUpdate
        _modified_updates = '' if self.UPDATES_TO_MODIFIED_UPDATE is None else ('ML' if self.FROM_LAST_X_UPDATES else 'MA')

        # implicit or explicit reference
        self.REFERENCE_IMPLICIT = True if reference == 'implicit' else False
        self.REFERENCE_EXPLICIT = True if reference == 'explicit' else False
        self.REFERENCE_COMBINED = True if reference == 'combined' else False
        _reference = 'RI' if self.REFERENCE_IMPLICIT else 'RE' if self.REFERENCE_EXPLICIT else 'RC'

        # Fetching all updates or specific updates
        self.FETCHING_ALL = True if fetching == 'all' else False
        self.FETCHING_SPECIFIC = True if fetching == 'specific' else False
        _fetching = 'FA' if self.FETCHING_ALL else 'FS'

        # Fetching all updates or specific updates
        self.MODIFICATIONS_AGGREGATED = True if modifications == 'aggregated' else False
        self.MODIFICATIONS_SORTED = True if modifications == 'sorted' else False
        _modifications = 'AG' if self.MODIFICATIONS_AGGREGATED else 'SO'

        # Content of updates repeated or related
        self.CONTENT_RELATED = True if content == 'related' else False
        self.CONTENT_REPEATED = True if content == 'repeated' else False
        _repeated = 'CL' if self.CONTENT_RELATED else 'CP'

        # Retrieving of versions between updates or from initial update
        self.RETRIEVING_BETWEEN = True if retrieve == 'between' else False
        self.RETRIEVING_INITIAL = True if retrieve == 'initial' else False
        _retrieve = 'IN' if self.RETRIEVING_INITIAL else ''

        _ingestion_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width,
                                       _reference, _repeated, _snapshots, _branches, _modified_updates))

        self.ingestion_results_file_name = os.path.join(
            os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'ingestion', reference,
            '{0}.json'.format(_ingestion_results))

        self.revision_store_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-revisions',
                                                     reference, _ingestion_results + '.nt')

        self.NUMBER_OF_QUERIES = numberOfQueries

        _query_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width, _reference,
                                   _fetching, _repeated, _snapshots, _branches, _modified_updates, _modifications,
                                   _retrieve, self.QUERY_ATOM, self.QUERY_TYPE))
        self.query_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'query',
                                                    self.QUERY_ATOM, reference, _query_results + '.txt')

        _figure_results = '_'.join((str(self.SEED), str(self.TRIPLES_PER_UPDATE), _closeness, _width, _fetching,
                                    _repeated, _snapshots, _branches, _modified_updates, _modifications,
                                    _retrieve, self.QUERY_ATOM, self.QUERY_TYPE))
        self.figures_query_results = os.path.join(os.path.dirname(__file__), '..', '..', 'figures', 'BEAR-B', 'query',
                                                  self.QUERY_ATOM, _figure_results + '.png')


