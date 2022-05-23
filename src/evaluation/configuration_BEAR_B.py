import os


class BearBConfiguration(object):

    raw_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'BEAR-B-alldata.CB.nt', '')
    processed_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-processed', 'by-date', '')
    updates_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'BEAR-B-processed', 'updates', '')

    REFERENCE_IMPLICIT = True  # implicit or explicit
    REFERENCE_EXPLICIT = False
    _reference = 'RI' if REFERENCE_IMPLICIT else 'RE'

    FETCHING_ALL = True  # implicit or explicit
    FETCHING_SPECIFIC = False
    _fetching = 'FA' if REFERENCE_IMPLICIT else 'FS'

    CONTENT_RELATED = True  # implicit or explicit
    CONTENT_REPEATED = False
    _repeated = 'CL' if REFERENCE_IMPLICIT else 'CP'


    MEAN_closeness = 15768000
    STANDARD_DEVIATION_closeness = 1000000      # 1000000, 10000000
    _closeness = 'LC' if STANDARD_DEVIATION_closeness == 1000000 else 'HC'

    MEAN_width = 432000     # 432000, 4320000
    STANDARD_DEVIATION_width = 86400
    _width = 'LW' if MEAN_width == 432000 else 'HW'

    MAX_SECONDS = int(31536000)

    NUMBER_OF_INSTANCES = 1     # 1, 2, 4
    START_DATE = '2015-01-01T00:00:00+00:00'
    QUERY_TIME = '2015-07-01T00:00:00+00:00'

    SEED = 0    # 0, 1, 2, 3, 4

    updates_file_name = '_'.join((str(SEED), str(NUMBER_OF_INSTANCES), _closeness, _width))
    statistics_updates_file_name = 'statistics_' + updates_file_name

    NUMBER_UPDATES_TO_SNAPSHOT = 20000
    _far_or_nearby = 'F'
    _snapshot_effective_dates = {'F': {'LC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'},
                                       'HC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'}},
                                 'N': {'LC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'},
                                       'HC': {'LW': '2015-07-01T00:00:00+00:00', 'HW': '2015-07-01T00:00:00+00:00'}}}
    SNAPSHOT_EFFECTIVE_DATE = _snapshot_effective_dates[_far_or_nearby][_closeness][_width]
    _snapshots = '' if NUMBER_UPDATES_TO_SNAPSHOT is None else 'S{0}'.format(_far_or_nearby)

    NUMBER_UPDATES_TO_BRANCH = 100
    _branches = '' if NUMBER_UPDATES_TO_BRANCH is None else 'B'

    NUMBER_UPDATES_TO_MODIFIED_UPDATE = 5
    FROM_LAST_X_UPDATES = 10    #
    _modified_updates = '' if NUMBER_UPDATES_TO_MODIFIED_UPDATE is None else 'MU'

    _ingestion_results = '_'.join((str(SEED), str(NUMBER_OF_INSTANCES), _reference, _repeated, _closeness, _width,
                                   _snapshots, _branches, _modified_updates))

    ingestion_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'ingestion',
                                               _ingestion_results)

    QUERY_TYPE = 'po'    # po or p
    QUERY_ATOM = 'VM'   # VM, DM, VQ
    NUMBER_OF_VERSIONS = 89

    _bear_b_results = {'VM': 'mat-{0}-queries'.format(QUERY_TYPE), 'DM': 'diff-{0}-queries'.format(QUERY_TYPE),
                       'VQ': 'ver-{0}-queries'.format(QUERY_TYPE)}

    bear_results_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw',
                                    'BEAR-B-{0}'.format(_bear_b_results[QUERY_ATOM]), '')
    bear_results_file_name = _bear_b_results[QUERY_ATOM]

    bear_queries_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'BEAR-B-queries',
                                          '{0}-queries.nt'.format(QUERY_TYPE))

    _query_results = '_'.join((str(SEED), str(NUMBER_OF_INSTANCES), _reference, _fetching, _repeated,
                               _closeness, _width, _snapshots, _branches, _modified_updates, QUERY_ATOM, QUERY_TYPE))
    query_results_file_name = os.path.join(os.path.dirname(__file__), '..', '..', 'results', 'BEAR-B', 'query',
                                           _query_results)


