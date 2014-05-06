import os
import sys
import shutil
import tempfile
import contextlib
from collections import defaultdict
from itertools import imap, product
from nose import SkipTest
from nose.tools import assert_true, assert_equal
import numpy
import numpy.random
from distributions.tests.util import seed_all
from distributions.util import scores_to_probs
#from distributions.fileutil import tempdir
from distributions.io.stream import protobuf_stream_load, protobuf_stream_dump
from distributions.lp.models import dd, dpd, nich, gp
from distributions.lp.clustering import PitmanYor
from distributions.util import multinomial_goodness_of_fit
import loom.schema_pb2
import loom.runner
import loom.util

assert SkipTest and dd and dpd and gp and nich  # pacify pyflakes

CLEANUP_ON_ERROR = int(os.environ.get('CLEANUP_ON_ERROR', 1))

SAMPLE_COUNT = 10000
TRUNCATE_COUNT = 32
MIN_GOODNESS_OF_FIT = 1e-3
SEED = 123456789

CLUSTERING = PitmanYor.from_dict({'alpha': 2.5, 'd': 0.0})

# There is no clear reason to expect feature_type to matter in posterior
# enumeration tests.  We run NICH because it is fast; anecdotally GP may be
# more sensitive in catching bugs.  Errors in other feature_types should be
# caught by other tests.
FEATURE_TYPES = {
    #'dd': dd,
    #'dpd': dpd,
    'nich': nich,
    #'gp': gp,
}

CAT_DIMENSIONS = [
    (rows, cols) for rows in xrange(2, 6) for cols in xrange(1, 3)
]

# This list was suggested by suggest_small_datasets below.
# For more suggestions, run python test_posterior_enum.py
KIND_DIMENSIONS = [
    (6, 1),  # does not test kind kernel
    (5, 2),
    (3, 3),
    (2, 4),
    (1, 6),
    # LARGE
    (3, 4), (4, 3),
    # SHORT
    #(1, 3),
    #(1, 4),
    # NARROW
    #(1, 2),
    #(2, 2),
    #(3, 2),
    #(4, 2),
    # TINY
    (3, 2), (2, 3),
    # HUGE, under 300k cells
    #(2,8), (3,6), (4,4), (5,3), (6,2),
]

DENSITIES = [
    1.0,
    0.5,
    0.0,
]


@contextlib.contextmanager
def tempdir(cleanup_on_error=True):
    oldwd = os.getcwd()
    wd = tempfile.mkdtemp()
    try:
        os.chdir(wd)
        yield wd
        cleanup_on_error = True
    finally:
        os.chdir(oldwd)
        if cleanup_on_error:
            shutil.rmtree(wd)


def test_cat_inference():
    datasets = product(CAT_DIMENSIONS, FEATURE_TYPES, DENSITIES, [False])
    errors = sum(loom.util.parallel_map(_test_dataset, datasets), [])
    assert_true(not errors, '\n'.join(['Failed'] + errors))


def test_kind_inference():
    raise SkipTest('FIXME kind kernel does not mix correctly')
    datasets = product(KIND_DIMENSIONS, FEATURE_TYPES, DENSITIES, [True])
    errors = sum(loom.util.parallel_map(_test_dataset, datasets), [])
    assert_true(not errors, '\n'.join(['Failed'] + errors))


def _test_dataset((dim, feature_type, density, infer_kind_structure)):
    seed_all(SEED)
    object_count, feature_count = dim
    errors = []
    with tempdir(cleanup_on_error=CLEANUP_ON_ERROR):

        model_name = os.path.abspath('model.pb')
        rows_name = os.path.abspath('rows.pbs')

        model = generate_model(feature_count, feature_type)
        dump_model(model, model_name)

        rows = generate_rows(
            object_count,
            feature_count,
            feature_type,
            density)
        dump_rows(rows, rows_name)

        if infer_kind_structure:
            configs = [
                {'kind_count': 0, 'kind_iters': 0},
                {'kind_count': 1, 'kind_iters': 10},
                {'kind_count': 10, 'kind_iters': 1},
            ]
        else:
            configs = [{'kind_count': 0, 'kind_iters': 0}]

        for config in configs:
            casename = '{}-{}-{}-{}-{}-{}'.format(
                object_count,
                feature_count,
                feature_type,
                density,
                config['kind_count'],
                config['kind_iters'])
            print 'Running', casename
            error = _test_dataset_config(
                casename,
                object_count,
                feature_count,
                model_name,
                rows_name,
                config)
            if error is not None:
                errors.append(error)
    return errors


def _test_dataset_config(
        casename,
        object_count,
        feature_count,
        model_name,
        rows_name,
        config):
    counts_dict = defaultdict(lambda: 0)
    scores_dict = {}
    for sample, score in generate_samples(model_name, rows_name, config):
        counts_dict[sample] += 1
        scores_dict[sample] = score

    latents = scores_dict.keys()
    expected_latent_count = count_crosscats(object_count, feature_count)
    assert len(latents) <= expected_latent_count, 'programmer error'
    #assert_equal(len(latents), expected_latent_count)  # too sensitive

    counts = numpy.array([counts_dict[key] for key in latents])
    scores = numpy.array([scores_dict[key] for key in latents])
    probs = scores_to_probs(scores)

    highest_by_prob = numpy.argsort(probs)[::-1][:TRUNCATE_COUNT]
    is_accurate = lambda p: SAMPLE_COUNT * p * (1 - p) >= 1
    highest_by_prob = [i for i in highest_by_prob if is_accurate(probs[i])]
    highest_by_count = numpy.argsort(counts)[::-1][:TRUNCATE_COUNT]
    highest = list(set(highest_by_prob) | set(highest_by_count))
    truncated = len(highest_by_prob) < len(probs)

    goodness_of_fit = multinomial_goodness_of_fit(
        probs[highest_by_prob],
        counts[highest_by_prob],
        total_count=SAMPLE_COUNT,
        truncated=truncated)

    message = '{}, goodness of fit = {:0.3g}'.format(casename, goodness_of_fit)
    if goodness_of_fit > MIN_GOODNESS_OF_FIT:
        print 'Passed {}'.format(message)
        return None
    else:
        print 'EXPECT\tACTUAL\tVALUE'
        lines = [(probs[i], counts[i], latents[i]) for i in highest]
        for prob, count, latent in sorted(lines, reverse=True):
            expect = prob * SAMPLE_COUNT
            pretty = pretty_latent(latent)
            print '{:0.1f}\t{}\t{}'.format(expect, count, pretty)
        print 'Failed {}'.format(message)
        return message


def generate_model(feature_count, feature_type):
    module = FEATURE_TYPES[feature_type]
    shared = module.Shared.from_dict(module.EXAMPLES[0]['shared'])
    cross_cat = loom.schema_pb2.CrossCat()
    kind = cross_cat.kinds.add()
    CLUSTERING.dump_protobuf(kind.product_model.clustering.pitman_yor)
    for featureid in xrange(feature_count):
        shared.dump_protobuf(kind.product_model.nich.add())
        kind.featureids.append(featureid)
        cross_cat.featureid_to_kindid.append(0)
    CLUSTERING.dump_protobuf(cross_cat.feature_clustering.pitman_yor)
    return cross_cat


def test_generate_model():
    for feature_type in FEATURE_TYPES:
        generate_model(10, feature_type)


def dump_model(model, model_name):
    with open(model_name, 'wb') as f:
        f.write(model.SerializeToString())


def generate_rows(object_count, feature_count, feature_type, density):
    assert object_count > 0, object_count
    assert feature_count > 0, feature_count
    assert 0 <= density and density <= 1, density

    # generate structure
    feature_assignments = CLUSTERING.sample_assignments(feature_count)
    kind_count = len(set(feature_assignments))
    object_assignments = [
        CLUSTERING.sample_assignments(object_count)
        for _ in xrange(kind_count)
    ]
    group_counts = [
        len(set(assignments))
        for assignments in object_assignments
    ]

    # generate data
    module = FEATURE_TYPES[feature_type]
    shared = module.Shared.from_dict(module.EXAMPLES[0]['shared'])

    def sampler_create():
        group = module.Group()
        group.init(shared)
        sampler = module.Sampler()
        sampler.init(shared, group)
        return sampler

    table = [[None] * feature_count for _ in xrange(object_count)]
    for f, k in enumerate(feature_assignments):
        samplers = [sampler_create() for _ in xrange(group_counts[k])]
        for i, g in enumerate(object_assignments[k]):
            if numpy.random.uniform() < density:
                table[i][f] = samplers[g].eval(shared)
    return table


def test_generate_rows():
    table = generate_rows(100, 100, 'nich', 1.0)
    assert_true(all(cell is not None for row in table for cell in row))

    table = generate_rows(100, 100, 'nich', 0.0)
    assert_true(all(cell is None for row in table for cell in row))

    table = generate_rows(100, 100, 'nich', 0.5)
    assert_true(any(cell is None for row in table for cell in row))
    assert_true(any(cell is not None for row in table for cell in row))


def serialize_rows(table):
    message = loom.schema_pb2.SparseRow()
    for i, values in enumerate(table):
        message.id = i
        for value in values:
            message.data.observed.append(value is not None)
            if value is None:
                pass
            elif isinstance(value, bool):
                message.data.booleans.append(value)
            elif isinstance(value, int):
                message.data.counts.append(value)
            elif isinstance(value, float):
                message.data.reals.append(value)
            else:
                raise ValueError('unknown value type: {}'.format(value))
        yield message.SerializeToString()
        message.Clear()


def dump_rows(table, rows_name):
    protobuf_stream_dump(serialize_rows(table), rows_name)


def test_dump_rows():
    for feature_type in FEATURE_TYPES:
        table = generate_rows(10, 10, feature_type, 0.5)
        with tempdir():
            rows_name = os.path.abspath('rows.pbs')
            dump_rows(table, rows_name)
            message = loom.schema_pb2.SparseRow()
            for string in protobuf_stream_load(rows_name):
                message.ParseFromString(string)
                print message


def generate_samples(model_name, rows_name, config):
    with tempdir(cleanup_on_error=CLEANUP_ON_ERROR):
        samples_name = os.path.abspath('samples.pbs.gz')
        loom.runner.posterior_enum(
            model_name,
            rows_name,
            samples_name,
            SAMPLE_COUNT,
            #debug=True,  # DEBUG,
            **config)
        message = loom.schema_pb2.PosteriorEnum.Sample()
        count = 0
        for string in protobuf_stream_load(samples_name):
            message.ParseFromString(string)
            sample = parse_sample(message)
            score = float(message.score)
            yield sample, score
            count += 1
        assert count == SAMPLE_COUNT


def parse_sample(message):
    return frozenset(
        (
            frozenset(kind.featureids),
            frozenset(frozenset(group.rowids) for group in kind.groups)
        )
        for kind in message.kinds
    )


def pretty_kind(kind):
    featureids, groups = kind
    return '{} |{}|'.format(
        ' '.join(imap(str, sorted(featureids))),
        '|'.join(sorted(
            ' '.join(imap(str, sorted(group)))
            for group in groups
        ))
    )


def pretty_latent(latent):
    return ' - '.join(sorted(pretty_kind(kind) for kind in latent))


#-----------------------------------------------------------------------------
# dataset suggestions

def enum_partitions(count):
    if count == 0:
        yield []
    elif count == 1:
        yield [[1]]
    else:
        for p in enum_partitions(count - 1):
            yield p + [[count]]
            for i, part in enumerate(p):
                yield p[:i] + [part + [count]] + p[1 + i:]


BELL_NUMBERS = [1, 1, 2, 5, 15, 52, 203, 877, 4140, 21147, 115975]


def test_enum_partitions():
    for i, bell_number in enumerate(BELL_NUMBERS):
        count = sum(1 for _ in enum_partitions(i))
        assert_equal(count, bell_number)


def count_crosscats(rows, cols):
    return sum(
        BELL_NUMBERS[rows] ** len(kinds)
        for kinds in enum_partitions(cols))


def suggest_small_datasets(max_count=300):
    enum_partitions
    max_rows = 10
    max_cols = 10
    print '=== Cross cat latent space sizes up to {} ==='.format(max_count)
    print '\t'.join('{} col'.format(cols) for cols in range(1, 1 + max_cols))
    print '-' * 8 * max_cols
    suggestions = {}
    for rows in range(1, 1 + max_rows):
        counts = []
        for cols in range(1, 1 + max_cols):
            count = count_crosscats(rows, cols)
            if count > max_count:
                suggestions[cols] = rows
                break
            counts.append(count)
        print '\t'.join(str(c) for c in counts)
    suggestions = ', '.join([
        '({},{})'.format(rows, cols)
        for cols, rows in suggestions.iteritems()
    ])
    print 'suggested test cases:', suggestions


if __name__ == '__main__':
    args = sys.argv[1:]
    if args:
        max_count = int(args[0])
    else:
        max_count = 300
    suggest_small_datasets(max_count)
