import loom.tasks


def _hepler_travis():
    """Silly helper to debug travis :( To be deleted."""
    import os
    cwd1 = os.getcwd()
    ls1 = '\n'.join(os.listdir(cwd1))
    os.chdir('..')
    cwd2 = os.getcwd()
    ls2 = '\n'.join(os.listdir(cwd2))
    return cwd1 + '\n' + ls1 + '\n\n\n' + cwd2 +  '\n' +ls2

def _learn_seeded_taxi(seed):
    """Helper function to learn the exact same taxi model with different seeds"""
    config = {'schedule': {'extra_passes': 1.0}, 'seed':seed}
    path_to_taxi = 'examples/taxi'
    name = 'taxi-{}'.format(seed)
    message = _hepler_travis()
    assert False, message
    loom.tasks.ingest(
            name,
            '{}/schema.json'.format(path_to_taxi),
            '{}/example.json'.format(path_to_taxi),
            debug=True
    )
    loom.tasks.infer(name, sample_count=0, config=config, debug=True)
    with loom.tasks.query(name) as server:
        dependencies = server.relate(['fare_amount', 'surchare'])
    return dependencies

def test_seeding():
    """Test if we can seed learning"""
    dependencies_1 = _learn_seeded_taxi(42)
    dependencies_2 = _learn_seeded_taxi(42)
    assert dependencies_1 == dependencies_2, 'Setting the seeed did not work'
    dependencies_3 = _learn_seeded_taxi(43)
    assert dependencies_1 != dependencies_3, 'Auto-seeding overwrites our seed'
    raise ValueError('this worked!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')



