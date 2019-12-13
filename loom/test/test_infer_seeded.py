import json

import numpy as np
import pandas as pd

import loom.tasks


def infer_and_relate(seed):
    """Helper function to infer taxi model using a given seed."""
    config = {'schedule': {'extra_passes': 1}, 'seed': seed}
    name = 'seeding-test'
    loom.tasks.ingest(name, 'synth-schema.json', 'synth.csv', debug=True)
    loom.tasks.infer(name, sample_count=1, config=config, debug=True)
    with loom.tasks.query(name) as server:
        dependencies = server.relate(['a', 'b'])
    return dependencies


def test_seeding():
    """Test if infer uses the given seed."""
    schema = {'a': 'nich', 'b': 'nich'}
    with open('synth-schema.json', 'w') as outfile:
        json.dump(schema, outfile)
    df = pd.DataFrame({
        'a': np.random.normal(0, 1, size=10),
        'b': np.random.normal(0, 1, size=10)
    })
    df.to_csv('synth.csv', index=False)
    dependencies_1 = infer_and_relate(42)
    dependencies_2 = infer_and_relate(42)
    assert dependencies_1 == dependencies_2, 'Setting seed failed.'
    dependencies_3 = infer_and_relate(43)
    assert dependencies_1 != dependencies_3, 'Setting seed failed.'
