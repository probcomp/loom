import pandas as pd
import numpy as np
import json

import loom.tasks

def _learn_seeded_model(seed):
    """Helper function to learn the exact same taxi model with different seeds"""
    config = {'schedule': {'extra_passes': 1.0}, 'seed':seed}
    name = 'seeding-test'
    loom.tasks.ingest(name, 'synth-schema.json', 'synth.csv', debug=True)
    loom.tasks.infer(name, sample_count=1, config=config, debug=True)
    with loom.tasks.query(name) as server:
        dependencies = server.relate(['a', 'b'])
    return dependencies

def test_seeding():
    """Test if we can seed learning"""
    schema = {'a':'nich', 'b': 'nich'}
    with open('synth-schema.json', 'w') as outfile:
        json.dump(schema, outfile)
    df = pd.DataFrame({
        'a':np.random.normal(0, 1, size=10),
        'b':np.random.normal(0, 1, size=10)
    })
    df.to_csv('synth.csv', index=False)
    dependencies_1 = _learn_seeded_model(42)
    dependencies_2 = _learn_seeded_model(42)
    assert dependencies_1 == dependencies_2, 'Setting the seeed did not work'
    dependencies_3 = _learn_seeded_model(43)
    assert dependencies_1 != dependencies_3, 'Auto-seeding overwrites our seed'
