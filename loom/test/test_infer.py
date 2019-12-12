import loom.tasks

def test_seeding():
    """Test if we can seed learning"""
    config = {'schedule': {'extra_passes': 1.0}, 'seed':42}
    path_to_taxi = 'examples/taxi/'
    loom.tasks.ingest(name, path_to_taxi + 'schema.json', 'example.json', debug=True)
    loom.tasks.infer(name, sample_count=0, config=config, debug=True)
    with loom.tasks.query(name) as server:
        print server.relate(['fare_amount', 'surchare'])
    raise ValueError('this worked!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
