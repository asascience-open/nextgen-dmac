import ingest_tools.filters as filters


def test_contains_filter():
    k = 'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    assert filters.key_contains(k, ['dbofs', 'cbofs'])

    k = 'nos/cbofs/nos.cbofs.fields.f001.20230315.t00z.nc.zarr'
    assert not filters.key_contains(k, ['dbofs'])
