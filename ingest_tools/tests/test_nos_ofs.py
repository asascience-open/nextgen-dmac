import ingest_tools.nos_ofs as nos_ofs


def test_parse_model_run_datestamp():
    key = 'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    model_date, model_hour = nos_ofs.parse_model_run_datestamp(key)
    assert model_date == '20230315'
    assert model_hour == '00'


def test_parse_model_run_datestamp_offset():
    key = 'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    model_date, offset = nos_ofs.parse_model_run_datestamp_offset(key)
    assert model_date == '20230315T01'
    assert offset == 1


def test_generate_model_run_glob_expression():
    key = 'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    model_date, model_hour = nos_ofs.parse_model_run_datestamp(key)
    glob_expression = nos_ofs.generate_model_run_glob_expression(key, model_date, model_hour)
    assert glob_expression == 'nos/nos.dbofs.fields.f*.20230315.t00z.nc.zarr'


def test_generate_best_time_series_glob_expression():
    key = 'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    glob_expression = nos_ofs.generate_best_time_series_glob_expression(key)
    assert glob_expression == 'nos/nos.dbofs.fields.f*.*.t*z.nc.zarr'
