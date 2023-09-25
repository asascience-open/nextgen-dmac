from ingest_tools.rtofs import generate_rtofs_output_key, generate_rtofs_best_time_series_glob_expression, parse_rtofs_model_run_datestamp_offset, generate_rtofs_best_timeseries_key


def test_generate_rtofs_output_key():
    key = 'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc'
    output_key = generate_rtofs_output_key(key) 
    assert output_key == 'rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'


def test_generate_rtofs_best_time_series_glob_expression():
    key = 'rtofs/rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'
    glob_expression = generate_rtofs_best_time_series_glob_expression(key)
    assert glob_expression == 'rtofs/rtofs.*.rtofs_glo_2ds_f*_diag.nc.zarr'

    key = 'rtofs/rtofs.20230925.rtofs_glo_3dz_f006_6hrly_hvr_US_east.nc.zarr'
    glob_expression = generate_rtofs_best_time_series_glob_expression(key)
    assert glob_expression == 'rtofs/rtofs.*.rtofs_glo_3dz_f*_6hrly_hvr_US_east.nc.zarr'


def test_parse_rtofs_model_run_datestamp_offset():
    key = 'rtofs/rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'
    model_date, offset = parse_rtofs_model_run_datestamp_offset(key)
    assert model_date == '20230922T01'
    assert offset == 1

    key = 'rtofs/rtofs.20230925.rtofs_glo_3dz_f006_6hrly_hvr_US_east.nc.zarr'
    model_date, offset = parse_rtofs_model_run_datestamp_offset(key)
    assert model_date == '20230925T06'
    assert offset == 6


def test_generate_best_timeseries_key():
    glob = 'rtofs/rtofs.*.rtofs_glo_2ds_f*_diag.nc.zarr'
    best_timeseries_key = generate_rtofs_best_timeseries_key(glob)
    assert best_timeseries_key == 'rtofs/rtofs.rtofs_glo_2ds_diag.best.nc.zarr'

    glob = 'rtofs/rtofs.*.rtofs_glo_3dz_f*_6hrly_hvr_US_east.nc.zarr'
    best_timeseries_key = generate_rtofs_best_timeseries_key(glob)
    assert best_timeseries_key == 'rtofs/rtofs.rtofs_glo_3dz_6hrly_hvr_US_east.best.nc.zarr'