from ingest_tools.rtofs import generate_rtofs_output_key


def test_generate_rtofs_output_key():
    key = 'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc'
    output_key = generate_rtofs_output_key(key) 
    assert output_key == 'rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'