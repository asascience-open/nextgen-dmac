import pytest
from ingest_tools.fvcom import FVCOM_Pipeline
from ingest_tools.nos_ofs import NOS_Pipeline
from ingest_tools.pipeline import Pipeline
from ingest_tools.rtofs import RTOFS_Pipeline


# TODO: This is simply a demonstration, but we can make this more robust to automatically test all available pipelines
@pytest.mark.parametrize('test_input', [
    [RTOFS_Pipeline(), 'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc', 'rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'],
    [NOS_Pipeline(), 'tbofs.20230314/nos.tbofs.fields.n002.20230314.t00z.nc', 'tbofs/nos.tbofs.fields.n002.20230314.t00z.nc.zarr'],
    [FVCOM_Pipeline(), 'ngofs2/nos.ngofs2.fields.f042.20231003.t09z.nc', 'ngofs2/nos.ngofs2.fields.f042.20231003.t09z.nc.zarr']
    ])
def test_pipelines(test_input):
    pipeline = test_input[0]
    test_key = test_input[1]
    expected_key = test_input[2]

    assert pipeline.accepts(test_key)
    
    out_key = pipeline.generate_output_key(test_key)
    assert out_key == expected_key