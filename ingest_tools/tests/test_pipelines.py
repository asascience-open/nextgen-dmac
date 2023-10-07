from ingest_tools.pipeline import Pipeline
from ingest_tools.rtofs import RTOFS_Pipeline


def test_rtofs_pipeline():
    p = RTOFS_Pipeline()
    test_file = 'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc'
    assert_pipeline_accepts(p, test_file)
    assert_pipeline_output_key(p, test_file, 'rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr')

def assert_pipeline_accepts(pipeline: Pipeline, test_key: str):
    assert pipeline.accepts(test_key)

def assert_pipeline_output_key(pipeline: Pipeline, test_key: str, expected_key: str):
    out_key = pipeline.generate_output_key(test_key)
    assert out_key == expected_key