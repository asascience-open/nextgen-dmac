# Prototype kerchunk grib aggregation & zarr IO for fast machine learning

This is prototype code shared by Camus Energy to demonstrate possible optimizations for kerchunk and zarr based on
our experience with machine learning in GCP.

We hope to move much of the dynamic zarr store code into Kerchunk in the summer of 2024 with help from the community
and an [IOOS GSOC intern](https://github.com/ioos/gsoc/issues/42).

The zarr parallelization is more difficult to see a path to integration in zarr python 2, but hopefully will be a
benchmark for performance in zarr python 3.

## Setup

```console
mkdir venv
python -m venv venv
source  venv/bin/activate
pip install -r requirements.txt

jupyter-lab build

jupyter-lab --port=${CAMUS_JUPYTER_PORT} --log_level=INFO --ip=0.0.0.0 --NotebookApp.notebook_dir=$(pwd -P)

python -m unittest
```




