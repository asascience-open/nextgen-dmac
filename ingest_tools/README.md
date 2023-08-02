# ingest tools

A python package and docker image for ingesting data into cloud native zarr virtual datasets

## Developing

Create a virtualenv and install the dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

## Testing

Install the package in editable mode and run the tests:

```bash
pip install -e .
pytest
```

## Dockerizing

**TODO**