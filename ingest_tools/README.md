# ingest tools

A python package and docker image for ingesting data into cloud native zarr virtual datasets

## Usage

### NOS OFS Model Data

This package contains tools for *kerchunking* NOS OFS model data into cloud native zarr virtual datasets. The usage this package is designed around, is for processing `AWS SQS` messages from NOS OFS NODD Bucket notifications. 

The first step in this process is *kerchunking* a single model timestep output into zarr metadata format.

**TODO** More info and instructions

The second step is generating virtual aggregations from the single model run outputs. This is typically done by scanning the bucket for matching files, applying the FMRC (link to logic here) logic to generate the virtual aggregation, and then writing the virtual aggregation to the bucket.

**TODO** More info and instructions

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

Build and push the docker image:

https://gallery.ecr.aws/m2c5k9c1/nextgen-dmac/ingest-tools

```bash
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/m2c5k9c1
docker build -t nextgen-dmac/ingest-tools .
docker tag nextgen-dmac/ingest-tools:latest public.ecr.aws/m2c5k9c1/nextgen-dmac/ingest-tools:latest
docker push public.ecr.aws/m2c5k9c1/nextgen-dmac/ingest-tools:latest
```
