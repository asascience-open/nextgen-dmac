# Ingest Service

Lambda service to ingest netcdf files from S3, kerchunk them, and store zarr metadata in s3 bucket

This service uses the [ingest-tools](https://github.com/asascience-open/nextgen-dmac/blob/main/kerchunk/Dockerfile) base image which is published as `nextgen-dmac/ingest-tools` on ECR.

TODO: Writeup the kerchunk process and flow of the lambda
