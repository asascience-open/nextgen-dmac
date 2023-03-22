# Kerchunk ingest for NOS model data

This folder houses a generic debian docker image that can run arbitrary kerchunk scripts. The main use case is for as a use in [argo-workflows](https://argoproj.github.io/argo-workflows), where the image can be used to compose kerchunk workflows as results of [events](https://argoproj.github.io/argo-events/).

Aside from the dockerfile, there are also workflows and kerchunk python scripts to ingest IOOS model data files as kerchunked files.

## Details

In AWS, an SQS queue (`nextgen-nos-newofsobject`) is subscribed to `arn:aws:sns:us-east-1:123901341784:NewOFSObject` which is documented here https://registry.opendata.aws/noaa-ofs/ . This notification is published for every OFS Object added to the bucket. This means we get a notification in the SQS channel every time that a model run output is published. 

This PR adds `nos-sqs-source.yaml` which subscribes to the SQS channel, `nos-sqs-sensor.yaml` which triggers our kerchunking python script. 

The python script runs in a custom docker image and kerchunks the OFS netcdf file and then writes it to the `nextgen-dmac` S3 bucket. 

For now, only `dbofs` model output is acted upon to prove out the concept. 

### Multizarr

Once the NOS objects are injested as Zarr files to the nextgen-dmac bucket, they can be be combined to multizarr files using kerchunk. To do this, some AWS architecture is needed: 

```
Zarr -> s3 (nextgen-dmac) -> SNS (arn:aws:sns:us-east-1:579273261343:NOSZarrUpdated) -> SQS (nextgen-nos-updatemultizarr)
```

Once the object notifications are published to the SQS channel, an argo workflow is triggered to update the multizarr files so they can be viewed as an FMRC.

## Notes

The keys for aws-secret need SQS, SNS, and S3 permissions.

Test `argo-events` using the webhooks source example: https://argoproj.github.io/argo-events/quick_start/

Clear `Success` or `Error` Pods: 

```bash
kubectl delete pod --field-selector=status.phase==Succeeded -n argo-events
kubectl delete pod --field-selector=status.phase==Failed -n argo-events
```

Build and push the docker image:

https://gallery.ecr.aws/m2c5k9c1/nextgen-dmac/kerchunk-nos

```bash
aws ecr-public get-login-password --region us-east-1 --no-verify-ssl | docker login --username AWS --password-stdin public.ecr.aws/m2c5k9c1
docker build -t nextgen-dmac/kerchunk-nos .
docker tag nextgen-dmac/kerchunk-nos:latest public.ecr.aws/m2c5k9c1/nextgen-dmac/kerchunk-nos:latest
docker push public.ecr.aws/m2c5k9c1/nextgen-dmac/kerchunk-nos:latest
```