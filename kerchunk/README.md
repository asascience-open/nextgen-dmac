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

Clear `Completed` or `Error` Pods: 

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

Test the python script locally, install deps in a virtualenv: 

```bash
virtualenv -p python3 env
source env/bin/activate
pip install -r requirements.txt
```

Dump SQS message as it appears, useful for making sure argo commands are receiving the right inputs

```bash
python kerchunk_nos_roms.py dump '{"Type": "Notification","MessageId": "64e0cdc6-ce97-53cb-90b2-914f059429c1","TopicArn": "arn:aws:sns:us-east-1:123901341784:NewOFSObject","Subject": "Amazon S3 Notification","Message": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2023-03-14T01:07:14.304Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAJSLYAQIR3HHVYDOK2\"},\"requestParameters\":{\"sourceIPAddress\":\"34.195.147.78\"},\"responseElements\":{\"x-amz-request-id\":\"NS093145WJSB3M5P\",\"x-amz-id-2\":\"TBLTM5eN3RpYYyVJS0d1fGVwhpsjED4Tw7eZMf/D3sYJJNVpWg3TRw7X/pZ9S/mYIE8NWOZy8hDFgjGGMNzYTE2YZ+3S88dfwN32jnoeVrA=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"NTY5YmEwY2QtYzE3Zi00NTQ2LTllZTQtNzE1ZTEwMmIxMGFl\",\"bucket\":{\"name\":\"noaa-ofs-pds\",\"ownerIdentity\":{\"principalId\":\"A2AJV00K47QOI1\"},\"arn\":\"arn:aws:s3:::noaa-ofs-pds\"},\"object\":{\"key\":\"tbofs.20230314/nos.tbofs.fields.n002.20230314.t00z.nc\",\"size\":20138396,\"eTag\":\"6b6b182fdbacb0cfc7d491c0f48c7d26\",\"sequencer\":\"00640FC8C21FD07CDB\"}}}]}","Timestamp": "2023-03-14T01:07:15.001Z","SignatureVersion": "1","Signature": "DsU2+uGwqEfGQ6EHSW/swtx6JqBplP5b0KXXSyPX4ewZMS29GaCtCGrowIsyW9nWAV3bI8tnD44/32wOU5Xvh1Xm2P1aocDuHkZAsSOi859OyaJcdyBIKT8txJbSo+X+ql6t/bO/pI/A0Cnjwn9pZDHul648UnQamL0jAyqWN+MWhGd8efO4dbrloq5Zi4wPI3KRWgE0L2aYg6IPXAHxm1S0u+996iL3MpxVrlGvLBdVFZXg14G53z83xAhlT2XmNzpcnHy9Pkl2a9LpXusesFNgyrsFmzq0cBXYAliSj0ALHxVSOpOK2sp4KL1UyKjy0HBwhUKv4pOdDTA02VQxmg==","SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem","UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123901341784:NewOFSObject:d8f7dac5-0db1-4541-a0fb-4e13b43299f1"}'
```

Generate and upload a single zarr file from a given NOAA OFS SQS message:

```bash
python kerchunk_nos_roms.py extract_single '{"Type": "Notification","MessageId": "64e0cdc6-ce97-53cb-90b2-914f059429c1","TopicArn": "arn:aws:sns:us-east-1:123901341784:NewOFSObject","Subject": "Amazon S3 Notification","Message": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2023-03-14T01:07:14.304Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAJSLYAQIR3HHVYDOK2\"},\"requestParameters\":{\"sourceIPAddress\":\"34.195.147.78\"},\"responseElements\":{\"x-amz-request-id\":\"NS093145WJSB3M5P\",\"x-amz-id-2\":\"TBLTM5eN3RpYYyVJS0d1fGVwhpsjED4Tw7eZMf/D3sYJJNVpWg3TRw7X/pZ9S/mYIE8NWOZy8hDFgjGGMNzYTE2YZ+3S88dfwN32jnoeVrA=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"NTY5YmEwY2QtYzE3Zi00NTQ2LTllZTQtNzE1ZTEwMmIxMGFl\",\"bucket\":{\"name\":\"noaa-ofs-pds\",\"ownerIdentity\":{\"principalId\":\"A2AJV00K47QOI1\"},\"arn\":\"arn:aws:s3:::noaa-ofs-pds\"},\"object\":{\"key\":\"tbofs.20230314/nos.tbofs.fields.n002.20230314.t00z.nc\",\"size\":20138396,\"eTag\":\"6b6b182fdbacb0cfc7d491c0f48c7d26\",\"sequencer\":\"00640FC8C21FD07CDB\"}}}]}","Timestamp": "2023-03-14T01:07:15.001Z","SignatureVersion": "1","Signature": "DsU2+uGwqEfGQ6EHSW/swtx6JqBplP5b0KXXSyPX4ewZMS29GaCtCGrowIsyW9nWAV3bI8tnD44/32wOU5Xvh1Xm2P1aocDuHkZAsSOi859OyaJcdyBIKT8txJbSo+X+ql6t/bO/pI/A0Cnjwn9pZDHul648UnQamL0jAyqWN+MWhGd8efO4dbrloq5Zi4wPI3KRWgE0L2aYg6IPXAHxm1S0u+996iL3MpxVrlGvLBdVFZXg14G53z83xAhlT2XmNzpcnHy9Pkl2a9LpXusesFNgyrsFmzq0cBXYAliSj0ALHxVSOpOK2sp4KL1UyKjy0HBwhUKv4pOdDTA02VQxmg==","SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem","UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123901341784:NewOFSObject:d8f7dac5-0db1-4541-a0fb-4e13b43299f1"}' nextgen-dmac nos
```

Generate and upload a MultiZarr Aggregation using kerchunk for the model run of a given single kerchunked zarr file:

```bash
python kerchunk_nos_roms.py update_model_run '{"Type":"Notification","MessageId":"d24cf2d9-e674-5061-a67e-ae4fb7e2e2b5","TopicArn":"arn:aws:sns:us-east-1:579273261343:NOSZarrUpdated","Subject":"Amazon S3 Notification","Message":"{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2023-03-23T13:08:37.449Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAYNX3AQEPTDURMZLL6\"},\"requestParameters\":{\"sourceIPAddress\":\"172.31.93.205\"},\"responseElements\":{\"x-amz-request-id\":\"4RJP5678MYJ04MBR\",\"x-amz-id-2\":\"H3nlK66Ihxco0JZ5SvWNdvQccoyQICJ64FxTfVSeVoKJJMeoQLUPNvJJ10Xp7q4YiFQPppiK2kR2Fmrf+KOFw/rjMpz3uoX7\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"nos-zarr-updated\",\"bucket\":{\"name\":\"nextgen-dmac\",\"ownerIdentity\":{\"principalId\":\"A3E5URXP2R44AP\"},\"arn\":\"arn:aws:s3:::nextgen-dmac\"},\"object\":{\"key\":\"nos/nos.dbofs.fields.f030.20230323.t12z.nc.zarr\",\"size\":42421,\"eTag\":\"38001514e00b8db822ff21c6545e5320\",\"sequencer\":\"00641C4F555597F27E\"}}}]}","Timestamp":"2023-03-23T13:08:38.797Z","SignatureVersion":"1","Signature":"YZWxIZjBDyzRQYN8JMGfCqUdP4sYaDFEyiIF/njLExYxKiGft6frBfYAo7Oa7L+R38Ly7tAAupavH9L4DfvSi2wvvSQvmifExaae/9hU6FR5WO87p6hkgtN7pZO4fy8umKq1jvMtnqUl+b3+bXVJHBghfT5Ca1wNsDo0juhbo6vPkaasFnHQuJj16ReMdKx+v699FjuOmIDJlHWVOI3JLJhP4xYpu4mh9TWt8U6L0ayJ8f4WM7rzmIIvIB59471GCNyuk1kMZUmH4tiO7AA/QS3GvA3pdDXh5zOkFhxSrP0V3rWSAbeaz0hukXACrx88hdljRuE2YNhtOVFMsOaBhg==","SigningCertURL":"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem","UnsubscribeURL":"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe\u0026SubscriptionArn=arn:aws:sns:us-east-1:579273261343:NOSZarrUpdated:7a5b63a9-dea2-462e-91bb-993e8a3ccd4e"}'
```