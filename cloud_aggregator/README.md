# Cloud Aggregator

Cloud native ingest and aggregation of gridded environmental data

## Prerequisites

* Set current AWS profile as ioos cloud

```bash
export AWS_PROFILE=ioos
```

* Install `pulumi`

```bash
brew install pulumi/tap/pulumi
```

* Set pulumi secrets passphase environment variable

```bash
export PULUMI_SECRETS_PROVIDER=<passphrase>
```

* Login to self managed pulumi state

```bash
pulumi login s3://cloud-aggregator-pulumi
```

At this point, the project was created with `pulumi new aws-python --force` but that is only done once. 

If the virtual environment is not available in the `venv` folder, create it with:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

## Deploy

We can deploy the stack with

```bash
pulumi up
```

## Cleanup

We can destroy the entire stack with 

```bash
pulumi destroy
```

This will not destroy the state, just the cloud resources. To get rif of the state we can use

```bash
pulumi stack rm
```
