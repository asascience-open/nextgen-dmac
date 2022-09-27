#!/bin/bash

# to install dask on Kuberenetes: (https://helm.dask.org/)
helm repo add dask https://helm.dask.org
helm repo update
helm install --version 2022.8.2 myrelease dask/dask

# to update to use config.yaml settings:
helm upgrade --cleanup-on-fail dask dask/dask --version 2022.8.2 --values config.yaml