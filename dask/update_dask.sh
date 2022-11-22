#!/bin/bash

helm upgrade dask-gateway dask-gateway --repo=https://helm.dask.org --install --namespace dask-gateway --values config.yaml