---
layout: default
title: Workflow Management
parent: Data Ingest
nav_order: 2
---

# Workflow Management

Workflow management is a modern pattern for managing a process that runs as a series of steps. In a data ingest pipeline, workflow management is critical for ensuring clean, valid data is coming into the system. There are numerous workflow platforms to choose from. Workflows are typically executed as a directed acyclic graph (DAG) which is a tree structure laying out each task. The software then spreads the execution of those steps across many processes to efficiently scale to meet the workload.

We are currently experimenting with [Argo Workflows](https://argoproj.github.io/argo-workflows/) to run our pipelines. The key considerations for choosing Argo are:

- Open Source
- Promising long-term support by Cloud Native Computing Foundation (CNCF)
- Runs natively on Kubernetes
- Supports both DAG and Cron workloads
- Template features for reuseability

## Workflows for Forecast Model Data

The primary tasks of the data ingest pipeline for model data are:

- Quality Control
- Compliance Checking
- Indexing / Transformation (i.e. generating zarr files)
- Metadata Indexing