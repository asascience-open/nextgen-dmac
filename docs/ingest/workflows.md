---
layout: default
title: Workflows Overview
parent: Data Ingest
nav_order: 1
---

# Workflow Management

Workflow management is a modern pattern for managing a process that runs as a series of steps. In a data ingest pipeline, workflow management is critical for ensuring clean, valid data is coming into the system. There are numerous workflow platforms to choose from. Workflows are typically executed as a directed acyclic graph (DAG) which is a tree structure laying out each task. The software then spreads the execution of those steps across many processes to efficiently scale to meet the workload.

## Workflows for Forecast Model Data

The primary tasks of the data ingest pipeline for model data are:

- Quality Control
- Compliance Checking
- Indexing / Transformation (i.e. generating zarr files)
- Metadata Indexing

## Argo Workflows

We chose to prototype with [Argo Workflows](https://argoproj.github.io/argo-workflows/) to run our pipelines. The key considerations for choosing Argo are:

- Open Source
- Promising long-term support by Cloud Native Computing Foundation (CNCF)
- Runs natively on Kubernetes
- Supports both DAG and Cron workloads
- Template features for reuseability

## Lambda Workflows

We chose to prototype on a serverless architecture on [AWS Lambda](https://aws.amazon.com/lambda/faqs/) to compare and contrast with the costs and maintenance of operating Argo Workflows. The main benefits of using Lambda are:

- No infrastructure to manage (i.e. no K8s cluster or engine)
- Easy to scale
- Highly optimized and cost-effective
- No idle compute costs

## Comparison

Both workflow systems we tested were effective in managing jobs to kerchunk the data. The main cost difference really becomes where the organization chooses to invest time in both development and maintenance. Argo is a mature project with a large community of users and contributors and the features have been tested heavily. Most features one would want in a workflow system have been built by Argo, but there is a learning curve to operate Argo effectively. Couple that with the fact that Argo runs on Kubernetes, so that requires additional infrastructure management. There are many other workflow management tools available that also have similar features, but generally they will all have a learning curve and require managed infrastructure to run on.

Lambda, on the other hand, is a serverless execution platform and not a workflow management system. This means that any orchestration, reporting, monitoring, or other features are not built in and need to be written and maintained in a custom codebase. The maintenance needs of the infrastructure are reduced because AWS handles the provisioning and maintenance of the compute resources, but the development costs may be higher to maintain custom code. 

Going forward, we have chosen to continue running the Lambda workflow to optimize data because:

- Cloud costs* have been lower than constantly running Kubernetes. This is because the Lambdas are not running when not needed.
- Time required to manage infrastructure is lower. Our team does not have dedicated developers managing infrastructure, and K8s/Argo management is complicated.
- The workflow is not that complex (~2 steps). If it were more complicated (i.e. with branching conditions) then it may be more advantageous to use Argo.
- Better portability. We used Pulumi IaC to help ensure that the same workflows can be deployed to other Amazon VPCs.
- Python code to execute Kerchunk jobs had to be written anyway. The code is more straightforward and portable than if it was specifically tied to a particular workflow engine.

The downside of this approach is that more of the features need to be developed such as observability and failure recovery.

> *Cloud costs can be higher on Lambdas without proper tuning and flow control.