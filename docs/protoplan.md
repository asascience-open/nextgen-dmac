---
layout: default
title: Prototype Plan
nav_order: 2
---

# Prototype Plan

## Technical Documentation

Completed prototypes have been documented alongside their code.

- [Kerchunk Ingest for NOS model data](https://github.com/asascience-open/nextgen-dmac/tree/main/kerchunk)
- [Argo Workflows Test of QARTOD and IOOS Compliance Checker](https://github.com/asascience-open/nextgen-dmac/tree/main/qc_and_cchecker)

## Prototype Plan

The planned prototypes seek to demonstrate a modern, interconnected system. Visit the links below to learn more about each prototype design and considerations.

**[Overall System Architecture](architecture/architecture.md)**
- [Kubernetes](architecture/kubernetes.md)
- [Nebari (Data Science Platform)](architecture/nebari.md)

**[Data Ingest](ingest/ingest.md)**

1.  [Workflow Management](ingest/workflows.md)
2.  [Event Messaging](ingest/events.md)

**[Data Storage and Discovery](metadata/storage-and-discovery.md)**

3.  [Scientific data store](metadata/data-formats.md)
4.  [Metadata Catalog](metadata/catalog.md)
5.  Catalog Queries

**Data Processing**

6.  Real-Time Analytics
7.  Dask Processing

**[Data Analysis and Presentation](analysis/analysis.md)**

8.  Jupyter Notebooks
9. [Restful Grids: App Data Access](analysis/data-access.md)
10. Client-side Rendering


![Prototype diagram](/assets/prototype-diagram.png)

## Prototype Relationships

The prototype system we are initially exploring is a combination of several open-source components that we are configuring to run in AWS. This diagram illustrates the planned relationships between the components as well as the expected interactions by various user groups.

![Prototype diagram](/assets/prototype-relationships.png)