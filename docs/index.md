---
layout: default
title: Prototype Plan
nav_order: 1
---

# Next-Gen DMAC
### Next-Generation Data Management and Cyberinfrastructure
#### A collaboration between [RPS Group Ocean Science](https://www.rpsgroup.com/services/oceans-and-coastal/) and the NOAA Integrated Ocean Observing System ([IOOS](https://ioos.noaa.gov/))
This public repository will describe the prototyping efforts and direction of the Next-Gen DMAC project, "Reaching for the Cloud: Architecting a Cloud-Native Service-Based Ecosystem for DMAC."

This repository is intended to be a collaborative working area for open discussion about cloud-based services for ocean science. Please feel welcome to start a [Discussion](https://github.com/asascience-open/nextgen-dmac/discussions), contribute your ideas, and even contribute to our prototyping efforts!

## Related Repositories

These are open-source repositories that demonstrate work toward cloud-native data access.

### [Restful Grids](https://github.com/asascience/restful-grids)

RESTful Grids is a project started at the 2021 IOOS Code Sprint with the idea of exploring data access through modern data APIs. This project focused on implementing the Environment Data Retrieval (EDR) API for gridded data, and also explored retrieving tiled dataset typically used by map displays. 



## Prototype Plan

The planned prototypes seek to demonstrate a modern, interconnected system. Visit the links below to learn more about each prototype design and considerations.

**[Overall System Architecture](architecture/architecture.md)**
- [Kubernetes](architecture/kubernetes.md)

**[Data Ingest](ingest/ingest.md)**

1.  [Job Orchestration](ingest/orchestration.md)
2.  [Distributed Data Ingest](ingest/distributed.md)
3.  [Event Messaging](ingest/events.md)

**[Data Storage and Discovery](metadata/storage-and-discovery.md)**

4.  [Scientific data store](metadata/data-formats.md)
5.  [Metadata Catalog](metadata/catalog.md)
6.  [Catalog Queries](metadata/queries.md)

**Data Processing**

7.  Real-Time Analytics
8.  Dask Processing

**Data Analysis and Presentation**

9.  Jupyter Notebooks
10. [Restful Grids: App Data Access](analysis/data-access.md)
11. Client-side Rendering


![Prototype diagram](/assets/prototype-diagram.png)

## Use-Cases