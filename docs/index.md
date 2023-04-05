---
layout: default
title: Prototype Plan
nav_order: 1
---

# Next-Gen DMAC
### Next-Generation Data Management and Cyberinfrastructure
#### A collaboration between [RPS Group Ocean Science](https://www.rpsgroup.com/services/oceans-and-coastal/) and the NOAA Integrated Ocean Observing System ([IOOS](https://ioos.noaa.gov/))
This public repository will describe the prototyping efforts and direction of the Next-Gen DMAC project, "Reaching for the Cloud: Architecting a Cloud-Native Service-Based Ecosystem for DMAC." The goal of this project is to identify the technological and process shifts needed to develop a cloud-native architecture that will serve the current and future needs of the IOOS community. We will be testing a variety of technologies to identify more efficient cloud processing, storage, and data collection options while experimenting with cloud-native architectural patterns to bring it all together.

For a great resource explaining the background information for the Next-Gen DMAC project, check out [This Slideshow.](https://github.com/asascience-open/nextgen-dmac/blob/main/docs/DMAC%20NextGen%20Background%20Info.pdf)

This repository is intended to be a collaborative working area for open discussion about cloud-based services for ocean science. Please feel welcome to start a [Discussion](https://github.com/asascience-open/nextgen-dmac/discussions), contribute your ideas, and even contribute to our prototyping efforts!

## Related Projects

We are addressing a wide variety of topics and collaborating with others to solve related problems. **[See a list of those projects here.](related_projects.md)**

## Steering Committee Meeting Minutes

The Steering Committee meets every quarter to discuss project updates and vet ideas for future development. **[The meeting minutes are here.](meetings.md)**

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
5.  [Catalog Queries](metadata/queries.md)

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