# Next-Gen DMAC
### Next-Generation Data Management and Cyberinfrastructure
#### A collaboration between [RPS Group Ocean Science](https://www.rpsgroup.com/services/oceans-and-coastal/) and the NOAA Integrated Ocean Observing System ([IOOS](https://ioos.noaa.gov/))
This public repository will describe the prototyping efforts and direction of the Next-Gen DMAC project, "Reaching for the Cloud: Architecting a Cloud-Native Service-Based Ecosystem for DMAC." The goal of this project is to identify the technological and process shifts needed to develop a cloud-native architecture that will serve the current and future needs of the IOOS community. We will be testing a variety of technologies to identify more efficient cloud processing, storage, and data collection options while experimenting with cloud-native architectural patterns to bring it all together.

For a great resource explaining the background information for the Next-Gen DMAC project, check out [This Slideshow.](https://github.com/asascience-open/nextgen-dmac/blob/main/docs/DMAC%20NextGen%20Background%20Info.pdf)

This repository is intended to be a collaborative working area for open discussion about cloud-based services for ocean science. Please feel welcome to start a [Discussion](https://github.com/asascience-open/nextgen-dmac/discussions), contribute your ideas, and even contribute to our prototyping efforts!

If you are new to contributing to GitHub, check out these links:
 - [Creating a Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)
 - [Getting started with GitHub (video)](https://www.youtube.com/watch?v=gvvvdearAPE)

### View the full documentation at our [GitHub Pages site.](https://asascience-open.github.io/nextgen-dmac/)

## Related Projects

We are addressing a wide variety of topics and collaborating with others to solve related problems. 

**[See a list of those projects here.](https://asascience-open.github.io/nextgen-dmac/related_projects.html)**

## Steering Committee Meeting Minutes

The Steering Committee meets every quarter to discuss project updates and vet ideas for future development. 

**[The meeting minutes are here.](https://asascience-open.github.io/nextgen-dmac/meetings.html)**

## Technical Documentation

Completed prototypes have been documented alongside their code.

- [Kerchunk Ingest for NOS model data](https://github.com/asascience-open/nextgen-dmac/tree/main/kerchunk)
- [Argo Workflows Test of QARTOD and IOOS Compliance Checker](https://github.com/asascience-open/nextgen-dmac/tree/main/qc_and_cchecker)

## Prototype Plan

The planned prototypes seek to demonstrate a modern, interconnected system. Visit the links below to learn more about each prototype design and considerations.

**[Overall System Architecture](https://asascience-open.github.io/nextgen-dmac/architecture/architecture.html)**
- [Kubernetes](https://asascience-open.github.io/nextgen-dmac/architecture/kubernetes.html)
- [Nebari (Data Science Platform)](https://asascience-open.github.io/nextgen-dmac/architecture/nebari.html)

**[Data Ingest](https://asascience-open.github.io/nextgen-dmac/ingest/ingest.html)**

1. [Workflow Management](https://asascience-open.github.io/nextgen-dmac/ingest/workflows.html)
2. [Event Messaging](https://asascience-open.github.io/nextgen-dmac/ingest/events.html)

**[Data Storage and Discovery](https://asascience-open.github.io/nextgen-dmac/metadata/storage-and-discovery.html)**

3.  [Scientific data store](https://asascience-open.github.io/nextgen-dmac/metadata/data-formats.html)
4.  [Metadata Catalog](https://asascience-open.github.io/nextgen-dmac/metadata/catalog.html)
5.  [Catalog Queries](https://asascience-open.github.io/nextgen-dmac/metadata/queries.html)

**Data Processing**

6. Real-Time Analytics
7. Dask Processing

**[Data Analysis and Presentation](https://asascience-open.github.io/nextgen-dmac/analysis/analysis.html)**

8. Jupyter Notebooks
9. [Restful Grids: App Data Access](https://asascience-open.github.io/nextgen-dmac/analysis/data-access.html)
10. Client-side Rendering

![Prototype diagram](/docs/assets/prototype-diagram.png)


## Prototype Relationships

The prototype system we are initially exploring is a combination of several open-source components that we are configuring to run in AWS. This diagram illustrates the planned relationships between the components as well as the expected interactions by various user groups.

![Prototype diagram](/docs/assets/prototype-relationships.png)