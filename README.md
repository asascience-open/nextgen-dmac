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
- Jupyter notebooks:

[![badge](https://img.shields.io/badge/launch-binder-579ACA.svg?logo=data:image/png;base64,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)](https://mybinder.org/v2/gh/asascience-open/nextgen-dmac/fa3dcdf7530b47017e656e79aa1bba481d6e1e8c?urlpath=lab%2Ftree%2Fbinder%2Fdbofs-examples%2Fbest-forecast.ipynb)

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