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

## Prototype Plan

The planned prototypes seek to demonstrate a modern, interconnected system. Visit the links below to learn more about each prototype design and considerations.

**[Data Ingest](ingest/ingest.md)**

1. [Job Orchestration](ingest/orchestration.md)
2. [Distributed Data Ingest](ingest/distributed.md)
3. [Event Messaging](ingest/events.md)

**Data Storage and Discovery**

<ol start="4">
<li>Zarr data store</li>
<li>STAC Catalog</li>
<li>Catalog Queries</li>
</ol>

**Data Processing**

<ol start="7">
<li>Real-Time Analytics</li>
<li>Dask Processing</li>
</ol>

**Data Analysis and Presentation**

<ol start="9">
<li>Jupyter Notebooks</li>
<li>Restful Grids: App Data Access</li>
<li>Client-side Rendering</li>
</ol>

![Prototype diagram](/assets/prototype-diagram.png)