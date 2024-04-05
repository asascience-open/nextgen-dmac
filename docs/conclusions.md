---
layout: default
title: Conclusions
nav_order: 10
---

# Conclusions and Recommendations

*Last Updated March 2024*

These are the recommendations based on the research performed during the period of performance for the project, *Reaching for the Cloud: Architecting a Cloud-Native Service-Based Ecosystem for DMAC.*

# Opportunities

## Supporting and Educating the IOOS Community

### System Integrations

## Growing an Inclusive Open-Source Community

This project enabled software engineers from many different organizations to collaborate toward a common goal. It is not uncommon for developers in the NOAA community to be independently building very similar software solutions. In the end we are all working toward better serving NOAA data to our unique clients. 

We should practice good community management to continue these open-source efforts and collaborations. Active community management is more than just setting up meetings. To name a few tasks: it's monitoring issues, improving documentation, addressing bugs, and establishing practices for contributors. Without active maintainers the community will wither.

## Serving Cloud-Native Data

This project has demonstrated several effective workflows for optimizing and serving IOOS data. We have the opportunity now to harden and further those demonstrations from prototype to production. The prototypes have gone far and have been effective demonstrations, but more can be done to improve their reliability and flexibility. 

- Monitoring will help to resolve issues when they arise, and provide more visibility into the workflows and the state of individual data products. 
- Exploring additional methods for time-series aggregations is another important topic for accessing forecasts and ensemble data faster.
- Optimizing the services will improve performance, reduce cloud costs, and enable us to use the cloud to scale.
- A metadata model, such as [STAC](../metadata/catalog.md), will enable data to be discovered more easily.

## Managing Data Quality

Guidelines for data standards will become increasingly important as IOOS develops their data lake. Conversations about metadata, CF standards compliance, and other data standards have been occuring in the context of this project. Moving forward, concurrence on standards and policies to enforce them will be important to ensure that the data continues to be of high quality. The antithesis of a data lake is a data swamp: disorganized, inconsistent from one product to another, and difficult to use. A quality data lake will accelerate decision making and downstream product development by providing dependable data that can be accessed in a cohesive manner.

## Viewing One IOOS

One of the challenges of the current DMAC is that the data is spread among many different portals, making it difficult to obtain a single view of all IOOS data. As the IOOS community converges toward a more open data lake it will become much simpler to link various datasets using consistent overarching services to obtain that data.

## AI-Ready Data

While we are focused on transforming IOOS data, AI is an important use-case that should not be ignored. Optimizing this data for reading from the cloud is the first step in ensuring that future projects consuming this data for AI applications will not need to "reinvent the wheel" for every unique dataset. 

## Preparing for Rapid Evolution