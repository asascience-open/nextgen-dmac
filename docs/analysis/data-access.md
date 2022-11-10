---
layout: default
parent: Data Analysis
nav_order: 2
---

# Application Data Access

Open access to data implies that a user can both physically access the data and understand and use that data for further research. One challenge in the geoscience community has been making that data accessible in a true on-demand environment due to the cost and complexity of processing that data.

Modern data access patterns tend to provide an Application Programming Interface (API) that serves both internal and external applications. Rather than expose raw data files or tables to consumers, these APIs provide views of the data. The current de-facto APIs serving NOAA data are THREDDS and ERDDAP. Unfortunately the current implementations serving this data are were not engineered with cloud-native design in mind and struggle to scale to the needs of modern data services.