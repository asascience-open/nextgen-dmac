---
layout: default
title: Data Ingest
nav_order: 3
has_children: true
---

# Data Ingest

Data Ingest starts the process of preparing data for transformation and notifying the rest of the system of its presence. There is little that an end-user needs to be aware of here, but from an engineering perspective this shapes how data will be available for later steps.

**Key Points**
- The system needs a method of incorporating raw data in order to provide more value to that data.
- *Raw data* referred to here may be a traditional GRIB or NETCDF file, but could also be video or imagery.
- The raw data does not necessarily need to be copied to be ingested; if data is already cloud accessible it still needs to be ingested but not copied.
- Data needs to pass quality control checks. Bad data inputs corrupt good data analysis. 
    - Using bad data to train AI/ML models wastes effort because it will likely produce inaccurate results.
- The metadata needs to be extracted from the raw data files to feed the larger system.
- The system requires that the data is indexable by byte-range requests. Common scientific formats such as GRIB and NetCDF adhere to this requirement and are easily indexed using the [kerchunk process](ingest-prototype.md).
- This process needs to be constantly monitored. A dedicated data team should be responsible for data ingest in operations.

## Data Ingest Concepts

The data ingest is the beginning point of making the system aware of data. The DMAC must be capable of serving many types of data such as tabular time-series, gridded forecasts, observations, and more. In true data lake fashion, the data itself can be stored in its native format (netCDF, GRIB, CSV, etc.) but we will index that data and extract metadata to facilitate search functions and analysis for later processing. This eliminates the need for relying on filename conventions or disk read operations for finding data, and allows all data to be managed equivalently throughout the system. Although specific tools (e.g. netCDF libraries) are still needed to read the raw data, data mashups become simplified when referenced through common API endpoints.