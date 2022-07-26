# Data Ingest

Data Ingest starts the process of preparing data for transformation and notifying the rest of the system of its presence. There is little that an end-user needs to be aware of here, but from an engineering perspective this shapes how data will be available for later steps.

## Data Ingest Concepts

The data ingest is the beginning point of making the system aware of data. The DMAC must be capable of serving many types of data such as tabular time-series, gridded forecasts, specific observations, and more. In true data lake fashion, the data itself can be stored in its native format (netCDF, GRIB, CSV, etc.) but we will index that data and extract metadata to facilitate search functions and analysis for later processing. This eliminates the need for relying on filename conventions or disk read operations for finding data, and allows all data to be managed equivalently throughout the system. Although specific tools (e.g. netCDF libraries) are still needed to read the raw data, data mashups become simplified when referenced through common API endpoints.

After indexing, data can be referred to in the rest of the system through its unique identifier, here named `ObjectId`. By referring to objects in this way we remove dependence on the filesystem element (whether it's block or object storage) and can reference data regardless of its physical location. This especially becomes important in cloud development where the processing machines may be ephemeral instances without access to a shared volume mount. It also allows supports better sharing between cloud platforms and prevents vendor lock-in.

This stage of ingest does not intend to define all possible data transformations and dependencies. Rather, an event-based subscription service allows downstream data operations to kick off as new data becomes available. For example, a "best forecast" aggregation might listen for new model forecasts, and update its contents as new forecasts arrive. As another example, if a zarr-formatted dataset is preferred for an application, that conversion process can be defined as a listener to the source data.

## Flow Diagram

The diagram below illustrates how raw data might be ingested into the system to be available for consumption.

<embed src="https://asascience-open.github.io/nextgen-dmac/assets/data-ingest.pdf" type="application/pdf" width="100%" height="700px"/>
