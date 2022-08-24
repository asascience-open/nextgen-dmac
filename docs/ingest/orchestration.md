---
layout: default
title: Orchestration
parent: Data Ingest
nav_order: 1
---

# Job Orchestration

Job orchestration gives visibility and control into the data ingest process. It is the core of data operations.

## Ingest

Data ingest takes place via the `intake` library, which has options to ingest
NetCDF, CSV, GRIB format, among others.  Currently, the NetCDF library is
supported.

## QC

QC is performed via the `ioos_qc` library.  The file contents are read in via
`intake`'s `.read()` method on a loaded netCDF datasource

## CF Compliance Checking

Climate and Forecast (CF) compliance checking is offered via the `compliance-checker`
library.
