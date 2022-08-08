---
layout: default
title: Data Storage and Discovery
nav_order: 3
has_children: true
---

# Data Storage and Discovery

Data storage and discovery is most impactful to consumers of the raw data and also has a profound impact on the overall system architecture. Access to data needs to be fast and scalable, and the system must continue to perform as data volumes grow.

Historically the scientific community has packaged metadata with the data, for example in netCDF or GRIB. In a cloud-native system, the metadata is typically stored separately from the raw data. This alleviates the scalability problem of trying to read metadata from individual files and opens to more possibilities such as metadata augmentation.

With metadata augmentation, one can start adding value to the data in numerous ways:
- Track data usage
- Fill in application-specific metadata
- Add labels for machine learning