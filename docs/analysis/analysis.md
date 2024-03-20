---
layout: default
nav_order: 4
has_children: true
title: Data Analysis
---

# Data Analysis

Data analysis capabilities refer to using raw data to generate insights which could be in the form of scientific studies, web applications, and other products where the data has been processed to portray specific information. We approached this topic with two major use-cases in mind.

The first use-case is for a scientist or data analyst who wants fast access to the data lake to perform studies using existing data. We provided access to Jupyter notebooks on the [Nebari](../architecture/nebari.md) platform to the [NOAA Coastal Ocean Reanalysis](cora.md) datasets.

The second use-case focused on typical day-to-day operations for other types of data access, typically web apps and downstream users pulling data. We [prototyped web services](data-access.md) to perform this data delivery to a test web application.