# Data Workflows

This document attempts to define three core data workflows touched upon in the DMAC community and provide understanding about how they fit together to create a robust foundation for building data consumer interfaces for cloud native data.

## 1. Zarr Aggregations

The first core workflow for this project has been the creation of zarr aggregations. To understand this product, it’s important to first understand how zarr works. Zarr datastores are simply collections of json metadata files and binary/ascii data payload files. Together they describe data arrays and collections in a flexible way because all pieces of a given zarr dataset can be stored in any cloud storage system or even dynamically generated as needed. 
The aggregations that ROS is creating for NOS model data utilize NODD’s cloud storage to enable building zarr datasets where the metadata is stored in one cloud bucket, and the actual data arrays remain in NODDs cloud bucket. The product is a set of consolidated zarr metadata json files that describe the metadata, variables, where the exact chunks of data that make up the dataset are located, and finally how to access/decode them. The metadata is the same metadata that we can currently use with NetCDF datasets, but it is human readable and “just” a json file.

Most importantly these aggregations are easily loadable with xarray, fsspec, and zarr, allowing any data consumers to access the full aggregations lazily when they need them, directly in the tools that they are most familiar with. Furthermore, other users who want to use javascript or other languages that have zarr libraries to read the data will be able to do so.

The main limitation of the zarr collections is that they are most useful for gridded data products. There is not as much utility for columnar products or timeseries point data and other formats such as parquet are better fits for those data types.

## 2.	Intake

Intake is a set of tools to simplify loading data for data scientists with python. It has a catalog format that allows users to share their collections of data with others to simplify loading. If one desired, an intake catalog could be generated that lists all the raw files on NODD or other cloud storage buckets to simplify users to load the full datasets directly. However, from a data user perspective, this is less useful. Instead, Intake can be used to simplify loading data from the zarr aggregations for users who are more familiar with this approach. An intake catalog for this use case would list the zarr aggregations as data products that are available to load and is trivial to create.

A benefit of providing intake support is that intake is not limited to N dimension model hindcast and forecast data. Intake catalogs can contain any kind of data support, including columnar datasets like buoys and sensors or GIS products like geojson. For a larger DMAC system where there is a combination of model data and point observation data, intake catalogs are useful because all datatypes available can be listed out in one catalog for users to load as they wish.

## 3. STAC
STAC catalogs are simply geojson files that have been formatted to the STAC spec to describe distributed data assets. The idea is like intake’s: A collection of catalog files provide links to the locations of data assets. These assets can be any kind of data and are not limited to gridded data.  STAC catalogs exist at a higher dimension than Intake, meaning that while intake describes how to open a datasets (e.g. load this dataset with xarray and cfgrib automatically), STAC simply provides the location of data assets and the metadata for the assets.

To give a more concrete example, while an Intake catalog can be created the allows a user to directly load in the model zarr aggregations, a STAC catalog would simply tell a user where that data is and what it contains. It provides a useful building block to create data discovery interfaces, as anything that can read geojson files can read STAC files, including python libraries. This allows a user to query against STAC catalogs to find the data they would desire, and then use that information to open the data with the tools as they choose. It is up to the user to choose how to use the data they find with the STAC catalog.

Another feature of STAC is the ability to specify custom metadata fields. For example, one spec is being worked on to describe data cubes better: https://github.com/stac-extensions/datacube. This means that additional metadata as described in the GRIB/NetCDF headers could also be extracted, placed in the STAC json, and made searchable. Additional metadata that doesn’t fit in the “standard” specs could be added via a custom STAC extension with a custom schema.
