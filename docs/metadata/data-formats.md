# Scientific Data Storage

A lot of work has already been done to identify cloud-native formats for scientific data. Cloud-native data formats are optimized for scalability and analysis. 

## Zarr

[Zarr](https://zarr.readthedocs.io/en/stable/) has attracted a lot of attention for its ability to store gridded data in a cloud-native format. Its ability to access data in chunks provides great performance boosts over traditional formats like netCDF and GRIB.

## Kerchunk

[Kerchunk](https://github.com/fsspec/kerchunk) uses byte-range metadata to offer similar performance benefits of zarr, but allows data to be stored in its native format (netCDF or GRIB).