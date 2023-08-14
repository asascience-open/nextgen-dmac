# Aggregator

This module contains stream operators that apply [Kerchunk](https://github.com/fsspec/kerchunk)
extraction and aggregation to NOAA High Resolution Rapid Refresh forecasts. 

The current implementation aggregates only the WRFSFCF surface product. There are several 
[additional products](https://www.nco.ncep.noaa.gov/pmb/products/hrrr/) available. The
implementation is designed to be extensible, processing additional "models" based on regex
matching. Adding the WRFSUBHF, the sub hourly (15 min) surface output is a top priority for
Camus, but we have struggled with getting scan_grib to work properly with the grib messages.

The HRRR model is run once every hour. Each model run produces overlapping forward forecasts.
For the WRFSFCF product, an 18 hour forward forecast is created every hour. Every 6 hours, the
model produces a 48 hour forward forecast. A total of 576 hourly outputs every 24 hours.
Visualizing these time steps would look like this
![Forecast Model Run Collection](https://docs.unidata.ucar.edu/netcdf-java/current/userguide/images/netcdf-java/tutorial/feature_types/fmrc.png).

This aggregator is designed to produce collections as described by the FMRC. Currently, it produces
both diagonal aggregations of a fixed forward horizon (multiyear time series created for each
forward hour 1, 2... 18 and upto 48 hours in 6 hour blocks) and verticals from a single forecast run.
Adding horizontal aggregations for a fixed valid time would be a straight forward extension
(allowing comparison of 1, 2... 18... 48 hour forecasts for the same valid_time to assess model skill).

## Run local validation test

Create a python 3.10 virtual environment on a AMD64 linux machine.
TODO: Fix eccodes/cfgrib on ARM64 architecture

From the root directory of the `nextgen-dmac` project, install the dependencies. Currently the project is 
pinned to an fsspec git sha that enables caching.
```console
pip3 install -r aggregator/requirements.txt
```

```console
~/nextgen-dmac$ python aggregator/operators.py -h
usage:
        Demo application to experiment with the HRRR stream operators using the local file system.
         [-h] [--batch_model {wrfsfcf}] [--cprofiler | --no-cprofiler]
         {forecast_run,daily_horizon,monthly_horizon,alltime_horizon,raw_zarr,alert}
         batch_start batch_end

positional arguments:
  {forecast_run,daily_horizon,monthly_horizon,alltime_horizon,raw_zarr,alert}
  batch_start           start date for processing
  batch_end             end date for processing

options:
  -h, --help            show this help message and exit
  --batch_model {wrfsfcf}
                        Select the wrf hrrr model output to submit for processing
  --cprofiler, --no-cprofiler
                        Flag to run with cprofiler (default: False)
```

Check that the unit tests work
```console
$ python3 -m unittest aggregator/tests/test_operators.py
....................ss..
----------------------------------------------------------------------
Ran 24 tests in 0.022s

OK (skipped=2)
```

Extract the variables and offsets from the raw files. This takes a couple hours one a 8cpu machine for just two days of data.
```console
python aggregator/operators.py raw_zarr 2022-10-31 2022-11-01
```

Once the json has been extracted, the aggregations are very fast. Daily, Monthly and Alltime do need to be run in order.
```console
python aggregator/operators.py forecast_run 2022-10-31 2022-11-01
python aggregator/operators.py daily_horizon 2022-10-31 2022-11-01
python aggregator/operators.py monthly_horizon 2022-10-31 2022-11-01
python aggregator/operators.py alltime_horizon 2022-10-31 2022-11-01
```

You can find the output in `/tmp/aggregator/gcp-public-data-weather/high-resolution-rapid-refresh/version_2/`

To read one of the alltime aggregations
```python
import xarray as xr
import fsspec
from kerchunk.grib2 import scan_grib # Required to load the codec for grib2

rpath = "/tmp/aggregator/gcp-public-data-weather/high-resolution-rapid-refresh/version_2/alltime_horizon/conus/hrrr.wrfsfcf.19-24_hour_horizon.zarr"
r_opts = {'anon':True}
t_opts = {}
fs = fsspec.filesystem(
    protocol="reference", 
    fo= rpath,  
    remote_protocol='gcs', 
    remote_options=r_opts,
)

kv_store = fs.get_mapper("")
ds = xr.open_dataset(kv_store, engine="zarr", backend_kwargs=dict(consolidated=False), chunks={'valid_time':1}, drop_variables=["heightAboveGround"])
ds
```

To read it with a simple cache that will store the remote grib data locally. This is experimental!
```python
import xarray as xr
import fsspec
from kerchunk.grib2 import scan_grib # Required to load the codec for grib2

rpath = "/tmp/aggregator/gcp-public-data-weather/high-resolution-rapid-refresh/version_2/alltime_horizon/conus/hrrr.wrfsfcf.19-24_hour_horizon.zarr"
r_opts = {'anon':True}
t_opts = {}
fs = fsspec.filesystem(
    protocol="reference", 
    fo= rpath,  
    remote_protocol='gcs', 
    remote_options=r_opts,
)

fs_cached = fsspec.filesystem("simplecache", cache_storage='/tmp/aggregator/cache', fs=fs)

kv_store = fs_cached.get_mapper("")
ds = xr.open_dataset(kv_store, engine="zarr", backend_kwargs=dict(consolidated=False), chunks={'valid_time':1}, drop_variables=["heightAboveGround"])
ds
```


## Major components
The operators module has two major methods and a set of stream operators designed for use 
with either GCP pubsub or AWS Kinesis (not fully implemented). Both
[GCP](https://cloud.google.com/storage/docs/pubsub-notifications) and 
[AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html) provide 
notifications for cloud storage events which can be used to drive event driven processing. 
The tools use fsspec to allow working with AWS S3, Google Cloud Storage (GCP) or the local 
filesystem for testing and development. The methods described below, handle the compute and 
IO. The stream operators handle the business logic of building the aggregations for a given 
storage event.

The runner process is mostly boiler plate based on the particular cloud vendors pubsub system.
It has not been provided here yet, but could be added. To prove the utility of the operators
a main method is provided for the operator module that demonstrates execution and the resulting
output on the local file system.

### Kerchunk Methods
The extract_grib method handles the IO and computation, calling kerchunk scan_grib, to extract
the variables and data offsets from a grib file and write zarr metadata to a new blob/file

The multizarr method handles the IO and computation, calling kerchunk MultiZarrToZarr.translate
to combine (aggregate) multiple HRRR files along the "valid_time" dimension.

### The Operators
The stream operators are intended to run in a multithreaded environment typically used for 
streaming even processing in python with Kinesis or Google Pubsub. The stream operators 
use a multi processing tool (Dask) to do the IO and compute work. This significantly improves
throughput, though average load is still observed to be less than 70% during backfill operations.
Using threads and processes at the same time requires the process pool to be created by the main
thread. The worker threads can submit tasks to the process pool though. This has the advantage
of isolating the business logic in the main process, while the compute and file IO happens in
the worker processes. This is especially helpful for the grib2 eccodes library which apears
to leak memory.

The long forecast horizon aggregations (the diagonals of the FMRC diagram) are built in steps: from hours
to days; from days to months; and from months to alltime. The Kerchunk multizarr method has improved 
significantly during 2022, so tree or stepwise aggregation may no longer be
[necessary](https://github.com/fsspec/kerchunk/issues/200). It is reasonable efficient and limits the 
number of chunks that need to be aggregated in any single step. There is an unbounded alltime aggregation 
operation which seems to work well out to 18 months, though it is increasingly expensive as the output
size continues to grow. Future improvements, using Parquet files to store kerchunk zarr data may significantly
improve this.

The near real time behavior is intended to be re used as much as possible to backfill historical data.
Each of the aggregation operators act on every update for recent forecast runs, but only on the final
timestep of an aggregation for older historical data. This adds some complexity to the operators, but
makes the operational behavior simpler. The load on the aggregators created by running a backfill is
relatively small. Only the initial extraction step requires extra resources and operational support.

#### HrrrGrib2ZarrExtractor
This operator should receive events from the public read only bucket owned by NOAA. They should be filtered
to only FINALIZE operations. Unfortunately the key structure does not allow restricting to a particular
HRRR product. This operator does the heaviest work, reading the original grib2 file and creating a single
zarr metadata (json) file for each grib2 file. There are 576 wrfsfcf files created each day. The 
zarr metadata version should be persisted to a new output bucket which is also configured for notifications.
The path will be prefixed with raw_zarr for filtering by down stream consumers.

#### HrrrForecastRunAggregator
This operator should receive events from the output bucket. The notifications can be filtered to only 
FINALIZE operations for the raw_zarr output of the HrrrGrib2ZarrExtractor operator. This operator will
construct the Forecast Run aggregations, the verticals of the FMRC diagram above, collecting all the 
time steps for a single forecast into 18 (and 48) hour collections. It will build the collection as the
timesteps arrive on a best effort basis for recent model runs. Only the final timestep of a historical
backfill will trigger the aggregation. It should be resilient to missing timesteps. These products
will be written to the forecast_run key prefix. At present there is no further downstream consumer.

#### HrrrDailyHorizonAggregator
This operator should also receive events from the output bucket. The notifications can be filtered to 
only FINALIZE operations for the raw_zarr output of the HrrrGrib2ZarrExtractor operator. This operator
will construct daily aggregations for a given forecast horizon, the first step in building the long
diagonal time series in the FMRC diagram. The daily aggregations are built on a best effort basis
as each timestep arrives for recent data. For a historical backfill, only the last timestep of a given
day will trigger the operator to make a collection. The daily aggregations are written the the same
output bucket under the daily_horizon key path.

#### HrrrMonthlyHorizonAggregator
This operator should receive events from the output bucket. Notifications should be filtered to only
FINALIZE operations for the daily_horizon. The operator will combine the daily aggregates updating
the appropriate calendar month each time the daily data changes for recent days. For historical backfills
the mothly aggregator only creates new collections for the final day of the month. The monthly aggregations
are written to the same output bucket under the monthly_horizon key path.

#### HrrrAllTimeHorizonAggregator
This operator should receive events from the output bucket. otifications should be filtered to only
FINALIZE operations for the monthly_horizon.The operator will combine the monthly aggregates for any
updated month. A filter for recent months should be considered. Depending on the use case, it may be
preferable to aggregate the monthly or daily aggregates for a specific time range in the consumer
application. Creating and the alltime aggregates is expensive. Parsing the alltime aggregates in
the consumer application is also expensive, but reading multiple days or months isn't free either.
The alltime aggregations are written to the same output bucket under the alltime_horizon key path.

#### BackfillHrrrGrib2ZarrExtractor
The backfill operator extends the HrrrGrib2ZarrExtractor operator class, but should be deployed with
different operational resources (topic, subscription & worker pool). A separate topic is required
because the topic for the NOAA bucket notifications belongs to NOAA and backfill messages can not 
be published to it. A separate subscription is then required to checkpoint the process as it reads 
the topic messages. The operator can then emit psuedo notifications for historical files which will
trigger the backill process to extract the zarr metadata and write it to the ouput bucket. The down
stream aggregators then receive notification events and update the aggregates appropriately as described
above. The recency criteria described above limit the thrashing of the aggregation operations in most
cases, but backfilling the current or previous day can cause issues.

#### DeadLetterQueueAlertsStreamOperator
Is a default implementation for a dead letter consumer when configured with a maximum number of
retries for a subscription. At the very least, the failures should be logged, ideally alerted.

### Known issues

1) dask workers die during backfill operations and do not appear to restart
2) there are occasional hiccups in posting grib2 products to GCP. AWS may be more reliable. The NODD team
provides excellent support via email (nodd at noaa.gov).
3) Couldn't use the latest version of eccodes (1.5.0) due to issue with lib cffi. Pinned eccodes 1.4.2
4) Dask workers don't setup logging correctly
5) Should simplify this with a real datetime library (end of month is horrible in python datetime)

