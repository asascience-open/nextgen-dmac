"""
MIT License

Copyright (c) 2022 Camus Energy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
import os
import re
import types
from abc import abstractmethod, ABC
from pathlib import PurePosixPath
from typing import Dict, Union
import json
import ujson
import fsspec
import gcsfs
import datetime
import xarray as xr

from dask.distributed import Client

from kerchunk.grib2 import scan_grib
from kerchunk.combine import MultiZarrToZarr

from google.cloud import pubsub_v1


logger = logging.getLogger(__name__)

# Arguments for scan_grib to extract surface variables
SCAN_SURFACE_INSTANT_GRIB = dict(
    filter=dict(typeOfLevel="surface", stepType="instant"),
    storage_options=dict(token=None),
)

# Arguments for scan_grib to extract height above ground variables
SCAN_HEIGHT_ABOVE_GROUND_INSTANT_GRIB = dict(
    filter=dict(typeOfLevel="heightAboveGround", stepType="instant"),
    storage_options=dict(token=None),
)

consts = types.SimpleNamespace()
consts.EXTRACTED_BUCKET = "gcp-public-data-weather"
consts.SEMANTIC_VERSION = "version_2"
consts.RAW_ZARR = "raw_zarr"
consts.FORECAST_RUN = "forecast_run"
consts.DAILY_HORIZON = "daily_horizon"
consts.MONTHLY_HORIZON = "monthly_horizon"
consts.ALLTIME_HORIZON = "alltime_horizon"
# Cloud archive goes back to 2014. Are there different variables or dimensions?
# https://rapidrefresh.noaa.gov/hrrr/
consts.ALL_TIME_START_DATE = "2020-06-01"


def extract_grib(
    input_fs: fsspec.spec.AbstractFileSystem,
    input_base_path: PurePosixPath,
    input_object_path: PurePosixPath,
    output_fs: fsspec.spec.AbstractFileSystem,
    output_base_path: PurePosixPath,
) -> str:
    """
    This method extracts data from the original grib2 file using the kerchunk scan_grib method.
    It puts the resulting zarr metadata in a blob in our own bucket using a similar path.

    :param input_fs:
    :param input_base_path: the bucket with the object to process
    :param input_object_path: the path to the object
    :param output_fs:
    :param output_base_path:
    """

    input_path = input_base_path / input_object_path

    if not input_fs.exists(input_path):
        # Raise a nice clear error that is easy to validate
        raise RuntimeError(
            f"HRRR GRIB Blob missing: {input_path}",
        )

    input_url = input_fs.open(input_path).full_name

    # The scan method produces a list of entries
    zarr_meta_surface = scan_grib(input_url, **SCAN_SURFACE_INSTANT_GRIB)

    zarr_meta_height_above_ground = scan_grib(
        input_url, **SCAN_HEIGHT_ABOVE_GROUND_INSTANT_GRIB
    )

    # Some filesystems have multiple string protocol names
    protocol = input_fs.protocol
    if isinstance(protocol, (list, tuple)):
        protocol = protocol[0]

    # The Multizarr To Zarr translate method produces a readable file from the aggregated metadata
    combined_zarr_meta = MultiZarrToZarr(
        zarr_meta_surface + zarr_meta_height_above_ground,
        remote_protocol=protocol,
        remote_options={},
        concat_dims=["valid_time"],
        identical_dims=["latitude", "longitude", "step"],
    ).translate()

    # Check for valid data - sometimes the output is truncated so make sure we don't aggregate broken data
    r_opts = {"anon": True}
    fs = fsspec.filesystem(
        protocol="reference",
        fo=combined_zarr_meta,
        remote_protocol=protocol,
        remote_options={},
    )
    ds = xr.open_dataset(
        fs.get_mapper(""),
        engine="zarr",
        backend_kwargs=dict(consolidated=False),
        chunks={"valid_time": 1},
        drop_variables=["heightAboveGround"],  # Why is does this break zarr?
    )
    temp_stats = ds.t.to_dataframe().describe()
    # logger.info(temp_stats)
    # Assert the count is the size of the gridded domain
    assert (
        temp_stats.loc["count", "t"] == 1059 * 1799
    ), "HRRR Temperature values are nan!"
    assert temp_stats.loc["mean", "t"] > 250.0, "HRRR Temperature values are too low!"

    # Parse the input path to get the output name
    # Example: gcs://high-resolution-rapid-refresh/hrrr.20221028/conus/hrrr.t00z.wrfsubhf01.grib2"
    _, _, model, prefixed_date, region, output_name = input_url.split("/")
    output_blob_path = os.path.join(
        output_base_path,
        model,
        consts.SEMANTIC_VERSION,
        consts.RAW_ZARR,
        region,
        prefixed_date,
        output_name.replace(".grib2", ".zarr"),
    )
    with output_fs.open(output_blob_path, "w") as f:
        ujson.dump(combined_zarr_meta, f, ensure_ascii=True)
    return output_blob_path


def filter_on_presence(paths: [str], fs: fsspec.spec.AbstractFileSystem) -> [str]:
    """
    Filter the input list of paths based on their presence in the file system
    :param paths: the
    :param fs:
    :return:
    """
    fs = fs or gcsfs.GCSFileSystem(token=None)

    paths = sorted(paths)

    missing = []
    absence = False
    gap = False
    for path in paths:
        if not fs.exists(path):
            missing.append(path)
            absence = True
        else:
            if absence:
                # If a blob is present after another is absent that is a more serious condition
                gap = True

    if absence:
        if gap:
            # These log messages are not visible when run in a dask worker process
            logger.info(
                "Gap in aggregation input with %s of %s blobs missing:%s %s",
                len(missing),
                len(paths),
                "" if len(missing) < 4 else "(truncated to 4!)",
                missing[:4],
            )
        else:
            logger.debug(
                "Last %s of %s blobs in aggregation input are missing: %s",
                len(missing),
                len(paths),
                missing,
            )

        # Skip these blobs in the aggregation!
        paths = sorted(set(paths) - set(missing))

    return paths


def multizarr(fs: fsspec.spec.AbstractFileSystem, blobs: [str], out_path: str) -> str:
    """
    Given a set of input blob paths for zarr data, create an aggregation and store it in the specified output path
    This method is naive and should probably stay that way. Don't do fancy parallelization here.
    Either kerchunk should do it or it should live at the pubsub task level, not here.
    :param fs: filesystem to read and write to
    :param blobs: a list of zarr metadata blobs to aggregate
    :param out_path: the output key path for the aggregated zarr data
    :return:
    """

    # MultiZarrToZarr will fail on missing blobs,
    # the error message is obtuse and hard to understand because the path is url encoded.
    # Better to explicitly check for the files that are present and ignore missing
    filtered_blobs = filter_on_presence(blobs, fs=fs)

    if len(filtered_blobs) == 0:
        raise RuntimeError("None of the aggregation blobs are present!")

    # Some filesystems have multiple string protocol names
    protocol = fs.protocol
    if isinstance(protocol, (list, tuple)):
        protocol = protocol[0]

    mzz = MultiZarrToZarr(
        [f"{protocol}://{blob}" for blob in filtered_blobs],
        remote_protocol=protocol,
        remote_options={},
        concat_dims=["valid_time"],
        identical_dims=["latitude", "longitude", "step"],
    )
    combined_zarr_meta = mzz.translate()

    with fs.open(out_path, "w") as f:
        ujson.dump(combined_zarr_meta, f, ensure_ascii=True)
    return out_path


def last_forecast_of_day_or_recent(
    forecast_date: datetime.date,
    forecast_hour: int,
    forecast_horizon: int,
    today: datetime.date = None,
) -> bool:
    """
    Given a forecast date, hour and horizon, determine whether it is recent or the last forecast of the day.
    :param forecast_date: the date of the forecast run
    :param forecast_hour: the hour of the forecast run
    :param forecast_horizon: the forward horizon of the forecast run
    :param today: optional injection of current date for testing
    :return: boolean
    """
    yesterday = (today or datetime.date.today()) - datetime.timedelta(
        days=1
    )  # Yesterday, UTC
    return (
        (forecast_date >= yesterday)
        or (forecast_hour == 23 and forecast_horizon <= 18)
        or (forecast_hour == 18 and forecast_horizon > 18)
    )


def last_forecast_of_month_or_this_month(
    forecast_date: datetime.date,
    today: datetime.date = None,
) -> bool:
    """
    Boolean method to determine whether we should try to aggregate the month.
    Assumptions:
    1) If it is the current month, run the aggregation
    2) If it is the last forecast for a given month, run the aggregation

    :param forecast_date: the date of the forecast run
    :param today: optional injection of current date for testing
    :return: boolean
    """
    # Today, UTC
    today = today or datetime.date.today()
    return (forecast_date.replace(day=1) == today.replace(day=1)) or (
        (forecast_date + datetime.timedelta(days=1)).replace(day=1)
        != forecast_date.replace(day=1)
    )


class TestStructures:
    # Must use outer namespace to match by type https://stackoverflow.com/q/71441761
    class FakeMessage:
        def __init__(
            self,
            attributes: Dict[str, str],
            data: str = "some data",
            protocol: str = "file",
            message_id="foobar",
        ):
            self.attributes = attributes
            self.data = data
            self.protocol = protocol
            self.message_id = message_id


class StreamOperator(ABC):
    """
    Base class for all Stream Operators
    """

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        Method called by the pubsub letter handler to process a message
        :param message:  the Pubsub Message https://cloud.google.com/python/docs/reference/pubsub/latest
        #TODO add aws pubsub message
        """


class StorageEventsStreamOperator(StreamOperator):
    """
    StreamOperator for Cloud Storage Events topics
    To filter by event type, use subscription filters in GCP
    """

    def __init__(
        self,
        client: Client,
        fs: fsspec.spec.AbstractFileSystem,
        date_test_hook: datetime.date = None,
    ):
        super().__init__()
        self.dask_client = client
        self._date_test_hook = date_test_hook
        self._fs = fs

    @abstractmethod
    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> tuple[str, str]:
        """
        The method that decodes the message data and controls execution the requested forecast training process
        :param message: a pubsub message
        """
        match type(message):
            case pubsub_v1.subscriber.message.Message:
                logger.info(
                    "GCS Attributes: %s", json.dumps(dict(**message.attributes))
                )

                object_id = message.attributes.get("objectId")
                bucket = message.attributes.get("bucketId")
                return bucket, object_id
            case TestStructures.FakeMessage:
                logger.info(
                    "Test Message Attributes: %s",
                    json.dumps(dict(**message.attributes)),
                )

                object_id = message.attributes.get("objectId")
                bucket = message.attributes.get("bucketId")
                return bucket, object_id
            case _:
                raise RuntimeError(f"Unexpected message type: {type(message)}")


class DeadLetterQueueAlertsStreamOperator(StreamOperator):
    SUBSCRIPTION = "noaa-hrrr-forecast-dead-letter-queue"

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ):
        logger.warning(
            "Dead Letter Queue Message Id %s:",
            message.message_id,
        )
        logger.warning(
            "Dead Letter Queue Message Attributes: %s",
            json.dumps(dict(**message.attributes)),
        )
        logger.warning(
            "Dead Letter Queue Message Data: %s",
            message.data,
        )
        # TODO Send a monitoring alert somewhere?


class HrrrForecastRunAggregator(StorageEventsStreamOperator):
    """
    This StreamOperator creates forcast run aggregations of 18 and/or 48 hours from the raw zarr timestep files.

    Example input path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t01z.wrfsfcf18.zarr
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t00z.wrfsfcf48.zarr
    Example output path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220720/hrrr.t01z.wrfsfcf.18_hour_forecast.zarr
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220720/hrrr.t00z.wrfsfcf.48_hour_forecast.zarr
    """

    # Keep the model matcher group till we can use it again with subhourly output
    HRRR_MATCHER = re.compile(
        r"^high-resolution-rapid-refresh\/version_2\/raw_zarr\/conus\/hrrr\.(?P<date>\d{8})\/hrrr\.t(?P<hour>\d{2})z\.(?P<model>wrfsfcf)(?P<horizon>\d{2})\.zarr$"
    )

    # TOPIC = "gcp-public-data-weather_events"
    # Filter for "high-resolution-rapid-refresh/version_2/raw_zarr" and event type finalize or sweep
    SUBSCRIPTION = "noaa-hrrr-forecast-run-aggregation"

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        On receiving a message about a new zarr blob, aggregate if it is newer than yesterday or completes a forecast
        run
        :param message: a pubsub message
        """
        bucket, object_id = super().transform(message)

        if bucket != consts.EXTRACTED_BUCKET:
            logger.warning("Received message with non default bucket: %s", bucket)

        matched = self.HRRR_MATCHER.match(object_id)
        if not matched:
            logger.warning(
                "Unexpected message not matching regex filter: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return
        forecast_hour = int(matched.group("hour"))
        forecast_horizon = int(matched.group("horizon"))
        model = matched.group("model")
        forecast_date = datetime.datetime.strptime(
            matched.group("date"), "%Y%m%d"
        ).date()
        logger.info(
            "Parsing forecast for date %s hour %d horizon %d model %s",
            forecast_date,
            forecast_hour,
            forecast_horizon,
            model,
        )
        # Yesterday, UTC
        yesterday = (
            self._date_test_hook or datetime.date.today()
        ) - datetime.timedelta(days=1)

        wrote_blob = None
        # If the model run is recent (forecast run yesterday or today), trigger the 18-hr aggregation for any
        # forecast horizon in the 18-hr forecast set.
        # If the message is from before yesterday, only trigger aggregation if it is the final 18 hr horizon.
        # This requires the sweep behavior for backfills because messages may arrive out of order, resulting in
        # gaps in the aggregation when the final timestep is processed.
        if (
            (forecast_date >= yesterday) and (forecast_horizon <= 18)
        ) or forecast_horizon == 18:
            logger.info("18 Forecast Run: %s", object_id)

            output_path = f"{bucket}/{object_id}".replace(
                f"{forecast_horizon:02d}.zarr", ".18_hour_forecast.zarr"
            ).replace(consts.RAW_ZARR, consts.FORECAST_RUN)
            # run all forecast hours and let the multizarr method aggregate the ones that exist
            input_paths = [
                f"{bucket}/{object_id}".replace(
                    f"{forecast_horizon:02d}.zarr", f"{i:02}.zarr"
                )
                for i in range(0, 19)
            ]

            multizarr_future = self.dask_client.submit(
                multizarr, self._fs, input_paths, output_path
            )
            wrote_blob = multizarr_future.result()
            logger.info("Completed aggregation for 18 forecast: %s", wrote_blob)

        # If the model run is recent (forecast run yesterday or today), always trigger 48-hr aggregation for any
        # result in the 48-hr forecast set, which are published for forecast hours (0, 6, 12, 18).
        # If the message is from before yesterday, only trigger aggregation if it is the final 48 hr horizon.
        # This requires the sweep behavior for backfills because messages may arrive out of order resulting in
        # gaps in the aggregation when the final timestep is processed.
        if (
            (forecast_date >= yesterday) and (forecast_hour in (0, 6, 12, 18))
        ) or forecast_horizon == 48:
            logger.info("48 Forecast Run: %s", object_id)

            output_path = f"{bucket}/{object_id}".replace(
                f"{forecast_horizon:02d}.zarr", ".48_hour_forecast.zarr"
            ).replace(consts.RAW_ZARR, consts.FORECAST_RUN)
            # run all forecast hours and let the multizarr method aggregate the ones that exist
            input_paths = [
                f"{bucket}/{object_id}".replace(
                    f"{forecast_horizon:02d}.zarr", f"{i:02}.zarr"
                )
                for i in range(0, 49)
            ]

            multizarr_future = self.dask_client.submit(
                multizarr, self._fs, input_paths, output_path
            )
            wrote_blob = multizarr_future.result()
            logger.info("Completed aggregation for 48 forecast: %s", wrote_blob)

        if wrote_blob is None:
            logger.info(
                "Skipping: %s is not recent nor the end of a forecast run", object_id
            )


class HrrrDailyHorizonAggregator(StorageEventsStreamOperator):
    """
    This StreamOperator creates daily HRRR aggregations by forecast horizon from the raw zarr timestep files.

    Example input path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t00z.wrfsfcf18.zarr
    Example output path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.18_hour_horizon.zarr
    """

    # Keep the model matcher group till we can use it again with subhourly output
    HRRR_MATCHER = re.compile(
        r"^high-resolution-rapid-refresh\/version_2\/raw_zarr\/conus\/hrrr\.(?P<date>\d{8})\/hrrr\.t(?P<hour>\d{2})z\.(?P<model>wrfsfcf)(?P<horizon>\d{2})\.zarr$"
    )

    # TOPIC = "gcp-public-data-weather_events"
    # Filter for "high-resolution-rapid-refresh/version_2/raw_zarr" and event type finalize or sweep
    SUBSCRIPTION = "noaa-hrrr-forecast-horizon-daily-aggregation"

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        On receiving a message about a new zarr blob, aggregate if it is from the yesterday or today or then end of an older day
        :param message: a pubsub message
        """
        bucket, object_id = super().transform(message)

        if bucket != consts.EXTRACTED_BUCKET:
            logger.warning("Received message with non default bucket: %s", bucket)

        matched = self.HRRR_MATCHER.match(object_id)
        if not matched:
            logger.warning(
                "Unexpected message not matching regex filter: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        forecast_date = datetime.datetime.strptime(
            matched.group("date"), "%Y%m%d"
        ).date()
        forecast_hour = int(matched.group("hour"))
        forecast_horizon = int(matched.group("horizon"))
        model = matched.group("model")
        logger.info(
            "Parsing raw_zarr for date %s hour %d horizon %d model %s",
            forecast_date,
            forecast_hour,
            forecast_horizon,
            model,
        )

        if not last_forecast_of_day_or_recent(
            forecast_date, forecast_hour, forecast_horizon, today=self._date_test_hook
        ):
            logger.info(
                "Skipping backfill message that does not complete a daily aggregation group: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        input_paths = []
        if forecast_horizon <= 18:
            # Create the output path for the horizon aggregation. HRRR produces an 18 hour horizon forecast every hour.
            # So if the horizon is <= 18 we produce a new aggregation every hour for each horizon.
            # For example, if the received event is for the 14 hour horizon forecast on 20220720 at 21z
            # (hrrr.20220720/hrrr.t21z.wrfsfcf14.zarr), the output would be:
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.14_hour_horizon.zarr
            output_path = os.path.join(
                bucket,
                "high-resolution-rapid-refresh",
                consts.SEMANTIC_VERSION,
                consts.DAILY_HORIZON,
                "conus",
                f"hrrr.{forecast_date.strftime('%Y%m%d')}",
                f"hrrr.{model}.{forecast_horizon:02}_hour_horizon.zarr",
            )

            # Enumerate the inputs to aggregate from the day and forecast horizon of the event message
            # For each raw zarr forecast horizon create an aggregation for the day upto the current forecast run hour
            # For example, if the received event is for the 14 hour horizon forecast on 20220720 at 21Z
            # (hrrr.20220720/hrrr.t21z.wrfsfcf14.zarr), the inputs would be
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t00z.wrfsfcf14.zarr
            # ...
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t21z.wrfsfcf14.zarr
            for hour in range(0, 24):
                # run all forecast hours and let the multizarr method aggregate the ones that exist
                input_paths.append(
                    os.path.join(
                        bucket,
                        "high-resolution-rapid-refresh",
                        consts.SEMANTIC_VERSION,
                        consts.RAW_ZARR,
                        "conus",
                        f"hrrr.{forecast_date.strftime('%Y%m%d')}",
                        f"hrrr.t{hour:02}z.{model}{forecast_horizon:02}.zarr",
                    )
                )

        elif forecast_horizon in (24, 30, 36, 42, 48):
            # Create the output path for the horizon aggregation. We only get 19-48 hour horizon forecasts every 6 hours.
            # Therefore, only the 24, 30, 36, 42, 48 hour horizons provide a complete forecast at each 6 hour interval.
            # For example, if the received event is for a 24 hour horizon forecast on 20220720 at 12Z, the output would be:
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.19-24_hour_horizon.zarr
            output_path = os.path.join(
                bucket,
                "high-resolution-rapid-refresh",
                consts.SEMANTIC_VERSION,
                consts.DAILY_HORIZON,
                "conus",
                f"hrrr.{forecast_date.strftime('%Y%m%d')}",
                f"hrrr.{model}.{forecast_horizon-5:02}-{forecast_horizon:02}_hour_horizon.zarr",
            )

            # Enumerate the inputs to aggregate from the day and forecast horizon of the event message
            # For each raw zarr forecast horizon in the range, create an aggregation for the day upto the current forecast run hour
            # For example, if the received event is for the 24 hour horizon forecast on 20220720 at 12Z
            # (hrrr.20220720/hrrr.t12z.wrfsfcf24.zarr), the inputs would be
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t00z.wrfsfcf19.zarr
            # ...
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t00z.wrfsfcf24.zarr
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t06z.wrfsfcf19.zarr
            # ...
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t06z.wrfsfcf24.zarr
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t12z.wrfsfcf19.zarr
            # ...
            # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t12z.wrfsfcf24.zarr
            for hour in (0, 6, 12, 18):
                # run all forecast hours and let the multizarr method aggregate the ones that exist
                for horizon in range(forecast_horizon - 5, forecast_horizon + 1):
                    input_paths.append(
                        os.path.join(
                            bucket,
                            "high-resolution-rapid-refresh",
                            consts.SEMANTIC_VERSION,
                            consts.RAW_ZARR,
                            "conus",
                            f"hrrr.{forecast_date.strftime('%Y%m%d')}",
                            f"hrrr.t{hour:02}z.{model}{horizon:02}.zarr",
                        )
                    )

        else:
            logger.info(
                "Skipping a message that does not complete an aggregation horizon: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        multizarr_future = self.dask_client.submit(
            multizarr, self._fs, input_paths, output_path
        )
        wrote_blob = multizarr_future.result()
        logger.info("Completed daily aggregation forecast: %s", wrote_blob)


class HrrrMonthlyHorizonAggregator(StorageEventsStreamOperator):
    """
    This StreamOperator creates monthly HRRR aggregations by forecast horizon from the daily aggregations.

    Example input path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.19-24_hour_horizon.zarr
    Example output path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202207/hrrr.wrfsfcf.19-24_hour_horizon.zarr
    """

    # Keep the model matcher group till we can use it again with subhourly output
    HRRR_MATCHER = re.compile(
        r"high-resolution-rapid-refresh\/version_2\/daily_horizon\/conus\/hrrr\.(?P<date>\d{8})\/hrrr\.(?P<model>wrfsfcf)\.(?P<horizon>\d{2}|\d{2}-\d{2})_hour_horizon\.zarr$"
    )

    # TOPIC = "gcp-public-data-weather_events"
    # Filter for "high-resolution-rapid-refresh/version_2/daily_horizon" and event type finalize
    SUBSCRIPTION = "noaa-hrrr-forecast-horizon-monthly-aggregation"

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        On receiving a message about a new zarr blob, aggregate if it is from the current month or the end of an old month
        :param message: a pubsub message
        """
        bucket, object_id = super().transform(message)

        if bucket != consts.EXTRACTED_BUCKET:
            logger.warning("Received message with non default bucket: %s", bucket)

        matched = self.HRRR_MATCHER.match(object_id)
        if not matched:
            logger.warning(
                "Unexpected message not matching regex filter: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        forecast_date = datetime.datetime.strptime(
            matched.group("date"), "%Y%m%d"
        ).date()
        forecast_horizon = matched.group("horizon")  # keep as string: "12" or "19-24"
        model = matched.group("model")
        logger.info(
            "Parsing daily output for monthly aggregation: date %s, horizon %s, model %s",
            forecast_date,
            forecast_horizon,
            model,
        )

        if not last_forecast_of_month_or_this_month(
            forecast_date, today=self._date_test_hook
        ):
            logger.info(
                "Skipping backfill message that does not complete a monthly aggregation group: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        # Create the output path for the monthly horizon aggregation.
        # For example, if the received event is for the 14 hour horizon daily aggregation forecast on 20220720
        # high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.14_hour_horizon.zarr
        # Would become:
        # high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202207/hrrr.wrfsfcf.14_hour_horizon.zarr
        output_path = os.path.join(
            bucket,
            "high-resolution-rapid-refresh",
            consts.SEMANTIC_VERSION,
            consts.MONTHLY_HORIZON,
            "conus",
            f"hrrr.{forecast_date.strftime('%Y%m')}",
            f"hrrr.{model}.{forecast_horizon}_hour_horizon.zarr",
        )

        # Enumerate the inputs to aggregate from the day and forecast horizon of the event message
        # For each raw zarr forecast horizon create an aggregation for the day upto the current forecast run hour
        # For example, if the received event is for the 14 hour horizon forecast on 20220720 at 21Z
        # (hrrr.20220720/hrrr.t21z.wrfsfcf14.zarr), the inputs would be
        # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.t00z.wrfsfcf14.zarr
        # ...
        # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t21z.wrfsfcf14.zarr
        input_paths = []
        month = forecast_date.month
        date = forecast_date.replace(day=1)
        while date.month == month:
            # Always run whole months!
            input_paths.append(
                os.path.join(
                    bucket,
                    "high-resolution-rapid-refresh",
                    consts.SEMANTIC_VERSION,
                    consts.DAILY_HORIZON,
                    "conus",
                    f"hrrr.{date.strftime('%Y%m%d')}",
                    f"hrrr.{model}.{forecast_horizon}_hour_horizon.zarr",
                )
            )
            date += datetime.timedelta(days=1)

        multizarr_future = self.dask_client.submit(
            multizarr, self._fs, input_paths, output_path
        )
        wrote_blob = multizarr_future.result()
        logger.info("Completed monthly forecast aggregation: %s", wrote_blob)


class HrrrAllTimeHorizonAggregator(StorageEventsStreamOperator):
    """
    This StreamOperator creates all time HRRR aggregations by forecast horizon from the monthly aggregations.
    This may be helpful if you are always looking at the full history. It is probably harmful if you are primarily
    looking at a single month at a time. Consider using multizarr aggregation on the fly to read the months you need.

    Example input path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.19-24_hour_horizon.zarr
    Example output path:
    gcs://gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202207/hrrr.wrfsfcf.19-24_hour_horizon.zarr
    """

    # Keep the model matcher group till we can use it again with subhourly output
    HRRR_MATCHER = re.compile(
        r"high-resolution-rapid-refresh\/version_2\/monthly_horizon\/conus\/hrrr\.(?P<date>\d{6})\/hrrr\.(?P<model>wrfsfcf)\.(?P<horizon>\d{2}|\d{2}-\d{2})_hour_horizon\.zarr$"
    )

    # TOPIC = "gcp-public-data-weather_events"
    # Filter for "high-resolution-rapid-refresh/version_2/monthly_horizon" and event type finalize
    SUBSCRIPTION = "noaa-hrrr-forecast-horizon-alltime-aggregation"

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        On receiving a message about a new zarr blob, aggregate if it is from the current month or the end of an old month
        :param message: a pubsub message
        """
        bucket, object_id = super().transform(message)

        if bucket != consts.EXTRACTED_BUCKET:
            logger.warning("Received message with non default bucket: %s", bucket)

        matched = self.HRRR_MATCHER.match(object_id)
        if not matched:
            logger.warning(
                "Unexpected message not matching regex filter: %s",
                ujson.dumps(dict(**message.attributes)),
            )
            return

        forecast_date = datetime.datetime.strptime(matched.group("date"), "%Y%m").date()
        forecast_horizon = matched.group("horizon")  # keep as string: "12" or "19-24"
        model = matched.group("model")
        logger.info(
            "Parsing monthly update for alltime aggregation: date %s horizon %s model %s",
            forecast_date,
            forecast_horizon,
            model,
        )

        # Create the output path for the alltime horizon aggregation.
        # For example, if the received event is for the 14 hour horizon monthly aggregation forecast on 202207
        # high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202207/hrrr.wrfsfcf.14_hour_horizon.zarr
        # Would become:
        # high-resolution-rapid-refresh/version_2/alltime_horizon/conus/hrrr.wrfsfcf.14_hour_horizon.zarr
        output_path = os.path.join(
            bucket,
            "high-resolution-rapid-refresh",
            consts.SEMANTIC_VERSION,
            consts.ALLTIME_HORIZON,
            "conus",
            f"hrrr.{model}.{forecast_horizon}_hour_horizon.zarr",
        )

        # Enumerate the inputs to aggregate from the month and forecast horizon of the event message
        # For each monthly zarr forecast horizon create an aggregation for the alltime upto the current forecast run
        # For example, if the received event is for the 14 hour horizon forecast on 202207
        # (hrrr.202207/hrrr.wrfsfcf14.zarr), the inputs would be
        # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202101/hrrr.wrfsfcf14.zarr
        # ...
        # gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.202211/hrrr.wrfsfcf14.zarr
        today = self._date_test_hook or datetime.date.today()
        input_paths = []
        date = datetime.date.fromisoformat(consts.ALL_TIME_START_DATE)
        while date <= today:
            input_paths.append(
                os.path.join(
                    bucket,
                    "high-resolution-rapid-refresh",
                    consts.SEMANTIC_VERSION,
                    consts.MONTHLY_HORIZON,
                    "conus",
                    f"hrrr.{date.strftime('%Y%m')}",
                    f"hrrr.{model}.{forecast_horizon}_hour_horizon.zarr",
                )
            )

            month = date.month
            while month == date.month:
                date += datetime.timedelta(days=10)
            date = date.replace(day=1)

        multizarr_future = self.dask_client.submit(
            multizarr, self._fs, input_paths, output_path
        )
        wrote_blob = multizarr_future.result()
        logger.info("Completed alltime forecast aggregation: %s", wrote_blob)


class HrrrGrib2ZarrExtractor(StorageEventsStreamOperator):
    """
    StreamOperator for extracting grib layers from HRRR output into zarr using Kerchunk
    Example input:
    gcs://high-resolution-rapid-refresh/hrrr.20221028/conus/hrrr.t00z.wrfsfcf01.grib2

    This is actually a public bucket that belongs to NOAA/GCP

    Matching only the sfcf product for now.
    https://www.nco.ncep.noaa.gov/pmb/products/hrrr/
    The subhourly data would be really valuable, but kerchunk scangrib does not handle the complex message types properly yet
    """

    HRRR_MATCHER = re.compile(
        r"^hrrr\.(?P<date>\d{8})\/conus\/hrrr\.t(?P<hour>\d{2})z\.(?P<model>wrfsfcf)(?P<horizon>\d{2})\.grib2$"
    )

    # TOPIC = "projects/gcp-public-data-weather/topics/gcp-public-data-hrrr"
    # FILTER for event type "OBJECT_FINALIZE" only, must filter output types with a regex because the path structure does not
    # allow a startswith filter for the wrfsfcf model output
    SUBSCRIPTION = "noaa-hrrr-forecast-grib"

    def __init__(
        self,
        *args,
        output_path: PurePosixPath = PurePosixPath(consts.EXTRACTED_BUCKET),
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.output_path = output_path

    def emit_metrics(self, matched):
        forecast_horizon = matched.group("horizon")
        model = matched.group("model")
        run_time = matched.group("date") + "T" + matched.group("hour")
        forecast_datetime = datetime.datetime.strptime(run_time, "%Y%m%dT%H")
        latency = datetime.datetime.now() - forecast_datetime
        self._emit_metrics(model, forecast_horizon, run_time, latency)

    def _emit_metrics(self, model, forecast_horizon, run_time, latency):
        logger.info(
            f"Model: {model}, horizon: {forecast_horizon}, run_time: {run_time}, latency: {latency}"
        )
        # TODO Override with real metrics library of choice!
        # Example: datadog statsd gauge
        # statsd.gauge(
        #     "ingestion.noaa_nwp.hrrr.latency",
        #     latency.total_seconds() / 60.0,  # measure HRRR latency in minutes
        #     tags=[
        #         f"model:{model}",
        #         f"horizon:{forecast_horizon}",
        #         f"run_time:{run_time}",
        #     ],
        #     )

    def transform(
        self,
        message: Union[
            pubsub_v1.subscriber.message.Message, TestStructures.FakeMessage
        ],
    ) -> None:
        """
        On receiving a message about new grib2 HRRR output, check to see if it is a matching model output type
        and extract the relevant datasets into zarr blobs stored by reference.
        """
        bucket, object_id = super().transform(message)

        matched = self.HRRR_MATCHER.match(object_id)
        if matched:
            logger.info("extracting: %s", object_id)

            match type(message):
                case pubsub_v1.subscriber.message.Message:
                    input_fs = fsspec.filesystem("gcs")
                case TestStructures.FakeMessage:
                    input_fs = fsspec.filesystem(message.attributes["protocol"])
                case _:
                    raise RuntimeError(f"Unknown message type {type(message)}")

            extract_future = self.dask_client.submit(
                extract_grib,
                input_fs,
                PurePosixPath(bucket),
                PurePosixPath(object_id),
                self._fs,
                self.output_path,
            )

            output_blob_path = extract_future.result()
            logger.info("finished extracting %s to %s", object_id, output_blob_path)
            self.emit_metrics(matched)

        else:
            logger.info("skipping: %s", object_id)


class BackfillHrrrGrib2ZarrExtractor(HrrrGrib2ZarrExtractor):
    """
    StreamOperator for backfilling extracted grib layers from HRRR output into zarr using Kerchunk
    This class only exists so that the grib2zarr transform can be attached to a separate backfill pubsub channel
    This prevents backing up the real time data while running backfill operations
    """

    # TOPIC = "projects/gcp-public-data-weather/topics/gcp-public-data-hrrr"
    # FILTER for event type "BACKFILL" only, must filter output types with a regex because the path structure does not
    # allow a startswith filter for the wrfsfcf model output
    SUBSCRIPTION = "noaa-hrrr-forecast-grib-backfill"

    def emit_metrics(self, matched):
        # Latency metrics make not sense in the backfill context
        logger.debug(matched)


"""
This is a glorified test script for local experimentation outside the PubSub system.
It does push real artifacts to GCS as currently written which may trigger further actions - user beware!
// TODO fix this so it is completely local!
"""

if __name__ == "__main__":

    import argparse
    from dask.distributed import Client
    import time
    import cProfile, pstats
    import concurrent.futures

    parser = argparse.ArgumentParser(
        """
        Demo application to experiment with the HRRR stream operators using the local file system.
        """
    )
    parser.add_argument(
        "mode",
        type=str,
        choices=[
            consts.FORECAST_RUN,
            consts.DAILY_HORIZON,
            consts.MONTHLY_HORIZON,
            consts.ALLTIME_HORIZON,
            consts.RAW_ZARR,
            "alert",
        ],
    )
    parser.add_argument(
        "batch_start",
        help="start date for processing",
        type=datetime.date.fromisoformat,
    )

    parser.add_argument(
        "batch_end",
        help="end date for processing",
        type=datetime.date.fromisoformat,
    )

    parser.add_argument(
        "--batch_model",
        # See https://www.nco.ncep.noaa.gov/pmb/products/hrrr/ for details on hrrr products
        help="Select the wrf hrrr model output to submit for processing",
        type=str,
        default="wrfsfcf",
        choices=[
            "wrfsfcf",
        ],
    )

    parser.add_argument(
        "--cprofiler",
        help="Flag to run with cprofiler",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    args = parser.parse_args()

    # Logging...
    log_level = logging.INFO
    init_logger = logging.getLogger()
    logging.Formatter.converter = time.gmtime
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03dZ P:%(processName)s T:%(threadName)s %(levelname)s:%(filename)s:%(funcName)s:%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    stream_handler.setFormatter(formatter)
    init_logger.addHandler(stream_handler)
    init_logger.setLevel(log_level)

    logger.info("Hello: %s", args)

    # Control where to read from and where to write too...
    grib_source_protocol = "gcs"  # this is where to look for the raw grib data from NOAA (add S3 when implemented)
    fs = fsspec.filesystem(
        "file", auto_mkdir=True
    )  # Write the output to the local file system
    base_path = "/tmp/aggregator/gcp-public-data-weather"  # use this as the base path for this demo application

    def create_messages_and_operator(client, mode, batch_start, batch_end, batch_model):

        messages = []
        match mode:
            case consts.FORECAST_RUN:
                operator = HrrrForecastRunAggregator(
                    client, fs=fs, date_test_hook=batch_end
                )

                for date in (
                    batch_start + datetime.timedelta(days=x)
                    for x in range(0, (batch_end - batch_start).days + 1)
                ):
                    for hour in range(0, 24):  # for each forecast run on the hour

                        # Each hour, NOAA runs at HRRR at least 19 hours into the future.
                        # Every 6 hours we get a 49 hour forecast but only for the wrfsfcf product.
                        forecast_horizons = (
                            (18, 48)
                            if (batch_model == "wrfsfcf") and (hour in [0, 6, 12, 18])
                            else (18,)
                        )

                        for (
                            horizon
                        ) in (
                            forecast_horizons
                        ):  # for each completed run emit just tha last timestep to force aggregation
                            blob = f"high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.{date.strftime('%Y%m%d')}/hrrr.t{hour:02}z.{batch_model}{horizon:02}.zarr"

                            messages.append(
                                TestStructures.FakeMessage(
                                    attributes=dict(
                                        objectId=blob,
                                        bucketId=base_path,
                                    )
                                )
                            )

            case consts.DAILY_HORIZON:
                operator = HrrrDailyHorizonAggregator(
                    client, fs=fs, date_test_hook=batch_end
                )

                for date in (
                    batch_start + datetime.timedelta(days=x)
                    for x in range(0, (batch_end - batch_start).days + 1)
                ):
                    for hour in range(18, 23):  # for the last forecast run of each day

                        # Each hour, NOAA runs at HRRR at least 19 hours into the future.
                        # Every 6 hours we get a 49 hour forecast but only for the wrfsfcf product.
                        forecast_horizons = [i for i in range(0, 19)]
                        if (batch_model == "wrfsfcf") and (hour in [0, 6, 12, 18]):
                            forecast_horizons += [24, 30, 36, 42, 48]

                        for (
                            horizon
                        ) in (
                            forecast_horizons
                        ):  # for each horizon emit just tha last timestep to force aggregation
                            blob = f"high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.{date.strftime('%Y%m%d')}/hrrr.t{hour:02}z.{batch_model}{horizon:02}.zarr"

                            messages.append(
                                TestStructures.FakeMessage(
                                    attributes=dict(
                                        objectId=blob,
                                        bucketId=base_path,
                                    )
                                )
                            )

            case consts.MONTHLY_HORIZON:
                operator = HrrrMonthlyHorizonAggregator(
                    client, fs=fs, date_test_hook=batch_end
                )

                # This is horrible. Should be rewritten using a real datetime library
                # Emit a message for the last day of the ending month
                batch_end_next_month = batch_end
                for i in range(35):
                    if batch_end_next_month.month != batch_end.month:
                        break
                    batch_end_next_month += datetime.timedelta(days=1)

                # Loop over each day - only emit messages when the month changes
                month = batch_start.month
                for date in (
                    batch_start + datetime.timedelta(days=x)
                    for x in range(0, (batch_end_next_month - batch_start).days + 1)
                ):  # for the last day in each month
                    if date.month == month:
                        last_date = date
                        continue

                    month = date.month

                    # Each hour, NOAA runs at HRRR at least 19 hours into the future.
                    # Every 6 hours we get a 49 hour forecast but only for the wrfsfcf product.
                    forecast_horizons = [f"{i:02}" for i in range(0, 19)]
                    if batch_model == "wrfsfcf":
                        forecast_horizons += [
                            "19-24",
                            "25-30",
                            "31-36",
                            "37-42",
                            "43-48",
                        ]

                    for (
                        horizon
                    ) in (
                        forecast_horizons
                    ):  # for each horizon emit just tha last timestep to force aggregation
                        blob = f"high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.{last_date.strftime('%Y%m%d')}/hrrr.{batch_model}.{horizon}_hour_horizon.zarr"

                        messages.append(
                            TestStructures.FakeMessage(
                                attributes=dict(
                                    objectId=blob,
                                    bucketId=base_path,
                                )
                            )
                        )

            case consts.ALLTIME_HORIZON:
                operator = HrrrAllTimeHorizonAggregator(
                    client, fs=fs, date_test_hook=batch_end
                )

                # Just emit a message for any month on each horizon
                forecast_horizons = [f"{i:02}" for i in range(0, 19)]
                if batch_model == "wrfsfcf":
                    forecast_horizons += ["19-24", "25-30", "31-36", "37-42", "43-48"]
                for horizon in forecast_horizons:
                    messages.append(
                        TestStructures.FakeMessage(
                            attributes=dict(
                                objectId=f"high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202210/hrrr.wrfsfcf.{horizon}_hour_horizon.zarr",
                                bucketId=base_path,
                            )
                        ),
                    )

            case consts.RAW_ZARR:
                operator = HrrrGrib2ZarrExtractor(
                    client, fs=fs, output_path=PurePosixPath(base_path)
                )

                logging.warning("Processing even a single whole day is 576 files.")
                logging.warning(
                    "This will take a while... you can ctrl-c to exit when ever you want."
                )

                for date in (
                    batch_start + datetime.timedelta(days=x)
                    for x in range(0, (batch_end - batch_start).days + 1)
                ):
                    for hour in range(0, 24):  # for each forecast run on the hour

                        # Each hour, NOAA runs at HRRR at least 19 hours into the future.
                        # Every 6 hours we get a 49 hour forecast but only for the wrfsfcf product.
                        forecast_horizon = (
                            49
                            if (batch_model == "wrfsfcf") and (hour in [0, 6, 12, 18])
                            else 19
                        )

                        for horizon in range(
                            0, forecast_horizon
                        ):  # for each time step created
                            blob = f"hrrr.{date.strftime('%Y%m%d')}/conus/hrrr.t{hour:02}z.{batch_model}{horizon:02}.grib2"

                            messages.append(
                                TestStructures.FakeMessage(
                                    attributes=dict(
                                        objectId=blob,
                                        bucketId="high-resolution-rapid-refresh",
                                        # Protocol is only used for Fake Messages - allows reading from GCS or AWS when working outside Pubsub
                                        protocol=grib_source_protocol,
                                    )
                                )
                            )

            case "alert":
                operator = DeadLetterQueueAlertsStreamOperator(None)
                messages.append(
                    TestStructures.FakeMessage(
                        attributes=dict(
                            objectId="hrrr.20220720/conus/hrrr.t00z.wrfsfcf21.grib2",
                            bucketId="high-resolution-rapid-refresh",
                        )
                    )
                )
            case _:
                raise RuntimeError("Bad mode argument: %s", mode)

        return operator, messages

    # Run with a single process dask client if trying to use cProfile, otherwise use dask multiprocess!
    with Client(processes=args.cprofiler is False) as dask_client:
        operator, messages = create_messages_and_operator(
            dask_client, args.mode, args.batch_start, args.batch_end, args.batch_model
        )

        logger.info("Attempting to transform %s message", len(messages))

        if args.cprofiler:
            for message in messages:
                logger.info("Processing message: %s", message.attributes)
                try:
                    with cProfile.Profile() as pr:
                        operator.transform(message)
                    pstats.Stats(pr).sort_stats("tottime").print_stats(50)
                except Exception:
                    # As of 2022-12-30 there are known missing/incomplete files in the GCP high-resolution-rapid-refresh bucket
                    logger.exception(
                        "processing message %s caused an error!", message.attributes
                    )

        else:
            # In a production system, use a threaded pubsub client to process notifications
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=os.cpu_count()
            ) as executor:
                futures = {
                    executor.submit(operator.transform, message): message
                    for message in messages
                }
                for future in concurrent.futures.as_completed(futures):
                    message = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        print(
                            "%s generated an exception: %s" % (message.attributes, exc)
                        )

    logger.info("Tada - all done!")
