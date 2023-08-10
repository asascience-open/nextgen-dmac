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

import datetime
import os.path
import tempfile
import unittest
from pathlib import PurePosixPath
from unittest.mock import Mock, patch
import fsspec
import aggregator.operators

INTEGRATION_TEST = False
INTEGRATION_TEST_BUCKET = "dev.camus-infra.camus.store"


class ModuleMethodsTest(unittest.TestCase):
    @unittest.skipUnless(INTEGRATION_TEST, "Skipping integration tests")
    def test_extract_grib_to_file(self):
        """
        takes a long time, requires access to public Google Cloud bucket
        :return:
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            aggregator.operators.extract_grib(
                fsspec.filesystem("gcs", token=None),
                PurePosixPath("high-resolution-rapid-refresh"),
                PurePosixPath("hrrr.20221219/conus/hrrr.t12z.wrfsfcf03.grib2"),
                fsspec.filesystem("file", auto_mkdir=True),
                temp_dir,
            )

            output_path = os.path.join(
                temp_dir,
                "high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20221219/hrrr.t12z.wrfsfcf03.zarr",
            )
            os.path.exists(output_path)
            with open(output_path, "r") as f:
                data = f.read()
            self.assertEqual(len(data), 47025, "The expected resultant file size")

    @unittest.skipUnless(INTEGRATION_TEST, "Skipping integration tests")
    def test_extract_grib_to_gcs(self):
        """
        takes a long time, requires access to public Google Cloud bucket
        :return:
        """
        output_fs = fsspec.filesystem("gcs", token=None)

        aggregator.operators.extract_grib(
            fsspec.filesystem("gcs", token=None),
            PurePosixPath("high-resolution-rapid-refresh"),
            PurePosixPath("hrrr.20221219/conus/hrrr.t12z.wrfsfcf03.grib2"),
            output_fs,
            PurePosixPath(INTEGRATION_TEST_BUCKET),
        )

        output_path = os.path.join(
            INTEGRATION_TEST_BUCKET,
            "high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20221219/hrrr.t12z.wrfsfcf03.zarr",
        )
        self.assertTrue(output_fs.exists(output_path))
        with output_fs.open(output_path, "r") as f:
            data = f.read()
        self.assertEqual(len(data), 47025, "The expected resultant file size")

        # Try to clean up - in no way guaranteed!
        output_fs.rm(output_path)

    def test_last_forecast_of_day_or_recent(self):

        self.assertTrue(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=12,
                forecast_horizon=6,
                today=datetime.date.fromisoformat("2022-11-01"),
            ),
            "The forecast date is greater than or equal to tomorrow",
        )

        self.assertTrue(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=12,
                forecast_horizon=6,
                today=datetime.date.fromisoformat("2022-11-02"),
            ),
            "The forecast date is greater than or equal to today",
        )

        self.assertTrue(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=12,
                forecast_horizon=6,
                today=datetime.date.fromisoformat("2022-11-03"),
            ),
            "The forecast date is greater than or equal to yesterday",
        )

        self.assertFalse(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=12,
                forecast_horizon=6,
                today=datetime.date.fromisoformat("2022-11-04"),
            ),
            "The forecast date is not greater than or equal to yesterday",
        )

        self.assertTrue(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=23,
                forecast_horizon=18,
                today=datetime.date.fromisoformat("2022-11-04"),
            ),
            "This is the last 18 hour forecast for a old date",
        )

        self.assertTrue(
            aggregator.operators.last_forecast_of_day_or_recent(
                forecast_date=datetime.date.fromisoformat("2022-11-02"),
                forecast_hour=18,
                forecast_horizon=48,
                today=datetime.date.fromisoformat("2022-11-04"),
            ),
            "This is the last 48 hour forecast for a old date",
        )

    def test_last_forecast_of_month_or_this_month(self):

        self.assertTrue(
            aggregator.operators.last_forecast_of_month_or_this_month(
                forecast_date=datetime.date.fromisoformat("2022-11-03"),
                today=datetime.date.fromisoformat("2022-11-13"),
            ),
            "The forecast date from the current month",
        )

        self.assertFalse(
            aggregator.operators.last_forecast_of_month_or_this_month(
                forecast_date=datetime.date.fromisoformat("2022-10-20"),
                today=datetime.date.fromisoformat("2022-11-04"),
            ),
            "The forecast date is not from the current month",
        )

        self.assertTrue(
            aggregator.operators.last_forecast_of_month_or_this_month(
                forecast_date=datetime.date.fromisoformat("2022-10-31"),
                today=datetime.date.fromisoformat("2022-11-04"),
            ),
            "The forecast is the last day of the month",
        )


class FilterOnPresenceTest(unittest.TestCase):
    # Watch out for fsspec.filesystem("memory") - it is a global and can hold state between tests!
    def setUp(self) -> None:
        self.fs = fsspec.filesystem("memory")

        self.test_files = [f"/test/dir/{val}" for val in "gcabidfeh"]
        self.expected = sorted(self.test_files)

        self.fs.mkdir("/test/dir")
        for fname in self.test_files:
            self.fs.touch(fname)

    def tearDown(self) -> None:
        self.fs.rm("/test", recursive=True)

    @patch("aggregator.operators.logger")
    def test_filter_on_presence_all_there(self, mock_logger):
        # With all the files present
        results = aggregator.operators.filter_on_presence(self.test_files, fs=self.fs)
        self.assertListEqual(results, self.expected)
        mock_logger.debug.assert_not_called()

    @patch("aggregator.operators.logger")
    def test_filter_on_presence_missing_last(self, mock_logger):
        # Remove the last two files and it an expected condition
        self.fs.rm("/test/dir/h")
        self.fs.rm("/test/dir/i")

        results = aggregator.operators.filter_on_presence(self.test_files, fs=self.fs)
        self.assertListEqual(results, self.expected[:-2])
        mock_logger.debug.assert_called_once_with(
            "Last %s of %s blobs in aggregation input are missing: %s",
            2,
            9,
            ["/test/dir/h", "/test/dir/i"],
        )

    @patch("aggregator.operators.logger")
    def test_filter_on_presence_gap(self, mock_logger):
        # Remove a file in the middle - a gap - and it emits a warning!
        self.fs.rm("/test/dir/c")
        results = aggregator.operators.filter_on_presence(self.test_files, fs=self.fs)
        self.expected.remove("/test/dir/c")
        self.assertListEqual(results, self.expected)
        mock_logger.info.assert_called_once_with(
            "Gap in aggregation input with %s of %s blobs missing:%s %s",
            1,
            9,
            "",
            ["/test/dir/c"],
        )


class HrrrForecastRunAggregatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_dask_client = Mock()
        self.mock_fs = Mock()
        self.instance = aggregator.operators.HrrrForecastRunAggregator(
            self.mock_dask_client,
            self.mock_fs,
            date_test_hook=datetime.date.fromisoformat("2022-08-01"),
        )

    def test_transform_recent_long(self):
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220801/hrrr.t06z.wrfsfcf17.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        self.assertEqual(2, self.mock_dask_client.submit.call_count)

        args, kwargs = self.mock_dask_client.submit.call_args_list[0]
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        self.assertEqual(len(args[2]), 19)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220801/hrrr.t06z.wrfsfcf{horizon:02d}.zarr"
                for horizon in range(0, 19)
            ],
        )
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220801/hrrr.t06z.wrfsfcf.18_hour_forecast.zarr",
        )

        args, kwargs = self.mock_dask_client.submit.call_args_list[1]
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        self.assertEqual(len(args[2]), 49)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220801/hrrr.t06z.wrfsfcf{horizon:02d}.zarr"
                for horizon in range(0, 49)
            ],
        )
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220801/hrrr.t06z.wrfsfcf.48_hour_forecast.zarr",
        )

    def test_transform_recent_short(self):
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220801/hrrr.t03z.wrfsfcf17.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        self.assertEqual(1, self.mock_dask_client.submit.call_count)

        args, kwargs = self.mock_dask_client.submit.call_args_list[0]
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        self.assertEqual(len(args[2]), 19)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220801/hrrr.t03z.wrfsfcf{horizon:02d}.zarr"
                for horizon in range(0, 19)
            ],
        )
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220801/hrrr.t03z.wrfsfcf.18_hour_forecast.zarr",
        )

    def test_transform_18hour(self):
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220601/hrrr.t02z.wrfsfcf18.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        self.assertEqual(1, self.mock_dask_client.submit.call_count)
        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        self.assertEqual(len(args[2]), 19)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220601/hrrr.t02z.wrfsfcf{horizon:02d}.zarr"
                for horizon in range(0, 19)
            ],
        )
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220601/hrrr.t02z.wrfsfcf.18_hour_forecast.zarr",
        )

    def test_transform_48hour(self):

        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220601/hrrr.t06z.wrfsfcf48.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        self.assertEqual(1, self.mock_dask_client.submit.call_count)
        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        self.assertEqual(len(args[2]), 49)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220601/hrrr.t06z.wrfsfcf{horizon:02d}.zarr"
                for horizon in range(0, 49)
            ],
        )
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/forecast_run/conus/hrrr.20220601/hrrr.t06z.wrfsfcf.48_hour_forecast.zarr",
        )

    def test_transform_14hour_horizon_ignored(self):

        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220601/hrrr.t02z.wrfsfcf14.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)
        self.mock_dask_client.assert_not_called()


class HrrrDailyHorizonAggregatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_dask_client = Mock()
        self.mock_fs = Mock()
        self.instance = aggregator.operators.HrrrDailyHorizonAggregator(
            self.mock_dask_client,
            self.mock_fs,
            datetime.date.fromisoformat("2022-08-01"),
        )

    def test_aggregate_daily_short_horizon_backfill_ignored(self):
        # This object event is ignored because it is not recent nor the last event for a given day

        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t14z.wrfsfcf05.zarr",
                bucketId="gcp-public-data-weather",
            )
        )

        # Not today or yesterday
        self.instance.transform(mock_message)
        self.mock_dask_client.assert_not_called()

    def test_aggregate_daily_short_horizon_backfill_end_of_day(self):
        # This object event completes a day so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t23z.wrfsfcf05.zarr",
                bucketId="gcp-public-data-weather",
            )
        )

        # Not today or yesterday
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 24)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t{hour:02d}z.wrfsfcf05.zarr"
                for hour in range(0, 24)
            ],
        )
        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220720/hrrr.wrfsfcf.05_hour_horizon.zarr",
        )

    def test_aggregate_daily_short_horizon_current_day(self):
        # This object event is a recent update so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220731/hrrr.t12z.wrfsfcf05.zarr",
                bucketId="gcp-public-data-weather",
            )
        )

        # Not today or yesterday
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 24)

        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220731/hrrr.t{hour:02d}z.wrfsfcf05.zarr"
                for hour in range(0, 24)
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220731/hrrr.wrfsfcf.05_hour_horizon.zarr",
        )

    def test_aggregate_daily_long_horizon_backfill_ignored(self):
        # This object event is ignored because it is not recent nor the last event for a given day
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220720/hrrr.t12z.wrfsfcf48.zarr",
                bucketId="gcp-public-data-weather",
            )
        )

        # Not today or yesterday
        self.instance.transform(mock_message)
        self.mock_dask_client.assert_not_called()

    def test_aggregate_daily_long_horizon_backfill_end_of_day(self):
        # This object event completes a day so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220520/hrrr.t18z.wrfsfcf42.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 24)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220520/hrrr.t{hour:02d}z.wrfsfcf{horizon:02d}.zarr"
                for hour in range(0, 24, 6)
                for horizon in range(37, 43)
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220520/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
        )

    def test_aggregate_daily_long_horizon_backfill_current_day(self):
        # This object event is a recent update so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220731/hrrr.t12z.wrfsfcf42.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 24)

        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/raw_zarr/conus/hrrr.20220731/hrrr.t{hour:02d}z.wrfsfcf{horizon:02d}.zarr"
                for hour in range(0, 24, 6)
                for horizon in range(37, 43)
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220731/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
        )


class HrrrMonthlyHorizonAggregatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_dask_client = Mock()
        self.mock_fs = Mock()
        self.instance = aggregator.operators.HrrrMonthlyHorizonAggregator(
            self.mock_dask_client,
            self.mock_fs,
            datetime.date.fromisoformat("2022-08-20"),
        )

    def test_aggregate_current_month(self):
        # This object event is a recent update, so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220810/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 31)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.202208{day:02d}/hrrr.wrfsfcf.37-42_hour_horizon.zarr"
                for day in range(1, 32)
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202208/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
        )

    def test_aggregate_last_day_of_month(self):
        # This object event is a recent update, so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220630/hrrr.wrfsfcf.12_hour_horizon.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 30)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.202206{day:02d}/hrrr.wrfsfcf.12_hour_horizon.zarr"
                for day in range(1, 31)
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202206/hrrr.wrfsfcf.12_hour_horizon.zarr",
        )

    def test_aggregate_noop_old_date(self):
        # This object event is a recent update, so it is processed
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/daily_horizon/conus/hrrr.20220610/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)
        self.mock_dask_client.submit.assert_not_called()


class HrrrAllTimeHorizonAggregatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_dask_client = Mock()
        self.mock_fs = Mock()
        self.instance = aggregator.operators.HrrrAllTimeHorizonAggregator(
            self.mock_dask_client,
            self.mock_fs,
            datetime.date.fromisoformat("2021-01-01"),
        )

    def test_aggregate_alltime(self):

        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                objectId="high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202009/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                bucketId="gcp-public-data-weather",
            )
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.multizarr)
        self.assertIs(args[1], self.mock_fs)
        # assert the input paths
        self.assertEqual(len(args[2]), 8)
        self.assertListEqual(
            args[2],
            [
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202006/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202007/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202008/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202009/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202010/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202011/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202012/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
                f"gcp-public-data-weather/high-resolution-rapid-refresh/version_2/monthly_horizon/conus/hrrr.202101/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
            ],
        )

        # assert the output path
        self.assertEqual(
            args[3],
            "gcp-public-data-weather/high-resolution-rapid-refresh/version_2/alltime_horizon/conus/hrrr.wrfsfcf.37-42_hour_horizon.zarr",
        )


class HrrrGrib2ZarrExtractorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_dask_client = Mock()
        self.mock_fs = Mock()
        self.instance = aggregator.operators.HrrrGrib2ZarrExtractor(
            self.mock_dask_client, self.mock_fs
        )

    def test_transform_selected(self):
        object = "hrrr.20220701/conus/hrrr.t00z.wrfsfcf18.grib2"
        bucket = "high-resolution-rapid-refresh"
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(objectId=object, bucketId=bucket, protocol="file")
        )
        self.instance.transform(mock_message)

        args, kwargs = self.mock_dask_client.submit.call_args
        self.assertIs(args[0], aggregator.operators.extract_grib)
        self.assertIsInstance(args[1], fsspec.spec.AbstractFileSystem)
        self.assertEqual(args[2], PurePosixPath(bucket))
        self.assertEqual(args[3], PurePosixPath(object))
        self.assertEqual(args[4], self.mock_fs)
        self.assertEqual(
            args[5], PurePosixPath(aggregator.operators.consts.EXTRACTED_BUCKET)
        )

    def test_transform_not_selected(self):
        object = "hrrr.20220701/conus/hrrr.t00z.foobar18.grib2"
        bucket = "high-resolution-rapid-refresh"
        mock_message = aggregator.operators.TestStructures.FakeMessage(
            attributes=dict(
                eventType="OBJECT_FINALIZE",
                objectId=object,
                bucketId=bucket,
            )
        )
        self.instance.transform(mock_message)
        self.mock_dask_client.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
