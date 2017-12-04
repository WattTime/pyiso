import pytz
from pandas import ExcelFile
from pandas import Timestamp
from unittest import TestCase
from datetime import datetime, timedelta
from freezegun import freeze_time
from mock import MagicMock
from pyiso import client_factory
from pyiso.base import BaseClient
from tests import fixture_path


@freeze_time('2017-10-16T12:43:00Z')
class TestBCHydroClient(TestCase):
    def setUp(self):
        self.tzaware_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        self.c = client_factory('BCH')

    def test_bchydro_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    def test_get_trade_date_range_before_lower_bound_returns_empty(self):
        start_at = self.tzaware_utcnow - timedelta(days=11)
        end_at = self.tzaware_utcnow - timedelta(days=10)
        results = self.c.get_trade(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Trade results should be empty.')

    def test_get_trade_forecast_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(seconds=1)
        end_at = self.tzaware_utcnow + timedelta(hours=2)
        results = self.c.get_trade(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Trade forecast results should be empty.')

    def test_get_trade_latest_returns_expected(self):
        xls_io = fixture_path(ba_name=self.c.__module__, filename='data1.xls')
        self.c.fetch_xls = MagicMock(return_value=ExcelFile(xls_io))

        results = self.c.get_trade(latest=True)
        self.assertTrue(len(results), 1)
        self.assertEqual(results[0]['timestamp'], Timestamp('2017-10-16T12:40:00Z'))
        self.assertAlmostEqual(results[0]['net_exp_MW'], 964.22479248)

    def test_get_trade_yesterday_returns_expected(self):
        xls_io = fixture_path(ba_name=self.c.__module__, filename='data1.xls')
        self.c.fetch_xls = MagicMock(return_value=ExcelFile(xls_io))

        results = self.c.get_trade(yesterday=True)
        self.assertTrue(len(results), 288)  # 24 hours * 12 observations per hour
        self.assertEqual(results[0]['timestamp'], Timestamp('2017-10-15T07:00:00Z'))
        self.assertAlmostEqual(results[0]['net_exp_MW'], 662.693040848)
        self.assertEqual(results[287]['timestamp'], Timestamp('2017-10-16T06:55:00Z'))
        self.assertAlmostEqual(results[287]['net_exp_MW'], 700.70085144)

    def test_get_trade_valid_range_returns_expected(self):
        xls_io = fixture_path(ba_name=self.c.__module__, filename='data1.xls')
        self.c.fetch_xls = MagicMock(return_value=ExcelFile(xls_io))
        start_at = self.tzaware_utcnow - timedelta(days=3)
        end_at = self.tzaware_utcnow - timedelta(days=1)

        results = self.c.get_trade(start_at=start_at, end_at=end_at)
        self.assertTrue(len(results), 576)  # 48 hours * 12 observations per hour
        self.assertEqual(results[0]['timestamp'], Timestamp('2017-10-13T12:45:00Z'))
        self.assertAlmostEqual(results[0]['net_exp_MW'], 1072.920944214)
        self.assertEqual(results[575]['timestamp'], Timestamp('2017-10-15T12:40:00Z'))
        self.assertAlmostEqual(results[575]['net_exp_MW'], 506.151199341)
