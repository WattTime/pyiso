from datetime import timedelta, datetime
from unittest import TestCase

import pytz
import requests_mock
from dateutil.parser import parse
from freezegun import freeze_time

from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture


class YukonEnergyClient(TestCase):
    def setUp(self):
        self.tzaware_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        self.c = client_factory('YUKON')

    def test_yukon_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    def test_get_generation_date_range_before_lower_bound_returns_empty(self):
        start_at = self.tzaware_utcnow - timedelta(hours=26)
        end_at = self.tzaware_utcnow - timedelta(hours=25)
        results = self.c.get_generation(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Generation mix results should be empty.')

    def test_get_generation_forecast_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(seconds=1)
        end_at = self.tzaware_utcnow + timedelta(hours=2)
        results = self.c.get_generation(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Generation mix forecast results should be empty.')

    def test_get_load_date_range_before_lower_bound_returns_empty(self):
        start_at = self.tzaware_utcnow - timedelta(hours=26)
        end_at = self.tzaware_utcnow - timedelta(hours=25)
        results = self.c.get_load(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Load results should be empty.')

    def test_get_load_forecast_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(seconds=1)
        end_at = self.tzaware_utcnow + timedelta(hours=2)
        results = self.c.get_load(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Load forecast results should be empty.')

    @freeze_time('2017-10-11T10:45:00Z')
    @requests_mock.Mocker()
    def test_get_load_latest_returns_expected(self, mocked_request):
        frozen_client = client_factory('YUKON')
        expected_url = 'http://www.yukonenergy.ca/consumption/chart_current.php?chart=current'
        expected_response = read_fixture(frozen_client.__module__, 'current.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = frozen_client.get_load(latest=True)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 38.74)

    @freeze_time('2017-10-11T10:45:00Z')
    @requests_mock.Mocker()
    def test_get_generation_latest_returns_expected(self, mocked_request):
        frozen_client = client_factory('YUKON')
        expected_url = 'http://www.yukonenergy.ca/consumption/chart_current.php?chart=current'
        expected_response = read_fixture(frozen_client.__module__, 'current.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = frozen_client.get_generation(latest=True)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertEqual(results[0]['fuel_name'], 'hydro')
        self.assertAlmostEqual(results[0]['gen_MW'], 38.74)
        self.assertEqual(results[1]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertEqual(results[1]['fuel_name'], 'thermal')
        self.assertAlmostEqual(results[1]['gen_MW'], 0)

    @freeze_time('2017-10-11T10:45:00Z')
    @requests_mock.Mocker()
    def test_get_generation_valid_date_range_during_dst_returns_expected(self, mocked_request):
        frozen_client = client_factory('YUKON')
        frozen_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_at = frozen_utcnow - timedelta(hours=12)
        end_at = frozen_utcnow
        expected_url = 'http://www.yukonenergy.ca/consumption/chart.php?chart=hourly'
        expected_response = read_fixture(frozen_client.__module__, 'hourly_2017-10-11.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = frozen_client.get_generation(start_at=start_at, end_at=end_at)

        expected_length = 22  # 2 fuels * 11 hours (1 hour is missing in this particular response)
        self.assertEqual(len(results), expected_length)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-10T23:00:00Z'))
        self.assertEqual(results[0]['fuel_name'], 'hydro')
        self.assertAlmostEqual(results[0]['gen_MW'], 51.36)
        self.assertEqual(results[21]['timestamp'], parse('2017-10-11T09:00:00Z'))
        self.assertEqual(results[21]['fuel_name'], 'thermal')
        self.assertAlmostEqual(results[21]['gen_MW'], 0)

    @freeze_time('2017-10-11T10:45:00Z')
    @requests_mock.Mocker()
    def test_get_load_valid_date_range_during_dst_returns_expected(self, mocked_request):
        frozen_client = client_factory('YUKON')
        frozen_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_at = frozen_utcnow - timedelta(hours=12)
        end_at = frozen_utcnow
        expected_url = 'http://www.yukonenergy.ca/consumption/chart.php?chart=hourly'
        expected_response = read_fixture(frozen_client.__module__, 'hourly_2017-10-11.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = frozen_client.get_load(start_at=start_at, end_at=end_at)

        expected_length = 11  # 11 hours (1 hour is missing in this particular response)
        self.assertEqual(len(results), expected_length)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-10T23:00:00Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 51.36)
        self.assertEqual(results[10]['timestamp'], parse('2017-10-11T09:00:00Z'))
        self.assertAlmostEqual(results[10]['load_MW'], 38.94)

    def test_get_trade_latest_returns_zero(self):
        results = self.c.get_trade(latest=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['net_exp_MW'], 0)

    def test_get_trade_date_range_retuns_zeros(self):
        start_at = self.tzaware_utcnow - timedelta(hours=12)
        end_at = self.tzaware_utcnow

        results = self.c.get_trade(start_at=start_at, end_at=end_at)

        self.assertEqual(len(results), 12)
        for result in results:
            self.assertEqual(result['net_exp_MW'], 0)
