from datetime import timedelta, datetime
from unittest import TestCase

import pytz
import requests_mock
from dateutil.parser import parse
from freezegun import freeze_time

from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture


@freeze_time('2017-10-11T10:45:00Z')
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

    @requests_mock.Mocker()
    def test_get_load_latest_returns_expected(self, mocked_request):
        expected_url = 'http://www.yukonenergy.ca/consumption/chart_current.php?chart=current'
        expected_response = read_fixture(self.c.NAME, 'current.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_load(latest=True)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 38.74)

    @requests_mock.Mocker()
    def test_get_generation_latest_returns_expected(self, mocked_request):
        expected_url = 'http://www.yukonenergy.ca/consumption/chart_current.php?chart=current'
        expected_response = read_fixture(self.c.NAME, 'current.html')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_generation(latest=True)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertEqual(results[0]['fuel_name'], 'hydro')
        self.assertAlmostEqual(results[0]['gen_MW'], 38.74)
        self.assertEqual(results[1]['timestamp'], parse('2017-10-11T10:40:00.000Z'))
        self.assertEqual(results[1]['fuel_name'], 'thermal')
        self.assertAlmostEqual(results[1]['gen_MW'], 0)
