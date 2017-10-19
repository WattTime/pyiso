from datetime import datetime, timedelta
from unittest import TestCase

import pytz
import requests_mock
from dateutil.parser import parse
from freezegun import freeze_time

from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture


@freeze_time('2017-10-05T11:45:00Z')
class TestNSPower(TestCase):
    def setUp(self):
        self.tzaware_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        self.c = client_factory('NSP')

    def test_nspower_from_client_factory(self):
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

    def test_get_load_date_range_after_upper_bound_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(hours=24)
        end_at = self.tzaware_utcnow + timedelta(hours=25)
        results = self.c.get_load(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Load forecast results should be empty.')

    @requests_mock.Mocker()
    def test_get_generation_valid_date_range_returns_expected(self, mocked_request):
        start_at = self.tzaware_utcnow - timedelta(hours=12)
        end_at = self.tzaware_utcnow
        expected_url = 'http://www.nspower.ca/system_report/today/currentmix.json'
        expected_response = read_fixture(self.c.__module__, 'currentmix.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_generation(start_at=start_at, end_at=end_at)

        expected_length = 96  # 8 fuels * 12 hours
        self.assertEqual(len(results), expected_length)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-05T00:00:00.000Z'))
        self.assertEqual(results[0]['fuel_name'], 'biomass')
        self.assertAlmostEqual(results[0]['gen_MW'], 1.95)
        self.assertEqual(results[95]['timestamp'], parse('2017-10-05T11:00:00.000Z'))
        self.assertEqual(results[95]['fuel_name'], 'wind')
        self.assertAlmostEqual(results[95]['gen_MW'], 35.52)

    @requests_mock.Mocker()
    def test_get_generation_latest_returns_expected(self, mocked_request):
        expected_url = 'http://www.nspower.ca/system_report/today/currentmix.json'
        expected_response = read_fixture(self.c.__module__, 'currentmix.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_generation(latest=True)

        expected_length = 8  # 8 fuels
        self.assertEqual(len(results), expected_length)

        # Check that all datetime values are equal and known fuel values
        expected_datetime = parse('2017-10-05T11:00:00.000Z')
        expected_mw_by_fuel = {
            'coal': 44.91,
            'dual': 14.13,
            'oil': 0.02,
            'ccgt': 0,
            'biomass': 2.44,
            'hydro': 2.74,
            'wind': 35.52,
            'other': 0.25
        }
        for result in results:
            self.assertEqual(result['timestamp'], expected_datetime)
            expected_gen_mw = expected_mw_by_fuel.get(result['fuel_name'], -1)
            self.assertAlmostEqual(result['gen_MW'], expected_gen_mw)

    @requests_mock.Mocker()
    def test_get_load_valid_date_range_returns_expected(self, mocked_request):
        hours = 12
        start_at = self.tzaware_utcnow - timedelta(hours=hours)
        end_at = self.tzaware_utcnow
        expected_url = 'http://www.nspower.ca/system_report/today/currentload.json'
        expected_response = read_fixture(self.c.__module__, 'currentload.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(results), hours)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-05T00:00:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 877.25)
        self.assertEqual(results[11]['timestamp'], parse('2017-10-05T11:00:00.000Z'))
        self.assertAlmostEqual(results[11]['load_MW'], 892.64)

    @requests_mock.Mocker()
    def test_get_load_latest_returns_expected(self, mocked_request):
        expected_url = 'http://www.nspower.ca/system_report/today/currentload.json'
        expected_response = read_fixture(self.c.__module__, 'currentload.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_load(latest=True)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['timestamp'], parse('2017-10-05T11:00:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 892.64)

    @requests_mock.Mocker()
    def test_get_load_valid_forecast_returns_expected(self, mocked_request):
        hours = 12
        start_at = self.tzaware_utcnow
        end_at = self.tzaware_utcnow + timedelta(hours=hours)
        expected_url = 'http://www.nspower.ca/system_report/today/forecast.json'
        expected_response = read_fixture(self.c.__module__, 'forecast.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(results), hours)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-05T12:00:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 1016)
        self.assertEqual(results[11]['timestamp'], parse('2017-10-05T23:00:00.000Z'))
        self.assertAlmostEqual(results[11]['load_MW'], 1020)

    @requests_mock.Mocker()
    def test_get_load_historical_and_forecast_date_range_returns_expected(self, mocked_request):
        hours = 12
        start_at = self.tzaware_utcnow - timedelta(hours=hours)
        end_at = self.tzaware_utcnow + timedelta(hours=hours)
        expected_historical_url = 'http://www.nspower.ca/system_report/today/currentload.json'
        expected_historical_response = read_fixture(self.c.__module__, 'currentload.json')
        mocked_request.get(expected_historical_url, content=expected_historical_response.encode('utf-8'))
        expected_forecast_url = 'http://www.nspower.ca/system_report/today/forecast.json'
        expected_forecast_reponse = read_fixture(self.c.__module__, 'forecast.json')
        mocked_request.get(expected_forecast_url, content=expected_forecast_reponse.encode('utf-8'))

        results = self.c.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(results), hours * 2)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-10-05T00:00:00.000Z'))
        self.assertAlmostEqual(results[0]['load_MW'], 877.25)
        self.assertEqual(results[23]['timestamp'], parse('2017-10-05T23:00:00.000Z'))
        self.assertAlmostEqual(results[23]['load_MW'], 1020)
