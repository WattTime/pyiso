from datetime import datetime, timedelta
from dateutil.parser import parse
from unittest import TestCase
from freezegun import freeze_time
from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture
import requests_mock
import pytz


@freeze_time('2017-09-29T12:55:00Z')
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
        expected_response = read_fixture(self.c.NAME, 'currentmix.json')
        mocked_request.get(expected_url, content=expected_response.encode('utf-8'))

        results = self.c.get_generation(start_at=start_at, end_at=end_at)

        expected_length = 96  # 8 * 12 fuels
        self.assertEqual(len(results), expected_length)

        # Spot check values at the start and end of the results
        self.assertEqual(results[0]['timestamp'], parse('2017-09-29T01:00:00.000Z'))
        self.assertEqual(results[0]['fuel_name'], 'biomass')
        self.assertEqual(results[0]['gen_MW'], 4.83)
        self.assertEqual(results[95]['timestamp'], parse('2017-09-29T12:00:00.000Z'))
        self.assertEqual(results[95]['fuel_name'], 'wind')
        self.assertEqual(results[95]['gen_MW'], 23.27)
