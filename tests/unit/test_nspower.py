from datetime import datetime, timedelta
from unittest import TestCase

import pytz

from pyiso import client_factory
from pyiso.base import BaseClient


class TestNSPower(TestCase):
    def setUp(self):
        self.tzaware_utcnow = datetime.utcnow().replace(tzinfo=pytz.utc)
        self.c = client_factory('NSP')

    def test_nspower_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    def test_generation_date_range_before_lower_bound_returns_empty(self):
        start_at = self.tzaware_utcnow - timedelta(hours=26)
        end_at = self.tzaware_utcnow - timedelta(hours=25)
        results = self.c.get_generation(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Generation mix results should be empty.')

    def test_generation_forecast_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(seconds=1)
        end_at = self.tzaware_utcnow + timedelta(hours=2)
        results = self.c.get_generation(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Generation mix forecast results should be empty.')

    def test_load_date_range_before_lower_bound_returns_empty(self):
        start_at = self.tzaware_utcnow - timedelta(hours=26)
        end_at = self.tzaware_utcnow - timedelta(hours=25)
        results = self.c.get_load(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Load results should be empty.')

    def test_load_date_range_after_upper_bound_returns_empty(self):
        start_at = self.tzaware_utcnow + timedelta(hours=24)
        end_at = self.tzaware_utcnow + timedelta(hours=25)
        results = self.c.get_load(start_at=start_at, end_at=end_at)
        self.assertListEqual(results, [], 'Load forecast results should be empty.')
