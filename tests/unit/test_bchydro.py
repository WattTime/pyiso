import pytz
from unittest import TestCase
from datetime import datetime, timedelta
from pyiso import client_factory
from pyiso.base import BaseClient


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
