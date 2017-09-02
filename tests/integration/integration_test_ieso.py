from datetime import timedelta
from unittest import TestCase
from nose.plugins.skip import SkipTest

import pytz

from pyiso import client_factory, LOGGER


class IntegrationTestIESO(TestCase):
    def setUp(self):
        self.ieso_client = client_factory('IESO')

    def test_get_generation_full_range(self):
        start_at = self.ieso_client.local_now - timedelta(days=5)
        end_at = self.ieso_client.local_now + timedelta(days=2)
        generation_ts = self.ieso_client.get_generation(start_at=start_at, end_at=end_at)
        self.assertFalse(len(generation_ts) == 0)

    def test_get_generation_yesterday(self):
        generation_ts = self.ieso_client.get_generation(yesterday=True)
        # 24 hours * 6 fuels = 144. +6 fuels because BaseClient sets end_date to 00:00 of current day and end_date must
        # be inclusive.
        self.assertEqual(len(generation_ts), 150)

    def test_get_generation_latest(self):
        generation_ts = self.ieso_client.get_generation(latest=True)
        self.assertEqual(len(generation_ts), 6)  # 6 fuels

    @SkipTest
    def test_get_load_full_range(self):
        start_at = self.ieso_client.local_now - timedelta(days=3)
        end_at = self.ieso_client.local_now + timedelta(days=2)
        load_ts = self.ieso_client.get_load(start_at=start_at, end_at=end_at)
        self._assert_entires_5min_apart(load_ts)

    def test_get_load_yesterday(self):
        load_ts = self.ieso_client.get_load(yesterday=True)
        self._assert_entires_5min_apart(load_ts)
        # 24 hours * 12 five-minute intervals + 1 because BaseClient sets end_date to 00:00 of current day and
        # end_date must be inclusive.
        self.assertEqual(len(load_ts), 289)

    def test_get_load_latest(self):
        load_ts = self.ieso_client.get_load(latest=True)
        self.assertEqual(len(load_ts), 1)

    @SkipTest
    def test_get_trade_full_range(self):
        start_at = self.ieso_client.local_now - timedelta(days=3)
        end_at = self.ieso_client.local_now + timedelta(days=2)
        trade_ts = self.ieso_client.get_trade(start_at=start_at, end_at=end_at)
        self._assert_entires_5min_apart(trade_ts)

    def test_get_trade_yesterday(self):
        trade_ts = self.ieso_client.get_trade(yesterday=True)
        self._assert_entires_5min_apart(trade_ts)
        # 24 hours * 12 five-minute intervals + 1 because BaseClient sets end_date to 00:00 of current day and
        # end_date must be inclusive.
        self.assertEqual(len(trade_ts), 289)

    def test_get_trade_latest(self):
        trade_ts = self.ieso_client.get_trade(latest=True)
        self.assertEqual(len(trade_ts), 1)
        self.assertNotEqual(trade_ts[0]['net_exp_MW'], 0)

    def _assert_entires_5min_apart(self, result_ts):
        prev_entry = None
        for entry in result_ts:
            if prev_entry:
                seconds_delta = (entry['timestamp'] - prev_entry['timestamp']).total_seconds()
                if seconds_delta > 300:
                    LOGGER.error('prev_entry timestamp: ' + str(
                        prev_entry['timestamp'].astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
                    ))
                    LOGGER.error('entry timestamp: ' + str(
                        entry['timestamp'].astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
                    ))
                self.assertEqual(300, seconds_delta)
            prev_entry = entry
