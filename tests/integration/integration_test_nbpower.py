from datetime import timedelta, datetime
from unittest import TestCase

import pytz

from pyiso import client_factory, LOGGER


class IntegrationTestNBP(TestCase):
    def setUp(self):
        self.nbpower_client = client_factory('NBP')

    def test_get_load_latest(self):
        load_ts = self.nbpower_client.get_load(latest=True)
        self.assertEqual(len(load_ts), 1)
        self.assertGreater(load_ts[0]['timestamp'], self.nbpower_client.local_now() - timedelta(hours=1))
        self.assertGreater(load_ts[0]['load_MW'], 0)

    def test_get_load_time_range(self):
        start_at = pytz.utc.localize(datetime.utcnow()) - timedelta(hours=1)
        end_at = pytz.utc.localize(datetime.utcnow() + timedelta(hours=4))
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)
        self.assertGreater(len(load_ts), 0)

    def test_get_trade_latest(self):
        trade_ts = self.nbpower_client.get_trade(latest=True)
        self.assertEqual(len(trade_ts), 1)
        self.assertGreater(trade_ts[0]['timestamp'], self.nbpower_client.local_now() - timedelta(hours=1))
        self.assertNotEqual(trade_ts[0]['net_exp_MW'], 0)

    def _assert_entries_1hr_apart(self, result_ts):
        prev_entry = None
        for entry in result_ts:
            if prev_entry:
                seconds_delta = (entry['timestamp'] - prev_entry['timestamp']).total_seconds()
                if seconds_delta > 3600:
                    LOGGER.error('prev_entry timestamp: ' + str(
                        prev_entry['timestamp'].astimezone(pytz.timezone(self.nbpower_client.TZ_NAME))
                    ))
                    LOGGER.error('entry timestamp: ' + str(
                        entry['timestamp'].astimezone(pytz.timezone(self.nbpower_client.TZ_NAME))
                    ))
                self.assertEqual(3600, seconds_delta)
            prev_entry = entry
