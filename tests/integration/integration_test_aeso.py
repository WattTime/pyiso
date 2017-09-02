from datetime import timedelta
from unittest import TestCase

from pyiso import client_factory


class IntegrationTestAESO(TestCase):
    def setUp(self):
        self.aeso_client = client_factory('AESO')

    def test_get_generation_latest(self):
        generation_ts = self.aeso_client.get_generation(latest=True)

        self.assertEqual(len(generation_ts), 5)  # Five fuels
        for row in generation_ts:
            self.assertIn(row.get('fuel_name', None), self.aeso_client.fuels)

    def test_get_load_yesterday(self):
        load_timeseries = self.aeso_client.get_load(yesterday=True)
        # 24 hours + 1 because BaseClient sets end_date to 00:00 of current day and
        # end_date must be inclusive. o_O
        self.assertEqual(len(load_timeseries), 25)

    def test_get_load_historical(self):
        local_now = self.aeso_client.local_now()
        start_at = local_now - timedelta(days=1)
        end_at = local_now
        load_timeseries = self.aeso_client.get_load(start_at=start_at, end_at=end_at)
        prev_entry = None
        for entry in load_timeseries:
            if prev_entry:
                secondsdelta = (entry['timestamp'] - prev_entry['timestamp']).total_seconds()
                # Each timestamp should be one hour apart.
                self.assertEqual(3600, secondsdelta)
            prev_entry = entry
