from datetime import datetime
from unittest import TestCase

from pyiso import client_factory


class IntegrationTestSaskPowerClient(TestCase):
    def setUp(self):
        self.sask_client = client_factory('SASK')

    def test_get_load_latest(self):
        load_ts = self.sask_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertLess(load_ts[0].get('timestamp', datetime.max), self.sask_client.local_now())
        self.assertGreater(load_ts[0].get('load_MW', 0), 0)
