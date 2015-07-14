from pyiso import client_factory
from unittest import TestCase
import pytz
import logging


class TestISONE(TestCase):
    def setUp(self):
        self.c = client_factory('ISONE')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.DEBUG)

    def test_auth(self):
        """Auth info should be set up from env during init"""
        self.assertEqual(len(self.c.auth), 2)

    def test_utcify(self):
        ts_str = '2014-05-03T02:32:44.000-04:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 2+4)
        self.assertEqual(ts.minute, 32)
        self.assertEqual(ts.second, 44)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_json_format(self):
        data = self.c.fetch_data('/genfuelmix/current.json', self.c.auth)

        # one item, GenFuelMixes
        self.assertIn('GenFuelMixes', data.keys())
        self.assertEqual(len(data.keys()), 1)

        # one item, GenFuelMix
        self.assertIn('GenFuelMix', data['GenFuelMixes'].keys())
        self.assertEqual(len(data['GenFuelMixes'].keys()), 1)

        # multiple items
        self.assertGreater(len(data['GenFuelMixes']['GenFuelMix']), 1)
        self.assertEqual(sorted(data['GenFuelMixes']['GenFuelMix'][0].keys()),
                         sorted(['FuelCategory', 'BeginDate', 'MarginalFlag', 'FuelCategoryRollup', 'GenMw']))
