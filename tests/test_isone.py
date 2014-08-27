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
        self.assertItemsEqual(data.keys(), ['GenFuelMixes'])
        self.assertItemsEqual(data['GenFuelMixes'].keys(), ['GenFuelMix'])
        self.assertGreater(len(data['GenFuelMixes']['GenFuelMix']), 1)
        self.assertItemsEqual(data['GenFuelMixes']['GenFuelMix'][0].keys(),
                              ['FuelCategory', 'BeginDate', 'MarginalFlag', 'FuelCategoryRollup', 'GenMw'])